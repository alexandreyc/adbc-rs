//! Load and use ADBC drivers.
//!
//! The driver manager provides an implementation of the ADBC interface which
//! uses FFI to wrap an object file implementation of
//! [`adbc.h`](https://github.com/apache/arrow-adbc/blob/main/adbc.h).
//!
//! There are three ways that drivers can be loaded:
//! 1. By statically linking the driver implementation using
//! [DriverManager::load_static].
//! 2. By dynamically linking the driver implementation using
//! [DriverManager::load_static].
//! 3. By loading the driver implementation at runtime (with
//! `dlopen/LoadLibrary`) using [DriverManager::load_dynamic].
//!
//! Drivers are initialized using a function provided by the driver as a main
//! entrypoint, canonically called `AdbcDriverInit`. Although many will use a
//! different name to support statically linking multiple drivers within the
//! same program.
//!
//! ## Using across threads
//!
//! [DriverManager] and [ManagedDatabase] are [Send] and [Sync] and thus can be
//! used across multiple threads. They hold their inner implementations within
//! [std::sync::Arc], so they are cheaply clonable.
//!
//! [ManagedConnection] and [ManagedStatement] aren't [Send] nor [Sync] and thus
//! cannot be used across multiple threads. So instead of using the same
//! connection across multiple threads, create a connection for each thread.
//!
//! ## Example
//!
//! ```rust
//! # use std::sync::Arc;
//! # use arrow::{
//! #     array::{Array, StringArray, Int64Array, Float64Array},
//! #     record_batch::{RecordBatch, RecordBatchReader},
//! #     datatypes::{Field, Schema, DataType},
//! #     compute::concat_batches,
//! # };
//! # use adbc_rs::{
//! #     driver_manager::DriverManager,
//! #     options::{AdbcVersion, DatabaseOptionKey, StatementOptionKey},
//! #     Connection, Database, Driver, Statement, Optionable
//! # };
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let opts = [(DatabaseOptionKey::Uri, ":memory:".into())];
//! let driver = DriverManager::load_dynamic("adbc_driver_sqlite", None, AdbcVersion::V100)?;
//! let database = driver.new_database_with_opts(opts.into_iter())?;
//! let connection = database.new_connection()?;
//! let statement = connection.new_statement()?;
//!
//! // Define some data.
//! # let columns: Vec<Arc<dyn Array>> = vec![
//! #     Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
//! #     Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
//! #     Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
//! # ];
//! # let schema = Schema::new(vec![
//! #     Field::new("a", DataType::Int64, true),
//! #     Field::new("b", DataType::Float64, true),
//! #     Field::new("c", DataType::Utf8, true),
//! # ]);
//! let input: RecordBatch = RecordBatch::try_new(Arc::new(schema), columns)?;
//!
//! // Ingest data.
//! statement.set_option(StatementOptionKey::TargetTable, "my_table".into())?;
//! statement.bind(input.clone())?;
//! statement.execute_update()?;
//!
//! // Extract data.
//! statement.set_sql_query("select * from my_table")?;
//! let output = statement.execute()?;
//! let schema = output.schema();
//! let output: Result<Vec<RecordBatch>, _> = output.collect();
//! let output = concat_batches(&schema, &output?)?;
//! assert_eq!(input, output);
//!
//! # Ok(())
//! # }
//! ```

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::ops::{Deref, DerefMut};
use std::os::raw::{c_char, c_void};
use std::ptr::{null, null_mut};
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, RecordBatch, RecordBatchReader, StructArray};
use arrow::ffi::{to_ffi, FFI_ArrowSchema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use crate::{
    error::Status,
    options::{self, AdbcVersion, OptionValue},
    Error, Result,
};
use crate::{ffi, ffi::types::driver_method, Optionable};
use crate::{Connection, Database, Driver, Statement};

const ERR_ONLY_STRING_OPT: &str = "Only string option value are supported with ADBC 1.0.0";
const ERR_CANCEL_UNSUPPORTED: &str =
    "Canceling connection or statement is not supported with ADBC 1.0.0";
const ERR_STATISTICS_UNSUPPORTED: &str = "Statistics are not supported with ADBC 1.0.0";

pub(crate) fn check_status(
    status: ffi::FFI_AdbcStatusCode,
    error: ffi::FFI_AdbcError,
) -> Result<()> {
    match status {
        ffi::constants::ADBC_STATUS_OK => Ok(()),
        _ => {
            let mut error: Error = error.into();
            error.status = Some(status.into());
            Err(error)
        }
    }
}

struct DriverManagerInner {
    driver: Mutex<ffi::FFI_AdbcDriver>,
    version: AdbcVersion, // Driver version
    _library: Option<libloading::Library>,
}

/// Implementation of [Driver].
#[derive(Clone)]
pub struct DriverManager {
    inner: Arc<DriverManagerInner>,
}

impl DriverManager {
    /// Load a driver from an initialization function.
    pub fn load_static(init: &crate::AdbcDriverInitFunc, version: AdbcVersion) -> Result<Self> {
        let driver = Self::load_impl(init, version)?;
        let inner = Arc::new(DriverManagerInner {
            driver: Mutex::new(driver),
            version,
            _library: None,
        });
        Ok(DriverManager { inner })
    }

    /// Load a driver from a dynamic library.
    ///
    /// Will attempt to load the dynamic library with the given `name`, find the
    /// symbol with name `entrypoint` (defaults to `AdbcDriverInit` if `None`),
    /// and then call create the driver using the resolved function.
    ///
    /// The `name` should not include any platform-specific prefixes or suffixes.
    /// For example, use `adbc_driver_sqlite` rather than `libadbc_driver_sqlite.so`.
    pub fn load_dynamic(
        name: &str,
        entrypoint: Option<&[u8]>,
        version: AdbcVersion,
    ) -> Result<Self> {
        let entrypoint = entrypoint.unwrap_or(b"AdbcDriverInit");
        let library = unsafe { libloading::Library::new(libloading::library_filename(name))? };
        let init: libloading::Symbol<ffi::FFI_AdbcDriverInitFunc> =
            unsafe { library.get(entrypoint)? };
        let driver = Self::load_impl(&init, version)?;
        let inner = Arc::new(DriverManagerInner {
            driver: Mutex::new(driver),
            version,
            _library: Some(library),
        });
        Ok(DriverManager { inner })
    }

    fn load_impl(
        init: &ffi::FFI_AdbcDriverInitFunc,
        version: AdbcVersion,
    ) -> Result<ffi::FFI_AdbcDriver> {
        let mut error = ffi::FFI_AdbcError::default();
        let mut driver = ffi::FFI_AdbcDriver::default();
        let status = unsafe {
            init(
                version.into(),
                &mut driver as *mut ffi::FFI_AdbcDriver as *mut c_void,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(driver)
    }
}

impl Driver for DriverManager {
    type DatabaseType = ManagedDatabase;

    fn new_database(&self) -> Result<Self::DatabaseType> {
        let opts: [(<Self::DatabaseType as Optionable>::Key, OptionValue); 0] = [];
        self.new_database_with_opts(opts.into_iter())
    }

    fn new_database_with_opts(
        &self,
        opts: impl Iterator<Item = (<Self::DatabaseType as Optionable>::Key, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut driver = self.inner.driver.lock().unwrap();
        let mut database = ffi::FFI_AdbcDatabase::default();

        // DatabaseNew
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(driver, DatabaseNew);
        let status = unsafe { method(&mut database, &mut error) };
        check_status(status, error)?;

        // DatabaseSetOption
        for (key, value) in opts {
            set_option_database(
                driver.deref_mut(),
                &mut database,
                self.inner.version,
                key,
                value,
            )?;
        }

        // DatabaseInit
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(&mut driver, DatabaseInit);
        let status = unsafe { method(&mut database, &mut error) };
        check_status(status, error)?;

        let inner = Arc::new(ManagedDatabaseInner {
            database: Mutex::new(database),
            version: self.inner.version,
            driver: self.inner.clone(),
        });
        Ok(Self::DatabaseType { inner })
    }
}

fn set_option_database(
    driver: &ffi::FFI_AdbcDriver,
    database: &mut ffi::FFI_AdbcDatabase,
    version: AdbcVersion,
    key: impl AsRef<str>,
    value: OptionValue,
) -> Result<()> {
    let key = CString::new(key.as_ref())?;
    let mut error = ffi::FFI_AdbcError::default();
    let status = match (version, value) {
        (_, OptionValue::String(value)) => {
            let value = CString::new(value)?;
            let method = driver_method!(driver, DatabaseSetOption);
            unsafe { method(database, key.as_ptr(), value.as_ptr(), &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Bytes(value)) => {
            let method = driver_method!(driver, DatabaseSetOptionBytes);
            unsafe {
                method(
                    database,
                    key.as_ptr(),
                    value.as_ptr(),
                    value.len(),
                    &mut error,
                )
            }
        }
        (AdbcVersion::V110, OptionValue::Int(value)) => {
            let method = driver_method!(driver, DatabaseSetOptionInt);
            unsafe { method(database, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Double(value)) => {
            let method = driver_method!(driver, DatabaseSetOptionDouble);
            unsafe { method(database, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V100, _) => Err(Error::with_message_and_status(
            ERR_ONLY_STRING_OPT,
            Status::NotImplemented,
        ))?,
    };
    check_status(status, error)
}

fn get_option_bytes<F>(key: impl AsRef<str>, mut populate: F) -> Result<Vec<u8>>
where
    F: FnMut(
        *const c_char,
        *mut u8,
        *mut usize,
        *mut ffi::FFI_AdbcError,
    ) -> ffi::FFI_AdbcStatusCode,
{
    const DEFAULT_LENGTH: usize = 128;
    let key = CString::new(key.as_ref())?;
    let mut run = |length| {
        let mut value = vec![0u8; length];
        let mut length: usize = value.len();
        let mut error = ffi::FFI_AdbcError::default();
        (
            populate(key.as_ptr(), value.as_mut_ptr(), &mut length, &mut error),
            length,
            value,
            error,
        )
    };

    let (status, length, value, error) = run(DEFAULT_LENGTH);
    check_status(status, error)?;

    if length <= DEFAULT_LENGTH {
        Ok(value[..length].to_vec())
    } else {
        let (status, _, value, error) = run(length);
        check_status(status, error)?;
        Ok(value)
    }
}

fn get_option_string<F>(key: impl AsRef<str>, mut populate: F) -> Result<String>
where
    F: FnMut(
        *const c_char,
        *mut c_char,
        *mut usize,
        *mut ffi::FFI_AdbcError,
    ) -> ffi::FFI_AdbcStatusCode,
{
    const DEFAULT_LENGTH: usize = 128;
    let key = CString::new(key.as_ref())?;
    let mut run = |length| {
        let mut value: Vec<c_char> = vec![0; length];
        let mut length: usize = value.len();
        let mut error = ffi::FFI_AdbcError::default();
        (
            populate(key.as_ptr(), value.as_mut_ptr(), &mut length, &mut error),
            length,
            value,
            error,
        )
    };

    let (status, length, value, error) = run(DEFAULT_LENGTH);
    check_status(status, error)?;

    let value = if length <= DEFAULT_LENGTH {
        value[..length].to_vec()
    } else {
        let (status, _, value, error) = run(length);
        check_status(status, error)?;
        value
    };

    let value = unsafe { CStr::from_ptr(value.as_ptr()) };
    Ok(value.to_string_lossy().to_string())
}

struct ManagedDatabaseInner {
    database: Mutex<ffi::FFI_AdbcDatabase>,
    driver: Arc<DriverManagerInner>,
    version: AdbcVersion,
}

impl Drop for ManagedDatabaseInner {
    fn drop(&mut self) {
        let driver = self.driver.driver.lock().unwrap();
        let mut database = self.database.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(driver, DatabaseRelease);
        let status = unsafe { method(database.deref_mut(), &mut error) };
        if let Err(err) = check_status(status, error) {
            panic!("unable to drop database: {:?}", err);
        }
    }
}

/// Implementation of [Database].
#[derive(Clone)]
pub struct ManagedDatabase {
    inner: Arc<ManagedDatabaseInner>,
}

impl Optionable for ManagedDatabase {
    type Key = options::DatabaseOptionKey;
    fn get_option_bytes(&self, key: Self::Key) -> Result<Vec<u8>> {
        let driver = self.inner.driver.driver.lock().unwrap();
        let mut database = self.inner.database.lock().unwrap();
        let method = driver_method!(driver, DatabaseGetOptionBytes);
        let populate = |key: *const c_char,
                        value: *mut u8,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(database.deref_mut(), key, value, length, error)
        };
        get_option_bytes(key, populate)
    }
    fn get_option_double(&self, key: Self::Key) -> Result<f64> {
        let driver = self.inner.driver.driver.lock().unwrap();
        let mut database = self.inner.database.lock().unwrap();
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: f64 = 0.0;
        let method = driver_method!(driver, DatabaseGetOptionDouble);
        let status = unsafe { method(database.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_int(&self, key: Self::Key) -> Result<i64> {
        let driver = self.inner.driver.driver.lock().unwrap();
        let mut database = self.inner.database.lock().unwrap();
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: i64 = 0;
        let method = driver_method!(driver, DatabaseGetOptionInt);
        let status = unsafe { method(database.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_string(&self, key: Self::Key) -> Result<String> {
        let driver = self.inner.driver.driver.lock().unwrap();
        let mut database = self.inner.database.lock().unwrap();
        let method = driver_method!(driver, DatabaseGetOption);
        let populate = |key: *const c_char,
                        value: *mut c_char,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(database.deref_mut(), key, value, length, error)
        };
        get_option_string(key, populate)
    }
    fn set_option(&self, key: Self::Key, value: OptionValue) -> Result<()> {
        let driver = self.inner.driver.driver.lock().unwrap();
        let mut database = self.inner.database.lock().unwrap();
        set_option_database(
            &driver,
            database.deref_mut(),
            self.inner.version,
            key,
            value,
        )
    }
}

impl Database for ManagedDatabase {
    type ConnectionType = ManagedConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        let opts: [(<Self::ConnectionType as Optionable>::Key, OptionValue); 0] = [];
        self.new_connection_with_opts(opts.into_iter())
    }

    fn new_connection_with_opts(
        &self,
        opts: impl Iterator<Item = (<Self::ConnectionType as Optionable>::Key, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let driver = self.inner.driver.driver.lock().unwrap();
        let mut database = self.inner.database.lock().unwrap();
        let mut connection = ffi::FFI_AdbcConnection::default();
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(driver, ConnectionNew);
        let status = unsafe { method(&mut connection, &mut error) };
        check_status(status, error)?;

        for (key, value) in opts {
            set_option_connection(&driver, &mut connection, self.inner.version, key, value)?;
        }

        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(driver, ConnectionInit);
        let status = unsafe { method(&mut connection, database.deref_mut(), &mut error) };
        check_status(status, error)?;

        let inner = ManagedConnectionInner {
            connection: RefCell::new(connection),
            version: self.inner.version,
            database: self.inner.clone(),
        };

        Ok(Self::ConnectionType {
            inner: Rc::new(inner),
        })
    }
}

fn set_option_connection(
    driver: &ffi::FFI_AdbcDriver,
    connection: &mut ffi::FFI_AdbcConnection,
    version: AdbcVersion,
    key: impl AsRef<str>,
    value: OptionValue,
) -> Result<()> {
    let key = CString::new(key.as_ref())?;
    let mut error = ffi::FFI_AdbcError::default();
    let status = match (version, value) {
        (_, OptionValue::String(value)) => {
            let value = CString::new(value)?;
            let method = driver_method!(driver, ConnectionSetOption);
            unsafe { method(connection, key.as_ptr(), value.as_ptr(), &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Bytes(value)) => {
            let method = driver_method!(driver, ConnectionSetOptionBytes);
            unsafe {
                method(
                    connection,
                    key.as_ptr(),
                    value.as_ptr(),
                    value.len(),
                    &mut error,
                )
            }
        }
        (AdbcVersion::V110, OptionValue::Int(value)) => {
            let method = driver_method!(driver, ConnectionSetOptionInt);
            unsafe { method(connection, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Double(value)) => {
            let method = driver_method!(driver, ConnectionSetOptionDouble);
            unsafe { method(connection, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V100, _) => Err(Error::with_message_and_status(
            ERR_ONLY_STRING_OPT,
            Status::NotImplemented,
        ))?,
    };
    check_status(status, error)
}

struct ManagedConnectionInner {
    connection: RefCell<ffi::FFI_AdbcConnection>,
    version: AdbcVersion,
    database: Arc<ManagedDatabaseInner>,
}

impl Drop for ManagedConnectionInner {
    fn drop(&mut self) {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionRelease);
        let status = unsafe { method(self.connection.borrow_mut().deref_mut(), &mut error) };
        if let Err(err) = check_status(status, error) {
            panic!("unable to drop connection: {:?}", err);
        }
    }
}

/// Implementation of [Connection].
pub struct ManagedConnection {
    inner: Rc<ManagedConnectionInner>,
}

impl Optionable for ManagedConnection {
    type Key = options::ConnectionOptionKey;
    fn get_option_bytes(&self, key: Self::Key) -> Result<Vec<u8>> {
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetOptionBytes);
        let populate = |key: *const c_char,
                        value: *mut u8,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(
                self.inner.connection.borrow_mut().deref_mut(),
                key,
                value,
                length,
                error,
            )
        };
        get_option_bytes(key, populate)
    }
    fn get_option_double(&self, key: Self::Key) -> Result<f64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: f64 = 0.0;
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetOptionDouble);
        let status = unsafe {
            method(
                self.inner.connection.borrow_mut().deref_mut(),
                key.as_ptr(),
                &mut value,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_int(&self, key: Self::Key) -> Result<i64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: i64 = 0;
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetOptionInt);
        let status = unsafe {
            method(
                self.inner.connection.borrow_mut().deref_mut(),
                key.as_ptr(),
                &mut value,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_string(&self, key: Self::Key) -> Result<String> {
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetOption);
        let populate = |key: *const c_char,
                        value: *mut c_char,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(
                self.inner.connection.borrow_mut().deref_mut(),
                key,
                value,
                length,
                error,
            )
        };
        get_option_string(key, populate)
    }
    fn set_option(&self, key: Self::Key, value: OptionValue) -> Result<()> {
        let driver = self.inner.database.driver.driver.lock().unwrap();
        set_option_connection(
            &driver,
            self.inner.connection.borrow_mut().deref_mut(),
            self.inner.version,
            key,
            value,
        )
    }
}

impl Connection for ManagedConnection {
    type StatementType = ManagedStatement;

    fn new_statement(&self) -> Result<Self::StatementType> {
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let mut statement = ffi::FFI_AdbcStatement::default();
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(driver, StatementNew);
        let status = unsafe {
            method(
                self.inner.connection.borrow_mut().deref_mut(),
                &mut statement,
                &mut error,
            )
        };
        check_status(status, error)?;

        Ok(Self::StatementType {
            statement: RefCell::new(statement),
            version: self.inner.version,
            connection: self.inner.clone(),
        })
    }

    fn cancel(&self) -> Result<()> {
        if let AdbcVersion::V100 = self.inner.version {
            return Err(Error::with_message_and_status(
                ERR_CANCEL_UNSUPPORTED,
                Status::NotImplemented,
            ));
        }
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionCancel);
        let status = unsafe { method(self.inner.connection.borrow_mut().deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn commit(&self) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionCommit);
        let status = unsafe { method(self.inner.connection.borrow_mut().deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn rollback(&self) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionRollback);
        let status = unsafe { method(self.inner.connection.borrow_mut().deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn get_info(
        &self,
        codes: Option<&[crate::options::InfoCode]>,
    ) -> Result<impl RecordBatchReader> {
        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let codes: Option<Vec<u32>> =
            codes.map(|codes| codes.iter().map(|code| code.into()).collect());
        let (codes_ptr, codes_len) = codes
            .as_ref()
            .map(|c| (c.as_ptr(), c.len()))
            .unwrap_or((null(), 0));
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetInfo);
        let status = unsafe {
            method(
                self.inner.connection.borrow_mut().deref_mut(),
                codes_ptr,
                codes_len,
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_objects(
        &self,
        depth: crate::options::ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&[&str]>,
        column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader> {
        let catalog = catalog.map(|c| CString::new(c)).transpose()?;
        let db_schema = db_schema.map(|c| CString::new(c)).transpose()?;
        let table_name = table_name.map(|c| CString::new(c)).transpose()?;
        let column_name = column_name.map(|c| CString::new(c)).transpose()?;
        let mut table_type = table_type
            .map(|t| {
                t.iter()
                    .map(|x| CString::new(*x))
                    .collect::<std::result::Result<Vec<CString>, _>>()
            })
            .transpose()?
            .map(|v| v.into_iter().map(|c| c.as_ptr()))
            .map(|c| c.collect::<Vec<_>>());

        let catalog_ptr = catalog.map(|c| c.as_ptr()).unwrap_or(null());
        let db_schema_ptr = db_schema.map(|c| c.as_ptr()).unwrap_or(null());
        let table_name_ptr = table_name.map(|c| c.as_ptr()).unwrap_or(null());
        let column_name_ptr = column_name.map(|c| c.as_ptr()).unwrap_or(null());
        let table_type_ptr = match &mut table_type {
            None => null(),
            Some(t) => {
                t.push(null());
                t.as_ptr()
            }
        };

        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetObjects);
        let mut stream = FFI_ArrowArrayStream::empty();

        let status = unsafe {
            method(
                self.inner.connection.borrow_mut().deref_mut(),
                depth.into(),
                catalog_ptr,
                db_schema_ptr,
                table_name_ptr,
                table_type_ptr,
                column_name_ptr,
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;

        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<impl RecordBatchReader> {
        if let AdbcVersion::V100 = self.inner.version {
            return Err(Error::with_message_and_status(
                ERR_STATISTICS_UNSUPPORTED,
                Status::NotImplemented,
            ));
        }

        let catalog = catalog.map(|c| CString::new(c)).transpose()?;
        let db_schema = db_schema.map(|c| CString::new(c)).transpose()?;
        let table_name = table_name.map(|c| CString::new(c)).transpose()?;

        let catalog_ptr = catalog.map(|c| c.as_ptr()).unwrap_or(null());
        let db_schema_ptr = db_schema.map(|c| c.as_ptr()).unwrap_or(null());
        let table_name_ptr = table_name.map(|c| c.as_ptr()).unwrap_or(null());

        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetStatistics);
        let status = unsafe {
            method(
                self.inner.connection.borrow_mut().deref_mut(),
                catalog_ptr,
                db_schema_ptr,
                table_name_ptr,
                approximate as std::os::raw::c_char,
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_statistics_name(&self) -> Result<impl RecordBatchReader> {
        if let AdbcVersion::V100 = self.inner.version {
            return Err(Error::with_message_and_status(
                ERR_STATISTICS_UNSUPPORTED,
                Status::NotImplemented,
            ));
        }
        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetStatisticNames);
        let status = unsafe {
            method(
                self.inner.connection.borrow_mut().deref_mut(),
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<arrow::datatypes::Schema> {
        let catalog = catalog.map(|c| CString::new(c)).transpose()?;
        let db_schema = db_schema.map(|c| CString::new(c)).transpose()?;
        let table_name = CString::new(table_name)?;

        let catalog_ptr = catalog.map(|c| c.as_ptr()).unwrap_or(null());
        let db_schema_ptr = db_schema.map(|c| c.as_ptr()).unwrap_or(null());
        let table_name_ptr = table_name.as_ptr();

        let mut error = ffi::FFI_AdbcError::default();
        let mut schema = FFI_ArrowSchema::empty();
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetTableSchema);
        let status = unsafe {
            method(
                self.inner.connection.borrow_mut().deref_mut(),
                catalog_ptr,
                db_schema_ptr,
                table_name_ptr,
                &mut schema,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader> {
        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetTableTypes);
        let status = unsafe {
            method(
                self.inner.connection.borrow_mut().deref_mut(),
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn read_partition(&self, partition: &[u8]) -> Result<impl RecordBatchReader> {
        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let driver = self.inner.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionReadPartition);
        let status = unsafe {
            method(
                self.inner.connection.borrow_mut().deref_mut(),
                partition.as_ptr(),
                partition.len(),
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }
}

fn set_option_statement(
    driver: &ffi::FFI_AdbcDriver,
    statement: &mut ffi::FFI_AdbcStatement,
    version: AdbcVersion,
    key: impl AsRef<str>,
    value: OptionValue,
) -> Result<()> {
    let key = CString::new(key.as_ref())?;
    let mut error = ffi::FFI_AdbcError::default();
    let status = match (version, value) {
        (_, OptionValue::String(value)) => {
            let value = CString::new(value)?;
            let method = driver_method!(driver, StatementSetOption);
            unsafe { method(statement, key.as_ptr(), value.as_ptr(), &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Bytes(value)) => {
            let method = driver_method!(driver, StatementSetOptionBytes);
            unsafe {
                method(
                    statement,
                    key.as_ptr(),
                    value.as_ptr(),
                    value.len(),
                    &mut error,
                )
            }
        }
        (AdbcVersion::V110, OptionValue::Int(value)) => {
            let method = driver_method!(driver, StatementSetOptionInt);
            unsafe { method(statement, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Double(value)) => {
            let method = driver_method!(driver, StatementSetOptionDouble);
            unsafe { method(statement, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V100, _) => Err(Error::with_message_and_status(
            ERR_ONLY_STRING_OPT,
            Status::NotImplemented,
        ))?,
    };
    check_status(status, error)
}

/// Implementation of [Statement].
pub struct ManagedStatement {
    statement: RefCell<ffi::FFI_AdbcStatement>,
    version: AdbcVersion,
    connection: Rc<ManagedConnectionInner>,
}

impl Statement for ManagedStatement {
    fn bind(&self, batch: RecordBatch) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementBind);
        let batch: StructArray = batch.into();
        let (mut array, mut schema) = to_ffi(&batch.to_data())?;
        let status = unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                &mut array,
                &mut schema,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(())
    }

    fn bind_stream(&self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementBindStream);
        let mut stream = FFI_ArrowArrayStream::new(reader);
        let status = unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(())
    }

    fn cancel(&self) -> Result<()> {
        if let AdbcVersion::V100 = self.version {
            return Err(Error::with_message_and_status(
                ERR_CANCEL_UNSUPPORTED,
                Status::NotImplemented,
            ));
        }
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementCancel);
        let status = unsafe { method(self.statement.borrow_mut().deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn execute(&self) -> Result<impl RecordBatchReader> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementExecuteQuery);
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                &mut stream,
                null_mut(),
                &mut error,
            )
        };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn execute_schema(&self) -> Result<arrow::datatypes::Schema> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementExecuteSchema);
        let mut schema = FFI_ArrowSchema::empty();
        let status = unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                &mut schema,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn execute_update(&self) -> Result<i64> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementExecuteQuery);
        let mut rows_affected: i64 = -1;
        let status = unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                null_mut(),
                &mut rows_affected,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(rows_affected)
    }

    fn execute_partitions(&self) -> Result<crate::Partitions> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementExecutePartitions);
        let mut schema = FFI_ArrowSchema::empty();
        let mut partitions = ffi::FFI_AdbcPartitions::default();
        let mut rows_affected: i64 = -1;
        let status = unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                &mut schema,
                &mut partitions,
                &mut rows_affected,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(partitions.into())
    }

    fn get_parameters_schema(&self) -> Result<arrow::datatypes::Schema> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementGetParameterSchema);
        let mut schema = FFI_ArrowSchema::empty();
        let status = unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                &mut schema,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn prepare(&self) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementPrepare);
        let status = unsafe { method(self.statement.borrow_mut().deref_mut(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_sql_query(&self, query: &str) -> Result<()> {
        let query = CString::new(query)?;
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementSetSqlQuery);
        let status = unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                query.as_ptr(),
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(())
    }

    fn set_substrait_plan(&self, plan: &[u8]) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementSetSubstraitPlan);
        let status = unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                plan.as_ptr(),
                plan.len(),
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(())
    }
}

impl Optionable for ManagedStatement {
    type Key = options::StatementOptionKey;
    fn get_option_bytes(&self, key: Self::Key) -> Result<Vec<u8>> {
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementGetOptionBytes);
        let populate = |key: *const c_char,
                        value: *mut u8,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                key,
                value,
                length,
                error,
            )
        };
        get_option_bytes(key, populate)
    }
    fn get_option_double(&self, key: Self::Key) -> Result<f64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: f64 = 0.0;
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementGetOptionDouble);
        let status = unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                key.as_ptr(),
                &mut value,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_int(&self, key: Self::Key) -> Result<i64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: i64 = 0;
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementGetOptionInt);
        let status = unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                key.as_ptr(),
                &mut value,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_string(&self, key: Self::Key) -> Result<String> {
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementGetOption);
        let populate = |key: *const c_char,
                        value: *mut c_char,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(
                self.statement.borrow_mut().deref_mut(),
                key,
                value,
                length,
                error,
            )
        };
        get_option_string(key, populate)
    }
    fn set_option(&self, key: Self::Key, value: OptionValue) -> Result<()> {
        let driver = self.connection.database.driver.driver.lock().unwrap();
        set_option_statement(
            &driver,
            self.statement.borrow_mut().deref_mut(),
            self.version,
            key,
            value,
        )
    }
}

impl Drop for ManagedStatement {
    fn drop(&mut self) {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementRelease);
        let status = unsafe { method(self.statement.borrow_mut().deref_mut(), &mut error) };
        if let Err(err) = check_status(status, error) {
            panic!("unable to drop statement: {:?}", err);
        }
    }
}
