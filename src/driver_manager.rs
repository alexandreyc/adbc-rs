use std::ffi::{CStr, CString};
use std::ops::{Deref, DerefMut};
use std::os::raw::{c_char, c_void};
use std::ptr::{null, null_mut};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, RecordBatch, RecordBatchReader, StructArray};
use arrow::ffi::{to_ffi, FFI_ArrowSchema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use crate::{driver_method, ffi, Optionable};
use crate::{
    error::Status,
    options::{self, AdbcVersion, OptionValue},
    Error, Result,
};
use crate::{Connection, Database, Driver, Statement};

const ERR_ONLY_STRING_OPT: &str = "Only string option value are supported with ADBC 1.0.0";
const ERR_CANCEL_UNSUPPORTED: &str =
    "Canceling connection or statement is not supported with ADBC 1.0.0";
const ERR_STATISTICS_UNSUPPORTED: &str = "Statistics are not supported with ADBC 1.0.0";

pub fn check_status(status: ffi::FFI_AdbcStatusCode, error: ffi::FFI_AdbcError) -> Result<()> {
    match status {
        ffi::constants::ADBC_STATUS_OK => Ok(()),
        _ => {
            let mut error: Error = error.into();
            error.status = Some(status.into());
            Err(error)
        }
    }
}

/// If applicable, keeps the loaded dynamic library in scope as long as the
/// FFI_AdbcDriver so that all it's function pointers remain valid.
pub struct DriverManager {
    driver: Arc<Mutex<ffi::FFI_AdbcDriver>>,
    version: AdbcVersion, // Driver version
    _library: Option<libloading::Library>,
}
impl DriverManager {
    pub fn load_static(init: &ffi::FFI_AdbcDriverInitFunc, version: AdbcVersion) -> Result<Self> {
        let driver = Self::load_impl(init, version)?;
        Ok(DriverManager {
            driver: Arc::new(Mutex::new(driver)),
            version,
            _library: None,
        })
    }

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
        Ok(DriverManager {
            driver: Arc::new(Mutex::new(driver)),
            version,
            _library: Some(library),
        })
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
    type DatabaseType<'driver> = ManagedDatabase<'driver>;

    fn new_database(&self) -> Result<Self::DatabaseType<'_>> {
        let opts: [(<Self::DatabaseType<'_> as Optionable>::Key, OptionValue); 0] = [];
        self.new_database_with_opts(opts.into_iter())
    }

    fn new_database_with_opts<'a>(
        &self,
        opts: impl Iterator<Item = (<Self::DatabaseType<'a> as Optionable>::Key, OptionValue)>,
    ) -> Result<Self::DatabaseType<'_>> {
        let mut database = ffi::FFI_AdbcDatabase::default();

        // DatabaseNew
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.driver.lock().unwrap();
        let method = driver_method!(driver, DatabaseNew);
        let status = unsafe { method(&mut database, &mut error) };
        check_status(status, error)?;

        // DatabaseSetOption
        for (key, value) in opts {
            set_option_database(driver.deref(), &mut database, self.version, key, value)?;
        }

        // DatabaseInit
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(driver, DatabaseInit);
        let status = unsafe { method(&mut database, &mut error) };
        check_status(status, error)?;

        Ok(Self::DatabaseType {
            database: Arc::new(Mutex::new(database)),
            version: self.version,
            driver: self,
        })
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

pub struct ManagedDatabase<'driver> {
    database: Arc<Mutex<ffi::FFI_AdbcDatabase>>,
    driver: &'driver DriverManager,
    version: AdbcVersion,
}
impl<'driver> Optionable for ManagedDatabase<'driver> {
    type Key = options::DatabaseOptionKey;
    fn get_option_bytes(&mut self, key: Self::Key) -> Result<Vec<u8>> {
        let driver = self.driver.driver.lock().unwrap();
        let method = driver_method!(driver, DatabaseGetOptionBytes);
        let mut database = self.database.lock().unwrap();
        let populate = |key: *const c_char,
                        value: *mut u8,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(database.deref_mut(), key, value, length, error)
        };
        get_option_bytes(key, populate)
    }
    fn get_option_double(&mut self, key: Self::Key) -> Result<f64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: f64 = 0.0;
        let driver = self.driver.driver.lock().unwrap();
        let method = driver_method!(driver, DatabaseGetOptionDouble);
        let mut database = self.database.lock().unwrap();
        let status = unsafe { method(database.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_int(&mut self, key: Self::Key) -> Result<i64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: i64 = 0;
        let driver = self.driver.driver.lock().unwrap();
        let method = driver_method!(driver, DatabaseGetOptionInt);
        let mut database = self.database.lock().unwrap();
        let status = unsafe { method(database.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_string(&mut self, key: Self::Key) -> Result<String> {
        let driver = self.driver.driver.lock().unwrap();
        let method = driver_method!(driver, DatabaseGetOption);
        let mut database = self.database.lock().unwrap();
        let populate = |key: *const c_char,
                        value: *mut c_char,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(database.deref_mut(), key, value, length, error)
        };
        get_option_string(key, populate)
    }
    fn set_option(&mut self, key: Self::Key, value: OptionValue) -> Result<()> {
        let driver = self.driver.driver.lock().unwrap();
        let mut database = self.database.lock().unwrap();
        set_option_database(
            driver.deref(),
            database.deref_mut(),
            self.version,
            key,
            value,
        )
    }
}
impl<'driver> Database for ManagedDatabase<'driver> {
    type ConnectionType<'database> = ManagedConnection<'driver, 'database> where Self: 'database;

    fn new_connection(&mut self) -> Result<Self::ConnectionType<'_>> {
        let opts: [(<Self::ConnectionType<'_> as Optionable>::Key, OptionValue); 0] = [];
        self.new_connection_with_opts(opts.into_iter())
    }

    fn new_connection_with_opts<'a>(
        &mut self,
        opts: impl Iterator<Item = (<Self::ConnectionType<'a> as Optionable>::Key, OptionValue)>,
    ) -> Result<Self::ConnectionType<'_>>
    where
        Self: 'a,
    {
        let mut connection = ffi::FFI_AdbcConnection::default();
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionNew);
        let status = unsafe { method(&mut connection, &mut error) };
        check_status(status, error)?;

        for (key, value) in opts {
            set_option_connection(driver.deref(), &mut connection, self.version, key, value)?;
        }

        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(driver, ConnectionInit);
        let mut database = self.database.lock().unwrap();
        let status = unsafe { method(&mut connection, database.deref_mut(), &mut error) };
        check_status(status, error)?;

        Ok(Self::ConnectionType {
            version: self.version,
            connection: Arc::new(Mutex::new(connection)),
            database: self,
        })
    }
}
impl<'driver> Drop for ManagedDatabase<'driver> {
    fn drop(&mut self) {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.driver.driver.lock().unwrap();
        let method = driver_method!(driver, DatabaseRelease);
        let mut database = self.database.lock().unwrap();
        let status = unsafe { method(database.deref_mut(), &mut error) };
        if let Err(err) = check_status(status, error) {
            panic!("unable to drop database: {:?}", err);
        }
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

pub struct ManagedConnection<'driver, 'database> {
    connection: Arc<Mutex<ffi::FFI_AdbcConnection>>,
    version: AdbcVersion,
    database: &'database ManagedDatabase<'driver>,
}
impl<'driver, 'database> Optionable for ManagedConnection<'driver, 'database> {
    type Key = options::ConnectionOptionKey;
    fn get_option_bytes(&mut self, key: Self::Key) -> Result<Vec<u8>> {
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetOptionBytes);
        let mut connection = self.connection.lock().unwrap();
        let populate = |key: *const c_char,
                        value: *mut u8,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(connection.deref_mut(), key, value, length, error)
        };
        get_option_bytes(key, populate)
    }
    fn get_option_double(&mut self, key: Self::Key) -> Result<f64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: f64 = 0.0;
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetOptionDouble);
        let mut connection = self.connection.lock().unwrap();
        let status =
            unsafe { method(connection.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_int(&mut self, key: Self::Key) -> Result<i64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: i64 = 0;
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetOptionInt);
        let mut connection = self.connection.lock().unwrap();
        let status =
            unsafe { method(connection.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_string(&mut self, key: Self::Key) -> Result<String> {
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetOption);
        let mut connection = self.connection.lock().unwrap();
        let populate = |key: *const c_char,
                        value: *mut c_char,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(connection.deref_mut(), key, value, length, error)
        };
        get_option_string(key, populate)
    }
    fn set_option(&mut self, key: Self::Key, value: OptionValue) -> Result<()> {
        let driver = self.database.driver.driver.lock().unwrap();
        let mut connection = self.connection.lock().unwrap();
        set_option_connection(
            driver.deref(),
            connection.deref_mut(),
            self.version,
            key,
            value,
        )
    }
}
impl<'driver, 'database> Connection for ManagedConnection<'driver, 'database> {
    type StatementType<'connection> = ManagedStatement<'driver, 'database, 'connection> where Self: 'connection;

    fn new_statement(&mut self) -> Result<Self::StatementType<'_>> {
        let mut statement = ffi::FFI_AdbcStatement::default();
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementNew);
        let mut connection = self.connection.lock().unwrap();
        let status = unsafe { method(connection.deref_mut(), &mut statement, &mut error) };
        check_status(status, error)?;

        Ok(Self::StatementType {
            statement: Arc::new(Mutex::new(statement)),
            version: self.version,
            connection: self,
        })
    }

    fn cancel(&mut self) -> Result<()> {
        if let AdbcVersion::V100 = self.version {
            return Err(Error::with_message_and_status(
                ERR_CANCEL_UNSUPPORTED,
                Status::NotImplemented,
            ));
        }
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionCancel);
        let mut connection = self.connection.lock().unwrap();
        let status = unsafe { method(connection.deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn commit(&mut self) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionCommit);
        let mut connection = self.connection.lock().unwrap();
        let status = unsafe { method(connection.deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn rollback(&mut self) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionRollback);
        let mut connection = self.connection.lock().unwrap();
        let status = unsafe { method(connection.deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn get_info(
        &mut self,
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
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetInfo);
        let mut connection = self.connection.lock().unwrap();
        let status = unsafe {
            method(
                connection.deref_mut(),
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
        &mut self,
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
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetObjects);
        let mut stream = FFI_ArrowArrayStream::empty();

        let mut connection = self.connection.lock().unwrap();
        let status = unsafe {
            method(
                connection.deref_mut(),
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
        &mut self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<impl RecordBatchReader> {
        if let AdbcVersion::V100 = self.version {
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
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetStatistics);
        let mut connection = self.connection.lock().unwrap();
        let status = unsafe {
            method(
                connection.deref_mut(),
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

    fn get_statistics_name(&mut self) -> Result<impl RecordBatchReader> {
        if let AdbcVersion::V100 = self.version {
            return Err(Error::with_message_and_status(
                ERR_STATISTICS_UNSUPPORTED,
                Status::NotImplemented,
            ));
        }
        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetStatisticNames);
        let mut connection = self.connection.lock().unwrap();
        let status = unsafe { method(connection.deref_mut(), &mut stream, &mut error) };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_table_schema(
        &mut self,
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
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetTableSchema);
        let mut connection = self.connection.lock().unwrap();
        let status = unsafe {
            method(
                connection.deref_mut(),
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

    fn get_table_types(&mut self) -> Result<impl RecordBatchReader> {
        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetTableTypes);
        let mut connection = self.connection.lock().unwrap();
        let status = unsafe { method(connection.deref_mut(), &mut stream, &mut error) };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn read_partition(&mut self, partition: &[u8]) -> Result<impl RecordBatchReader> {
        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionReadPartition);
        let mut connection = self.connection.lock().unwrap();
        let status = unsafe {
            method(
                connection.deref_mut(),
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
impl<'driver, 'database> Drop for ManagedConnection<'driver, 'database> {
    fn drop(&mut self) {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, ConnectionRelease);
        let mut connection = self.connection.lock().unwrap();
        let status = unsafe { method(connection.deref_mut(), &mut error) };
        if let Err(err) = check_status(status, error) {
            panic!("unable to drop connection: {:?}", err);
        }
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

pub struct ManagedStatement<'driver, 'database, 'connection> {
    statement: Arc<Mutex<ffi::FFI_AdbcStatement>>,
    version: AdbcVersion,
    connection: &'connection ManagedConnection<'driver, 'database>,
}
impl<'driver, 'database, 'connection> Statement
    for ManagedStatement<'driver, 'database, 'connection>
{
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementBind);
        let batch: StructArray = batch.into();
        let (mut array, mut schema) = to_ffi(&batch.to_data())?;
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe { method(statement.deref_mut(), &mut array, &mut schema, &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementBindStream);
        let mut stream = FFI_ArrowArrayStream::new(reader);
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe { method(statement.deref_mut(), &mut stream, &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn cancel(&mut self) -> Result<()> {
        if let AdbcVersion::V100 = self.version {
            return Err(Error::with_message_and_status(
                ERR_CANCEL_UNSUPPORTED,
                Status::NotImplemented,
            ));
        }
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementCancel);
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe { method(statement.deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementExecuteQuery);
        let mut stream = FFI_ArrowArrayStream::empty();
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe { method(statement.deref_mut(), &mut stream, null_mut(), &mut error) };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn execute_schema(&mut self) -> Result<arrow::datatypes::Schema> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementExecuteSchema);
        let mut schema = FFI_ArrowSchema::empty();
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe { method(statement.deref_mut(), &mut schema, &mut error) };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn execute_update(&mut self) -> Result<i64> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementExecuteQuery);
        let mut rows_affected: i64 = -1;
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe {
            method(
                statement.deref_mut(),
                null_mut(),
                &mut rows_affected,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(rows_affected)
    }

    fn execute_partitions(&mut self) -> Result<crate::Partitions> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementExecutePartitions);
        let mut schema = FFI_ArrowSchema::empty();
        let mut partitions = ffi::FFI_AdbcPartitions::default();
        let mut rows_affected: i64 = -1;
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe {
            method(
                statement.deref_mut(),
                &mut schema,
                &mut partitions,
                &mut rows_affected,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(partitions.into())
    }

    fn get_parameters_schema(&mut self) -> Result<arrow::datatypes::Schema> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementGetParameterSchema);
        let mut schema = FFI_ArrowSchema::empty();
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe { method(statement.deref_mut(), &mut schema, &mut error) };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn prepare(&mut self) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementPrepare);
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe { method(statement.deref_mut(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_sql_query(&mut self, query: &str) -> Result<()> {
        let query = CString::new(query)?;
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementSetSqlQuery);
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe { method(statement.deref_mut(), query.as_ptr(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: &[u8]) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementSetSubstraitPlan);
        let mut statement = self.statement.lock().unwrap();
        let status =
            unsafe { method(statement.deref_mut(), plan.as_ptr(), plan.len(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }
}
impl<'driver, 'database, 'connection> Optionable
    for ManagedStatement<'driver, 'database, 'connection>
{
    type Key = options::StatementOptionKey;
    fn get_option_bytes(&mut self, key: Self::Key) -> Result<Vec<u8>> {
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementGetOptionBytes);
        let mut statement = self.statement.lock().unwrap();
        let populate = |key: *const c_char,
                        value: *mut u8,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(statement.deref_mut(), key, value, length, error)
        };
        get_option_bytes(key, populate)
    }
    fn get_option_double(&mut self, key: Self::Key) -> Result<f64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: f64 = 0.0;
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementGetOptionDouble);
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe { method(statement.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_int(&mut self, key: Self::Key) -> Result<i64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: i64 = 0;
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementGetOptionInt);
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe { method(statement.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_string(&mut self, key: Self::Key) -> Result<String> {
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementGetOption);
        let mut statement = self.statement.lock().unwrap();
        let populate = |key: *const c_char,
                        value: *mut c_char,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(statement.deref_mut(), key, value, length, error)
        };
        get_option_string(key, populate)
    }
    fn set_option(&mut self, key: Self::Key, value: OptionValue) -> Result<()> {
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let mut statement = self.statement.lock().unwrap();
        set_option_statement(
            driver.deref(),
            statement.deref_mut(),
            self.version,
            key,
            value,
        )
    }
}
impl<'driver, 'database, 'connection> Drop for ManagedStatement<'driver, 'database, 'connection> {
    fn drop(&mut self) {
        let mut error = ffi::FFI_AdbcError::default();
        let driver = self.connection.database.driver.driver.lock().unwrap();
        let method = driver_method!(driver, StatementRelease);
        let mut statement = self.statement.lock().unwrap();
        let status = unsafe { method(statement.deref_mut(), &mut error) };
        if let Err(err) = check_status(status, error) {
            panic!("unable to drop statement: {:?}", err);
        }
    }
}
