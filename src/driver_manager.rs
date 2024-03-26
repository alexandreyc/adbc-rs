use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::os::raw::{c_char, c_void};
use std::ptr::{null, null_mut};

use arrow::array::{Array, RecordBatch, RecordBatchReader, StructArray};
use arrow::ffi::{to_ffi, FFI_ArrowSchema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use crate::ffi::{FFI_AdbcDriver, FFI_AdbcError, FFI_AdbcPartitions, FFI_AdbcStatusCode};
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
    driver: ffi::FFI_AdbcDriver,
    version: AdbcVersion, // Driver version
    _library: Option<libloading::Library>,
}
impl DriverManager {
    pub fn load_static(init: &ffi::FFI_AdbcDriverInitFunc, version: AdbcVersion) -> Result<Self> {
        let driver = Self::load_impl(init, version)?;
        Ok(DriverManager {
            driver,
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
            driver,
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
        let database_new = driver_method!(&self.driver as *const FFI_AdbcDriver, DatabaseNew);
        let status = unsafe { database_new(&mut database, &mut error) };
        check_status(status, error)?;
        database.private_driver = &self.driver;

        // DatabaseSetOption
        for (key, value) in opts {
            set_option_database(&mut database, self.version, key, value)?;
        }

        // DatabaseInit
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(&self.driver as *const FFI_AdbcDriver, DatabaseInit);
        let status = unsafe { method(&mut database, &mut error) };
        check_status(status, error)?;

        Ok(Self::DatabaseType {
            database,
            version: self.version,
            _phantom: PhantomData,
        })
    }
}

fn set_option_database(
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
            let method = driver_method!(database.private_driver, DatabaseSetOption);
            unsafe { method(database, key.as_ptr(), value.as_ptr(), &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Bytes(value)) => {
            let method = driver_method!(database.private_driver, DatabaseSetOptionBytes);
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
            let method = driver_method!(database.private_driver, DatabaseSetOptionInt);
            unsafe { method(database, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Double(value)) => {
            let method = driver_method!(database.private_driver, DatabaseSetOptionDouble);
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
    F: FnMut(*const c_char, *mut u8, *mut usize, *mut FFI_AdbcError) -> FFI_AdbcStatusCode,
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
    F: FnMut(*const c_char, *mut c_char, *mut usize, *mut FFI_AdbcError) -> FFI_AdbcStatusCode,
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
    database: ffi::FFI_AdbcDatabase,
    version: AdbcVersion,
    _phantom: PhantomData<&'driver DriverManager>,
}
impl<'driver> Optionable for ManagedDatabase<'driver> {
    type Key = options::DatabaseOptionKey;
    fn get_option_bytes(&mut self, key: Self::Key) -> Result<Vec<u8>> {
        let method = driver_method!(self.database.private_driver, DatabaseGetOptionBytes);
        let populate = |key: *const c_char,
                        value: *mut u8,
                        length: *mut usize,
                        error: *mut FFI_AdbcError| unsafe {
            method(&mut self.database, key, value, length, error)
        };
        get_option_bytes(key, populate)
    }
    fn get_option_double(&mut self, key: Self::Key) -> Result<f64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: f64 = 0.0;
        let method = driver_method!(self.database.private_driver, DatabaseGetOptionDouble);
        let status = unsafe { method(&mut self.database, key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_int(&mut self, key: Self::Key) -> Result<i64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: i64 = 0;
        let method = driver_method!(self.database.private_driver, DatabaseGetOptionInt);
        let status = unsafe { method(&mut self.database, key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_string(&mut self, key: Self::Key) -> Result<String> {
        let method = driver_method!(self.database.private_driver, DatabaseGetOption);
        let populate = |key: *const c_char,
                        value: *mut c_char,
                        length: *mut usize,
                        error: *mut FFI_AdbcError| unsafe {
            method(&mut self.database, key, value, length, error)
        };
        get_option_string(key, populate)
    }
    fn set_option(&mut self, key: Self::Key, value: OptionValue) -> Result<()> {
        set_option_database(&mut self.database, self.version, key, value)
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
        let connection_new = driver_method!(self.database.private_driver, ConnectionNew);
        let status = unsafe { connection_new(&mut connection, &mut error) };
        check_status(status, error)?;
        connection.private_driver = self.database.private_driver;

        for (key, value) in opts {
            set_option_connection(&mut connection, self.version, key, value)?;
        }

        let mut error = ffi::FFI_AdbcError::default();
        let connection_init = driver_method!(self.database.private_driver, ConnectionInit);
        let status = unsafe { connection_init(&mut connection, &mut self.database, &mut error) };
        check_status(status, error)?;

        Ok(Self::ConnectionType {
            version: self.version,
            connection,
            _phantom: PhantomData,
        })
    }
}
impl<'driver> Drop for ManagedDatabase<'driver> {
    fn drop(&mut self) {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.database.private_driver, DatabaseRelease);
        let status = unsafe { method(&mut self.database, &mut error) };
        if let Err(err) = check_status(status, error) {
            panic!("unable to drop database: {:?}", err);
        }
    }
}

fn set_option_connection(
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
            let method = driver_method!(connection.private_driver, ConnectionSetOption);
            unsafe { method(connection, key.as_ptr(), value.as_ptr(), &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Bytes(value)) => {
            let method = driver_method!(connection.private_driver, ConnectionSetOptionBytes);
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
            let method = driver_method!(connection.private_driver, ConnectionSetOptionInt);
            unsafe { method(connection, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Double(value)) => {
            let method = driver_method!(connection.private_driver, ConnectionSetOptionDouble);
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
    connection: ffi::FFI_AdbcConnection,
    version: AdbcVersion,
    _phantom: PhantomData<&'database ManagedDatabase<'driver>>,
}
impl<'driver, 'database> Optionable for ManagedConnection<'driver, 'database> {
    type Key = options::ConnectionOptionKey;
    fn get_option_bytes(&mut self, key: Self::Key) -> Result<Vec<u8>> {
        let method = driver_method!(self.connection.private_driver, ConnectionGetOptionBytes);
        let populate = |key: *const c_char,
                        value: *mut u8,
                        length: *mut usize,
                        error: *mut FFI_AdbcError| unsafe {
            method(&mut self.connection, key, value, length, error)
        };
        get_option_bytes(key, populate)
    }
    fn get_option_double(&mut self, key: Self::Key) -> Result<f64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: f64 = 0.0;
        let method = driver_method!(self.connection.private_driver, ConnectionGetOptionDouble);
        let status = unsafe { method(&mut self.connection, key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_int(&mut self, key: Self::Key) -> Result<i64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: i64 = 0;
        let method = driver_method!(self.connection.private_driver, ConnectionGetOptionInt);
        let status = unsafe { method(&mut self.connection, key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_string(&mut self, key: Self::Key) -> Result<String> {
        let method = driver_method!(self.connection.private_driver, ConnectionGetOption);
        let populate = |key: *const c_char,
                        value: *mut c_char,
                        length: *mut usize,
                        error: *mut FFI_AdbcError| unsafe {
            method(&mut self.connection, key, value, length, error)
        };
        get_option_string(key, populate)
    }
    fn set_option(&mut self, key: Self::Key, value: OptionValue) -> Result<()> {
        set_option_connection(&mut self.connection, self.version, key, value)
    }
}
impl<'driver, 'database> Connection for ManagedConnection<'driver, 'database> {
    type StatementType<'connection> = ManagedStatement<'driver, 'database, 'connection> where Self: 'connection;

    fn new_statement(&mut self) -> Result<Self::StatementType<'_>> {
        let mut statement = ffi::FFI_AdbcStatement::default();
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.connection.private_driver, StatementNew);
        let status = unsafe { method(&mut self.connection, &mut statement, &mut error) };
        check_status(status, error)?;
        statement.private_driver = self.connection.private_driver;

        Ok(Self::StatementType {
            statement,
            version: self.version,
            _phantom: PhantomData,
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
        let method = driver_method!(self.connection.private_driver, ConnectionCancel);
        let status = unsafe { method(&mut self.connection, &mut error) };
        check_status(status, error)
    }

    fn commit(&mut self) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.connection.private_driver, ConnectionCommit);
        let status = unsafe { method(&mut self.connection, &mut error) };
        check_status(status, error)
    }

    fn rollback(&mut self) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.connection.private_driver, ConnectionRollback);
        let status = unsafe { method(&mut self.connection, &mut error) };
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
        let method = driver_method!(self.connection.private_driver, ConnectionGetInfo);
        let status = unsafe {
            method(
                &mut self.connection,
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
        let method = driver_method!(self.connection.private_driver, ConnectionGetObjects);
        let mut stream = FFI_ArrowArrayStream::empty();

        let status = unsafe {
            method(
                &mut self.connection,
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

        let catalog = catalog
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());
        let db_schema = db_schema
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());
        let table_name = table_name
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());

        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let method = driver_method!(self.connection.private_driver, ConnectionGetStatistics);
        let status = unsafe {
            method(
                &mut self.connection,
                catalog,
                db_schema,
                table_name,
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
        let method = driver_method!(self.connection.private_driver, ConnectionGetStatisticNames);
        let status = unsafe { method(&mut self.connection, &mut stream, &mut error) };
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
        let catalog = catalog
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());
        let db_schema = db_schema
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());
        let table_name = CString::new(table_name)?;

        let mut error = ffi::FFI_AdbcError::default();
        let mut schema = FFI_ArrowSchema::empty();
        let method = driver_method!(self.connection.private_driver, ConnectionGetTableSchema);
        let status = unsafe {
            method(
                &mut self.connection,
                catalog,
                db_schema,
                table_name.as_ptr(),
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
        let method = driver_method!(self.connection.private_driver, ConnectionGetTableTypes);
        let status = unsafe { method(&mut self.connection, &mut stream, &mut error) };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn read_partition(&mut self, partition: &[u8]) -> Result<impl RecordBatchReader> {
        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let method = driver_method!(self.connection.private_driver, ConnectionReadPartition);
        let status = unsafe {
            method(
                &mut self.connection,
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
        let connection_release = driver_method!(self.connection.private_driver, ConnectionRelease);
        let status = unsafe { connection_release(&mut self.connection, &mut error) };
        if let Err(err) = check_status(status, error) {
            panic!("unable to drop connection: {:?}", err);
        }
    }
}

fn set_option_statement(
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
            let method = driver_method!(statement.private_driver, StatementSetOption);
            unsafe { method(statement, key.as_ptr(), value.as_ptr(), &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Bytes(value)) => {
            let method = driver_method!(statement.private_driver, StatementSetOptionBytes);
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
            let method = driver_method!(statement.private_driver, StatementSetOptionInt);
            unsafe { method(statement, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Double(value)) => {
            let method = driver_method!(statement.private_driver, StatementSetOptionDouble);
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
    statement: ffi::FFI_AdbcStatement,
    version: AdbcVersion,
    _phantom: PhantomData<&'connection ManagedConnection<'driver, 'database>>,
}
impl<'driver, 'database, 'connection> Statement
    for ManagedStatement<'driver, 'database, 'connection>
{
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.statement.private_driver, StatementBind);
        let batch: StructArray = batch.into();
        let (mut array, mut schema) = to_ffi(&batch.to_data())?;
        let status = unsafe { method(&mut self.statement, &mut array, &mut schema, &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.statement.private_driver, StatementBindStream);
        let mut stream = FFI_ArrowArrayStream::new(reader);
        let status = unsafe { method(&mut self.statement, &mut stream, &mut error) };
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
        let method = driver_method!(self.statement.private_driver, StatementCancel);
        let status = unsafe { method(&mut self.statement, &mut error) };
        check_status(status, error)
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.statement.private_driver, StatementExecuteQuery);
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = unsafe { method(&mut self.statement, &mut stream, null_mut(), &mut error) };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn execute_schema(&mut self) -> Result<arrow::datatypes::Schema> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.statement.private_driver, StatementExecuteSchema);
        let mut schema = FFI_ArrowSchema::empty();
        let status = unsafe { method(&mut self.statement, &mut schema, &mut error) };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn execute_update(&mut self) -> Result<i64> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.statement.private_driver, StatementExecuteQuery);
        let mut rows_affected: i64 = -1;
        let status = unsafe {
            method(
                &mut self.statement,
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
        let method = driver_method!(self.statement.private_driver, StatementExecutePartitions);
        let mut schema = FFI_ArrowSchema::empty();
        let mut partitions = FFI_AdbcPartitions::default();
        let mut rows_affected: i64 = -1;
        let status = unsafe {
            method(
                &mut self.statement,
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
        let method = driver_method!(self.statement.private_driver, StatementGetParameterSchema);
        let mut schema = FFI_ArrowSchema::empty();
        let status = unsafe { method(&mut self.statement, &mut schema, &mut error) };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn prepare(&mut self) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.statement.private_driver, StatementPrepare);
        let status = unsafe { method(&mut self.statement, &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_sql_query(&mut self, query: &str) -> Result<()> {
        let query = CString::new(query)?;
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.statement.private_driver, StatementSetSqlQuery);
        let status = unsafe { method(&mut self.statement, query.as_ptr(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: &[u8]) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.statement.private_driver, StatementSetSubstraitPlan);
        let status = unsafe { method(&mut self.statement, plan.as_ptr(), plan.len(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }
}
impl<'driver, 'database, 'connection> Optionable
    for ManagedStatement<'driver, 'database, 'connection>
{
    type Key = options::StatementOptionKey;
    fn get_option_bytes(&mut self, key: Self::Key) -> Result<Vec<u8>> {
        let method = driver_method!(self.statement.private_driver, StatementGetOptionBytes);
        let populate = |key: *const c_char,
                        value: *mut u8,
                        length: *mut usize,
                        error: *mut FFI_AdbcError| unsafe {
            method(&mut self.statement, key, value, length, error)
        };
        get_option_bytes(key, populate)
    }
    fn get_option_double(&mut self, key: Self::Key) -> Result<f64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: f64 = 0.0;
        let method = driver_method!(self.statement.private_driver, StatementGetOptionDouble);
        let status = unsafe { method(&mut self.statement, key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_int(&mut self, key: Self::Key) -> Result<i64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: i64 = 0;
        let method = driver_method!(self.statement.private_driver, StatementGetOptionInt);
        let status = unsafe { method(&mut self.statement, key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_string(&mut self, key: Self::Key) -> Result<String> {
        let method = driver_method!(self.statement.private_driver, StatementGetOption);
        let populate = |key: *const c_char,
                        value: *mut c_char,
                        length: *mut usize,
                        error: *mut FFI_AdbcError| unsafe {
            method(&mut self.statement, key, value, length, error)
        };
        get_option_string(key, populate)
    }
    fn set_option(&mut self, key: Self::Key, value: OptionValue) -> Result<()> {
        set_option_statement(&mut self.statement, self.version, key, value)
    }
}
impl<'driver, 'database, 'connection> Drop for ManagedStatement<'driver, 'database, 'connection> {
    fn drop(&mut self) {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.statement.private_driver, StatementRelease);
        let status = unsafe { method(&mut self.statement, &mut error) };
        if let Err(err) = check_status(status, error) {
            panic!("unable to drop statement: {:?}", err);
        }
    }
}
