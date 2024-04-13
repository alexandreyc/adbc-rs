use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::hash::Hash;
use std::os::raw::{c_char, c_void};

use arrow::array::StructArray;
use arrow::datatypes::DataType;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use crate::error::{Error, Result, Status};
use crate::ffi::constants::ADBC_STATUS_OK;
use crate::ffi::{
    FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcError, FFI_AdbcStatement,
    FFI_AdbcStatusCode,
};
use crate::options::{InfoCode, OptionConnection, OptionDatabase, OptionValue};
use crate::{check_err, Connection, Database, Driver, Optionable, Statement};

/// Invariant: options.is_none() XOR database.is_none()
struct ExportedDatabase<DriverType: Driver + Default> {
    options: Option<HashMap<OptionDatabase, OptionValue>>, // Pre-init options
    database: Option<DriverType::DatabaseType>,
}

/// Invariant: options.is_none() XOR database.is_none()
struct ExportedConnection<DriverType: Driver + Default> {
    options: Option<HashMap<OptionConnection, OptionValue>>, // Pre-init options
    connection: Option<<DriverType::DatabaseType as Database>::ConnectionType>,
}

struct ExportedStatement<DriverType: Driver + Default> {
    statement:
        <<DriverType::DatabaseType as Database>::ConnectionType as Connection>::StatementType,
}

pub(crate) fn make_ffi_driver<DriverType: Driver + Default + 'static>() -> FFI_AdbcDriver {
    FFI_AdbcDriver {
        private_data: std::ptr::null_mut(),
        private_manager: std::ptr::null(),
        release: Some(release_ffi_driver),
        DatabaseInit: Some(database_init::<DriverType>),
        DatabaseNew: Some(database_new::<DriverType>),
        DatabaseSetOption: Some(database_set_option::<DriverType>),
        DatabaseRelease: Some(database_release::<DriverType>),
        ConnectionCommit: Some(connection_commit::<DriverType>),
        ConnectionGetInfo: Some(connection_get_info::<DriverType>),
        ConnectionGetObjects: None,
        ConnectionGetTableSchema: Some(connection_get_table_schema::<DriverType>),
        ConnectionGetTableTypes: Some(connection_get_table_types::<DriverType>),
        ConnectionInit: Some(connection_init::<DriverType>),
        ConnectionNew: Some(connection_new::<DriverType>),
        ConnectionSetOption: Some(connection_set_option::<DriverType>),
        ConnectionReadPartition: Some(connection_read_partition::<DriverType>),
        ConnectionRelease: Some(connection_release::<DriverType>),
        ConnectionRollback: Some(connection_rollback::<DriverType>),
        StatementBind: Some(statement_bind::<DriverType>),
        StatementBindStream: Some(statement_bind_stream::<DriverType>),
        StatementExecuteQuery: None,
        StatementExecutePartitions: None,
        StatementGetParameterSchema: None,
        StatementNew: Some(statement_new::<DriverType>),
        StatementPrepare: None,
        StatementRelease: Some(statement_release::<DriverType>),
        StatementSetOption: Some(statement_set_option::<DriverType>),
        StatementSetSqlQuery: None,
        StatementSetSubstraitPlan: None,
        ErrorGetDetailCount: None,
        ErrorGetDetail: None,
        ErrorFromArrayStream: None,
        DatabaseGetOption: Some(database_get_option::<DriverType>),
        DatabaseGetOptionBytes: Some(database_get_option_bytes::<DriverType>),
        DatabaseGetOptionDouble: Some(database_get_option_double::<DriverType>),
        DatabaseGetOptionInt: Some(database_get_option_int::<DriverType>),
        DatabaseSetOptionBytes: Some(database_set_option_bytes::<DriverType>),
        DatabaseSetOptionDouble: Some(database_set_option_double::<DriverType>),
        DatabaseSetOptionInt: Some(database_set_option_int::<DriverType>),
        ConnectionCancel: Some(connection_cancel::<DriverType>),
        ConnectionGetOption: Some(connection_get_option::<DriverType>),
        ConnectionGetOptionBytes: Some(connection_get_option_bytes::<DriverType>),
        ConnectionGetOptionDouble: Some(connection_get_option_double::<DriverType>),
        ConnectionGetOptionInt: Some(connection_get_option_int::<DriverType>),
        ConnectionGetStatistics: None,
        ConnectionGetStatisticNames: Some(connection_get_statistic_names::<DriverType>),
        ConnectionSetOptionBytes: Some(connection_set_option_bytes::<DriverType>),
        ConnectionSetOptionDouble: Some(connection_set_option_double::<DriverType>),
        ConnectionSetOptionInt: Some(connection_set_option_int::<DriverType>),
        StatementCancel: None,
        StatementExecuteSchema: None,
        StatementGetOption: Some(statement_get_option::<DriverType>),
        StatementGetOptionBytes: Some(statement_get_option_bytes::<DriverType>),
        StatementGetOptionDouble: Some(statement_get_option_double::<DriverType>),
        StatementGetOptionInt: Some(statement_get_option_int::<DriverType>),
        StatementSetOptionBytes: Some(statement_set_option_bytes::<DriverType>),
        StatementSetOptionDouble: Some(statement_set_option_double::<DriverType>),
        StatementSetOptionInt: Some(statement_set_option_int::<DriverType>),
    }
}

/// Export a Rust driver to a C driver.
///
/// The default name recommended is `AdbcDriverInit` or `<Prefix>DriverInit`.
///
/// The driver type must implement [Driver] and [Default].
#[macro_export]
macro_rules! export_driver {
    ($func_name:ident, $driver_type:ty) => {
        #[no_mangle]
        pub unsafe extern "C" fn $func_name(
            version: std::os::raw::c_int,
            driver: *mut std::os::raw::c_void,
            error: *mut $crate::ffi::FFI_AdbcError,
        ) -> $crate::ffi::FFI_AdbcStatusCode {
            if version != $crate::options::AdbcVersion::V110.into() {
                let err = $crate::error::Error::with_message_and_status(
                    &format!("Unsupported ADBC version: {}", version),
                    $crate::error::Status::NotImplemented,
                );
                $crate::check_err!(Err(err), error);
            }

            if driver.is_null() {
                let err = $crate::error::Error::with_message_and_status(
                    "Passed null pointer to initialization function",
                    $crate::error::Status::NotImplemented,
                );
                $crate::check_err!(Err(err), error);
            }

            let ffi_driver = $crate::driver_exporter::make_ffi_driver::<$driver_type>();
            unsafe {
                std::ptr::write_unaligned(driver as *mut $crate::ffi::FFI_AdbcDriver, ffi_driver);
            }
            $crate::ffi::constants::ADBC_STATUS_OK
        }
    };
}

unsafe extern "C" fn release_ffi_driver(
    driver: *mut FFI_AdbcDriver,
    _error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    // TODO: if there is no private data is there more we should do?
    if let Some(driver) = driver.as_mut() {
        driver.release = None;
    }
    ADBC_STATUS_OK
}

// Option helpers

unsafe fn copy_string(src: &str, dst: *mut c_char, length: *mut usize) -> Result<()> {
    let n = src.len() + 1; // +1 for nul terminator
    let src = CString::new(src)?;
    if n <= *length {
        std::ptr::copy(src.as_ptr(), dst, n);
    }
    *length = n;
    Ok::<(), Error>(())
}

unsafe fn copy_bytes(src: &[u8], dst: *mut u8, length: *mut usize) -> Result<()> {
    let n = src.len();
    if n <= *length {
        std::ptr::copy(src.as_ptr(), dst, n);
    }
    *length = n;
    Ok::<(), Error>(())
}

unsafe fn get_option_int<'a, OptionType, Object>(
    object: Option<&Object>,
    options: &mut Option<HashMap<OptionType, OptionValue>>,
    key: *const c_char,
) -> Result<i64>
where
    OptionType: Hash + Eq + From<&'a str>,
    Object: Optionable<Option = OptionType>,
{
    let key = CStr::from_ptr(key).to_str()?;

    if let Some(options) = options.as_mut() {
        let optvalue = options
            .get(&key.into())
            .ok_or(Error::with_message_and_status(
                &format!("Option key not found: {}", key),
                Status::NotFound,
            ))?;
        if let OptionValue::Int(optvalue) = optvalue {
            Ok(*optvalue)
        } else {
            let err = Error::with_message_and_status(
                &format!("Option value for key {:?} has wrong type", key),
                Status::InvalidState,
            );
            Err(err)
        }
    } else {
        let object = object.expect("Broken invariant");
        let optvalue = object.get_option_int(key.into())?;
        Ok(optvalue)
    }
}

unsafe fn get_option_double<'a, OptionType, Object>(
    object: Option<&Object>,
    options: &mut Option<HashMap<OptionType, OptionValue>>,
    key: *const c_char,
) -> Result<f64>
where
    OptionType: Hash + Eq + From<&'a str>,
    Object: Optionable<Option = OptionType>,
{
    let key = CStr::from_ptr(key).to_str()?;

    if let Some(options) = options.as_mut() {
        let optvalue = options
            .get(&key.into())
            .ok_or(Error::with_message_and_status(
                &format!("Option key not found: {}", key),
                Status::NotFound,
            ))?;
        if let OptionValue::Double(optvalue) = optvalue {
            Ok(*optvalue)
        } else {
            let err = Error::with_message_and_status(
                &format!("Option value for key {:?} has wrong type", key),
                Status::InvalidState,
            );
            Err(err)
        }
    } else {
        let object = object.expect("Broken invariant");
        let optvalue = object.get_option_double(key.into())?;
        Ok(optvalue)
    }
}

unsafe fn get_option<'a, OptionType, Object>(
    object: Option<&Object>,
    options: &mut Option<HashMap<OptionType, OptionValue>>,
    key: *const c_char,
) -> Result<String>
where
    OptionType: Hash + Eq + From<&'a str>,
    Object: Optionable<Option = OptionType>,
{
    let key = CStr::from_ptr(key).to_str()?;

    if let Some(options) = options.as_ref() {
        let optvalue = options
            .get(&key.into())
            .ok_or(Error::with_message_and_status(
                &format!("Option key not found: {}", key),
                Status::NotFound,
            ))?;
        if let OptionValue::String(optvalue) = optvalue {
            Ok(optvalue.clone())
        } else {
            let err = Error::with_message_and_status(
                &format!("Option value for key {:?} has wrong type", key),
                Status::InvalidState,
            );
            Err(err)
        }
    } else {
        let database = object.as_ref().expect("Broken invariant");
        let optvalue = database.get_option_string(key.into())?;
        Ok(optvalue)
    }
}

unsafe fn get_option_bytes<'a, OptionType, Object>(
    object: Option<&Object>,
    options: &mut Option<HashMap<OptionType, OptionValue>>,
    key: *const c_char,
) -> Result<Vec<u8>>
where
    OptionType: Hash + Eq + From<&'a str>,
    Object: Optionable<Option = OptionType>,
{
    let key = CStr::from_ptr(key).to_str()?;

    if let Some(options) = options.as_ref() {
        let optvalue = options
            .get(&key.into())
            .ok_or(Error::with_message_and_status(
                &format!("Option key not found: {}", key),
                Status::NotFound,
            ))?;
        if let OptionValue::Bytes(optvalue) = optvalue {
            Ok(optvalue.clone())
        } else {
            let err = Error::with_message_and_status(
                &format!("Option value for key {:?} has wrong type", key),
                Status::InvalidState,
            );
            Err(err)
        }
    } else {
        let connection = object.as_ref().expect("Broken invariant");
        let optvalue = connection.get_option_bytes(key.into())?;
        Ok(optvalue)
    }
}

// Database

unsafe fn database_private_data<'a, DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
) -> Result<&'a mut ExportedDatabase<DriverType>> {
    let database = database.as_mut().ok_or(Error::with_message_and_status(
        "Passed null database pointer",
        Status::InvalidState,
    ))?;
    let exported = database.private_data as *mut ExportedDatabase<DriverType>;
    let exported = exported.as_mut().ok_or(Error::with_message_and_status(
        "Uninitialized database",
        Status::InvalidState,
    ));
    exported
}

unsafe fn database_set_option_impl<DriverType: Driver + Default, Value: Into<OptionValue>>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: Value,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());

    let key = check_err!(CStr::from_ptr(key).to_str(), error);

    if let Some(options) = exported.options.as_mut() {
        options.insert(key.into(), value.into());
    } else {
        let database = exported.database.as_mut().expect("Broken invariant");
        check_err!(database.set_option(key.into(), value.into()), error);
    }

    ADBC_STATUS_OK
}

unsafe extern "C" fn database_new<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let database = database.as_mut().ok_or(Error::with_message_and_status(
        "Passed null database pointer",
        Status::InvalidState,
    ));
    let database = check_err!(database, error);

    let exported = Box::new(ExportedDatabase::<DriverType> {
        options: Some(HashMap::new()),
        database: None::<DriverType::DatabaseType>,
    });
    database.private_data = Box::into_raw(exported) as *mut c_void;

    ADBC_STATUS_OK
}

unsafe extern "C" fn database_init<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(exported.options.is_some() && exported.database.is_none());

    let driver = DriverType::default();
    let options = exported.options.take().expect("Broken invariant");
    let database = driver.new_database_with_opts(options.into_iter());
    let database = check_err!(database, error);
    exported.database = Some(database);

    ADBC_STATUS_OK
}

unsafe extern "C" fn database_release<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let database = database.as_mut().ok_or(Error::with_message_and_status(
        "Passed null database pointer",
        Status::InvalidState,
    ));
    let database = check_err!(database, error);
    let exported = Box::from_raw(database.private_data as *mut ExportedDatabase<DriverType>);

    drop(exported);
    database.private_data = std::ptr::null_mut();

    ADBC_STATUS_OK
}

unsafe extern "C" fn database_set_option<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *const c_char,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let value = check_err!(CStr::from_ptr(value).to_str(), error);
    database_set_option_impl::<DriverType, &str>(database, key, value, error)
}

unsafe extern "C" fn database_set_option_int<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    database_set_option_impl::<DriverType, i64>(database, key, value, error)
}

unsafe extern "C" fn database_set_option_double<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: f64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    database_set_option_impl::<DriverType, f64>(database, key, value, error)
}

unsafe extern "C" fn database_set_option_bytes<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *const u8,
    length: usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let value = std::slice::from_raw_parts(value, length);
    database_set_option_impl::<DriverType, &[u8]>(database, key, value, error)
}

unsafe extern "C" fn database_get_option<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *mut c_char,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());

    let optvalue = get_option(exported.database.as_ref(), &mut exported.options, key);
    let optvalue = check_err!(optvalue, error);
    check_err!(copy_string(&optvalue, value, length), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn database_get_option_int<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *mut i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());
    let optvalue = check_err!(
        get_option_int(exported.database.as_ref(), &mut exported.options, key),
        error
    );
    std::ptr::write_unaligned(value, optvalue);
    ADBC_STATUS_OK
}

unsafe extern "C" fn database_get_option_double<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *mut f64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());
    let optvalue = check_err!(
        get_option_double(exported.database.as_ref(), &mut exported.options, key),
        error
    );
    std::ptr::write_unaligned(value, optvalue);
    ADBC_STATUS_OK
}

unsafe extern "C" fn database_get_option_bytes<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *mut u8,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());

    let optvalue = get_option_bytes(exported.database.as_ref(), &mut exported.options, key);
    let optvalue = check_err!(optvalue, error);
    check_err!(copy_bytes(&optvalue, value, length), error);

    ADBC_STATUS_OK
}

// Connection

unsafe fn connection_private_data<'a, DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
) -> Result<&'a mut ExportedConnection<DriverType>> {
    let connection = connection.as_mut().ok_or(Error::with_message_and_status(
        "Passed null connection pointer",
        Status::InvalidState,
    ))?;
    let exported = connection.private_data as *mut ExportedConnection<DriverType>;
    let exported = exported.as_mut().ok_or(Error::with_message_and_status(
        "Uninitialized connection",
        Status::InvalidState,
    ));
    exported
}

unsafe fn connection_set_option_impl<DriverType: Driver + Default, Value: Into<OptionValue>>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: Value,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    debug_assert!(exported.options.is_some() ^ exported.connection.is_some());

    let key = check_err!(CStr::from_ptr(key).to_str(), error);

    if let Some(options) = exported.options.as_mut() {
        options.insert(key.into(), value.into());
    } else {
        let connection = exported.connection.as_mut().expect("Broken invariant");
        check_err!(connection.set_option(key.into(), value.into()), error);
    }

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_new<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let connection = connection.as_mut().ok_or(Error::with_message_and_status(
        "Passed null connection pointer",
        Status::InvalidState,
    ));
    let connection = check_err!(connection, error);

    let exported = Box::new(ExportedConnection::<DriverType> {
        options: Some(HashMap::new()),
        connection: None,
    });
    connection.private_data = Box::into_raw(exported) as *mut c_void;

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_init<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    database: *mut FFI_AdbcDatabase,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported_connection = check_err!(connection_private_data::<DriverType>(connection), error);
    let exported_database = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(
        exported_connection.options.is_some()
            && exported_connection.connection.is_none()
            && exported_database.database.is_some()
    );

    let options = exported_connection
        .options
        .take()
        .expect("Broken invariant");

    let connection = exported_database
        .database
        .as_ref()
        .expect("Broken invariant")
        .new_connection_with_opts(options.into_iter());
    let connection = check_err!(connection, error);
    exported_connection.connection = Some(connection);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_release<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let connection = connection.as_mut().ok_or(Error::with_message_and_status(
        "Passed null connection pointer",
        Status::InvalidState,
    ));
    let connection = check_err!(connection, error);

    let exported = Box::from_raw(connection.private_data as *mut ExportedConnection<DriverType>);
    drop(exported);
    connection.private_data = std::ptr::null_mut();

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_set_option<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *const c_char,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let value = check_err!(CStr::from_ptr(value).to_str(), error);
    connection_set_option_impl::<DriverType, &str>(connection, key, value, error)
}

unsafe extern "C" fn connection_set_option_int<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    connection_set_option_impl::<DriverType, i64>(connection, key, value, error)
}

unsafe extern "C" fn connection_set_option_double<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: f64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    connection_set_option_impl::<DriverType, f64>(connection, key, value, error)
}

unsafe extern "C" fn connection_set_option_bytes<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *const u8,
    length: usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let value = std::slice::from_raw_parts(value, length);
    connection_set_option_impl::<DriverType, &[u8]>(connection, key, value, error)
}

unsafe extern "C" fn connection_get_option<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *mut c_char,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    debug_assert!(exported.options.is_some() ^ exported.connection.is_some());
    let optvalue = get_option(exported.connection.as_ref(), &mut exported.options, key);
    let optvalue = check_err!(optvalue, error);
    check_err!(copy_string(&optvalue, value, length), error);
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_option_int<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *mut i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    debug_assert!(exported.options.is_some() ^ exported.connection.is_some());
    let optvalue = check_err!(
        get_option_int(exported.connection.as_ref(), &mut exported.options, key),
        error
    );
    std::ptr::write_unaligned(value, optvalue);
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_option_double<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *mut f64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    debug_assert!(exported.options.is_some() ^ exported.connection.is_some());
    let optvalue = check_err!(
        get_option_double(exported.connection.as_ref(), &mut exported.options, key),
        error
    );
    std::ptr::write_unaligned(value, optvalue);
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_option_bytes<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *mut u8,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    debug_assert!(exported.options.is_some() ^ exported.connection.is_some());
    let optvalue = get_option_bytes(exported.connection.as_ref(), &mut exported.options, key);
    let optvalue = check_err!(optvalue, error);
    check_err!(copy_bytes(&optvalue, value, length), error);
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_table_types<DriverType: Driver + Default + 'static>(
    connection: *mut FFI_AdbcConnection,
    stream: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");
    let reader = check_err!(connection.get_table_types(), error);
    let reader = Box::new(reader);
    let reader = FFI_ArrowArrayStream::new(reader);
    std::ptr::write_unaligned(stream, reader);
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_table_schema<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    catalog: *const c_char,
    db_schema: *const c_char,
    table: *const c_char,
    schema: *mut FFI_ArrowSchema,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");

    let catalog = catalog
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .transpose();
    let catalog = check_err!(catalog, error);

    let db_schema = db_schema
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .transpose();
    let db_schema = check_err!(db_schema, error);

    let table = table
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .transpose();
    let table = check_err!(table, error);

    if let Some(table) = table {
        let table_schema = connection.get_table_schema(catalog, db_schema, table);
        let table_schema = check_err!(table_schema, error);
        let table_schema: FFI_ArrowSchema = check_err!(table_schema.try_into(), error);
        std::ptr::write_unaligned(schema, table_schema);
    } else {
        check_err!(
            Err(Error::with_message_and_status(
                "Passed null table pointer",
                Status::InvalidState
            )),
            error
        );
    }

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_info<DriverType: Driver + Default + 'static>(
    connection: *mut FFI_AdbcConnection,
    info_codes: *const u32,
    length: usize,
    stream: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");

    let info_codes = if info_codes.is_null() {
        None
    } else {
        let info_codes = std::slice::from_raw_parts(info_codes, length);
        let info_codes: Result<Vec<InfoCode>> =
            info_codes.iter().map(|c| InfoCode::try_from(*c)).collect();
        let info_codes = check_err!(info_codes, error);
        Some(info_codes)
    };

    let reader = check_err!(connection.get_info(info_codes), error);
    let reader = Box::new(reader);
    let reader = FFI_ArrowArrayStream::new(reader);
    std::ptr::write_unaligned(stream, reader);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_commit<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");
    check_err!(connection.commit(), error);
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_rollback<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");
    check_err!(connection.rollback(), error);
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_cancel<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");
    check_err!(connection.cancel(), error);
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_statistic_names<DriverType: Driver + Default + 'static>(
    connection: *mut FFI_AdbcConnection,
    stream: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");

    let reader = check_err!(connection.get_statistic_names(), error);
    let reader = Box::new(reader);
    let reader = FFI_ArrowArrayStream::new(reader);
    std::ptr::write_unaligned(stream, reader);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_read_partition<DriverType: Driver + Default + 'static>(
    connection: *mut FFI_AdbcConnection,
    partition: *const u8,
    length: usize,
    stream: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");

    let partition = std::slice::from_raw_parts(partition, length);
    let reader = check_err!(connection.read_partition(partition), error);
    let reader = Box::new(reader);
    let reader = FFI_ArrowArrayStream::new(reader);
    std::ptr::write_unaligned(stream, reader);

    ADBC_STATUS_OK
}

// Statement

unsafe fn statement_private_data<'a, DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
) -> Result<&'a mut ExportedStatement<DriverType>> {
    let statement = statement.as_mut().ok_or(Error::with_message_and_status(
        "Passed null statement pointer",
        Status::InvalidState,
    ))?;
    let exported = statement.private_data as *mut ExportedStatement<DriverType>;
    let exported = exported.as_mut().ok_or(Error::with_message_and_status(
        "Uninitialized statement",
        Status::InvalidState,
    ));
    exported
}

unsafe fn statement_set_option_impl<DriverType: Driver + Default, Value: Into<OptionValue>>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: Value,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let key = check_err!(CStr::from_ptr(key).to_str(), error);
    check_err!(
        exported.statement.set_option(key.into(), value.into()),
        error
    );
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_new<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    statement: *mut FFI_AdbcStatement,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported_connection = check_err!(connection_private_data::<DriverType>(connection), error);
    let inner_connection = exported_connection
        .connection
        .as_ref()
        .expect("Broken invariant");

    let statement = statement.as_mut().ok_or(Error::with_message_and_status(
        "Passed null statement pointer",
        Status::InvalidState,
    ));
    let statement = check_err!(statement, error);
    let inner_statement = check_err!(inner_connection.new_statement(), error);

    let exported = Box::new(ExportedStatement::<DriverType> {
        statement: inner_statement,
    });
    statement.private_data = Box::into_raw(exported) as *mut c_void;

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_release<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let statement = statement.as_mut().ok_or(Error::with_message_and_status(
        "Passed null statement pointer",
        Status::InvalidState,
    ));
    let statement = check_err!(statement, error);
    let exported = Box::from_raw(statement.private_data as *mut ExportedStatement<DriverType>);

    drop(exported);
    statement.private_data = std::ptr::null_mut();

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_set_option<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: *const c_char,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let value = check_err!(CStr::from_ptr(value).to_str(), error);
    statement_set_option_impl::<DriverType, &str>(statement, key, value, error)
}

unsafe extern "C" fn statement_set_option_int<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    statement_set_option_impl::<DriverType, i64>(statement, key, value, error)
}

unsafe extern "C" fn statement_set_option_double<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: f64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    statement_set_option_impl::<DriverType, f64>(statement, key, value, error)
}

unsafe extern "C" fn statement_set_option_bytes<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: *const u8,
    length: usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let value = std::slice::from_raw_parts(value, length);
    statement_set_option_impl::<DriverType, &[u8]>(statement, key, value, error)
}

unsafe extern "C" fn statement_get_option<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: *mut c_char,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let optvalue = get_option(Some(&exported.statement), &mut None, key);
    let optvalue = check_err!(optvalue, error);
    check_err!(copy_string(&optvalue, value, length), error);
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_get_option_int<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: *mut i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let optvalue = check_err!(
        get_option_int(Some(&exported.statement), &mut None, key),
        error
    );
    std::ptr::write_unaligned(value, optvalue);
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_get_option_double<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: *mut f64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let optvalue = check_err!(
        get_option_double(Some(&exported.statement), &mut None, key),
        error
    );
    std::ptr::write_unaligned(value, optvalue);
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_get_option_bytes<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: *mut u8,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let optvalue = get_option_bytes(Some(&exported.statement), &mut None, key);
    let optvalue = check_err!(optvalue, error);
    check_err!(copy_bytes(&optvalue, value, length), error);
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_bind<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    data: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let statement = &exported.statement;

    if data.is_null() {
        check_err!(
            Err(Error::with_message_and_status(
                "Passed null data pointer",
                Status::InvalidArguments
            )),
            error
        );
    }

    let schema = schema.as_ref().ok_or(Error::with_message_and_status(
        "Passed null schema pointer",
        Status::InvalidState,
    ));
    let schema = check_err!(schema, error);
    let data = FFI_ArrowArray::from_raw(data);
    let array = check_err!(from_ffi(data, schema), error);

    if !matches!(array.data_type(), DataType::Struct(_)) {
        check_err!(
            Err(Error::with_message_and_status(
                "You must pass a struct array to statement bind",
                Status::InvalidArguments
            )),
            error
        );
    }

    let array: StructArray = array.into();
    check_err!(statement.bind(array.into()), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_bind_stream<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    stream: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let statement = &exported.statement;

    if stream.is_null() {
        check_err!(
            Err(Error::with_message_and_status(
                "Passed null stream pointer",
                Status::InvalidArguments
            )),
            error
        );
    }

    let reader = check_err!(ArrowArrayStreamReader::from_raw(stream), error);
    let reader = Box::new(reader);
    check_err!(statement.bind_stream(reader), error);

    ADBC_STATUS_OK
}
