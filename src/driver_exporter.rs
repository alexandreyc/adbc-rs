use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::{c_char, c_void};

use crate::check_err;
use crate::error::{Error, Status};
use crate::ffi::constants::ADBC_STATUS_OK;
use crate::ffi::{
    FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcError, FFI_AdbcStatusCode,
};
use crate::options::{OptionConnection, OptionDatabase, OptionValue};
use crate::{Database, Driver, Optionable};

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

pub(crate) fn make_ffi_driver<DriverType: Driver + Default>() -> FFI_AdbcDriver {
    FFI_AdbcDriver {
        private_data: std::ptr::null_mut(),
        private_manager: std::ptr::null(),
        release: Some(release_ffi_driver),
        DatabaseInit: Some(database_init::<DriverType>),
        DatabaseNew: Some(database_new::<DriverType>),
        DatabaseSetOption: Some(database_set_option::<DriverType>),
        DatabaseRelease: Some(database_release::<DriverType>),
        ConnectionCommit: None,
        ConnectionGetInfo: None,
        ConnectionGetObjects: None,
        ConnectionGetTableSchema: None,
        ConnectionGetTableTypes: None,
        ConnectionInit: Some(connection_init::<DriverType>),
        ConnectionNew: Some(connection_new::<DriverType>),
        ConnectionSetOption: None,
        ConnectionReadPartition: None,
        ConnectionRelease: Some(connection_release::<DriverType>),
        ConnectionRollback: None,
        StatementBind: None,
        StatementBindStream: None,
        StatementExecuteQuery: None,
        StatementExecutePartitions: None,
        StatementGetParameterSchema: None,
        StatementNew: None,
        StatementPrepare: None,
        StatementRelease: None,
        StatementSetOption: None,
        StatementSetSqlQuery: None,
        StatementSetSubstraitPlan: None,
        ErrorGetDetailCount: None,
        ErrorGetDetail: None,
        ErrorFromArrayStream: None,
        DatabaseGetOption: None,
        DatabaseGetOptionBytes: None,
        DatabaseGetOptionDouble: None,
        DatabaseGetOptionInt: Some(database_get_option_int::<DriverType>),
        DatabaseSetOptionBytes: None,
        DatabaseSetOptionDouble: None,
        DatabaseSetOptionInt: Some(database_set_option_int::<DriverType>),
        ConnectionCancel: None,
        ConnectionGetOption: None,
        ConnectionGetOptionBytes: None,
        ConnectionGetOptionDouble: None,
        ConnectionGetOptionInt: None,
        ConnectionGetStatistics: None,
        ConnectionGetStatisticNames: None,
        ConnectionSetOptionBytes: None,
        ConnectionSetOptionDouble: None,
        ConnectionSetOptionInt: None,
        StatementCancel: None,
        StatementExecuteSchema: None,
        StatementGetOption: None,
        StatementGetOptionBytes: None,
        StatementGetOptionDouble: None,
        StatementGetOptionInt: None,
        StatementSetOptionBytes: None,
        StatementSetOptionDouble: None,
        StatementSetOptionInt: None,
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

unsafe extern "C" fn database_new<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let database = database.as_mut().ok_or(Error::with_message_and_status(
        "Passed null pointer to DatabaseNew",
        Status::InvalidArguments,
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
    let database = database.as_mut().ok_or(Error::with_message_and_status(
        "Passed null pointer to DatabaseInit",
        Status::InvalidArguments,
    ));
    let database = check_err!(database, error);

    let mut exported = Box::from_raw(database.private_data as *mut ExportedDatabase<DriverType>);
    debug_assert!(exported.options.is_some() && exported.database.is_none());

    let driver = DriverType::default();
    let opts = exported.options.take().unwrap();
    let inner = driver.new_database_with_opts(opts.into_iter());
    let inner = check_err!(inner, error);
    exported.database = Some(inner);

    database.private_data = Box::into_raw(exported) as *mut c_void;
    ADBC_STATUS_OK
}

unsafe extern "C" fn database_set_option<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *const c_char,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let database = database.as_mut().ok_or(Error::with_message_and_status(
        "Passed null pointer to DatabaseSetOption",
        Status::InvalidArguments,
    ));
    let database = check_err!(database, error);
    let mut exported = Box::from_raw(database.private_data as *mut ExportedDatabase<DriverType>);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());

    let key = check_err!(CStr::from_ptr(key).to_str(), error);
    let value = check_err!(CStr::from_ptr(value).to_str(), error);

    if let Some(options) = exported.options.as_mut() {
        options.insert(key.into(), value.into());
    } else {
        let inner = exported.database.as_mut().expect("Invariant violated");
        let res = inner.set_option(key.into(), value.into());
        check_err!(res, error);
    }

    database.private_data = Box::into_raw(exported) as *mut c_void;
    ADBC_STATUS_OK
}

unsafe extern "C" fn database_release<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let database = database.as_mut().ok_or(Error::with_message_and_status(
        "Passed null pointer to DatabaseRelease",
        Status::InvalidArguments,
    ));
    let database = check_err!(database, error);
    let exported = Box::from_raw(database.private_data as *mut ExportedDatabase<DriverType>);
    drop(exported);
    database.private_data = std::ptr::null_mut();
    ADBC_STATUS_OK
}

unsafe extern "C" fn database_set_option_int<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let database = database.as_mut().ok_or(Error::with_message_and_status(
        "Passed null pointer to DatabaseSetOptionInt",
        Status::InvalidArguments,
    ));
    let database = check_err!(database, error);
    let mut exported = Box::from_raw(database.private_data as *mut ExportedDatabase<DriverType>);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());

    let key = check_err!(CStr::from_ptr(key).to_str(), error);

    if let Some(options) = exported.options.as_mut() {
        options.insert(key.into(), value.into());
    } else {
        let inner = exported.database.as_mut().expect("Invariant violated");
        let res = inner.set_option(key.into(), value.into());
        check_err!(res, error);
    }

    database.private_data = Box::into_raw(exported) as *mut c_void;
    ADBC_STATUS_OK
}

unsafe extern "C" fn database_get_option_int<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *mut i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let database = database.as_mut().ok_or(Error::with_message_and_status(
        "Passed null pointer to DatabaseGetOptionInt",
        Status::InvalidArguments,
    ));
    let database = check_err!(database, error);
    let mut exported = Box::from_raw(database.private_data as *mut ExportedDatabase<DriverType>);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());

    let key = check_err!(CStr::from_ptr(key).to_str(), error);

    if let Some(options) = exported.options.as_mut() {
        let optvalue = options
            .get(&key.into())
            .ok_or(Error::with_message_and_status(
                &format!("Database option key not found: {}", key),
                Status::NotFound,
            ));
        let optvalue = check_err!(optvalue, error);
        if let OptionValue::Int(optvalue) = optvalue {
            std::ptr::write_unaligned(value, *optvalue);
        } else {
            let err = Error::with_message_and_status(
                &format!("Database option value has wrong type: {}", key),
                Status::InvalidState,
            );
            check_err!(Err(err), error);
        }
    } else {
        let inner = exported.database.as_mut().expect("Invariant violated");
        let optvalue = inner.get_option_int(key.into());
        let optvalue = check_err!(optvalue, error);
        std::ptr::write_unaligned(value, optvalue);
    }

    // TODO: if we return early (possible due to check_err! calls), this line is
    // not executed, is this a problem?
    database.private_data = Box::into_raw(exported) as *mut c_void;
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_new<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let connection = connection.as_mut().ok_or(Error::with_message_and_status(
        "Passed null pointer to ConnectionNew",
        Status::InvalidArguments,
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
    let connection = connection.as_mut().ok_or(Error::with_message_and_status(
        "Passed null pointer to ConnectionInit",
        Status::InvalidArguments,
    ));
    let connection = check_err!(connection, error);
    let mut exported_connection =
        Box::from_raw(connection.private_data as *mut ExportedConnection<DriverType>);

    debug_assert!(
        exported_connection.options.is_some() && exported_connection.connection.is_none()
    );

    let database = database.as_mut().ok_or(Error::with_message_and_status(
        "Passed null pointer to ConnectionInit",
        Status::InvalidArguments,
    ));
    let database = check_err!(database, error);
    let exported_database =
        Box::from_raw(database.private_data as *mut ExportedDatabase<DriverType>);

    debug_assert!(exported_database.database.is_some());

    let opts = exported_connection.options.take().unwrap();
    let inner = exported_database
        .database
        .as_ref()
        .unwrap()
        .new_connection_with_opts(opts.into_iter());
    let inner = check_err!(inner, error);
    exported_connection.connection = Some(inner);

    connection.private_data = Box::into_raw(exported_connection) as *mut c_void;
    database.private_data = Box::into_raw(exported_database) as *mut c_void;

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_release<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let connection = connection.as_mut().ok_or(Error::with_message_and_status(
        "Passed null pointer to ConnectionRelease",
        Status::InvalidArguments,
    ));
    let connection = check_err!(connection, error);
    let exported = Box::from_raw(connection.private_data as *mut ExportedConnection<DriverType>);
    drop(exported);
    connection.private_data = std::ptr::null_mut();
    ADBC_STATUS_OK
}
