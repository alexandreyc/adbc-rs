use std::os::raw::{c_int, c_void};

use arrow::record_batch::RecordBatch;

use adbc_rs::driver_manager::DriverManager;
use adbc_rs::options::{AdbcVersion, InfoCode, ObjectDepth, OptionValue};
use adbc_rs::{ffi, Connection, Database};
use adbc_rs::{Driver, Optionable};

#[link(name = "adbc_driver_sqlite", kind = "static")]
extern "C" {
    fn SqliteDriverInit(
        version: c_int,
        raw_driver: *mut c_void,
        error: *mut ffi::FFI_AdbcError,
    ) -> ffi::FFI_AdbcStatusCode;
}

#[test]
fn test_driver_manager() {
    let init = &(SqliteDriverInit as ffi::FFI_AdbcDriverInitFunc);

    assert!(DriverManager::load_static(init, AdbcVersion::V110).is_err());

    let driver = DriverManager::load_static(init, AdbcVersion::V100);
    assert!(driver.is_ok());
    let driver = driver.unwrap();

    assert!(driver.new_database().is_ok());

    let opts = [("uri", OptionValue::String("".into()))];
    assert!(driver.new_database_with_opts(opts.into_iter()).is_ok());

    // Non-string options aren't allowed with ADBC 1.0.0
    let opts = [("uri", OptionValue::Int(42))];
    assert!(driver.new_database_with_opts(opts.into_iter()).is_err());
}

fn get_driver() -> DriverManager {
    DriverManager::load_static(
        &(SqliteDriverInit as ffi::FFI_AdbcDriverInitFunc),
        AdbcVersion::V100,
    )
    .unwrap()
}

#[test]
fn test_database() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();

    assert!(database.new_connection().is_ok());

    // `adbc.connection.autocommit` can only be set after init
    let opts = [(
        "adbc.connection.autocommit",
        OptionValue::String("true".into()),
    )];
    assert!(database.new_connection_with_opts(opts.into_iter()).is_err());

    // Unknown connection option
    let opts = [("my.option", OptionValue::String("".into()))];
    assert!(database.new_connection_with_opts(opts.into_iter()).is_err());
}

#[test]
fn test_connection() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();

    assert!(connection
        .set_option(
            "adbc.connection.autocommit",
            OptionValue::String("true".into())
        )
        .is_ok());

    // Unknown connection option
    assert!(connection
        .set_option("my.option", OptionValue::String("".into()))
        .is_err());

    assert!(connection.new_statement().is_ok());
}

#[test]
fn test_connection_get_table_types() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();

    let table_types: Vec<RecordBatch> = connection
        .get_table_types()
        .unwrap()
        .map(|b| b.unwrap())
        .collect();
    assert_eq!(table_types.len(), 1);
    assert_eq!(table_types[0].num_columns(), 1);
    assert_eq!(table_types[0].num_rows(), 2);
}

#[test]
fn test_connection_get_info() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();

    let info: Vec<RecordBatch> = connection
        .get_info(None)
        .unwrap()
        .map(|b| b.unwrap())
        .collect();
    assert_eq!(info.len(), 1);
    assert_eq!(info[0].num_columns(), 2);
    assert_eq!(info[0].num_rows(), 5);

    let info: Vec<RecordBatch> = connection
        .get_info(Some(&[
            InfoCode::VendorName,
            InfoCode::DriverVersion,
            InfoCode::DriverName,
            InfoCode::VendorVersion,
        ]))
        .unwrap()
        .map(|b| b.unwrap())
        .collect();
    assert_eq!(info.len(), 1);
    assert_eq!(info[0].num_columns(), 2);
    assert_eq!(info[0].num_rows(), 4);
}

#[test]
fn test_connection_get_objects() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();

    let objects: Vec<RecordBatch> = connection
        .get_objects(ObjectDepth::All, None, None, None, None, None)
        .unwrap()
        .map(|b| b.unwrap())
        .collect();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].num_rows(), 1);
    assert_eq!(objects[0].num_columns(), 2);

    let objects: Vec<RecordBatch> = connection
        .get_objects(
            ObjectDepth::All,
            None,
            None,
            None,
            Some(&["table", "view"]),
            None,
        )
        .unwrap()
        .map(|b| b.unwrap())
        .collect();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].num_rows(), 1);
    assert_eq!(objects[0].num_columns(), 2);

    let objects: Vec<RecordBatch> = connection
        .get_objects(
            ObjectDepth::All,
            Some("my_catalog"),
            Some("my_schema"),
            Some("my_table"),
            Some(&["table", "view"]),
            Some("my_column"),
        )
        .unwrap()
        .map(|b| b.unwrap())
        .collect();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].num_rows(), 0);
    assert_eq!(objects[0].num_columns(), 2);
}

// TODOs
// - Test `get_option_*`
