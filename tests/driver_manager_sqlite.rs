use arrow::datatypes::{Field, Schema};

use adbc_rs::driver_manager::{DriverManager, ManagedDatabase};
use adbc_rs::options::{AdbcVersion, OptionConnection, OptionDatabase};
use adbc_rs::{error::Status, Driver, Optionable};
use adbc_rs::{Connection, Database, Statement};

mod common;

// By passing in ":memory:" for URI, we create a distinct temporary database for
// each test, preventing noisy neighbor issues on tests.
const URI: &str = ":memory:";

fn get_driver() -> DriverManager {
    DriverManager::load_dynamic("adbc_driver_sqlite", None, AdbcVersion::V100).unwrap()
}

fn get_database(driver: &DriverManager) -> ManagedDatabase {
    let opts = [(OptionDatabase::Uri, URI.into())];
    driver.new_database_with_opts(opts.into_iter()).unwrap()
}

#[test]
fn test_driver() {
    let driver = get_driver();
    common::test_driver(&driver, URI);
}

#[test]
fn test_database() {
    let driver = get_driver();
    let database = get_database(&driver);
    common::test_database(&database);
}

#[test]
fn test_database_get_option() {
    let driver = get_driver();
    let database = get_database(&driver);

    let error = database
        .get_option_bytes(OptionDatabase::Username)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = database
        .get_option_string(OptionDatabase::Username)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = database
        .get_option_int(OptionDatabase::Username)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = database
        .get_option_double(OptionDatabase::Username)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_connection() {
    let driver = get_driver();
    let database = get_database(&driver);
    let mut connection = database.new_connection().unwrap();
    common::test_connection(&mut connection);
}

#[test]
fn test_connection_get_option() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();

    let error = connection
        .get_option_bytes(OptionConnection::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = connection
        .get_option_string(OptionConnection::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = connection
        .get_option_int(OptionConnection::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = connection
        .get_option_double(OptionConnection::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_connection_cancel() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();

    let error = connection.cancel().unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_connection_commit_rollback() {
    let driver = get_driver();
    let database = get_database(&driver);
    let mut connection = database.new_connection().unwrap();
    common::test_connection_commit_rollback(&mut connection);
}

#[test]
fn test_connection_read_partition() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_read_partition(&connection);
}

#[test]
fn test_connection_get_table_types() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_get_table_types(&connection, &["table", "view"]);
}

#[test]
fn test_connection_get_info() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_get_info(&connection, 5);
}

#[test]
fn test_connection_get_objects() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_get_objects(&connection, 1, 1);
}

#[test]
fn test_connection_get_table_schema() {
    let driver = get_driver();
    let database = get_database(&driver);
    let mut connection = database.new_connection().unwrap();
    common::test_connection_get_table_schema(&mut connection);
}

#[test]
fn test_connection_get_statistic_names() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    assert!(connection.get_statistic_names().is_err());
}

#[test]
fn test_connection_get_statistics() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    assert!(connection.get_statistics(None, None, None, false).is_err());
}

#[test]
fn test_statement() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement(&mut statement);
}

#[test]
fn test_statement_prepare() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    common::test_statement_prepare(&statement);
}

#[test]
fn test_statement_set_substrait_plan() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    common::test_statement_set_substrait_plan(&statement);
}

#[test]
fn test_statement_get_parameters_schema() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    let error = statement.get_parameters_schema().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);

    statement.set_sql_query("select 42").unwrap();
    statement.prepare().unwrap();
    let got = statement.get_parameters_schema().unwrap();
    let fields: Vec<Field> = vec![];
    let actual = Schema::new(fields);
    assert_eq!(got, actual);
}

#[test]
fn test_statement_execute() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    common::test_statement_execute(&statement);
}

#[test]
fn test_statement_execute_update() {
    let driver = get_driver();
    let database = get_database(&driver);
    let mut connection = database.new_connection().unwrap();
    common::test_statement_execute_update(&mut connection);
}

#[test]
fn test_statement_execute_schema() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    let error = statement.execute_schema().unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_statement_execute_partitions() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    common::test_statement_execute_partitions(&statement);
}

#[test]
fn test_statement_cancel() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    let error = statement.cancel().unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_statement_bind() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    common::test_statement_bind(&statement);
}

#[test]
fn test_statement_bind_stream() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    common::test_statement_bind_stream(&statement);
}

#[test]
fn test_ingestion_roundtrip() {
    let driver = get_driver();
    let database = get_database(&driver);
    let mut connection = database.new_connection().unwrap();
    common::test_ingestion_roundtrip(&mut connection);
}
