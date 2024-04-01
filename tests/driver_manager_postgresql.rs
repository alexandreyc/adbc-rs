use arrow::datatypes::{DataType, Field, Schema};

use adbc_rs::driver_manager::{DriverManager, ManagedDatabase};
use adbc_rs::options::{AdbcVersion, ConnectionOptionKey, DatabaseOptionKey, StatementOptionKey};
use adbc_rs::{error::Status, Driver, Optionable};
use adbc_rs::{Connection, Database, Statement};

mod common;

const URI: &str = "postgres://al:@127.0.0.1:5432/postgres";

fn get_driver() -> DriverManager {
    DriverManager::load_dynamic("adbc_driver_postgresql", None, AdbcVersion::V110).unwrap()
}

fn get_database(driver: &DriverManager) -> ManagedDatabase {
    let opts = [(DatabaseOptionKey::Uri, URI.into())];
    driver.new_database_with_opts(opts.into_iter()).unwrap()
}

#[test]
fn test_driver() {
    let driver = get_driver();
    common::test_driver(&driver, URI);
    // PostgreSQL's driver requires option "uri" to be set before creating a connection.
    assert!(driver.new_database().is_err());
}

#[test]
fn test_database() {
    let driver = get_driver();
    let database = get_database(&driver);
    common::test_database(&database);
}

#[test]
fn test_database_get_set_option() {
    let driver = get_driver();
    let database = get_database(&driver);

    let error = database
        .get_option_bytes(DatabaseOptionKey::Uri)
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotFound);

    let error = database
        .get_option_string(DatabaseOptionKey::Uri)
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotFound);

    let error = database.get_option_int(DatabaseOptionKey::Uri).unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotFound);

    let error = database
        .get_option_double(DatabaseOptionKey::Uri)
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotFound);

    database
        .set_option(DatabaseOptionKey::Uri, "uri".into())
        .unwrap();

    let error = database
        .set_option(DatabaseOptionKey::Uri, b"uri".into())
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    let error = database
        .set_option(DatabaseOptionKey::Uri, 42.into())
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    let error = database
        .set_option(DatabaseOptionKey::Uri, 42.0.into())
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);
}

#[test]
fn test_connection() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    common::test_connection(&connection);
}

#[test]
fn test_connection_get_set_option() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();

    let value = connection
        .get_option_string(ConnectionOptionKey::AutoCommit)
        .unwrap();
    assert_eq!(value, "true");

    let error = connection
        .get_option_bytes(ConnectionOptionKey::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotFound);

    let error = connection
        .get_option_int(ConnectionOptionKey::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotFound);

    let error = connection
        .get_option_double(ConnectionOptionKey::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotFound);

    let value = connection
        .get_option_string(ConnectionOptionKey::CurrentSchema)
        .unwrap();
    assert_eq!(value, "public");

    connection
        .set_option(ConnectionOptionKey::CurrentSchema, "my_schema".into())
        .unwrap();

    let error = connection
        .set_option(ConnectionOptionKey::CurrentSchema, b"my_schema".into())
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    let error = connection
        .set_option(ConnectionOptionKey::CurrentSchema, 42.into())
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    let error = connection
        .set_option(ConnectionOptionKey::CurrentSchema, 42.0.into())
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);
}

#[test]
fn test_connection_cancel() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    connection.cancel().unwrap();
}

#[test]
fn test_connection_commit_rollback() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_commit_rollback(&connection);
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
    common::test_connection_get_table_types(
        &connection,
        &[
            "toast_table",
            "materialized_view",
            "table",
            "view",
            "partitioned_table",
            "foreign_table",
        ],
    );
}

#[test]
fn test_connection_get_info() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_get_info(&connection, 6);
}

#[test]
fn test_connection_get_objects() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_get_objects(&connection, 3, 3);
}

#[test]
fn test_connection_get_table_schema() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_get_table_schema(&connection);
}

#[test]
fn test_connection_get_statistics_name() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let names = common::concat_reader(connection.get_statistics_name().unwrap());
    assert_eq!(names.num_columns(), 2);
    assert_eq!(names.num_rows(), 0);
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
    let statement = connection.new_statement().unwrap();
    common::test_statement(&statement);
}

#[test]
fn test_statement_get_set_option() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    let error = statement
        .set_option(StatementOptionKey::TargetTable, b"table".into())
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    let error = statement
        .set_option(StatementOptionKey::TargetTable, 42.0.into())
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    statement
        .set_option(StatementOptionKey::TargetTable, "table".into())
        .unwrap();

    statement
        .set_option(
            StatementOptionKey::Other("adbc.postgresql.batch_size_hint_bytes".into()),
            1024.into(),
        )
        .unwrap();

    let error = statement
        .get_option_bytes(StatementOptionKey::TargetTable)
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotFound);

    let error = statement
        .get_option_double(StatementOptionKey::TargetTable)
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotFound);

    let value = statement
        .get_option_string(StatementOptionKey::TargetTable)
        .unwrap();
    assert_eq!(value, "table");

    let value = statement
        .get_option_int(StatementOptionKey::Other(
            "adbc.postgresql.batch_size_hint_bytes".into(),
        ))
        .unwrap();
    assert_eq!(value, 1024);
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
    assert_eq!(error.status.unwrap(), Status::NotImplemented);
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
    let connection = database.new_connection().unwrap();
    common::test_statement_execute_update(&connection);
}

#[test]
fn test_statement_execute_schema() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    let error = statement.execute_schema().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::InvalidState);

    statement.set_sql_query("select 42 as col").unwrap();
    let got = statement.execute_schema().unwrap();
    let actual = Schema::new(vec![Field::new("col", DataType::Int32, true)]);
    assert_eq!(got, actual);
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
    statement.cancel().unwrap();
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
    let connection = database.new_connection().unwrap();
    common::test_ingestion_roundtrip(&connection);
}
