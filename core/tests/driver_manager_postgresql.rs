use std::env;

use arrow::datatypes::{DataType, Field, Schema};

use adbc_core::driver_manager::{DriverManager, ManagedDatabase};
use adbc_core::options::{AdbcVersion, OptionConnection, OptionDatabase, OptionStatement};
use adbc_core::{error::Status, Driver, Optionable};
use adbc_core::{Connection, Database, Statement};

mod common;

fn get_driver() -> DriverManager {
    DriverManager::load_dynamic("adbc_driver_postgresql", None, AdbcVersion::V110).unwrap()
}

fn get_uri() -> String {
    env::var("TEST_ADBC_POSTGRESQL_URI")
        .expect("environment variable TEST_ADBC_POSTGRESQL_URI is not defined")
}

fn get_database(driver: &mut DriverManager) -> ManagedDatabase {
    let opts = [(OptionDatabase::Uri, get_uri().into())];
    driver.new_database_with_opts(opts).unwrap()
}

#[test]
fn test_driver() {
    let mut driver = get_driver();
    common::test_driver(&mut driver, &get_uri());
    // PostgreSQL's driver requires option "uri" to be set before creating a connection.
    assert!(driver.new_database().is_err());
}

#[test]
fn test_database() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    common::test_database(&mut database);
}

#[test]
fn test_database_get_set_option() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);

    let error = database.get_option_bytes(OptionDatabase::Uri).unwrap_err();
    assert_eq!(error.status, Status::NotFound);

    let error = database.get_option_string(OptionDatabase::Uri).unwrap_err();
    assert_eq!(error.status, Status::NotFound);

    let error = database.get_option_int(OptionDatabase::Uri).unwrap_err();
    assert_eq!(error.status, Status::NotFound);

    let error = database.get_option_double(OptionDatabase::Uri).unwrap_err();
    assert_eq!(error.status, Status::NotFound);

    database
        .set_option(OptionDatabase::Uri, "uri".into())
        .unwrap();

    let error = database
        .set_option(OptionDatabase::Uri, b"uri".into())
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = database
        .set_option(OptionDatabase::Uri, 42.into())
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = database
        .set_option(OptionDatabase::Uri, 42.0.into())
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_connection() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    common::test_connection(&mut connection);
}

#[test]
fn test_connection_get_set_option() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();

    let value = connection
        .get_option_string(OptionConnection::AutoCommit)
        .unwrap();
    assert_eq!(value, "true");

    let error = connection
        .get_option_bytes(OptionConnection::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);

    let error = connection
        .get_option_int(OptionConnection::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);

    let error = connection
        .get_option_double(OptionConnection::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);

    let value = connection
        .get_option_string(OptionConnection::CurrentSchema)
        .unwrap();
    assert_eq!(value, "public");

    connection
        .set_option(OptionConnection::CurrentSchema, "my_schema".into())
        .unwrap();

    let error = connection
        .set_option(OptionConnection::CurrentSchema, b"my_schema".into())
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = connection
        .set_option(OptionConnection::CurrentSchema, 42.into())
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = connection
        .set_option(OptionConnection::CurrentSchema, 42.0.into())
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_connection_cancel() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    connection.cancel().unwrap();
}

#[test]
fn test_connection_commit_rollback() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    common::test_connection_commit_rollback(&mut connection);
}

#[test]
fn test_connection_read_partition() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_read_partition(&connection);
}

#[test]
fn test_connection_get_table_types() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
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
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_get_info(&connection, 6);
}

#[test]
fn test_connection_get_objects() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_get_objects(&connection, 3, 3);
}

#[test]
fn test_connection_get_table_schema() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    common::test_connection_get_table_schema(&mut connection);
}

#[test]
fn test_connection_get_statistic_names() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let connection = database.new_connection().unwrap();
    let names = common::concat_reader(connection.get_statistic_names().unwrap());
    assert_eq!(names.num_columns(), 2);
    assert_eq!(names.num_rows(), 0);
}

#[test]
fn test_connection_get_statistics() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let connection = database.new_connection().unwrap();
    assert!(connection.get_statistics(None, None, None, false).is_err());
}

#[test]
fn test_statement() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement(&mut statement);
}

#[test]
fn test_statement_get_set_option() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    let error = statement
        .set_option(OptionStatement::TargetTable, b"table".into())
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = statement
        .set_option(OptionStatement::TargetTable, 42.0.into())
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    statement
        .set_option(OptionStatement::TargetTable, "table".into())
        .unwrap();

    statement
        .set_option(
            OptionStatement::Other("adbc.postgresql.batch_size_hint_bytes".into()),
            1024.into(),
        )
        .unwrap();

    let error = statement
        .get_option_bytes(OptionStatement::TargetTable)
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);

    let error = statement
        .get_option_double(OptionStatement::TargetTable)
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);

    let value = statement
        .get_option_string(OptionStatement::TargetTable)
        .unwrap();
    assert_eq!(value, "table");

    let value = statement
        .get_option_int(OptionStatement::Other(
            "adbc.postgresql.batch_size_hint_bytes".into(),
        ))
        .unwrap();
    assert_eq!(value, 1024);
}

#[test]
fn test_statement_prepare() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement_prepare(&mut statement);
}

#[test]
fn test_statement_set_substrait_plan() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement_set_substrait_plan(&mut statement);
}

#[test]
fn test_statement_get_parameters_schema() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    let error = statement.get_parameters_schema().unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_statement_execute() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement_execute(&mut statement);
}

#[test]
fn test_statement_execute_update() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    common::test_statement_execute_update(&mut connection);
}

#[test]
fn test_statement_execute_schema() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    let error = statement.execute_schema().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);

    statement.set_sql_query("select 42 as col").unwrap();
    let got = statement.execute_schema().unwrap();
    let actual = Schema::new(vec![Field::new("col", DataType::Int32, true)]);
    assert_eq!(got, actual);
}

#[test]
fn test_statement_execute_partitions() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement_execute_partitions(&mut statement);
}

#[test]
fn test_statement_cancel() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    statement.cancel().unwrap();
}

#[test]
fn test_statement_bind() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement_bind(&mut statement);
}

#[test]
fn test_statement_bind_stream() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement_bind_stream(&mut statement);
}

#[test]
fn test_ingestion_roundtrip() {
    let mut driver = get_driver();
    let mut database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    common::test_ingestion_roundtrip(&mut connection);
}
