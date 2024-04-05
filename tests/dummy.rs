use adbc_rs::dummy::DummyDriver;
use adbc_rs::options::{
    IngestMode, IsolationLevel, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement,
};
use adbc_rs::{error::Status, Driver, Optionable};
use adbc_rs::{Connection, Database, Statement};

pub mod common;

#[test]
fn test_driver() {
    let driver = DummyDriver {};

    let database = driver.new_database();
    database.unwrap();

    let opts = [
        (OptionDatabase::Username, "Alice".into()),
        (OptionDatabase::Password, "VerySecret".into()),
    ];
    let database = driver.new_database_with_opts(opts.into_iter()).unwrap();

    let value = database
        .get_option_string(OptionDatabase::Username)
        .unwrap();
    assert_eq!(value, "Alice");
}

#[test]
fn test_database() {
    let driver = DummyDriver {};
    let database = driver.new_database().unwrap();

    let connection = database.new_connection();
    connection.unwrap();

    let opts = [(
        OptionConnection::IsolationLevel,
        IsolationLevel::Linearizable.into(),
    )];
    let connection = database.new_connection_with_opts(opts.into_iter()).unwrap();

    let value = connection
        .get_option_string(OptionConnection::IsolationLevel)
        .unwrap();
    let actual: String = IsolationLevel::Linearizable.into();
    assert_eq!(value, actual);
}

#[test]
fn test_database_optionable() {
    let driver = DummyDriver {};
    let mut database = driver.new_database().unwrap();

    database
        .set_option(OptionDatabase::Uri, "uri://".into())
        .unwrap();
    let value = database.get_option_string(OptionDatabase::Uri).unwrap();
    assert_eq!(value, "uri://");

    database
        .set_option(OptionDatabase::Uri, b"uri://".into())
        .unwrap();
    let value = database.get_option_bytes(OptionDatabase::Uri).unwrap();
    assert_eq!(value, b"uri://");

    database
        .set_option(OptionDatabase::Username, 3.14.into())
        .unwrap();
    let value = database
        .get_option_double(OptionDatabase::Username)
        .unwrap();
    assert_eq!(value, 3.14);

    database
        .set_option(OptionDatabase::Username, 42.into())
        .unwrap();
    let value = database.get_option_int(OptionDatabase::Username).unwrap();
    assert_eq!(value, 42);

    let err = database
        .get_option_int(OptionDatabase::Other("unset_option".into()))
        .unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotFound);
}

#[test]
fn test_connection() {
    let driver = DummyDriver {};
    let database = driver.new_database().unwrap();
    let connection = database.new_connection().unwrap();

    let statement = connection.new_statement();
    statement.unwrap();

    let err = connection.cancel().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = connection.commit().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = connection.get_info(None).unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = connection
        .get_objects(ObjectDepth::All, None, None, None, None, None)
        .unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = connection
        .get_statistics(None, None, None, false)
        .unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = connection.get_statistics_name().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = connection
        .get_table_schema(None, None, "my_table")
        .unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = connection.get_table_types().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = connection.read_partition(b"").unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = connection.rollback().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);
}

#[test]
fn test_connection_optionable() {
    let driver = DummyDriver {};
    let database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();

    connection
        .set_option(OptionConnection::AutoCommit, "true".into())
        .unwrap();
    let value = connection
        .get_option_string(OptionConnection::AutoCommit)
        .unwrap();
    assert_eq!(value, "true");

    connection
        .set_option(OptionConnection::CurrentCatalog, 42.into())
        .unwrap();
    let value = connection
        .get_option_int(OptionConnection::CurrentCatalog)
        .unwrap();
    assert_eq!(value, 42);

    connection
        .set_option(OptionConnection::CurrentSchema, 3.14.into())
        .unwrap();
    let value = connection
        .get_option_double(OptionConnection::CurrentSchema)
        .unwrap();
    assert_eq!(value, 3.14);

    connection
        .set_option(OptionConnection::Other("other_option".into()), b"".into())
        .unwrap();
    let value = connection
        .get_option_bytes(OptionConnection::Other("other_option".into()))
        .unwrap();
    assert_eq!(value, b"");

    let err = connection
        .get_option_bytes(OptionConnection::Other("unset_option".into()))
        .unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotFound);
}

#[test]
fn test_statement() {
    let driver = DummyDriver {};
    let database = driver.new_database().unwrap();
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    let err = statement.cancel().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = statement.execute().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = statement.execute_partitions().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = statement.execute_partitions().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = statement.execute_schema().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = statement.execute_update().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = statement.get_parameters_schema().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = statement.prepare().unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = statement.set_sql_query("").unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let err = statement.set_substrait_plan(b"").unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let batch = common::sample_batch();
    let err = statement.bind(batch).unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);

    let reader = Box::new(common::SingleBatchReader::new(common::sample_batch()));
    let err = statement.bind_stream(reader).unwrap_err();
    assert_eq!(err.status.unwrap(), Status::NotImplemented);
}

#[test]
fn test_statement_optionable() {
    let driver = DummyDriver {};
    let database = driver.new_database().unwrap();
    let connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    statement
        .set_option(OptionStatement::Incremental, "true".into())
        .unwrap();
    let value = statement
        .get_option_string(OptionStatement::Incremental)
        .unwrap();
    assert_eq!(value, "true");

    statement
        .set_option(OptionStatement::MaxProgress, 42.into())
        .unwrap();
    let value = statement
        .get_option_int(OptionStatement::MaxProgress)
        .unwrap();
    assert_eq!(value, 42);

    statement
        .set_option(OptionStatement::Progress, 3.14.into())
        .unwrap();
    let value = statement
        .get_option_double(OptionStatement::Progress)
        .unwrap();
    assert_eq!(value, 3.14);

    statement
        .set_option(OptionStatement::IngestMode, IngestMode::Append.into())
        .unwrap();
    let value = statement
        .get_option_string(OptionStatement::IngestMode)
        .unwrap();
    let actual: String = IngestMode::Append.into();
    assert_eq!(value, actual);
}
