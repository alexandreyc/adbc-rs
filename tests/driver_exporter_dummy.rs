use std::ops::Deref;

use adbc_rs::driver_manager::{ManagedConnection, ManagedDatabase, ManagedStatement};
use adbc_rs::dummy::{DummyConnection, DummyDatabase, DummyStatement};

use adbc_rs::options::InfoCode;
use adbc_rs::{
    driver_manager::DriverManager,
    dummy::DummyDriver,
    options::{
        AdbcVersion, IngestMode, IsolationLevel, OptionConnection, OptionDatabase, OptionStatement,
    },
    schemas, Connection, Database, Driver, Optionable,
};

pub mod common;

const OPTION_STRING_LONG: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const OPTION_BYTES_LONG: &[u8] = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn get_exported() -> (
    DriverManager,
    ManagedDatabase,
    ManagedConnection,
    ManagedStatement,
) {
    // TODO: make something more robust
    let driver =
        DriverManager::load_dynamic("adbc_rs", Some(b"DummyDriverInit"), AdbcVersion::V110)
            .unwrap();
    let database = driver.new_database().unwrap();
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    (driver, database, connection, statement)
}

fn get_native() -> (DummyDriver, DummyDatabase, DummyConnection, DummyStatement) {
    let driver = DummyDriver {};
    let database = driver.new_database().unwrap();
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    (driver, database, connection, statement)
}

#[test]
fn test_database_options() {
    let (driver, _, _, _) = get_exported();

    // Pre-init options.
    let options = [
        (OptionDatabase::Username, "Alice".into()),
        (OptionDatabase::Password, 42.into()),
        (OptionDatabase::Uri, 3.14.into()),
        (OptionDatabase::Other("pre.bytes".into()), b"Hello".into()),
        (
            OptionDatabase::Other("pre.string.long".into()),
            OPTION_STRING_LONG.into(),
        ),
        (
            OptionDatabase::Other("pre.bytes.long".into()),
            OPTION_BYTES_LONG.into(),
        ),
    ];

    let mut database = driver.new_database_with_opts(options.into_iter()).unwrap();

    let value = database
        .get_option_string(OptionDatabase::Username)
        .unwrap();
    assert_eq!(value, "Alice");

    let value = database.get_option_int(OptionDatabase::Password).unwrap();
    assert_eq!(value, 42);

    let value = database.get_option_double(OptionDatabase::Uri).unwrap();
    assert_eq!(value, 3.14);

    let value = database
        .get_option_bytes(OptionDatabase::Other("pre.bytes".into()).into())
        .unwrap();
    assert_eq!(value, b"Hello");

    let value = database
        .get_option_string(OptionDatabase::Other("pre.string.long".into()).into())
        .unwrap();
    assert_eq!(value, OPTION_STRING_LONG);

    let value = database
        .get_option_bytes(OptionDatabase::Other("pre.bytes.long".into()).into())
        .unwrap();
    assert_eq!(value, OPTION_BYTES_LONG);

    // Post-init options.
    database
        .set_option(OptionDatabase::Other("post.string".into()), "Bob".into())
        .unwrap();
    let value = database
        .get_option_string(OptionDatabase::Other("post.string".into()))
        .unwrap();
    assert_eq!(value, "Bob");

    database
        .set_option(OptionDatabase::Other("post.int".into()), 1337.into())
        .unwrap();
    let value = database
        .get_option_int(OptionDatabase::Other("post.int".into()))
        .unwrap();
    assert_eq!(value, 1337);

    database
        .set_option(OptionDatabase::Other("post.double".into()), 1.41.into())
        .unwrap();
    let value = database
        .get_option_double(OptionDatabase::Other("post.double".into()))
        .unwrap();
    assert_eq!(value, 1.41);

    database
        .set_option(OptionDatabase::Other("post.bytes".into()), b"Bye".into())
        .unwrap();
    let value = database
        .get_option_bytes(OptionDatabase::Other("post.bytes".into()))
        .unwrap();
    assert_eq!(value, b"Bye");

    database
        .set_option(
            OptionDatabase::Other("post.string.long".into()),
            OPTION_STRING_LONG.into(),
        )
        .unwrap();
    let value = database
        .get_option_string(OptionDatabase::Other("post.string.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_STRING_LONG);

    database
        .set_option(
            OptionDatabase::Other("post.bytes.long".into()),
            OPTION_BYTES_LONG.into(),
        )
        .unwrap();
    let value = database
        .get_option_bytes(OptionDatabase::Other("post.bytes.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_BYTES_LONG);
}

#[test]
fn test_connection_options() {
    let (_, database, _, _) = get_exported();

    // Pre-init options
    let options = [
        (OptionConnection::CurrentCatalog, "Alice".into()),
        (OptionConnection::AutoCommit, 42.into()),
        (OptionConnection::CurrentSchema, 3.14.into()),
        (
            OptionConnection::IsolationLevel,
            IsolationLevel::Linearizable.into(),
        ),
        (OptionConnection::Other("pre.bytes".into()), b"Hello".into()),
        (OptionConnection::ReadOnly, OPTION_STRING_LONG.into()),
        (
            OptionConnection::Other("pre.bytes.long".into()),
            OPTION_BYTES_LONG.into(),
        ),
    ];
    let mut connection = database
        .new_connection_with_opts(options.into_iter())
        .unwrap();

    let value = connection
        .get_option_string(OptionConnection::CurrentCatalog)
        .unwrap();
    assert_eq!(value, "Alice");

    let value = connection
        .get_option_int(OptionConnection::AutoCommit)
        .unwrap();
    assert_eq!(value, 42);

    let value = connection
        .get_option_double(OptionConnection::CurrentSchema)
        .unwrap();
    assert_eq!(value, 3.14);

    let value = connection
        .get_option_string(OptionConnection::IsolationLevel)
        .unwrap();
    assert_eq!(value, Into::<String>::into(IsolationLevel::Linearizable));

    let value = connection
        .get_option_bytes(OptionConnection::Other("pre.bytes".into()))
        .unwrap();
    assert_eq!(value, b"Hello");

    let value = connection
        .get_option_string(OptionConnection::ReadOnly)
        .unwrap();
    assert_eq!(value, OPTION_STRING_LONG);

    let value = connection
        .get_option_bytes(OptionConnection::Other("pre.bytes.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_BYTES_LONG);

    // Post-init options
    connection
        .set_option(OptionConnection::AutoCommit, "true".into())
        .unwrap();
    let value = connection
        .get_option_string(OptionConnection::AutoCommit)
        .unwrap();
    assert_eq!(value, "true");

    connection
        .set_option(OptionConnection::CurrentCatalog, 1337.into())
        .unwrap();
    let value = connection
        .get_option_int(OptionConnection::CurrentCatalog)
        .unwrap();
    assert_eq!(value, 1337);

    connection
        .set_option(OptionConnection::CurrentSchema, 1.41.into())
        .unwrap();
    let value = connection
        .get_option_double(OptionConnection::CurrentSchema)
        .unwrap();
    assert_eq!(value, 1.41);

    connection
        .set_option(OptionConnection::Other("post.bytes".into()), b"Bye".into())
        .unwrap();
    let value = connection
        .get_option_bytes(OptionConnection::Other("post.bytes".into()))
        .unwrap();
    assert_eq!(value, b"Bye");

    connection
        .set_option(
            OptionConnection::Other("post.string.long".into()),
            OPTION_STRING_LONG.into(),
        )
        .unwrap();
    let value = connection
        .get_option_string(OptionConnection::Other("post.string.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_STRING_LONG);

    connection
        .set_option(
            OptionConnection::Other("post.bytes.long".into()),
            OPTION_BYTES_LONG.into(),
        )
        .unwrap();
    let value = connection
        .get_option_bytes(OptionConnection::Other("post.bytes.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_BYTES_LONG);
}

#[test]
fn test_statement_options() {
    let (_, _, _, mut statement) = get_exported();

    statement
        .set_option(OptionStatement::Incremental, "true".into())
        .unwrap();
    let value = statement
        .get_option_string(OptionStatement::Incremental)
        .unwrap();
    assert_eq!(value, "true");

    statement
        .set_option(OptionStatement::TargetTable, 42.into())
        .unwrap();
    let value = statement
        .get_option_int(OptionStatement::TargetTable)
        .unwrap();
    assert_eq!(value, 42);

    statement
        .set_option(OptionStatement::MaxProgress, 3.14.into())
        .unwrap();
    let value = statement
        .get_option_double(OptionStatement::MaxProgress)
        .unwrap();
    assert_eq!(value, 3.14);

    statement
        .set_option(OptionStatement::Other("bytes".into()), b"Hello".into())
        .unwrap();
    let value = statement
        .get_option_bytes(OptionStatement::Other("bytes".into()))
        .unwrap();
    assert_eq!(value, b"Hello");

    statement
        .set_option(OptionStatement::IngestMode, IngestMode::CreateAppend.into())
        .unwrap();
    let value = statement
        .get_option_string(OptionStatement::IngestMode)
        .unwrap();
    assert_eq!(value, Into::<String>::into(IngestMode::CreateAppend));

    statement
        .set_option(
            OptionStatement::Other("bytes.long".into()),
            OPTION_BYTES_LONG.into(),
        )
        .unwrap();
    let value = statement
        .get_option_bytes(OptionStatement::Other("bytes.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_BYTES_LONG);

    statement
        .set_option(
            OptionStatement::Other("string.long".into()),
            OPTION_STRING_LONG.into(),
        )
        .unwrap();
    let value = statement
        .get_option_string(OptionStatement::Other("string.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_STRING_LONG);
}

#[test]
fn test_connection_get_table_types() {
    let (_, _, exported_connection, _) = get_exported();
    let (_, _, native_connection, _) = get_native();

    let exported_table_types =
        common::concat_reader(exported_connection.get_table_types().unwrap());
    let native_table_types = common::concat_reader(native_connection.get_table_types().unwrap());

    assert_eq!(
        exported_table_types.schema(),
        *schemas::GET_TABLE_TYPES.deref()
    );
    assert_eq!(exported_table_types, native_table_types);
}

#[test]
fn test_connection_get_table_schema() {
    let (_, _, exported_connection, _) = get_exported();
    let (_, _, native_connection, _) = get_native();

    let exported_schema = exported_connection
        .get_table_schema(Some("default"), Some("default"), "default")
        .unwrap();
    let native_schema = native_connection
        .get_table_schema(Some("default"), Some("default"), "default")
        .unwrap();

    assert_eq!(exported_schema, native_schema);
}

#[test]
fn test_connection_get_info() {
    let (_, _, exported_connection, _) = get_exported();
    let (_, _, native_connection, _) = get_native();

    let exported_info = common::concat_reader(exported_connection.get_info(None).unwrap());
    let native_info = common::concat_reader(native_connection.get_info(None).unwrap());
    assert_eq!(exported_info.schema(), *schemas::GET_INFO_SCHEMA.deref());
    assert_eq!(exported_info, native_info);

    let exported_info = common::concat_reader(
        exported_connection
            .get_info(Some(vec![
                InfoCode::DriverAdbcVersion,
                InfoCode::DriverName,
            ]))
            .unwrap(),
    );
    let native_info = common::concat_reader(
        native_connection
            .get_info(Some(vec![
                InfoCode::DriverAdbcVersion,
                InfoCode::DriverName,
            ]))
            .unwrap(),
    );
    assert_eq!(exported_info.schema(), *schemas::GET_INFO_SCHEMA.deref());
    assert_eq!(exported_info, native_info);
}
