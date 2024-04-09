use adbc_rs::{
    driver_manager::DriverManager,
    options::{AdbcVersion, IsolationLevel, OptionConnection, OptionDatabase},
    Database, Driver, Optionable,
};

const OPTION_STRING_LONG: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const OPTION_BYTES_LONG: &[u8] = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn get_driver() -> DriverManager {
    // TODO: make something more robust
    DriverManager::load_dynamic("adbc_rs", Some(b"DummyDriverInit"), AdbcVersion::V110).unwrap()
}

#[test]
fn test_database_options() {
    let driver = get_driver();

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
    let driver = get_driver();
    let database = driver.new_database().unwrap();

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
