use std::os::raw::{c_int, c_void};
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use adbc_rs::driver_manager::DriverManager;
use adbc_rs::options::{AdbcVersion, InfoCode, ObjectDepth, OptionValue};
use adbc_rs::{error::Status, Driver, Optionable};
use adbc_rs::{ffi, Connection, Database, Statement};

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

fn concat_reader(reader: impl RecordBatchReader) -> RecordBatch {
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    concat_batches(&schema, &batches).unwrap()
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
            "adbc.connection.autocommit", // TODO: use proper enum
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
fn test_connection_cancel() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();

    let error = connection.cancel().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);
}

#[test]
fn test_connection_commit_rollback() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();

    let error = connection.commit().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::InvalidState);

    let error = connection.rollback().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::InvalidState);

    connection
        .set_option(
            "adbc.connection.autocommit", // TODO: use proper enum
            OptionValue::String("false".into()),
        )
        .unwrap();

    connection.commit().unwrap();
    connection.rollback().unwrap();

    // TODO: implement a more involved test?
}

#[test]
fn test_connection_read_partition() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    assert!(connection.read_partition(b"").is_err());
}

#[test]
fn test_connection_get_table_types() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let table_types = concat_reader(connection.get_table_types().unwrap());
    assert_eq!(table_types.num_columns(), 1);
    assert_eq!(table_types.num_rows(), 2);
}

#[test]
fn test_connection_get_info() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();

    let info = concat_reader(connection.get_info(None).unwrap());
    assert_eq!(info.num_columns(), 2);
    assert_eq!(info.num_rows(), 5);

    let info = concat_reader(
        connection
            .get_info(Some(&[
                InfoCode::VendorName,
                InfoCode::DriverVersion,
                InfoCode::DriverName,
                InfoCode::VendorVersion,
            ]))
            .unwrap(),
    );
    assert_eq!(info.num_columns(), 2);
    assert_eq!(info.num_rows(), 4);
}

#[test]
fn test_connection_get_objects() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();

    let objects = concat_reader(
        connection
            .get_objects(ObjectDepth::All, None, None, None, None, None)
            .unwrap(),
    );
    assert_eq!(objects.num_rows(), 1);
    assert_eq!(objects.num_columns(), 2);

    let objects = concat_reader(
        connection
            .get_objects(
                ObjectDepth::All,
                None,
                None,
                None,
                Some(&["table", "view"]),
                None,
            )
            .unwrap(),
    );
    assert_eq!(objects.num_rows(), 1);
    assert_eq!(objects.num_columns(), 2);

    let objects = concat_reader(
        connection
            .get_objects(
                ObjectDepth::All,
                Some("my_catalog"),
                Some("my_schema"),
                Some("my_table"),
                Some(&["table", "view"]),
                Some("my_column"),
            )
            .unwrap(),
    );
    assert_eq!(objects.num_rows(), 0);
    assert_eq!(objects.num_columns(), 2);
}

#[test]
fn test_connection_get_table_schema() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();

    let schema = connection
        .get_table_schema(None, None, "sqlite_master")
        .unwrap();
    assert_eq!(schema.fields().len(), 5);

    let schema = connection.get_table_schema(None, None, "my_table");
    assert!(schema.is_err());

    // TODO: this panics because the SQLite C driver does not treat a non-null catalog (or schema)
    // as an error, instead it returns a zeroed schema...
    // See: SqliteConnectionGetTableSchema
    // let schema = connection.get_table_schema(Some("my_catalog"), None, "sqlite_master");
    // assert!(schema.is_err());
}

#[test]
fn test_connection_get_statistics_name() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    assert!(connection.get_statistics_name().is_err());
}

#[test]
fn test_connection_get_statistics() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    assert!(connection.get_statistics(None, None, None, false).is_err());
}

#[test]
fn test_statement() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    statement
        .set_option(
            "adbc.ingest.mode", // TODO: use proper enum
            OptionValue::String("adbc.ingest.mode.create".into()),
        )
        .unwrap();

    statement
        .set_option("unknown.key", OptionValue::String("unknown.value".into()))
        .unwrap_err();
}

#[test]
fn test_statement_prepare() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    let error = statement.prepare().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::InvalidState);
}

#[test]
fn test_statement_set_sql_query_and_prepare() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("select 42").unwrap();
    statement.prepare().unwrap();
}

#[test]
fn test_statement_set_substrait_plan() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    let error = statement.set_substrait_plan(b"").unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);
}

#[test]
fn test_statement_get_parameters_schema() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    let error = statement.get_parameters_schema().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::InvalidState);

    statement.set_sql_query("select 42").unwrap();
    statement.prepare().unwrap();
    statement.get_parameters_schema().unwrap();
}

#[test]
fn test_statement_execute() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    assert!(statement.execute().is_err());

    statement.set_sql_query("select 42").unwrap();
    let batch = concat_reader(statement.execute().unwrap());
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
}

#[test]
fn test_statement_execute_update() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    assert!(statement.execute_update().is_err());

    statement.set_sql_query("create table t(a int)").unwrap();
    statement.execute_update().unwrap();

    statement.set_sql_query("insert into t values(42)").unwrap();
    let rows_affected = statement.execute_update().unwrap();
    assert_eq!(rows_affected, 1);
}

#[test]
fn test_statement_execute_schema() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    let error = statement.execute_schema().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);
}

#[test]
fn test_statement_cancel() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    let error = statement.cancel().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);
}

#[test]
fn test_statement_bind() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    let batch = RecordBatch::try_new(schema, columns).unwrap();

    statement.bind(batch).unwrap();
}

#[test]
fn test_statement_bind_stream() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let reader = SingleBatchReader::new(batch);

    statement.bind_stream(Box::new(reader)).unwrap();
}

#[test]
fn test_ingestion_roundtrip() {
    let driver = get_driver();
    let mut database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    let batch = sample_batch();

    // Ingest
    statement
        .set_option(
            "adbc.ingest.target_table",
            OptionValue::String("my_table".into()),
        )
        .unwrap();

    statement.bind(batch.clone()).unwrap();
    statement.execute_update().unwrap();

    // Read back
    statement.set_sql_query("select * from my_table").unwrap();
    let batch_got = concat_reader(statement.execute().unwrap());

    assert_eq!(batch, batch_got);
}

fn sample_batch() -> RecordBatch {
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
        Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
    ];
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Float64, true),
        Field::new("c", DataType::Utf8, true),
    ]);
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl SingleBatchReader {
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }
}

impl Iterator for SingleBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// TODOs
// - Test `get_option_*` (needs an 1.1.0 driver)
// - Test non-string setting options (needs an 1.1.0 driver)
