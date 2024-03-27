use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use adbc_rs::driver_manager::{DriverManager, ManagedDatabase};
use adbc_rs::options::{
    AdbcVersion, ConnectionOptionKey, DatabaseOptionKey, InfoCode, ObjectDepth, OptionValue,
    StatementOptionKey,
};
use adbc_rs::{error::Status, Driver, Optionable};
use adbc_rs::{Connection, Database, Statement};

fn get_driver() -> DriverManager {
    // DriverManager::load_static(
    //     &(SqliteDriverInit as ffi::FFI_AdbcDriverInitFunc),
    //     AdbcVersion::V100,
    // )
    // .unwrap()
    DriverManager::load_dynamic("adbc_driver_postgresql", None, AdbcVersion::V110).unwrap()
}

fn get_database<'a>(driver: &'a DriverManager) -> ManagedDatabase<'a> {
    let opts = [(
        DatabaseOptionKey::Uri,
        OptionValue::String("postgres://al:@127.0.0.1:5432/postgres".into()),
    )];
    driver.new_database_with_opts(opts.into_iter()).unwrap()
}

fn concat_reader(reader: impl RecordBatchReader) -> RecordBatch {
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    concat_batches(&schema, &batches).unwrap()
}

#[test]
fn test_database() {
    let driver = get_driver();
    let database = get_database(&driver);

    assert!(database.new_connection().is_ok());

    // `adbc.connection.autocommit` can only be set after init (not true for
    // PostgreSQL).
    /*
    let opts = [(
        ConnectionOptionKey::AutoCommit,
        OptionValue::String("true".into()),
    )];
    assert!(database.new_connection_with_opts(opts.into_iter()).is_err());
    */

    // Unknown connection option
    let opts = [(
        ConnectionOptionKey::Other("unknown".into()),
        OptionValue::String("".into()),
    )];
    assert!(database.new_connection_with_opts(opts.into_iter()).is_err());
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
        .set_option(DatabaseOptionKey::Uri, OptionValue::String("uri".into()))
        .unwrap();

    let error = database
        .set_option(DatabaseOptionKey::Uri, OptionValue::Bytes(b"uri".into()))
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    let error = database
        .set_option(DatabaseOptionKey::Uri, OptionValue::Int(42))
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    let error = database
        .set_option(DatabaseOptionKey::Uri, OptionValue::Double(42.0))
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);
}

#[test]
fn test_connection() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();

    assert!(connection
        .set_option(
            ConnectionOptionKey::AutoCommit,
            OptionValue::String("true".into())
        )
        .is_ok());

    // Unknown connection option
    assert!(connection
        .set_option(
            ConnectionOptionKey::Other("unknown".into()),
            OptionValue::String("".into())
        )
        .is_err());

    assert!(connection.new_statement().is_ok());
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
        .set_option(
            ConnectionOptionKey::CurrentSchema,
            OptionValue::String("my_schema".into()),
        )
        .unwrap();

    let error = connection
        .set_option(
            ConnectionOptionKey::CurrentSchema,
            OptionValue::Bytes(b"my_schema".into()),
        )
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    let error = connection
        .set_option(ConnectionOptionKey::CurrentSchema, OptionValue::Int(42))
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    let error = connection
        .set_option(
            ConnectionOptionKey::CurrentSchema,
            OptionValue::Double(42.0),
        )
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

    let error = connection.commit().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::InvalidState);

    let error = connection.rollback().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::InvalidState);

    connection
        .set_option(
            ConnectionOptionKey::AutoCommit,
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
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    assert!(connection.read_partition(b"").is_err());
}

#[test]
fn test_connection_get_table_types() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let table_types = concat_reader(connection.get_table_types().unwrap());
    assert_eq!(table_types.num_columns(), 1);
    assert_eq!(table_types.num_rows(), 6);
}

#[test]
fn test_connection_get_info() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();

    let info = concat_reader(connection.get_info(None).unwrap());
    assert_eq!(info.num_columns(), 2);
    assert_eq!(info.num_rows(), 6);

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
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();

    let objects = concat_reader(
        connection
            .get_objects(ObjectDepth::All, None, None, None, None, None)
            .unwrap(),
    );
    assert_eq!(objects.num_rows(), 3);
    assert_eq!(objects.num_columns(), 2);

    let objects = connection
        .get_objects(
            ObjectDepth::All,
            None,
            None,
            None,
            Some(&["table", "view"]),
            None,
        )
        .unwrap();
    let objects = concat_reader(objects);
    assert_eq!(objects.num_rows(), 3);
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
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    statement
        .set_sql_query("create temp table my_table(a int, b text);")
        .unwrap();
    statement.execute_update().unwrap();
    drop(statement);

    let schema_got = connection.get_table_schema(None, None, "my_table").unwrap();
    let schema_expected = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    assert_eq!(schema_got, schema_expected);

    let schema = connection.get_table_schema(None, None, "nonexistent_table");
    assert!(schema.is_err());
}

#[test]
fn test_connection_get_statistics_name() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let names = concat_reader(connection.get_statistics_name().unwrap());
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

    statement
        .set_option(
            StatementOptionKey::IngestMode,
            OptionValue::String("adbc.ingest.mode.create".into()),
        )
        .unwrap();

    statement
        .set_option(
            StatementOptionKey::Other("unknown".into()),
            OptionValue::String("unknown.value".into()),
        )
        .unwrap_err();
}

#[test]
fn test_statement_get_set_option() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    let error = statement
        .set_option(
            StatementOptionKey::TargetTable,
            OptionValue::Bytes(b"table".into()),
        )
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    let error = statement
        .set_option(StatementOptionKey::TargetTable, OptionValue::Double(42.0))
        .unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    statement
        .set_option(
            StatementOptionKey::TargetTable,
            OptionValue::String("table".into()),
        )
        .unwrap();

    statement
        .set_option(
            StatementOptionKey::Other("adbc.postgresql.batch_size_hint_bytes".into()),
            OptionValue::Int(1024),
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
    let error = statement.prepare().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::InvalidState);
}

#[test]
fn test_statement_set_sql_query_and_prepare() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    statement.set_sql_query("select 42").unwrap();
    statement.prepare().unwrap();
}

#[test]
fn test_statement_set_substrait_plan() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    let error = statement.set_substrait_plan(b"").unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);
}

#[test]
fn test_statement_get_parameters_schema() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    let error = statement.get_parameters_schema().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);

    statement.set_sql_query("select 42").unwrap();
    statement.prepare().unwrap();
    // statement.get_parameters_schema().unwrap();
}

#[test]
fn test_statement_execute() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    assert!(statement.execute().is_err());

    statement.set_sql_query("select 42").unwrap();
    let batch = concat_reader(statement.execute().unwrap());
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
}

#[test]
fn test_statement_execute_update() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    assert!(statement.execute_update().is_err());

    statement
        .set_sql_query("create temp table t(a int)")
        .unwrap();
    statement.execute_update().unwrap();

    statement.set_sql_query("insert into t values(42)").unwrap();
    let rows_affected = statement.execute_update().unwrap();
    assert_eq!(rows_affected, -1);
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
    let schema_got = statement.execute_schema().unwrap();
    let schema_expected = Schema::new(vec![Field::new("col", DataType::Int32, true)]);
    assert_eq!(schema_got, schema_expected);
}

#[test]
fn test_statement_execute_partitions() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    let error = statement.execute_partitions().unwrap_err();
    assert_eq!(error.status.unwrap(), Status::NotImplemented);
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

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    let batch = RecordBatch::try_new(schema, columns).unwrap();

    statement.bind(batch).unwrap();
}

#[test]
fn test_statement_bind_stream() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let reader = SingleBatchReader::new(batch);

    statement.bind_stream(Box::new(reader)).unwrap();
}

#[test]
fn test_ingestion_roundtrip() {
    let driver = get_driver();
    let database = get_database(&driver);
    let connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();

    let batch = sample_batch();

    // Ingest
    statement
        .set_option(
            StatementOptionKey::TargetTable,
            OptionValue::String("my_table".into()),
        )
        .unwrap();

    statement.bind(batch.clone()).unwrap();
    statement.execute_update().unwrap();

    // Read back
    statement.set_sql_query("select * from my_table").unwrap();
    let batch_got = concat_reader(statement.execute().unwrap());
    assert_eq!(batch, batch_got);

    // Delete table.
    // Note that the table might not be deleted when the previous select fail.
    // In that case, the table must be manually deleted.
    statement.set_sql_query("drop table my_table").unwrap();
    statement.execute_update().unwrap();
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
