use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug, hash::Hash};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use crate::{
    error::{Error, Result, Status},
    options::{
        InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
    },
    Connection, Database, Driver, Optionable, Statement,
};

pub struct SingleBatchReader {
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

fn set_option<T>(options: &mut HashMap<T, OptionValue>, key: T, value: OptionValue) -> Result<()>
where
    T: Eq + Hash,
{
    options.insert(key, value);
    Ok(())
}

fn get_option_bytes<T>(options: &HashMap<T, OptionValue>, key: T, kind: &str) -> Result<Vec<u8>>
where
    T: Eq + Hash + Debug,
{
    let value = options.get(&key);
    match value {
        None => Err(Error::with_message_and_status(
            &format!("Unrecognized {} option: {:?}", kind, key),
            Status::NotFound,
        )),
        Some(value) => match value {
            OptionValue::Bytes(value) => Ok(value.clone()),
            _ => Err(Error::with_message_and_status(
                &format!("Incorrect value for {} option: {:?}", kind, key),
                Status::InvalidData,
            )),
        },
    }
}

fn get_option_double<T>(options: &HashMap<T, OptionValue>, key: T, kind: &str) -> Result<f64>
where
    T: Eq + Hash + Debug,
{
    let value = options.get(&key);
    match value {
        None => Err(Error::with_message_and_status(
            &format!("Unrecognized {} option: {:?}", kind, key),
            Status::NotFound,
        )),
        Some(value) => match value {
            OptionValue::Double(value) => Ok(*value),
            _ => Err(Error::with_message_and_status(
                &format!("Incorrect value for {} option: {:?}", kind, key),
                Status::InvalidData,
            )),
        },
    }
}

fn get_option_int<T>(options: &HashMap<T, OptionValue>, key: T, kind: &str) -> Result<i64>
where
    T: Eq + Hash + Debug,
{
    let value = options.get(&key);
    match value {
        None => Err(Error::with_message_and_status(
            &format!("Unrecognized {} option: {:?}", kind, key),
            Status::NotFound,
        )),
        Some(value) => match value {
            OptionValue::Int(value) => Ok(*value),
            _ => Err(Error::with_message_and_status(
                &format!("Incorrect value for {} option: {:?}", kind, key),
                Status::InvalidData,
            )),
        },
    }
}

fn get_option_string<T>(options: &HashMap<T, OptionValue>, key: T, kind: &str) -> Result<String>
where
    T: Eq + Hash + Debug,
{
    let value = options.get(&key);
    match value {
        None => Err(Error::with_message_and_status(
            &format!("Unrecognized {} option: {:?}", kind, key),
            Status::NotFound,
        )),
        Some(value) => match value {
            OptionValue::String(value) => Ok(value.clone()),
            _ => Err(Error::with_message_and_status(
                &format!("Incorrect value for {} option: {:?}", kind, key),
                Status::InvalidData,
            )),
        },
    }
}

/// A dummy driver mainly used for example and testing.
///
/// It contains:
/// - Two table types: `table` and `view`
/// - One catalog: `default`
/// - One database schema: `default`
/// - One table: `default`
#[derive(Default)]
pub struct DummyDriver {}

impl Driver for DummyDriver {
    type DatabaseType = DummyDatabase;

    fn new_database(&self) -> Result<Self::DatabaseType> {
        self.new_database_with_opts([].into_iter())
    }

    fn new_database_with_opts(
        &self,
        opts: impl Iterator<Item = (<Self::DatabaseType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = Self::DatabaseType {
            options: HashMap::new(),
        };
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

pub struct DummyDatabase {
    options: HashMap<OptionDatabase, OptionValue>,
}

impl Optionable for DummyDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        set_option(&mut self.options, key, value)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        get_option_bytes(&self.options, key, "database")
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        get_option_double(&self.options, key, "database")
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        get_option_int(&self.options, key, "database")
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_option_string(&self.options, key, "database")
    }
}

impl Database for DummyDatabase {
    type ConnectionType = DummyConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.new_connection_with_opts([].into_iter())
    }

    fn new_connection_with_opts(
        &self,
        opts: impl Iterator<Item = (<Self::ConnectionType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = Self::ConnectionType {
            options: HashMap::new(),
        };
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

pub struct DummyConnection {
    options: HashMap<OptionConnection, OptionValue>,
}

impl Optionable for DummyConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        set_option(&mut self.options, key, value)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        get_option_bytes(&self.options, key, "connection")
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        get_option_double(&self.options, key, "connection")
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        get_option_int(&self.options, key, "connection")
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_option_string(&self.options, key, "connection")
    }
}

impl Connection for DummyConnection {
    type StatementType = DummyStatement;

    fn new_statement(&self) -> Result<Self::StatementType> {
        Ok(Self::StatementType {
            options: HashMap::new(),
        })
    }

    fn cancel(&self) -> Result<()> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    fn commit(&self) -> Result<()> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    #[allow(refining_impl_trait)]
    fn get_info(&self, _codes: Option<&[InfoCode]>) -> Result<ArrowArrayStreamReader> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    #[allow(refining_impl_trait)]
    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<&[&str]>,
        _column_name: Option<&str>,
    ) -> Result<ArrowArrayStreamReader> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    #[allow(refining_impl_trait)]
    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<ArrowArrayStreamReader> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    #[allow(refining_impl_trait)]
    fn get_statistics_name(&self) -> Result<ArrowArrayStreamReader> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<arrow::datatypes::Schema> {
        let catalog = catalog.unwrap_or("default");
        let db_schema = db_schema.unwrap_or("default");

        if catalog == "default" && db_schema == "default" && table_name == "default" {
            let schema = Schema::new(vec![
                Field::new("a", DataType::UInt32, true),
                Field::new("b", DataType::Float64, false),
                Field::new("c", DataType::Utf8, true),
            ]);
            Ok(schema)
        } else {
            Err(Error::with_message_and_status(
                &format!(
                    "Table {}.{}.{} does not exist",
                    catalog, db_schema, table_name
                ),
                Status::NotFound,
            ))
        }
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let array = Arc::new(StringArray::from(vec!["table", "view"]));
        let batch = RecordBatch::try_new(schema, vec![array])?;
        let reader = SingleBatchReader::new(batch);
        Ok(reader)
    }

    #[allow(refining_impl_trait)]
    fn read_partition(&self, _partition: &[u8]) -> Result<ArrowArrayStreamReader> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    fn rollback(&self) -> Result<()> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }
}

pub struct DummyStatement {
    options: HashMap<OptionStatement, OptionValue>,
}

impl Optionable for DummyStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        set_option(&mut self.options, key, value)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        get_option_bytes(&self.options, key, "statement")
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        get_option_double(&self.options, key, "statement")
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        get_option_int(&self.options, key, "statement")
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_option_string(&self.options, key, "statement")
    }
}

impl Statement for DummyStatement {
    fn bind(&self, _batch: arrow::array::RecordBatch) -> Result<()> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    fn bind_stream(&self, _reader: Box<dyn arrow::array::RecordBatchReader + Send>) -> Result<()> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    fn cancel(&self) -> Result<()> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    #[allow(refining_impl_trait)]
    fn execute(&self) -> Result<ArrowArrayStreamReader> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    fn execute_partitions(&self) -> Result<crate::Partitions> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    fn execute_schema(&self) -> Result<arrow::datatypes::Schema> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    fn execute_update(&self) -> Result<i64> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    fn get_parameters_schema(&self) -> Result<arrow::datatypes::Schema> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    fn prepare(&self) -> Result<()> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    fn set_sql_query(&self, _query: &str) -> Result<()> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }

    fn set_substrait_plan(&self, _plan: &[u8]) -> Result<()> {
        Err(Error::with_message_and_status("", Status::NotImplemented))
    }
}

crate::export_driver!(DummyDriverInit, DummyDriver);
