use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug, hash::Hash};

use arrow::array::{
    Array, BooleanArray, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
    UInt32Array, UnionArray,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
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

#[derive(Debug)]
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

    fn get_info(&self, _codes: Option<Vec<InfoCode>>) -> Result<impl RecordBatchReader> {
        let string_value_array = StringArray::from(vec!["MyVendorName"]);
        let bool_value_array = BooleanArray::from(vec![true]);
        let int64_value_array = Int64Array::from(vec![42]);
        let int32_bitmask_array = Int32Array::from(vec![1337]);
        let string_list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            Arc::new(StringArray::from(vec!["Hello", "World"])),
            None,
        );

        let int32_to_int32_list_map_array = MapArray::try_new(
            Arc::new(Field::new_struct(
                "entries",
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new_list("value", Field::new_list_field(DataType::Int32, true), true),
                ],
                false,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            StructArray::new(
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new_list("value", Field::new_list_field(DataType::Int32, true), true),
                ]
                .into(),
                vec![
                    Arc::new(Int32Array::from(vec![42, 1337])),
                    Arc::new(ListArray::new(
                        Arc::new(Field::new("item", DataType::Int32, true)),
                        OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6])),
                        Arc::new(Int32Array::from(vec![1, 2, 3, 1, 4, 9])),
                        None,
                    )),
                ],
                None,
            ),
            None,
            false,
        )?;

        let name_array = UInt32Array::from(vec![
            Into::<u32>::into(&InfoCode::VendorName),
            Into::<u32>::into(&InfoCode::VendorVersion),
            Into::<u32>::into(&InfoCode::VendorArrowVersion),
            Into::<u32>::into(&InfoCode::DriverName),
            Into::<u32>::into(&InfoCode::DriverVersion),
            Into::<u32>::into(&InfoCode::DriverArrowVersion),
        ]);

        let type_id_buffer = Buffer::from_slice_ref([0_i8, 1, 2, 3, 4, 5]);
        let value_offsets_buffer = Buffer::from_slice_ref([0_i32, 0, 0, 0, 0, 0]);

        let value_array = UnionArray::try_new(
            &[0, 1, 2, 3, 4, 5],
            type_id_buffer,
            Some(value_offsets_buffer),
            vec![
                (
                    Field::new("string_value", string_value_array.data_type().clone(), true),
                    Arc::new(string_value_array),
                ),
                (
                    Field::new("bool_value", bool_value_array.data_type().clone(), true),
                    Arc::new(bool_value_array),
                ),
                (
                    Field::new("int64_value", int64_value_array.data_type().clone(), true),
                    Arc::new(int64_value_array),
                ),
                (
                    Field::new(
                        "int32_bitmask",
                        int32_bitmask_array.data_type().clone(),
                        true,
                    ),
                    Arc::new(int32_bitmask_array),
                ),
                (
                    Field::new("string_list", string_list_array.data_type().clone(), true),
                    Arc::new(string_list_array),
                ),
                (
                    Field::new(
                        "int32_to_int32_list_map",
                        int32_to_int32_list_map_array.data_type().clone(),
                        true,
                    ),
                    Arc::new(int32_to_int32_list_map_array),
                ),
            ],
        )?;

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("info_name", name_array.data_type().clone(), false),
                Field::new("info_value", value_array.data_type().clone(), true),
            ])),
            vec![Arc::new(name_array), Arc::new(value_array)],
        )?;
        let reader = SingleBatchReader::new(batch);
        Ok(reader)
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
