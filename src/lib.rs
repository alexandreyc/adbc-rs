pub mod driver_manager;
mod error;
pub mod ffi;
pub mod options;

use arrow::datatypes::Schema;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use error::{Error, Result};

pub trait Optionable {
    /// Sets a post-init database option.
    fn set_option(&mut self, key: impl AsRef<str>, value: options::OptionValue) -> Result<()>;

    /// Gets a database option value by key.
    fn get_option_string(&mut self, key: impl AsRef<str>) -> Result<String>;

    /// Gets a database option value by key.
    fn get_option_bytes(&mut self, key: impl AsRef<str>) -> Result<Vec<u8>>;

    /// Gets a database option value by key.
    fn get_option_int(&mut self, key: impl AsRef<str>) -> Result<i64>;

    /// Gets a database option value by key.
    fn get_option_double(&mut self, key: impl AsRef<str>) -> Result<f64>;
}

pub trait Driver {
    type DatabaseType: Database;

    /// Allocates and initializes a new database without pre-init options.
    fn new_database(&self) -> Result<Self::DatabaseType>;

    /// Allocates and initializes a new database with pre-init options.
    fn new_database_with_opts(
        &self,
        opts: impl Iterator<Item = (impl AsRef<str>, options::OptionValue)>,
    ) -> Result<Self::DatabaseType>;
}

pub trait Database: Optionable {
    type ConnectionType: Connection;

    /// Allocates and initializes a new connection without pre-init options.
    fn new_connection(&mut self) -> Result<Self::ConnectionType>;

    /// Allocates and initializes a new connection with pre-init options.
    fn new_connection_with_opts(
        &mut self,
        opts: impl Iterator<Item = (impl AsRef<str>, options::OptionValue)>,
    ) -> Result<Self::ConnectionType>;
}

pub trait Connection: Optionable {
    type StatementType: Statement;

    /// Allocates and initializes a new statement.
    fn new_statement(&mut self) -> Result<Self::StatementType>;

    fn cancel(&mut self) -> Result<()>;
    fn get_info(&mut self, codes: Option<&[options::InfoCode]>) -> Result<impl RecordBatchReader>;
    fn get_objects(
        &mut self,
        depth: options::ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&[&str]>,
        column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader>;
    fn get_table_schema(
        &mut self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
        approximate: bool,
    ) -> Result<Schema>;
    fn get_table_types(&mut self) -> Result<impl RecordBatchReader>;
    fn get_statistics_name(&mut self) -> Result<impl RecordBatchReader>;
    fn get_statistics(
        &mut self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
    ) -> Result<impl RecordBatchReader>;
    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
    fn read_partition(&mut self, partition: &[u8]) -> Result<impl RecordBatchReader>;
}

pub trait Statement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()>;
    fn bind_stream(&mut self, reader: impl RecordBatchReader) -> Result<()>;
    fn execute(&mut self) -> Result<impl RecordBatchReader>;
    fn execute_update(&mut self) -> Result<i64>;
    fn execute_schema(&mut self) -> Result<Schema>;
    // fn execute_partitions(&mut self) -> Result<PartitionedResult>; // TODO
    fn get_parameters_schema(&mut self) -> Result<Schema>;
    fn prepare(&mut self) -> Result<()>;
    fn set_sql_query(&mut self, query: &str) -> Result<()>;
    fn set_substrait_plan(&mut self, plan: &[u8]) -> Result<()>;
    fn cancel(&mut self) -> Result<()>;
}
