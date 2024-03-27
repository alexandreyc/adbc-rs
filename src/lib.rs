pub mod driver_manager;
pub mod error;
pub mod ffi;
pub mod options;

use arrow::datatypes::Schema;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use error::{Error, Result};

pub trait Optionable {
    type Key: AsRef<str>;

    /// Sets a post-init database option.
    fn set_option(&mut self, key: Self::Key, value: options::OptionValue) -> Result<()>;

    /// Gets a database option value by key.
    fn get_option_string(&self, key: Self::Key) -> Result<String>;

    /// Gets a database option value by key.
    fn get_option_bytes(&self, key: Self::Key) -> Result<Vec<u8>>;

    /// Gets a database option value by key.
    fn get_option_int(&self, key: Self::Key) -> Result<i64>;

    /// Gets a database option value by key.
    fn get_option_double(&self, key: Self::Key) -> Result<f64>;
}

pub trait Driver {
    type DatabaseType<'driver>: Database
    where
        Self: 'driver;

    /// Allocates and initializes a new database without pre-init options.
    fn new_database(&self) -> Result<Self::DatabaseType<'_>>;

    /// Allocates and initializes a new database with pre-init options.
    fn new_database_with_opts<'a>(
        &self,
        opts: impl Iterator<
            Item = (
                <Self::DatabaseType<'a> as Optionable>::Key,
                options::OptionValue,
            ),
        >,
    ) -> Result<Self::DatabaseType<'_>>
    where
        Self: 'a;
}

pub trait Database: Optionable {
    type ConnectionType<'database>: Connection
    where
        Self: 'database;

    /// Allocates and initializes a new connection without pre-init options.
    fn new_connection(&mut self) -> Result<Self::ConnectionType<'_>>;

    /// Allocates and initializes a new connection with pre-init options.
    fn new_connection_with_opts<'a>(
        &mut self,
        opts: impl Iterator<
            Item = (
                <Self::ConnectionType<'a> as Optionable>::Key,
                options::OptionValue,
            ),
        >,
    ) -> Result<Self::ConnectionType<'_>>
    where
        Self: 'a;
}

pub trait Connection: Optionable {
    type StatementType<'connection>: Statement
    where
        Self: 'connection;

    /// Allocates and initializes a new statement.
    fn new_statement(&mut self) -> Result<Self::StatementType<'_>>;

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
    ) -> Result<Schema>;
    fn get_table_types(&mut self) -> Result<impl RecordBatchReader>;
    fn get_statistics_name(&mut self) -> Result<impl RecordBatchReader>;
    fn get_statistics(
        &mut self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<impl RecordBatchReader>;
    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
    fn read_partition(&mut self, partition: &[u8]) -> Result<impl RecordBatchReader>;
}

pub trait Statement: Optionable {
    fn bind(&mut self, batch: RecordBatch) -> Result<()>;
    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()>;
    fn execute(&mut self) -> Result<impl RecordBatchReader>;
    fn execute_update(&mut self) -> Result<i64>;
    fn execute_schema(&mut self) -> Result<Schema>;
    fn execute_partitions(&mut self) -> Result<Partitions>;
    fn get_parameters_schema(&mut self) -> Result<Schema>;
    fn prepare(&mut self) -> Result<()>;
    fn set_sql_query(&mut self, query: &str) -> Result<()>;
    fn set_substrait_plan(&mut self, plan: &[u8]) -> Result<()>;
    fn cancel(&mut self) -> Result<()>;
}

type Partitions = Vec<Vec<u8>>;
