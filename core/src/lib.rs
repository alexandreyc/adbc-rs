//! ADBC: Arrow Database Connectivity
//!
//! ADBC is a set of APIs and libraries for [Arrow](https://arrow.apache.org/)-native
//! access to databases. Execute SQL and [Substrait](https://substrait.io/)
//! queries, query database catalogs, and more, all using Arrow data to
//! eliminate unnecessary data copies, speed up access, and make it more
//! convenient to build analytical applications.
//!
//! Read more about ADBC at <https://arrow.apache.org/adbc/>
//!
//! This library currently provides:
//! - An abstract Rust API to be implemented by vendor-specific drivers.
//! - A driver manager which implements this same API, but dynamically loads
//! drivers internally and forwards calls appropriately using the [C API](https://github.com/apache/arrow-adbc/blob/main/adbc.h).
//!
//! We hope to provide the ability to expose Rust drivers as C drivers in the
//! near future permitting other languages to use Rust drivers.
//!
//! # Native Rust drivers
//!
//! Native Rust drivers will implement the abstract API consisting of the traits:
//! - [Driver]
//! - [Database]
//! - [Connection]
//! - [Statement]
//!
//! For drivers implemented in Rust, using these will be more efficient and
//! safe, since it avoids the overhead of going through C FFI.
//!
//! # Driver Manager
//!
//! The [driver_manager] module allows loading drivers exposing the C API,
//! either from an initialization function (link-time, either static or dynamic)
//! or by dynamically finding such a function in a dynamic library (run-time).

pub mod driver_exporter;
pub mod driver_manager;
pub mod error;
pub mod ffi;
pub mod options;
pub mod schemas;

pub use ffi::FFI_AdbcDriverInitFunc as AdbcDriverInitFunc;

use arrow::datatypes::Schema;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use error::{Error, Result};
use options::{OptionConnection, OptionDatabase, OptionStatement, OptionValue};

/// Ability to configure an object by setting/getting options.
pub trait Optionable {
    type Option: AsRef<str>;

    /// Set a post-init option.
    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()>;

    /// Get a string option value by key.
    fn get_option_string(&self, key: Self::Option) -> Result<String>;

    /// Get a bytes option value by key.
    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>>;

    /// Get an integer option value by key.
    fn get_option_int(&self, key: Self::Option) -> Result<i64>;

    /// Get a float option value by key.
    fn get_option_double(&self, key: Self::Option) -> Result<f64>;
}

/// A handle to an ADBC driver.
pub trait Driver {
    type DatabaseType: Database;

    /// Allocate and initialize a new database without pre-init options.
    fn new_database(&self) -> Result<Self::DatabaseType>;

    /// Allocate and initialize a new database with pre-init options.
    fn new_database_with_opts(
        &self,
        opts: impl Iterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType>;
}

/// A handle to an ADBC database.
///
/// Databases hold state shared by multiple connections. This typically means
/// configuration and caches. For in-memory databases, it provides a place to
/// hold ownership of the in-memory database.
///
/// Databases must be kept alive as long as any connections exist.
pub trait Database: Optionable<Option = OptionDatabase> {
    type ConnectionType: Connection;

    /// Allocate and initialize a new connection without pre-init options.
    fn new_connection(&self) -> Result<Self::ConnectionType>;

    /// Allocate and initialize a new connection with pre-init options.
    fn new_connection_with_opts(
        &self,
        opts: impl Iterator<Item = (options::OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType>;
}

/// A handle to an ADBC connection.
///
/// Connections provide methods for query execution, managing prepared
/// statements, using transactions, and so on.
///
/// # Autocommit
///
/// Connections should start in autocommit mode. They can be moved out by
/// setting [options::OptionConnection::AutoCommit] to "false". Turning off
/// autocommit allows customizing the isolation level.
pub trait Connection: Optionable<Option = OptionConnection> {
    type StatementType: Statement;

    /// Allocate and initialize a new statement.
    fn new_statement(&self) -> Result<Self::StatementType>;

    /// Cancel the in-progress operation on a connection.
    fn cancel(&self) -> Result<()>;

    /// Get metadata about the database/driver.
    ///
    /// # Arguments
    ///
    /// - `codes` - Requested metadata. If `None`, retrieve all available metadata.
    ///
    /// # Result
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name                  | Field Type
    /// ----------------------------|------------------------
    /// info_name                   | uint32 not null
    /// info_value                  | INFO_SCHEMA
    ///
    /// INFO_SCHEMA is a dense union with members:
    ///
    /// Field Name (Type Code)      | Field Type
    /// ----------------------------|------------------------
    /// string_value (0)            | utf8
    /// bool_value (1)              | bool
    /// int64_value (2)             | int64
    /// int32_bitmask (3)           | int32
    /// string_list (4)             | list\<utf8\>
    /// int32_to_int32_list_map (5) | map\<int32, list\<int32\>\>
    fn get_info(
        &self,
        codes: Option<Vec<options::InfoCode>>,
    ) -> Result<impl RecordBatchReader + Send>;

    /// Get a hierarchical view of all catalogs, database schemas, tables, and
    /// columns.
    ///
    /// # Arguments
    ///
    /// - `depth` - The level of nesting to query.
    /// - `catalog` - Only show tables in the given catalog. If `None`,
    ///   do not filter by catalog. If an empty string, only show tables
    ///   without a catalog.  May be a search pattern.
    /// - `db_schema` - Only show tables in the given database schema. If
    ///   `None`, do not filter by database schema. If an empty string, only show
    ///   tables without a database schema. May be a search pattern.
    /// - `table_name` - Only show tables with the given name. If `None`, do not
    ///   filter by name. May be a search pattern.
    /// - `table_type` - Only show tables matching one of the given table
    ///   types. If `None`, show tables of any type. Valid table types can be fetched
    ///   from [Connection::get_table_types].
    /// - `column_name` - Only show columns with the given name. If
    ///   `None`, do not filter by name.  May be a search pattern..
    ///
    /// # Result
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// | Field Name               | Field Type               |
    /// |--------------------------|--------------------------|
    /// | catalog_name             | utf8                     |
    /// | catalog_db_schemas       | list\<DB_SCHEMA_SCHEMA\> |
    ///
    /// DB_SCHEMA_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              |
    /// |--------------------------|-------------------------|
    /// | db_schema_name           | utf8                    |
    /// | db_schema_tables         | list\<TABLE_SCHEMA\>    |
    ///
    /// TABLE_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type                |
    /// |--------------------------|---------------------------|
    /// | table_name               | utf8 not null             |
    /// | table_type               | utf8 not null             |
    /// | table_columns            | list\<COLUMN_SCHEMA\>     |
    /// | table_constraints        | list\<CONSTRAINT_SCHEMA\> |
    ///
    /// COLUMN_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              | Comments |
    /// |--------------------------|-------------------------|----------|
    /// | column_name              | utf8 not null           |          |
    /// | ordinal_position         | int32                   | (1)      |
    /// | remarks                  | utf8                    | (2)      |
    /// | xdbc_data_type           | int16                   | (3)      |
    /// | xdbc_type_name           | utf8                    | (3)      |
    /// | xdbc_column_size         | int32                   | (3)      |
    /// | xdbc_decimal_digits      | int16                   | (3)      |
    /// | xdbc_num_prec_radix      | int16                   | (3)      |
    /// | xdbc_nullable            | int16                   | (3)      |
    /// | xdbc_column_def          | utf8                    | (3)      |
    /// | xdbc_sql_data_type       | int16                   | (3)      |
    /// | xdbc_datetime_sub        | int16                   | (3)      |
    /// | xdbc_char_octet_length   | int32                   | (3)      |
    /// | xdbc_is_nullable         | utf8                    | (3)      |
    /// | xdbc_scope_catalog       | utf8                    | (3)      |
    /// | xdbc_scope_schema        | utf8                    | (3)      |
    /// | xdbc_scope_table         | utf8                    | (3)      |
    /// | xdbc_is_autoincrement    | bool                    | (3)      |
    /// | xdbc_is_generatedcolumn  | bool                    | (3)      |
    ///
    /// 1. The column's ordinal position in the table (starting from 1).
    /// 2. Database-specific description of the column.
    /// 3. Optional value.  Should be null if not supported by the driver.
    ///    `xdbc_` values are meant to provide JDBC/ODBC-compatible metadata
    ///    in an agnostic manner.
    ///
    /// CONSTRAINT_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              | Comments |
    /// |--------------------------|-------------------------|----------|
    /// | constraint_name          | utf8                    |          |
    /// | constraint_type          | utf8 not null           | (1)      |
    /// | constraint_column_names  | list\<utf8\> not null     | (2)      |
    /// | constraint_column_usage  | list\<USAGE_SCHEMA\>      | (3)      |
    ///
    /// 1. One of `CHECK`, `FOREIGN KEY`, `PRIMARY KEY`, or `UNIQUE`.
    /// 2. The columns on the current table that are constrained, in
    ///    order.
    /// 3. For `FOREIGN KEY` only, the referenced table and columns.
    ///
    /// USAGE_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              |
    /// |--------------------------|-------------------------|
    /// | fk_catalog               | utf8                    |
    /// | fk_db_schema             | utf8                    |
    /// | fk_table                 | utf8 not null           |
    /// | fk_column_name           | utf8 not null           |
    ///
    fn get_objects(
        &self,
        depth: options::ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader + Send>;

    /// Get the Arrow schema of a table.
    ///
    /// # Arguments
    ///
    /// - `catalog` - The catalog (or `None` if not applicable).
    /// - `db_schema` - The database schema (or `None` if not applicable).
    /// - `table_name` - The table name.
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema>;

    /// Get a list of table types in the database.
    ///
    /// # Result
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name     | Field Type
    /// ---------------|--------------
    /// table_type     | utf8 not null
    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send>;

    /// Get the names of statistics specific to this driver.
    ///
    /// # Result
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name     | Field Type
    /// ---------------|----------------
    /// statistic_name | utf8 not null
    /// statistic_key  | int16 not null
    ///
    /// # Since
    /// ADBC API revision 1.1.0
    fn get_statistic_names(&self) -> Result<impl RecordBatchReader + Send>;

    /// Get statistics about the data distribution of table(s).
    ///
    /// # Arguments
    ///
    /// - `catalog` - The catalog (or `None` if not applicable). May be a search pattern.
    /// - `db_schema` - The database schema (or `None` if not applicable). May be a search pattern
    /// - `table_name` - The table name (or `None` if not applicable). May be a search pattern
    /// - `approximate` - If false, request exact values of statistics, else
    /// allow for best-effort, approximate, or cached values. The database may
    /// return approximate values regardless, as indicated in the result.  
    /// Requesting exact values may be expensive or unsupported.
    ///
    /// # Result
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// | Field Name               | Field Type                       |
    /// |--------------------------|----------------------------------|
    /// | catalog_name             | utf8                             |
    /// | catalog_db_schemas       | list\<DB_SCHEMA_SCHEMA\> not null|
    ///
    /// DB_SCHEMA_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type                        |
    /// |--------------------------|-----------------------------------|
    /// | db_schema_name           | utf8                              |
    /// | db_schema_statistics     | list\<STATISTICS_SCHEMA\> not null|
    ///
    /// STATISTICS_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type                       | Comments |
    /// |--------------------------|----------------------------------| -------- |
    /// | table_name               | utf8 not null                    |          |
    /// | column_name              | utf8                             | (1)      |
    /// | statistic_key            | int16 not null                   | (2)      |
    /// | statistic_value          | VALUE_SCHEMA not null            |          |
    /// | statistic_is_approximate | bool not null                    | (3)      |
    ///
    /// 1. If null, then the statistic applies to the entire table.
    /// 2. A dictionary-encoded statistic name (although we do not use the Arrow
    ///    dictionary type). Values in [0, 1024) are reserved for ADBC.  Other
    ///    values are for implementation-specific statistics.  For the definitions
    ///    of predefined statistic types, TODO (change this when statistics enum is added )
    ///     see \ref adbc-table-statistics. To get
    ///    driver-specific statistic names, use [Connection::get_statistic_names].
    /// 3. If true, then the value is approximate or best-effort.
    ///
    /// VALUE_SCHEMA is a dense union with members:
    ///
    /// | Field Name               | Field Type                       |
    /// |--------------------------|----------------------------------|
    /// | int64                    | int64                            |
    /// | uint64                   | uint64                           |
    /// | float64                  | float64                          |
    /// | binary                   | binary                           |
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<impl RecordBatchReader + Send>;

    /// Commit any pending transactions. Only used if autocommit is disabled.
    ///
    /// Behavior is undefined if this is mixed with SQL transaction statements.
    fn commit(&self) -> Result<()>;

    /// Roll back any pending transactions. Only used if autocommit is disabled.
    ///
    /// Behavior is undefined if this is mixed with SQL transaction statements.
    fn rollback(&self) -> Result<()>;

    /// Retrieve a given partition of data.
    ///
    /// A partition can be retrieved from [Statement::execute_partitions].
    ///
    /// # Arguments
    ///
    /// - `partition` - The partition descriptor.
    fn read_partition(&self, partition: &[u8]) -> Result<impl RecordBatchReader + Send>;
}

/// A handle to an ADBC statement.
///
/// A statement is a container for all state needed to execute a database query,
/// such as the query itself, parameters for prepared statements, driver
/// parameters, etc.
///
/// Statements may represent queries or prepared statements.
///
/// Statements may be used multiple times and can be reconfigured
/// (e.g. they can be reused to execute multiple different queries).
/// However, executing a statement (and changing certain other state)
/// will invalidate result sets obtained prior to that execution.
///
/// Multiple statements may be created from a single connection.
/// However, the driver may block or error if they are used concurrently
/// (whether from a single thread or multiple threads).
pub trait Statement: Optionable<Option = OptionStatement> {
    /// Bind Arrow data. This can be used for bulk inserts or prepared
    /// statements.
    fn bind(&self, batch: RecordBatch) -> Result<()>;

    /// Bind Arrow data. This can be used for bulk inserts or prepared
    /// statements.
    fn bind_stream(&self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()>;

    /// Execute a statement and get the results.
    ///
    /// This invalidates any prior result sets.
    fn execute(&self) -> Result<impl RecordBatchReader + Send>;

    /// Execute a statement that doesn’t have a result set and get the number
    /// of affected rows.
    ///
    /// This invalidates any prior result sets.
    ///
    /// # Result
    ///
    /// Will return the number of rows affected. If the affected row count is
    /// unknown or unsupported by the database, will return `Ok(-1)`.
    fn execute_update(&self) -> Result<i64>;

    /// Get the schema of the result set of a query without executing it.
    ///
    /// This invalidates any prior result sets.
    ///
    /// Depending on the driver, this may require first executing
    /// [Statement::prepare].
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    fn execute_schema(&self) -> Result<Schema>;

    /// Execute a statement and get the results as a partitioned result set.
    fn execute_partitions(&self) -> Result<PartitionedResult>;

    /// Get the schema for bound parameters.
    ///
    /// This retrieves an Arrow schema describing the number, names, and
    /// types of the parameters in a parameterized statement. The fields
    /// of the schema should be in order of the ordinal position of the
    /// parameters; named parameters should appear only once.
    ///
    /// If the parameter does not have a name, or the name cannot be
    /// determined, the name of the corresponding field in the schema will
    /// be an empty string. If the type cannot be determined, the type of
    /// the corresponding field will be NA (NullType).
    ///
    /// This should be called after [Statement::prepare].
    fn get_parameters_schema(&self) -> Result<Schema>;

    /// Turn this statement into a prepared statement to be executed multiple
    /// times.
    ///
    /// This invalidates any prior result sets.
    fn prepare(&self) -> Result<()>;

    /// Set the SQL query to execute.
    ///
    /// The query can then be executed with [Statement::execute]. For queries
    /// expected to be executed repeatedly, call [Statement::prepare] first.
    fn set_sql_query(&self, query: &str) -> Result<()>;

    /// Set the Substrait plan to execute.
    ///
    /// The query can then be executed with [Statement::execute]. For queries
    /// expected to be executed repeatedly, call [Statement::prepare] first.
    fn set_substrait_plan(&self, plan: &[u8]) -> Result<()>;

    /// Cancel execution of an in-progress query.
    ///
    /// This can be called during [Statement::execute] (or similar), or while
    /// consuming a result set returned from such.
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    fn cancel(&self) -> Result<()>;
}

/// Each data partition is described by an opaque byte array and can be
/// retrieved with [Connection::read_partition].
pub type Partitions = Vec<Vec<u8>>;

/// A partitioned result set as returned by [Statement::execute_partitions].
#[derive(Debug, PartialEq, Eq)]
pub struct PartitionedResult {
    /// The result partitions.
    pub partitions: Partitions,
    /// The schema of the result set.
    pub schema: Schema,
    /// The number of rows affected if known, else -1.
    pub rows_affected: i64,
}
