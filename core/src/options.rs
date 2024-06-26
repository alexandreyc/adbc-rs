//! Various option and configuration types.

use std::os::raw::c_int;

use crate::{
    error::{Error, Status},
    ffi::constants,
};

/// Option value.
///
/// Can be created with various implementations of [From].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum OptionValue {
    String(String),
    Bytes(Vec<u8>),
    Int(i64),
    Double(f64),
}

impl OptionValue {
    /// Gets the data type of the option's value.
    pub(crate) fn get_type(&self) -> &str {
        match self {
            Self::String(_) => "String",
            Self::Bytes(_) => "Bytes",
            Self::Int(_) => "Int",
            Self::Double(_) => "Double",
        }
    }
}

impl From<String> for OptionValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for OptionValue {
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

impl From<i64> for OptionValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<f64> for OptionValue {
    fn from(value: f64) -> Self {
        Self::Double(value)
    }
}

impl From<Vec<u8>> for OptionValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl From<&[u8]> for OptionValue {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.into())
    }
}

impl<const N: usize> From<[u8; N]> for OptionValue {
    fn from(value: [u8; N]) -> Self {
        Self::Bytes(value.into())
    }
}

impl<const N: usize> From<&[u8; N]> for OptionValue {
    fn from(value: &[u8; N]) -> Self {
        Self::Bytes(value.into())
    }
}

/// ADBC revision versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AdbcVersion {
    /// Version 1.0.0.
    V100,
    /// Version 1.1.0.
    V110,
}

impl From<AdbcVersion> for c_int {
    fn from(value: AdbcVersion) -> Self {
        match value {
            AdbcVersion::V100 => constants::ADBC_VERSION_1_0_0,
            AdbcVersion::V110 => constants::ADBC_VERSION_1_1_0,
        }
    }
}

impl TryFrom<c_int> for AdbcVersion {
    type Error = Error;
    fn try_from(value: c_int) -> Result<Self, Self::Error> {
        match value {
            constants::ADBC_VERSION_1_0_0 => Ok(AdbcVersion::V100),
            constants::ADBC_VERSION_1_1_0 => Ok(AdbcVersion::V110),
            _ => Err(Error::with_message_and_status(
                format!("Unknown ADBC version: {}", value),
                Status::InvalidArguments,
            )),
        }
    }
}

/// Info codes for database/driver metadata.
#[derive(Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum InfoCode {
    /// The database vendor/product name (type: utf8).
    VendorName,
    /// The database vendor/product version (type: utf8).
    VendorVersion,
    /// The database vendor/product Arrow library version (type: utf8).
    VendorArrowVersion,
    /// The driver name (type: utf8).
    DriverName,
    /// The driver version (type: utf8).
    DriverVersion,
    /// The driver Arrow library version (type: utf8).
    DriverArrowVersion,
    /// The driver ADBC API version (type: int64).
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    DriverAdbcVersion,
    // TODO(alexandreyc): add new codes (see https://github.com/apache/arrow-adbc/commit/aa04aadccd319e6fa3abb07154fa8d87b58d5c21)
}

impl From<&InfoCode> for u32 {
    fn from(value: &InfoCode) -> Self {
        match value {
            InfoCode::VendorName => constants::ADBC_INFO_VENDOR_NAME,
            InfoCode::VendorVersion => constants::ADBC_INFO_VENDOR_VERSION,
            InfoCode::VendorArrowVersion => constants::ADBC_INFO_VENDOR_ARROW_VERSION,
            InfoCode::DriverName => constants::ADBC_INFO_DRIVER_NAME,
            InfoCode::DriverVersion => constants::ADBC_INFO_DRIVER_VERSION,
            InfoCode::DriverArrowVersion => constants::ADBC_INFO_DRIVER_ARROW_VERSION,
            InfoCode::DriverAdbcVersion => constants::ADBC_INFO_DRIVER_ADBC_VERSION,
        }
    }
}

impl TryFrom<u32> for InfoCode {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            constants::ADBC_INFO_VENDOR_NAME => Ok(InfoCode::VendorName),
            constants::ADBC_INFO_VENDOR_VERSION => Ok(InfoCode::VendorVersion),
            constants::ADBC_INFO_VENDOR_ARROW_VERSION => Ok(InfoCode::VendorArrowVersion),
            constants::ADBC_INFO_DRIVER_NAME => Ok(InfoCode::DriverName),
            constants::ADBC_INFO_DRIVER_VERSION => Ok(InfoCode::DriverVersion),
            constants::ADBC_INFO_DRIVER_ARROW_VERSION => Ok(InfoCode::DriverArrowVersion),
            constants::ADBC_INFO_DRIVER_ADBC_VERSION => Ok(InfoCode::DriverAdbcVersion),
            v => Err(Error::with_message_and_status(
                format!("Unknown info code: {v}"),
                Status::InvalidData,
            )),
        }
    }
}

/// Depth parameter for [get_objects][crate::Connection::get_objects] method.
#[derive(Debug)]
#[non_exhaustive]
pub enum ObjectDepth {
    /// Catalogs, schemas, tables, and columns.
    All,
    /// Catalogs only.
    Catalogs,
    /// Catalogs and schemas.
    Schemas,
    /// Catalogs, schemas, and tables.
    Tables,
    /// Catalogs, schemas, tables, and columns. Identical to [ObjectDepth::All].
    Columns,
}

impl From<ObjectDepth> for c_int {
    fn from(value: ObjectDepth) -> Self {
        match value {
            ObjectDepth::All => constants::ADBC_OBJECT_DEPTH_ALL,
            ObjectDepth::Catalogs => constants::ADBC_OBJECT_DEPTH_CATALOGS,
            ObjectDepth::Schemas => constants::ADBC_OBJECT_DEPTH_DB_SCHEMAS,
            ObjectDepth::Tables => constants::ADBC_OBJECT_DEPTH_TABLES,
            ObjectDepth::Columns => constants::ADBC_OBJECT_DEPTH_COLUMNS,
        }
    }
}

impl TryFrom<c_int> for ObjectDepth {
    type Error = Error;

    fn try_from(value: c_int) -> Result<Self, Error> {
        match value {
            constants::ADBC_OBJECT_DEPTH_ALL => Ok(ObjectDepth::All),
            constants::ADBC_OBJECT_DEPTH_CATALOGS => Ok(ObjectDepth::Catalogs),
            constants::ADBC_OBJECT_DEPTH_DB_SCHEMAS => Ok(ObjectDepth::Schemas),
            constants::ADBC_OBJECT_DEPTH_TABLES => Ok(ObjectDepth::Tables),
            v => Err(Error::with_message_and_status(
                format!("Unknown object depth: {v}"),
                Status::InvalidData,
            )),
        }
    }
}

/// Database option key.
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
#[non_exhaustive]
pub enum OptionDatabase {
    /// Canonical option key for URIs.
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    Uri,
    /// Canonical option key for usernames.
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    Username,
    /// Canonical option key for passwords.
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    Password,
    /// Driver-specific key.
    Other(String),
}

impl AsRef<str> for OptionDatabase {
    fn as_ref(&self) -> &str {
        match self {
            Self::Uri => constants::ADBC_OPTION_URI,
            Self::Username => constants::ADBC_OPTION_USERNAME,
            Self::Password => constants::ADBC_OPTION_PASSWORD,
            Self::Other(key) => key,
        }
    }
}

impl From<&str> for OptionDatabase {
    fn from(value: &str) -> Self {
        match value {
            constants::ADBC_OPTION_URI => Self::Uri,
            constants::ADBC_OPTION_USERNAME => Self::Username,
            constants::ADBC_OPTION_PASSWORD => Self::Password,
            key => Self::Other(key.into()),
        }
    }
}

/// Connection option key.
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
#[non_exhaustive]
pub enum OptionConnection {
    /// Whether autocommit is enabled.
    AutoCommit,
    /// Whether the current connection should be restricted to being read-only.
    ReadOnly,
    /// The catalog used by the connection.
    /// # Since
    /// ADBC API revision 1.1.0
    CurrentCatalog,
    /// The database schema used by the connection.
    /// # Since
    /// ADBC API revision 1.1.0
    CurrentSchema,
    /// The isolation level of the connection. See [IsolationLevel].
    IsolationLevel,
    /// Driver-specific key.
    Other(String),
}

impl AsRef<str> for OptionConnection {
    fn as_ref(&self) -> &str {
        match self {
            Self::AutoCommit => constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT,
            Self::ReadOnly => constants::ADBC_CONNECTION_OPTION_READ_ONLY,
            Self::CurrentCatalog => constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG,
            Self::CurrentSchema => constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA,
            Self::IsolationLevel => constants::ADBC_CONNECTION_OPTION_ISOLATION_LEVEL,
            Self::Other(key) => key,
        }
    }
}

impl From<&str> for OptionConnection {
    fn from(value: &str) -> Self {
        match value {
            constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT => Self::AutoCommit,
            constants::ADBC_CONNECTION_OPTION_READ_ONLY => Self::ReadOnly,
            constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG => Self::CurrentCatalog,
            constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA => Self::CurrentSchema,
            constants::ADBC_CONNECTION_OPTION_ISOLATION_LEVEL => Self::IsolationLevel,
            key => Self::Other(key.into()),
        }
    }
}

/// Statement option key.
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
#[non_exhaustive]
pub enum OptionStatement {
    /// The ingest mode for a bulk insert. See [IngestMode].
    IngestMode,
    /// The name of the target table for a bulk insert.
    TargetTable,
    /// Whether query execution is nonblocking. By default, execution is blocking.
    ///
    /// When enabled, [execute_partitions][crate::Statement::execute_partitions]
    /// will return partitions as soon as they are available, instead of returning
    /// them all at the end. When there are no more to return, it will return an
    /// empty set of partitions. The methods [execute][crate::Statement::execute],
    /// [execute_schema][crate::Statement::execute_schema] and
    /// [execute_update][crate::Statement::execute_update] are not affected.
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    Incremental,
    /// Get the progress of a query. It's a read-only option that should be
    /// read with [get_option_double][crate::Optionable::get_option_double].
    ///
    /// The value is not necessarily in any particular range or have any
    /// particular units. For example, it might be a percentage, bytes of data,
    /// rows of data, number of workers, etc. The max value can be retrieved
    /// via [OptionStatement::MaxProgress]. This represents the progress of
    /// execution, not of consumption (i.e., it is independent of how much of the
    /// result set has been read by the client).
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    Progress,
    /// Get the maximum progress of a query. It's a read-only option that should be
    /// read with [get_option_double][crate::Optionable::get_option_double].
    ///
    /// This is the value of [OptionStatement::Progress] for a completed query.
    /// If not supported, or if the value is nonpositive, then the maximum is not
    /// known. For instance, the query may be fully streaming and the driver
    /// does not know when the result set will end.
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    MaxProgress,
    /// Driver-specific key.
    Other(String),
}

impl AsRef<str> for OptionStatement {
    fn as_ref(&self) -> &str {
        match self {
            Self::IngestMode => constants::ADBC_INGEST_OPTION_MODE,
            Self::TargetTable => constants::ADBC_INGEST_OPTION_TARGET_TABLE,
            Self::Incremental => constants::ADBC_STATEMENT_OPTION_INCREMENTAL,
            Self::Progress => constants::ADBC_STATEMENT_OPTION_PROGRESS,
            Self::MaxProgress => constants::ADBC_STATEMENT_OPTION_MAX_PROGRESS,
            Self::Other(key) => key,
        }
    }
}

impl From<&str> for OptionStatement {
    fn from(value: &str) -> Self {
        match value {
            constants::ADBC_INGEST_OPTION_MODE => Self::IngestMode,
            constants::ADBC_INGEST_OPTION_TARGET_TABLE => Self::TargetTable,
            constants::ADBC_STATEMENT_OPTION_INCREMENTAL => Self::Incremental,
            constants::ADBC_STATEMENT_OPTION_PROGRESS => Self::Progress,
            constants::ADBC_STATEMENT_OPTION_MAX_PROGRESS => Self::MaxProgress,
            key => Self::Other(key.into()),
        }
    }
}

/// Isolation level value for key [OptionConnection::IsolationLevel].
#[derive(Debug)]
#[non_exhaustive]
pub enum IsolationLevel {
    /// Use database or driver default isolation level.
    Default,
    /// The lowest isolation level. Dirty reads are allowed, so one transaction
    /// may see not-yet-committed changes made by others.
    ReadUncommitted,
    /// Lock-based concurrency control keeps write locks until the end of the
    /// transaction, but read locks are released as soon as a SELECT is
    /// performed. Non-repeatable reads can occur in this isolation level.
    ///
    /// More simply put, `ReadCommitted` is an isolation level that guarantees
    /// that any data read is committed at the moment it is read. It simply
    /// restricts the reader from seeing any intermediate, uncommitted,
    /// 'dirty' reads. It makes no promise whatsoever that if the transaction
    /// re-issues the read, it will find the same data; data is free to change
    /// after it is read.
    ReadCommitted,
    /// Lock-based concurrency control keeps read AND write locks (acquired on
    /// selection data) until the end of the transaction.
    ///
    /// However, range-locks are not managed, so phantom reads can occur.
    /// Write skew is possible at this isolation level in some systems.
    RepeatableRead,
    /// This isolation guarantees that all reads in the transaction will see a
    /// consistent snapshot of the database and the transaction should only
    /// successfully commit if no updates conflict with any concurrent updates
    /// made since that snapshot.
    Snapshot,
    /// Serializability requires read and write locks to be released only at the
    /// end of the transaction. This includes acquiring range-locks when a
    /// select query uses a ranged WHERE clause to avoid phantom reads.
    Serializable,
    /// The central distinction between serializability and linearizability is
    /// that serializability is a global property; a property of an entire
    /// history of operations and transactions. Linearizability is a local
    /// property; a property of a single operation/transaction.
    ///
    /// Linearizability can be viewed as a special case of strict serializability
    /// where transactions are restricted to consist of a single operation applied
    /// to a single object.
    Linearizable,
}

impl From<IsolationLevel> for String {
    fn from(value: IsolationLevel) -> Self {
        match value {
            IsolationLevel::Default => constants::ADBC_OPTION_ISOLATION_LEVEL_DEFAULT.into(),
            IsolationLevel::ReadUncommitted => {
                constants::ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED.into()
            }
            IsolationLevel::ReadCommitted => {
                constants::ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED.into()
            }
            IsolationLevel::RepeatableRead => {
                constants::ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ.into()
            }
            IsolationLevel::Snapshot => constants::ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT.into(),
            IsolationLevel::Serializable => {
                constants::ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE.into()
            }
            IsolationLevel::Linearizable => {
                constants::ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE.into()
            }
        }
    }
}

impl From<IsolationLevel> for OptionValue {
    fn from(value: IsolationLevel) -> Self {
        Self::String(value.into())
    }
}

/// Ingestion mode value for key [OptionStatement::IngestMode].
#[derive(Debug)]
#[non_exhaustive]
pub enum IngestMode {
    /// Create the table and insert data; error if the table exists.
    Create,
    /// Do not create the table, and insert data; error if the table does not
    /// exist ([Status::NotFound]) or does not match the schema of the data to
    /// append ([Status::AlreadyExists]).
    Append,
    /// Create the table and insert data; drop the original table if it already
    /// exists.
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    Replace,
    /// Insert data; create the table if it does not exist, or error if the
    /// table exists, but the schema does not match the schema of the data to
    /// append ([Status::AlreadyExists]).
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    CreateAppend,
}

impl From<IngestMode> for String {
    fn from(value: IngestMode) -> Self {
        match value {
            IngestMode::Create => constants::ADBC_INGEST_OPTION_MODE_CREATE.into(),
            IngestMode::Append => constants::ADBC_INGEST_OPTION_MODE_APPEND.into(),
            IngestMode::Replace => constants::ADBC_INGEST_OPTION_MODE_REPLACE.into(),
            IngestMode::CreateAppend => constants::ADBC_INGEST_OPTION_MODE_CREATE_APPEND.into(),
        }
    }
}
impl From<IngestMode> for OptionValue {
    fn from(value: IngestMode) -> Self {
        Self::String(value.into())
    }
}
