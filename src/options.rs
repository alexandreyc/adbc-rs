use crate::ffi::constants;
use std::os::raw::c_int;

pub enum OptionValue {
    String(String),
    Bytes(Vec<u8>),
    Int(i64),
    Double(f64),
}

#[derive(Clone, Copy)]
pub enum AdbcVersion {
    V100,
    V110,
}

impl From<AdbcVersion> for i32 {
    fn from(value: AdbcVersion) -> Self {
        match value {
            AdbcVersion::V100 => constants::ADBC_VERSION_1_0_0,
            AdbcVersion::V110 => constants::ADBC_VERSION_1_1_0,
        }
    }
}

pub enum InfoCode {
    VendorName,
    VendorVersion,
    VendorArrowVersion,
    DriverName,
    DriverVersion,
    DriverArrowVersion,
    DriverAdbcVersion,
}

pub enum ObjectDepth {
    All,
    Catalogs,
    Schemas,
    Tables,
    Columns,
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

pub enum DatabaseOptionKey {
    Uri,
    Username,
    Password,
    Other(String),
}

impl AsRef<str> for DatabaseOptionKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::Uri => constants::ADBC_OPTION_URI,
            Self::Username => constants::ADBC_OPTION_USERNAME,
            Self::Password => constants::ADBC_OPTION_PASSWORD,
            Self::Other(key) => &key,
        }
    }
}

pub enum ConnectionOptionKey {
    AutoCommit,
    ReadOnly,
    CurrentCatalog,
    CurrentSchema,
    IsolationLevel,
    Other(String),
}

impl AsRef<str> for ConnectionOptionKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::AutoCommit => constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT,
            Self::ReadOnly => constants::ADBC_CONNECTION_OPTION_READ_ONLY,
            Self::CurrentCatalog => constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG,
            Self::CurrentSchema => constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA,
            Self::IsolationLevel => constants::ADBC_CONNECTION_OPTION_ISOLATION_LEVEL,
            Self::Other(key) => &key,
        }
    }
}

pub enum StatementOptionKey {
    IngestMode,
    TargetTable,
    Incremental,
    Progress,
    MaxProgress,
    Other(String),
}

impl AsRef<str> for StatementOptionKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::IngestMode => constants::ADBC_INGEST_OPTION_MODE,
            Self::TargetTable => constants::ADBC_INGEST_OPTION_TARGET_TABLE,
            Self::Incremental => constants::ADBC_STATEMENT_OPTION_INCREMENTAL,
            Self::Progress => constants::ADBC_STATEMENT_OPTION_PROGRESS,
            Self::MaxProgress => constants::ADBC_STATEMENT_OPTION_MAX_PROGRESS,
            Self::Other(key) => &key,
        }
    }
}

pub enum IsolationLevel {
    Default,
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Snapshot,
    Serializable,
    Linearizable,
}

impl From<IsolationLevel> for OptionValue {
    fn from(value: IsolationLevel) -> Self {
        match value {
            IsolationLevel::Default => {
                Self::String(constants::ADBC_OPTION_ISOLATION_LEVEL_DEFAULT.into())
            }
            IsolationLevel::ReadUncommitted => {
                Self::String(constants::ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED.into())
            }
            IsolationLevel::ReadCommitted => {
                Self::String(constants::ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED.into())
            }
            IsolationLevel::RepeatableRead => {
                Self::String(constants::ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ.into())
            }
            IsolationLevel::Snapshot => {
                Self::String(constants::ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT.into())
            }
            IsolationLevel::Serializable => {
                Self::String(constants::ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE.into())
            }
            IsolationLevel::Linearizable => {
                Self::String(constants::ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE.into())
            }
        }
    }
}

pub enum IngestMode {
    Create,
    Append,
    Replace,
    CreateAppend,
}

impl From<IngestMode> for OptionValue {
    fn from(value: IngestMode) -> Self {
        match value {
            IngestMode::Create => Self::String(constants::ADBC_INGEST_OPTION_MODE_CREATE.into()),
            IngestMode::Append => Self::String(constants::ADBC_INGEST_OPTION_MODE_APPEND.into()),
            IngestMode::Replace => Self::String(constants::ADBC_INGEST_OPTION_MODE_REPLACE.into()),
            IngestMode::CreateAppend => {
                Self::String(constants::ADBC_INGEST_OPTION_MODE_CREATE_APPEND.into())
            }
        }
    }
}
