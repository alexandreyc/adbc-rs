use crate::options::AdbcVersion;

use super::types::FFI_AdbcStatusCode;
use std::os::raw::c_int;

pub const ADBC_STATUS_OK: FFI_AdbcStatusCode = 0;
pub const ADBC_STATUS_UNKNOWN: FFI_AdbcStatusCode = 1;
pub const ADBC_STATUS_NOT_IMPLEMENTED: FFI_AdbcStatusCode = 2;
pub const ADBC_STATUS_NOT_FOUND: FFI_AdbcStatusCode = 3;
pub const ADBC_STATUS_ALREADY_EXISTS: FFI_AdbcStatusCode = 4;
pub const ADBC_STATUS_INVALID_ARGUMENT: FFI_AdbcStatusCode = 5;
pub const ADBC_STATUS_INVALID_STATE: FFI_AdbcStatusCode = 6;
pub const ADBC_STATUS_INVALID_DATA: FFI_AdbcStatusCode = 7;
pub const ADBC_STATUS_INTEGRITY: FFI_AdbcStatusCode = 8;
pub const ADBC_STATUS_INTERNAL: FFI_AdbcStatusCode = 9;
pub const ADBC_STATUS_IO: FFI_AdbcStatusCode = 10;
pub const ADBC_STATUS_CANCELLED: FFI_AdbcStatusCode = 11;
pub const ADBC_STATUS_TIMEOUT: FFI_AdbcStatusCode = 12;
pub const ADBC_STATUS_UNAUTHENTICATED: FFI_AdbcStatusCode = 13;
pub const ADBC_STATUS_UNAUTHORIZED: FFI_AdbcStatusCode = 14;

const ADBC_VERSION_1_0_0: i32 = 1000000;
const ADBC_VERSION_1_1_0: i32 = 1001000;

impl From<AdbcVersion> for i32 {
    fn from(value: AdbcVersion) -> Self {
        match value {
            AdbcVersion::V100 => ADBC_VERSION_1_0_0,
            AdbcVersion::V110 => ADBC_VERSION_1_1_0,
        }
    }
}

pub(crate) const ADBC_INFO_VENDOR_NAME: u32 = 0;
pub(crate) const ADBC_INFO_VENDOR_VERSION: u32 = 1;
pub(crate) const ADBC_INFO_VENDOR_ARROW_VERSION: u32 = 2;
pub(crate) const ADBC_INFO_DRIVER_NAME: u32 = 100;
pub(crate) const ADBC_INFO_DRIVER_VERSION: u32 = 101;
pub(crate) const ADBC_INFO_DRIVER_ARROW_VERSION: u32 = 102;
pub(crate) const ADBC_INFO_DRIVER_ADBC_VERSION: u32 = 103;

pub(crate) const ADBC_OBJECT_DEPTH_ALL: c_int = 0;
pub(crate) const ADBC_OBJECT_DEPTH_CATALOGS: c_int = 1;
pub(crate) const ADBC_OBJECT_DEPTH_DB_SCHEMAS: c_int = 2;
pub(crate) const ADBC_OBJECT_DEPTH_TABLES: c_int = 3;
pub(crate) const ADBC_OBJECT_DEPTH_COLUMNS: c_int = ADBC_OBJECT_DEPTH_ALL;

pub(crate) const ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA: i32 = i32::MIN;

const ADBC_INGEST_OPTION_TARGET_TABLE: &str = "adbc.ingest.target_table";
const ADBC_INGEST_OPTION_MODE: &str = "adbc.ingest.mode";
pub(crate) const ADBC_INGEST_OPTION_MODE_CREATE: &str = "adbc.ingest.mode.create";
pub(crate) const ADBC_INGEST_OPTION_MODE_APPEND: &str = "adbc.ingest.mode.append";
pub(crate) const ADBC_INGEST_OPTION_MODE_REPLACE: &str = "adbc.ingest.mode.replace";
pub(crate) const ADBC_INGEST_OPTION_MODE_CREATE_APPEND: &str = "adbc.ingest.mode.create_append";

// #define ADBC_OPTION_VALUE_ENABLED "true"
// #define ADBC_OPTION_VALUE_DISABLED "false"

// #define ADBC_OPTION_URI "uri"
// #define ADBC_OPTION_USERNAME "username"
// #define ADBC_OPTION_PASSWORD "password"

// #define ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_KEY 0
// #define ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_NAME "adbc.statistic.byte_width"
// #define ADBC_STATISTIC_DISTINCT_COUNT_KEY 1
// #define ADBC_STATISTIC_DISTINCT_COUNT_NAME "adbc.statistic.distinct_count"
// #define ADBC_STATISTIC_MAX_BYTE_WIDTH_KEY 2
// #define ADBC_STATISTIC_MAX_BYTE_WIDTH_NAME "adbc.statistic.byte_width"
// #define ADBC_STATISTIC_MAX_VALUE_KEY 3
// #define ADBC_STATISTIC_MAX_VALUE_NAME "adbc.statistic.byte_width"
// #define ADBC_STATISTIC_MIN_VALUE_KEY 4
// #define ADBC_STATISTIC_MIN_VALUE_NAME "adbc.statistic.byte_width"
// #define ADBC_STATISTIC_NULL_COUNT_KEY 5
// #define ADBC_STATISTIC_NULL_COUNT_NAME "adbc.statistic.null_count"
// #define ADBC_STATISTIC_ROW_COUNT_KEY 6
// #define ADBC_STATISTIC_ROW_COUNT_NAME "adbc.statistic.row_count"

// #define ADBC_CONNECTION_OPTION_AUTOCOMMIT "adbc.connection.autocommit"
// #define ADBC_CONNECTION_OPTION_READ_ONLY "adbc.connection.readonly"
// #define ADBC_CONNECTION_OPTION_CURRENT_CATALOG "adbc.connection.catalog"
// #define ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA "adbc.connection.db_schema"
// #define ADBC_CONNECTION_OPTION_ISOLATION_LEVEL \

// #define ADBC_STATEMENT_OPTION_INCREMENTAL "adbc.statement.exec.incremental"
// #define ADBC_STATEMENT_OPTION_PROGRESS "adbc.statement.exec.progress"
// #define ADBC_STATEMENT_OPTION_MAX_PROGRESS "adbc.statement.exec.max_progress"

// #define ADBC_OPTION_ISOLATION_LEVEL_DEFAULT \
// #define ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED \
// #define ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED \
// #define ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ \
// #define ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT \
// #define ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE \
// #define ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE \

// #define ADBC_ERROR_1_0_0_SIZE (offsetof(struct AdbcError, private_data))
// #define ADBC_ERROR_1_1_0_SIZE (sizeof(struct AdbcError))
// #define ADBC_DRIVER_1_0_0_SIZE (offsetof(struct AdbcDriver, ErrorGetDetailCount))
// #define ADBC_DRIVER_1_1_0_SIZE (sizeof(struct AdbcDriver))
