use std::os::raw::c_int;

use super::types::FFI_AdbcStatusCode;

pub(crate) const ADBC_STATUS_OK: FFI_AdbcStatusCode = 0;
pub(crate) const ADBC_STATUS_UNKNOWN: FFI_AdbcStatusCode = 1;
pub(crate) const ADBC_STATUS_NOT_IMPLEMENTED: FFI_AdbcStatusCode = 2;
pub(crate) const ADBC_STATUS_NOT_FOUND: FFI_AdbcStatusCode = 3;
pub(crate) const ADBC_STATUS_ALREADY_EXISTS: FFI_AdbcStatusCode = 4;
pub(crate) const ADBC_STATUS_INVALID_ARGUMENT: FFI_AdbcStatusCode = 5;
pub(crate) const ADBC_STATUS_INVALID_STATE: FFI_AdbcStatusCode = 6;
pub(crate) const ADBC_STATUS_INVALID_DATA: FFI_AdbcStatusCode = 7;
pub(crate) const ADBC_STATUS_INTEGRITY: FFI_AdbcStatusCode = 8;
pub(crate) const ADBC_STATUS_INTERNAL: FFI_AdbcStatusCode = 9;
pub(crate) const ADBC_STATUS_IO: FFI_AdbcStatusCode = 10;
pub(crate) const ADBC_STATUS_CANCELLED: FFI_AdbcStatusCode = 11;
pub(crate) const ADBC_STATUS_TIMEOUT: FFI_AdbcStatusCode = 12;
pub(crate) const ADBC_STATUS_UNAUTHENTICATED: FFI_AdbcStatusCode = 13;
pub(crate) const ADBC_STATUS_UNAUTHORIZED: FFI_AdbcStatusCode = 14;

pub(crate) const ADBC_VERSION_1_0_0: i32 = 1000000;
pub(crate) const ADBC_VERSION_1_1_0: i32 = 1001000;

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

pub(crate) const ADBC_INGEST_OPTION_TARGET_TABLE: &str = "adbc.ingest.target_table";
pub(crate) const ADBC_INGEST_OPTION_MODE: &str = "adbc.ingest.mode";

pub(crate) const ADBC_INGEST_OPTION_MODE_CREATE: &str = "adbc.ingest.mode.create";
pub(crate) const ADBC_INGEST_OPTION_MODE_APPEND: &str = "adbc.ingest.mode.append";
pub(crate) const ADBC_INGEST_OPTION_MODE_REPLACE: &str = "adbc.ingest.mode.replace";
pub(crate) const ADBC_INGEST_OPTION_MODE_CREATE_APPEND: &str = "adbc.ingest.mode.create_append";

pub(crate) const ADBC_OPTION_URI: &str = "uri";
pub(crate) const ADBC_OPTION_USERNAME: &str = "username";
pub(crate) const ADBC_OPTION_PASSWORD: &str = "password";

pub(crate) const ADBC_CONNECTION_OPTION_AUTOCOMMIT: &str = "adbc.connection.autocommit";
pub(crate) const ADBC_CONNECTION_OPTION_READ_ONLY: &str = "adbc.connection.readonly";
pub(crate) const ADBC_CONNECTION_OPTION_CURRENT_CATALOG: &str = "adbc.connection.catalog";
pub(crate) const ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA: &str = "adbc.connection.db_schema";
pub(crate) const ADBC_CONNECTION_OPTION_ISOLATION_LEVEL: &str =
    "adbc.connection.transaction.isolation_level";

pub(crate) const ADBC_STATEMENT_OPTION_INCREMENTAL: &str = "adbc.statement.exec.incremental";
pub(crate) const ADBC_STATEMENT_OPTION_PROGRESS: &str = "adbc.statement.exec.progress";
pub(crate) const ADBC_STATEMENT_OPTION_MAX_PROGRESS: &str = "adbc.statement.exec.max_progress";

pub(crate) const ADBC_OPTION_ISOLATION_LEVEL_DEFAULT: &str =
    "adbc.connection.transaction.isolation.default";
pub(crate) const ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED: &str =
    "adbc.connection.transaction.isolation.read_uncommitted";
pub(crate) const ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED: &str =
    "adbc.connection.transaction.isolation.read_committed";
pub(crate) const ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ: &str =
    "adbc.connection.transaction.isolation.repeatable_read";
pub(crate) const ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT: &str =
    "adbc.connection.transaction.isolation.snapshot";
pub(crate) const ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE: &str =
    "adbc.connection.transaction.isolation.serializable";
pub(crate) const ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE: &str =
    "adbc.connection.transaction.isolation.linearizable";

pub(crate) const ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_KEY: i16 = 0;
// #define ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_NAME "adbc.statistic.byte_width"

pub(crate) const ADBC_STATISTIC_DISTINCT_COUNT_KEY: i16 = 1;
// #define ADBC_STATISTIC_DISTINCT_COUNT_NAME "adbc.statistic.distinct_count"

pub(crate) const ADBC_STATISTIC_MAX_BYTE_WIDTH_KEY: i16 = 2;
// #define ADBC_STATISTIC_MAX_BYTE_WIDTH_NAME "adbc.statistic.byte_width"

pub(crate) const ADBC_STATISTIC_MAX_VALUE_KEY: i16 = 3;
// #define ADBC_STATISTIC_MAX_VALUE_NAME "adbc.statistic.byte_width"

pub(crate) const ADBC_STATISTIC_MIN_VALUE_KEY: i16 = 4;
// #define ADBC_STATISTIC_MIN_VALUE_NAME "adbc.statistic.byte_width"

pub(crate) const ADBC_STATISTIC_NULL_COUNT_KEY: i16 = 5;
// #define ADBC_STATISTIC_NULL_COUNT_NAME "adbc.statistic.null_count"

pub(crate) const ADBC_STATISTIC_ROW_COUNT_KEY: i16 = 6;
// #define ADBC_STATISTIC_ROW_COUNT_NAME "adbc.statistic.row_count"

// #define ADBC_OPTION_VALUE_ENABLED "true"
// #define ADBC_OPTION_VALUE_DISABLED "false"
// #define ADBC_ERROR_1_0_0_SIZE (offsetof(struct AdbcError, private_data))
// #define ADBC_ERROR_1_1_0_SIZE (sizeof(struct AdbcError))
// #define ADBC_DRIVER_1_0_0_SIZE (offsetof(struct AdbcDriver, ErrorGetDetailCount))
// #define ADBC_DRIVER_1_1_0_SIZE (sizeof(struct AdbcDriver))
