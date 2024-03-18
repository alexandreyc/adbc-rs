#![allow(non_camel_case_types, non_snake_case)]

use std::os::raw::{c_char, c_int};

use super::*;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;

// DatabaseInit
pub(crate) type FuncDatabaseInit =
    unsafe extern "C" fn(*mut FFI_AdbcDatabase, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn DatabaseInit(
    _: *mut FFI_AdbcDatabase,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// DatabaseNew
pub(crate) type FuncDatabaseNew =
    unsafe extern "C" fn(*mut FFI_AdbcDatabase, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn DatabaseNew(
    _: *mut FFI_AdbcDatabase,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// DatabaseSetOption
pub(crate) type FuncDatabaseSetOption = unsafe extern "C" fn(
    *mut FFI_AdbcDatabase,
    *const c_char,
    *const c_char,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn DatabaseSetOption(
    _: *mut FFI_AdbcDatabase,
    _: *const c_char,
    _: *const c_char,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// DatabaseRelease
pub(crate) type FuncDatabaseRelease =
    unsafe extern "C" fn(*mut FFI_AdbcDatabase, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn DatabaseRelease(
    _: *mut FFI_AdbcDatabase,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// ConnectionCommit
pub(crate) type FuncConnectionCommit =
    unsafe extern "C" fn(*mut FFI_AdbcConnection, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionCommit(
    _: *mut FFI_AdbcConnection,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// ConnectionGetInfo
pub(crate) type FuncConnectionGetInfo = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *const u32,
    usize,
    *mut FFI_ArrowArrayStream,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionGetInfo(
    _: *mut FFI_AdbcConnection,
    _: *const u32,
    _: usize, // TODO: is usize the good type? in C it's size_t
    _: *mut FFI_ArrowArrayStream,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// ConnectionGetObjects
pub(crate) type FuncConnectionGetObjects = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    c_int,
    *const c_char,
    *const c_char,
    *const c_char,
    *const *const c_char,
    *const c_char,
    *mut FFI_ArrowArrayStream,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionGetObjects(
    _: *mut FFI_AdbcConnection,
    _: c_int,
    _: *const c_char,
    _: *const c_char,
    _: *const c_char,
    _: *const *const c_char,
    _: *const c_char,
    _: *mut FFI_ArrowArrayStream,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// ConnectionGetTableSchema
pub(crate) type FuncConnectionGetTableSchema = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *const c_char,
    *const c_char,
    *const c_char,
    *mut FFI_ArrowSchema,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionGetTableSchema(
    _: *mut FFI_AdbcConnection,
    _: *const c_char,
    _: *const c_char,
    _: *const c_char,
    _: *mut FFI_ArrowSchema,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// ConnectionGetTableTypes
pub(crate) type FuncConnectionGetTableTypes = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *mut FFI_ArrowArrayStream,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionGetTableTypes(
    _: *mut FFI_AdbcConnection,
    _: *mut FFI_ArrowArrayStream,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// ConnectionInit
pub(crate) type FuncConnectionInit = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *mut FFI_AdbcDatabase,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionInit(
    _: *mut FFI_AdbcConnection,
    _: *mut FFI_AdbcDatabase,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// ConnectionNew
pub(crate) type FuncConnectionNew =
    unsafe extern "C" fn(*mut FFI_AdbcConnection, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionNew(
    _: *mut FFI_AdbcConnection,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// ConnectionSetOption
pub(crate) type FuncConnectionSetOption = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *const c_char,
    *const c_char,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionSetOption(
    _: *mut FFI_AdbcConnection,
    _: *const c_char,
    _: *const c_char,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// ConnectionReadPartition
pub(crate) type FuncConnectionReadPartition = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *const u8,
    usize,
    *mut FFI_ArrowArrayStream,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionReadPartition(
    _: *mut FFI_AdbcConnection,
    _: *const u8,
    _: usize,
    _: *mut FFI_ArrowArrayStream,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// ConnectionRelease
pub(crate) type FuncConnectionRelease =
    unsafe extern "C" fn(*mut FFI_AdbcConnection, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionRelease(
    _: *mut FFI_AdbcConnection,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// ConnectionRollback
pub(crate) type FuncConnectionRollback =
    unsafe extern "C" fn(*mut FFI_AdbcConnection, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionRollback(
    _: *mut FFI_AdbcConnection,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// StatementBind
pub(crate) type FuncStatementBind = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *mut FFI_ArrowArray,
    *mut FFI_ArrowSchema,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementBind(
    _: *mut FFI_AdbcStatement,
    _: *mut FFI_ArrowArray,
    _: *mut FFI_ArrowSchema,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// StatementBindStream
pub(crate) type FuncStatementBindStream = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *mut FFI_ArrowArrayStream,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementBindStream(
    _: *mut FFI_AdbcStatement,
    _: *mut FFI_ArrowArrayStream,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// StatementExecuteQuery
pub(crate) type FuncStatementExecuteQuery = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *mut FFI_ArrowArrayStream,
    *mut i64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementExecuteQuery(
    _: *mut FFI_AdbcStatement,
    _: *mut FFI_ArrowArrayStream,
    _: *mut i64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// StatementExecutePartitions
pub(crate) type FuncStatementExecutePartitions = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *mut FFI_ArrowSchema,
    *mut FFI_AdbcPartitions,
    *mut i64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementExecutePartitions(
    _: *mut FFI_AdbcStatement,
    _: *mut FFI_ArrowSchema,
    _: *mut FFI_AdbcPartitions,
    _: *mut i64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// StatementGetParameterSchema
pub(crate) type FuncStatementGetParameterSchema = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *mut FFI_ArrowSchema,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementGetParameterSchema(
    _: *mut FFI_AdbcStatement,
    _: *mut FFI_ArrowSchema,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// StatementNew
pub(crate) type FuncStatementNew = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *mut FFI_AdbcStatement,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementNew(
    _: *mut FFI_AdbcConnection,
    _: *mut FFI_AdbcStatement,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// AdbcStatusCode (*StatementPrepare)(struct AdbcStatement*, struct AdbcError*);
pub(crate) type FuncStatementPrepare =
    unsafe extern "C" fn(*mut FFI_AdbcStatement, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementPrepare(
    _: *mut FFI_AdbcStatement,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// AdbcStatusCode (*StatementRelease)(struct AdbcStatement*, struct AdbcError*);
pub(crate) type FuncStatementRelease =
    unsafe extern "C" fn(*mut FFI_AdbcStatement, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementRelease(
    _: *mut FFI_AdbcStatement,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// AdbcStatusCode (*StatementSetOption)(struct AdbcStatement*, const char*, const char*,
//          struct AdbcError*);
pub(crate) type FuncStatementSetOption = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *const c_char,
    *const c_char,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementSetOption(
    _: *mut FFI_AdbcStatement,
    _: *const c_char,
    _: *const c_char,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// AdbcStatusCode (*StatementSetSqlQuery)(struct AdbcStatement*, const char*,
//            struct AdbcError*);
pub(crate) type FuncStatementSetSqlQuery = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *const c_char,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementSetSqlQuery(
    _: *mut FFI_AdbcStatement,
    _: *const c_char,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// AdbcStatusCode (*StatementSetSubstraitPlan)(struct AdbcStatement*, const uint8_t*,
//                 size_t, struct AdbcError*);
pub(crate) type FuncStatementSetSubstraitPlan = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *const u8,
    usize,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementSetSubstraitPlan(
    _: *mut FFI_AdbcStatement,
    _: *const u8,
    _: usize,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

// From here, starts ADBC 1.1.0 only methods

// ErrorGetDetailCount
pub(crate) type FuncErrorGetDetailCount = unsafe extern "C" fn(*const FFI_AdbcError) -> c_int;
pub(crate) unsafe extern "C" fn ErrorGetDetailCount(_: *const FFI_AdbcError) -> c_int {
    0
}

// ErrorGetDetail
pub(crate) type FuncErrorGetDetail =
    unsafe extern "C" fn(*const FFI_AdbcError, c_int) -> FFI_AdbcErrorDetail;
pub(crate) unsafe extern "C" fn ErrorGetDetail(
    _: *const FFI_AdbcError,
    _: c_int,
) -> FFI_AdbcErrorDetail {
    FFI_AdbcErrorDetail::default()
}

// ErrorFromArrayStream
pub(crate) type FuncErrorFromArrayStream = unsafe extern "C" fn(
    *mut FFI_ArrowArrayStream,
    *mut FFI_AdbcStatusCode,
) -> *const FFI_AdbcError;
pub(crate) unsafe extern "C" fn ErrorFromArrayStream(
    _: *mut FFI_ArrowArrayStream,
    _: *mut FFI_AdbcStatusCode,
) -> *const FFI_AdbcError {
    std::ptr::null()
}

pub(crate) type FuncDatabaseGetOption = unsafe extern "C" fn(
    *mut FFI_AdbcDatabase,
    *const c_char,
    *mut c_char,
    *mut usize,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn DatabaseGetOption(
    _: *mut FFI_AdbcDatabase,
    _: *const c_char,
    _: *mut c_char,
    _: *mut usize,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncDatabaseGetOptionBytes = unsafe extern "C" fn(
    *mut FFI_AdbcDatabase,
    *const c_char,
    *mut u8,
    *mut usize,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn DatabaseGetOptionBytes(
    _: *mut FFI_AdbcDatabase,
    _: *const c_char,
    _: *mut u8,
    _: *mut usize,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncDatabaseGetOptionDouble = unsafe extern "C" fn(
    *mut FFI_AdbcDatabase,
    *const c_char,
    *mut f64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn DatabaseGetOptionDouble(
    _: *mut FFI_AdbcDatabase,
    _: *const c_char,
    _: *mut f64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncDatabaseGetOptionInt = unsafe extern "C" fn(
    *mut FFI_AdbcDatabase,
    *const c_char,
    *mut i64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn DatabaseGetOptionInt(
    _: *mut FFI_AdbcDatabase,
    _: *const c_char,
    _: *mut i64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncDatabaseSetOptionBytes = unsafe extern "C" fn(
    *mut FFI_AdbcDatabase,
    *const c_char,
    *const u8,
    usize,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn DatabaseSetOptionBytes(
    _: *mut FFI_AdbcDatabase,
    _: *const c_char,
    _: *const u8,
    _: usize,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncDatabaseSetOptionDouble = unsafe extern "C" fn(
    *mut FFI_AdbcDatabase,
    *const c_char,
    f64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn DatabaseSetOptionDouble(
    _: *mut FFI_AdbcDatabase,
    _: *const c_char,
    _: f64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncDatabaseSetOptionInt = unsafe extern "C" fn(
    *mut FFI_AdbcDatabase,
    *const c_char,
    i64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn DatabaseSetOptionInt(
    _: *mut FFI_AdbcDatabase,
    _: *const c_char,
    _: i64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncConnectionCancel =
    unsafe extern "C" fn(*mut FFI_AdbcConnection, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionCancel(
    _: *mut FFI_AdbcConnection,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncConnectionGetOption = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *const c_char,
    *mut c_char,
    *mut usize,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionGetOption(
    _: *mut FFI_AdbcConnection,
    _: *const c_char,
    _: *mut c_char,
    _: *mut usize,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncConnectionGetOptionBytes = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *const c_char,
    *mut u8,
    *mut usize,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionGetOptionBytes(
    _: *mut FFI_AdbcConnection,
    _: *const c_char,
    _: *mut u8,
    _: *mut usize,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncConnectionGetOptionDouble = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *const c_char,
    *mut f64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionGetOptionDouble(
    _: *mut FFI_AdbcConnection,
    _: *const c_char,
    _: *mut f64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncConnectionGetOptionInt = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *const c_char,
    *mut i64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionGetOptionInt(
    _: *mut FFI_AdbcConnection,
    _: *const c_char,
    _: *mut i64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncConnectionGetStatistics = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *const c_char,
    *const c_char,
    *const c_char,
    c_char,
    *mut FFI_ArrowArrayStream,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionGetStatistics(
    _: *mut FFI_AdbcConnection,
    _: *const c_char,
    _: *const c_char,
    _: *const c_char,
    _: c_char,
    _: *mut FFI_ArrowArrayStream,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncConnectionGetStatisticNames = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *mut FFI_ArrowArrayStream,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionGetStatisticNames(
    _: *mut FFI_AdbcConnection,
    _: *mut FFI_ArrowArrayStream,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncConnectionSetOptionBytes = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *const c_char,
    *const u8,
    usize,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionSetOptionBytes(
    _: *mut FFI_AdbcConnection,
    _: *const c_char,
    _: *const u8,
    _: usize,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncConnectionSetOptionDouble = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *const c_char,
    f64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionSetOptionDouble(
    _: *mut FFI_AdbcConnection,
    _: *const c_char,
    _: f64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncConnectionSetOptionInt = unsafe extern "C" fn(
    *mut FFI_AdbcConnection,
    *const c_char,
    i64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn ConnectionSetOptionInt(
    _: *mut FFI_AdbcConnection,
    _: *const c_char,
    _: i64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncStatementCancel =
    unsafe extern "C" fn(*mut FFI_AdbcStatement, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementCancel(
    _: *mut FFI_AdbcStatement,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncStatementExecuteSchema = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *mut FFI_ArrowSchema,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementExecuteSchema(
    _: *mut FFI_AdbcStatement,
    _: *mut FFI_ArrowSchema,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncStatementGetOption = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *const c_char,
    *mut c_char,
    *mut usize,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementGetOption(
    _: *mut FFI_AdbcStatement,
    _: *const c_char,
    _: *mut c_char,
    _: *mut usize,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncStatementGetOptionBytes = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *const c_char,
    *mut u8,
    *mut usize,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementGetOptionBytes(
    _: *mut FFI_AdbcStatement,
    _: *const c_char,
    _: *mut u8,
    _: *mut usize,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncStatementGetOptionDouble = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *const c_char,
    *mut f64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementGetOptionDouble(
    _: *mut FFI_AdbcStatement,
    _: *const c_char,
    _: *mut f64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncStatementGetOptionInt = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *const c_char,
    *mut i64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementGetOptionInt(
    _: *mut FFI_AdbcStatement,
    _: *const c_char,
    _: *mut i64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncStatementSetOptionBytes = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *const c_char,
    *const u8,
    usize,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementSetOptionBytes(
    _: *mut FFI_AdbcStatement,
    _: *const c_char,
    _: *const u8,
    _: usize,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncStatementSetOptionDouble = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *const c_char,
    f64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementSetOptionDouble(
    _: *mut FFI_AdbcStatement,
    _: *const c_char,
    _: f64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}

pub(crate) type FuncStatementSetOptionInt = unsafe extern "C" fn(
    *mut FFI_AdbcStatement,
    *const c_char,
    i64,
    *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode;
pub(crate) unsafe extern "C" fn StatementSetOptionInt(
    _: *mut FFI_AdbcStatement,
    _: *const c_char,
    _: i64,
    _: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    crate::ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED
}
