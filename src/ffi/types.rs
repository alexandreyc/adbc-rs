#![allow(non_camel_case_types, non_snake_case)]

use crate::driver_manager::check_status;
use crate::{error, ffi};
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::{null, null_mut};

use super::functions;

pub type FFI_AdbcStatusCode = u8;

pub type FFI_AdbcDriverInitFunc =
    unsafe extern "C" fn(c_int, *mut c_void, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcError {
    message: *const c_char,
    vendor_code: i32,
    sqlstate: [c_char; 5],
    release: Option<unsafe extern "C" fn(*const Self)>,
    /// Added in ADBC 1.1.0.
    private_data: *const c_void,
    /// Added in ADBC 1.1.0.
    private_driver: *const FFI_AdbcDriver,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcErrorDetail {
    /// The metadata key.
    key: *const c_char,
    /// The binary metadata value.
    value: *const u8,
    /// The length of the metadata value.
    value_length: usize,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcDatabase {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    private_data: *mut c_void,
    /// The associated driver (used by the driver manager to help track state).
    private_driver: *mut FFI_AdbcDriver,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcConnection {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    private_data: *const c_void,
    /// The associated driver (used by the driver manager to help track state).
    private_driver: *const FFI_AdbcDriver,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcStatement {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    private_data: *const c_void,
    /// The associated driver (used by the driver manager to help track state).
    private_driver: *const FFI_AdbcDriver,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcPartitions {
    /// The number of partitions.
    num_partitions: usize,

    /// The partitions of the result set, where each entry (up to
    /// num_partitions entries) is an opaque identifier that can be
    /// passed to AdbcConnectionReadPartition.
    partitions: *const *const u8,

    /// The length of each corresponding entry in partitions.
    // const size_t* partition_lengths;
    partition_lengths: *const usize,

    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    private_data: *mut c_void,

    /// Release the contained partitions.
    /// Unlike other structures, this is an embedded callback to make it
    /// easier for the driver manager and driver to cooperate.
    release: Option<unsafe extern "C" fn(*const Self)>,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcDriver {
    /// Opaque driver-defined state.
    /// This field is NULL if the driver is unintialized/freed (but
    /// it need not have a value even if the driver is initialized).
    private_data: *mut c_void,
    /// Opaque driver manager-defined state.
    /// This field is NULL if the driver is unintialized/freed (but
    /// it need not have a value even if the driver is initialized).
    private_manager: *mut c_void,

    release: Option<
        unsafe extern "C" fn(driver: *mut Self, error: *mut FFI_AdbcError) -> FFI_AdbcStatusCode,
    >,

    pub(crate) DatabaseInit: Option<functions::FuncDatabaseInit>,
    pub(crate) DatabaseNew: Option<functions::FuncDatabaseNew>,
    pub(crate) DatabaseSetOption: Option<functions::FuncDatabaseSetOption>,
    pub(crate) DatabaseRelease: Option<functions::FuncDatabaseRelease>,

    pub(crate) ConnectionCommit: Option<functions::FuncConnectionCommit>,
    pub(crate) ConnectionGetInfo: Option<functions::FuncConnectionGetInfo>,
    pub(crate) ConnectionGetObjects: Option<functions::FuncConnectionGetObjects>,
    pub(crate) ConnectionGetTableSchema: Option<functions::FuncConnectionGetTableSchema>,
    pub(crate) ConnectionGetTableTypes: Option<functions::FuncConnectionGetTableTypes>,
    pub(crate) ConnectionInit: Option<functions::FuncConnectionInit>,
    pub(crate) ConnectionNew: Option<functions::FuncConnectionNew>,
    pub(crate) ConnectionSetOption: Option<functions::FuncConnectionSetOption>,
    pub(crate) ConnectionReadPartition: Option<functions::FuncConnectionReadPartition>,
    pub(crate) ConnectionRelease: Option<functions::FuncConnectionRelease>,
    pub(crate) ConnectionRollback: Option<functions::FuncConnectionRollback>,

    pub(crate) StatementBind: Option<functions::FuncStatementBind>,
    pub(crate) StatementBindStream: Option<functions::FuncStatementBindStream>,
    pub(crate) StatementExecuteQuery: Option<functions::FuncStatementExecuteQuery>,
    pub(crate) StatementExecutePartitions: Option<functions::FuncStatementExecutePartitions>,
    pub(crate) StatementGetParameterSchema: Option<functions::FuncStatementGetParameterSchema>,
    pub(crate) StatementNew: Option<functions::FuncStatementNew>,
    pub(crate) StatementPrepare: Option<functions::FuncStatementPrepare>,
    pub(crate) StatementRelease: Option<functions::FuncStatementRelease>,
    pub(crate) StatementSetOption: Option<functions::FuncStatementSetOption>,
    pub(crate) StatementSetSqlQuery: Option<functions::FuncStatementSetSqlQuery>,
    pub(crate) StatementSetSubstraitPlan: Option<functions::FuncStatementSetSubstraitPlan>,

    pub(crate) ErrorGetDetailCount: Option<functions::FuncErrorGetDetailCount>,
    pub(crate) ErrorGetDetail: Option<functions::FuncErrorGetDetail>,
    pub(crate) ErrorFromArrayStream: Option<functions::FuncErrorFromArrayStream>,

    pub(crate) DatabaseGetOption: Option<functions::FuncDatabaseGetOption>,
    pub(crate) DatabaseGetOptionBytes: Option<functions::FuncDatabaseGetOptionBytes>,
    pub(crate) DatabaseGetOptionDouble: Option<functions::FuncDatabaseGetOptionDouble>,
    pub(crate) DatabaseGetOptionInt: Option<functions::FuncDatabaseGetOptionInt>,
    pub(crate) DatabaseSetOptionBytes: Option<functions::FuncDatabaseSetOptionBytes>,
    pub(crate) DatabaseSetOptionDouble: Option<functions::FuncDatabaseSetOptionDouble>,
    pub(crate) DatabaseSetOptionInt: Option<functions::FuncDatabaseSetOptionInt>,
    pub(crate) ConnectionCancel: Option<functions::FuncConnectionCancel>,
    pub(crate) ConnectionGetOption: Option<functions::FuncConnectionGetOption>,
    pub(crate) ConnectionGetOptionBytes: Option<functions::FuncConnectionGetOptionBytes>,
    pub(crate) ConnectionGetOptionDouble: Option<functions::FuncConnectionGetOptionDouble>,
    pub(crate) ConnectionGetOptionInt: Option<functions::FuncConnectionGetOptionInt>,
    pub(crate) ConnectionGetStatistics: Option<functions::FuncConnectionGetStatistics>,
    pub(crate) ConnectionGetStatisticNames: Option<functions::FuncConnectionGetStatisticNames>,
    pub(crate) ConnectionSetOptionBytes: Option<functions::FuncConnectionSetOptionBytes>,
    pub(crate) ConnectionSetOptionDouble: Option<functions::FuncConnectionSetOptionDouble>,
    pub(crate) ConnectionSetOptionInt: Option<functions::FuncConnectionSetOptionInt>,
    pub(crate) StatementCancel: Option<functions::FuncStatementCancel>,
    pub(crate) StatementExecuteSchema: Option<functions::FuncStatementExecuteSchema>,
    pub(crate) StatementGetOption: Option<functions::FuncStatementGetOption>,
    pub(crate) StatementGetOptionBytes: Option<functions::FuncStatementGetOptionBytes>,
    pub(crate) StatementGetOptionDouble: Option<functions::FuncStatementGetOptionDouble>,
    pub(crate) StatementGetOptionInt: Option<functions::FuncStatementGetOptionInt>,
    pub(crate) StatementSetOptionBytes: Option<functions::FuncStatementSetOptionBytes>,
    pub(crate) StatementSetOptionDouble: Option<functions::FuncStatementSetOptionDouble>,
    pub(crate) StatementSetOptionInt: Option<functions::FuncStatementSetOptionInt>,
}

#[macro_export]
macro_rules! driver_method {
    ($driver:expr, $method:ident) => {
        $driver.$method.unwrap_or(crate::ffi::functions::$method)
    };
}

impl From<FFI_AdbcStatusCode> for error::Status {
    fn from(value: FFI_AdbcStatusCode) -> Self {
        match value {
            ffi::constants::ADBC_STATUS_OK => error::Status::Ok,
            ffi::constants::ADBC_STATUS_UNKNOWN => error::Status::Unknown,
            ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED => error::Status::NotImplemented,
            ffi::constants::ADBC_STATUS_NOT_FOUND => error::Status::NotFound,
            ffi::constants::ADBC_STATUS_ALREADY_EXISTS => error::Status::AlreadyExists,
            ffi::constants::ADBC_STATUS_INVALID_ARGUMENT => error::Status::InvalidArguments,
            ffi::constants::ADBC_STATUS_INVALID_STATE => error::Status::InvalidState,
            ffi::constants::ADBC_STATUS_INVALID_DATA => error::Status::InvalidData,
            ffi::constants::ADBC_STATUS_INTEGRITY => error::Status::Integrity,
            ffi::constants::ADBC_STATUS_INTERNAL => error::Status::Internal,
            ffi::constants::ADBC_STATUS_IO => error::Status::IO,
            ffi::constants::ADBC_STATUS_CANCELLED => error::Status::Cancelled,
            ffi::constants::ADBC_STATUS_TIMEOUT => error::Status::Timeout,
            ffi::constants::ADBC_STATUS_UNAUTHENTICATED => error::Status::Unauthenticated,
            ffi::constants::ADBC_STATUS_UNAUTHORIZED => error::Status::Unauthorized,
            _ => panic!("Invalid ADBC status code value: {}", value),
        }
    }
}

impl Default for FFI_AdbcDriver {
    fn default() -> Self {
        Self {
            private_data: null_mut(),
            private_manager: null_mut(),
            release: None, // TODO: change this value?

            DatabaseInit: None,
            DatabaseNew: None,
            DatabaseSetOption: None,
            DatabaseRelease: None,
            ConnectionCommit: None,
            ConnectionGetInfo: None,
            ConnectionGetObjects: None,
            ConnectionGetTableSchema: None,
            ConnectionGetTableTypes: None,
            ConnectionInit: None,
            ConnectionNew: None,
            ConnectionSetOption: None,
            ConnectionReadPartition: None,
            ConnectionRelease: None,
            ConnectionRollback: None,

            StatementBind: None,
            StatementBindStream: None,
            StatementExecuteQuery: None,
            StatementExecutePartitions: None,
            StatementGetParameterSchema: None,
            StatementNew: None,
            StatementPrepare: None,
            StatementRelease: None,
            StatementSetOption: None,
            StatementSetSqlQuery: None,
            StatementSetSubstraitPlan: None,

            ErrorGetDetailCount: None,
            ErrorGetDetail: None,
            ErrorFromArrayStream: None,

            DatabaseGetOption: None,
            DatabaseGetOptionBytes: None,
            DatabaseGetOptionDouble: None,
            DatabaseGetOptionInt: None,
            DatabaseSetOptionBytes: None,
            DatabaseSetOptionDouble: None,
            DatabaseSetOptionInt: None,
            ConnectionCancel: None,
            ConnectionGetOption: None,
            ConnectionGetOptionBytes: None,
            ConnectionGetOptionDouble: None,
            ConnectionGetOptionInt: None,
            ConnectionGetStatistics: None,
            ConnectionGetStatisticNames: None,
            ConnectionSetOptionBytes: None,
            ConnectionSetOptionDouble: None,
            ConnectionSetOptionInt: None,
            StatementCancel: None,
            StatementExecuteSchema: None,
            StatementGetOption: None,
            StatementGetOptionBytes: None,
            StatementGetOptionDouble: None,
            StatementGetOptionInt: None,
            StatementSetOptionBytes: None,
            StatementSetOptionDouble: None,
            StatementSetOptionInt: None,
        }
    }
}

impl Default for FFI_AdbcError {
    fn default() -> Self {
        Self {
            message: null(),
            vendor_code: ffi::constants::ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA,
            sqlstate: [0; 5],
            release: None, // TODO: is this correct?
            private_data: null(),
            private_driver: null(),
        }
    }
}

impl Default for FFI_AdbcDatabase {
    fn default() -> Self {
        Self {
            private_data: null_mut(),
            private_driver: null_mut(),
        }
    }
}

impl Default for FFI_AdbcConnection {
    fn default() -> Self {
        Self {
            private_data: null_mut(),
            private_driver: null_mut(),
        }
    }
}

impl Default for FFI_AdbcErrorDetail {
    fn default() -> Self {
        Self {
            key: null(),
            value: null(),
            value_length: 0,
        }
    }
}

impl Default for FFI_AdbcStatement {
    fn default() -> Self {
        Self {
            private_data: null(),
            private_driver: null(),
        }
    }
}

impl From<FFI_AdbcError> for error::Error {
    fn from(value: FFI_AdbcError) -> Self {
        let message = match value.message.is_null() {
            true => None,
            false => {
                let message = unsafe { CStr::from_ptr(value.message) };
                Some(message.to_string_lossy().to_string())
            }
        };

        let mut error = error::Error {
            message,
            status: None,
            vendor_code: value.vendor_code,
            sqlstate: value.sqlstate,
            details: None,
        };

        if value.vendor_code == ffi::constants::ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA {
            if let Some(driver) = unsafe { value.private_driver.as_ref() } {
                if let Some(get_detail_count) = driver.ErrorGetDetailCount {
                    if let Some(get_detail) = driver.ErrorGetDetail {
                        let num_details = unsafe { get_detail_count(&value) };
                        let details = (0..num_details)
                            .map(|i| {
                                let detail = unsafe { get_detail(&value, i) };
                                // TODO: should we check that detail.key != NULL?
                                let key = unsafe { CStr::from_ptr(detail.key) }
                                    .to_string_lossy()
                                    .to_string();
                                let value = unsafe {
                                    std::slice::from_raw_parts(detail.value, detail.value_length)
                                };
                                (key, value.to_vec())
                            })
                            .collect();
                        error.details = Some(details);
                    }
                }
            }
        }

        error
    }
}

impl Drop for FFI_AdbcError {
    fn drop(&mut self) {
        if let Some(release) = self.release {
            unsafe { release(self) };
        }
    }
}

impl Drop for FFI_AdbcDriver {
    fn drop(&mut self) {
        if let Some(release) = self.release {
            let mut error = ffi::FFI_AdbcError::default();
            let status = unsafe { release(self, &mut error) };
            if let Err(err) = check_status(status, error) {
                panic!("unable to drop driver: {:?}", err);
            }
        }
    }
}

impl Drop for FFI_AdbcPartitions {
    fn drop(&mut self) {
        if let Some(release) = self.release {
            unsafe { release(self) };
        }
    }
}
