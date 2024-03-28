#![allow(non_camel_case_types, non_snake_case)]

use std::ffi::CStr;
use std::ops::Deref;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::{null, null_mut};

use super::methods;
use crate::driver_manager::check_status;
use crate::{error, ffi, Partitions};

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
    pub(crate) private_driver: *const FFI_AdbcDriver,
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
    private_data: *const c_void,
    /// The associated driver (used by the driver manager to help track state).
    pub(crate) private_driver: *const FFI_AdbcDriver,
}

unsafe impl Send for FFI_AdbcDatabase {}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcConnection {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    private_data: *const c_void,
    /// The associated driver (used by the driver manager to help track state).
    pub(crate) private_driver: *const FFI_AdbcDriver,
}

unsafe impl Send for FFI_AdbcConnection {}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcStatement {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    private_data: *const c_void,
    /// The associated driver (used by the driver manager to help track state).
    pub(crate) private_driver: *const FFI_AdbcDriver,
}

unsafe impl Send for FFI_AdbcStatement {}

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
    private_data: *const c_void,

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
    private_data: *const c_void,
    /// Opaque driver manager-defined state.
    /// This field is NULL if the driver is unintialized/freed (but
    /// it need not have a value even if the driver is initialized).
    private_manager: *const c_void,

    release: Option<
        unsafe extern "C" fn(driver: *mut Self, error: *mut FFI_AdbcError) -> FFI_AdbcStatusCode,
    >,

    pub(crate) DatabaseInit: Option<methods::FuncDatabaseInit>,
    pub(crate) DatabaseNew: Option<methods::FuncDatabaseNew>,
    pub(crate) DatabaseSetOption: Option<methods::FuncDatabaseSetOption>,
    pub(crate) DatabaseRelease: Option<methods::FuncDatabaseRelease>,

    pub(crate) ConnectionCommit: Option<methods::FuncConnectionCommit>,
    pub(crate) ConnectionGetInfo: Option<methods::FuncConnectionGetInfo>,
    pub(crate) ConnectionGetObjects: Option<methods::FuncConnectionGetObjects>,
    pub(crate) ConnectionGetTableSchema: Option<methods::FuncConnectionGetTableSchema>,
    pub(crate) ConnectionGetTableTypes: Option<methods::FuncConnectionGetTableTypes>,
    pub(crate) ConnectionInit: Option<methods::FuncConnectionInit>,
    pub(crate) ConnectionNew: Option<methods::FuncConnectionNew>,
    pub(crate) ConnectionSetOption: Option<methods::FuncConnectionSetOption>,
    pub(crate) ConnectionReadPartition: Option<methods::FuncConnectionReadPartition>,
    pub(crate) ConnectionRelease: Option<methods::FuncConnectionRelease>,
    pub(crate) ConnectionRollback: Option<methods::FuncConnectionRollback>,

    pub(crate) StatementBind: Option<methods::FuncStatementBind>,
    pub(crate) StatementBindStream: Option<methods::FuncStatementBindStream>,
    pub(crate) StatementExecuteQuery: Option<methods::FuncStatementExecuteQuery>,
    pub(crate) StatementExecutePartitions: Option<methods::FuncStatementExecutePartitions>,
    pub(crate) StatementGetParameterSchema: Option<methods::FuncStatementGetParameterSchema>,
    pub(crate) StatementNew: Option<methods::FuncStatementNew>,
    pub(crate) StatementPrepare: Option<methods::FuncStatementPrepare>,
    pub(crate) StatementRelease: Option<methods::FuncStatementRelease>,
    pub(crate) StatementSetOption: Option<methods::FuncStatementSetOption>,
    pub(crate) StatementSetSqlQuery: Option<methods::FuncStatementSetSqlQuery>,
    pub(crate) StatementSetSubstraitPlan: Option<methods::FuncStatementSetSubstraitPlan>,

    pub(crate) ErrorGetDetailCount: Option<methods::FuncErrorGetDetailCount>,
    pub(crate) ErrorGetDetail: Option<methods::FuncErrorGetDetail>,
    pub(crate) ErrorFromArrayStream: Option<methods::FuncErrorFromArrayStream>,

    pub(crate) DatabaseGetOption: Option<methods::FuncDatabaseGetOption>,
    pub(crate) DatabaseGetOptionBytes: Option<methods::FuncDatabaseGetOptionBytes>,
    pub(crate) DatabaseGetOptionDouble: Option<methods::FuncDatabaseGetOptionDouble>,
    pub(crate) DatabaseGetOptionInt: Option<methods::FuncDatabaseGetOptionInt>,
    pub(crate) DatabaseSetOptionBytes: Option<methods::FuncDatabaseSetOptionBytes>,
    pub(crate) DatabaseSetOptionDouble: Option<methods::FuncDatabaseSetOptionDouble>,
    pub(crate) DatabaseSetOptionInt: Option<methods::FuncDatabaseSetOptionInt>,
    pub(crate) ConnectionCancel: Option<methods::FuncConnectionCancel>,
    pub(crate) ConnectionGetOption: Option<methods::FuncConnectionGetOption>,
    pub(crate) ConnectionGetOptionBytes: Option<methods::FuncConnectionGetOptionBytes>,
    pub(crate) ConnectionGetOptionDouble: Option<methods::FuncConnectionGetOptionDouble>,
    pub(crate) ConnectionGetOptionInt: Option<methods::FuncConnectionGetOptionInt>,
    pub(crate) ConnectionGetStatistics: Option<methods::FuncConnectionGetStatistics>,
    pub(crate) ConnectionGetStatisticNames: Option<methods::FuncConnectionGetStatisticNames>,
    pub(crate) ConnectionSetOptionBytes: Option<methods::FuncConnectionSetOptionBytes>,
    pub(crate) ConnectionSetOptionDouble: Option<methods::FuncConnectionSetOptionDouble>,
    pub(crate) ConnectionSetOptionInt: Option<methods::FuncConnectionSetOptionInt>,
    pub(crate) StatementCancel: Option<methods::FuncStatementCancel>,
    pub(crate) StatementExecuteSchema: Option<methods::FuncStatementExecuteSchema>,
    pub(crate) StatementGetOption: Option<methods::FuncStatementGetOption>,
    pub(crate) StatementGetOptionBytes: Option<methods::FuncStatementGetOptionBytes>,
    pub(crate) StatementGetOptionDouble: Option<methods::FuncStatementGetOptionDouble>,
    pub(crate) StatementGetOptionInt: Option<methods::FuncStatementGetOptionInt>,
    pub(crate) StatementSetOptionBytes: Option<methods::FuncStatementSetOptionBytes>,
    pub(crate) StatementSetOptionDouble: Option<methods::FuncStatementSetOptionDouble>,
    pub(crate) StatementSetOptionInt: Option<methods::FuncStatementSetOptionInt>,
}

unsafe impl Send for FFI_AdbcDriver {}

#[macro_export]
macro_rules! driver_method {
    ($driver:expr, $method:ident) => {
        $driver
            .deref()
            .$method
            .unwrap_or(crate::ffi::methods::$method)
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

impl From<FFI_AdbcPartitions> for Partitions {
    fn from(value: FFI_AdbcPartitions) -> Self {
        let mut partitions = Vec::with_capacity(value.num_partitions);
        for p in 0..value.num_partitions {
            let partition = unsafe {
                let ptr = (*value.partitions).add(p);
                let len = *value.partition_lengths.add(p);
                std::slice::from_raw_parts(ptr, len)
            };
            partitions.push(partition.to_vec());
        }
        partitions
    }
}

impl Default for FFI_AdbcDriver {
    fn default() -> Self {
        Self {
            private_data: null_mut(),
            private_manager: null_mut(),
            release: None,
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
            release: None,
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

impl Default for FFI_AdbcPartitions {
    fn default() -> Self {
        Self {
            num_partitions: 0,
            partitions: null(),
            partition_lengths: null(),
            private_data: null_mut(),
            release: None,
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
                let get_detail_count = driver_method!(driver, ErrorGetDetailCount);
                let get_detail = driver_method!(driver, ErrorGetDetail);
                let num_details = unsafe { get_detail_count(&value) };
                let details = (0..num_details)
                    .map(|i| unsafe { get_detail(&value, i) })
                    .filter(|d| !d.key.is_null() && !d.value.is_null())
                    .map(|d| unsafe {
                        let key = CStr::from_ptr(d.key).to_string_lossy().to_string();
                        let value = std::slice::from_raw_parts(d.value, d.value_length);
                        (key, value.to_vec())
                    })
                    .collect();
                error.details = Some(details);
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
