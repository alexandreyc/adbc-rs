#![allow(non_camel_case_types, non_snake_case)]

use std::ffi::{CStr, CString};
use std::mem::ManuallyDrop;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::{null, null_mut};

use super::methods;
use crate::{error, ffi, Partitions};

pub type FFI_AdbcStatusCode = u8;

/// A driver initialization function.
pub type FFI_AdbcDriverInitFunc =
    unsafe extern "C" fn(c_int, *mut c_void, *mut FFI_AdbcError) -> FFI_AdbcStatusCode;

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcError {
    message: *mut c_char,
    vendor_code: i32,
    sqlstate: [c_char; 5],
    release: Option<unsafe extern "C" fn(*mut Self)>,
    /// Added in ADBC 1.1.0.
    pub(crate) private_data: *mut c_void,
    /// Added in ADBC 1.1.0.
    pub private_driver: *const FFI_AdbcDriver,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcErrorDetail {
    /// The metadata key.
    pub(crate) key: *const c_char,
    /// The binary metadata value.
    pub(crate) value: *const u8,
    /// The length of the metadata value.
    pub(crate) value_length: usize,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcDatabase {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    pub(crate) private_data: *mut c_void,
    /// The associated driver (used by the driver manager to help track state).
    pub(crate) private_driver: *const FFI_AdbcDriver,
}

unsafe impl Send for FFI_AdbcDatabase {}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcConnection {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    pub(crate) private_data: *mut c_void,
    /// The associated driver (used by the driver manager to help track state).
    pub(crate) private_driver: *const FFI_AdbcDriver,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcStatement {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    pub(crate) private_data: *mut c_void,
    /// The associated driver (used by the driver manager to help track state).
    pub(crate) private_driver: *const FFI_AdbcDriver,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcPartitions {
    /// The number of partitions.
    num_partitions: usize,

    /// The partitions of the result set, where each entry (up to
    /// num_partitions entries) is an opaque identifier that can be
    /// passed to AdbcConnectionReadPartition.
    // It's defined as a const const pointer in C but we need to release it so it
    // probably needs to be mutable.
    partitions: *mut *mut u8,

    /// The length of each corresponding entry in partitions.
    // It's defined as a const pointer in C but we need to release it so it
    // probably needs to be mutable.
    partition_lengths: *mut usize,

    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    pub(crate) private_data: *mut c_void,

    /// Release the contained partitions.
    /// Unlike other structures, this is an embedded callback to make it
    /// easier for the driver manager and driver to cooperate.
    release: Option<unsafe extern "C" fn(*mut Self)>,
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_AdbcDriver {
    /// Opaque driver-defined state.
    /// This field is NULL if the driver is unintialized/freed (but
    /// it need not have a value even if the driver is initialized).
    pub(crate) private_data: *mut c_void,
    /// Opaque driver manager-defined state.
    /// This field is NULL if the driver is unintialized/freed (but
    /// it need not have a value even if the driver is initialized).
    pub(crate) private_manager: *const c_void,

    pub(crate) release: Option<
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
unsafe impl Sync for FFI_AdbcDriver {}

macro_rules! driver_method {
    ($driver:expr, $method:ident) => {
        $driver.$method.unwrap_or(crate::ffi::methods::$method)
    };
}

pub(crate) use driver_method;

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
            _ => panic!("Invalid ADBC status code value: {value}"),
        }
    }
}

impl From<error::Status> for FFI_AdbcStatusCode {
    fn from(value: error::Status) -> Self {
        match value {
            error::Status::Ok => ffi::constants::ADBC_STATUS_OK,
            error::Status::Unknown => ffi::constants::ADBC_STATUS_UNKNOWN,
            error::Status::NotImplemented => ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED,
            error::Status::NotFound => ffi::constants::ADBC_STATUS_NOT_FOUND,
            error::Status::AlreadyExists => ffi::constants::ADBC_STATUS_ALREADY_EXISTS,
            error::Status::InvalidArguments => ffi::constants::ADBC_STATUS_INVALID_ARGUMENT,
            error::Status::InvalidState => ffi::constants::ADBC_STATUS_INVALID_STATE,
            error::Status::InvalidData => ffi::constants::ADBC_STATUS_INVALID_DATA,
            error::Status::Integrity => ffi::constants::ADBC_STATUS_INTEGRITY,
            error::Status::Internal => ffi::constants::ADBC_STATUS_INTERNAL,
            error::Status::IO => ffi::constants::ADBC_STATUS_IO,
            error::Status::Cancelled => ffi::constants::ADBC_STATUS_CANCELLED,
            error::Status::Timeout => ffi::constants::ADBC_STATUS_TIMEOUT,
            error::Status::Unauthenticated => ffi::constants::ADBC_STATUS_UNAUTHENTICATED,
            error::Status::Unauthorized => ffi::constants::ADBC_STATUS_UNAUTHORIZED,
        }
    }
}

impl From<&error::Status> for FFI_AdbcStatusCode {
    fn from(value: &error::Status) -> Self {
        match value {
            error::Status::Ok => ffi::constants::ADBC_STATUS_OK,
            error::Status::Unknown => ffi::constants::ADBC_STATUS_UNKNOWN,
            error::Status::NotImplemented => ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED,
            error::Status::NotFound => ffi::constants::ADBC_STATUS_NOT_FOUND,
            error::Status::AlreadyExists => ffi::constants::ADBC_STATUS_ALREADY_EXISTS,
            error::Status::InvalidArguments => ffi::constants::ADBC_STATUS_INVALID_ARGUMENT,
            error::Status::InvalidState => ffi::constants::ADBC_STATUS_INVALID_STATE,
            error::Status::InvalidData => ffi::constants::ADBC_STATUS_INVALID_DATA,
            error::Status::Integrity => ffi::constants::ADBC_STATUS_INTEGRITY,
            error::Status::Internal => ffi::constants::ADBC_STATUS_INTERNAL,
            error::Status::IO => ffi::constants::ADBC_STATUS_IO,
            error::Status::Cancelled => ffi::constants::ADBC_STATUS_CANCELLED,
            error::Status::Timeout => ffi::constants::ADBC_STATUS_TIMEOUT,
            error::Status::Unauthenticated => ffi::constants::ADBC_STATUS_UNAUTHENTICATED,
            error::Status::Unauthorized => ffi::constants::ADBC_STATUS_UNAUTHORIZED,
        }
    }
}

impl From<FFI_AdbcPartitions> for Partitions {
    fn from(value: FFI_AdbcPartitions) -> Self {
        let mut partitions = Vec::with_capacity(value.num_partitions);
        for p in 0..value.num_partitions {
            let partition = unsafe {
                let ptr = *value.partitions.add(p);
                let len = *value.partition_lengths.add(p);
                std::slice::from_raw_parts(ptr, len)
            };
            partitions.push(partition.to_vec());
        }
        partitions
    }
}

// Taken from `Vec::into_raw_parts` which is currently nightly-only.
fn vec_into_raw_parts<T>(data: Vec<T>) -> (*mut T, usize, usize) {
    let mut md = ManuallyDrop::new(data);
    (md.as_mut_ptr(), md.len(), md.capacity())
}

// We need to store capacities to correctly release memory because the C API
// only stores lengths and so capacties are lost in translation.
struct PartitionsPrivateData {
    partitions_capacity: usize,
    partition_lengths_capacity: usize,
    partition_capacities: Vec<usize>,
}

impl From<Partitions> for FFI_AdbcPartitions {
    fn from(value: Partitions) -> Self {
        let num_partitions = value.len();
        let mut partition_lengths = Vec::with_capacity(num_partitions);
        let mut partition_capacities = Vec::with_capacity(num_partitions);
        let mut partition_ptrs = Vec::with_capacity(num_partitions);

        for partition in value.into_iter() {
            let (partition_ptr, partition_len, partition_cap) = vec_into_raw_parts(partition);
            partition_lengths.push(partition_len);
            partition_capacities.push(partition_cap);
            partition_ptrs.push(partition_ptr);
        }

        let (partition_lengths_ptr, _, partition_lengths_cap) =
            vec_into_raw_parts(partition_lengths);
        let (partitions_ptr, _, partitions_cap) = vec_into_raw_parts(partition_ptrs);
        let private_data = Box::new(PartitionsPrivateData {
            partitions_capacity: partitions_cap,
            partition_lengths_capacity: partition_lengths_cap,
            partition_capacities,
        });

        FFI_AdbcPartitions {
            num_partitions,
            partition_lengths: partition_lengths_ptr,
            partitions: partitions_ptr,
            private_data: Box::into_raw(private_data) as *mut c_void,
            release: Some(release_ffi_partitions),
        }
    }
}

unsafe extern "C" fn release_ffi_partitions(partitions: *mut FFI_AdbcPartitions) {
    match partitions.as_mut() {
        None => (),
        Some(partitions) => {
            let private_data = Box::from_raw(partitions.private_data as *mut PartitionsPrivateData);

            let partition_lengths = Vec::from_raw_parts(
                partitions.partition_lengths,
                partitions.num_partitions,
                private_data.partition_lengths_capacity,
            );
            drop(partition_lengths);

            for p in 0..partitions.num_partitions {
                let partition_ptr = *partitions.partitions.add(p);
                let partition_len = *partitions.partition_lengths.add(p);
                let partition_cap = private_data.partition_capacities[p];
                let partition = Vec::from_raw_parts(partition_ptr, partition_len, partition_cap);
                drop(partition);
            }

            let partitions_vec = Vec::from_raw_parts(
                partitions.partitions,
                partitions.num_partitions,
                private_data.partitions_capacity,
            );
            drop(partitions_vec);

            drop(private_data);
        }
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
            message: null_mut(),
            vendor_code: ffi::constants::ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA,
            sqlstate: [0; 5],
            release: None,
            private_data: null_mut(),
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
            private_data: null_mut(),
            private_driver: null(),
        }
    }
}

impl Default for FFI_AdbcPartitions {
    fn default() -> Self {
        Self {
            num_partitions: 0,
            partitions: null_mut(),
            partition_lengths: null_mut(),
            private_data: null_mut(),
            release: None,
        }
    }
}

impl FFI_AdbcError {
    pub fn with_driver(driver: *const FFI_AdbcDriver) -> Self {
        FFI_AdbcError {
            private_driver: driver,
            ..Default::default()
        }
    }
}

impl TryFrom<&FFI_AdbcError> for error::Error {
    type Error = error::Error;

    fn try_from(value: &FFI_AdbcError) -> Result<Self, Self::Error> {
        let message = match value.message.is_null() {
            true => "<empty>".to_string(),
            false => {
                let message = unsafe { CStr::from_ptr(value.message) };
                let message = message.to_owned();
                message.into_string()?
            }
        };

        let mut error = error::Error {
            message,
            status: error::Status::Unknown,
            vendor_code: value.vendor_code,
            sqlstate: value.sqlstate,
            details: None,
        };

        if value.vendor_code == ffi::constants::ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA {
            if let Some(driver) = unsafe { value.private_driver.as_ref() } {
                let get_detail_count = driver_method!(driver, ErrorGetDetailCount);
                let get_detail = driver_method!(driver, ErrorGetDetail);
                let num_details = unsafe { get_detail_count(value) };
                let details = (0..num_details)
                    .map(|i| unsafe { get_detail(value, i) })
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

        Ok(error)
    }
}

impl TryFrom<FFI_AdbcError> for error::Error {
    type Error = error::Error;

    fn try_from(value: FFI_AdbcError) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

// Invariant: keys.len() == values.len()
pub(crate) struct ErrorPrivateData {
    pub(crate) keys: Vec<CString>,
    pub(crate) values: Vec<Vec<u8>>,
}

impl TryFrom<error::Error> for FFI_AdbcError {
    type Error = error::Error;

    fn try_from(mut value: error::Error) -> Result<Self, Self::Error> {
        let message = CString::new(value.message)?;

        let private_data = match value.details.take() {
            None => null_mut(),
            Some(details) => {
                let keys: Result<Vec<CString>, _> = details
                    .iter()
                    .map(|(key, _)| CString::new(key.as_str()))
                    .collect();
                let values: Vec<Vec<u8>> = details.into_iter().map(|(_, value)| value).collect();

                let private_data = Box::new(ErrorPrivateData {
                    keys: keys?,
                    values,
                });
                let private_data = Box::into_raw(private_data);
                private_data as *mut c_void
            }
        };

        Ok(FFI_AdbcError {
            message: message.into_raw(),
            release: Some(release_ffi_error),
            vendor_code: value.vendor_code,
            sqlstate: value.sqlstate,
            private_data,
            private_driver: null(),
        })
    }
}

unsafe extern "C" fn release_ffi_error(error: *mut FFI_AdbcError) {
    match error.as_mut() {
        None => (),
        Some(error) => {
            drop(CString::from_raw(error.message));

            if !error.private_data.is_null() {
                let private_data = Box::from_raw(error.private_data as *mut ErrorPrivateData);
                drop(private_data);
            }
        }
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
            if let Err(err) = ffi::check_status(status, error) {
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
