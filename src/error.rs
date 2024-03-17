use std::ffi::NulError;

use arrow;

#[derive(Debug)]
pub enum Status {
    Ok,               // ADBC_STATUS_OK
    Unknown,          // ADBC_STATUS_UNKNOWN
    NotImplemented,   // ADBC_STATUS_NOT_IMPLEMENTED
    NotFound,         // ADBC_STATUS_NOT_FOUND
    AlreadyExists,    // ADBC_STATUS_ALREADY_EXISTS
    InvalidArguments, // ADBC_STATUS_INVALID_ARGUMENT
    InvalidState,     // ADBC_STATUS_INVALID_STATE
    InvalidData,      // ADBC_STATUS_INVALID_DATA
    Integrity,        // ADBC_STATUS_INTEGRITY
    Internal,         // ADBC_STATUS_INTERNAL
    IO,               // ADBC_STATUS_IO
    Cancelled,        // ADBC_STATUS_CANCELLED
    Timeout,          // ADBC_STATUS_TIMEOUT
    Unauthenticated,  // ADBC_STATUS_UNAUTHENTICATED
    Unauthorized,     // ADBC_STATUS_UNAUTHORIZED
}

#[derive(Debug)]
pub struct Error {
    pub(crate) message: Option<String>,
    pub(crate) status: Option<Status>,
    pub(crate) vendor_code: i32,
    pub(crate) sqlstate: [i8; 5],
    pub(crate) details: Option<Vec<(String, Vec<u8>)>>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<arrow::error::ArrowError> for Error {
    fn from(value: arrow::error::ArrowError) -> Self {
        Self {
            message: Some(value.to_string()),
            status: Some(Status::Internal),
            vendor_code: 0,
            sqlstate: [0; 5],
            details: None,
        }
    }
}

impl From<NulError> for Error {
    fn from(value: NulError) -> Self {
        Self {
            message: Some(format!(
                "Interior null byte was found at position {}",
                value.nul_position()
            )),
            status: Some(Status::InvalidData),
            vendor_code: 0,
            sqlstate: [0; 5],
            details: None,
        }
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Self {
            message: Some(value.into()),
            status: None,
            vendor_code: 0,
            sqlstate: [0; 5],
            details: None,
        }
    }
}

// impl std::error::Error for Error {} // TODO
