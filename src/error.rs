use std::{ffi::NulError, fmt::Display};

use arrow::error::ArrowError;

#[derive(Debug, PartialEq, Eq)]
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
    pub message: Option<String>,
    pub status: Option<Status>,
    pub vendor_code: i32,
    pub sqlstate: [i8; 5],
    pub details: Option<Vec<(String, Vec<u8>)>>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn with_message_and_status(message: &str, status: Status) -> Self {
        Self {
            message: Some(message.into()),
            status: Some(status),
            vendor_code: 0,
            sqlstate: [0; 5],
            details: None,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} (sqlstate: {:?}, vendor_code: {})",
            self.status
                .as_ref()
                .map(|s| format!("{:?}", s))
                .unwrap_or_default(),
            self.message.as_ref().unwrap_or(&"".into()),
            self.sqlstate,
            self.vendor_code
        )
    }
}

impl std::error::Error for Error {}

impl From<ArrowError> for Error {
    fn from(value: ArrowError) -> Self {
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
