//! Error and result types.

use std::{ffi::NulError, fmt::Display};

use arrow::error::ArrowError;

/// Status of an operation.
#[derive(Debug, PartialEq, Eq)]
pub enum Status {
    /// No error.
    Ok,
    /// An unknown error occurred.
    Unknown,
    /// The operation is not implemented or supported.
    NotImplemented,
    /// A requested resource was not found.
    NotFound,
    /// A requested resource already exists.
    AlreadyExists,
    /// The arguments are invalid, likely a programming error.
    /// For instance, they may be of the wrong format, or out of range.
    InvalidArguments,
    /// The preconditions for the operation are not met, likely a programming error.
    /// For instance, the object may be uninitialized, or may have not
    /// been fully configured.
    InvalidState,
    /// Invalid data was processed (not a programming error).
    /// For instance, a division by zero may have occurred during query
    /// execution.
    InvalidData,
    /// The database's integrity was affected.
    /// For instance, a foreign key check may have failed, or a uniqueness
    /// constraint may have been violated.
    Integrity,
    /// An error internal to the driver or database occurred.
    Internal,
    /// An I/O error occurred.
    /// For instance, a remote service may be unavailable.
    IO,
    /// The operation was cancelled, not due to a timeout.
    Cancelled,
    /// The operation was cancelled due to a timeout.
    Timeout,
    /// Authentication failed.
    Unauthenticated,
    /// The client is not authorized to perform the given operation.
    Unauthorized,
}

/// An ADBC error.
#[derive(Debug)]
pub struct Error {
    /// The error message.
    pub message: Option<String>,
    /// The status of the operation.
    pub status: Option<Status>,
    /// A vendor-specific error code, if applicable.
    pub vendor_code: i32,
    /// A SQLSTATE error code, if provided, as defined by the SQL:2003 standard.
    /// If not set, it should be set to `\0\0\0\0\0`.
    pub sqlstate: [i8; 5],
    /// Additional metadata.
    pub details: Option<Vec<(String, Vec<u8>)>>,
}

/// Result type wrapping [Error].
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

impl From<libloading::Error> for Error {
    fn from(value: libloading::Error) -> Self {
        Self {
            message: Some(format!("Error with dynamic library: {}", value)),
            status: Some(Status::Internal),
            vendor_code: 0,
            sqlstate: [0; 5],
            details: None,
        }
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(value: std::str::Utf8Error) -> Self {
        Self {
            message: Some(format!("Error while decoding UTF-8: {}", value)),
            status: Some(Status::Internal),
            vendor_code: 0,
            sqlstate: [0; 5],
            details: None,
        }
    }
}
