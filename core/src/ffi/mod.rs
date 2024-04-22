pub mod constants;
pub(crate) mod methods;
pub(crate) mod types;
pub use types::{
    FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcDriverInitFunc, FFI_AdbcError,
    FFI_AdbcErrorDetail, FFI_AdbcPartitions, FFI_AdbcStatement, FFI_AdbcStatusCode,
};

use crate::error::{Error, Result};

pub(crate) fn check_status(status: FFI_AdbcStatusCode, error: FFI_AdbcError) -> Result<()> {
    match status {
        constants::ADBC_STATUS_OK => Ok(()),
        _ => {
            let mut error: Error = error.try_into()?;
            error.status = status.try_into()?;
            Err(error)
        }
    }
}
