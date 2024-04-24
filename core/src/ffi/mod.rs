//! C-compatible items as defined in [`adbc.h`](https://github.com/apache/arrow-adbc/blob/main/adbc.h)

pub mod constants;
pub(crate) mod methods;
pub(crate) mod types;
pub use types::{
    FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcDriverInitFunc, FFI_AdbcError,
    FFI_AdbcErrorDetail, FFI_AdbcPartitions, FFI_AdbcStatement, FFI_AdbcStatusCode,
};
