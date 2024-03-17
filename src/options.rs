use crate::ffi::constants;

pub enum OptionValue {
    String(String),
    Bytes(Vec<u8>),
    Int(i64),
    Double(f64),
}

#[derive(Clone, Copy)]
pub enum AdbcVersion {
    V100,
    V110,
}

pub enum InfoCode {
    VendorName,
    VendorVersion,
    VendorArrowVersion,
    DriverName,
    DriverVersion,
    DriverArrowVersion,
    DriverAdbcVersion,
}

pub enum ObjectDepth {
    All,
    Catalogs,
    Schemas,
    Tables,
    Columns,
}

impl From<&InfoCode> for u32 {
    fn from(value: &InfoCode) -> Self {
        match value {
            InfoCode::VendorName => constants::ADBC_INFO_VENDOR_NAME,
            InfoCode::VendorVersion => constants::ADBC_INFO_VENDOR_VERSION,
            InfoCode::VendorArrowVersion => constants::ADBC_INFO_VENDOR_ARROW_VERSION,
            InfoCode::DriverName => constants::ADBC_INFO_DRIVER_NAME,
            InfoCode::DriverVersion => constants::ADBC_INFO_DRIVER_VERSION,
            InfoCode::DriverArrowVersion => constants::ADBC_INFO_DRIVER_ARROW_VERSION,
            InfoCode::DriverAdbcVersion => constants::ADBC_INFO_DRIVER_ADBC_VERSION,
        }
    }
}
