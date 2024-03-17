use crate::ffi::constants;
use std::os::raw::c_int;

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

impl From<ObjectDepth> for c_int {
    fn from(value: ObjectDepth) -> Self {
        match value {
            ObjectDepth::All => constants::ADBC_OBJECT_DEPTH_ALL,
            ObjectDepth::Catalogs => constants::ADBC_OBJECT_DEPTH_CATALOGS,
            ObjectDepth::Schemas => constants::ADBC_OBJECT_DEPTH_DB_SCHEMAS,
            ObjectDepth::Tables => constants::ADBC_OBJECT_DEPTH_TABLES,
            ObjectDepth::Columns => constants::ADBC_OBJECT_DEPTH_COLUMNS,
        }
    }
}
