use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr::{null, null_mut};
use std::rc::Rc;

use arrow::array::{Array, RecordBatch, RecordBatchReader, StructArray};
use arrow::ffi::{to_ffi, FFI_ArrowSchema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use crate::ffi::FFI_AdbcStatement;
use crate::{driver_method, ffi, Optionable};
use crate::{
    options::{AdbcVersion, OptionValue},
    Error, Result,
};
use crate::{Connection, Database, Driver, Statement};

pub fn check_status(status: ffi::FFI_AdbcStatusCode, error: ffi::FFI_AdbcError) -> Result<()> {
    match status {
        ffi::constants::ADBC_STATUS_OK => Ok(()),
        _ => {
            let mut error: Error = error.into();
            error.status = Some(status.into());
            Err(error)
        }
    }
}

pub struct DriverManager {
    driver: Rc<ffi::FFI_AdbcDriver>,
    version: AdbcVersion, // Driver version
}
impl DriverManager {
    pub fn load_static(init: &ffi::FFI_AdbcDriverInitFunc, version: AdbcVersion) -> Result<Self> {
        let mut error = ffi::FFI_AdbcError::default();
        let mut driver = ffi::FFI_AdbcDriver::default();
        let status = unsafe {
            init(
                version.into(),
                &mut driver as *mut ffi::FFI_AdbcDriver as *mut c_void,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(DriverManager {
            driver: Rc::new(driver),
            version,
        })
    }

    pub fn load_dynamic(
        name: &str,
        entrypoint: Option<&[u8]>,
        version: AdbcVersion,
    ) -> Result<Self> {
        todo!()
    }
}
impl Driver for DriverManager {
    type DatabaseType = ManagedDatabase;

    fn new_database(&self) -> Result<Self::DatabaseType> {
        let opts: [(&str, OptionValue); 0] = [];
        self.new_database_with_opts(opts.into_iter())
    }

    fn new_database_with_opts(
        &self,
        opts: impl Iterator<Item = (impl AsRef<str>, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = ffi::FFI_AdbcDatabase::default();

        // DatabaseNew
        let mut error = ffi::FFI_AdbcError::default();
        let database_new = crate::driver_method!(self.driver, DatabaseNew);
        let status = unsafe { database_new(&mut database, &mut error) };
        check_status(status, error)?;

        // DatabaseSetOption
        for (key, value) in opts {
            set_option_database(self.driver.clone(), self.version, &mut database, key, value)?;
        }

        // DatabaseInit
        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, DatabaseInit);
        let status = unsafe { method(&mut database, &mut error) };
        check_status(status, error)?;

        Ok(Self::DatabaseType {
            driver: self.driver.clone(),
            version: self.version,
            database,
        })
    }
}

fn set_option_database(
    driver: Rc<ffi::FFI_AdbcDriver>,
    version: AdbcVersion,
    database: &mut ffi::FFI_AdbcDatabase,
    key: impl AsRef<str>,
    value: OptionValue,
) -> Result<()> {
    let key = CString::new(key.as_ref())?;
    let mut error = ffi::FFI_AdbcError::default();
    let status = match (version, value) {
        (_, OptionValue::String(value)) => {
            let value = CString::new(value)?;
            let method = crate::driver_method!(driver, DatabaseSetOption);
            unsafe { method(database, key.as_ptr(), value.as_ptr(), &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Bytes(value)) => {
            let method = crate::driver_method!(driver, DatabaseSetOptionBytes);
            unsafe {
                method(
                    database,
                    key.as_ptr(),
                    value.as_ptr(),
                    value.len(),
                    &mut error,
                )
            }
        }
        (AdbcVersion::V110, OptionValue::Int(value)) => {
            let method = crate::driver_method!(driver, DatabaseSetOptionInt);
            unsafe { method(database, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Double(value)) => {
            let method = crate::driver_method!(driver, DatabaseSetOptionDouble);
            unsafe { method(database, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V100, _) => {
            return Err("Only string option value are supported with ADBC 1.0.0".into());
        }
    };
    check_status(status, error)
}

pub struct ManagedDatabase {
    driver: Rc<ffi::FFI_AdbcDriver>,
    version: AdbcVersion,
    database: ffi::FFI_AdbcDatabase,
}
impl Optionable for ManagedDatabase {
    fn get_option_bytes(&mut self, key: impl AsRef<str>) -> Result<Vec<u8>> {
        todo!()
    }
    fn get_option_double(&mut self, key: impl AsRef<str>) -> Result<f64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: f64 = 0.0;
        let method = crate::driver_method!(self.driver, DatabaseGetOptionDouble);
        let status = unsafe { method(&mut self.database, key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_int(&mut self, key: impl AsRef<str>) -> Result<i64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: i64 = 0;
        let method = crate::driver_method!(self.driver, DatabaseGetOptionInt);
        let status = unsafe { method(&mut self.database, key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_string(&mut self, key: impl AsRef<str>) -> Result<String> {
        todo!()
    }
    fn set_option(&mut self, key: impl AsRef<str>, value: OptionValue) -> Result<()> {
        set_option_database(
            self.driver.clone(),
            self.version,
            &mut self.database,
            key,
            value,
        )
    }
}
impl Database for ManagedDatabase {
    type ConnectionType = ManagedConnection;

    fn new_connection(&mut self) -> Result<Self::ConnectionType> {
        let opts: [(&str, OptionValue); 0] = [];
        self.new_connection_with_opts(opts.into_iter())
    }

    fn new_connection_with_opts(
        &mut self,
        opts: impl Iterator<Item = (impl AsRef<str>, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = ffi::FFI_AdbcConnection::default();

        // ConnectioNew
        let mut error = ffi::FFI_AdbcError::default();
        let connection_new = crate::driver_method!(self.driver, ConnectionNew);
        let status = unsafe { connection_new(&mut connection, &mut error) };
        check_status(status, error)?;

        // ConnectionSetOption
        for (key, value) in opts {
            set_option_connection(
                self.driver.clone(),
                self.version,
                &mut connection,
                key,
                value,
            )?;
        }

        // ConnectionInit
        let mut error = ffi::FFI_AdbcError::default();
        let connection_init = crate::driver_method!(self.driver, ConnectionInit);
        let status = unsafe { connection_init(&mut connection, &mut self.database, &mut error) };
        check_status(status, error)?;

        Ok(Self::ConnectionType {
            driver: self.driver.clone(),
            version: self.version,
            connection,
        })
    }
}
impl Drop for ManagedDatabase {
    fn drop(&mut self) {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.driver, DatabaseRelease);
        let status = unsafe { method(&mut self.database, &mut error) };
        if let Err(err) = check_status(status, error) {
            panic!("unable to drop database: {:?}", err);
        }
    }
}

fn set_option_connection(
    driver: Rc<ffi::FFI_AdbcDriver>,
    version: AdbcVersion,
    connection: &mut ffi::FFI_AdbcConnection,
    key: impl AsRef<str>,
    value: OptionValue,
) -> Result<()> {
    let key = CString::new(key.as_ref())?;
    let mut error = ffi::FFI_AdbcError::default();
    let status = match (version, value) {
        (_, OptionValue::String(value)) => {
            let value = CString::new(value)?;
            let method = crate::driver_method!(driver, ConnectionSetOption);
            unsafe { method(connection, key.as_ptr(), value.as_ptr(), &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Bytes(value)) => {
            let method = crate::driver_method!(driver, ConnectionSetOptionBytes);
            unsafe {
                method(
                    connection,
                    key.as_ptr(),
                    value.as_ptr(),
                    value.len(),
                    &mut error,
                )
            }
        }
        (AdbcVersion::V110, OptionValue::Int(value)) => {
            let method = crate::driver_method!(driver, ConnectionSetOptionInt);
            unsafe { method(connection, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Double(value)) => {
            let method = crate::driver_method!(driver, ConnectionSetOptionDouble);
            unsafe { method(connection, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V100, _) => {
            return Err("Only string option value are supported with ADBC 1.0.0".into())
        }
    };
    check_status(status, error)
}

pub struct ManagedConnection {
    driver: Rc<ffi::FFI_AdbcDriver>,
    version: AdbcVersion,
    connection: ffi::FFI_AdbcConnection,
}
impl Optionable for ManagedConnection {
    fn get_option_bytes(&mut self, key: impl AsRef<str>) -> Result<Vec<u8>> {
        todo!()
    }
    fn get_option_double(&mut self, key: impl AsRef<str>) -> Result<f64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: f64 = 0.0;
        let method = crate::driver_method!(self.driver, ConnectionGetOptionDouble);
        let status = unsafe { method(&mut self.connection, key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_int(&mut self, key: impl AsRef<str>) -> Result<i64> {
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::default();
        let mut value: i64 = 0;
        let method = crate::driver_method!(self.driver, ConnectionGetOptionInt);
        let status = unsafe { method(&mut self.connection, key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }
    fn get_option_string(&mut self, key: impl AsRef<str>) -> Result<String> {
        todo!()
    }
    fn set_option(&mut self, key: impl AsRef<str>, value: OptionValue) -> Result<()> {
        set_option_connection(
            self.driver.clone(),
            self.version,
            &mut self.connection,
            key,
            value,
        )
    }
}
impl Connection for ManagedConnection {
    type StatementType = ManagedStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        let mut statement = ffi::FFI_AdbcStatement::default();
        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, StatementNew);
        let status = unsafe { method(&mut self.connection, &mut statement, &mut error) };
        check_status(status, error)?;
        Ok(Self::StatementType {
            driver: self.driver.clone(),
            statement,
            version: self.version,
        })
    }

    fn cancel(&mut self) -> Result<()> {
        if let AdbcVersion::V100 = self.version {
            return Err("Canceling connection is not supported with ADBC 1.0.0".into());
        }

        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, ConnectionCancel);
        let status = unsafe { method(&mut self.connection, &mut error) };
        check_status(status, error)
    }

    fn commit(&mut self) -> Result<()> {
        todo!()
    }

    fn rollback(&mut self) -> Result<()> {
        todo!()
    }

    fn get_info(
        &mut self,
        codes: Option<&[crate::options::InfoCode]>,
    ) -> Result<impl RecordBatchReader> {
        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let codes: Option<Vec<u32>> =
            codes.map(|codes| codes.iter().map(|code| code.into()).collect());
        let (codes_ptr, codes_len) = codes
            .as_ref()
            .map(|c| (c.as_ptr(), c.len()))
            .unwrap_or((null(), 0));
        let method = crate::driver_method!(self.driver, ConnectionGetInfo);
        let status = unsafe {
            method(
                &mut self.connection,
                codes_ptr,
                codes_len,
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_objects(
        &mut self,
        depth: crate::options::ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&[&str]>,
        column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader> {
        let catalog = catalog
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());
        let db_schema = db_schema
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());
        let table_name = table_name
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());
        let column_name = column_name
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());
        let table_type = table_type
            .map(|t| {
                t.iter()
                    .map(|x| CString::new(*x))
                    .collect::<std::result::Result<Vec<CString>, _>>()
            })
            .transpose()?;
        let table_type = table_type
            .as_ref()
            .map(|c| {
                let mut array = c.iter().map(|c| c.as_ptr()).collect::<Vec<_>>();
                array.push(null());
                array
            })
            .map(|c| c.as_ptr())
            .unwrap_or(null());

        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, ConnectionGetObjects);
        let mut stream = FFI_ArrowArrayStream::empty();

        let status = unsafe {
            method(
                &mut self.connection,
                depth.into(),
                catalog,
                db_schema,
                table_name,
                table_type,
                column_name,
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;

        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_statistics(
        &mut self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<impl RecordBatchReader> {
        if let AdbcVersion::V100 = self.version {
            return Err("Statistics are not supported with ADBC 1.0.0".into());
        }

        let catalog = catalog
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());
        let db_schema = db_schema
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());
        let table_name = table_name
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());

        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let method = crate::driver_method!(self.driver, ConnectionGetStatistics);
        let status = unsafe {
            method(
                &mut self.connection,
                catalog,
                db_schema,
                table_name,
                approximate as std::os::raw::c_char,
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_statistics_name(&mut self) -> Result<impl RecordBatchReader> {
        if let AdbcVersion::V100 = self.version {
            return Err("Statistics are not supported with ADBC 1.0.0".into());
        }
        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let method = crate::driver_method!(self.driver, ConnectionGetStatisticNames);
        let status = unsafe { method(&mut self.connection, &mut stream, &mut error) };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_table_schema(
        &mut self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<arrow::datatypes::Schema> {
        let catalog = catalog
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());
        let db_schema = db_schema
            .map(|c| CString::new(c))
            .transpose()?
            .map(|c| c.as_ptr())
            .unwrap_or(null());
        let table_name = CString::new(table_name)?;

        let mut error = ffi::FFI_AdbcError::default();
        let mut schema = FFI_ArrowSchema::empty();
        let method = crate::driver_method!(self.driver, ConnectionGetTableSchema);
        let status = unsafe {
            method(
                &mut self.connection,
                catalog,
                db_schema,
                table_name.as_ptr(),
                &mut schema,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn get_table_types(&mut self) -> Result<impl RecordBatchReader> {
        let mut error = ffi::FFI_AdbcError::default();
        let mut stream = FFI_ArrowArrayStream::empty();
        let get_table_types = crate::driver_method!(self.driver, ConnectionGetTableTypes);
        let status = unsafe { get_table_types(&mut self.connection, &mut stream, &mut error) };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn read_partition(&mut self, partition: &[u8]) -> Result<ArrowArrayStreamReader> {
        todo!()
    }
}
impl Drop for ManagedConnection {
    fn drop(&mut self) {
        let mut error = ffi::FFI_AdbcError::default();
        let connection_release = driver_method!(self.driver, ConnectionRelease);
        let status = unsafe { connection_release(&mut self.connection, &mut error) };
        if let Err(err) = check_status(status, error) {
            panic!("unable to drop connection: {:?}", err);
        }
    }
}

pub struct ManagedStatement {
    driver: Rc<ffi::FFI_AdbcDriver>,
    statement: FFI_AdbcStatement,
    version: AdbcVersion,
}
impl Statement for ManagedStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, StatementBind);
        let batch: StructArray = batch.into();
        let (mut array, mut schema) = to_ffi(&batch.to_data())?;
        let status = unsafe { method(&mut self.statement, &mut array, &mut schema, &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, StatementBindStream);
        let mut stream = FFI_ArrowArrayStream::new(reader);
        let status = unsafe { method(&mut self.statement, &mut stream, &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn cancel(&mut self) -> Result<()> {
        if let AdbcVersion::V100 = self.version {
            return Err("Canceling statement is not supported with ADBC 1.0.0".into());
        }

        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, StatementCancel);
        let status = unsafe { method(&mut self.statement, &mut error) };
        check_status(status, error)
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, StatementExecuteQuery);
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = unsafe { method(&mut self.statement, &mut stream, null_mut(), &mut error) };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn execute_schema(&mut self) -> Result<arrow::datatypes::Schema> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, StatementExecuteSchema);
        let mut schema = FFI_ArrowSchema::empty();
        let status = unsafe { method(&mut self.statement, &mut schema, &mut error) };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn execute_update(&mut self) -> Result<i64> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, StatementExecuteQuery);
        let mut rows_affected: i64 = -1;
        let status = unsafe {
            method(
                &mut self.statement,
                null_mut(),
                &mut rows_affected,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(rows_affected)
    }

    fn get_parameters_schema(&mut self) -> Result<arrow::datatypes::Schema> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, StatementGetParameterSchema);
        let mut schema = FFI_ArrowSchema::empty();
        let status = unsafe { method(&mut self.statement, &mut schema, &mut error) };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn prepare(&mut self) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, StatementPrepare);
        let status = unsafe { method(&mut self.statement, &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_sql_query(&mut self, query: &str) -> Result<()> {
        let query = CString::new(query)?;
        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, StatementSetSqlQuery);
        let status = unsafe { method(&mut self.statement, query.as_ptr(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: &[u8]) -> Result<()> {
        let mut error = ffi::FFI_AdbcError::default();
        let method = crate::driver_method!(self.driver, StatementSetSubstraitPlan);
        let status = unsafe { method(&mut self.statement, plan.as_ptr(), plan.len(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }
}
impl Drop for ManagedStatement {
    fn drop(&mut self) {
        let mut error = ffi::FFI_AdbcError::default();
        let method = driver_method!(self.driver, StatementRelease);
        let status = unsafe { method(&mut self.statement, &mut error) };
        if let Err(err) = check_status(status, error) {
            panic!("unable to drop statement: {:?}", err);
        }
    }
}
