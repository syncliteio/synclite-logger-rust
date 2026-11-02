//! SQLite runtime JNI functions shared by the Android AAR.
//!
//! Kotlin keeps prepared-statement state on the managed side and delegates each
//! operation to a connection handle. This avoids holding Rust borrows across
//! JNI calls while retaining the Rust runtime's transaction and logging logic.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString, JValue};
use jni::sys::{jboolean, jlong, jobjectArray, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;

use synclite::{DestinationOptions, DeviceType, DstSyncMode, DstType, SyncLiteOptions, Value};

use crate::{guard, jstring_to_opt_string, jstring_to_string};

struct AndroidSqliteConnection {
    inner: Mutex<Option<synclite::rusqlite::Connection>>,
}

fn parse_device_type(value: &str) -> Result<DeviceType, String> {
    match value.trim().to_ascii_uppercase().as_str() {
        "SQLITE" => Ok(DeviceType::SQLITE),
        "SQLITE_STORE" => Ok(DeviceType::SQLITE_STORE),
        "STREAMING" => Ok(DeviceType::STREAMING),
        _ => Err(format!(
            "unsupported Android device type {value:?}; expected SQLITE, SQLITE_STORE, or STREAMING"
        )),
    }
}

fn parse_destination_type(value: &str) -> Result<DstType, String> {
    match value.trim().to_ascii_uppercase().as_str() {
        "SQLITE" => Ok(DstType::Sqlite),
        "POSTGRES" | "POSTGRESQL" => Ok(DstType::Postgres),
        _ => Err(format!(
            "unsupported Android destination type {value:?}; expected SQLITE or POSTGRES"
        )),
    }
}

fn parse_sync_mode(value: &str) -> Result<DstSyncMode, String> {
    match value.trim().to_ascii_uppercase().as_str() {
        "CONSOLIDATION" => Ok(DstSyncMode::Consolidation),
        "REPLICATION" => Ok(DstSyncMode::Replication),
        _ => Err(format!(
            "unknown dst_sync_mode {value:?}; expected CONSOLIDATION or REPLICATION"
        )),
    }
}

fn value_from_java(env: &mut JNIEnv<'_>, value: JObject<'_>) -> Result<Value, String> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    if env
        .is_instance_of(&value, "java/lang/String")
        .map_err(|e| e.to_string())?
    {
        let text = JString::from(value);
        return jstring_to_string(env, &text).map(Value::Text);
    }
    if env
        .is_instance_of(&value, "java/lang/Long")
        .map_err(|e| e.to_string())?
        || env
            .is_instance_of(&value, "java/lang/Integer")
            .map_err(|e| e.to_string())?
        || env
            .is_instance_of(&value, "java/lang/Short")
            .map_err(|e| e.to_string())?
        || env
            .is_instance_of(&value, "java/lang/Byte")
            .map_err(|e| e.to_string())?
        || env
            .is_instance_of(&value, "java/lang/Boolean")
            .map_err(|e| e.to_string())?
    {
        let method = if env
            .is_instance_of(&value, "java/lang/Boolean")
            .map_err(|e| e.to_string())?
        {
            "booleanValue"
        } else {
            "longValue"
        };
        let signature = if method == "booleanValue" {
            "()Z"
        } else {
            "()J"
        };
        let result = env
            .call_method(&value, method, signature, &[])
            .map_err(|e| e.to_string())?;
        return if method == "booleanValue" {
            result
                .z()
                .map(|v| Value::Int(i64::from(v)))
                .map_err(|e| e.to_string())
        } else {
            result.j().map(Value::Int).map_err(|e| e.to_string())
        };
    }
    if env
        .is_instance_of(&value, "java/lang/Double")
        .map_err(|e| e.to_string())?
        || env
            .is_instance_of(&value, "java/lang/Float")
            .map_err(|e| e.to_string())?
    {
        return env
            .call_method(&value, "doubleValue", "()D", &[])
            .and_then(|result| result.d())
            .map(Value::Real)
            .map_err(|e| e.to_string());
    }
    if env
        .is_instance_of(&value, "[B")
        .map_err(|e| e.to_string())?
    {
        let bytes = env
            .convert_byte_array(&JByteArray::from(value))
            .map_err(|e| e.to_string())?;
        return Ok(Value::Blob(bytes));
    }
    Err(
        "SQL parameters must be null, Boolean, integer, floating-point, String, or ByteArray"
            .into(),
    )
}

fn params_from_java(env: &mut JNIEnv<'_>, params: JObjectArray<'_>) -> Result<Vec<Value>, String> {
    if params.is_null() {
        return Ok(Vec::new());
    }
    let count = env.get_array_length(&params).map_err(|e| e.to_string())?;
    let mut values = Vec::with_capacity(count as usize);
    for index in 0..count {
        let item = env
            .get_object_array_element(&params, index)
            .map_err(|e| e.to_string())?;
        values.push(value_from_java(env, item)?);
    }
    Ok(values)
}

fn java_value<'local>(env: &mut JNIEnv<'local>, value: &Value) -> Result<JObject<'local>, String> {
    match value {
        Value::Null => Ok(JObject::null()),
        Value::Int(value) => env
            .call_static_method(
                "java/lang/Long",
                "valueOf",
                "(J)Ljava/lang/Long;",
                &[JValue::Long(*value)],
            )
            .and_then(|result| result.l())
            .map_err(|e| e.to_string()),
        Value::Real(value) => env
            .call_static_method(
                "java/lang/Double",
                "valueOf",
                "(D)Ljava/lang/Double;",
                &[JValue::Double(*value)],
            )
            .and_then(|result| result.l())
            .map_err(|e| e.to_string()),
        Value::Text(value) => env
            .new_string(value)
            .map(JObject::from)
            .map_err(|e| e.to_string()),
        Value::Blob(value) => env
            .byte_array_from_slice(value)
            .map(JObject::from)
            .map_err(|e| e.to_string()),
    }
}

fn rows_to_java<'local>(
    env: &mut JNIEnv<'local>,
    rows: Vec<Vec<Value>>,
) -> Result<jobjectArray, String> {
    let row_array = env
        .new_object_array(rows.len() as i32, "[Ljava/lang/Object;", JObject::null())
        .map_err(|e| e.to_string())?;
    for (row_index, row) in rows.iter().enumerate() {
        let cell_array = env
            .new_object_array(row.len() as i32, "java/lang/Object", JObject::null())
            .map_err(|e| e.to_string())?;
        for (column_index, value) in row.iter().enumerate() {
            let java_value = java_value(env, value)?;
            env.set_object_array_element(&cell_array, column_index as i32, java_value)
                .map_err(|e| e.to_string())?;
        }
        env.set_object_array_element(&row_array, row_index as i32, cell_array)
            .map_err(|e| e.to_string())?;
    }
    Ok(row_array.into_raw())
}

unsafe fn connection_from_handle<'a>(handle: jlong) -> Result<&'a AndroidSqliteConnection, String> {
    if handle == 0 {
        return Err("connection handle is closed".into());
    }
    Ok(&*(handle as *mut AndroidSqliteConnection))
}

fn with_connection<T>(
    handle: jlong,
    operation: impl FnOnce(&mut synclite::rusqlite::Connection) -> Result<T, String>,
) -> Result<T, String> {
    // SAFETY: callers receive the handle only from nativeOpen/nativeInitialize;
    // nativeClose consumes it only after clearing the Kotlin-side handle.
    let connection = unsafe { connection_from_handle(handle)? };
    let mut guard = connection
        .inner
        .lock()
        .map_err(|_| "connection mutex poisoned".to_string())?;
    let connection = guard
        .as_mut()
        .ok_or_else(|| "connection is closed".to_string())?;
    operation(connection)
}

#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeSqlite_nativeInitialize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    device_type: JString<'local>,
    device_name: JString<'local>,
    db_path: JString<'local>,
    dst_type: JString<'local>,
    dst_connection_string: JString<'local>,
    dst_database: JString<'local>,
    dst_schema: JString<'local>,
    dst_sync_mode: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let destination = DestinationOptions {
            dst_type: parse_destination_type(&jstring_to_string(env, &dst_type)?)?,
            dst_connection_string: jstring_to_string(env, &dst_connection_string)?,
            dst_database: jstring_to_opt_string(env, &dst_database)?,
            dst_schema: jstring_to_opt_string(env, &dst_schema)?,
            dst_sync_mode: parse_sync_mode(&jstring_to_string(env, &dst_sync_mode)?)?,
        };
        let device_type = parse_device_type(&jstring_to_string(env, &device_type)?)?;
        let device_name = jstring_to_string(env, &device_name)?;
        let db_path = jstring_to_string(env, &db_path)?;
        synclite::initialize(
            device_type,
            &device_name,
            db_path,
            Some(destination),
            SyncLiteOptions::default(),
        )
        .map_err(|error| error.to_string())
    });
}

#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeSqlite_nativeOpen<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    db_path: JString<'local>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let connection =
            synclite::rusqlite::Connection::open(PathBuf::from(jstring_to_string(env, &db_path)?))
                .map_err(|error| error.to_string())?;
        Ok(Box::into_raw(Box::new(AndroidSqliteConnection {
            inner: Mutex::new(Some(connection)),
        })) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeSqlite_nativeConnectionInitialize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    db_path: JString<'local>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let connection = synclite::rusqlite::Connection::initialize(PathBuf::from(
            jstring_to_string(env, &db_path)?,
        ))
        .map_err(|error| error.to_string())?;
        Ok(Box::into_raw(Box::new(AndroidSqliteConnection {
            inner: Mutex::new(Some(connection)),
        })) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeSqlite_nativeExecute<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    sql: JString<'local>,
    params: JObjectArray<'local>,
) -> jlong {
    guard(&mut env, -1, |env| {
        let sql = jstring_to_string(env, &sql)?;
        let params = params_from_java(env, params)?;
        with_connection(handle, |connection| {
            connection
                .execute(&sql, &params)
                .map_err(|error| error.to_string())
        })
        .and_then(|count| {
            i64::try_from(count).map_err(|_| "affected-row count exceeds i64".to_string())
        })
    })
}

#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeSqlite_nativeQuery<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    sql: JString<'local>,
    params: JObjectArray<'local>,
) -> jobjectArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let sql = jstring_to_string(env, &sql)?;
        let params = params_from_java(env, params)?;
        let rows = with_connection(handle, |connection| {
            connection
                .query(&sql, &params)
                .map_err(|error| error.to_string())
        })?;
        rows_to_java(env, rows)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeSqlite_nativeSetAutoCommit<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    enabled: jboolean,
) {
    guard(&mut env, (), |_| {
        with_connection(handle, |connection| {
            connection.set_auto_commit(enabled != JNI_FALSE);
            Ok(())
        })
    });
}

#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeSqlite_nativeGetAutoCommit<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |_| {
        with_connection(handle, |connection| {
            Ok(if connection.get_auto_commit() {
                JNI_TRUE
            } else {
                JNI_FALSE
            })
        })
    })
}

macro_rules! connection_void {
    ($name:ident, $operation:ident) => {
        #[no_mangle]
        pub extern "system" fn $name<'local>(
            mut env: JNIEnv<'local>,
            _class: JClass<'local>,
            handle: jlong,
        ) {
            guard(&mut env, (), |_| {
                with_connection(handle, |connection| {
                    connection.$operation().map_err(|error| error.to_string())
                })
            });
        }
    };
}

connection_void!(Java_io_synclite_NativeSqlite_nativeCommit, commit);
connection_void!(Java_io_synclite_NativeSqlite_nativeRollback, rollback);
connection_void!(Java_io_synclite_NativeSqlite_nativeFlush, flush);

#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeSqlite_nativeClose<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    guard(&mut env, (), |_| {
        if handle == 0 {
            return Ok(());
        }
        // SAFETY: ownership transfers exactly once from Kotlin's close path.
        let boxed = unsafe { Box::from_raw(handle as *mut AndroidSqliteConnection) };
        let connection = boxed
            .inner
            .into_inner()
            .map_err(|_| "connection mutex poisoned".to_string())?
            .ok_or_else(|| "connection is closed".to_string())?;
        connection.close().map_err(|error| error.to_string())
    });
}

#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeSqlite_nativeAwaitSync<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    db_path: JString<'local>,
    timeout_ms: jlong,
) {
    guard(&mut env, (), |env| {
        let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
        synclite::await_sync(PathBuf::from(jstring_to_string(env, &db_path)?), timeout)
            .map_err(|error| error.to_string())
    });
}
