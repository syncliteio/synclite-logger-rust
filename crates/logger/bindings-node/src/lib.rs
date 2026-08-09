//! Node.js N-API bindings for the in-process SyncLite runtime.
//!
//! Values cross the small initial N-API surface as JSON. JSON arrays whose
//! entries are byte values (`0..=255`) represent SQL blobs; query blobs are
//! emitted the same way.

#![forbid(unsafe_code)]

use std::sync::{Arc, Mutex};

use logger_core::record::ArgValue;
use logger_core::DeviceType;
use logger_db_traits::{Row, Value};
use napi::bindgen_prelude::{Error, Result, Status};
use napi_derive::napi;
use serde_json::Value as JsonValue;
use synclite::{
    duckdb as sl_duck, rusqlite as sl_sqlite, DestinationOptions as RustDestinationOptions,
    DstSyncMode, DstType, SyncLiteOptions,
};

fn napi_error(message: impl Into<String>) -> Error {
    Error::new(Status::GenericFailure, message.into())
}

fn parse_params(params_json: Option<String>) -> Result<Vec<Value>> {
    let Some(params_json) = params_json else {
        return Ok(Vec::new());
    };
    let parsed: JsonValue = serde_json::from_str(&params_json)
        .map_err(|e| napi_error(format!("invalid parameters JSON: {e}")))?;
    let JsonValue::Array(values) = parsed else {
        return Err(napi_error("parameters must be a JSON array"));
    };
    values.into_iter().map(json_to_value).collect()
}

fn json_to_value(value: JsonValue) -> Result<Value> {
    match value {
        JsonValue::Null => Ok(ArgValue::Null),
        JsonValue::Bool(value) => Ok(ArgValue::Text(if value { "1" } else { "0" }.into())),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(ArgValue::Int(value))
            } else if let Some(value) = value.as_f64() {
                Ok(ArgValue::Real(value))
            } else {
                Err(napi_error("number is outside SyncLite's supported range"))
            }
        }
        JsonValue::String(value) => Ok(ArgValue::Text(value)),
        JsonValue::Array(values) => {
            let mut bytes = Vec::with_capacity(values.len());
            for value in values {
                let Some(byte) = value.as_u64() else {
                    return Err(napi_error("blob arrays must contain only unsigned byte values"));
                };
                if byte > u8::MAX as u64 {
                    return Err(napi_error("blob array values must be in the range 0..=255"));
                }
                bytes.push(byte as u8);
            }
            Ok(ArgValue::Blob(bytes))
        }
        JsonValue::Object(_) => Err(napi_error("object parameters are not supported")),
    }
}

fn value_to_json(value: Value) -> JsonValue {
    match value {
        ArgValue::Null => JsonValue::Null,
        ArgValue::Int(value) => JsonValue::from(value),
        ArgValue::Real(value) => serde_json::Number::from_f64(value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ArgValue::Text(value) => JsonValue::String(value),
        ArgValue::Blob(value) => JsonValue::Array(value.into_iter().map(JsonValue::from).collect()),
    }
}

fn rows_to_json(rows: Vec<Row>) -> Result<String> {
    let rows: Vec<JsonValue> = rows
        .into_iter()
        .map(|row| JsonValue::Array(row.into_iter().map(value_to_json).collect()))
        .collect();
    serde_json::to_string(&rows).map_err(|e| napi_error(format!("serialize query rows: {e}")))
}

/// Block until the embedded shipper and consolidator apply pending commits.
#[napi]
pub fn await_sync(db_path: String, timeout_seconds: u32) -> Result<()> {
    synclite::await_sync(db_path, std::time::Duration::from_secs(timeout_seconds as u64))
        .map_err(|e| napi_error(e.to_string()))
}

fn parse_device_type(s: &str) -> Result<DeviceType> {
    match s.trim().to_ascii_uppercase().as_str() {
        "SQLITE" => Ok(DeviceType::SQLITE),
        "SQLITE_STORE" => Ok(DeviceType::SQLITE_STORE),
        "STREAMING" => Ok(DeviceType::STREAMING),
        "DUCKDB" => Ok(DeviceType::DUCKDB),
        "DUCKDB_STORE" => Ok(DeviceType::DUCKDB_STORE),
        other => Err(napi_error(format!(
            "unknown device_type {other:?}; expected one of \
             SQLITE, SQLITE_STORE, STREAMING, DUCKDB, DUCKDB_STORE"
        ))),
    }
}

fn parse_dst_type(s: &str) -> Result<DstType> {
    match s.trim().to_ascii_uppercase().as_str() {
        "SQLITE" => Ok(DstType::Sqlite),
        "DUCKDB" => Ok(DstType::DuckDb),
        "POSTGRES" | "POSTGRESQL" => Ok(DstType::Postgres),
        other => Err(napi_error(format!(
            "unknown dst_type {other:?}; expected one of SQLITE, DUCKDB, POSTGRES"
        ))),
    }
}

fn parse_dst_sync_mode(s: &str) -> Result<DstSyncMode> {
    match s.trim().to_ascii_uppercase().as_str() {
        "CONSOLIDATION" => Ok(DstSyncMode::Consolidation),
        "REPLICATION" => Ok(DstSyncMode::Replication),
        other => Err(napi_error(format!(
            "unknown dst_sync_mode {other:?}; expected CONSOLIDATION or REPLICATION"
        ))),
    }
}

/// Register a device + destination ahead of any `open(...)` call. Accepts a
/// JSON string mirroring the Python `initialize(...)` keyword arguments:
/// `{ "device_type", "device_name", "db_path", "destination": { "dst_type",
/// "dst_connection_string", "dst_database", "dst_schema", "dst_sync_mode" },
/// "config_path" }`. `destination` and `config_path` are optional.
#[napi]
pub fn initialize(config_json: String) -> Result<()> {
    let parsed: JsonValue = serde_json::from_str(&config_json)
        .map_err(|e| napi_error(format!("invalid initialize config JSON: {e}")))?;
    let JsonValue::Object(config) = parsed else {
        return Err(napi_error("initialize config must be a JSON object"));
    };

    let get_str = |key: &str| -> Option<String> {
        config.get(key).and_then(JsonValue::as_str).map(str::to_string)
    };

    let device_type = get_str("device_type")
        .ok_or_else(|| napi_error("initialize config requires \"device_type\""))?;
    let device_name = get_str("device_name")
        .ok_or_else(|| napi_error("initialize config requires \"device_name\""))?;
    let db_path = get_str("db_path")
        .ok_or_else(|| napi_error("initialize config requires \"db_path\""))?;

    let device = parse_device_type(&device_type)?;

    let destination = match config.get("destination") {
        None | Some(JsonValue::Null) => None,
        Some(JsonValue::Object(dest)) => {
            let dst_type = dest
                .get("dst_type")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| napi_error("destination requires \"dst_type\""))?;
            let dst_connection_string = dest
                .get("dst_connection_string")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| napi_error("destination requires \"dst_connection_string\""))?;
            let dst_sync_mode = dest
                .get("dst_sync_mode")
                .and_then(JsonValue::as_str)
                .unwrap_or("CONSOLIDATION");
            Some(RustDestinationOptions {
                dst_type: parse_dst_type(dst_type)?,
                dst_connection_string: dst_connection_string.to_string(),
                dst_database: dest
                    .get("dst_database")
                    .and_then(JsonValue::as_str)
                    .map(str::to_string),
                dst_schema: dest
                    .get("dst_schema")
                    .and_then(JsonValue::as_str)
                    .map(str::to_string),
                dst_sync_mode: parse_dst_sync_mode(dst_sync_mode)?,
            })
        }
        Some(_) => return Err(napi_error("\"destination\" must be a JSON object")),
    };

    let opts = SyncLiteOptions {
        config_path: get_str("config_path").map(std::path::PathBuf::from),
        ..SyncLiteOptions::default()
    };

    synclite::initialize(device, &device_name, &db_path, destination, opts)
        .map_err(|e| napi_error(e.to_string()))
}

macro_rules! connection_api {
    ($name:ident, $connection:path) => {
        #[napi]
        pub struct $name {
            inner: Arc<Mutex<Option<$connection>>>,
        }

        #[napi]
        impl $name {
            #[napi(factory)]
            pub fn open(db_path: String) -> Result<Self> {
                let connection = <$connection>::open(db_path).map_err(|e| napi_error(e.to_string()))?;
                Ok(Self {
                    inner: Arc::new(Mutex::new(Some(connection))),
                })
            }

            #[napi(factory)]
            pub fn initialize(db_path: String) -> Result<Self> {
                let connection = <$connection>::initialize(db_path).map_err(|e| napi_error(e.to_string()))?;
                Ok(Self {
                    inner: Arc::new(Mutex::new(Some(connection))),
                })
            }

            #[napi(factory)]
            pub fn open_with_config(config_path: String) -> Result<Self> {
                let connection = <$connection>::open_with_config(config_path)
                    .map_err(|e| napi_error(e.to_string()))?;
                Ok(Self {
                    inner: Arc::new(Mutex::new(Some(connection))),
                })
            }

            #[napi(factory)]
            pub fn initialize_with_config(config_path: String) -> Result<Self> {
                let connection = <$connection>::initialize_with_config(config_path)
                    .map_err(|e| napi_error(e.to_string()))?;
                Ok(Self {
                    inner: Arc::new(Mutex::new(Some(connection))),
                })
            }

            #[napi]
            pub fn execute(&self, sql: String, params_json: Option<String>) -> Result<u32> {
                let params = parse_params(params_json)?;
                self.with_connection(|connection| {
                    connection.execute(&sql, &params).map_err(|e| napi_error(e.to_string()))
                })
                .and_then(|count| u32::try_from(count).map_err(|_| napi_error("affected-row count exceeds u32")))
            }

            #[napi]
            pub fn query_json(&self, sql: String, params_json: Option<String>) -> Result<String> {
                let params = parse_params(params_json)?;
                let rows = self.with_connection(|connection| {
                    connection.query(&sql, &params).map_err(|e| napi_error(e.to_string()))
                })?;
                rows_to_json(rows)
            }

            #[napi]
            pub fn set_auto_commit(&self, auto_commit: bool) -> Result<()> {
                self.with_connection(|connection| {
                    connection.set_auto_commit(auto_commit);
                    Ok(())
                })
            }

            #[napi]
            pub fn get_auto_commit(&self) -> Result<bool> {
                self.with_connection(|connection| Ok(connection.get_auto_commit()))
            }

            #[napi]
            pub fn commit(&self) -> Result<()> {
                self.with_connection(|connection| connection.commit().map_err(|e| napi_error(e.to_string())))
            }

            #[napi]
            pub fn rollback(&self) -> Result<()> {
                self.with_connection(|connection| connection.rollback().map_err(|e| napi_error(e.to_string())))
            }

            #[napi]
            pub fn flush(&self) -> Result<()> {
                self.with_connection(|connection| connection.flush().map_err(|e| napi_error(e.to_string())))
            }

            #[napi]
            pub fn close(&self) -> Result<()> {
                let mut guard = self.inner.lock().map_err(|_| napi_error("connection mutex poisoned"))?;
                match guard.take() {
                    Some(connection) => connection.close().map_err(|e| napi_error(e.to_string())),
                    None => Ok(()),
                }
            }
        }

        impl $name {
            fn with_connection<T>(&self, operation: impl FnOnce(&mut $connection) -> Result<T>) -> Result<T> {
                let mut guard = self.inner.lock().map_err(|_| napi_error("connection mutex poisoned"))?;
                let connection = guard.as_mut().ok_or_else(|| napi_error("connection is closed"))?;
                operation(connection)
            }
        }
    };
}

connection_api!(SqliteConnection, sl_sqlite::Connection);
connection_api!(DuckDbConnection, sl_duck::Connection);
