//! Python bindings (PyO3) over the `synclite` wrapper crate.
//!
//! Mirrors the Rust user-facing API 1:1 so Python samples read like the
//! `synclite_rusqlite.rs` / `synclite_duckdb.rs` examples — no DB-API
//! adapter, no pre/post hooks, no separate user-DB connection. Python
//! holds a `Connection` and a `Statement` directly bound to the Rust
//! types.
//!
//! Surface:
//!
//! - module fns: `initialize`, `await_sync`
//! - classes: `Connection` (sqlite / sqlite-store / streaming),
//!   `DuckDBConnection` (duckdb / duckdb-store), `Statement`,
//!   `DuckDBStatement`, `DestinationOptions`
//!
//! `DeviceType`, `DstType`, `DstSyncMode` are accepted as case-insensitive
//! strings on the Python side to keep the binding small.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use logger_core::DeviceType;
use logger_db_traits::Value;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use synclite::{
    duckdb as sl_duck,
    rusqlite as sl_sqlite,
    DestinationOptions as RustDestinationOptions, DstSyncMode, DstType, SyncLiteOptions,
};

// ---------- module-level: initialize / await_sync ----------------------------

/// `synclite::initialize` — register a device + destination ahead of any
/// `Connection::open(...)` call. Use a per-process initialize, then open
/// any number of connections.
#[pyfunction]
#[pyo3(signature = (
    device_type,
    device_name,
    db_path,
    destination = None,
    config_path = None,
))]
fn initialize(
    device_type: &str,
    device_name: &str,
    db_path: &str,
    destination: Option<PyRef<'_, PyDestinationOptions>>,
    config_path: Option<&str>,
) -> PyResult<()> {
    let dt = parse_device_type(device_type)?;
    let dest = destination.map(|d| d.to_rust());
    let opts = SyncLiteOptions {
        config_path: config_path.map(PathBuf::from),
        ..SyncLiteOptions::default()
    };
    synclite::initialize(dt, device_name, db_path, dest, opts).map_err(map_err)
}

/// `synclite::await_sync` — block until the embedded shipper +
/// consolidator have drained every rolled segment for `db_path` (up to
/// `timeout_seconds`). Pair with `connection.flush()` before close.
#[pyfunction]
fn await_sync(db_path: &str, timeout_seconds: f64) -> PyResult<()> {
    let timeout = Duration::from_secs_f64(timeout_seconds.max(0.0));
    synclite::await_sync(db_path, timeout).map_err(map_err)
}

// ---------- DestinationOptions ----------------------------------------------

/// Python mirror of `synclite::DestinationOptions`.
#[pyclass(module = "synclite._native", name = "DestinationOptions")]
#[derive(Clone)]
struct PyDestinationOptions {
    #[pyo3(get, set)]
    dst_type: String,
    #[pyo3(get, set)]
    dst_connection_string: String,
    #[pyo3(get, set)]
    dst_database: Option<String>,
    #[pyo3(get, set)]
    dst_schema: Option<String>,
    #[pyo3(get, set)]
    dst_sync_mode: String,
}

#[pymethods]
impl PyDestinationOptions {
    #[new]
    #[pyo3(signature = (
        dst_type,
        dst_connection_string,
        dst_database = None,
        dst_schema = None,
        dst_sync_mode = "CONSOLIDATION".to_string(),
    ))]
    fn new(
        dst_type: String,
        dst_connection_string: String,
        dst_database: Option<String>,
        dst_schema: Option<String>,
        dst_sync_mode: String,
    ) -> Self {
        Self {
            dst_type,
            dst_connection_string,
            dst_database,
            dst_schema,
            dst_sync_mode,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "DestinationOptions(dst_type={:?}, dst_connection_string={:?}, \
             dst_database={:?}, dst_schema={:?}, dst_sync_mode={:?})",
            self.dst_type,
            self.dst_connection_string,
            self.dst_database,
            self.dst_schema,
            self.dst_sync_mode,
        )
    }
}

impl PyDestinationOptions {
    fn to_rust(&self) -> RustDestinationOptions {
        RustDestinationOptions {
            dst_type: parse_dst_type(&self.dst_type).unwrap_or(DstType::Sqlite),
            dst_connection_string: self.dst_connection_string.clone(),
            dst_database: self.dst_database.clone(),
            dst_schema: self.dst_schema.clone(),
            dst_sync_mode: parse_dst_sync_mode(&self.dst_sync_mode)
                .unwrap_or(DstSyncMode::Consolidation),
        }
    }
}

// ---------- Connection (sqlite / sqlite-store / streaming) ------------------

/// SyncLite-wrapped SQLite-family connection — `rusqlite`-shaped.
#[pyclass(module = "synclite._native", name = "Connection", unsendable)]
struct PyConnection {
    inner: Mutex<Option<sl_sqlite::Connection>>,
}

#[pymethods]
impl PyConnection {
    /// Open a SyncLite-managed SQLite database by path. The device must
    /// have been previously registered via `initialize(...)` or have a
    /// persisted metadata layout from a prior open.
    #[staticmethod]
    fn open(db_path: &str) -> PyResult<Self> {
        let c = sl_sqlite::Connection::open(db_path).map_err(map_err)?;
        Ok(Self::wrap(c))
    }

    /// Initialize + open in one call using path-derived defaults
    /// (`DeviceType::Sqlite`, device name derived from filename).
    #[staticmethod]
    fn initialize(db_path: &str) -> PyResult<Self> {
        let c = sl_sqlite::Connection::initialize(db_path).map_err(map_err)?;
        Ok(Self::wrap(c))
    }

    /// Open from an existing `synclite.conf` file (legacy `synclite_logger.conf`
    /// also accepted). Use this for
    /// store / streaming devices where the conf carries `device-type`.
    #[staticmethod]
    fn open_with_config(conf_path: &str) -> PyResult<Self> {
        let c = sl_sqlite::Connection::open_with_config(conf_path).map_err(map_err)?;
        Ok(Self::wrap(c))
    }

    /// Initialize + open from an existing config file.
    #[staticmethod]
    fn initialize_with_config(conf_path: &str) -> PyResult<Self> {
        let c = sl_sqlite::Connection::initialize_with_config(conf_path).map_err(map_err)?;
        Ok(Self::wrap(c))
    }

    /// Execute a mutating statement.
    #[pyo3(signature = (sql, params = None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        let p = convert_params(py, params)?;
        self.with_conn(|c| c.execute(sql, &p).map_err(map_err))
    }

    /// Execute a query and return all rows as `list[tuple]`.
    #[pyo3(signature = (sql, params = None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let p = convert_params(py, params)?;
        let rows = self.with_conn(|c| c.query(sql, &p).map_err(map_err))?;
        rows_to_pylist(py, rows)
    }

    /// Prepare a statement for repeated `execute` / `query` /
    /// `add_batch` + `execute_batch`.
    fn prepare(slf: Py<Self>, sql: &str) -> PyResult<PyStatement> {
        Ok(PyStatement {
            conn: slf,
            sql: sql.to_string(),
            batches: Vec::new(),
        })
    }

    /// Toggle user-facing autocommit (matches the Java/Rust wrapper).
    fn set_auto_commit(&self, auto_commit: bool) -> PyResult<()> {
        self.with_conn(|c| {
            c.set_auto_commit(auto_commit);
            Ok(())
        })
    }

    fn get_auto_commit(&self) -> PyResult<bool> {
        self.with_conn(|c| Ok(c.get_auto_commit()))
    }

    fn commit(&self) -> PyResult<()> {
        self.with_conn(|c| c.commit().map_err(map_err))
    }

    fn rollback(&self) -> PyResult<()> {
        self.with_conn(|c| c.rollback().map_err(map_err))
    }

    /// Roll the active log segment so the in-process shipper can pick
    /// it up. Pair with `await_sync` before exiting a short-lived
    /// program.
    fn flush(&self) -> PyResult<()> {
        self.with_conn(|c| c.flush().map_err(map_err))
    }

    fn close(&self) -> PyResult<()> {
        let mut guard = self.lock_inner()?;
        match guard.take() {
            Some(c) => c.close().map_err(map_err),
            None => Ok(()),
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.close()
    }
}

impl PyConnection {
    fn wrap(c: sl_sqlite::Connection) -> Self {
        Self {
            inner: Mutex::new(Some(c)),
        }
    }

    fn lock_inner(&self) -> PyResult<std::sync::MutexGuard<'_, Option<sl_sqlite::Connection>>> {
        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("connection mutex poisoned"))
    }

    fn with_conn<F, R>(&self, f: F) -> PyResult<R>
    where
        F: FnOnce(&mut sl_sqlite::Connection) -> PyResult<R>,
    {
        let mut guard = self.lock_inner()?;
        let c = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        f(c)
    }
}

/// Python-side prepared statement for `Connection`. Holds a reference
/// to the parent connection so the Rust borrow is taken per-call.
#[pyclass(module = "synclite._native", name = "Statement", unsendable)]
struct PyStatement {
    conn: Py<PyConnection>,
    sql: String,
    batches: Vec<Vec<Value>>,
}

#[pymethods]
impl PyStatement {
    /// Execute the prepared statement once.
    #[pyo3(signature = (params = None))]
    fn execute(
        &self,
        py: Python<'_>,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        let p = convert_params(py, params)?;
        let bound = self.conn.bind(py);
        let conn = bound.borrow();
        conn.with_conn(|c| c.execute(&self.sql, &p).map_err(map_err))
    }

    /// Run a query through the prepared statement.
    #[pyo3(signature = (params = None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let p = convert_params(py, params)?;
        let bound = self.conn.bind(py);
        let conn = bound.borrow();
        let rows = conn.with_conn(|c| c.query(&self.sql, &p).map_err(map_err))?;
        rows_to_pylist(py, rows)
    }

    /// Queue one batch row.
    fn add_batch(&mut self, py: Python<'_>, params: &Bound<'_, PyAny>) -> PyResult<()> {
        let p = convert_params(py, Some(params))?;
        self.batches.push(p);
        Ok(())
    }

    /// Clear queued batch rows.
    fn clear_batch(&mut self) {
        self.batches.clear();
    }

    /// Execute every queued batch row as one prepared batch.
    fn execute_batch(&mut self, py: Python<'_>) -> PyResult<Vec<u64>> {
        let batches = std::mem::take(&mut self.batches);
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let sql = self.sql.clone();
        let bound = self.conn.bind(py);
        let conn = bound.borrow();
        conn.with_conn(move |c| {
            let mut stmt = c.prepare(&sql).map_err(map_err)?;
            for row in &batches {
                stmt.add_batch(row);
            }
            stmt.execute_batch().map_err(map_err)
        })
    }
}

// ---------- DuckDBConnection -------------------------------------------------

/// SyncLite-wrapped DuckDB-family connection — `duckdb`-shaped.
#[pyclass(module = "synclite._native", name = "DuckDBConnection", unsendable)]
struct PyDuckConnection {
    inner: Mutex<Option<sl_duck::Connection>>,
}

#[pymethods]
impl PyDuckConnection {
    #[staticmethod]
    fn open(db_path: &str) -> PyResult<Self> {
        let c = sl_duck::Connection::open(db_path).map_err(map_err)?;
        Ok(Self::wrap(c))
    }

    #[staticmethod]
    fn initialize(db_path: &str) -> PyResult<Self> {
        let c = sl_duck::Connection::initialize(db_path).map_err(map_err)?;
        Ok(Self::wrap(c))
    }

    #[staticmethod]
    fn open_with_config(conf_path: &str) -> PyResult<Self> {
        let c = sl_duck::Connection::open_with_config(conf_path).map_err(map_err)?;
        Ok(Self::wrap(c))
    }

    #[staticmethod]
    fn initialize_with_config(conf_path: &str) -> PyResult<Self> {
        let c = sl_duck::Connection::initialize_with_config(conf_path).map_err(map_err)?;
        Ok(Self::wrap(c))
    }

    #[pyo3(signature = (sql, params = None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        let p = convert_params(py, params)?;
        self.with_conn(|c| c.execute(sql, &p).map_err(map_err))
    }

    #[pyo3(signature = (sql, params = None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let p = convert_params(py, params)?;
        let rows = self.with_conn(|c| c.query(sql, &p).map_err(map_err))?;
        rows_to_pylist(py, rows)
    }

    fn prepare(slf: Py<Self>, sql: &str) -> PyResult<PyDuckStatement> {
        Ok(PyDuckStatement {
            conn: slf,
            sql: sql.to_string(),
            batches: Vec::new(),
        })
    }

    fn set_auto_commit(&self, auto_commit: bool) -> PyResult<()> {
        self.with_conn(|c| {
            c.set_auto_commit(auto_commit);
            Ok(())
        })
    }

    fn get_auto_commit(&self) -> PyResult<bool> {
        self.with_conn(|c| Ok(c.get_auto_commit()))
    }

    fn commit(&self) -> PyResult<()> {
        self.with_conn(|c| c.commit().map_err(map_err))
    }

    fn rollback(&self) -> PyResult<()> {
        self.with_conn(|c| c.rollback().map_err(map_err))
    }

    fn flush(&self) -> PyResult<()> {
        self.with_conn(|c| c.flush().map_err(map_err))
    }

    fn close(&self) -> PyResult<()> {
        let mut guard = self.lock_inner()?;
        match guard.take() {
            Some(c) => c.close().map_err(map_err),
            None => Ok(()),
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.close()
    }
}

impl PyDuckConnection {
    fn wrap(c: sl_duck::Connection) -> Self {
        Self {
            inner: Mutex::new(Some(c)),
        }
    }

    fn lock_inner(&self) -> PyResult<std::sync::MutexGuard<'_, Option<sl_duck::Connection>>> {
        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("connection mutex poisoned"))
    }

    fn with_conn<F, R>(&self, f: F) -> PyResult<R>
    where
        F: FnOnce(&mut sl_duck::Connection) -> PyResult<R>,
    {
        let mut guard = self.lock_inner()?;
        let c = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        f(c)
    }
}

#[pyclass(module = "synclite._native", name = "DuckDBStatement", unsendable)]
struct PyDuckStatement {
    conn: Py<PyDuckConnection>,
    sql: String,
    batches: Vec<Vec<Value>>,
}

#[pymethods]
impl PyDuckStatement {
    #[pyo3(signature = (params = None))]
    fn execute(
        &self,
        py: Python<'_>,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        let p = convert_params(py, params)?;
        let bound = self.conn.bind(py);
        let conn = bound.borrow();
        conn.with_conn(|c| c.execute(&self.sql, &p).map_err(map_err))
    }

    #[pyo3(signature = (params = None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let p = convert_params(py, params)?;
        let bound = self.conn.bind(py);
        let conn = bound.borrow();
        let rows = conn.with_conn(|c| c.query(&self.sql, &p).map_err(map_err))?;
        rows_to_pylist(py, rows)
    }

    fn add_batch(&mut self, py: Python<'_>, params: &Bound<'_, PyAny>) -> PyResult<()> {
        let p = convert_params(py, Some(params))?;
        self.batches.push(p);
        Ok(())
    }

    fn clear_batch(&mut self) {
        self.batches.clear();
    }

    fn execute_batch(&mut self, py: Python<'_>) -> PyResult<Vec<u64>> {
        let batches = std::mem::take(&mut self.batches);
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let sql = self.sql.clone();
        let bound = self.conn.bind(py);
        let conn = bound.borrow();
        conn.with_conn(move |c| {
            let mut stmt = c.prepare(&sql).map_err(map_err)?;
            for row in &batches {
                stmt.add_batch(row);
            }
            stmt.execute_batch().map_err(map_err)
        })
    }
}

// ---------- helpers ----------------------------------------------------------

fn parse_device_type(s: &str) -> PyResult<DeviceType> {
    match s.trim().to_ascii_uppercase().as_str() {
        "SQLITE" => Ok(DeviceType::Sqlite),
        "SQLITE_STORE" => Ok(DeviceType::SqliteStore),
        "STREAMING" => Ok(DeviceType::Streaming),
        "DUCKDB" => Ok(DeviceType::DuckDb),
        "DUCKDB_STORE" => Ok(DeviceType::DuckDbStore),
        other => Err(PyValueError::new_err(format!(
            "unknown device_type {other:?}; expected one of \
             SQLITE, SQLITE_STORE, STREAMING, DUCKDB, DUCKDB_STORE"
        ))),
    }
}

fn parse_dst_type(s: &str) -> PyResult<DstType> {
    match s.trim().to_ascii_uppercase().as_str() {
        "SQLITE" => Ok(DstType::Sqlite),
        "DUCKDB" => Ok(DstType::DuckDb),
        "POSTGRES" | "POSTGRESQL" => Ok(DstType::Postgres),
        other => Err(PyValueError::new_err(format!(
            "unknown dst_type {other:?}; expected one of SQLITE, DUCKDB, POSTGRES"
        ))),
    }
}

fn parse_dst_sync_mode(s: &str) -> PyResult<DstSyncMode> {
    match s.trim().to_ascii_uppercase().as_str() {
        "CONSOLIDATION" => Ok(DstSyncMode::Consolidation),
        "REPLICATION" => Ok(DstSyncMode::Replication),
        other => Err(PyValueError::new_err(format!(
            "unknown dst_sync_mode {other:?}; expected CONSOLIDATION or REPLICATION"
        ))),
    }
}

fn map_err(err: logger_core::Error) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

fn convert_params(
    py: Python<'_>,
    params: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<Value>> {
    let Some(obj) = params else {
        return Ok(Vec::new());
    };
    if obj.is_none() {
        return Ok(Vec::new());
    }
    let seq = py_seq(obj)?;
    let mut out = Vec::with_capacity(seq.len());
    for item in seq.into_iter() {
        out.push(convert_value(py, item)?);
    }
    Ok(out)
}

fn py_seq<'py>(obj: &Bound<'py, PyAny>) -> PyResult<Vec<Bound<'py, PyAny>>> {
    if let Ok(list) = obj.downcast::<PyList>() {
        return Ok(list.iter().collect());
    }
    if let Ok(tuple) = obj.downcast::<PyTuple>() {
        return Ok(tuple.iter().collect());
    }
    Err(PyTypeError::new_err(
        "expected a list or tuple of parameters",
    ))
}

fn convert_value(_py: Python<'_>, obj: Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(Value::Int(if b { 1 } else { 0 }));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(Value::Int(i));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(Value::Real(f));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::Text(s));
    }
    if let Ok(bytes) = obj.downcast::<PyBytes>() {
        return Ok(Value::Blob(bytes.as_bytes().to_vec()));
    }
    Err(PyValueError::new_err(format!(
        "unsupported parameter type: {}",
        obj.get_type().name()?
    )))
}

fn value_to_py<'py>(py: Python<'py>, v: &Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match v {
        Value::Null => py.None().into_bound(py),
        Value::Int(i) => i.into_py(py).into_bound(py),
        Value::Real(f) => f.into_py(py).into_bound(py),
        Value::Text(s) => s.into_py(py).into_bound(py),
        Value::Blob(b) => PyBytes::new_bound(py, b).into_any(),
    })
}

fn rows_to_pylist<'py>(
    py: Python<'py>,
    rows: Vec<Vec<Value>>,
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty_bound(py);
    for row in rows {
        let mut cells: Vec<Bound<'py, PyAny>> = Vec::with_capacity(row.len());
        for v in &row {
            cells.push(value_to_py(py, v)?);
        }
        let tup = PyTuple::new_bound(py, cells);
        out.append(tup)?;
    }
    Ok(out)
}

/// Module entry point. Loaded by Python as `synclite._native`.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDestinationOptions>()?;
    m.add_class::<PyConnection>()?;
    m.add_class::<PyStatement>()?;
    m.add_class::<PyDuckConnection>()?;
    m.add_class::<PyDuckStatement>()?;
    m.add_function(wrap_pyfunction!(initialize, m)?)?;
    m.add_function(wrap_pyfunction!(await_sync, m)?)?;
    m.add("__version__", env!("SYNCLITE_VERSION"))?;
    Ok(())
}
