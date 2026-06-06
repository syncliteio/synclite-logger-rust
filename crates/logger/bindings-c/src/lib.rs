//! C ABI bindings over `synclite-runtime`.
//!
//! This exposes a minimal, stable surface for non-Rust language bindings.

#![warn(missing_docs)]

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

use logger_runtime::{Runtime, RuntimeContract};

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Opaque runtime handle for C callers.
pub struct SyncLiteRuntime {
    inner: Runtime,
}

fn set_last_error(msg: String) {
    let sanitized = msg.replace('\0', " ");
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = CString::new(sanitized).ok();
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

fn cstr_arg<'a>(ptr: *const c_char, name: &str) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err(format!("{name} is null"));
    }
    // SAFETY: caller provides a valid NUL-terminated UTF-8 string pointer.
    let c = unsafe { CStr::from_ptr(ptr) };
    c.to_str()
        .map_err(|_| format!("{name} is not valid UTF-8"))
}

/// Open a SyncLite runtime from `synclite.conf` path (legacy `synclite_logger.conf`
/// also accepted).
///
/// Returns null on error and sets thread-local `last_error`.
#[no_mangle]
pub extern "C" fn synclite_runtime_open_config(conf_path: *const c_char) -> *mut SyncLiteRuntime {
    clear_last_error();
    let conf = match cstr_arg(conf_path, "conf_path") {
        Ok(v) => v,
        Err(e) => {
            set_last_error(e);
            return std::ptr::null_mut();
        }
    };

    match Runtime::open(conf) {
        Ok(rt) => Box::into_raw(Box::new(SyncLiteRuntime { inner: rt })),
        Err(e) => {
            set_last_error(e.to_string());
            std::ptr::null_mut()
        }
    }
}

/// Log one SQL statement with no bound parameters.
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn synclite_runtime_log_sql(
    runtime: *mut SyncLiteRuntime,
    sql: *const c_char,
) -> c_int {
    clear_last_error();
    if runtime.is_null() {
        set_last_error("runtime is null".to_string());
        return -1;
    }
    let sql = match cstr_arg(sql, "sql") {
        Ok(v) => v,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };

    // SAFETY: non-null runtime pointer is owned by caller and created by this crate.
    let rt = unsafe { &mut *runtime };
    match rt.inner.log(sql, &[]) {
        Ok(_) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Commit current transaction scope.
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn synclite_runtime_commit(runtime: *mut SyncLiteRuntime) -> c_int {
    clear_last_error();
    if runtime.is_null() {
        set_last_error("runtime is null".to_string());
        return -1;
    }
    // SAFETY: non-null runtime pointer is owned by caller and created by this crate.
    let rt = unsafe { &mut *runtime };
    match rt.inner.commit() {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Flush buffered log records without commit/rollback fate.
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn synclite_runtime_flush_log(runtime: *mut SyncLiteRuntime) -> c_int {
    clear_last_error();
    if runtime.is_null() {
        set_last_error("runtime is null".to_string());
        return -1;
    }
    // SAFETY: non-null runtime pointer is owned by caller and created by this crate.
    let rt = unsafe { &mut *runtime };
    match rt.inner.flush_log() {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Roll back current transaction scope.
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn synclite_runtime_rollback(runtime: *mut SyncLiteRuntime) -> c_int {
    clear_last_error();
    if runtime.is_null() {
        set_last_error("runtime is null".to_string());
        return -1;
    }
    // SAFETY: non-null runtime pointer is owned by caller and created by this crate.
    let rt = unsafe { &mut *runtime };
    match rt.inner.rollback() {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Close and free runtime handle.
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn synclite_runtime_close(runtime: *mut SyncLiteRuntime) -> c_int {
    clear_last_error();
    if runtime.is_null() {
        set_last_error("runtime is null".to_string());
        return -1;
    }

    // SAFETY: pointer was allocated with Box::into_raw in this crate.
    let boxed = unsafe { Box::from_raw(runtime) };
    match boxed.inner.close() {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Return the last thread-local error message.
///
/// Pointer is valid until the next bindings call on the same thread.
#[no_mangle]
pub extern "C" fn synclite_runtime_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| {
        if let Some(msg) = slot.borrow().as_ref() {
            msg.as_ptr()
        } else {
            std::ptr::null()
        }
    })
}

// ============================================================================
// High-level C ABI — mirrors the Rust facade and the Python binding.
// ============================================================================
//
// Surface:
//   * `synclite_initialize` / `synclite_await_sync`           (module fns)
//   * `synclite_connection_*` / `synclite_stmt_*`             (SQLite family)
//   * `synclite_duckdb_connection_*` / `synclite_duckdb_stmt_*` (DuckDB family)
//   * `synclite_rows_*` for query results
//   * `synclite_last_error()` — shared with the runtime surface above
//
// Conventions:
//   * Functions return `int` (0 = ok, -1 = error) unless they return a handle.
//   * Handles are opaque pointers freed by their `*_close` / `*_free` fn.
//   * Strings are NUL-terminated UTF-8.
//   * `SyncLiteValue` is plain-old-data; lifetime of `text`/`blob` for *inputs*
//     is the caller's; for query result cells it lives as long as the
//     containing `SyncLiteRows`.

use std::ptr;
use std::slice;
use std::time::Duration;

use logger_core::DeviceType;
use logger_db_traits::{Row, Value};
use synclite::{
    duckdb as sl_duck, rusqlite as sl_sqlite, DestinationOptions, DstSyncMode, DstType,
    SyncLiteOptions,
};

/// Alias kept for the public C surface.
#[allow(non_camel_case_types)]
pub type c_size = usize;

/// Tagged-union variant for parameter binding and row cells.
#[repr(C)]
#[derive(Copy, Clone)]
pub enum SyncLiteValueTag {
    /// SQL NULL.
    Null = 0,
    /// 64-bit signed integer.
    Int = 1,
    /// IEEE-754 double.
    Real = 2,
    /// UTF-8 text (NUL-terminated).
    Text = 3,
    /// Raw bytes.
    Blob = 4,
}

/// Variant payload for parameters and result cells.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct SyncLiteValue {
    /// Discriminant.
    pub tag: SyncLiteValueTag,
    /// Set when `tag == Int`.
    pub int_val: i64,
    /// Set when `tag == Real`.
    pub real_val: f64,
    /// Set when `tag == Text`; NUL-terminated UTF-8.
    pub text_val: *const c_char,
    /// Set when `tag == Blob`.
    pub blob_ptr: *const u8,
    /// Byte length for `tag == Blob` (and ignored otherwise).
    pub blob_len: c_size,
}

/// Plain-old-data destination spec passed across the FFI boundary.
#[repr(C)]
pub struct SyncLiteDestination {
    /// "SQLITE" | "DUCKDB" | "POSTGRES"
    pub dst_type: *const c_char,
    /// Backend-specific connection URI / DSN.
    pub dst_connection_string: *const c_char,
    /// Optional logical database name (may be NULL).
    pub dst_database: *const c_char,
    /// Optional schema name (may be NULL).
    pub dst_schema: *const c_char,
    /// "CONSOLIDATION" | "REPLICATION"
    pub dst_sync_mode: *const c_char,
}

/// Opaque SQLite-family connection handle.
pub struct SyncLiteConnection {
    inner: Option<sl_sqlite::Connection>,
}

/// Opaque DuckDB-family connection handle.
pub struct SyncLiteDuckConnection {
    inner: Option<sl_duck::Connection>,
}

/// Opaque prepared-statement handle (carries SQL + queued batch rows).
///
/// The C ABI keeps the statement decoupled from a borrowed `Statement<'_>`
/// because re-borrowing through the FFI is not expressible. Each call
/// re-prepares against the parent connection.
pub struct SyncLiteStatement {
    parent: *mut SyncLiteConnection,
    sql: CString,
    batches: Vec<Vec<Value>>,
}

/// Opaque DuckDB-family prepared statement.
pub struct SyncLiteDuckStatement {
    parent: *mut SyncLiteDuckConnection,
    sql: CString,
    batches: Vec<Vec<Value>>,
}

/// Opaque query result set. Cells live until `synclite_rows_free` is called.
pub struct SyncLiteRows {
    cols: c_size,
    cells: Vec<SyncLiteValue>,
    _strings: Vec<CString>, // backing storage for Text/Blob cell pointers
    _blobs: Vec<Vec<u8>>,
}

// ---------- helpers ---------------------------------------------------------

fn opt_cstr<'a>(p: *const c_char) -> Option<&'a str> {
    if p.is_null() {
        None
    } else {
        // SAFETY: caller passes either NULL or a valid NUL-terminated UTF-8 string.
        unsafe { CStr::from_ptr(p) }.to_str().ok()
    }
}

fn req_cstr<'a>(p: *const c_char, name: &str) -> Result<&'a str, String> {
    opt_cstr(p).ok_or_else(|| format!("{name} is null or not valid UTF-8"))
}

fn parse_device_type(s: &str) -> Result<DeviceType, String> {
    match s.trim().to_ascii_uppercase().as_str() {
        "SQLITE" => Ok(DeviceType::Sqlite),
        "SQLITE_STORE" => Ok(DeviceType::SqliteStore),
        "STREAMING" => Ok(DeviceType::Streaming),
        "DUCKDB" => Ok(DeviceType::DuckDb),
        "DUCKDB_STORE" => Ok(DeviceType::DuckDbStore),
        other => Err(format!(
            "unknown device_type {other:?}; expected SQLITE | SQLITE_STORE | STREAMING | DUCKDB | DUCKDB_STORE"
        )),
    }
}

fn parse_dst_type(s: &str) -> Result<DstType, String> {
    match s.trim().to_ascii_uppercase().as_str() {
        "SQLITE" => Ok(DstType::Sqlite),
        "DUCKDB" => Ok(DstType::DuckDb),
        "POSTGRES" | "POSTGRESQL" => Ok(DstType::Postgres),
        other => Err(format!(
            "unknown dst_type {other:?}; expected SQLITE | DUCKDB | POSTGRES"
        )),
    }
}

fn parse_dst_sync_mode(s: &str) -> Result<DstSyncMode, String> {
    match s.trim().to_ascii_uppercase().as_str() {
        "CONSOLIDATION" => Ok(DstSyncMode::Consolidation),
        "REPLICATION" => Ok(DstSyncMode::Replication),
        other => Err(format!(
            "unknown dst_sync_mode {other:?}; expected CONSOLIDATION | REPLICATION"
        )),
    }
}

unsafe fn convert_params(ptr: *const SyncLiteValue, n: c_size) -> Result<Vec<Value>, String> {
    if n == 0 {
        return Ok(Vec::new());
    }
    if ptr.is_null() {
        return Err("params pointer is null but n > 0".into());
    }
    let slice = slice::from_raw_parts(ptr, n);
    let mut out = Vec::with_capacity(n);
    for v in slice {
        out.push(match v.tag {
            SyncLiteValueTag::Null => Value::Null,
            SyncLiteValueTag::Int => Value::Int(v.int_val),
            SyncLiteValueTag::Real => Value::Real(v.real_val),
            SyncLiteValueTag::Text => {
                if v.text_val.is_null() {
                    Value::Null
                } else {
                    let s = CStr::from_ptr(v.text_val)
                        .to_str()
                        .map_err(|_| "TEXT param is not valid UTF-8".to_string())?;
                    Value::Text(s.to_owned())
                }
            }
            SyncLiteValueTag::Blob => {
                if v.blob_ptr.is_null() || v.blob_len == 0 {
                    Value::Blob(Vec::new())
                } else {
                    let bytes = slice::from_raw_parts(v.blob_ptr, v.blob_len).to_vec();
                    Value::Blob(bytes)
                }
            }
        });
    }
    Ok(out)
}

fn build_rows(rows_in: Vec<Row>) -> Box<SyncLiteRows> {
    let mut cells: Vec<SyncLiteValue> = Vec::new();
    let mut strings: Vec<CString> = Vec::new();
    let mut blobs: Vec<Vec<u8>> = Vec::new();
    let cols = rows_in.first().map(|r| r.len()).unwrap_or(0);
    for row in &rows_in {
        for v in row {
            let cell = match v {
                Value::Null => SyncLiteValue {
                    tag: SyncLiteValueTag::Null,
                    int_val: 0,
                    real_val: 0.0,
                    text_val: ptr::null(),
                    blob_ptr: ptr::null(),
                    blob_len: 0,
                },
                Value::Int(i) => SyncLiteValue {
                    tag: SyncLiteValueTag::Int,
                    int_val: *i,
                    real_val: 0.0,
                    text_val: ptr::null(),
                    blob_ptr: ptr::null(),
                    blob_len: 0,
                },
                Value::Real(f) => SyncLiteValue {
                    tag: SyncLiteValueTag::Real,
                    int_val: 0,
                    real_val: *f,
                    text_val: ptr::null(),
                    blob_ptr: ptr::null(),
                    blob_len: 0,
                },
                Value::Text(s) => {
                    let owned = CString::new(s.replace('\0', " ")).unwrap();
                    let p = owned.as_ptr();
                    strings.push(owned);
                    SyncLiteValue {
                        tag: SyncLiteValueTag::Text,
                        int_val: 0,
                        real_val: 0.0,
                        text_val: p,
                        blob_ptr: ptr::null(),
                        blob_len: 0,
                    }
                }
                Value::Blob(b) => {
                    let owned = b.clone();
                    let p = owned.as_ptr();
                    let len = owned.len();
                    blobs.push(owned);
                    SyncLiteValue {
                        tag: SyncLiteValueTag::Blob,
                        int_val: 0,
                        real_val: 0.0,
                        text_val: ptr::null(),
                        blob_ptr: p,
                        blob_len: len,
                    }
                }
            };
            cells.push(cell);
        }
    }
    Box::new(SyncLiteRows {
        cols,
        cells,
        _strings: strings,
        _blobs: blobs,
    })
}

// ---------- module: initialize / await_sync ---------------------------------

/// Initialize SyncLite for a device. `destination` and `config_path` may be
/// NULL. Returns 0 on success, -1 on error.
///
/// # Safety
/// All non-NULL pointers must be valid NUL-terminated UTF-8 strings.
/// `destination`, if non-NULL, must point to a fully-populated
/// `SyncLiteDestination` whose own string fields satisfy the same rule.
#[no_mangle]
pub unsafe extern "C" fn synclite_initialize(
    device_type: *const c_char,
    device_name: *const c_char,
    db_path: *const c_char,
    destination: *const SyncLiteDestination,
    config_path: *const c_char,
) -> c_int {
    clear_last_error();
    let result: Result<(), String> = (|| {
        let dt = parse_device_type(req_cstr(device_type, "device_type")?)?;
        let name = req_cstr(device_name, "device_name")?;
        let path = req_cstr(db_path, "db_path")?;
        let dest = if destination.is_null() {
            None
        } else {
            let d = &*destination;
            Some(DestinationOptions {
                dst_type: parse_dst_type(req_cstr(d.dst_type, "dst_type")?)?,
                dst_connection_string: req_cstr(
                    d.dst_connection_string,
                    "dst_connection_string",
                )?
                .to_owned(),
                dst_database: opt_cstr(d.dst_database).map(|s| s.to_owned()),
                dst_schema: opt_cstr(d.dst_schema).map(|s| s.to_owned()),
                dst_sync_mode: parse_dst_sync_mode(req_cstr(
                    d.dst_sync_mode,
                    "dst_sync_mode",
                )?)?,
            })
        };
        let opts = SyncLiteOptions {
            config_path: opt_cstr(config_path).map(PathBuf::from),
            ..SyncLiteOptions::default()
        };
        synclite::initialize(dt, name, path, dest, opts).map_err(|e| e.to_string())
    })();
    match result {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e);
            -1
        }
    }
}

/// Block until the embedded shipper + consolidator have drained every
/// rolled segment for `db_path` (up to `timeout_seconds`). 0 = ok.
///
/// # Safety
/// `db_path` must be a valid NUL-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn synclite_await_sync(
    db_path: *const c_char,
    timeout_seconds: f64,
) -> c_int {
    clear_last_error();
    let path = match req_cstr(db_path, "db_path") {
        Ok(s) => s,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    let timeout = Duration::from_secs_f64(timeout_seconds.max(0.0));
    match synclite::await_sync(path, timeout) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Return the last thread-local error message, or NULL if none.
#[no_mangle]
pub extern "C" fn synclite_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| {
        if let Some(msg) = slot.borrow().as_ref() {
            msg.as_ptr()
        } else {
            std::ptr::null()
        }
    })
}

// ---------- SQLite-family Connection ---------------------------------------

macro_rules! conn_impl {
    (
        $open_fn:ident, $open_cfg_fn:ident, $init_fn:ident, $init_cfg_fn:ident,
        $execute_fn:ident, $query_fn:ident, $prepare_fn:ident,
        $set_ac_fn:ident, $get_ac_fn:ident, $commit_fn:ident, $rollback_fn:ident,
        $flush_fn:ident, $close_fn:ident,
        $stmt_exec_fn:ident, $stmt_query_fn:ident,
        $stmt_add_fn:ident, $stmt_clear_fn:ident, $stmt_batch_fn:ident, $stmt_free_fn:ident,
        $conn_ty:ident, $stmt_ty:ident, $facade:path, $run_helper:ident
    ) => {
        /// Open a SyncLite-managed database by path. `initialize` must have
        /// already been called (or a prior layout exists). Returns NULL on error.
        ///
        /// # Safety
        /// `db_path` must be a NUL-terminated UTF-8 string.
        #[no_mangle]
        pub unsafe extern "C" fn $open_fn(db_path: *const c_char) -> *mut $conn_ty {
            clear_last_error();
            let path = match req_cstr(db_path, "db_path") {
                Ok(s) => s,
                Err(e) => {
                    set_last_error(e);
                    return ptr::null_mut();
                }
            };
            match <$facade>::open(path) {
                Ok(c) => Box::into_raw(Box::new($conn_ty::wrap(c))),
                Err(e) => {
                    set_last_error(e.to_string());
                    ptr::null_mut()
                }
            }
        }

        /// Open from an existing `synclite.conf` (legacy `synclite_logger.conf`
        /// also accepted). Returns NULL on error.
        ///
        /// # Safety
        /// `conf_path` must be a NUL-terminated UTF-8 string.
        #[no_mangle]
        pub unsafe extern "C" fn $open_cfg_fn(conf_path: *const c_char) -> *mut $conn_ty {
            clear_last_error();
            let path = match req_cstr(conf_path, "conf_path") {
                Ok(s) => s,
                Err(e) => {
                    set_last_error(e);
                    return ptr::null_mut();
                }
            };
            match <$facade>::open_with_config(path) {
                Ok(c) => Box::into_raw(Box::new($conn_ty::wrap(c))),
                Err(e) => {
                    set_last_error(e.to_string());
                    ptr::null_mut()
                }
            }
        }

        /// Initialize + open in one call using path-derived defaults.
        ///
        /// # Safety
        /// `db_path` must be a NUL-terminated UTF-8 string.
        #[no_mangle]
        pub unsafe extern "C" fn $init_fn(db_path: *const c_char) -> *mut $conn_ty {
            clear_last_error();
            let path = match req_cstr(db_path, "db_path") {
                Ok(s) => s,
                Err(e) => {
                    set_last_error(e);
                    return ptr::null_mut();
                }
            };
            match <$facade>::initialize(path) {
                Ok(c) => Box::into_raw(Box::new($conn_ty::wrap(c))),
                Err(e) => {
                    set_last_error(e.to_string());
                    ptr::null_mut()
                }
            }
        }

        /// Initialize + open from an existing config file.
        ///
        /// # Safety
        /// `conf_path` must be a NUL-terminated UTF-8 string.
        #[no_mangle]
        pub unsafe extern "C" fn $init_cfg_fn(conf_path: *const c_char) -> *mut $conn_ty {
            clear_last_error();
            let path = match req_cstr(conf_path, "conf_path") {
                Ok(s) => s,
                Err(e) => {
                    set_last_error(e);
                    return ptr::null_mut();
                }
            };
            match <$facade>::initialize_with_config(path) {
                Ok(c) => Box::into_raw(Box::new($conn_ty::wrap(c))),
                Err(e) => {
                    set_last_error(e.to_string());
                    ptr::null_mut()
                }
            }
        }

        /// Execute a mutating statement. Writes rows-affected to `out_rows`
        /// (may be NULL). Returns 0 on success, -1 on error.
        ///
        /// # Safety
        /// `conn` must be a live handle from this crate. `params`/`n` must
        /// describe a valid array (or `n == 0` with `params` ignored).
        #[no_mangle]
        pub unsafe extern "C" fn $execute_fn(
            conn: *mut $conn_ty,
            sql: *const c_char,
            params: *const SyncLiteValue,
            n: c_size,
            out_rows: *mut u64,
        ) -> c_int {
            clear_last_error();
            if conn.is_null() {
                set_last_error("connection is null".into());
                return -1;
            }
            let conn_ref = &mut *conn;
            let sql_s = match req_cstr(sql, "sql") {
                Ok(s) => s,
                Err(e) => {
                    set_last_error(e);
                    return -1;
                }
            };
            let p = match convert_params(params, n) {
                Ok(v) => v,
                Err(e) => {
                    set_last_error(e);
                    return -1;
                }
            };
            let inner = match conn_ref.inner.as_mut() {
                Some(i) => i,
                None => {
                    set_last_error("connection is closed".into());
                    return -1;
                }
            };
            match inner.execute(sql_s, &p) {
                Ok(rows) => {
                    if !out_rows.is_null() {
                        *out_rows = rows;
                    }
                    0
                }
                Err(e) => {
                    set_last_error(e.to_string());
                    -1
                }
            }
        }

        /// Run a query. On success writes an owned `SyncLiteRows*` to `*out`
        /// (the caller frees with `synclite_rows_free`). Returns 0 / -1.
        ///
        /// # Safety
        /// Same as `*_execute`, plus `out` must point to writable storage.
        #[no_mangle]
        pub unsafe extern "C" fn $query_fn(
            conn: *mut $conn_ty,
            sql: *const c_char,
            params: *const SyncLiteValue,
            n: c_size,
            out: *mut *mut SyncLiteRows,
        ) -> c_int {
            clear_last_error();
            if conn.is_null() || out.is_null() {
                set_last_error("connection or out is null".into());
                return -1;
            }
            let conn_ref = &mut *conn;
            let sql_s = match req_cstr(sql, "sql") {
                Ok(s) => s,
                Err(e) => {
                    set_last_error(e);
                    return -1;
                }
            };
            let p = match convert_params(params, n) {
                Ok(v) => v,
                Err(e) => {
                    set_last_error(e);
                    return -1;
                }
            };
            let inner = match conn_ref.inner.as_mut() {
                Some(i) => i,
                None => {
                    set_last_error("connection is closed".into());
                    return -1;
                }
            };
            match inner.query(sql_s, &p) {
                Ok(rows) => {
                    *out = Box::into_raw(build_rows(rows));
                    0
                }
                Err(e) => {
                    set_last_error(e.to_string());
                    -1
                }
            }
        }

        /// Create a prepared statement handle. The handle borrows `conn`
        /// logically; the connection must outlive the statement.
        ///
        /// # Safety
        /// `conn` must be live; `sql` must be a NUL-terminated UTF-8 string.
        #[no_mangle]
        pub unsafe extern "C" fn $prepare_fn(
            conn: *mut $conn_ty,
            sql: *const c_char,
        ) -> *mut $stmt_ty {
            clear_last_error();
            if conn.is_null() {
                set_last_error("connection is null".into());
                return ptr::null_mut();
            }
            let s = match req_cstr(sql, "sql") {
                Ok(v) => v,
                Err(e) => {
                    set_last_error(e);
                    return ptr::null_mut();
                }
            };
            let stmt = $stmt_ty {
                parent: conn,
                sql: CString::new(s).unwrap(),
                batches: Vec::new(),
            };
            Box::into_raw(Box::new(stmt))
        }

        /// Set user-facing autocommit on the connection. 0 / -1.
        ///
        /// # Safety
        /// `conn` must be a live handle from this crate.
        #[no_mangle]
        pub unsafe extern "C" fn $set_ac_fn(conn: *mut $conn_ty, auto_commit: c_int) -> c_int {
            clear_last_error();
            if conn.is_null() {
                set_last_error("connection is null".into());
                return -1;
            }
            let conn_ref = &mut *conn;
            match conn_ref.inner.as_mut() {
                Some(c) => {
                    c.set_auto_commit(auto_commit != 0);
                    0
                }
                None => {
                    set_last_error("connection is closed".into());
                    -1
                }
            }
        }

        /// Read autocommit flag. Returns 0/1, or -1 on error.
        ///
        /// # Safety
        /// `conn` must be a live handle from this crate.
        #[no_mangle]
        pub unsafe extern "C" fn $get_ac_fn(conn: *mut $conn_ty) -> c_int {
            clear_last_error();
            if conn.is_null() {
                set_last_error("connection is null".into());
                return -1;
            }
            let conn_ref = &mut *conn;
            match conn_ref.inner.as_mut() {
                Some(c) => {
                    if c.get_auto_commit() { 1 } else { 0 }
                }
                None => {
                    set_last_error("connection is closed".into());
                    -1
                }
            }
        }

        /// Commit current scope. 0 / -1.
        ///
        /// # Safety
        /// `conn` must be a live handle from this crate.
        #[no_mangle]
        pub unsafe extern "C" fn $commit_fn(conn: *mut $conn_ty) -> c_int {
            $run_helper(conn, |c| c.commit())
        }

        /// Roll back current scope. 0 / -1.
        ///
        /// # Safety
        /// `conn` must be a live handle from this crate.
        #[no_mangle]
        pub unsafe extern "C" fn $rollback_fn(conn: *mut $conn_ty) -> c_int {
            $run_helper(conn, |c| c.rollback())
        }

        /// Roll the active log segment. 0 / -1.
        ///
        /// # Safety
        /// `conn` must be a live handle from this crate.
        #[no_mangle]
        pub unsafe extern "C" fn $flush_fn(conn: *mut $conn_ty) -> c_int {
            $run_helper(conn, |c| c.flush())
        }

        /// Close and free the connection handle. 0 / -1.
        ///
        /// # Safety
        /// `conn` must be a handle previously returned by an `*_open*` /
        /// `*_initialize*` fn from this crate, and must not be used again.
        #[no_mangle]
        pub unsafe extern "C" fn $close_fn(conn: *mut $conn_ty) -> c_int {
            clear_last_error();
            if conn.is_null() {
                return 0;
            }
            let mut boxed = Box::from_raw(conn);
            if let Some(c) = boxed.inner.take() {
                if let Err(e) = c.close() {
                    set_last_error(e.to_string());
                    return -1;
                }
            }
            0
        }

        /// Execute the prepared statement once. 0 / -1.
        ///
        /// # Safety
        /// `stmt` must be live; its parent connection must still be open.
        #[no_mangle]
        pub unsafe extern "C" fn $stmt_exec_fn(
            stmt: *mut $stmt_ty,
            params: *const SyncLiteValue,
            n: c_size,
            out_rows: *mut u64,
        ) -> c_int {
            clear_last_error();
            if stmt.is_null() {
                set_last_error("statement is null".into());
                return -1;
            }
            let s = &mut *stmt;
            $execute_fn(s.parent, s.sql.as_ptr(), params, n, out_rows)
        }

        /// Run a query through the prepared statement. 0 / -1.
        ///
        /// # Safety
        /// `stmt`/`out` must be valid; parent connection must still be open.
        #[no_mangle]
        pub unsafe extern "C" fn $stmt_query_fn(
            stmt: *mut $stmt_ty,
            params: *const SyncLiteValue,
            n: c_size,
            out: *mut *mut SyncLiteRows,
        ) -> c_int {
            clear_last_error();
            if stmt.is_null() {
                set_last_error("statement is null".into());
                return -1;
            }
            let s = &mut *stmt;
            $query_fn(s.parent, s.sql.as_ptr(), params, n, out)
        }

        /// Queue one row of bound parameters for `*_execute_batch`. 0 / -1.
        ///
        /// # Safety
        /// `stmt` must be live; `params`/`n` describe a valid array.
        #[no_mangle]
        pub unsafe extern "C" fn $stmt_add_fn(
            stmt: *mut $stmt_ty,
            params: *const SyncLiteValue,
            n: c_size,
        ) -> c_int {
            clear_last_error();
            if stmt.is_null() {
                set_last_error("statement is null".into());
                return -1;
            }
            let s = &mut *stmt;
            match convert_params(params, n) {
                Ok(v) => {
                    s.batches.push(v);
                    0
                }
                Err(e) => {
                    set_last_error(e);
                    -1
                }
            }
        }

        /// Discard all queued batch rows. Always returns 0.
        ///
        /// # Safety
        /// `stmt` must be live.
        #[no_mangle]
        pub unsafe extern "C" fn $stmt_clear_fn(stmt: *mut $stmt_ty) -> c_int {
            clear_last_error();
            if !stmt.is_null() {
                (&mut *stmt).batches.clear();
            }
            0
        }

        /// Execute every queued batch row in one prepared cycle. On success,
        /// allocates a `uint64_t[len]` of rows-affected counts at `*out_rows`
        /// (caller frees with `synclite_free_u64_array`) and writes `len` to
        /// `*out_len`. 0 / -1.
        ///
        /// # Safety
        /// All output pointers must be writable; parent connection must be open.
        #[no_mangle]
        pub unsafe extern "C" fn $stmt_batch_fn(
            stmt: *mut $stmt_ty,
            out_rows: *mut *mut u64,
            out_len: *mut c_size,
        ) -> c_int {
            clear_last_error();
            if stmt.is_null() {
                set_last_error("statement is null".into());
                return -1;
            }
            let s = &mut *stmt;
            if s.parent.is_null() {
                set_last_error("parent connection is null".into());
                return -1;
            }
            let batches = std::mem::take(&mut s.batches);
            if batches.is_empty() {
                if !out_rows.is_null() { *out_rows = ptr::null_mut(); }
                if !out_len.is_null() { *out_len = 0; }
                return 0;
            }
            let parent = &mut *s.parent;
            let conn = match parent.inner.as_mut() {
                Some(c) => c,
                None => {
                    set_last_error("connection is closed".into());
                    return -1;
                }
            };
            let sql_s = match s.sql.to_str() {
                Ok(v) => v,
                Err(_) => {
                    set_last_error("sql is not valid UTF-8".into());
                    return -1;
                }
            };
            let mut prepared = match conn.prepare(sql_s) {
                Ok(p) => p,
                Err(e) => {
                    set_last_error(e.to_string());
                    return -1;
                }
            };
            for row in &batches {
                prepared.add_batch(row);
            }
            match prepared.execute_batch() {
                Ok(counts) => {
                    let mut boxed = counts.into_boxed_slice();
                    if !out_len.is_null() { *out_len = boxed.len(); }
                    if !out_rows.is_null() {
                        *out_rows = boxed.as_mut_ptr();
                    }
                    std::mem::forget(boxed);
                    0
                }
                Err(e) => {
                    set_last_error(e.to_string());
                    -1
                }
            }
        }

        /// Free a prepared-statement handle.
        ///
        /// # Safety
        /// `stmt` must be a handle previously returned by `*_prepare` from
        /// this crate, and must not be used again.
        #[no_mangle]
        pub unsafe extern "C" fn $stmt_free_fn(stmt: *mut $stmt_ty) {
            if !stmt.is_null() {
                drop(Box::from_raw(stmt));
            }
        }
    };
}

impl SyncLiteConnection {
    fn wrap(c: sl_sqlite::Connection) -> Self {
        Self { inner: Some(c) }
    }
}

impl SyncLiteDuckConnection {
    fn wrap(c: sl_duck::Connection) -> Self {
        Self { inner: Some(c) }
    }
}

unsafe fn run_on_conn<F>(conn: *mut SyncLiteConnection, f: F) -> c_int
where
    F: FnOnce(&mut sl_sqlite::Connection) -> Result<(), logger_core::Error>,
{
    clear_last_error();
    if conn.is_null() {
        set_last_error("connection is null".into());
        return -1;
    }
    let c = &mut *conn;
    match c.inner.as_mut() {
        Some(inner) => match f(inner) {
            Ok(()) => 0,
            Err(e) => {
                set_last_error(e.to_string());
                -1
            }
        },
        None => {
            set_last_error("connection is closed".into());
            -1
        }
    }
}

unsafe fn run_on_duck<F>(conn: *mut SyncLiteDuckConnection, f: F) -> c_int
where
    F: FnOnce(&mut sl_duck::Connection) -> Result<(), logger_core::Error>,
{
    clear_last_error();
    if conn.is_null() {
        set_last_error("connection is null".into());
        return -1;
    }
    let c = &mut *conn;
    match c.inner.as_mut() {
        Some(inner) => match f(inner) {
            Ok(()) => 0,
            Err(e) => {
                set_last_error(e.to_string());
                -1
            }
        },
        None => {
            set_last_error("connection is closed".into());
            -1
        }
    }
}

conn_impl!(
    synclite_connection_open,
    synclite_connection_open_with_config,
    synclite_connection_initialize,
    synclite_connection_initialize_with_config,
    synclite_connection_execute,
    synclite_connection_query,
    synclite_connection_prepare,
    synclite_connection_set_auto_commit,
    synclite_connection_get_auto_commit,
    synclite_connection_commit,
    synclite_connection_rollback,
    synclite_connection_flush,
    synclite_connection_close,
    synclite_stmt_execute,
    synclite_stmt_query,
    synclite_stmt_add_batch,
    synclite_stmt_clear_batch,
    synclite_stmt_execute_batch,
    synclite_stmt_free,
    SyncLiteConnection,
    SyncLiteStatement,
    sl_sqlite::Connection,
    run_on_conn
);

conn_impl!(
    synclite_duckdb_connection_open,
    synclite_duckdb_connection_open_with_config,
    synclite_duckdb_connection_initialize,
    synclite_duckdb_connection_initialize_with_config,
    synclite_duckdb_connection_execute,
    synclite_duckdb_connection_query,
    synclite_duckdb_connection_prepare,
    synclite_duckdb_connection_set_auto_commit,
    synclite_duckdb_connection_get_auto_commit,
    synclite_duckdb_connection_commit,
    synclite_duckdb_connection_rollback,
    synclite_duckdb_connection_flush,
    synclite_duckdb_connection_close,
    synclite_duckdb_stmt_execute,
    synclite_duckdb_stmt_query,
    synclite_duckdb_stmt_add_batch,
    synclite_duckdb_stmt_clear_batch,
    synclite_duckdb_stmt_execute_batch,
    synclite_duckdb_stmt_free,
    SyncLiteDuckConnection,
    SyncLiteDuckStatement,
    sl_duck::Connection,
    run_on_duck
);

// ---------- Rows + array free helpers ---------------------------------------

/// Number of rows in a result set.
///
/// # Safety
/// `rows` must be a live handle from `synclite_*_query`.
#[no_mangle]
pub unsafe extern "C" fn synclite_rows_count(rows: *const SyncLiteRows) -> c_size {
    if rows.is_null() { return 0; }
    let r = &*rows;
    if r.cols == 0 { 0 } else { r.cells.len() / r.cols }
}

/// Number of columns in each row of a result set.
///
/// # Safety
/// `rows` must be a live handle from `synclite_*_query`.
#[no_mangle]
pub unsafe extern "C" fn synclite_rows_cols(rows: *const SyncLiteRows) -> c_size {
    if rows.is_null() { 0 } else { (&*rows).cols }
}

/// Borrow a cell. Returned pointer lives until `synclite_rows_free`. NULL on
/// out-of-bounds.
///
/// # Safety
/// `rows` must be a live handle from `synclite_*_query`.
#[no_mangle]
pub unsafe extern "C" fn synclite_rows_cell(
    rows: *const SyncLiteRows,
    row: c_size,
    col: c_size,
) -> *const SyncLiteValue {
    if rows.is_null() { return ptr::null(); }
    let r = &*rows;
    if r.cols == 0 || col >= r.cols { return ptr::null(); }
    let idx = row * r.cols + col;
    if idx >= r.cells.len() { return ptr::null(); }
    &r.cells[idx] as *const SyncLiteValue
}

/// Free a result set. No-op on NULL.
///
/// # Safety
/// `rows` must be a handle previously returned by `synclite_*_query`,
/// and must not be used again.
#[no_mangle]
pub unsafe extern "C" fn synclite_rows_free(rows: *mut SyncLiteRows) {
    if !rows.is_null() {
        drop(Box::from_raw(rows));
    }
}

/// Free a `uint64_t*` buffer allocated by `synclite_*_stmt_execute_batch`.
///
/// # Safety
/// `ptr` must have been produced by `synclite_*_stmt_execute_batch` with
/// length `len`, or be NULL with `len == 0`.
#[no_mangle]
pub unsafe extern "C" fn synclite_free_u64_array(ptr: *mut u64, len: c_size) {
    if ptr.is_null() || len == 0 { return; }
    drop(Vec::from_raw_parts(ptr, len, len));
}

// Bring PathBuf into scope only inside the high-level impl.
use std::path::PathBuf;



