//! C ABI bindings over `synclite-runtime`.
//!
//! This exposes a minimal, stable surface for non-Rust language bindings.

#![warn(missing_docs)]

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

use synclite_runtime::{Runtime, RuntimeContract};

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

/// Open a SyncLite runtime from `synclite_logger.conf` path.
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
