//! JNI surface that lets the pure-Java SyncLite logger
//! (`io.synclite.logger`) drive the Rust consolidator in-process.
//!
//! The Java side wraps this in `io.synclite.SyncLite` and ships
//! it as a single jar (`synclite-consolidator`). The Java logger continues
//! to write the on-disk `*.sqllog` segments exactly as today; this
//! crate only spawns a `consolidator_runtime::Consolidator` per device
//! and lets the Java side notify it when a new segment lands in the
//! stage directory.
//!
//! Surface (all under `io.synclite.NativeConsolidator`):
//!
//! - `nativeSpawnConsolidator(...)        -> long handle`
//! - `nativeNotifyStagePath(handle, path) -> void`
//! - `nativeCatchUpStageDir(handle, dir)  -> void`
//! - `nativeStopConsolidator(handle)      -> void`
//! - `nativePauseSync(dbPath)             -> void`
//! - `nativeResumeSync(dbPath)            -> void`
//! - `nativeIsSyncPaused(dbPath)          -> boolean`
//! - `nativeReinitialize(dbPath, clean)   -> void`
//!
//! Handles are `Box::into_raw(Box::new(Arc<Consolidator>)) as jlong`.
//! All exports trap panics + map errors to `SyncLiteException`.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::Arc;

use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;

use consolidator_core::{
    ConsolidatorLayout, DestinationSyncMode as DstSyncMode, DstType, MetadataStore,
};
use consolidator_runtime::Consolidator;

// ---------- error / panic helpers --------------------------------------------

const EXCEPTION_CLASS: &str = "io/synclite/consolidator/SyncLiteException";

fn throw(env: &mut JNIEnv<'_>, msg: &str) {
    if env.exception_check().unwrap_or(false) {
        return;
    }
    let _ = env.throw_new(EXCEPTION_CLASS, msg);
}

fn guard<R, F>(env: &mut JNIEnv<'_>, default: R, body: F) -> R
where
    F: FnOnce(&mut JNIEnv<'_>) -> Result<R, String>,
{
    let env_ptr = env as *mut JNIEnv<'_>;
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the JNIEnv outlives this closure within the native call.
        let env = unsafe { &mut *env_ptr };
        body(env)
    }));
    match result {
        Ok(Ok(v)) => v,
        Ok(Err(msg)) => {
            throw(env, &msg);
            default
        }
        Err(panic) => {
            let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                format!("rust panic: {s}")
            } else if let Some(s) = panic.downcast_ref::<String>() {
                format!("rust panic: {s}")
            } else {
                "rust panic in synclite native binding".to_string()
            };
            throw(env, &msg);
            default
        }
    }
}

fn jstring_to_string(env: &mut JNIEnv<'_>, s: &JString<'_>) -> Result<String, String> {
    if s.is_null() {
        return Err("required Java string was null".into());
    }
    env.get_string(s)
        .map(|js| js.into())
        .map_err(|e| format!("invalid Java string: {e}"))
}

fn jstring_to_opt_string(
    env: &mut JNIEnv<'_>,
    s: &JString<'_>,
) -> Result<Option<String>, String> {
    if s.is_null() {
        return Ok(None);
    }
    env.get_string(s)
        .map(|js| Some(String::from(js)))
        .map_err(|e| format!("invalid Java string: {e}"))
}

fn parse_dst_type(s: &str) -> Result<DstType, String> {
    match s.trim().to_ascii_uppercase().as_str() {
        "SQLITE" => Ok(DstType::Sqlite),
        "DUCKDB" => Ok(DstType::DuckDb),
        "POSTGRES" | "POSTGRESQL" => Ok(DstType::Postgres),
        other => Err(format!(
            "unknown dst_type {other:?}; expected one of SQLITE, DUCKDB, POSTGRES"
        )),
    }
}

fn parse_sync_mode(s: &str) -> Result<DstSyncMode, String> {
    match s.trim().to_ascii_uppercase().as_str() {
        "CONSOLIDATION" => Ok(DstSyncMode::Consolidation),
        "REPLICATION" => Ok(DstSyncMode::Replication),
        other => Err(format!(
            "unknown dst_sync_mode {other:?}; expected CONSOLIDATION or REPLICATION"
        )),
    }
}

// ---------- handle marshalling -----------------------------------------------

type Handle = Arc<Consolidator>;

fn box_handle(h: Handle) -> jlong {
    Box::into_raw(Box::new(h)) as jlong
}

unsafe fn handle_ref<'a>(handle: jlong) -> Option<&'a Handle> {
    if handle == 0 {
        None
    } else {
        Some(&*(handle as *const Handle))
    }
}

unsafe fn take_handle_box(handle: jlong) -> Option<Box<Handle>> {
    if handle == 0 {
        None
    } else {
        Some(Box::from_raw(handle as *mut Handle))
    }
}

// ---------- exports ----------------------------------------------------------

/// Spawn an in-process `Consolidator` and return an opaque handle.
///
/// Mirrors `ConsolidatorLayout::new` with the small set of fields the
/// Java runtime exposes today; everything else uses Rust defaults.
/// `metadataStore` accepts `LOCAL` or `DESTINATION` (case-insensitive).
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeSpawnConsolidator<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    work_dir: JString<'local>,
    device_data_root: JString<'local>,
    device_id: JString<'local>,
    device_name: JString<'local>,
    device_type: JString<'local>,
    database_name: JString<'local>,
    dst_type_str: JString<'local>,
    dst_connection_string: JString<'local>,
    dst_sync_mode_str: JString<'local>,
    dst_database: JString<'local>,
    dst_schema: JString<'local>,
    metadata_store_str: JString<'local>,
    stage_dir: JString<'local>,
    device_polling_interval_ms: jlong,
) -> jlong {
    guard(&mut env, 0, |env| {
        // Extract the embedded `synclitecdc` helper and set
        // SYNCLITE_CDC_LIB_DIR so the consolidator runtime can dlopen
        // it for SQL replicator replay. The pure-Rust facade does this
        // inside `initialize`; the JNI spawn path bypasses that, so we
        // mirror it here. Without this, SQL device replay falls back
        // to the compat path and writes empty `.cdclog` segments.
        synclite::cdc_native::ensure_extracted();

        let work_dir = PathBuf::from(jstring_to_string(env, &work_dir)?);
        let device_data_root = PathBuf::from(jstring_to_string(env, &device_data_root)?);
        let device_id = jstring_to_string(env, &device_id)?;
        let device_name = jstring_to_string(env, &device_name)?;
        let device_type_s = jstring_to_string(env, &device_type)?;
        let database_name = jstring_to_string(env, &database_name)?;
        let dst_type_s = jstring_to_string(env, &dst_type_str)?;
        let dst_connection_string = jstring_to_string(env, &dst_connection_string)?;
        let dst_sync_mode_s = jstring_to_string(env, &dst_sync_mode_str)?;
        let dst_database_opt = jstring_to_opt_string(env, &dst_database)?;
        let dst_schema_opt = jstring_to_opt_string(env, &dst_schema)?;
        let metadata_store_s = jstring_to_string(env, &metadata_store_str)?;
        let stage_dir_opt = jstring_to_opt_string(env, &stage_dir)?;

        let dst_type = parse_dst_type(&dst_type_s)?;
        let dst_sync_mode = parse_sync_mode(&dst_sync_mode_s)?;
        let metadata_store = match metadata_store_s.trim().to_ascii_uppercase().as_str() {
            "LOCAL" => MetadataStore::Local,
            "DESTINATION" => MetadataStore::Destination,
            other => {
                return Err(format!(
                    "unknown metadata_store {other:?}; expected LOCAL or DESTINATION"
                ));
            }
        };

        let dst_connection_string =
            synclite::normalize_dst_connection_string(dst_type, &dst_connection_string);

        let mut layout = ConsolidatorLayout::new(
            &device_data_root,
            Some(work_dir),
            device_id,
            device_name,
            device_type_s,
            database_name,
            /* dst_index = */ 1,
            /* destination_apply_enabled = */ true,
            metadata_store,
            dst_type,
            dst_sync_mode,
            dst_connection_string,
            /* dst_oper_retry_count = */ 5,
            /* dst_oper_retry_interval_ms = */ 1000,
            /* dst_idempotent_data_ingestion = */ false,
            /* dst_insert_batch_size = */ 1000,
            /* dst_update_batch_size = */ 1000,
            /* dst_delete_batch_size = */ 1000,
            /* cleanup_stage_files = */ true,
        );
        layout.dst_database = dst_database_opt;
        layout.dst_schema = dst_schema_opt;
        if let Some(stage_dir_str) = stage_dir_opt {
            let trimmed = stage_dir_str.trim();
            if !trimmed.is_empty() {
                layout.stage_dir = Some(PathBuf::from(trimmed));
            }
        }
        if device_polling_interval_ms > 0 {
            layout.device_polling_interval_ms = device_polling_interval_ms as u64;
        }

        let consolidator = Consolidator::spawn(layout).map_err(|e| e.to_string())?;
        Ok(box_handle(consolidator))
    })
}

/// Tell the consolidator that a new finalized segment is available at
/// `stage_path`. The Java side calls this from a `WatchService`
/// listener so the consolidator drains immediately instead of waiting
/// for its next polling tick.
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeNotifyStagePath<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    handle: jlong,
    stage_path: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let path = PathBuf::from(jstring_to_string(env, &stage_path)?);
        let consolidator = unsafe { handle_ref(handle) }
            .ok_or_else(|| "consolidator handle is null or closed".to_string())?;
        consolidator
            .notify_stage_path(path)
            .map_err(|e| e.to_string())
    })
}

/// Sweep the stage directory once and notify the consolidator for
/// every existing segment that has not been consumed yet. Used at
/// startup before the `WatchService` is attached, so segments left
/// behind by a previous JVM run get picked up.
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeCatchUpStageDir<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    handle: jlong,
    stage_dir: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let dir = PathBuf::from(jstring_to_string(env, &stage_dir)?);
        let consolidator = unsafe { handle_ref(handle) }
            .ok_or_else(|| "consolidator handle is null or closed".to_string())?;
        consolidator
            .catch_up_stage_dir(&dir)
            .map_err(|e| e.to_string())
    })
}

/// Tell the consolidator that the bootstrap snapshot is available so
/// it can initialize destination state from `backup_path`
/// (`<db>.synclite.backup`) and the matching `metadata_path`
/// (`<db>.synclite.metadata`). Until this fires the worker buffers
/// every `Msg::StagePathReady` it receives and applies them only after
/// bootstrap completes.
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeNotifyBootstrapReady<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    handle: jlong,
    backup_path: JString<'local>,
    metadata_path: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let backup = PathBuf::from(jstring_to_string(env, &backup_path)?);
        let metadata = PathBuf::from(jstring_to_string(env, &metadata_path)?);
        let consolidator = unsafe { handle_ref(handle) }
            .ok_or_else(|| "consolidator handle is null or closed".to_string())?;
        consolidator
            .notify_bootstrap_ready(backup, metadata)
            .map_err(|e| e.to_string())
    })
}

/// Stop the consolidator and free the handle. Idempotent: passing a
/// null/0 handle is a no-op. The dropping `Consolidator::drop`
/// sends `Shutdown` to the worker thread and joins it before
/// returning.
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeStopConsolidator<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        if let Some(boxed) = unsafe { take_handle_box(handle) } {
            drop(boxed);
        }
        Ok(())
    })
}

// ---------- path-based control / inspection ---------------------------------
//
// These mirror the top-level helpers in `synclite::` and operate on
// the per-device on-disk state (sentinels under `<db>.synclite/`,
// `synclite_txn` in the device DB, consolidator stats DB under
// the default work-dir). They do not need a `Consolidator` handle,
// so the Java side does not have to track per-device state to call
// them.

/// Pause destination consolidation for `db_path` (idempotent).
/// The Java logger keeps writing segments; only the apply step pauses.
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativePauseSync<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    db_path: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let path = jstring_to_string(env, &db_path)?;
        synclite::pause_sync(&path).map_err(|e| e.to_string())
    })
}

/// Resume destination consolidation for `db_path` (idempotent).
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeResumeSync<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    db_path: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let path = jstring_to_string(env, &db_path)?;
        synclite::resume_sync(&path).map_err(|e| e.to_string())
    })
}

/// Return `true` if a pause sentinel currently exists for `db_path`.
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeIsSyncPaused<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    db_path: JString<'local>,
) -> jni::sys::jboolean {
    guard(&mut env, 0, |env| {
        let path = jstring_to_string(env, &db_path)?;
        let paused = synclite::is_sync_paused(&path).map_err(|e| e.to_string())?;
        Ok(if paused { 1u8 } else { 0u8 })
    })
}

/// Wipe per-device local state and (when reachable) clean destination
/// metadata rows so the next `synclite::initialize` re-seeds the
/// device from scratch as the same logical device. A sentinel file
/// dropped under the device home causes that next init to force
/// `dst-object-init-mode-1=OVERWRITE_OBJECT` for the re-seed only,
/// so user tables on the destination are cleared (REPLICATION:
/// drop+recreate, CONSOLIDATION: truncate this device's rows). See
/// `synclite::reinitialize::reinitialize` for the full contract.
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeReinitialize<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    db_path: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let path = jstring_to_string(env, &db_path)?;
        synclite::reinitialize(&path).map_err(|e| e.to_string())
    })
}

/// Block until the in-process consolidator has applied every commit
/// the device has produced, or `timeout_ms` elapses. 0 = no wait.
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeAwaitSync<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    db_path: JString<'local>,
    timeout_ms: jlong,
) {
    guard(&mut env, (), |env| {
        let path = jstring_to_string(env, &db_path)?;
        let timeout = std::time::Duration::from_millis(timeout_ms.max(0) as u64);
        synclite::await_sync(&path, timeout).map_err(|e| e.to_string())
    })
}

/// Block until the consolidator's applied commit-id reaches
/// `target_commit_id`. Caller (the Java logger) supplies the target
/// because the Rust runtime cannot read `synclite_txn` from JDBC
/// backends like Derby / H2 / HyperSQL where the table lives inside
/// the backend's own DB file. 0 / negative target = no wait.
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeAwaitAppliedCommit<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    db_path: JString<'local>,
    target_commit_id: jlong,
    timeout_ms: jlong,
) {
    guard(&mut env, (), |env| {
        let path = jstring_to_string(env, &db_path)?;
        let timeout = std::time::Duration::from_millis(timeout_ms.max(0) as u64);
        synclite::await_applied_commit(&path, target_commit_id, timeout)
            .map_err(|e| e.to_string())
    })
}

/// Return `[Integer state, String status, String statusDescription, Long lastHeartbeatTimeMs]`
/// where `state` is the ordinal of `io.synclite.consolidator.SyncState`
/// (0 = NOT_INITIALIZED, 1 = PAUSED, 2 = RUNNING).
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeSyncStatus<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    db_path: JString<'local>,
) -> jni::sys::jobjectArray {
    let null = std::ptr::null_mut();
    guard(&mut env, null, |env| {
        let path = jstring_to_string(env, &db_path)?;
        let st = synclite::sync_status(&path).map_err(|e| e.to_string())?;
        let state_ord = match st.state {
            synclite::SyncState::NotInitialized => 0i32,
            synclite::SyncState::Paused => 1,
            synclite::SyncState::Running => 2,
        };

        let obj_class = env
            .find_class("java/lang/Object")
            .map_err(|e| format!("FindClass Object: {e}"))?;
        let arr = env
            .new_object_array(4, obj_class, jni::objects::JObject::null())
            .map_err(|e| format!("new_object_array: {e}"))?;

        let state_obj = env
            .new_object("java/lang/Integer", "(I)V", &[jni::objects::JValue::Int(state_ord)])
            .map_err(|e| format!("new Integer: {e}"))?;
        env.set_object_array_element(&arr, 0, &state_obj)
            .map_err(|e| format!("set [0]: {e}"))?;

        let status_str = env
            .new_string(&st.status)
            .map_err(|e| format!("new_string status: {e}"))?;
        env.set_object_array_element(&arr, 1, &status_str)
            .map_err(|e| format!("set [1]: {e}"))?;

        let desc_str = env
            .new_string(&st.status_description)
            .map_err(|e| format!("new_string desc: {e}"))?;
        env.set_object_array_element(&arr, 2, &desc_str)
            .map_err(|e| format!("set [2]: {e}"))?;

        let heartbeat_obj = env
            .new_object(
                "java/lang/Long",
                "(J)V",
                &[jni::objects::JValue::Long(st.last_heartbeat_time_ms)],
            )
            .map_err(|e| format!("new Long: {e}"))?;
        env.set_object_array_element(&arr, 3, &heartbeat_obj)
            .map_err(|e| format!("set [3]: {e}"))?;

        Ok(arr.into_raw())
    })
}

/// Return a 6-long array of consolidator counters for `db_path`:
/// `[log_segments_applied, processed_oper_count, processed_txn_count,
///   processed_log_size, last_consolidated_commit_id, last_heartbeat_time_ms]`.
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeSyncStatistics<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    db_path: JString<'local>,
) -> jni::sys::jlongArray {
    let null = std::ptr::null_mut();
    guard(&mut env, null, |env| {
        let path = jstring_to_string(env, &db_path)?;
        let s = synclite::sync_statistics(&path).map_err(|e| e.to_string())?;
        let arr = env
            .new_long_array(6)
            .map_err(|e| format!("new_long_array: {e}"))?;
        let vals: [jlong; 6] = [
            s.log_segments_applied,
            s.processed_oper_count,
            s.processed_txn_count,
            s.processed_log_size,
            s.last_consolidated_commit_id,
            s.last_heartbeat_time_ms,
        ];
        env.set_long_array_region(&arr, 0, &vals)
            .map_err(|e| format!("set_long_array_region: {e}"))?;
        Ok(arr.into_raw())
    })
}

/// Return `[source_commit_id, applied_commit_id_or_min, latency_ms]`.
/// `applied_commit_id_or_min` is `Long.MIN_VALUE` when the consolidator
/// has not yet recorded an applied commit (destination unreachable,
/// consolidator not running, etc.); `latency_ms` is `-1` in that case.
#[no_mangle]
pub extern "system" fn Java_io_synclite_NativeConsolidator_nativeSyncLatency<'local>(
    mut env: JNIEnv<'local>,
    _cls: JClass<'local>,
    db_path: JString<'local>,
) -> jni::sys::jlongArray {
    let null = std::ptr::null_mut();
    guard(&mut env, null, |env| {
        let path = jstring_to_string(env, &db_path)?;
        let l = synclite::sync_latency(&path).map_err(|e| e.to_string())?;
        let applied = l.applied_commit_id.unwrap_or(i64::MIN);
        let arr = env
            .new_long_array(3)
            .map_err(|e| format!("new_long_array: {e}"))?;
        let vals: [jlong; 3] = [l.source_commit_id, applied, l.latency_ms];
        env.set_long_array_region(&arr, 0, &vals)
            .map_err(|e| format!("set_long_array_region: {e}"))?;
        Ok(arr.into_raw())
    })
}
