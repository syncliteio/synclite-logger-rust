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
//! Handles are boxed Rust values containing an `Arc<Consolidator>`.
//! All exports trap panics + map errors to `SyncLiteException`.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;

use consolidator_core::{
    ConsolidatorLayout, DestinationSyncMode as DstSyncMode, DstType, MetadataStore,
};
use consolidator_runtime::Consolidator;

// ---------- error / panic helpers --------------------------------------------

const EXCEPTION_CLASS: &str = "io/synclite/SyncLiteException";

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

fn jstring_to_opt_string(env: &mut JNIEnv<'_>, s: &JString<'_>) -> Result<Option<String>, String> {
    if s.is_null() {
        return Ok(None);
    }
    env.get_string(s)
        .map(|js| Some(String::from(js)))
        .map_err(|e| format!("invalid Java string: {e}"))
}

fn java_thread_is_interrupted(env: &mut JNIEnv<'_>, thread: &JObject<'_>) -> bool {
    env.call_method(thread, "isInterrupted", "()Z", &[])
        .and_then(|value| value.z())
        // Treat a failed interruption check as cancellation. If Java left an
        // exception pending, guard() preserves it rather than replacing it.
        .unwrap_or(true)
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

struct Handle {
    consolidator: Arc<Consolidator>,
    db_path: PathBuf,
    registration: RegistrationToken,
}

#[derive(Clone)]
struct RegisteredDestination {
    token: u64,
    layout: ConsolidatorLayout,
}

struct DestinationGeneration {
    generation: u64,
    expected_count: i32,
    destinations: HashMap<i32, RegisteredDestination>,
}

#[derive(Default)]
struct DestinationLayoutRegistry {
    active: HashMap<PathBuf, DestinationGeneration>,
    pending: HashMap<PathBuf, DestinationGeneration>,
}

#[derive(Clone, Copy)]
struct RegistrationToken {
    generation: u64,
    destination_index: i32,
    token: u64,
}

static DESTINATION_LAYOUTS: OnceLock<Mutex<DestinationLayoutRegistry>> = OnceLock::new();
static NEXT_REGISTRATION_ID: AtomicU64 = AtomicU64::new(1);

fn destination_layouts() -> &'static Mutex<DestinationLayoutRegistry> {
    DESTINATION_LAYOUTS.get_or_init(|| Mutex::new(DestinationLayoutRegistry::default()))
}

fn destination_registry_key(db_path: &PathBuf) -> PathBuf {
    std::fs::canonicalize(db_path).unwrap_or_else(|_| db_path.clone())
}

fn register_destination_layout(
    db_path: &PathBuf,
    layout: ConsolidatorLayout,
    destination_count: i32,
) -> Result<RegistrationToken, String> {
    let key = destination_registry_key(db_path);
    let destination_index = layout.dst_index;
    let mut registry = destination_layouts()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if destination_index == 1 {
        registry.pending.insert(
            key.clone(),
            DestinationGeneration {
                generation: NEXT_REGISTRATION_ID.fetch_add(1, Ordering::Relaxed),
                expected_count: destination_count,
                destinations: HashMap::new(),
            },
        );
    }

    let pending = registry.pending.get_mut(&key).ok_or_else(|| {
        format!(
            "destination {destination_index} was registered before destination 1 for {}",
            key.display()
        )
    })?;
    if pending.expected_count != destination_count {
        return Err(format!(
            "destination count changed during registration for {}: expected {}, got {}",
            key.display(),
            pending.expected_count,
            destination_count
        ));
    }
    if pending.destinations.contains_key(&destination_index) {
        return Err(format!(
            "destination {destination_index} was registered more than once for {}",
            key.display()
        ));
    }

    let token = NEXT_REGISTRATION_ID.fetch_add(1, Ordering::Relaxed);
    let generation = pending.generation;
    pending
        .destinations
        .insert(destination_index, RegisteredDestination { token, layout });

    let complete = pending.destinations.len() == destination_count as usize
        && (1..=destination_count).all(|index| pending.destinations.contains_key(&index));
    if complete {
        let complete_generation = registry
            .pending
            .remove(&key)
            .expect("completed destination generation must still be pending");
        // Publish the complete generation in one map update. Callers can see
        // either the previous complete set or this one, never a partial set.
        registry.active.insert(key, complete_generation);
    }

    Ok(RegistrationToken {
        generation,
        destination_index,
        token,
    })
}

fn generation_contains_token(
    generation: &DestinationGeneration,
    registration: RegistrationToken,
) -> bool {
    generation.generation == registration.generation
        && generation
            .destinations
            .get(&registration.destination_index)
            .is_some_and(|destination| destination.token == registration.token)
}

fn unregister_destination_layout(db_path: &PathBuf, registration: RegistrationToken) {
    let mut registry = destination_layouts()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if registry
        .pending
        .get(db_path)
        .is_some_and(|generation| generation_contains_token(generation, registration))
    {
        // A stopped worker invalidates the complete generation being built.
        registry.pending.remove(db_path);
    }
    if registry
        .active
        .get(db_path)
        .is_some_and(|generation| generation_contains_token(generation, registration))
    {
        // Never expose the remaining workers as a complete destination set.
        registry.active.remove(db_path);
    }
}

fn registered_destination_layouts(db_path: &PathBuf) -> Result<Vec<ConsolidatorLayout>, String> {
    let key = destination_registry_key(db_path);
    let registry = destination_layouts()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(pending) = registry.pending.get(&key) {
        return Err(format!(
            "destination registration is incomplete for {}: registered {} of {}",
            key.display(),
            pending.destinations.len(),
            pending.expected_count
        ));
    }
    let generation = registry
        .active
        .get(&key)
        .ok_or_else(|| format!("no live destinations are registered for {}", key.display()))?;
    let mut layouts = generation
        .destinations
        .values()
        .map(|destination| destination.layout.clone())
        .collect::<Vec<_>>();
    layouts.sort_by_key(|layout| layout.dst_index);
    Ok(layouts)
}

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
    db_path: JString<'local>,
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
    dst_index: jint,
    destination_count: jint,
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

        let db_path = PathBuf::from(jstring_to_string(env, &db_path)?);
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

        if destination_count <= 0 {
            return Err("destination_count must be greater than zero".to_string());
        }
        if dst_index <= 0 || dst_index > destination_count {
            return Err(format!(
                "dst_index must be in the range 1..={destination_count}, got {dst_index}"
            ));
        }

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
            dst_index,
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
        layout.all_dst_indexes = (1..=destination_count).collect();
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

        let await_layout = layout.clone();
        let consolidator = Consolidator::spawn(layout).map_err(|e| e.to_string())?;
        let registration =
            match register_destination_layout(&db_path, await_layout, destination_count) {
                Ok(registration) => registration,
                Err(error) => {
                    consolidator.shutdown();
                    return Err(error);
                }
            };
        Ok(box_handle(Handle {
            consolidator,
            db_path: destination_registry_key(&db_path),
            registration,
        }))
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
        let handle = unsafe { handle_ref(handle) }
            .ok_or_else(|| "consolidator handle is null or closed".to_string())?;
        handle
            .consolidator
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
        let handle = unsafe { handle_ref(handle) }
            .ok_or_else(|| "consolidator handle is null or closed".to_string())?;
        handle
            .consolidator
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
        let handle = unsafe { handle_ref(handle) }
            .ok_or_else(|| "consolidator handle is null or closed".to_string())?;
        handle
            .consolidator
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
            unregister_destination_layout(&boxed.db_path, boxed.registration);
            boxed.consolidator.shutdown();
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

/// Block until every configured destination has applied every commit
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

/// Block until the minimum applied commit-id across all configured
/// destinations reaches `target_commit_id`. Caller (the Java logger)
/// supplies the target because the Rust runtime cannot read
/// `synclite_txn` from JDBC backends like Derby / H2 / HyperSQL where
/// the table lives inside the backend's own DB file. A non-positive
/// target is already complete; a non-positive timeout means no deadline.
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
        let timeout = (timeout_ms > 0).then(|| std::time::Duration::from_millis(timeout_ms as u64));
        let layouts = registered_destination_layouts(&PathBuf::from(&path))?;
        let current_thread = env
            .call_static_method(
                "java/lang/Thread",
                "currentThread",
                "()Ljava/lang/Thread;",
                &[],
            )
            .and_then(|value| value.l())
            .map_err(|error| format!("failed to inspect the current Java thread: {error}"))?;
        synclite::await_applied_commit_for_destinations_with_control(
            &layouts,
            target_commit_id,
            timeout,
            || java_thread_is_interrupted(env, &current_thread),
        )
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
            .new_object(
                "java/lang/Integer",
                "(I)V",
                &[jni::objects::JValue::Int(state_ord)],
            )
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_db_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("synclite-jni-registry-{label}-{nonce}.db"))
    }

    fn layout(db_path: &PathBuf, destination_index: i32, marker: &str) -> ConsolidatorLayout {
        ConsolidatorLayout::new(
            &db_path.with_extension("device"),
            Some(db_path.with_extension(format!("work-{destination_index}"))),
            "device-id",
            "device-name",
            "SQLITE",
            "source.db",
            destination_index,
            true,
            MetadataStore::Destination,
            DstType::Sqlite,
            DstSyncMode::Consolidation,
            marker.to_string(),
            1,
            1,
            false,
            1,
            1,
            1,
            true,
        )
    }

    #[test]
    fn registry_publishes_only_complete_destination_generations() {
        let db_path = unique_db_path("complete");
        let first = register_destination_layout(&db_path, layout(&db_path, 1, "first"), 2).unwrap();
        assert!(registered_destination_layouts(&db_path)
            .unwrap_err()
            .contains("registered 1 of 2"));

        let second =
            register_destination_layout(&db_path, layout(&db_path, 2, "second"), 2).unwrap();
        let layouts = registered_destination_layouts(&db_path).unwrap();
        assert_eq!(layouts.len(), 2);
        assert_eq!(layouts[0].dst_index, 1);
        assert_eq!(layouts[1].dst_index, 2);

        unregister_destination_layout(&db_path, first);
        assert!(registered_destination_layouts(&db_path).is_err());
        unregister_destination_layout(&db_path, second);
    }

    #[test]
    fn stale_handles_cannot_unregister_a_replacement_generation() {
        let db_path = unique_db_path("replacement");
        let old_first =
            register_destination_layout(&db_path, layout(&db_path, 1, "old-1"), 2).unwrap();
        let old_second =
            register_destination_layout(&db_path, layout(&db_path, 2, "old-2"), 2).unwrap();

        let new_first =
            register_destination_layout(&db_path, layout(&db_path, 1, "new-1"), 2).unwrap();
        let new_second =
            register_destination_layout(&db_path, layout(&db_path, 2, "new-2"), 2).unwrap();

        unregister_destination_layout(&db_path, old_first);
        unregister_destination_layout(&db_path, old_second);
        let layouts = registered_destination_layouts(&db_path).unwrap();
        assert_eq!(layouts.len(), 2);
        assert_eq!(layouts[0].dst_connection_string, "new-1");
        assert_eq!(layouts[1].dst_connection_string, "new-2");

        unregister_destination_layout(&db_path, new_first);
        unregister_destination_layout(&db_path, new_second);
    }
}
