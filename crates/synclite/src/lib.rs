//! Top-level facade. Reads SyncLite config and assembles a working
//! [`Logger`] with the right backend, shipper, and archivers wired up.
//!
//! ```no_run
//! use synclite::Logger;
//! let mut logger = Logger::open("synclite.conf").unwrap();
//! logger.execute("CREATE TABLE t(x INTEGER)", &[]).unwrap();
//! logger.close().unwrap();
//! ```
//!
//! Configuration keys are 100% compatible with the Java SyncLite logger
//! and consolidator. Recognized keys (in addition to those parsed by
//! [`logger_config::SyncLiteConfig`]):
//!
//! - `db-engine`: `SQLITE` (default) or `DUCKDB`.
//! - `device-name`: used as the user DB filename stem. Defaults to `device`.
//! - `device-type`: Java device type (`SQLITE`, `DUCKDB`, `STREAMING`, ...).
//! - `local-data-stage-directory`: where log segments are written. Defaults
//!   to `<userHome>/synclite/job1/stageDir`.
//! - `device-data-root`: work-dir root for the embedded consolidator.
//!   Defaults to `<userHome>/synclite/job1/workDir`.
//! - `device-stage-type` (or indexed `device-stage-type-N`): selects the
//!   shipper transport. Supported values: `FS`, `S3`, `MINIO`, `SFTP`.
//! - `device-upload-root`: when stage type is `FS`, the directory the
//!   shipper publishes finalized segments into. Defaults to
//!   `local-data-stage-directory`.
//!
//! When the `s3` feature is enabled and `device-stage-type=S3`:
//!
//! - `s3:endpoint` — optional custom endpoint URL.
//! - `s3:data-stage-bucket-name` — required: target bucket.
//! - `s3:access-key` / `s3:secret-key` — static credentials.
//! - `s3:command-stage-bucket-name` — optional command bucket.
//!
//! When `device-stage-type=MINIO`:
//!
//! - `minio:endpoint`, `minio:access-key`, `minio:secret-key`,
//!   `minio:data-stage-bucket-name`, `minio:command-stage-bucket-name`.
//!
//! When the `sftp` feature is enabled and `device-stage-type=SFTP`:
//!
//! - `sftp:host` — required.
//! - `sftp:port` — TCP port (default 22).
//! - `sftp:user-name` — required.
//! - `sftp:password` — required.
//! - `sftp:remote-data-stage-directory` — required.
//! - `sftp:remote-command-stage-directory` — optional.
//!
//! All keys above accept an indexed form `<key>-N` (single: no suffix;
//! multi: `-1`, `-2`, ...). For prefixed forms it is `s3-N:`, `minio-N:`,
//! `sftp-N:` — exactly matching the Java logger.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use logger_archiver::{Archiver, FsArchiver};
use logger_db_duckdb::{
    DuckDbDevice, DuckDbDeviceConfig,
};
use logger_db_sqlite::{
    SqliteDevice, SqliteDeviceConfig,
};
use logger_db_traits::DbDevice;

// Convenience re-exports so downstream apps only need `synclite` in
// `Cargo.toml` and don't have to depend on `logger_core` / `logger_db_traits`.
pub use logger_core::{Backend, DeviceType, Error, Result};
pub use logger_db_traits::{Row, Value};
use logger_shipper::{LogCleaner, LogShipper, ShipperConfig};

#[path = "logger/backup.rs"]
mod backup;
#[path = "logger/app_lock.rs"]
mod app_lock;
#[path = "logger/layout.rs"]
mod layout;
#[path = "logger/metadata.rs"]
mod metadata;
#[path = "logger/mover.rs"]
mod mover;
#[path = "logger/sql_split.rs"]
mod sql_split;
mod consolidator;
mod cdc_native;
/// Pause / resume sync API. Halts shipping + consolidation while the
/// in-process logger keeps appending segments locally.
pub mod pause;
mod reinitialize;
/// Sync status / latency / statistics inspection helpers.
pub mod status;

pub use pause::{is_sync_paused, pause_sync, resume_sync};
pub use reinitialize::reinitialize;
pub use status::{sync_latency, sync_statistics, sync_status, SyncLatency, SyncState, SyncStatistics, SyncStatus};

use app_lock::AppLock;
use consolidator::{
    Consolidator, ConsolidatorLayout, DstDataTypeMapping,
    DstDeviceSchemaNamePolicy, DstIdempotentDataIngestionMethod,
    DstObjectInitMode, FilterMapperRules, MetadataStore, ValueMapperRules,
};
use layout::{ArchiveLayout, DeviceLayout};
use metadata::Metadata;

pub use consolidator::{DstType, DstSyncMode};
pub use logger_config::SyncLiteConfig;

/// SyncLite-wrapped `duckdb`-style connection and statement APIs.
pub mod duckdb;
/// SyncLite-wrapped `rusqlite`-style connection and statement APIs.
pub mod rusqlite;

/// Java-compatible key for the FS shipper publish/upload root.
pub const DEVICE_UPLOAD_ROOT_KEY: &str = "device-upload-root";
/// Java-compatible key for the embedded consolidator work-dir root.
pub const DEVICE_DATA_ROOT_KEY: &str = "device-data-root";
/// Java-compatible key for SQLite page size of generated `.sqllog` files.
pub const LOG_SEGMENT_PAGE_SIZE_KEY: &str = "log-segment-page-size";
/// Java-compatible key for shipper polling frequency.
pub const LOG_SEGMENT_SHIPPING_FREQUENCY_MS_KEY: &str = "log-segment-shipping-frequency-ms";
/// Java default for `logSegmentPageSize`.
pub const DEFAULT_LOG_SEGMENT_PAGE_SIZE: u32 = 512;
/// Java default for `logSegmentFlushBatchSize`.
pub const DEFAULT_LOG_SEGMENT_FLUSH_BATCH_SIZE: u64 = 10_000;
/// Default polling frequency for segment shipping.
pub const DEFAULT_LOG_SEGMENT_SHIPPING_FREQUENCY_MS: u64 = 1000;
/// Java parity: device-name max length.
pub const MAX_DEVICE_NAME_LEN: usize = 64;

/// Derive an alphanumeric device name from a database path's file stem,
/// stripping any non-alphanumeric characters. Falls back to `"device"`
/// when the stem is empty or yields no alphanumeric characters.
pub fn derive_device_name(db_path: &Path) -> String {
    let stem = db_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    let cleaned: String = stem.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
    if cleaned.is_empty() {
        "device".to_string()
    } else {
        cleaned
    }
}

/// Java-parity default for `local-data-stage-directory`:
/// `<userHome>/synclite/job1/stageDir`.
pub fn default_local_stage_dir() -> PathBuf {
    user_home_or_cwd()
        .join("synclite")
        .join("job1")
        .join("stageDir")
}

/// Java-parity default for `device-data-root` (consolidator work-dir root):
/// `<userHome>/synclite/job1/workDir`.
pub fn default_device_data_root() -> PathBuf {
    user_home_or_cwd()
        .join("synclite")
        .join("job1")
        .join("workDir")
}

fn user_home_or_cwd() -> PathBuf {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

/// Destination overrides for [`initialize`].
#[derive(Debug, Clone)]
pub struct DestinationOptions {
    /// Destination backend/type.
    pub dst_type: DstType,
    /// Destination connection string or local destination path.
    pub dst_connection_string: String,
    /// Optional destination database / catalog name.
    ///
    /// **Required** for [`DstType::Postgres`] and [`DstType::DuckDb`];
    /// rejected for [`DstType::Sqlite`] which is a single-file engine
    /// with no catalog concept.
    pub dst_database: Option<String>,
    /// Optional destination schema name.
    ///
    /// **Required** for [`DstType::Postgres`] (defaults to `public` if
    /// the user really wants that — it must still be passed explicitly).
    /// Optional for [`DstType::DuckDb`] (defaults to `main`).
    /// Rejected for [`DstType::Sqlite`].
    pub dst_schema: Option<String>,
    /// Synchronization mode used by the destination consolidator.
    pub dst_sync_mode: DstSyncMode,
}

impl Default for DestinationOptions {
    fn default() -> Self {
        Self {
            dst_type: DstType::Sqlite,
            dst_connection_string: String::new(),
            dst_database: None,
            dst_schema: None,
            dst_sync_mode: DstSyncMode::Consolidation,
        }
    }
}

/// Optional overrides for [`initialize`].
#[derive(Debug, Clone, Default)]
pub struct SyncLiteOptions {
    /// Optional explicit device name override.
    pub device_name: Option<String>,
    /// Optional parsed config to merge into initialization.
    pub config: Option<SyncLiteConfig>,
    /// Optional config file path to load and merge into initialization.
    pub config_path: Option<PathBuf>,
}

#[deprecated(note = "use SyncLiteOptions instead")]
/// Backward-compatible alias for the renamed SyncLite options bag.
pub type InitializeOptions = SyncLiteOptions;

/// Extra-key names for the S3 stage transport (Java-logger compatible).
pub mod s3_keys {
    /// Required: target data-stage bucket.
    pub const BUCKET: &str = "s3:data-stage-bucket-name";
    /// Optional: target command-stage bucket.
    pub const COMMAND_BUCKET: &str = "s3:command-stage-bucket-name";
    /// Optional: custom endpoint URL.
    pub const ENDPOINT: &str = "s3:endpoint";
    /// Required: static access key.
    pub const ACCESS_KEY: &str = "s3:access-key";
    /// Required: static secret key.
    pub const SECRET_KEY: &str = "s3:secret-key";
}

/// Extra-key names for the MinIO stage transport (Java-logger compatible).
/// MinIO is a separate transport in Java; we mirror that here.
pub mod minio_keys {
    /// Required: target data-stage bucket.
    pub const BUCKET: &str = "minio:data-stage-bucket-name";
    /// Optional: target command-stage bucket.
    pub const COMMAND_BUCKET: &str = "minio:command-stage-bucket-name";
    /// Required: endpoint URL.
    pub const ENDPOINT: &str = "minio:endpoint";
    /// Required: static access key.
    pub const ACCESS_KEY: &str = "minio:access-key";
    /// Required: static secret key.
    pub const SECRET_KEY: &str = "minio:secret-key";
}

/// Extra-key names for the SFTP stage transport (Java-logger compatible).
pub mod sftp_keys {
    /// Required: server hostname.
    pub const HOST: &str = "sftp:host";
    /// Optional: TCP port (default 22).
    pub const PORT: &str = "sftp:port";
    /// Required: SSH username.
    pub const USERNAME: &str = "sftp:user-name";
    /// Required: password.
    pub const PASSWORD: &str = "sftp:password";
    /// Required: remote data-stage directory.
    pub const REMOTE_DIR: &str = "sftp:remote-data-stage-directory";
    /// Optional: remote command-stage directory.
    pub const REMOTE_COMMAND_DIR: &str = "sftp:remote-command-stage-directory";
}

/// Top-level logger. Wraps a [`DbDevice`] and (optionally) a background
/// [`LogShipper`].
pub struct Logger {
    device: Box<dyn DbDevice>,
    /// Held to keep the worker alive for the logger's lifetime. Dropped
    /// when the logger drops, which drains and stops the worker.
    _shipper: Option<Arc<LogShipper>>,
    /// Held to keep the per-device consolidator alive for the logger lifetime.
    _consolidators: Vec<Arc<Consolidator>>,
    /// Held to keep the app lock alive for the logger's lifetime.
    _app_lock: Option<AppLock>,
    /// Java-parity per-device trace file writer (`<db>.synclite/<db>.trace`).
    _tracer: Arc<synclite_observability::Tracer>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DestinationKind {
    Fs,
    S3,
    Minio,
    Sftp,
}

#[derive(Debug, Clone)]
struct DestinationSpec {
    kind: DestinationKind,
    suffix: String,
    key_name: String,
}

/// User-facing top-level API alias.
///
/// `SyncLite` and [`Logger`] are equivalent types.
pub type SyncLite = Logger;

static INITIALIZED_DEVICES: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
const DEFAULT_BATCH_CAPACITY: usize = 4096;

/// Prepared-statement style wrapper.
///
/// Mirrors the standard prepare/add_batch/execute_batch lifecycle while
/// routing execution through SyncLite logging.
pub struct PreparedStatement<'a> {
    logger: &'a mut Logger,
    sql: String,
    batch_params: Vec<Vec<Value>>,
}

impl<'a> PreparedStatement<'a> {
    /// Execute the statement once with one parameter set.
    pub fn execute(&mut self, params: &[Value]) -> Result<u64> {
        self.logger.execute(&self.sql, params)
    }

    /// Execute a read-only query with one parameter set.
    pub fn query(&mut self, params: &[Value]) -> Result<Vec<Row>> {
        self.logger.query(&self.sql, params)
    }

    /// Append one parameter set to the batch.
    pub fn add_batch(&mut self, params: &[Value]) {
        self.batch_params.push(params.to_vec());
    }

    /// Drop all currently buffered parameter sets.
    pub fn clear_batch(&mut self) {
        self.batch_params.clear();
    }

    /// Execute all buffered rows and clear the buffer.
    ///
    /// Rows are removed from memory before execution starts, matching JDBC's
    /// effective no-leak behavior across rollback/error boundaries.
    pub fn execute_batch(&mut self) -> Result<Vec<u64>> {
        let batch = std::mem::take(&mut self.batch_params);
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        self.logger.execute_prepared_batch(&self.sql, &batch)
    }
}

impl std::fmt::Debug for Logger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Logger")
            .field("backend", &self.device.backend())
            .field("has_shipper", &self._shipper.is_some())
            .field("consolidator_count", &self._consolidators.len())
            .finish()
    }
}

/// Initialize a SyncLite device using required `device_type`,
/// `device_name`, and `db_path`, plus optional overrides carried in
/// [`SyncLiteOptions`].
///
/// Idempotent within the current process for the same database path.
/// This is the top-level entry point; it is not specific to the logger
/// subsystem (also brings up the shipper and consolidator pipelines).
pub fn initialize<P: AsRef<Path>>(
    device_type: DeviceType,
    device_name: &str,
    db_path: P,
    destination: Option<DestinationOptions>,
    options: SyncLiteOptions,
) -> Result<()> {
    cdc_native::ensure_extracted();

    let mut cfg = match (options.config, options.config_path) {
        (Some(_), Some(_)) => {
            return Err(Error::Config(
                "initialize accepts either config or config_path, not both".to_string(),
            ));
        }
        (Some(cfg), None) => cfg,
        (None, Some(conf_path)) => SyncLiteConfig::load(conf_path)?,
        (None, None) => SyncLiteConfig::default(),
    };

    // Explicit `device_name` arg is authoritative on first init for
    // this db path; it overrides any value carried in
    // `options.device_name` or the loaded config. On a reopen the
    // device name persisted in metadata at first init wins, and
    // supplying a different name here is rejected by `Logger::open_with`.
    validate_device_name(device_name)?;
    cfg.device_name = Some(device_name.to_string());
    let _ = options.device_name;

    apply_destination_initialize_options(&mut cfg, destination)?;

    let normalized_db_path = normalize_db_path(db_path.as_ref())?;
    let backend = device_type.backend();
    cfg.backend = Some(backend);
    cfg.device_type = Some(device_type);
    cfg.db_path = Some(normalized_db_path.clone());
    if cfg.local_stage_dir.is_none() {
        cfg.local_stage_dir = Some(default_local_stage_dir());
    }

    // Fire any pending reinitialize trigger files dropped alongside
    // the DB. Must run before we open the logger so the next steps
    // start from a clean slate. No-op when no trigger is present.
    reinitialize::maybe_run_trigger(&normalized_db_path, device_name)?;

    // Apply any pending pause/resume trigger files dropped alongside
    // the DB. Trigger-file processing here mirrors `reinitialize`:
    // file presence becomes pause-sentinel state, then the trigger
    // file is removed.
    pause::maybe_run_trigger(&normalized_db_path, device_name)?;

    // Merge any keys persisted by a prior `synclite::initialize` or
    // `reinitialize` (notably `dst-idempotent-data-ingestion-1=true`
    // after a reinit) into cfg.extra; explicit caller-supplied keys win.
    hydrate_initialize_config_from_metadata(&normalized_db_path, &mut cfg);

    // Offline-first: a short connect/round-trip probe of the
    // destination so a bad host / wrong password / missing DB shows
    // up immediately in logs instead of only as a downstream
    // `await_sync` timeout. We never block initialize on the result —
    // the background consolidator will keep retrying when the
    // destination comes back.
    probe_destination_best_effort(&cfg);

    // Persist destination + device config keys into the per-device
    // metadata file BEFORE we ever bring up a Logger. This way any
    // later `Connection::open(db_path)` reconstructs an identical
    // config via `default_config_for_backend`, so the long-running
    // consolidator that actually drains user writes has the right
    // destination wired up.
    persist_initialize_config_to_metadata(&normalized_db_path, &cfg)?;

    let registry = INITIALIZED_DEVICES.get_or_init(|| Mutex::new(HashSet::new()));
    let mut guard = registry
        .lock()
        .map_err(|_| Error::Config("initialize registry mutex poisoned".to_string()))?;
    if guard.contains(&normalized_db_path) {
        return Ok(());
    }

    let logger = Logger::open_with(cfg)?;
    logger.close()?;
    guard.insert(normalized_db_path);
    Ok(())
}

/// Block until the in-process consolidator has fully applied every
/// committed write for the device at `db_path`, or `timeout` elapses.
///
/// Completion is decided by comparing two persisted commit ids:
///   * `source` = `MAX(commit_id)` from the device's `synclite_txn`
///     table (advanced by every successful user commit).
///   * `applied` = `commit_id` recorded in the consolidator's
///     `synclite_metadata` (advanced after every applied segment),
///     with `device_status.last_consolidated_commit_id` as fallback.
/// Returns `Ok` once `applied >= source`.
///
/// Two short-circuit cases return `Ok` immediately:
///   1. **No user commits yet** — `source == 0` (only `initialize`
///      ran, nothing for the consolidator to do).
///   2. **No destination configured** — the device was initialized
///      without any `dst-*` keys, so no consolidator was ever spawned
///      and there is nothing to wait for.
///
/// Rust-only ergonomic helper — the Java logger has no in-process
/// consolidator so this concept doesn't apply there.
///
/// Typical usage from a short-lived sample/CLI before exit:
/// ```ignore
/// conn.flush()?;                                  // roll active segment
/// synclite::await_sync(&db_path, std::time::Duration::from_secs(30))?;
/// ```
pub fn await_sync<P: AsRef<Path>>(db_path: P, timeout: std::time::Duration) -> Result<()> {
    let normalized_db_path = normalize_db_path(db_path.as_ref())?;
    let layout = DeviceLayout::new(normalized_db_path.clone());

    if !layout.metadata_path.exists() {
        return Err(Error::Config(format!(
            "await_sync: device metadata not found at {}",
            layout.metadata_path.display()
        )));
    }

    // Edge case: this device was initialized without any destination
    // configured. There is no consolidator that could ever advance
    // the applied commit id, so sync is trivially "done".
    let md = Metadata::open_or_create(&layout.metadata_path)?;
    let sync_configured = md.get_i64("sync_configured")?.unwrap_or(0) != 0;
    drop(md);
    if !sync_configured {
        return Ok(());
    }

    let poll = std::time::Duration::from_millis(100);
    let deadline = std::time::Instant::now() + timeout;

    loop {
        // `synclite_txn` is bootstrapped (with seed row (0,0)) during
        // initialize() for every device backend / type, so this read
        // is uniform across Sqlite / SqliteStore / DuckDb / DuckDbStore
        // / Streaming.
        let source = status::read_source_commit_id(&normalized_db_path).unwrap_or(0);
        // Edge case: only initialize() has run — no user commits have
        // landed in synclite_txn, so there is nothing for the
        // consolidator to apply.
        if source == 0 {
            return Ok(());
        }
        let applied = status::read_applied_commit_id(&layout).unwrap_or(0);
        if applied >= source {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(Error::Config(format!(
                "await_sync: timed out after {:?} (source_commit_id={}, applied_commit_id={})",
                timeout, source, applied
            )));
        }
        std::thread::sleep(poll);
    }
}

impl Logger {
    /// Read a config file (or config directory) from disk and build a fully
    /// wired logger.
    ///
    /// Supported file names include `synclite_logger.conf`,
    /// `synclite_consolidator.conf`, and `synclite.conf`.
    pub fn open<P: AsRef<Path>>(conf_path: P) -> Result<Self> {
        let cfg = SyncLiteConfig::load_any(conf_path)?;
        Self::open_with(cfg)
    }

    /// Build a fully wired logger from an already-parsed config.
    pub fn open_with(cfg: SyncLiteConfig) -> Result<Self> {
        let (backend, device_type, db_path) = resolve_backend_device_type_db_path(&cfg)?;
        let requested_device_name = cfg.device_name.clone();
        if let Some(name) = requested_device_name.as_deref() {
            validate_device_name(name)?;
        }
        let stage_dir = cfg
            .local_stage_dir
            .clone()
            .unwrap_or_else(default_local_stage_dir);
        std::fs::create_dir_all(&stage_dir)?;
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Java-parity trace file. Default level is ERROR (matches
        // `SyncLite.initTracer`). Failures here are non-fatal: tracing is
        // observability, not correctness.
        let trace_level = cfg
            .trace_level
            .as_deref()
            .map(synclite_observability::TraceLevel::parse)
            .transpose()
            .map_err(|e| Error::Config(format!("invalid trace-level: {e}")))?
            .unwrap_or(synclite_observability::TraceLevel::Error);
        let tracer = synclite_observability::Tracer::for_logger(&db_path, trace_level)
            .map_err(|e| Error::Config(format!("failed to open trace file: {e}")))?;

        // SyncLite layout.
        let layout = DeviceLayout::new(db_path.clone());
        std::fs::create_dir_all(&layout.device_home)?;

        let app_lock = AppLock::try_lock(&db_path)?;

        // Device identity is keyed on `dbPath` (Java parity).
        // `<dbPath>.synclite/<dbfile>.synclite.metadata` owns the UUID
        // *and* the device_name. A reopen that supplies a different
        // device-name for the same path is rejected: the device was
        // already minted with the persisted name and UUID, and silently
        // accepting a new name would fork the stage subdir, consolidator
        // state, and destination identity.
        let md = Metadata::open_or_create(&layout.metadata_path)?;
        let uuid = match md.get("uuid")? {
            Some(v) => v,
            None => {
                let new_uuid = uuid::Uuid::new_v4().to_string();
                md.put("uuid", &new_uuid)?;
                new_uuid
            }
        };
        let device_name = match md.get("device_name")? {
            Some(persisted) => {
                if let Some(requested) = requested_device_name.as_deref() {
                    if requested != persisted {
                        return Err(Error::Config(format!(
                            "device-name mismatch for db_path={}: requested='{}', persisted='{}'. \
                             A SyncLite device is identified by its db path; reopen with device-name='{}' \
                             or move/remove the existing device home at '{}' to mint a new device.",
                            db_path.display(),
                            requested,
                            persisted,
                            persisted,
                            layout.device_home.display()
                        )));
                    }
                }
                persisted
            }
            None => {
                let name = requested_device_name
                    .clone()
                    .unwrap_or_else(|| "device".into());
                validate_device_name(&name)?;
                md.put("device_name", &name)?;
                name
            }
        };
        if md.get("device_type")?.is_none() {
            md.put("device_type", &device_type.to_string())?;
        }
        if md.get("allow_concurrent_writers")?.is_none() {
            md.put_i64(
                "allow_concurrent_writers",
                i64::from(device_type.allows_concurrent_writers()),
            )?;
        }
        if md.get("database_id")?.is_none() {
            md.put_i64("database_id", 0)?;
        }
        if md.get("database_name")?.is_none() {
            md.put("database_name", &layout.db_file_name)?;
        }

        synclite_observability::tracer_info!(
            tracer,
            "SQLLogger",
            "Opening device: name={} uuid={} type={} backend={:?} stageDir={}",
            device_name,
            uuid,
            device_type,
            backend,
            stage_dir.display()
        );

        let archive = ArchiveLayout::new(&stage_dir, &device_name, &uuid, &layout.db_file_name);
        std::fs::create_dir_all(&archive.stage_subdir)?;

        // Java creates synclite_txn during DB init, then invokes the
        // backup agent. Bootstrap the table first so the initial backup
        // snapshot includes it, matching that ordering.
        backup::bootstrap_synclite_txn_table(&db_path, backend)?;

        // Eager backup-at-init (Java BackupAgent parity). Runs once per
        // device lifetime; subsequent opens find both flags set and
        // short-circuit.
        backup::run_initial_backup_if_needed(&layout, &archive, &md, backend, &cfg)?;

        let consolidators = if has_any_destination_config(&cfg) {
            // Persist a marker so path-only callers (e.g. await_sync)
            // can tell a "device with no destination configured" apart
            // from a "device whose consolidator hasn't applied yet".
            Metadata::open_or_create(&layout.metadata_path)?.put_i64("sync_configured", 1)?;
            let dst_indices = parse_cfg_destination_indices(&cfg);
            let multi_destination = dst_indices.len() > 1;
            let all_dst_indexes: Vec<i32> = dst_indices.iter().map(|i| *i as i32).collect();
            let destination_sync_mode = parse_cfg_destination_sync_mode(&cfg);
            let device_data_root = cfg
                .extra
                .get(DEVICE_DATA_ROOT_KEY)
                .map(PathBuf::from)
                .unwrap_or_else(default_device_data_root);
            std::fs::create_dir_all(&device_data_root)?;
            let mut workers = Vec::new();

            // Java parity: PrometheusDumper startup
            // (ConfLoader.enablePrometheusStatisticsPublisher /
            // prometheusPushGatewayURL / prometheusStatisticsPublisherIntervalS).
            // Idempotent: only the first invocation across the process
            // spawns the pusher thread.
            if let Some(raw) = cfg.extra.get("enable-prometheus-statistics-publisher") {
                let enabled = match raw.trim().to_ascii_lowercase().as_str() {
                    "true" => true,
                    "false" => false,
                    other => {
                        return Err(Error::Config(format!(
                            "Invalid value specified for enable-prometheus-statistics-publisher in configuration file : {other}"
                        )))
                    }
                };
                if enabled {
                    let url = cfg
                        .extra
                        .get("prometheus-push-gateway-url")
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| {
                            Error::Config(
                                "prometheus-push-gateway-url not specified while enable-prometheus-statistics-publisher is true"
                                    .to_string(),
                            )
                        })?;
                    let interval = match cfg
                        .extra
                        .get("prometheus-statistics-publisher-interval-s")
                    {
                        Some(v) => v.trim().parse::<i64>().map_err(|_| {
                            Error::Config(
                                "Invalid value specified for prometheus-statistics-publisher-interval-s in configuration file"
                                    .to_string(),
                            )
                        })?,
                        None => 60,
                    };
                    if interval < 0 {
                        return Err(Error::Config(
                            "Please specify a positive numeric value for prometheus-statistics-publisher-interval-s in configuration file"
                                .to_string(),
                        ));
                    }
                    consolidator_runtime::monitor::start_prometheus_publisher(
                        url,
                        interval as u64,
                    );
                }
            }

            for dst_index in dst_indices {
                let metadata_store = parse_cfg_string_for_index(
                    &cfg,
                    &["metadata-store", "dst-metadata-store"],
                    dst_index,
                )
                .map(|v| {
                    if v.eq_ignore_ascii_case("local") || v.eq_ignore_ascii_case("false") {
                        MetadataStore::Local
                    } else {
                        MetadataStore::Destination
                    }
                })
                .unwrap_or(MetadataStore::Destination);

                let dst_type = parse_cfg_destination_backend_for_index(&cfg, dst_index);
                let dst_alias = parse_cfg_string_for_index(&cfg, &["dst-alias"], dst_index)
                    .unwrap_or_else(|| format!("DB-{dst_index}"));
                let dst_connection_string =
                    parse_cfg_destination_connection_for_index(
                        &cfg,
                        dst_index,
                        dst_type,
                        device_data_root
                            .join(&dst_alias)
                            .join(format!("synclite_destination_apply_{dst_index}.db")),
                    );

                let dst_oper_retry_count = parse_cfg_u32_for_index(
                    &cfg,
                    &["dst-oper-retry-count"],
                    dst_index,
                    3,
                );
                let dst_oper_retry_interval_ms = parse_cfg_u64_for_index(
                    &cfg,
                    &["dst-oper-retry-interval-ms"],
                    dst_index,
                    1000,
                );
                let dst_idempotent_data_ingestion = parse_cfg_bool_for_index(
                    &cfg,
                    &["dst-idempotent-data-ingestion"],
                    dst_index,
                    destination_sync_mode == DstSyncMode::Replication,
                );
                let dst_insert_batch_size = parse_cfg_u32_for_index(
                    &cfg,
                    &["dst-insert-batch-size"],
                    dst_index,
                    1000,
                );
                let dst_update_batch_size = parse_cfg_u32_for_index(
                    &cfg,
                    &["dst-update-batch-size"],
                    dst_index,
                    1000,
                );
                let dst_delete_batch_size = parse_cfg_u32_for_index(
                    &cfg,
                    &["dst-delete-batch-size"],
                    dst_index,
                    1000,
                );

                // Java parity: per-destination work dir is
                // <device-data-root>/<dst-alias> when there are multiple
                // destinations. With a single destination the consolidator
                // writes directly to <device-data-root>.
                let consolidator_work_dir = if multi_destination {
                    Some(device_data_root.join(&dst_alias))
                } else {
                    Some(device_data_root.clone())
                };

                let consolidator_layout = ConsolidatorLayout::new(
                    &layout.device_home,
                    consolidator_work_dir,
                    uuid.clone(),
                    device_name.clone(),
                    device_type.to_string(),
                    layout.db_file_name.clone(),
                    dst_index as i32,
                    true,
                    metadata_store,
                    dst_type,
                    destination_sync_mode,
                    dst_connection_string,
                    dst_oper_retry_count,
                    dst_oper_retry_interval_ms,
                    dst_idempotent_data_ingestion,
                    dst_insert_batch_size,
                    dst_update_batch_size,
                    dst_delete_batch_size,
                    true,
                );
                let mut consolidator_layout = consolidator_layout;
                // Java parity: `device-trace-level` (consolidator alias for the
                // logger `trace-level` key). Already merged in the config parser.
                consolidator_layout.trace_level = cfg.trace_level.clone();
                // Pause sentinel: shared across all destinations for
                // this device. Presence == sync paused.
                consolidator_layout.pause_sentinel =
                    Some(pause::pause_sentinel_path(&layout.device_home));
                // Java parity: each consolidator needs the full destination set
                // so stage cleanup can gate on min(last applied seq) across all
                // destinations (Device.getLastConsolidatedLogSegmentSequenceNumber).
                consolidator_layout.all_dst_indexes = all_dst_indexes.clone();
                // Java parity: per-destination filter-mapper rules
                // (`dst-enable-filter-mapper-rules-N`,
                // `dst-filter-mapper-rules-file-N`,
                // `dst-allow-unspecified-tables-N`,
                // `dst-allow-unspecified-columns-N`).
                let enable_filter_mapper = parse_cfg_bool_for_index(
                    &cfg,
                    &["dst-enable-filter-mapper-rules"],
                    dst_index,
                    false,
                );
                if enable_filter_mapper {
                    let rules_file = parse_cfg_string_for_index(
                        &cfg,
                        &["dst-filter-mapper-rules-file"],
                        dst_index,
                    )
                    .ok_or_else(|| {
                        crate::Error::Config(format!(
                            "dst-filter-mapper-rules-file-{dst_index} must be specified when dst-enable-filter-mapper-rules-{dst_index}=true"
                        ))
                    })?;
                    let allow_unspec_tables = parse_cfg_bool_for_index(
                        &cfg,
                        &["dst-allow-unspecified-tables"],
                        dst_index,
                        false,
                    );
                    let allow_unspec_cols = parse_cfg_bool_for_index(
                        &cfg,
                        &["dst-allow-unspecified-columns"],
                        dst_index,
                        false,
                    );
                    consolidator_layout.filter_mapper = FilterMapperRules::parse_rules_file(
                        std::path::Path::new(&rules_file),
                        allow_unspec_tables,
                        allow_unspec_cols,
                    )
                    .map_err(crate::Error::Config)?;
                }
                // Java parity: per-destination value-mapper rules
                // (`dst-enable-value-mapper-N`, `dst-value-mappings-file-N`).
                let enable_value_mapper = parse_cfg_bool_for_index(
                    &cfg,
                    &["dst-enable-value-mapper"],
                    dst_index,
                    false,
                );
                if enable_value_mapper {
                    let mappings_file = parse_cfg_string_for_index(
                        &cfg,
                        &["dst-value-mappings-file"],
                        dst_index,
                    )
                    .ok_or_else(|| {
                        crate::Error::Config(format!(
                            "dst-value-mappings-file-{dst_index} must be specified when dst-enable-value-mapper-{dst_index}=true"
                        ))
                    })?;
                    consolidator_layout.value_mapper = ValueMapperRules::parse_mappings_file(
                        std::path::Path::new(&mappings_file),
                        &consolidator_layout.filter_mapper,
                    )
                    .map_err(crate::Error::Config)?;
                }

                // Java parity: `dst-data-type-mapping-N` +
                // `dst-vector-extension-enabled-N` + collect every
                // `map-src-<type>-to-dst-N` user override into the layout
                // so the DataTypeMapper can honor CUSTOMIZED mode.
                let mapping_mode = match parse_cfg_string_for_index(
                    &cfg,
                    &["dst-data-type-mapping"],
                    dst_index,
                ) {
                    Some(raw) => DstDataTypeMapping::parse(raw.trim()).ok_or_else(|| {
                        Error::Config(format!(
                            "Invalid value specified for dst-data-type-mapping-{dst_index} in configuration file : {raw}"
                        ))
                    })?,
                    None => DstDataTypeMapping::default(),
                };
                consolidator_layout.dst_data_type_mapping = mapping_mode;
                consolidator_layout.dst_vector_extension_enabled = parse_cfg_bool_for_index(
                    &cfg,
                    &["dst-vector-extension-enabled"],
                    dst_index,
                    false,
                );
                let suffix = format!("-to-dst-{dst_index}");
                for (k, v) in cfg.extra.iter() {
                    if let Some(rest) = k.strip_prefix("map-src-") {
                        if let Some(src_token) = rest.strip_suffix(&suffix) {
                            let key = src_token.trim().to_ascii_lowercase();
                            consolidator_layout
                                .user_type_overrides
                                .insert(key, v.trim().to_string());
                        }
                    }
                }

                // Java parity: Tier A — `dst-object-init-mode-N`.
                if let Some(raw) =
                    parse_cfg_string_for_index(&cfg, &["dst-object-init-mode"], dst_index)
                {
                    consolidator_layout.dst_object_init_mode = DstObjectInitMode::parse(raw.trim())
                        .ok_or_else(|| {
                            Error::Config(format!(
                                "Invalid value specified for dst-object-init-mode-{dst_index} in configuration file : {raw}"
                            ))
                        })?;
                }

                // Java parity: Tier B — `dst-idempotent-data-ingestion-method-N`.
                if let Some(raw) = parse_cfg_string_for_index(
                    &cfg,
                    &["dst-idempotent-data-ingestion-method"],
                    dst_index,
                ) {
                    consolidator_layout.dst_idempotent_data_ingestion_method =
                        DstIdempotentDataIngestionMethod::parse(raw.trim()).ok_or_else(|| {
                            Error::Config(format!(
                                "Invalid value specified for dst-idempotent-data-ingestion-method-{dst_index} in configuration file : {raw}"
                            ))
                        })?;
                }

                // Java parity: Tier B — `dst-device-schema-name-policy-N`.
                if let Some(raw) = parse_cfg_string_for_index(
                    &cfg,
                    &["dst-device-schema-name-policy"],
                    dst_index,
                ) {
                    consolidator_layout.dst_device_schema_name_policy =
                        DstDeviceSchemaNamePolicy::parse(raw.trim()).ok_or_else(|| {
                            Error::Config(format!(
                                "Invalid value specified for dst-device-schema-name-policy-{dst_index} in configuration file : {raw}"
                            ))
                        })?;
                }

                // Java parity: Tier B — small scalar knobs.
                consolidator_layout.dst_connection_timeout_s = parse_cfg_u32_for_index(
                    &cfg,
                    &["dst-connection-timeout-s"],
                    dst_index,
                    30,
                );
                consolidator_layout.dst_skip_failed_log_files = parse_cfg_bool_for_index(
                    &cfg,
                    &["dst-skip-failed-log-files"],
                    dst_index,
                    false,
                );
                consolidator_layout.dst_set_unparsable_values_to_null = parse_cfg_bool_for_index(
                    &cfg,
                    &["dst-set-unparsable-values-to-null"],
                    dst_index,
                    false,
                );
                consolidator_layout.dst_quote_object_names = parse_cfg_bool_for_index(
                    &cfg,
                    &["dst-quote-object-names"],
                    dst_index,
                    false,
                );
                consolidator_layout.dst_quote_column_names = parse_cfg_bool_for_index(
                    &cfg,
                    &["dst-quote-column-names"],
                    dst_index,
                    false,
                );
                consolidator_layout.dst_use_catalog_scope_resolution = parse_cfg_bool_for_index(
                    &cfg,
                    &["dst-use-catalog-scope-resolution"],
                    dst_index,
                    true,
                );
                consolidator_layout.dst_use_schema_scope_resolution = parse_cfg_bool_for_index(
                    &cfg,
                    &["dst-use-schema-scope-resolution"],
                    dst_index,
                    true,
                );
                consolidator_layout.dst_oper_predicate_optimization = parse_cfg_bool_for_index(
                    &cfg,
                    &["dst-oper-predicate-optimization"],
                    dst_index,
                    true,
                );
                consolidator_layout.dst_database =
                    parse_cfg_string_for_index(&cfg, &["dst-database"], dst_index)
                        .map(|s| s.trim().to_string());
                consolidator_layout.dst_schema =
                    parse_cfg_string_for_index(&cfg, &["dst-schema"], dst_index)
                        .map(|s| s.trim().to_string());
                consolidator_layout.dst_user =
                    parse_cfg_string_for_index(&cfg, &["dst-user"], dst_index)
                        .map(|s| s.trim().to_string());
                consolidator_layout.dst_password =
                    parse_cfg_string_for_index(&cfg, &["dst-password"], dst_index);
                consolidator_layout.dst_alias = dst_alias.clone();
                consolidator_layout.dst_type_name =
                    parse_cfg_string_for_index(&cfg, &["dst-type-name"], dst_index)
                        .map(|s| s.trim().to_string());
                consolidator_layout.dst_create_table_suffix =
                    parse_cfg_string_for_index(&cfg, &["dst-create-table-suffix"], dst_index)
                        .unwrap_or_default();

                // Java parity: Tier B — `dst-enable-triggers-N` + `dst-triggers-file-N`.
                consolidator_layout.dst_enable_triggers = parse_cfg_bool_for_index(
                    &cfg,
                    &["dst-enable-triggers"],
                    dst_index,
                    false,
                );
                if consolidator_layout.dst_enable_triggers {
                    let triggers_file = parse_cfg_string_for_index(
                        &cfg,
                        &["dst-triggers-file"],
                        dst_index,
                    )
                    .ok_or_else(|| {
                        Error::Config(format!(
                            "dst-triggers-file-{dst_index} must be specified when dst-enable-triggers-{dst_index}=true"
                        ))
                    })?;
                    let trig_path = std::path::PathBuf::from(triggers_file.trim());
                    consolidator_layout.dst_triggers =
                        consolidator::parse_triggers_file(&trig_path).map_err(Error::Config)?;
                    consolidator_layout.dst_triggers_file = Some(trig_path);
                }

                workers.push(Consolidator::spawn(consolidator_layout)?);
            }

            // Java parity: each spawned per-destination consolidator
            // counts as one initialized + registered device for
            // Monitor.PrometheusDumper.
            let m = consolidator_runtime::monitor::monitor();
            m.incr_registered_device_cnt(workers.len() as i64);
            m.incr_initialized_device_cnt(workers.len() as i64);
            m.incr_initialization_cnt(workers.len() as i64);

            workers
        } else {
            Vec::new()
        };
        // Shipper (FS / S3 / SFTP if configured).
        let shipper = build_shipper(
            &cfg,
            archive.stage_subdir.clone(),
            tracer.clone(),
            consolidators.clone(),
        )?;

        // Embedded consolidator notification is driven by the shipper
        // (via `on_shipped`) when both are wired up: a segment must
        // reach every configured archiver before the consolidator is
        // allowed to apply + clean it up, otherwise the consolidator's
        // post-apply cleanup could delete the staged file before it has
        // been shipped to all stages. With no shipper, the mover
        // remains the sole notifier.
        let mover_consolidators = if shipper.is_some() {
            Vec::new()
        } else {
            consolidators.clone()
        };

        // The move callback: device home → stage subdir → shipper.
        let cb = mover::make_callback(
            archive.stage_subdir.clone(),
            layout.metadata_path.clone(),
            shipper.clone(),
            mover_consolidators,
            false,
            tracer.clone(),
        );

        // Open backend. Segments are *written* in the device home; resume
        // state is read from the stage subdir where finalized segments
        // accumulate after the move.
        let log_segment_page_size = cfg
            .log_segment_page_size
            .or_else(|| {
                cfg.extra
                    .get(LOG_SEGMENT_PAGE_SIZE_KEY)
                    .and_then(|v| v.parse::<u32>().ok())
            })
            .unwrap_or(DEFAULT_LOG_SEGMENT_PAGE_SIZE);
        let log_segment_flush_batch_size = cfg
            .log_segment_flush_batch_size
            .unwrap_or(DEFAULT_LOG_SEGMENT_FLUSH_BATCH_SIZE);
        let max_inlined_log_args = cfg.max_inlined_log_args.unwrap_or(16);

        let device: Box<dyn DbDevice> = match backend {
            Backend::Sqlite => {
                let mut dcfg =
                    SqliteDeviceConfig::new(db_path.clone(), layout.device_home.clone());
                dcfg.resume_dir = Some(archive.stage_subdir.clone());
                dcfg.on_segment_ready = Some(cb);
                dcfg.log_segment_page_size = log_segment_page_size;
                dcfg.log_segment_flush_batch_size = log_segment_flush_batch_size;
                dcfg.device_type = device_type;
                dcfg.max_inlined_log_args = max_inlined_log_args;
                dcfg.skip_restart_recovery = cfg.skip_restart_recovery.unwrap_or(false);
                Box::new(SqliteDevice::open(dcfg)?)
            }
            Backend::DuckDb => {
                let mut dcfg =
                    DuckDbDeviceConfig::new(db_path.clone(), layout.device_home.clone());
                dcfg.resume_dir = Some(archive.stage_subdir.clone());
                dcfg.on_segment_ready = Some(cb);
                dcfg.log_segment_page_size = log_segment_page_size;
                dcfg.log_segment_flush_batch_size = log_segment_flush_batch_size;
                dcfg.device_type = device_type;
                dcfg.max_inlined_log_args = max_inlined_log_args;
                dcfg.skip_restart_recovery = cfg.skip_restart_recovery.unwrap_or(false);
                Box::new(DuckDbDevice::open(dcfg)?)
            }
        };

        // Run consolidator bootstrap/catch-up only after device open.
        // For DuckDB this ensures restart recovery has already decided the
        // tail transaction fate before staged sqllog segments are scanned.
        //
        // When a shipper is wired up the shipper drives consolidator
        // notifications (via `on_shipped`) and its own startup catch-up
        // will re-issue them for every pre-existing staged segment, so
        // running `catch_up_stage_dir` here would race the shipper and
        // could let the consolidator apply + delete a segment before
        // every archiver has shipped it. With no shipper, the
        // consolidator is the only catch-up driver.
        let consolidator_owns_catch_up = shipper.is_none();
        for consolidator in &consolidators {
            consolidator.notify_bootstrap_ready(
                archive.stage_backup_path.clone(),
                archive.stage_metadata_path.clone(),
            )?;
            if consolidator_owns_catch_up {
                consolidator.catch_up_stage_dir(&archive.stage_subdir)?;
            }
        }

        // Java parity: emit a single INFO trace describing the recovered
        // state after backend open. Mirrors `SQLLogger.restartRecovery`
        // success log: "Restart recovery completed : ...".
        let recovered_seq = md.get_i64("log_segment_sequence_number")?.unwrap_or(-1);
        let recovered_data_seq = md.get_i64("data_file_sequence_number")?.unwrap_or(-1);
        synclite_observability::tracer_info!(
            tracer,
            "SQLLogger",
            "Restart recovery completed : dbPath={} logSegmentSeq={} dataFileSeq={}",
            db_path.display(),
            recovered_seq,
            recovered_data_seq
        );

        Ok(Self {
            device,
            _shipper: shipper,
            _consolidators: consolidators,
            _app_lock: Some(app_lock),
            _tracer: tracer,
        })
    }

    /// Backend in use.
    pub fn backend(&self) -> Backend {
        self.device.backend()
    }

    /// Execute a mutating statement.
    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64> {
        self.device.execute(sql, params)
    }

    /// Execute a mutating statement without writing to commandlog.
    pub fn execute_unlogged(&mut self, sql: &str, params: &[Value]) -> Result<u64> {
        self.device.execute_unlogged(sql, params)
    }

    /// Runtime pre-hook before wrapper executes on user DB.
    pub fn pre_user_execute(&mut self, sql: &str, params: &[Value]) -> Result<()> {
        self.device.pre_user_execute(sql, params)
    }

    /// Runtime post-hook after wrapper executes on user DB.
    pub fn post_user_execute(&mut self, sql: &str) -> Result<()> {
        self.device.post_user_execute(sql)
    }

    /// Runtime pre-hook before wrapper executes a prepared batch on user DB.
    pub fn pre_user_execute_batch(&mut self, sql: &str, batch_params: &[Vec<Value>]) -> Result<()> {
        self.device.pre_user_execute_batch(sql, batch_params)
    }

    /// Runtime post-hook after wrapper executes a prepared batch on user DB.
    pub fn post_user_execute_batch(&mut self, sql: &str) -> Result<()> {
        self.device.post_user_execute_batch(sql)
    }

    /// Log one SQL operation without executing it on the user DB.
    pub fn log_sql(&mut self, sql: &str, params: &[Value]) -> Result<()> {
        self.device.log_record(Some(sql), params)
    }

    pub(crate) fn log_record(&mut self, sql: Option<&str>, params: &[Value]) -> Result<()> {
        self.device.log_record(sql, params)
    }

    /// Prepare one SQL statement for repeated execution.
    pub fn prepare<'a>(&'a mut self, sql: impl Into<String>) -> PreparedStatement<'a> {
        PreparedStatement {
            logger: self,
            sql: sql.into(),
            batch_params: Vec::with_capacity(DEFAULT_BATCH_CAPACITY),
        }
    }

    /// Execute a prepared batch directly against the wrapped device.
    pub fn execute_prepared_batch(&mut self, sql: &str, batch_params: &[Vec<Value>]) -> Result<Vec<u64>> {
        let backend = self.backend();
        if backend == Backend::DuckDb || backend == Backend::Sqlite {
            self.pre_user_execute_batch(sql, batch_params)?;
            for (idx, params) in batch_params.iter().enumerate() {
                let logged_sql = if idx == 0 { Some(sql) } else { None };
                self.log_record(logged_sql, params)?;
            }
        }
        self.device.execute_prepared_batch(sql, batch_params)
    }

    /// Run a read-only query.
    pub fn query(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        self.device.query(sql, params)
    }

    /// Commit the active logical transaction.
    pub fn commit(&mut self) -> Result<()> {
        self.device.commit()
    }

    /// Flush buffered log records without deciding transaction fate.
    pub fn flush_log(&mut self) -> Result<()> {
        self.device.flush_log()
    }

    /// Finalize the active log segment (rolls to a fresh one). Use this
    /// before [`await_sync`] to force the current segment to ship
    /// without closing the logger.
    pub fn flush(&mut self) -> Result<()> {
        self.device.roll_segment()
    }

    /// Roll back the active logical transaction.
    pub fn rollback(&mut self) -> Result<()> {
        self.device.rollback()
    }

    /// Close the logger, finalizing the current segment.
    pub fn close(self) -> Result<()> {
        let Logger {
            device,
            _shipper,
            _consolidators,
            _app_lock,
            _tracer,
        } = self;
        synclite_observability::tracer_info!(_tracer, "SQLLogger", "Device closed");
        device.close()?;
        // Drop the shipper after close so the final segment has a chance
        // to be enqueued.
        drop(_shipper);
        drop(_consolidators);
        drop(_app_lock);
        drop(_tracer);
        Ok(())
    }
}

fn validate_device_name(device_name: &str) -> Result<()> {
    if device_name.is_empty() {
        return Err(Error::Config("device-name must not be empty".into()));
    }
    if device_name.len() > MAX_DEVICE_NAME_LEN {
        return Err(Error::Config(format!(
            "device-name length exceeded the maximum allowed length of {}",
            MAX_DEVICE_NAME_LEN
        )));
    }
    if !device_name.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        return Err(Error::Config(
            "device-name must only contain alphanumeric characters".into(),
        ));
    }
    Ok(())
}

fn should_enable_event_consolidator(device_type: DeviceType) -> bool {
    device_type.is_store()
        || device_type.is_streaming()
        || matches!(device_type, DeviceType::Sqlite | DeviceType::DuckDb)
}

/// Java parity: the embedded consolidator is activated iff the config
/// declares at least one destination (`dst-type[-N]` plus either
/// `dst-connection-string[-N]` or matching scheme keys), or
/// `initialize` was given a `DestinationOptions`.
fn has_any_destination_config(cfg: &SyncLiteConfig) -> bool {
    if !should_enable_event_consolidator(
        cfg.device_type
            .unwrap_or_else(|| DeviceType::default_for_backend(cfg.backend.unwrap_or(Backend::Sqlite))),
    ) {
        return false;
    }
    if cfg.extra.contains_key("dst-type") || cfg.extra.contains_key("dst-connection-string") {
        return true;
    }
    cfg.extra.keys().any(|k| {
        k.starts_with("dst-type-") || k.starts_with("dst-connection-string-")
    })
}

fn parse_destination_specs(extra: &HashMap<String, String>) -> Result<Vec<DestinationSpec>> {
    let mut specs = Vec::new();

    if let Some(value) = extra.get("device-stage-type") {
        let kind = parse_destination_kind(value, "device-stage-type")?;
        if !has_stage_destination_config(extra, kind, "") {
            return Ok(specs);
        }
        specs.push(DestinationSpec {
            kind,
            suffix: String::new(),
            key_name: "device-stage-type".to_string(),
        });
        return Ok(specs);
    }

    let mut idx = 1usize;
    loop {
        let key = format!("device-stage-type-{idx}");
        let Some(value) = extra.get(&key) else { break };
        let kind = parse_destination_kind(value, &key)?;
        if !has_stage_destination_config(extra, kind, &format!("-{idx}")) {
            idx += 1;
            continue;
        }
        specs.push(DestinationSpec {
            kind,
            suffix: format!("-{idx}"),
            key_name: key,
        });
        idx += 1;
    }

    Ok(specs)
}

fn parse_destination_kind(value: &str, key_name: &str) -> Result<DestinationKind> {
    match value.trim().to_ascii_uppercase().as_str() {
        "FS" | "MS_ONEDRIVE" | "GOOGLE_DRIVE" => Ok(DestinationKind::Fs),
        "S3" => Ok(DestinationKind::S3),
        "MINIO" | "LOCAL_MINIO" | "REMOTE_MINIO" => Ok(DestinationKind::Minio),
        "SFTP" | "LOCAL_SFTP" | "REMOTE_SFTP" => Ok(DestinationKind::Sftp),
        "KAFKA" => Err(Error::Config(format!(
            "unsupported {key_name}: KAFKA (Kafka stage is not implemented yet)"
        ))),
        other => Err(Error::Config(format!(
            "unsupported {key_name}: {other} (supported: FS/MS_ONEDRIVE/GOOGLE_DRIVE, S3, MINIO/LOCAL_MINIO/REMOTE_MINIO, SFTP/LOCAL_SFTP/REMOTE_SFTP)"
        ))),
    }
}

fn has_stage_destination_config(
    extra: &HashMap<String, String>,
    kind: DestinationKind,
    suffix: &str,
) -> bool {
    match kind {
        // FS does not require a separate target; it defaults to the local
        // stage directory (Java parity: local-data-stage-directory is the
        // upload root).
        DestinationKind::Fs => true,
        DestinationKind::S3 => {
            extra.contains_key(&indexed_key("s3", suffix, ":data-stage-bucket-name"))
                || extra.contains_key(s3_keys::BUCKET)
        }
        DestinationKind::Minio => {
            extra.contains_key(&indexed_key("minio", suffix, ":data-stage-bucket-name"))
                || extra.contains_key(minio_keys::BUCKET)
        }
        DestinationKind::Sftp => {
            extra.contains_key(&indexed_key("sftp", suffix, ":host"))
                || extra.contains_key(sftp_keys::HOST)
        }
    }
}

/// Build an indexed prefixed key like `s3-1:host`. Single-destination uses
/// no suffix (`""`); multi uses `-N` (already including the leading dash).
fn indexed_key(prefix: &str, suffix: &str, rest: &str) -> String {
    format!("{prefix}{suffix}{rest}")
}

fn key_for(base: &str, suffix: &str) -> String {
    format!("{base}{suffix}")
}

fn normalize_db_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()?.join(path))
}

#[allow(dead_code)]
fn parse_cfg_bool(cfg: &SyncLiteConfig, keys: &[&str], default: bool) -> bool {
    for key in keys {
        if let Some(v) = cfg.extra.get(*key) {
            if let Ok(parsed) = v.parse::<bool>() {
                return parsed;
            }
        }
    }
    default
}

fn parse_cfg_string_for_index(cfg: &SyncLiteConfig, keys: &[&str], dst_index: usize) -> Option<String> {
    for key in keys {
        let indexed = format!("{key}-{dst_index}");
        if let Some(v) = cfg.extra.get(&indexed) {
            if !v.trim().is_empty() {
                return Some(v.clone());
            }
        }
        if dst_index == 1 {
            if let Some(v) = cfg.extra.get(*key) {
                if !v.trim().is_empty() {
                    return Some(v.clone());
                }
            }
        }
    }
    None
}

fn parse_cfg_bool_for_index(
    cfg: &SyncLiteConfig,
    keys: &[&str],
    dst_index: usize,
    default_value: bool,
) -> bool {
    parse_cfg_string_for_index(cfg, keys, dst_index)
        .map(|v| {
            let n = v.trim().to_ascii_lowercase();
            matches!(n.as_str(), "1" | "true" | "yes" | "on" | "destination")
        })
        .unwrap_or(default_value)
}

fn parse_cfg_u32_for_index(
    cfg: &SyncLiteConfig,
    keys: &[&str],
    dst_index: usize,
    default_value: u32,
) -> u32 {
    parse_cfg_string_for_index(cfg, keys, dst_index)
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default_value)
}

fn parse_cfg_u64_for_index(
    cfg: &SyncLiteConfig,
    keys: &[&str],
    dst_index: usize,
    default_value: u64,
) -> u64 {
    parse_cfg_string_for_index(cfg, keys, dst_index)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default_value)
}

#[allow(dead_code)]
fn parse_cfg_path_for_index(
    cfg: &SyncLiteConfig,
    keys: &[&str],
    dst_index: usize,
    default_value: PathBuf,
) -> PathBuf {
    parse_cfg_string_for_index(cfg, keys, dst_index)
        .map(PathBuf::from)
        .unwrap_or(default_value)
}

fn parse_cfg_destination_indices(cfg: &SyncLiteConfig) -> Vec<usize> {
    let prefixes = [
        "dst-type-",
        "dst-connection-string-",
        "dst-alias-",
        "metadata-store-",
        "dst-metadata-store-",
        "dst-oper-retry-count-",
        "dst-oper-retry-interval-ms-",
        "dst-idempotent-data-ingestion-",
        "dst-insert-batch-size-",
        "dst-update-batch-size-",
        "dst-delete-batch-size-",
    ];

    let mut indices: Vec<usize> = cfg
        .extra
        .keys()
        .filter_map(|k| {
            prefixes.iter().find_map(|prefix| {
                k.strip_prefix(prefix)
                    .and_then(|suffix| suffix.parse::<usize>().ok())
            })
        })
        .collect();

    if indices.is_empty() {
        indices.push(1);
    }
    indices.sort_unstable();
    indices.dedup();
    indices
}

fn parse_cfg_destination_backend_for_index(cfg: &SyncLiteConfig, dst_index: usize) -> DstType {
    if let Some(v) = parse_cfg_string_for_index(cfg, &["dst-type"], dst_index) {
        let normalized = v.trim().to_ascii_uppercase();
        return match normalized.as_str() {
            "DUCKDB" => DstType::DuckDb,
            "POSTGRES" | "POSTGRESQL" | "PG" => DstType::Postgres,
            _ => DstType::Sqlite,
        };
    }
    DstType::Sqlite
}

fn parse_cfg_destination_connection_for_index(
    cfg: &SyncLiteConfig,
    dst_index: usize,
    dst_type: DstType,
    default_db_path: PathBuf,
) -> String {
    let raw_conn = parse_cfg_string_for_index(cfg, &["dst-connection-string"], dst_index);

    match dst_type {
        DstType::Sqlite => parse_sqlite_path_from_connection(raw_conn.as_deref())
            .unwrap_or(default_db_path)
            .to_string_lossy()
            .into_owned(),
        DstType::DuckDb => parse_duckdb_path_from_connection(raw_conn.as_deref())
            .unwrap_or(default_db_path)
            .to_string_lossy()
            .into_owned(),
        DstType::Postgres => raw_conn
            .as_deref()
            .and_then(translate_postgres_connection_string)
            .map(|s| s.to_string())
            .or(raw_conn)
            .unwrap_or_default(),
    }
}

pub(crate) fn parse_sqlite_path_from_connection(conn: Option<&str>) -> Option<PathBuf> {
    let conn = conn?.trim();
    let lower = conn.to_ascii_lowercase();
    if lower.starts_with("jdbc:sqlite:") {
        let body = &conn["jdbc:sqlite:".len()..];
        let path_part = body.split('?').next().unwrap_or(body).trim();
        if !path_part.is_empty() {
            return Some(PathBuf::from(path_part));
        }
    }
    if lower.starts_with("sqlite://") {
        let body = &conn["sqlite://".len()..];
        let path_part = body.split('?').next().unwrap_or(body).trim();
        if !path_part.is_empty() {
            return Some(PathBuf::from(path_part));
        }
    }
    if lower.starts_with("file:") {
        let body = &conn["file:".len()..];
        let path_part = body.split('?').next().unwrap_or(body).trim();
        if !path_part.is_empty() {
            return Some(PathBuf::from(path_part));
        }
    }
    None
}

pub(crate) fn parse_duckdb_path_from_connection(conn: Option<&str>) -> Option<PathBuf> {
    let conn = conn?.trim();
    let lower = conn.to_ascii_lowercase();
    if lower.starts_with("jdbc:duckdb:") {
        let body = &conn["jdbc:duckdb:".len()..];
        let path_part = body.split('?').next().unwrap_or(body).trim();
        if !path_part.is_empty() {
            return Some(PathBuf::from(path_part));
        }
    }
    if lower.starts_with("duckdb:") {
        let body = &conn["duckdb:".len()..];
        let path_part = body.split('?').next().unwrap_or(body).trim();
        if !path_part.is_empty() {
            return Some(PathBuf::from(path_part));
        }
    }
    None
}

pub(crate) fn translate_postgres_connection_string(conn: &str) -> Option<&str> {
    let trimmed = conn.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("jdbc:postgresql://") {
        return Some(&trimmed[5..]);
    }
    Some(trimmed)
}

fn apply_destination_initialize_options(
    cfg: &mut SyncLiteConfig,
    destination: Option<DestinationOptions>,
) -> Result<()> {
    if let Some(destination) = destination {
        let DestinationOptions {
            dst_type,
            dst_connection_string,
            dst_database,
            dst_schema,
            dst_sync_mode,
        } = destination;

        // Java parity: catalog/schema concepts vary per destination engine.
        // SQLite is a single-file engine and has no catalog/schema concepts —
        // reject them so callers do not silently lose state.
        let db = dst_database
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let sch = dst_schema
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        match dst_type {
            DstType::Sqlite => {
                if db.is_some() || sch.is_some() {
                    return Err(Error::Config(
                        "dst-database / dst-schema are not supported for dst-type=SQLITE".to_string(),
                    ));
                }
            }
            DstType::Postgres => {
                if db.is_none() {
                    return Err(Error::Config(
                        "dst-database is required for dst-type=POSTGRES".to_string(),
                    ));
                }
                if sch.is_none() {
                    return Err(Error::Config(
                        "dst-schema is required for dst-type=POSTGRES".to_string(),
                    ));
                }
            }
            DstType::DuckDb => {
                if db.is_none() {
                    return Err(Error::Config(
                        "dst-database is required for dst-type=DUCKDB".to_string(),
                    ));
                }
                // schema optional for DuckDB (defaults to `main`).
            }
        }

        cfg.extra.insert(
            "dst-type".to_string(),
            destination_backend_to_cfg_value(dst_type),
        );
        cfg.extra
            .insert("dst-connection-string".to_string(), dst_connection_string);
        cfg.extra.insert(
            "dst-sync-mode".to_string(),
            destination_sync_mode_to_cfg_value(dst_sync_mode),
        );
        if let Some(d) = db {
            cfg.extra.insert("dst-database".to_string(), d);
        }
        if let Some(s) = sch {
            cfg.extra.insert("dst-schema".to_string(), s);
        }
    }
    Ok(())
}

fn destination_backend_to_cfg_value(dst_type: DstType) -> String {
    match dst_type {
        DstType::Sqlite => "SQLITE".to_string(),
        DstType::DuckDb => "DUCKDB".to_string(),
        DstType::Postgres => "POSTGRES".to_string(),
    }
}

fn destination_sync_mode_to_cfg_value(mode: DstSyncMode) -> String {
    match mode {
        DstSyncMode::Consolidation => "CONSOLIDATION".to_string(),
        DstSyncMode::Replication => "REPLICATION".to_string(),
    }
}

fn parse_cfg_destination_sync_mode(cfg: &SyncLiteConfig) -> DstSyncMode {
    for key in ["dst-sync-mode"] {
        if let Some(v) = cfg.extra.get(key) {
            let normalized = v.trim().to_ascii_uppercase();
            return match normalized.as_str() {
                "REPLICARION" => DstSyncMode::Replication,
                "REPLICATION" => DstSyncMode::Replication,
                _ => DstSyncMode::Consolidation,
            };
        }
    }
    DstSyncMode::Consolidation
}

pub(crate) fn default_config_for_backend(db_path: PathBuf, dst_type: Backend) -> SyncLiteConfig {
    // Prefer the device_name previously persisted in this device's
    // metadata file (written by `Logger::open_with` on first init) so
    // that `Connection::open(db_path)` reuses the existing stage subdir.
    // Fall back to deriving from the db file name when no metadata
    // exists yet.
    let device_name = persisted_device_name(&db_path)
        .unwrap_or_else(|| derive_device_name(&db_path));
    let stage_dir = default_local_stage_dir();

    let mut cfg = SyncLiteConfig::default();
    cfg.device_name = Some(device_name);
    cfg.backend = Some(dst_type);
    cfg.device_type = Some(
        persisted_device_type(&db_path)
            .unwrap_or_else(|| DeviceType::default_for_backend(dst_type)),
    );
    cfg.local_stage_dir = Some(stage_dir);
    cfg.db_path = Some(db_path.clone());
    // Re-hydrate destination keys persisted by a prior
    // `synclite::initialize(..)` so this Logger spawns the same
    // consolidator (postgres/duckdb/sqlite dst).
    hydrate_initialize_config_from_metadata(&db_path, &mut cfg);
    cfg
}

fn persisted_device_type(db_path: &Path) -> Option<DeviceType> {
    let layout = DeviceLayout::new(db_path.to_path_buf());
    if !layout.metadata_path.exists() {
        return None;
    }
    let md = Metadata::open_or_create(&layout.metadata_path).ok()?;
    let s = md.get("device_type").ok().flatten()?;
    s.parse::<DeviceType>().ok()
}

fn persisted_device_name(db_path: &Path) -> Option<String> {
    let layout = DeviceLayout::new(db_path.to_path_buf());
    if !layout.metadata_path.exists() {
        return None;
    }
    let md = Metadata::open_or_create(&layout.metadata_path).ok()?;
    md.get("device_name").ok().flatten()
}

/// Keys that `synclite::initialize(..)` mirrors from `cfg.extra` into
/// the device metadata file so a later `Connection::open(db_path)`
/// reconstructs an identical destination/consolidator config. Mirrors
/// `apply_destination_initialize_options` plus any per-destination
/// suffix variants we need to round-trip.
const PERSISTED_INIT_EXTRA_KEYS: &[&str] = &[
    "dst-type",
    "dst-connection-string",
    "dst-sync-mode",
    "dst-database",
    "dst-schema",
    // Persisted by `reinitialize` to force idempotent ingestion ON
    // for the post-reinit re-seed (tolerates already-populated dst).
    "dst-idempotent-data-ingestion-1",
    // Persisted so the device-side `reinitialize` can locate the
    // consolidator metadata DB (LOCAL vs DESTINATION) and apply the
    // same filter-mapper rules the consolidator used when creating
    // destination tables — required for accurate dst-name resolution.
    "metadata-store-1",
    "dst-metadata-store-1",
    "dst-enable-filter-mapper-rules-1",
    "dst-filter-mapper-rules-file-1",
    "dst-allow-unspecified-tables-1",
    "dst-allow-unspecified-columns-1",
    // Persisted so the device-side `reinitialize` honors the same
    // retry policy the consolidator uses for destination operations.
    "dst-oper-retry-count-1",
    "dst-oper-retry-interval-ms-1",
];

/// Best-effort probe of the configured destination. Offline-first:
/// any failure is logged via `tracing::warn!` and swallowed so the
/// application can keep accepting local writes; the background
/// consolidator will continue to retry on its own schedule. No-op
/// when no destination is configured (pure local logging mode).
fn probe_destination_best_effort(cfg: &SyncLiteConfig) {
    let Some(dst_type_raw) = cfg.extra.get("dst-type") else {
        return;
    };
    let Some(conn_str) = cfg.extra.get("dst-connection-string") else {
        return;
    };
    let dst_type = dst_type_raw.trim().to_ascii_uppercase();
    let result = match dst_type.as_str() {
        "POSTGRES" | "POSTGRESQL" => probe_postgres(conn_str),
        "SQLITE" => probe_sqlite(conn_str),
        "DUCKDB" => probe_duckdb(conn_str),
        // Unknown destination kinds: nothing to probe here.
        _ => return,
    };
    match result {
        Ok(()) => {
            tracing::info!(dst_type = %dst_type, "destination connectivity probe ok");
        }
        Err(e) => {
            tracing::warn!(
                dst_type = %dst_type,
                error = %e,
                "destination connectivity probe failed; continuing in offline-first mode, consolidator will retry"
            );
        }
    }
}

fn probe_postgres(conn_str: &str) -> Result<()> {
    use ::postgres::{Client, NoTls};
    // A short-lived connect + trivial round-trip is enough to surface
    // bad host / auth / database name.
    let mut client = Client::connect(conn_str.trim(), NoTls).map_err(|e| {
        crate::Error::Config(format!("cannot connect to Postgres destination: {e}"))
    })?;
    client.simple_query("SELECT 1").map_err(|e| {
        crate::Error::Config(format!("Postgres destination handshake failed: {e}"))
    })?;
    Ok(())
}

fn probe_sqlite(conn_str: &str) -> Result<()> {
    let path = conn_str.trim();
    ::rusqlite::Connection::open(path)
        .map_err(|e| {
            crate::Error::Config(format!("cannot open SQLite destination at {path}: {e}"))
        })?
        .execute_batch("SELECT 1")
        .map_err(|e| {
            crate::Error::Config(format!("SQLite destination probe failed at {path}: {e}"))
        })?;
    Ok(())
}

fn probe_duckdb(conn_str: &str) -> Result<()> {
    let path = conn_str.trim();
    let conn = ::duckdb::Connection::open(path).map_err(|e| {
        crate::Error::Config(format!("cannot open DuckDB destination at {path}: {e}"))
    })?;
    conn.execute_batch("SELECT 1").map_err(|e| {
        crate::Error::Config(format!("DuckDB destination probe failed at {path}: {e}"))
    })?;
    Ok(())
}

/// Per-`initialize` snapshot persisted into the device metadata file.
/// Read back by `hydrate_initialize_config_from_metadata` when a
/// subsequent `Connection::open(db_path)` builds a new Logger.
fn persist_initialize_config_to_metadata(db_path: &Path, cfg: &SyncLiteConfig) -> Result<()> {    let layout = DeviceLayout::new(db_path.to_path_buf());
    if let Some(parent) = layout.metadata_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let md = Metadata::open_or_create(&layout.metadata_path)?;
    for key in PERSISTED_INIT_EXTRA_KEYS {
        match cfg.extra.get(*key) {
            Some(value) => {
                md.put(key, value)?;
            }
            None => {
                // Leave any pre-existing value alone: a re-init with
                // None destination must not erase a previously
                // configured destination silently.
            }
        }
    }
    Ok(())
}

fn hydrate_initialize_config_from_metadata(db_path: &Path, cfg: &mut SyncLiteConfig) {
    let layout = DeviceLayout::new(db_path.to_path_buf());
    if !layout.metadata_path.exists() {
        return;
    }
    let Ok(md) = Metadata::open_or_create(&layout.metadata_path) else {
        return;
    };
    for key in PERSISTED_INIT_EXTRA_KEYS {
        if cfg.extra.contains_key(*key) {
            continue;
        }
        if let Ok(Some(value)) = md.get(key) {
            cfg.extra.insert((*key).to_string(), value);
        }
    }
}

pub(crate) fn resolve_backend_device_type_db_path(
    cfg: &SyncLiteConfig,
) -> Result<(Backend, DeviceType, PathBuf)> {
    let backend = cfg
        .backend
        .or_else(|| cfg.device_type.map(DeviceType::backend))
        .unwrap_or(Backend::Sqlite);
    let device_type = cfg
        .device_type
        .unwrap_or_else(|| DeviceType::default_for_backend(backend));
    if device_type.backend() != backend {
        return Err(Error::Config(format!(
            "device-type {device_type} does not match db-engine {backend}"
        )));
    }
    let device_name = cfg.device_name.clone().unwrap_or_else(|| "device".into());
    let stage_dir = cfg
        .local_stage_dir
        .clone()
        .unwrap_or_else(default_local_stage_dir);
    let db_path = cfg.db_path.clone().unwrap_or_else(|| {
        let ext = match backend {
            Backend::Sqlite => "db",
            Backend::DuckDb => "duckdb",
        };
        stage_dir.join(format!("{device_name}.{ext}"))
    });
    Ok((backend, device_type, db_path))
}

fn build_shipper(
    cfg: &SyncLiteConfig,
    stage_subdir: PathBuf,
    tracer: Arc<synclite_observability::Tracer>,
    consolidators: Vec<Arc<Consolidator>>,
) -> Result<Option<Arc<LogShipper>>> {
    let mut archivers: Vec<Arc<dyn Archiver>> = Vec::new();

    let destination_specs = parse_destination_specs(&cfg.extra)?;
    if destination_specs.is_empty() {
        return Ok(None);
    }
    for spec in destination_specs {
        match spec.kind {
            DestinationKind::Fs => {
                // Java parity: FS shipper publishes into `device-upload-root`.
                // When unset, fall back to `local-data-stage-directory`
                // (Java's behavior for an empty FS destination).
                let target_str = cfg
                    .extra
                    .get(&key_for(DEVICE_UPLOAD_ROOT_KEY, &spec.suffix))
                    .or_else(|| cfg.extra.get(DEVICE_UPLOAD_ROOT_KEY))
                    .cloned();
                let target = match target_str {
                    Some(s) => PathBuf::from(s),
                    None => cfg
                        .local_stage_dir
                        .clone()
                        .ok_or_else(|| {
                            Error::Config(format!(
                                "{}=FS requires `{DEVICE_UPLOAD_ROOT_KEY}` or `local-data-stage-directory`",
                                spec.key_name
                            ))
                        })?,
                };
                std::fs::create_dir_all(&target).map_err(|e| {
                    Error::Config(format!(
                        "{DEVICE_UPLOAD_ROOT_KEY}={}: {e}",
                        target.display()
                    ))
                })?;
                archivers.push(Arc::new(FsArchiver::new(&target)));
            }
            DestinationKind::S3 => {
                #[cfg(feature = "s3")]
                {
                    let arch = build_s3_archiver_for_suffix(cfg, &spec.suffix, "s3")?
                        .ok_or_else(|| {
                            Error::Config(format!(
                                "{}=S3 but S3 destination settings are incomplete",
                                spec.key_name
                            ))
                        })?;
                    archivers.push(arch);
                }
                #[cfg(not(feature = "s3"))]
                {
                    return Err(Error::Config(format!(
                        "{}=S3 requires enabling the s3 feature",
                        spec.key_name
                    )));
                }
            }
            DestinationKind::Minio => {
                #[cfg(feature = "s3")]
                {
                    let arch = build_s3_archiver_for_suffix(cfg, &spec.suffix, "minio")?
                        .ok_or_else(|| {
                            Error::Config(format!(
                                "{}=MINIO but MinIO destination settings are incomplete",
                                spec.key_name
                            ))
                        })?;
                    archivers.push(arch);
                }
                #[cfg(not(feature = "s3"))]
                {
                    return Err(Error::Config(format!(
                        "{}=MINIO requires enabling the s3 feature",
                        spec.key_name
                    )));
                }
            }
            DestinationKind::Sftp => {
                #[cfg(feature = "sftp")]
                {
                    let arch = build_sftp_archiver_for_suffix(cfg, &spec.suffix)?
                        .ok_or_else(|| {
                            Error::Config(format!(
                                "{}=SFTP but SFTP destination settings are incomplete",
                                spec.key_name
                            ))
                        })?;
                    archivers.push(arch);
                }
                #[cfg(not(feature = "sftp"))]
                {
                    return Err(Error::Config(format!(
                        "{}=SFTP requires enabling the sftp feature",
                        spec.key_name
                    )));
                }
            }
        }
    }

    if archivers.is_empty() {
        return Ok(None);
    }

    let shipping_frequency_ms = cfg
        .log_segment_shipping_frequency_ms
        .or_else(|| {
            cfg.extra
                .get(LOG_SEGMENT_SHIPPING_FREQUENCY_MS_KEY)
                .and_then(|v| v.parse::<u64>().ok())
        })
        .unwrap_or(DEFAULT_LOG_SEGMENT_SHIPPING_FREQUENCY_MS);
    if shipping_frequency_ms == 0 {
        return Err(Error::Config(
            "log-segment-shipping-frequency-ms must be > 0".into(),
        ));
    }

    let mut scfg = ShipperConfig::new(archivers);
    // Java parity: wrap the LogCleaner callback so we emit `Log segment shipped`
    // INFO (mirrors Java `LogShipper.ship` / `LogCleaner` success traces)
    // before delegating to the cleaner that removes the staged file.
    //
    // When an embedded consolidator is also wired up for this device,
    // hand the segment off to the consolidator from this hook instead
    // of running LogCleaner — the consolidator owns post-apply stage
    // cleanup, and notifying it only *after* the shipper has reached
    // every archiver guarantees a segment is replicated to all
    // configured stages before the consolidator can delete it.
    let cleaner_cb = LogCleaner::new().as_callback();
    let trace_cb_tracer = tracer.clone();
    scfg.on_shipped = Some(Arc::new(move |p: &Path| {
        synclite_observability::tracer_info!(
            trace_cb_tracer,
            "LogShipper",
            "Log segment shipped : path={}",
            p.display()
        );
        // Sidecar txn files (`<seq>.sqllog.<idx>.txn`) are
        // shipper-only — the consolidator consumes them via the
        // owning main segment, matching mover::make_callback's
        // `notify_consolidator_for_txn_files=false` default.
        let is_main_segment = p
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with(".sqllog"))
            .unwrap_or(false);
        if is_main_segment && !consolidators.is_empty() {
            for c in &consolidators {
                if let Err(e) = c.notify_stage_path(p.to_path_buf()) {
                    tracing::error!(
                        segment = %p.display(),
                        error = %e,
                        "failed to notify consolidator after shipping"
                    );
                }
            }
            return;
        }
        cleaner_cb(p);
        synclite_observability::tracer_info!(
            trace_cb_tracer,
            "LogCleaner",
            "Log segment cleaned : path={}",
            p.display()
        );
    }));
    scfg.stage_dir = Some(stage_subdir);
    scfg.scan_interval = std::time::Duration::from_millis(shipping_frequency_ms);
    Ok(Some(Arc::new(LogShipper::spawn_with(scfg)?)))
}

/// Build an S3 archiver from either `s3:`/`s3-N:` keys (`scheme="s3"`) or
/// `minio:`/`minio-N:` keys (`scheme="minio"`). Same Java-compatible
/// spelling on both sides; MinIO is a separate transport but uses the
/// S3-compatible HTTP API under the hood.
#[cfg(feature = "s3")]
fn build_s3_archiver_for_suffix(
    cfg: &SyncLiteConfig,
    suffix: &str,
    scheme: &str,
) -> Result<Option<Arc<dyn Archiver>>> {
    use logger_archiver::{S3Archiver, S3Config, StaticCredentials};
    let k_bucket = format!("{scheme}{suffix}:data-stage-bucket-name");
    let k_endpoint = format!("{scheme}{suffix}:endpoint");
    let k_access = format!("{scheme}{suffix}:access-key");
    let k_secret = format!("{scheme}{suffix}:secret-key");

    let Some(bucket) = cfg.extra.get(&k_bucket) else {
        return Ok(None);
    };
    let mut s3 = S3Config::new(bucket.to_string());
    if let Some(e) = cfg.extra.get(&k_endpoint) {
        s3 = s3.with_endpoint_url(e.clone());
        // MinIO almost always needs path-style.
        if scheme == "minio" {
            s3 = s3.with_path_style(true);
        }
    }
    if let (Some(ak), Some(sk)) = (cfg.extra.get(&k_access), cfg.extra.get(&k_secret)) {
        s3 = s3.with_credentials(StaticCredentials {
            access_key_id: ak.clone(),
            secret_access_key: sk.clone(),
            session_token: None,
        });
    }
    Ok(Some(Arc::new(S3Archiver::new(s3)?)))
}

#[cfg(feature = "sftp")]
fn build_sftp_archiver_for_suffix(
    cfg: &SyncLiteConfig,
    suffix: &str,
) -> Result<Option<Arc<dyn Archiver>>> {
    use logger_archiver::{SftpArchiver, SftpAuth, SftpConfig};
    let k_host = format!("sftp{suffix}:host");
    let k_port = format!("sftp{suffix}:port");
    let k_user = format!("sftp{suffix}:user-name");
    let k_password = format!("sftp{suffix}:password");
    let k_remote_dir = format!("sftp{suffix}:remote-data-stage-directory");

    let Some(host) = cfg.extra.get(&k_host) else {
        return Ok(None);
    };
    let username = cfg.extra.get(&k_user).ok_or_else(|| {
        Error::Config(format!("{k_user} is required when {k_host} is set"))
    })?;
    let remote_dir = cfg.extra.get(&k_remote_dir).ok_or_else(|| {
        Error::Config(format!("{k_remote_dir} is required when {k_host} is set"))
    })?;
    let password = cfg.extra.get(&k_password).ok_or_else(|| {
        Error::Config(format!("{k_password} is required when {k_host} is set"))
    })?;
    let auth = SftpAuth::Password { password: password.clone() };
    let mut s = SftpConfig::new(host.clone(), username.clone(), auth, remote_dir.clone());
    if let Some(p) = cfg.extra.get(&k_port) {
        let port: u16 = p.parse().map_err(|e| {
            Error::Config(format!("{k_port}={p}: {e}"))
        })?;
        s = s.with_port(port);
    }
    Ok(Some(Arc::new(SftpArchiver::new(s)?)))
}

#[cfg(test)]
mod tests {
    use super::{validate_device_name, MAX_DEVICE_NAME_LEN};

    #[test]
    fn device_name_validation_accepts_java_compatible_names() {
        assert!(validate_device_name("A1Z9").is_ok());
        assert!(validate_device_name("").is_ok());
        assert!(validate_device_name(&"a".repeat(MAX_DEVICE_NAME_LEN)).is_ok());
    }

    #[test]
    fn device_name_validation_rejects_invalid_names() {
        assert!(validate_device_name("bad-name").is_err());
        assert!(validate_device_name(&"a".repeat(MAX_DEVICE_NAME_LEN + 1)).is_err());
    }
}






