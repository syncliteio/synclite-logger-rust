//! Top-level facade. Reads `synclite_logger.conf` and assembles a working
//! [`Logger`] with the right backend, shipper, and archivers wired up.
//!
//! ```no_run
//! use synclite::Logger;
//! let mut logger = Logger::open("synclite_logger.conf").unwrap();
//! logger.execute("CREATE TABLE t(x INTEGER)", &[]).unwrap();
//! logger.close().unwrap();
//! ```
//!
//! Recognized config keys (in addition to those parsed by
//! [`synclite_config::SyncLiteConfig`]):
//!
//! - `db-engine`: `SQLITE` (default) or `DUCKDB`.
//! - `device-name`: used as the user DB filename stem. Defaults to `device`.
//! - `local-data-stage-directory`: where log segments are written. Defaults
//!   to `<cwd>/synclite-stage`.
//! - `destination-fs-target-dir` *(extra key)*: enables the FS archiver +
//!   background shipper + cleaner. If absent, the logger runs without
//!   shipping (segments accumulate locally).
//!
//! When the `s3` feature is enabled the following keys also light up an
//! S3 archiver alongside (or instead of) the FS archiver:
//!
//! - `destination-s3-bucket` — required to enable S3.
//! - `destination-s3-key-prefix` — optional key/folder prefix.
//! - `destination-s3-region` — AWS region (defaults to `us-east-1`).
//! - `destination-s3-endpoint` — custom endpoint URL (MinIO, R2, ...).
//! - `destination-s3-path-style` — `true` to force path-style addressing.
//! - `destination-s3-access-key-id` / `destination-s3-secret-access-key`
//!   / `destination-s3-session-token` — static credentials override.
//!
//! When the `sftp` feature is enabled the following keys light up an
//! SFTP archiver:
//!
//! - `destination-sftp-host` — required to enable SFTP.
//! - `destination-sftp-port` — TCP port (default 22).
//! - `destination-sftp-username` — required.
//! - `destination-sftp-password` — password auth.
//! - `destination-sftp-private-key-path` — OpenSSH private key file.
//! - `destination-sftp-private-key-passphrase` — optional passphrase.
//! - `destination-sftp-remote-dir` — target directory on the server.
//! - `destination-sftp-accept-any-host-key` — `true`/`false`
//!   (default `true`; disables MITM protection).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use synclite_archiver::{Archiver, FsArchiver};
use synclite_config::SyncLiteConfig;
use synclite_core::{Backend, DeviceType, Error, Result};
use synclite_db_duckdb::{
    DuckDbDevice, DuckDbDeviceConfig,
};
use synclite_db_sqlite::{
    SqliteDevice, SqliteDeviceConfig,
};
use synclite_db_traits::{DbDevice, Row, Value};
use synclite_shipper::{LogCleaner, LogShipper, ShipperConfig};

mod backup;
mod app_lock;
mod layout;
mod metadata;
mod mover;
mod sql_split;

use app_lock::AppLock;
use layout::{ArchiveLayout, DeviceLayout};
use metadata::Metadata;

/// SyncLite-wrapped `duckdb`-style connection and statement APIs.
pub mod duckdb;
/// SyncLite-wrapped `rusqlite`-style connection and statement APIs.
pub mod rusqlite;

/// Extra-key name for the FS archiver target directory.
pub const FS_TARGET_DIR_KEY: &str = "destination-fs-target-dir";
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

/// Extra-key names for the S3 archiver.
pub mod s3_keys {
    /// Required: target bucket.
    pub const BUCKET: &str = "destination-s3-bucket";
    /// Optional: key prefix (folder).
    pub const KEY_PREFIX: &str = "destination-s3-key-prefix";
    /// Optional: AWS region.
    pub const REGION: &str = "destination-s3-region";
    /// Optional: custom endpoint URL.
    pub const ENDPOINT: &str = "destination-s3-endpoint";
    /// Optional: `true` to force path-style addressing.
    pub const PATH_STYLE: &str = "destination-s3-path-style";
    /// Optional: static access key id.
    pub const ACCESS_KEY_ID: &str = "destination-s3-access-key-id";
    /// Optional: static secret access key.
    pub const SECRET_ACCESS_KEY: &str = "destination-s3-secret-access-key";
    /// Optional: static session token.
    pub const SESSION_TOKEN: &str = "destination-s3-session-token";
}

/// Extra-key names for the SFTP archiver.
pub mod sftp_keys {
    /// Required: server hostname.
    pub const HOST: &str = "destination-sftp-host";
    /// Optional: TCP port (default 22).
    pub const PORT: &str = "destination-sftp-port";
    /// Required: SSH username.
    pub const USERNAME: &str = "destination-sftp-username";
    /// Optional: password auth.
    pub const PASSWORD: &str = "destination-sftp-password";
    /// Optional: OpenSSH private-key file path.
    pub const PRIVATE_KEY_PATH: &str = "destination-sftp-private-key-path";
    /// Optional: passphrase decrypting the private key.
    pub const PRIVATE_KEY_PASSPHRASE: &str = "destination-sftp-private-key-passphrase";
    /// Required: target directory on the server.
    pub const REMOTE_DIR: &str = "destination-sftp-remote-dir";
    /// Optional: `true`/`false`; default `true`.
    pub const ACCEPT_ANY_HOST_KEY: &str = "destination-sftp-accept-any-host-key";
}

/// Top-level logger. Wraps a [`DbDevice`] and (optionally) a background
/// [`LogShipper`].
pub struct Logger {
    device: Box<dyn DbDevice>,
    /// Held to keep the worker alive for the logger's lifetime. Dropped
    /// when the logger drops, which drains and stops the worker.
    _shipper: Option<Arc<LogShipper>>,
    /// Held to keep the app lock alive for the logger's lifetime.
    _app_lock: Option<AppLock>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DestinationKind {
    Fs,
    S3,
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
            .finish()
    }
}

impl Logger {
    /// Initialize a device using defaults for the selected device type.
    ///
    /// Mirrors Java `SyncLite.initialize(deviceType, dbPath)` behavior:
    /// idempotent within the current process for the same database path.
    pub fn initialize<P: AsRef<Path>>(device_type: DeviceType, db_path: P) -> Result<()> {
        let cfg = SyncLiteConfig::default();
        Self::initialize_with_config(device_type, db_path, cfg)
    }

    /// Initialize a device using defaults and an explicit device name.
    pub fn initialize_with_device_name<P: AsRef<Path>>(
        device_type: DeviceType,
        db_path: P,
        device_name: impl Into<String>,
    ) -> Result<()> {
        let mut cfg = SyncLiteConfig::default();
        cfg.device_name = Some(device_name.into());
        Self::initialize_with_config(device_type, db_path, cfg)
    }

    /// Initialize a device using a fully populated config object.
    ///
    /// The supplied config is copied and normalized with the provided
    /// `device_type` and `db_path` before initialization.
    pub fn initialize_with_config<P: AsRef<Path>>(
        device_type: DeviceType,
        db_path: P,
        mut cfg: SyncLiteConfig,
    ) -> Result<()> {
        if let Some(ref device_name) = cfg.device_name {
            validate_device_name(device_name)?;
        }

        let normalized_db_path = normalize_db_path(db_path.as_ref())?;
        let backend = device_type.backend();
        cfg.backend = Some(backend);
        cfg.device_type = Some(device_type);
        cfg.db_path = Some(normalized_db_path.clone());
        if cfg.device_name.is_none() {
            let default_name = normalized_db_path
                .file_stem()
                .and_then(|s| s.to_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("device")
                .to_string();
            cfg.device_name = Some(default_name);
        }
        if cfg.local_stage_dir.is_none() {
            let stage_dir = normalized_db_path
                .parent()
                .map(|p| p.join("synclite-stage"))
                .unwrap_or_else(|| PathBuf::from("synclite-stage"));
            cfg.local_stage_dir = Some(stage_dir);
        }

        let registry = INITIALIZED_DEVICES.get_or_init(|| Mutex::new(HashSet::new()));
        let mut guard = registry
            .lock()
            .map_err(|_| Error::Config("initialize registry mutex poisoned".to_string()))?;
        if guard.contains(&normalized_db_path) {
            return Ok(());
        }

        let logger = Self::open_with(cfg)?;
        logger.close()?;
        guard.insert(normalized_db_path);
        Ok(())
    }

    /// Initialize using config loaded from `synclite_logger.conf` path.
    pub fn initialize_with_config_path<P: AsRef<Path>, Q: AsRef<Path>>(
        device_type: DeviceType,
        db_path: P,
        conf_path: Q,
    ) -> Result<()> {
        let cfg = SyncLiteConfig::load(conf_path)?;
        Self::initialize_with_config(device_type, db_path, cfg)
    }

    /// Initialize using config-path plus explicit device name override.
    pub fn initialize_with_config_path_and_device_name<P: AsRef<Path>, Q: AsRef<Path>>(
        device_type: DeviceType,
        db_path: P,
        conf_path: Q,
        device_name: impl Into<String>,
    ) -> Result<()> {
        let mut cfg = SyncLiteConfig::load(conf_path)?;
        cfg.device_name = Some(device_name.into());
        Self::initialize_with_config(device_type, db_path, cfg)
    }

    /// Initialize using config object plus explicit device name override.
    pub fn initialize_with_config_and_device_name<P: AsRef<Path>>(
        device_type: DeviceType,
        db_path: P,
        mut cfg: SyncLiteConfig,
        device_name: impl Into<String>,
    ) -> Result<()> {
        cfg.device_name = Some(device_name.into());
        Self::initialize_with_config(device_type, db_path, cfg)
    }

    /// Read a config file from disk and build a fully wired logger.
    pub fn open<P: AsRef<Path>>(conf_path: P) -> Result<Self> {
        let cfg = SyncLiteConfig::load(conf_path)?;
        Self::open_with(cfg)
    }

    /// Build a fully wired logger from an already-parsed config.
    pub fn open_with(cfg: SyncLiteConfig) -> Result<Self> {
        let (backend, device_type, db_path) = resolve_backend_device_type_db_path(&cfg)?;
        let device_name = cfg.device_name.clone().unwrap_or_else(|| "device".into());
        validate_device_name(&device_name)?;
        let stage_dir = cfg
            .local_stage_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from("synclite-stage"));
        std::fs::create_dir_all(&stage_dir)?;
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // SyncLite layout.
        let layout = DeviceLayout::new(db_path.clone());
        std::fs::create_dir_all(&layout.device_home)?;

        let app_lock = AppLock::try_lock(&db_path)?;

        let md = Metadata::open_or_create(&layout.metadata_path)?;
        let uuid = match md.get("uuid")? {
            Some(v) => v,
            None => {
                let new_uuid = uuid::Uuid::new_v4().to_string();
                md.put("uuid", &new_uuid)?;
                new_uuid
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

        // Shipper (FS / S3 / SFTP if configured).
        let shipper = build_shipper(&cfg, archive.stage_subdir.clone())?;

        // The move callback: device home → stage subdir → shipper.
        let cb = mover::make_callback(
            archive.stage_subdir.clone(),
            layout.metadata_path.clone(),
            shipper.clone(),
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

        Ok(Self {
            device,
            _shipper: shipper,
            _app_lock: Some(app_lock),
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
            // Java parity for prepared batching: capture commandlog rows at
            // batch-build time (equivalent boundary in Rust API) before
            // executeBatch() performs DB work.
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

    /// Roll back the active logical transaction.
    pub fn rollback(&mut self) -> Result<()> {
        self.device.rollback()
    }

    /// Close the logger, finalizing the current segment.
    pub fn close(self) -> Result<()> {
        let Logger {
            device,
            _shipper,
            _app_lock,
        } = self;
        device.close()?;
        // Drop the shipper after close so the final segment has a chance
        // to be enqueued.
        drop(_shipper);
        drop(_app_lock);
        Ok(())
    }
}

fn validate_device_name(device_name: &str) -> Result<()> {
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

fn parse_destination_specs(extra: &HashMap<String, String>) -> Result<Vec<DestinationSpec>> {
    let mut specs = Vec::new();

    if let Some(value) = extra.get("destination-type") {
        specs.push(DestinationSpec {
            kind: parse_destination_kind(value, "destination-type")?,
            suffix: String::new(),
            key_name: "destination-type".to_string(),
        });
        return Ok(specs);
    }

    let mut idx = 1usize;
    loop {
        let key = format!("destination-type-{idx}");
        let Some(value) = extra.get(&key) else {
            break;
        };
        specs.push(DestinationSpec {
            kind: parse_destination_kind(value, &key)?,
            suffix: format!("-{idx}"),
            key_name: key,
        });
        idx += 1;
    }

    Ok(specs)
}

fn parse_destination_kind(value: &str, key_name: &str) -> Result<DestinationKind> {
    match value.trim().to_ascii_uppercase().as_str() {
        "FS" => Ok(DestinationKind::Fs),
        "S3" => Ok(DestinationKind::S3),
        "SFTP" => Ok(DestinationKind::Sftp),
        other => Err(Error::Config(format!(
            "unsupported {key_name}: {other} (supported: FS, S3, SFTP)"
        ))),
    }
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

pub(crate) fn default_config_for_backend(db_path: PathBuf, backend: Backend) -> SyncLiteConfig {
    let device_name = db_path
        .file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("device")
        .to_string();
    let stage_dir = db_path
        .parent()
        .map(|p| p.join("synclite-stage"))
        .unwrap_or_else(|| PathBuf::from("synclite-stage"));

    let mut cfg = SyncLiteConfig::default();
    cfg.device_name = Some(device_name);
    cfg.backend = Some(backend);
    cfg.device_type = Some(DeviceType::default_for_backend(backend));
    cfg.local_stage_dir = Some(stage_dir);
    cfg.db_path = Some(db_path);
    cfg
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
        .unwrap_or_else(|| PathBuf::from("synclite-stage"));
    let db_path = cfg.db_path.clone().unwrap_or_else(|| {
        let ext = match backend {
            Backend::Sqlite => "db",
            Backend::DuckDb => "duckdb",
        };
        stage_dir.join(format!("{device_name}.{ext}"))
    });
    Ok((backend, device_type, db_path))
}

fn build_shipper(cfg: &SyncLiteConfig, stage_subdir: PathBuf) -> Result<Option<Arc<LogShipper>>> {
    let mut archivers: Vec<Arc<dyn Archiver>> = Vec::new();

    let destination_specs = parse_destination_specs(&cfg.extra)?;
    if !destination_specs.is_empty() {
        for spec in destination_specs {
            match spec.kind {
                DestinationKind::Fs => {
                    let indexed_stage_key = key_for("local-data-stage-directory", &spec.suffix);
                    let target = cfg
                        .extra
                        .get(&indexed_stage_key)
                        .or_else(|| cfg.extra.get(FS_TARGET_DIR_KEY))
                        .ok_or_else(|| {
                            Error::Config(format!(
                                "{} requires {} or {}",
                                spec.key_name, indexed_stage_key, FS_TARGET_DIR_KEY
                            ))
                        })?;
                    let target = PathBuf::from(target);
                    std::fs::create_dir_all(&target).map_err(|e| {
                        Error::Config(format!(
                            "{}={}: {e}",
                            indexed_stage_key,
                            target.display()
                        ))
                    })?;
                    archivers.push(Arc::new(FsArchiver::new(&target)));
                }
                DestinationKind::S3 => {
                    #[cfg(feature = "s3")]
                    {
                        if let Some(arch) = build_s3_archiver_for_suffix(cfg, &spec.suffix)? {
                            archivers.push(arch);
                        } else {
                            return Err(Error::Config(format!(
                                "{} is S3 but S3 destination settings are incomplete",
                                spec.key_name
                            )));
                        }
                    }
                    #[cfg(not(feature = "s3"))]
                    {
                        return Err(Error::Config(format!(
                            "{}=S3 requires enabling the s3 feature",
                            spec.key_name
                        )));
                    }
                }
                DestinationKind::Sftp => {
                    #[cfg(feature = "sftp")]
                    {
                        if let Some(arch) = build_sftp_archiver_for_suffix(cfg, &spec.suffix)? {
                            archivers.push(arch);
                        } else {
                            return Err(Error::Config(format!(
                                "{} is SFTP but SFTP destination settings are incomplete",
                                spec.key_name
                            )));
                        }
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
    } else {
        if let Some(target) = cfg.extra.get(FS_TARGET_DIR_KEY) {
            let target = PathBuf::from(target);
            std::fs::create_dir_all(&target).map_err(|e| {
                Error::Config(format!(
                    "{FS_TARGET_DIR_KEY}={}: {e}",
                    target.display()
                ))
            })?;
            archivers.push(Arc::new(FsArchiver::new(&target)));
        }

        #[cfg(feature = "s3")]
        if let Some(arch) = build_s3_archiver(cfg)? {
            archivers.push(arch);
        }

        #[cfg(feature = "sftp")]
        if let Some(arch) = build_sftp_archiver(cfg)? {
            archivers.push(arch);
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
    scfg.on_shipped = Some(LogCleaner::new().as_callback());
    scfg.stage_dir = Some(stage_subdir);
    scfg.scan_interval = std::time::Duration::from_millis(shipping_frequency_ms);
    Ok(Some(Arc::new(LogShipper::spawn_with(scfg)?)))
}

#[cfg(feature = "s3")]
fn build_s3_archiver(cfg: &SyncLiteConfig) -> Result<Option<Arc<dyn Archiver>>> {
    build_s3_archiver_for_suffix(cfg, "")
}

#[cfg(feature = "s3")]
fn build_s3_archiver_for_suffix(
    cfg: &SyncLiteConfig,
    suffix: &str,
) -> Result<Option<Arc<dyn Archiver>>> {
    use synclite_archiver::{S3Archiver, S3Config, StaticCredentials};
    let indexed_bucket = format!("s3{suffix}:data-stage-bucket-name");
    let indexed_endpoint = format!("s3{suffix}:endpoint");
    let indexed_access_key = format!("s3{suffix}:access-key");
    let indexed_secret_key = format!("s3{suffix}:secret-key");

    let Some(bucket) = cfg
        .extra
        .get(&indexed_bucket)
        .or_else(|| cfg.extra.get(s3_keys::BUCKET))
    else {
        return Ok(None);
    };
    let mut s3 = S3Config::new(bucket.to_string());
    if let Some(p) = cfg
        .extra
        .get(&key_for(s3_keys::KEY_PREFIX, suffix))
        .or_else(|| cfg.extra.get(s3_keys::KEY_PREFIX))
    {
        s3 = s3.with_key_prefix(p.clone());
    }
    if let Some(r) = cfg
        .extra
        .get(&key_for(s3_keys::REGION, suffix))
        .or_else(|| cfg.extra.get(s3_keys::REGION))
    {
        s3 = s3.with_region(r.clone());
    }
    if let Some(e) = cfg
        .extra
        .get(&indexed_endpoint)
        .or_else(|| cfg.extra.get(&key_for(s3_keys::ENDPOINT, suffix)))
        .or_else(|| cfg.extra.get(s3_keys::ENDPOINT))
    {
        s3 = s3.with_endpoint_url(e.clone());
    }
    if let Some(v) = cfg
        .extra
        .get(&key_for(s3_keys::PATH_STYLE, suffix))
        .or_else(|| cfg.extra.get(s3_keys::PATH_STYLE))
    {
        let on = matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes");
        s3 = s3.with_path_style(on);
    }
    if let (Some(ak), Some(sk)) = (
        cfg.extra
            .get(&indexed_access_key)
            .or_else(|| cfg.extra.get(&key_for(s3_keys::ACCESS_KEY_ID, suffix)))
            .or_else(|| cfg.extra.get(s3_keys::ACCESS_KEY_ID)),
        cfg.extra
            .get(&indexed_secret_key)
            .or_else(|| cfg.extra.get(&key_for(s3_keys::SECRET_ACCESS_KEY, suffix)))
            .or_else(|| cfg.extra.get(s3_keys::SECRET_ACCESS_KEY)),
    ) {
        s3 = s3.with_credentials(StaticCredentials {
            access_key_id: ak.clone(),
            secret_access_key: sk.clone(),
            session_token: cfg
                .extra
                .get(&key_for(s3_keys::SESSION_TOKEN, suffix))
                .or_else(|| cfg.extra.get(s3_keys::SESSION_TOKEN))
                .cloned(),
        });
    }
    Ok(Some(Arc::new(S3Archiver::new(s3)?)))
}

#[cfg(feature = "sftp")]
fn build_sftp_archiver(cfg: &SyncLiteConfig) -> Result<Option<Arc<dyn Archiver>>> {
    build_sftp_archiver_for_suffix(cfg, "")
}

#[cfg(feature = "sftp")]
fn build_sftp_archiver_for_suffix(
    cfg: &SyncLiteConfig,
    suffix: &str,
) -> Result<Option<Arc<dyn Archiver>>> {
    use synclite_archiver::{SftpArchiver, SftpAuth, SftpConfig};
    let indexed_host = format!("sftp{suffix}:host");
    let indexed_port = format!("sftp{suffix}:port");
    let indexed_user = format!("sftp{suffix}:user-name");
    let indexed_password = format!("sftp{suffix}:password");
    let indexed_remote_dir = format!("sftp{suffix}:remote-data-stage-directory");

    let Some(host) = cfg
        .extra
        .get(&indexed_host)
        .or_else(|| cfg.extra.get(&key_for(sftp_keys::HOST, suffix)))
        .or_else(|| cfg.extra.get(sftp_keys::HOST))
    else {
        return Ok(None);
    };
    let username = cfg
        .extra
        .get(&indexed_user)
        .or_else(|| cfg.extra.get(&key_for(sftp_keys::USERNAME, suffix)))
        .or_else(|| cfg.extra.get(sftp_keys::USERNAME))
        .ok_or_else(|| {
        Error::Config(format!(
            "{} is required when {} is set",
            sftp_keys::USERNAME,
            sftp_keys::HOST
        ))
    })?;
    let remote_dir = cfg
        .extra
        .get(&indexed_remote_dir)
        .or_else(|| cfg.extra.get(&key_for(sftp_keys::REMOTE_DIR, suffix)))
        .or_else(|| cfg.extra.get(sftp_keys::REMOTE_DIR))
        .ok_or_else(|| {
        Error::Config(format!(
            "{} is required when {} is set",
            sftp_keys::REMOTE_DIR,
            sftp_keys::HOST
        ))
    })?;
    let auth = if let Some(pw) = cfg
        .extra
        .get(&indexed_password)
        .or_else(|| cfg.extra.get(&key_for(sftp_keys::PASSWORD, suffix)))
        .or_else(|| cfg.extra.get(sftp_keys::PASSWORD))
    {
        SftpAuth::Password { password: pw.clone() }
    } else if let Some(kp) = cfg
        .extra
        .get(&key_for(sftp_keys::PRIVATE_KEY_PATH, suffix))
        .or_else(|| cfg.extra.get(sftp_keys::PRIVATE_KEY_PATH))
    {
        SftpAuth::PrivateKeyFile {
            path: PathBuf::from(kp),
            passphrase: cfg
                .extra
                .get(&key_for(sftp_keys::PRIVATE_KEY_PASSPHRASE, suffix))
                .or_else(|| cfg.extra.get(sftp_keys::PRIVATE_KEY_PASSPHRASE))
                .cloned(),
        }
    } else {
        return Err(Error::Config(format!(
            "sftp: either {} or {} must be set",
            sftp_keys::PASSWORD,
            sftp_keys::PRIVATE_KEY_PATH
        )));
    };
    let mut s = SftpConfig::new(host.clone(), username.clone(), auth, remote_dir.clone());
    if let Some(p) = cfg
        .extra
        .get(&indexed_port)
        .or_else(|| cfg.extra.get(&key_for(sftp_keys::PORT, suffix)))
        .or_else(|| cfg.extra.get(sftp_keys::PORT))
    {
        let port: u16 = p.parse().map_err(|e| {
            Error::Config(format!("{}={p}: {e}", sftp_keys::PORT))
        })?;
        s = s.with_port(port);
    }
    if let Some(v) = cfg
        .extra
        .get(&key_for(sftp_keys::ACCEPT_ANY_HOST_KEY, suffix))
        .or_else(|| cfg.extra.get(sftp_keys::ACCEPT_ANY_HOST_KEY))
    {
        let on = matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes");
        s = s.with_accept_any_host_key(on);
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
