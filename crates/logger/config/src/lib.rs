//! Parser for SyncLite properties config files.
//!
//! The Java logger accepts a Java-properties-style file with keys such as
//! `device-name`, `local-data-stage-directory`, `device-stage-type`, etc. We
//! parse the same format here so existing configs work unchanged.
//! This loader accepts logger, consolidator, or merged config file names
//! and normalizes common consolidator key aliases into logger keys.
//!
//! Only a small, growing subset of keys is recognized today – the rest are
//! preserved in [`SyncLiteConfig::extra`] for forward compatibility.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use logger_core::{Backend, DeviceType, Error, Result};

/// Parsed contents of a `synclite.conf` file (legacy `synclite_logger.conf`
/// also accepted).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncLiteConfig {
    /// Logical device name (free-form).
    pub device_name: Option<String>,

    /// Embedded database backend to use. Defaults to [`Backend::Sqlite`] when
    /// absent.
    pub backend: Option<Backend>,

    /// Java-compatible device type / mode.
    pub device_type: Option<DeviceType>,

    /// Directory that holds log segments before they are shipped.
    pub local_stage_dir: Option<PathBuf>,

    /// Path to the user-facing database file. When absent, the logger
    /// derives a path of the form `<local_stage_dir>/<device_name>.db`
    /// (or `.duckdb`) for backward compatibility.
    pub db_path: Option<PathBuf>,

    /// Maximum log records per segment before rolling over.
    pub log_segment_flush_batch_size: Option<u64>,

    /// Maximum number of log records to keep before forcing a segment switch.
    pub log_queue_size: Option<u64>,

    /// Segment-switch threshold in record count.
    pub log_segment_switch_log_count_threshold: Option<u64>,

    /// Segment-switch threshold in milliseconds.
    pub log_segment_switch_duration_threshold_ms: Option<u64>,

    /// Frequency in milliseconds at which the shipper scans and ships
    /// pending finalized segments.
    pub log_segment_shipping_frequency_ms: Option<u64>,

    /// SQLite page size used for generated `.sqllog` segment files.
    /// Mirrors Java's `log-segment-page-size` option.
    pub log_segment_page_size: Option<u32>,

    /// Maximum number of inlined log arguments before widening the log segment.
    pub max_inlined_log_args: Option<u32>,

    /// Use a pre-created data backup instead of building one on init.
    pub use_precreated_data_backup: Option<bool>,

    /// Vacuum the generated data backup before shipping.
    pub vacuum_data_backup: Option<bool>,

    /// Skip restart recovery on open.
    pub skip_restart_recovery: Option<bool>,

    /// Disable async logging for transactional devices.
    pub disable_async_logging_for_txn_device: Option<bool>,

    /// Enable async logging for appender devices.
    pub enable_async_logging_for_appender_device: Option<bool>,

    /// Optional encryption key file.
    pub encryption_key_file: Option<PathBuf>,

    /// Tables to include during backup processing.
    pub include_tables: Option<Vec<String>>,

    /// Tables to exclude during backup processing.
    pub exclude_tables: Option<Vec<String>>,

    /// Enable the command handler pipeline.
    pub enable_command_handler: Option<bool>,

    /// Command handler type name.
    pub command_handler_type: Option<String>,

    /// External command handler executable/path.
    pub external_command_handler: Option<String>,

    /// Command handler poll frequency.
    pub command_handler_frequency_ms: Option<u64>,

    /// Java-parity trace level for per-device trace files.
    /// Accepts `ERROR`, `INFO`, or `DEBUG` (case-insensitive). Parsed from
    /// the `trace-level` (logger) or `device-trace-level` (consolidator) key.
    /// Stored as a raw string here; the observability crate parses it.
    pub trace_level: Option<String>,

    /// Any keys not yet modeled explicitly. Preserved verbatim so callers can
    /// still reach config that future crates will consume.
    pub extra: HashMap<String, String>,
}

impl SyncLiteConfig {
    /// Parse a SyncLite properties config file from disk.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let f = File::open(path.as_ref())?;
        let props = java_properties::read(BufReader::new(f))
            .map_err(|e| Error::Config(format!("failed to parse properties: {e}")))?;
        Self::from_map(props)
    }

    /// Parse config from either a file path or a directory containing one of:
    /// `synclite.conf`, `synclite_logger.conf`, `synclite_consolidator.conf`.
    pub fn load_any<P: AsRef<Path>>(path: P) -> Result<Self> {
        let p = path.as_ref();
        if p.is_dir() {
            for name in [
                "synclite.conf",
                "synclite_logger.conf",
                "synclite_consolidator.conf",
            ] {
                let candidate = p.join(name);
                if candidate.exists() {
                    return Self::load(candidate);
                }
            }
            return Err(Error::Config(format!(
                "no SyncLite config found under {} (expected synclite.conf, synclite_logger.conf, or synclite_consolidator.conf)",
                p.display()
            )));
        }
        Self::load(p)
    }

    /// Build a config from an already-parsed key/value map.
    pub fn from_map(mut props: HashMap<String, String>) -> Result<Self> {
        normalize_key_aliases(&mut props);

        let mut cfg = SyncLiteConfig::default();

        if let Some(v) = props.remove("device-name") {
            cfg.device_name = Some(v);
        }
        if let Some(v) = props.remove("local-data-stage-directory") {
            cfg.local_stage_dir = Some(PathBuf::from(v));
        }
        if let Some(v) = props.remove("db-path") {
            cfg.db_path = Some(PathBuf::from(v));
        }
        if let Some(v) = props.remove("log-segment-flush-batch-size") {
            cfg.log_segment_flush_batch_size = Some(
                v.parse()
                    .map_err(|e| Error::Config(format!("log-segment-flush-batch-size: {e}")))?,
            );
        }
        if let Some(v) = props.remove("log-queue-size") {
            cfg.log_queue_size = Some(
                v.parse()
                    .map_err(|e| Error::Config(format!("log-queue-size: {e}")))?,
            );
        }
        if let Some(v) = props.remove("log-segment-switch-log-count-threshold") {
            cfg.log_segment_switch_log_count_threshold = Some(
                v.parse().map_err(|e| {
                    Error::Config(format!("log-segment-switch-log-count-threshold: {e}"))
                })?,
            );
        }
        if let Some(v) = props.remove("log-segment-switch-duration-threshold-ms") {
            cfg.log_segment_switch_duration_threshold_ms = Some(
                v.parse().map_err(|e| {
                    Error::Config(format!("log-segment-switch-duration-threshold-ms: {e}"))
                })?,
            );
        }
        if let Some(v) = props.remove("log-segment-shipping-frequency-ms") {
            cfg.log_segment_shipping_frequency_ms = Some(
                v.parse().map_err(|e| {
                    Error::Config(format!("log-segment-shipping-frequency-ms: {e}"))
                })?,
            );
        }
        if let Some(v) = props.remove("log-segment-page-size") {
            cfg.log_segment_page_size = Some(
                v.parse()
                    .map_err(|e| Error::Config(format!("log-segment-page-size: {e}")))?,
            );
        }
        if let Some(v) = props.remove("log-max-inlined-arg-count") {
            cfg.max_inlined_log_args = Some(
                v.parse()
                    .map_err(|e| Error::Config(format!("log-max-inlined-arg-count: {e}")))?,
            );
        }
        if let Some(v) = props.remove("use-precreated-data-backup") {
            cfg.use_precreated_data_backup = Some(
                v.parse()
                    .map_err(|e| Error::Config(format!("use-precreated-data-backup: {e}")))?,
            );
        }
        if let Some(v) = props.remove("vacuum-data-backup") {
            cfg.vacuum_data_backup = Some(
                v.parse()
                    .map_err(|e| Error::Config(format!("vacuum-data-backup: {e}")))?,
            );
        }
        if let Some(v) = props.remove("skip-restart-recovery") {
            cfg.skip_restart_recovery = Some(
                v.parse()
                    .map_err(|e| Error::Config(format!("skip-restart-recovery: {e}")))?,
            );
        }
        if let Some(v) = props.remove("disable-async-logging-for-transactional-device") {
            cfg.disable_async_logging_for_txn_device = Some(
                v.parse().map_err(|e| {
                    Error::Config(format!("disable-async-logging-for-transactional-device: {e}"))
                })?,
            );
        }
        if let Some(v) = props.remove("enable-async-logging-for-appender-device") {
            cfg.enable_async_logging_for_appender_device = Some(
                v.parse().map_err(|e| {
                    Error::Config(format!("enable-async-logging-for-appender-device: {e}"))
                })?,
            );
        }
        if let Some(v) = props.remove("device-encryption-key-file") {
            cfg.encryption_key_file = Some(PathBuf::from(v));
        }
        if let Some(v) = props.remove("include-tables") {
            cfg.include_tables = Some(v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect());
        }
        if let Some(v) = props.remove("exclude-tables") {
            cfg.exclude_tables = Some(v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect());
        }
        if let Some(v) = props.remove("enable-command-handler") {
            cfg.enable_command_handler = Some(
                v.parse()
                    .map_err(|e| Error::Config(format!("enable-command-handler: {e}")))?,
            );
        }
        if let Some(v) = props.remove("command-handler-type") {
            cfg.command_handler_type = Some(v);
        }
        if let Some(v) = props.remove("external-command-handler") {
            cfg.external_command_handler = Some(v);
        }
        if let Some(v) = props.remove("command-handler-frequency-ms") {
            cfg.command_handler_frequency_ms = Some(
                v.parse().map_err(|e| Error::Config(format!("command-handler-frequency-ms: {e}")))?,
            );
        }
        // Java parity: `trace-level` for the logger, `device-trace-level` for
        // the consolidator. Either key sets the same field.
        if let Some(v) = props.remove("trace-level") {
            cfg.trace_level = Some(v);
        }
        if cfg.trace_level.is_none() {
            if let Some(v) = props.remove("device-trace-level") {
                cfg.trace_level = Some(v);
            }
        } else {
            // Drop the alias if the canonical key already won.
            props.remove("device-trace-level");
        }
        if let Some(v) = props.remove("db-engine") {
            cfg.backend = Some(parse_backend(&v)?);
        }
        if let Some(v) = props.remove("device-type") {
            cfg.device_type = Some(
                v.parse()
                    .map_err(|e: String| Error::Config(e))?,
            );
        }

        cfg.extra = props;
        Ok(cfg)
    }
}

fn parse_backend(s: &str) -> Result<Backend> {
    match s.trim().to_ascii_uppercase().as_str() {
        "SQLITE" => Ok(Backend::Sqlite),
        "DUCKDB" => Ok(Backend::DuckDb),
        other => Err(Error::Config(format!("unsupported db-engine: {other}"))),
    }
}

fn move_alias(props: &mut HashMap<String, String>, from: &str, to: &str) {
    if from == to {
        return;
    }
    if props.contains_key(to) {
        props.remove(from);
        return;
    }
    if let Some(v) = props.remove(from) {
        props.insert(to.to_string(), v);
    }
}

fn normalize_key_aliases(props: &mut HashMap<String, String>) {
    // Accept `db-type` (used by other SyncLite tooling) as alias for the
    // canonical `db-engine`.
    move_alias(props, "db-type", "db-engine");

    // Legacy Java logger key `destination-type[-N]` is now spelled
    // `device-stage-type[-N]` (matching the consolidator). Accept either.
    move_alias(props, "destination-type", "device-stage-type");
    let legacy_indexed: Vec<String> = props
        .keys()
        .filter(|k| k.starts_with("destination-type-"))
        .cloned()
        .collect();
    for k in legacy_indexed {
        let suffix = &k["destination-type-".len()..];
        move_alias(props, &k, &format!("device-stage-type-{suffix}"));
    }

    // Java consolidator stage transport keys -> Java logger stage keys.
    // Same physical stage server is configured on both sides; accept either.
    // SFTP
    move_alias(props, "stage-sftp-host", "sftp:host");
    move_alias(props, "stage-sftp-port", "sftp:port");
    move_alias(props, "stage-sftp-user", "sftp:user-name");
    move_alias(props, "stage-sftp-password", "sftp:password");
    move_alias(props, "stage-sftp-data-directory", "sftp:remote-data-stage-directory");
    move_alias(props, "stage-sftp-command-directory", "sftp:remote-command-stage-directory");
    // S3
    move_alias(props, "stage-s3-endpoint", "s3:endpoint");
    move_alias(props, "stage-s3-access-key", "s3:access-key");
    move_alias(props, "stage-s3-secret-key", "s3:secret-key");
    move_alias(props, "stage-s3-data-bucket-name", "s3:data-stage-bucket-name");
    move_alias(props, "stage-s3-command-bucket-name", "s3:command-stage-bucket-name");
    // MinIO (separate transport, Java parity)
    move_alias(props, "stage-minio-endpoint", "minio:endpoint");
    move_alias(props, "stage-minio-access-key", "minio:access-key");
    move_alias(props, "stage-minio-secret-key", "minio:secret-key");
    move_alias(props, "stage-minio-data-bucket-name", "minio:data-stage-bucket-name");
    move_alias(props, "stage-minio-command-bucket-name", "minio:command-stage-bucket-name");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_map() {
        let mut m = HashMap::new();
        m.insert("device-name".into(), "test-device".into());
        m.insert("db-engine".into(), "sqlite".into());
        m.insert("local-data-stage-directory".into(), "/tmp/stage".into());
        m.insert("custom-key".into(), "kept".into());

        let cfg = SyncLiteConfig::from_map(m).unwrap();
        assert_eq!(cfg.device_name.as_deref(), Some("test-device"));
        assert_eq!(cfg.backend, Some(Backend::Sqlite));
        assert_eq!(cfg.device_type, None);
        assert_eq!(
            cfg.local_stage_dir,
            Some(PathBuf::from("/tmp/stage"))
        );
        assert_eq!(cfg.extra.get("custom-key").map(String::as_str), Some("kept"));
    }

    #[test]
    fn parses_device_type() {
        let mut m = HashMap::new();
        m.insert("device-type".into(), "duckdb_store".into());

        let cfg = SyncLiteConfig::from_map(m).unwrap();
        assert_eq!(cfg.device_type, Some(DeviceType::DuckDbStore));
    }

    #[test]
    fn rejects_unknown_backend() {
        let mut m = HashMap::new();
        m.insert("db-engine".into(), "h2".into());
        let err = SyncLiteConfig::from_map(m).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }
}


