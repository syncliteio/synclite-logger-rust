//! Java log4j/reload4j-parity per-device trace files for the Rust SyncLite
//! logger and consolidator.
//!
//! Mirrors Java's `RollingFileAppender` + `PatternLayout` behavior:
//!
//! * Logger: one trace file per device at
//!   `<dbPath>.synclite/<dbName>.trace`, rotated at 10 KB with 10 backups,
//!   pattern `%d %-5p [%c{1}] %m%n` (matching `SyncLite.initTracer`).
//! * Consolidator device: one trace file per device at
//!   `<deviceRoot>/synclite_device.trace`, rotated at 10 MB with 10 backups,
//!   pattern `%d %-5p [%c{1}] [%t] %m%n` (matching `Device.initLogger`).
//! * Consolidator global: one trace file at
//!   `<workDir>/synclite_consolidator.trace`.
//!
//! Level is configurable via `trace-level` (logger) or `device-trace-level`
//! (consolidator). Accepted values are `ERROR`, `INFO`, `DEBUG`.

#![forbid(unsafe_code)]

use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use file_rotate::{compression::Compression, suffix::AppendCount, ContentLimit, FileRotate};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use time::format_description::FormatItem;
use time::macros::format_description;
use time::OffsetDateTime;

/// Errors produced when initializing a tracer.
#[derive(Debug, thiserror::Error)]
pub enum TracerError {
    /// I/O error creating the trace directory or file.
    #[error("trace io error: {0}")]
    Io(#[from] std::io::Error),
    /// Invalid string supplied for the trace level.
    #[error("invalid trace level: {0}")]
    InvalidLevel(String),
}

/// Result alias for tracer setup.
pub type Result<T> = std::result::Result<T, TracerError>;

/// Java-parity trace level. Ordered so that `DEBUG > INFO > ERROR`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TraceLevel {
    /// Only ERROR events are written.
    Error = 0,
    /// ERROR + INFO events are written. Java default for the consolidator.
    Info = 1,
    /// All events (ERROR + INFO + DEBUG) are written.
    Debug = 2,
}

impl TraceLevel {
    /// Parse a level from a case-insensitive string (`ERROR`, `INFO`, `DEBUG`).
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "ERROR" => Ok(TraceLevel::Error),
            "INFO" => Ok(TraceLevel::Info),
            "DEBUG" => Ok(TraceLevel::Debug),
            other => Err(TracerError::InvalidLevel(other.to_string())),
        }
    }

    /// log4j-style 5-character padded name.
    pub fn as_padded(&self) -> &'static str {
        match self {
            TraceLevel::Error => "ERROR",
            TraceLevel::Info => "INFO ",
            TraceLevel::Debug => "DEBUG",
        }
    }

    /// `true` if an event of `event_level` should be emitted at this tracer level.
    #[inline]
    pub fn enabled_for(&self, event_level: TraceLevel) -> bool {
        event_level <= *self
    }
}

impl fmt::Display for TraceLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_padded().trim_end())
    }
}

/// Whether the trace line should include the current thread name in `[brackets]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatVariant {
    /// `%d %-5p [%c{1}] %m%n` (Java logger).
    LoggerStyle,
    /// `%d %-5p [%c{1}] [%t] %m%n` (Java consolidator device + global).
    ConsolidatorStyle,
}

/// Rotation settings for the underlying `RollingFileAppender`.
#[derive(Debug, Clone, Copy)]
pub struct RotationPolicy {
    /// Max size of the active file before rotation.
    pub max_bytes: usize,
    /// Number of historical backups to keep (`maxBackupIndex`).
    pub max_backups: usize,
}

impl RotationPolicy {
    /// Java logger default: 10 KB / 10 backups (`SyncLite.initTracer`).
    pub const fn logger_default() -> Self {
        Self { max_bytes: 10 * 1024, max_backups: 10 }
    }
    /// Java consolidator default: 10 MB / 10 backups (`RollingFileAppender` defaults).
    pub const fn consolidator_default() -> Self {
        Self { max_bytes: 10 * 1024 * 1024, max_backups: 10 }
    }
}

/// A per-device or global trace writer mirroring Java's log4j appender.
///
/// Cheap to clone via `Arc`. All writes are serialized through a single mutex
/// so concurrent threads produce well-formed lines.
pub struct Tracer {
    inner: Mutex<FileRotate<AppendCount>>,
    level: TraceLevel,
    variant: FormatVariant,
    path: PathBuf,
}

const TS_FMT: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second],[subsecond digits:3]");

impl Tracer {
    /// Construct a [`Tracer`] at `path` with the given rotation policy, level, and format.
    pub fn open(
        path: impl Into<PathBuf>,
        rotation: RotationPolicy,
        level: TraceLevel,
        variant: FormatVariant,
    ) -> Result<Arc<Self>> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let rotate = FileRotate::new(
            &path,
            AppendCount::new(rotation.max_backups),
            ContentLimit::Bytes(rotation.max_bytes),
            Compression::None,
            #[cfg(unix)]
            None,
        );
        Ok(Arc::new(Self {
            inner: Mutex::new(rotate),
            level,
            variant,
            path,
        }))
    }

    /// Construct a Java-logger-parity tracer for a user database file.
    ///
    /// Writes to `<db_path>.synclite/<db_filename>.trace`.
    pub fn for_logger(db_path: &Path, level: TraceLevel) -> Result<Arc<Self>> {
        let device_home = device_home_for(db_path);
        let file_name = db_path
            .file_name()
            .map(|s| s.to_os_string())
            .unwrap_or_else(|| std::ffi::OsString::from("device"));
        let mut trace_name = file_name;
        trace_name.push(".trace");
        let trace_path = device_home.join(trace_name);
        Self::open(
            trace_path,
            RotationPolicy::logger_default(),
            level,
            FormatVariant::LoggerStyle,
        )
    }

    /// Construct a Java-consolidator-device parity tracer.
    ///
    /// Writes to `<device_root>/synclite_device.trace`.
    pub fn for_consolidator_device(device_root: &Path, level: TraceLevel) -> Result<Arc<Self>> {
        Self::open(
            device_root.join("synclite_device.trace"),
            RotationPolicy::consolidator_default(),
            level,
            FormatVariant::ConsolidatorStyle,
        )
    }

    /// Construct a Java-consolidator-global parity tracer.
    ///
    /// Writes to `<work_dir>/synclite_consolidator.trace`.
    pub fn for_consolidator_global(work_dir: &Path, level: TraceLevel) -> Result<Arc<Self>> {
        Self::open(
            work_dir.join("synclite_consolidator.trace"),
            RotationPolicy::consolidator_default(),
            level,
            FormatVariant::ConsolidatorStyle,
        )
    }

    /// Currently configured level.
    pub fn level(&self) -> TraceLevel {
        self.level
    }

    /// Trace file path (active file; rotated backups append `.1`..`.N`).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns `true` if events at `event_level` will be written.
    #[inline]
    pub fn is_enabled(&self, event_level: TraceLevel) -> bool {
        self.level.enabled_for(event_level)
    }

    /// Write a pre-formatted record at `event_level` with category `target`.
    ///
    /// Filtered by the configured level. The line is formatted to match the
    /// Java `PatternLayout` and includes a trailing newline.
    pub fn log(&self, event_level: TraceLevel, target: &str, args: fmt::Arguments<'_>) {
        if !self.is_enabled(event_level) {
            return;
        }
        let ts = OffsetDateTime::now_local()
            .unwrap_or_else(|_| OffsetDateTime::now_utc());
        let ts_str = ts.format(&TS_FMT).unwrap_or_else(|_| String::from("0000-00-00 00:00:00,000"));
        let category = short_category(target);

        // Build the line in a single String, then write under the mutex to
        // keep lines atomic with respect to rotation.
        let mut line = String::with_capacity(96);
        // %d %-5p [%c{1}]
        line.push_str(&ts_str);
        line.push(' ');
        line.push_str(event_level.as_padded());
        line.push_str(" [");
        line.push_str(&category);
        line.push(']');
        if self.variant == FormatVariant::ConsolidatorStyle {
            // [%t]
            line.push(' ');
            line.push('[');
            line.push_str(&current_thread_name());
            line.push(']');
        }
        line.push(' ');
        line.push_str(&format!("{args}"));
        line.push('\n');

        let mut writer = self.inner.lock();
        let _ = writer.write_all(line.as_bytes());
        let _ = writer.flush();
    }

    /// Convenience: write at `ERROR` level.
    pub fn error(&self, target: &str, args: fmt::Arguments<'_>) {
        self.log(TraceLevel::Error, target, args)
    }

    /// Convenience: write at `INFO` level.
    pub fn info(&self, target: &str, args: fmt::Arguments<'_>) {
        self.log(TraceLevel::Info, target, args)
    }

    /// Convenience: write at `DEBUG` level.
    pub fn debug(&self, target: &str, args: fmt::Arguments<'_>) {
        self.log(TraceLevel::Debug, target, args)
    }
}

impl fmt::Debug for Tracer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tracer")
            .field("path", &self.path)
            .field("level", &self.level)
            .field("variant", &self.variant)
            .finish()
    }
}

/// Mirror Java's `SyncLite.initTracer` path computation:
/// `<dbPath>.synclite/<dbName>.trace` — but we return the device home for callers.
fn device_home_for(db_path: &Path) -> PathBuf {
    let parent = db_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let stem = db_path
        .file_name()
        .map(|s| s.to_os_string())
        .unwrap_or_else(|| std::ffi::OsString::from("device"));
    let mut dir_name = stem;
    dir_name.push(".synclite");
    parent.join(dir_name)
}

/// Java `%c{1}` keeps only the last segment of a dotted category.
fn short_category(target: &str) -> String {
    target.rsplit('.').next().unwrap_or(target).to_string()
}

fn current_thread_name() -> String {
    thread::current()
        .name()
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{:?}", thread::current().id()))
}

/// Convenience macro mirroring `tracer.info("target", "fmt", args)`.
///
/// Usage: `tracer_info!(tracer, "SQLLogger", "opened device {}", name);`
#[macro_export]
macro_rules! tracer_info {
    ($tracer:expr, $target:expr, $($arg:tt)+) => {
        $tracer.info($target, ::std::format_args!($($arg)+))
    };
}

/// Convenience macro mirroring `tracer.debug(...)`.
#[macro_export]
macro_rules! tracer_debug {
    ($tracer:expr, $target:expr, $($arg:tt)+) => {
        $tracer.debug($target, ::std::format_args!($($arg)+))
    };
}

/// Convenience macro mirroring `tracer.error(...)`.
#[macro_export]
macro_rules! tracer_error {
    ($tracer:expr, $target:expr, $($arg:tt)+) => {
        $tracer.error($target, ::std::format_args!($($arg)+))
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn parses_trace_level() {
        assert_eq!(TraceLevel::parse("info").unwrap(), TraceLevel::Info);
        assert_eq!(TraceLevel::parse("DEBUG").unwrap(), TraceLevel::Debug);
        assert_eq!(TraceLevel::parse("  Error ").unwrap(), TraceLevel::Error);
        assert!(TraceLevel::parse("WARN").is_err());
    }

    #[test]
    fn level_ordering_filters_events() {
        let l = TraceLevel::Info;
        assert!(l.enabled_for(TraceLevel::Error));
        assert!(l.enabled_for(TraceLevel::Info));
        assert!(!l.enabled_for(TraceLevel::Debug));
    }

    #[test]
    fn logger_path_matches_java_layout() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("mydb.db");
        let tracer = Tracer::for_logger(&db, TraceLevel::Debug).unwrap();
        let expected = tmp.path().join("mydb.db.synclite").join("mydb.db.trace");
        assert_eq!(tracer.path(), expected);
        // First event must create the parent dir.
        tracer.info("SQLLogger", format_args!("hello"));
        assert!(expected.exists(), "trace file should be created");
    }

    #[test]
    fn consolidator_device_path_matches_java_layout() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("device-root");
        std::fs::create_dir_all(&root).unwrap();
        let tracer = Tracer::for_consolidator_device(&root, TraceLevel::Info).unwrap();
        assert_eq!(tracer.path(), root.join("synclite_device.trace"));
    }

    #[test]
    fn line_format_logger_style() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("d.db");
        let tracer = Tracer::for_logger(&db, TraceLevel::Debug).unwrap();
        tracer.info("io.synclite.logger.SQLLogger", format_args!("hi {}", 42));
        let content = fs::read_to_string(tracer.path()).unwrap();
        // Expected shape: "<ts> INFO  [SQLLogger] hi 42\n"
        let re = regex_lite_match(&content);
        assert!(re, "unexpected line: {content:?}");
    }

    #[test]
    fn line_format_consolidator_includes_thread() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("d");
        std::fs::create_dir_all(&root).unwrap();
        let tracer = Tracer::for_consolidator_device(&root, TraceLevel::Debug).unwrap();
        let handle = std::thread::Builder::new()
            .name("worker-7".into())
            .spawn({
                let t = tracer.clone();
                move || t.info("com.synclite.consolidator.device.Device", format_args!("started"))
            })
            .unwrap();
        handle.join().unwrap();
        let content = fs::read_to_string(tracer.path()).unwrap();
        assert!(content.contains("[Device]"), "expected [Device] category: {content:?}");
        assert!(content.contains("[worker-7]"), "expected [worker-7] thread: {content:?}");
        assert!(content.contains("started"));
    }

    #[test]
    fn filters_by_level() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("d.db");
        let tracer = Tracer::for_logger(&db, TraceLevel::Error).unwrap();
        tracer.info("SQLLogger", format_args!("ignored"));
        tracer.debug("SQLLogger", format_args!("ignored"));
        tracer.error("SQLLogger", format_args!("kept"));
        let content = fs::read_to_string(tracer.path()).unwrap();
        assert!(content.contains("kept"));
        assert!(!content.contains("ignored"), "level-filtered events leaked: {content}");
    }

    #[test]
    fn rotates_at_byte_limit() {
        let tmp = tempdir().unwrap();
        let trace_file = tmp.path().join("rot.trace");
        let tracer = Tracer::open(
            trace_file.clone(),
            RotationPolicy { max_bytes: 200, max_backups: 3 },
            TraceLevel::Debug,
            FormatVariant::LoggerStyle,
        )
        .unwrap();
        for i in 0..50 {
            tracer.info("SQLLogger", format_args!("line {i} {}", "x".repeat(20)));
        }
        // At least one backup must exist.
        let entries: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(entries.iter().any(|n| n.starts_with("rot.trace.")),
            "expected rotated file, got: {entries:?}");
    }

    fn regex_lite_match(s: &str) -> bool {
        // Match: digits, '-', digits, '-', digits, ' ', hh:mm:ss,ms, ' ', INFO, " [SQLLogger] hi 42"
        let mut chars = s.chars().peekable();
        for _ in 0..10 {
            if chars.next().is_none() {
                return false;
            }
        }
        // Just look for "INFO  [SQLLogger] hi 42"
        s.contains("INFO  [SQLLogger] hi 42")
    }
}
