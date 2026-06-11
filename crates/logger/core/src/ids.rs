//! Strongly-typed identifier newtypes and shared enums.
//!
//! Newtypes prevent accidental mixing of, e.g., a commit id with an operation
//! id at compile time.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Identifies which embedded database backend is in use.
///
/// Only SQLite and DuckDB are supported in the Rust port. Adding a new
/// backend means adding a new variant here plus a new crate implementing
/// the `DbDevice` trait (defined in `synclite-db-traits`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Backend {
    /// SQLite via `rusqlite`.
    Sqlite,
    /// DuckDB via `duckdb-rs`.
    DuckDb,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Backend::Sqlite => f.write_str("SQLITE"),
            Backend::DuckDb => f.write_str("DUCKDB"),
        }
    }
}

/// Java-compatible device type / mode.
///
/// Variant identifiers intentionally use Java's `UPPER_SNAKE_CASE` spelling
/// (see `com.synclite.consolidator.device.DeviceType`) so the Rust and Java
/// codebases address the same enumerator with the same symbol. The
/// `non_camel_case_types` lint is suppressed for this reason.
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DeviceType {
    /// SQLite transactional SQL device.
    SQLITE,
    /// SQLite store device.
    SQLITE_STORE,
    /// SQLite streaming device.
    STREAMING,
    /// DuckDB transactional SQL device.
    DUCKDB,
    /// DuckDB store device.
    DUCKDB_STORE,
}

impl DeviceType {
    /// Backend implied by this device type.
    pub fn backend(self) -> Backend {
        match self {
            DeviceType::SQLITE | DeviceType::SQLITE_STORE | DeviceType::STREAMING => {
                Backend::Sqlite
            }
            DeviceType::DUCKDB | DeviceType::DUCKDB_STORE => Backend::DuckDb,
        }
    }

    /// Whether this device follows transactional SQL-device logging semantics.
    pub fn is_transactional(self) -> bool {
           matches!(self, DeviceType::SQLITE | DeviceType::DUCKDB)
    }

    /// Whether this device participates in in-doubt restart recovery.
    ///
    /// Restart recovery reconciles the user DB `synclite_txn` state with the
    /// latest segment's terminal fate and applies to SQL, store, and streaming
    /// modes.
    pub fn participates_in_restart_recovery(self) -> bool {
        true
    }

    /// Whether this device type is a STORE mode device.
    pub fn is_store(self) -> bool {
        matches!(self, DeviceType::SQLITE_STORE | DeviceType::DUCKDB_STORE)
    }

    /// Whether this device type is the STREAMING mode.
    pub fn is_streaming(self) -> bool {
        matches!(self, DeviceType::STREAMING)
    }

    /// Default device type for a backend when no explicit mode is configured.
    pub fn default_for_backend(backend: Backend) -> Self {
        match backend {
            Backend::Sqlite => DeviceType::SQLITE,
            Backend::DuckDb => DeviceType::DUCKDB,
        }
    }
}

impl fmt::Display for DeviceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeviceType::SQLITE => f.write_str("SQLITE"),
            DeviceType::SQLITE_STORE => f.write_str("SQLITE_STORE"),
            DeviceType::STREAMING => f.write_str("STREAMING"),
            DeviceType::DUCKDB => f.write_str("DUCKDB"),
            DeviceType::DUCKDB_STORE => f.write_str("DUCKDB_STORE"),
        }
    }
}

impl std::str::FromStr for DeviceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_uppercase().as_str() {
            "SQLITE" => Ok(DeviceType::SQLITE),
            "SQLITE_STORE" => Ok(DeviceType::SQLITE_STORE),
            "STREAMING" => Ok(DeviceType::STREAMING),
            "DUCKDB" => Ok(DeviceType::DUCKDB),
            "DUCKDB_STORE" => Ok(DeviceType::DUCKDB_STORE),
            other => Err(format!("unsupported device-type: {other}")),
        }
    }
}

/// Stable identifier for a logical SyncLite device (one database file +
/// staging pipeline).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeviceId(pub Uuid);

impl DeviceId {
    /// Generate a fresh random device id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for DeviceId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Monotonic commit id, mirroring the Java `commitId` column on the
/// `commandlog` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CommitId(pub u64);

/// Per-commit operation counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OperationId(pub u64);

/// Monotonic log-segment sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentSequence(pub u64);

