//! Error and Result types for the SyncLite logger.

use thiserror::Error;

/// Canonical Result alias used across the workspace.
pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error type. Crates may convert their own errors into this via
/// `From` impls; downstream FFI shims map variants to integer codes.
#[derive(Debug, Error)]
pub enum Error {
    /// An I/O operation failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Configuration was invalid or could not be parsed.
    #[error("configuration error: {0}")]
    Config(String),

    /// The underlying database returned an error.
    #[error("database error: {0}")]
    Db(String),

    /// The log subsystem (segment writer / placer) reported an error.
    #[error("log error: {0}")]
    Log(String),

    /// A staging archiver (FS / S3 / Kafka / ...) failed to ship a segment.
    #[error("archiver error: {0}")]
    Archiver(String),

    /// Internal invariant violated. These indicate bugs.
    #[error("internal error: {0}")]
    Internal(String),
}

