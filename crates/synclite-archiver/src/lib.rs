//! Pluggable archivers that ship finalized log segments to a staging
//! destination (local filesystem, S3, SFTP, Kafka, ...).
//!
//! Each backend lives behind a Cargo feature so embedders only pay for the
//! transports they actually need. Phase 2 ships the local filesystem
//! archiver only; remote transports arrive in later phases.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::path::Path;
use synclite_core::Result;

/// A target capable of accepting a finalized log segment.
///
/// Implementations must be cheap to clone via [`Arc`](std::sync::Arc) and
/// safe to call from a background worker thread.
pub trait Archiver: Send + Sync {
    /// Short human-readable name (e.g. `"fs"`, `"s3"`).
    fn name(&self) -> &str;

    /// Transfer `segment_path` to the configured destination. The segment
    /// file must remain readable on the source until this call returns.
    fn ship(&self, segment_path: &Path) -> Result<()>;
}

#[cfg(feature = "fs")]
mod fs;
#[cfg(feature = "fs")]
pub use fs::FsArchiver;

#[cfg(feature = "s3")]
mod s3;
#[cfg(feature = "s3")]
pub use s3::{S3Archiver, S3Config, StaticCredentials};

#[cfg(feature = "sftp")]
mod sftp;
#[cfg(feature = "sftp")]
pub use sftp::{SftpArchiver, SftpAuth, SftpConfig};
