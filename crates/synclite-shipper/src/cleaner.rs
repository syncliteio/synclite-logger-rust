//! [`LogCleaner`]: deletes local log-segment files once they have been
//! confirmed shipped to every archiver.
//!
//! The cleaner is intentionally tiny — it owns no state and exposes a
//! [`ShippedCallback`](crate::ShippedCallback) factory you wire into
//! [`ShipperConfig::on_shipped`](crate::ShipperConfig::on_shipped).
//!
//! Removal is best-effort: a missing file is treated as success (the
//! consolidator may have removed it already), and an I/O error is logged
//! via `tracing::error!` but does not panic.

use std::path::Path;
use std::sync::Arc;

use crate::ShippedCallback;

/// Deletes shipped segments from the local stage directory.
#[derive(Debug, Default, Clone, Copy)]
pub struct LogCleaner;

impl LogCleaner {
    /// Convenience constructor. The cleaner is stateless; this exists so
    /// callers read naturally: `LogCleaner::new().as_callback()`.
    pub fn new() -> Self {
        Self
    }

    /// Synchronously delete `segment_path`. Missing files are not an error.
    pub fn delete(segment_path: &Path) {
        match std::fs::remove_file(segment_path) {
            Ok(()) => {
                tracing::debug!(segment = %segment_path.display(), "cleaned shipped segment");
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!(segment = %segment_path.display(), "segment already gone");
            }
            Err(e) => {
                tracing::error!(
                    segment = %segment_path.display(),
                    error = %e,
                    "failed to remove shipped segment"
                );
            }
        }
    }

    /// Produce a [`ShippedCallback`] that deletes each shipped segment.
    pub fn as_callback(self) -> ShippedCallback {
        Arc::new(|p: &Path| Self::delete(p))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn deletes_existing_file() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("seg.db");
        fs::write(&p, b"x").unwrap();
        assert!(p.exists());
        LogCleaner::delete(&p);
        assert!(!p.exists());
    }

    #[test]
    fn missing_file_is_ok() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("nope.db");
        // Must not panic.
        LogCleaner::delete(&p);
    }

    #[test]
    fn callback_form_deletes() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("seg.db");
        fs::write(&p, b"x").unwrap();
        let cb = LogCleaner::new().as_callback();
        cb(&p);
        assert!(!p.exists());
    }
}
