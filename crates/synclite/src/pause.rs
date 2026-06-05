//! Pause / resume sync.
//!
//! `pause_sync(db_path)` halts application of staged segments on the
//! destination for a device. The logger, mover, and shipper keep
//! running, so user writes continue to flow into local segments and
//! those segments continue to be staged and shipped to the upload
//! root. Only the consolidator's apply-to-destination step is paused.
//! `resume_sync(db_path)` clears the pause and the consolidator
//! drains whatever queued up while paused.
//!
//! State is stored as a sentinel file at
//! `<db_path>.synclite/sync_paused`. Workers poll the file on each
//! iteration; presence == paused. Survives process restart so a
//! device that was paused before shutdown stays paused when the
//! process is brought back up.
//!
//! Trigger-file protocol mirrors `reinitialize`: dropping a file
//! named `pause_sync.<device-name>` or `resume_sync.<device-name>`
//! alongside the database file makes the next `synclite::initialize`
//! call apply the corresponding operation and then delete the
//! trigger file.

use std::path::{Path, PathBuf};

use logger_core::{Error, Result};

use crate::layout::DeviceLayout;
use crate::normalize_db_path;

/// File name of the pause sentinel inside the device home.
pub const PAUSE_SENTINEL_NAME: &str = "sync_paused";
/// Trigger-file basename for `pause_sync` (dropped alongside the DB file).
pub const TRIGGER_PAUSE: &str = "pause_sync";
/// Trigger-file basename for `resume_sync` (dropped alongside the DB file).
pub const TRIGGER_RESUME: &str = "resume_sync";

/// Path of the pause sentinel for a given device home.
pub fn pause_sentinel_path(device_home: &Path) -> PathBuf {
    device_home.join(PAUSE_SENTINEL_NAME)
}

/// Pause destination consolidation for the device at `db_path`.
/// Idempotent. The logger, mover, and shipper remain active.
pub fn pause_sync<P: AsRef<Path>>(db_path: P) -> Result<()> {
    let layout = require_device(db_path.as_ref())?;
    let sentinel = pause_sentinel_path(&layout.device_home);
    if let Some(parent) = sentinel.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if !sentinel.exists() {
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&sentinel)?;
    }
    Ok(())
}

/// Resume destination consolidation for the device at `db_path`.
/// Idempotent.
pub fn resume_sync<P: AsRef<Path>>(db_path: P) -> Result<()> {
    let layout = require_device(db_path.as_ref())?;
    let sentinel = pause_sentinel_path(&layout.device_home);
    if sentinel.exists() {
        std::fs::remove_file(&sentinel)?;
    }
    Ok(())
}

/// Returns `true` if the device at `db_path` is currently paused.
pub fn is_sync_paused<P: AsRef<Path>>(db_path: P) -> Result<bool> {
    let layout = require_device(db_path.as_ref())?;
    Ok(pause_sentinel_path(&layout.device_home).exists())
}

/// Trigger-file check called from `synclite::initialize` before the
/// logger is brought up. Applies any `pause_sync.<device-name>` or
/// `resume_sync.<device-name>` file dropped alongside the database
/// and removes the trigger on success.
pub(crate) fn maybe_run_trigger(db_path: &Path, device_name: &str) -> Result<()> {
    let parent = db_path.parent().unwrap_or_else(|| Path::new("."));
    let pause = parent.join(format!("{TRIGGER_PAUSE}.{device_name}"));
    let resume = parent.join(format!("{TRIGGER_RESUME}.{device_name}"));
    if !pause.exists() && !resume.exists() {
        return Ok(());
    }
    // Triggers only make sense once the device has been initialized
    // at least once (the sentinel lives in the device home, which is
    // created by `initialize`). Materialize the device home eagerly
    // so a trigger dropped before the very first init still takes
    // effect on this bring-up.
    let layout = DeviceLayout::new(db_path.to_path_buf());
    std::fs::create_dir_all(&layout.device_home)?;
    let sentinel = pause_sentinel_path(&layout.device_home);
    if pause.exists() {
        if !sentinel.exists() {
            std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&sentinel)?;
        }
        let _ = std::fs::remove_file(&pause);
    }
    if resume.exists() {
        if sentinel.exists() {
            let _ = std::fs::remove_file(&sentinel);
        }
        let _ = std::fs::remove_file(&resume);
    }
    Ok(())
}

fn require_device(db_path: &Path) -> Result<DeviceLayout> {
    let normalized = normalize_db_path(db_path)?;
    let layout = DeviceLayout::new(normalized);
    if !layout.metadata_path.exists() {
        return Err(Error::Config(format!(
            "pause/resume: device metadata not found at {}",
            layout.metadata_path.display()
        )));
    }
    Ok(layout)
}
