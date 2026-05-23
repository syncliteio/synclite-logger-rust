//! SyncLite device layout computation.
//!
//! Mirrors the path conventions from `SyncLite.java`:
//!
//! - Device home: `<db_path>.synclite/` (the `.synclite` suffix is
//!   appended to the *full* db path string, not a sibling directory).
//! - Local metadata file: `<device_home>/<db_file_name>.synclite.metadata`.
//! - Local backup file: `<device_home>/<db_file_name>.synclite.backup`.
//! - Stage subdir: `<local_stage_dir>/synclite-<device_name>-<uuid>/`
//!   (Java's `writeArchieveName`). When `device_name` is empty, the
//!   form is `synclite-<uuid>`.

use std::path::{Path, PathBuf};

/// Suffix appended to the user DB path to form the per-device home dir.
pub const DEVICE_HOME_SUFFIX: &str = ".synclite";
/// Suffix of the per-device metadata SQLite file.
pub const METADATA_SUFFIX: &str = ".synclite.metadata";
/// Suffix of the local data-backup file produced at init.
pub const BACKUP_SUFFIX: &str = ".synclite.backup";
/// Prefix used to form the `writeArchieveName` (stage subdir name).
pub const ARCHIVE_NAME_PREFIX: &str = "synclite-";

/// Path bundle for a device.
#[derive(Debug, Clone)]
pub struct DeviceLayout {
    /// User-facing database file.
    pub db_path: PathBuf,
    /// Just the file name component of `db_path` (e.g. `demo.db`).
    pub db_file_name: String,
    /// `<db_path>.synclite/` directory.
    pub device_home: PathBuf,
    /// `<device_home>/<db_file_name>.synclite.metadata`.
    pub metadata_path: PathBuf,
    /// `<device_home>/<db_file_name>.synclite.backup`.
    pub backup_local_path: PathBuf,
}

impl DeviceLayout {
    /// Compute base paths anchored at the user DB. `archive` paths are
    /// added separately in [`DeviceLayout::with_archive`] once the
    /// per-device UUID is known.
    pub fn new(db_path: PathBuf) -> Self {
        let db_file_name = db_path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        // Append `.synclite` to the full path string (Java behavior).
        let device_home: PathBuf = format!("{}{}", db_path.display(), DEVICE_HOME_SUFFIX).into();
        let metadata_path = device_home.join(format!("{db_file_name}{METADATA_SUFFIX}"));
        let backup_local_path = device_home.join(format!("{db_file_name}{BACKUP_SUFFIX}"));
        Self {
            db_path,
            db_file_name,
            device_home,
            metadata_path,
            backup_local_path,
        }
    }
}

/// Stage-subdir paths derived once the UUID is known.
#[derive(Debug, Clone)]
pub struct ArchiveLayout {
    /// `synclite-<device_name>-<uuid>` (or `synclite-<uuid>`).
    pub archive_name: String,
    /// `<local_stage_dir>/<archive_name>/`.
    pub stage_subdir: PathBuf,
    /// `<stage_subdir>/<db_file_name>.synclite.metadata`.
    pub stage_metadata_path: PathBuf,
    /// `<stage_subdir>/<db_file_name>.synclite.backup`.
    pub stage_backup_path: PathBuf,
}

impl ArchiveLayout {
    /// Compute archive paths.
    pub fn new(
        stage_dir: &Path,
        device_name: &str,
        uuid: &str,
        db_file_name: &str,
    ) -> Self {
        let archive_name = if device_name.is_empty() {
            format!("{ARCHIVE_NAME_PREFIX}{uuid}")
        } else {
            format!("{ARCHIVE_NAME_PREFIX}{device_name}-{uuid}")
        };
        let stage_subdir = stage_dir.join(&archive_name);
        let stage_metadata_path = stage_subdir.join(format!("{db_file_name}{METADATA_SUFFIX}"));
        let stage_backup_path = stage_subdir.join(format!("{db_file_name}{BACKUP_SUFFIX}"));
        Self {
            archive_name,
            stage_subdir,
            stage_metadata_path,
            stage_backup_path,
        }
    }
}
