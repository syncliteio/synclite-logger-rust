use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use logger_core::{Error, Result};

use crate::layout::DEVICE_HOME_SUFFIX;

/// Process-wide lock guard for a single SyncLite DB file.
#[derive(Debug)]
pub struct AppLock {
    file: File,
    #[allow(dead_code)]
    lock_path: PathBuf,
}

impl AppLock {
    /// Acquire the native lock file `<db_path>.synclite/<db_name>.lock`.
    pub fn try_lock(db_path: &Path) -> Result<Self> {
        let db_name = db_path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
            Error::Config(format!(
                "cannot derive database file name for lock: {}",
                db_path.display()
            ))
        })?;

        let lock_dir: PathBuf = format!("{}{}", db_path.display(), DEVICE_HOME_SUFFIX).into();
        std::fs::create_dir_all(&lock_dir)?;
        let lock_path = lock_dir.join(format!("{db_name}.lock"));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)?;

        file.try_lock_exclusive().map_err(|_e| {
            Error::Config(format!(
                "Failed to lock db file {}. Another application is using this db file",
                db_path.display()
            ))
        })?;

        Ok(Self { file, lock_path })
    }
}

impl Drop for AppLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}


