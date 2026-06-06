//! Local-filesystem archiver. Copies a segment into a target directory
//! using a `<name>.tmp` → `<name>` rename so partial files never become
//! visible to consumers.

use std::fs;
use std::path::{Path, PathBuf};

use logger_core::{Error, Result};

use crate::Archiver;

/// Filesystem-backed archiver.
#[derive(Debug, Clone)]
pub struct FsArchiver {
    target_dir: PathBuf,
}

impl FsArchiver {
    /// Create an archiver that ships into `target_dir`. The directory is
    /// created on first use.
    pub fn new<P: Into<PathBuf>>(target_dir: P) -> Self {
        Self {
            target_dir: target_dir.into(),
        }
    }

    /// Target directory segments are shipped to.
    pub fn target_dir(&self) -> &Path {
        &self.target_dir
    }
}

impl Archiver for FsArchiver {
    fn name(&self) -> &str {
        "fs"
    }

    fn ship(&self, segment_path: &Path) -> Result<()> {
        fs::create_dir_all(&self.target_dir)?;
        let file_name = segment_path.file_name().ok_or_else(|| {
            Error::Archiver(format!(
                "segment path has no file name: {}",
                segment_path.display()
            ))
        })?;
        let final_path = self.target_dir.join(file_name);
        let tmp_path = self.target_dir.join({
            let mut s = std::ffi::OsString::from(file_name);
            s.push(".tmp");
            s
        });

        // Copy to .tmp first; rename for atomic visibility.
        fs::copy(segment_path, &tmp_path)
            .map_err(|e| Error::Archiver(format!("copy {}: {e}", segment_path.display())))?;
        fs::rename(&tmp_path, &final_path).map_err(|e| {
            // Best-effort cleanup on rename failure.
            let _ = fs::remove_file(&tmp_path);
            Error::Archiver(format!("rename to {}: {e}", final_path.display()))
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn copies_file_atomically() {
        let src_dir = tempdir().unwrap();
        let dst_dir = tempdir().unwrap();
        let src = src_dir.path().join("seg-7.db");
        let mut f = fs::File::create(&src).unwrap();
        f.write_all(b"hello").unwrap();
        drop(f);

        let archiver = FsArchiver::new(dst_dir.path());
        archiver.ship(&src).unwrap();

        let landed = dst_dir.path().join("seg-7.db");
        assert!(landed.exists());
        assert_eq!(fs::read(&landed).unwrap(), b"hello");
        // No leftover .tmp file.
        assert!(!dst_dir.path().join("seg-7.db.tmp").exists());
    }
}


