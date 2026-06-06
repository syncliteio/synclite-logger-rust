//! Metadata manager — persists logger state across restarts.
//!
//! Mirrors the Java `MetadataManager`: a small key/value SQLite database
//! tracking device status, log segment sequence numbers, and similar
//! bookkeeping. Kept deliberately tiny so the Rust logger can read/write
//! metadata files that the Java consolidator already understands.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use logger_core::{Error, Result};

/// Standard well-known metadata keys.
pub mod keys {
    /// Lifecycle status of the device (`NEW`, `READY_TO_APPLY`, ...).
    pub const STATUS: &str = "status";
    /// Last issued log-segment sequence number (as a decimal string).
    pub const LOG_SEGMENT_SEQUENCE: &str = "log-segment-sequence";
    /// Last issued data-file sequence number (as a decimal string).
    pub const DATA_FILE_SEQUENCE: &str = "data-file-sequence";
}

/// Persistent key/value store backed by a tiny SQLite file.
pub struct MetadataManager {
    path: PathBuf,
    conn: Mutex<Connection>,
}

impl MetadataManager {
    /// Open (creating if necessary) the metadata database at `path`.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open(&path).map_err(db_err)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT);",
        )
        .map_err(db_err)?;
        Ok(Self {
            path,
            conn: Mutex::new(conn),
        })
    }

    /// Path of the underlying metadata file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Look up a metadata value, returning `None` if the key is absent.
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT value FROM metadata WHERE key = ?1",
            params![key],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(db_err)
    }

    /// Upsert a metadata value.
    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO metadata(key, value) VALUES(?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )
        .map_err(db_err)?;
        Ok(())
    }

    /// Delete a metadata key. Returns `true` if a row was removed.
    pub fn remove(&self, key: &str) -> Result<bool> {
        let conn = self.conn.lock();
        let n = conn
            .execute("DELETE FROM metadata WHERE key = ?1", params![key])
            .map_err(db_err)?;
        Ok(n > 0)
    }
}

fn db_err(e: rusqlite::Error) -> Error {
    Error::Db(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_in_memory() {
        // Use a temp file so we exercise the on-disk path.
        let dir = std::env::temp_dir().join(format!("synclite-meta-{}", uuid_hex()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("metadata.db");

        let mm = MetadataManager::open(&path).unwrap();
        assert!(mm.get(keys::STATUS).unwrap().is_none());

        mm.set(keys::STATUS, "NEW").unwrap();
        assert_eq!(mm.get(keys::STATUS).unwrap().as_deref(), Some("NEW"));

        mm.set(keys::STATUS, "READY_TO_APPLY").unwrap();
        assert_eq!(
            mm.get(keys::STATUS).unwrap().as_deref(),
            Some("READY_TO_APPLY")
        );

        assert!(mm.remove(keys::STATUS).unwrap());
        assert!(mm.get(keys::STATUS).unwrap().is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    fn uuid_hex() -> String {
        // Avoid pulling uuid as a dev-dep just for the test.
        use std::time::{SystemTime, UNIX_EPOCH};
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{n:x}")
    }
}


