//! Per-device metadata SQLite file.
//!
//! Schema: `metadata(key TEXT PRIMARY KEY, value TEXT)`. Mirrors the
//! `MetadataManager` table from `synclite-logger-java` so the file is
//! readable by Java consolidators if anyone ever cross-checks.
//!
//! Known keys used by this port:
//!
//! - `uuid` — UUIDv4 string, generated on first open.
//! - `device_type` — `SQLITE` or `DUCKDB`.
//! - `database_id` — informational; defaults to `0`.
//! - `database_name` — basename of the device DB file.
//! - `log_segment_sequence_number` — last seq used (`-1` when none).
//! - `backup_taken` — `1` once the data backup has been written locally.
//! - `backup_shipped` — `1` once the backup + metadata copy reach the
//!   stage subdir.

use std::path::Path;

use rusqlite::{params, Connection};
use logger_core::{Error, Result};

/// Wrapper around a `metadata` SQLite file.
pub struct Metadata {
    conn: Connection,
}

impl Metadata {
    /// Open (creating if needed) the metadata file at `path`. Ensures the
    /// `metadata` table exists.
    pub fn open_or_create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path).map_err(map_err)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT);",
        )
        .map_err(map_err)?;
        Ok(Self { conn })
    }

    /// Lookup a key.
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        match self
            .conn
            .query_row("SELECT value FROM metadata WHERE key=?1", [key], |r| {
                r.get::<_, String>(0)
            }) {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }

    /// Read a signed-integer property (returns `None` if missing or
    /// unparseable).
    pub fn get_i64(&self, key: &str) -> Result<Option<i64>> {
        Ok(self.get(key)?.and_then(|s| s.parse::<i64>().ok()))
    }

    /// Insert or update a key/value pair.
    pub fn put(&self, key: &str, value: &str) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO metadata(key,value) VALUES(?1,?2) \
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![key, value],
            )
            .map_err(map_err)?;
        Ok(())
    }

    /// Convenience: store an i64.
    pub fn put_i64(&self, key: &str, value: i64) -> Result<()> {
        self.put(key, &value.to_string())
    }
}

fn map_err(e: rusqlite::Error) -> Error {
    Error::Config(format!("metadata: {e}"))
}


