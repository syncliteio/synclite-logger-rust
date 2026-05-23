//! End-to-end DuckDB test. Avoids the `tempfile` crate because its
//! `rustix`/`fastrand` chain conflicts with `libduckdb-sys` on Windows
//! (unresolved `Rm*` Restart Manager symbols at link time).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection as SqliteConn;
use synclite_core::record::ArgValue;
use synclite_db_duckdb::{DuckDbDevice, DuckDbDeviceConfig};
use synclite_db_traits::DbDevice;

/// Tiny RAII temp dir replacement.
struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = N.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("synclite-duckdb-{tag}-{nanos:x}-{n}"));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn duckdb_writes_user_db_and_two_segments() {
    let dir = TempDir::new("e2e");
    let cfg = DuckDbDeviceConfig::new(dir.path().join("user.duckdb"), dir.path().join("stage"));

    let mut device = DuckDbDevice::open(cfg.clone()).unwrap();

    // Segment 0: DDL + insert
    device
        .execute(
            "CREATE TABLE metrics(ts BIGINT, name TEXT, value DOUBLE)",
            &[],
        )
        .unwrap();
    device
        .execute(
            "INSERT INTO metrics VALUES (?, ?, ?)",
            &[
                ArgValue::Int(1000),
                ArgValue::Text("cpu".into()),
                ArgValue::Real(0.75),
            ],
        )
        .unwrap();
    device.roll_segment().unwrap();

    // Segment 1: one more insert
    device
        .execute(
            "INSERT INTO metrics VALUES (?, ?, ?)",
            &[
                ArgValue::Int(2000),
                ArgValue::Text("mem".into()),
                ArgValue::Real(0.42),
            ],
        )
        .unwrap();

    // Query path — must NOT be logged.
    let rows = device
        .query("SELECT count(*) FROM metrics", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], ArgValue::Int(2));

    Box::new(device).close().unwrap();

    // ---- verify user DuckDB ----
    let user = duckdb::Connection::open(cfg.db_path).unwrap();
    let n: i64 = user
        .query_row("SELECT count(*) FROM metrics", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2);

    // ---- verify segment 0 (SQLite) ----
    let s0 = SqliteConn::open(cfg.segment_dir.join("commandlog-0.db")).unwrap();
    let status: String = s0
        .query_row("SELECT value FROM metadata WHERE key='status'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(status, "READY_TO_APPLY");
    let c0: i64 = s0
        .query_row("SELECT count(*) FROM commandlog", [], |r| r.get(0))
        .unwrap();
    assert_eq!(c0, 2);

    // ---- verify segment 1 (SQLite) ----
    let s1 = SqliteConn::open(cfg.segment_dir.join("commandlog-1.db")).unwrap();
    let status: String = s1
        .query_row("SELECT value FROM metadata WHERE key='status'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(status, "READY_TO_APPLY");
    let c1: i64 = s1
        .query_row("SELECT count(*) FROM commandlog", [], |r| r.get(0))
        .unwrap();
    assert_eq!(c1, 1);
}
