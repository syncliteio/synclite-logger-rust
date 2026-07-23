//! Java-style commit-boundary segment auto-switch for the DuckDB device.
//!
//! Mirrors the SQLite rotation test. Avoids the `tempfile` crate for the
//! same link-time reason as the DuckDB end-to-end test.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use logger_core::record::ArgValue;
use logger_db_duckdb::{DuckDbDevice, DuckDbDeviceConfig};
use logger_db_traits::DbDevice;
use rusqlite::Connection as SqliteConn;

/// Tiny RAII temp dir replacement (see end_to_end.rs for rationale).
struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = N.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("synclite-duckdb-rot-{tag}-{nanos:x}-{n}"));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Every finalized main segment must end on a commit boundary. For DuckDB,
/// transaction bodies live in per-txn `.txn` sidecars while the main
/// `.sqllog` carries the COMMIT markers, so a non-empty main segment ends
/// with a COMMIT record.
fn assert_commit_boundaries(segment_dir: &Path) -> usize {
    let mut checked = 0;
    for entry in std::fs::read_dir(segment_dir).unwrap() {
        let path = entry.unwrap().path();
        // Only main segments: `<seq>.sqllog` (exclude `.txn` sidecars).
        if path.extension().and_then(|e| e.to_str()) != Some("sqllog") {
            continue;
        }
        let seg = SqliteConn::open(&path).unwrap();
        let count: i64 = seg
            .query_row("SELECT count(*) FROM commandlog", [], |r| r.get(0))
            .unwrap();
        if count == 0 {
            continue;
        }
        let last_sql: String = seg
            .query_row(
                "SELECT sql FROM commandlog ORDER BY change_number DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            last_sql, "COMMIT",
            "segment {:?} does not end on a commit boundary",
            path
        );
        checked += 1;
    }
    checked
}

fn main_segment_count(segment_dir: &Path) -> usize {
    std::fs::read_dir(segment_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path().extension().and_then(|x| x.to_str()) == Some("sqllog")
        })
        .count()
}

#[test]
fn count_threshold_rotates_on_commit_boundary() {
    let dir = TempDir::new("count");
    let mut cfg =
        DuckDbDeviceConfig::new(dir.path().join("user.duckdb"), dir.path().join("stage"));
    cfg.log_segment_switch_log_count_threshold = Some(2);
    cfg.log_segment_switch_duration_threshold_ms = None;

    let mut device: Box<dyn DbDevice> = Box::new(DuckDbDevice::open(cfg.clone()).unwrap());

    device
        .execute("CREATE TABLE t(k BIGINT, v TEXT)", &[])
        .unwrap();
    device.commit().unwrap();

    for i in 0..20 {
        device
            .execute(
                "INSERT INTO t VALUES (?, ?)",
                &[ArgValue::Int(i), ArgValue::Text(format!("v{i}"))],
            )
            .unwrap();
        device.commit().unwrap();
    }
    device.close().unwrap();

    assert!(
        main_segment_count(&cfg.segment_dir) > 1,
        "expected count-based rotation to produce multiple segments"
    );
    assert_commit_boundaries(&cfg.segment_dir);

    let user = duckdb::Connection::open(&cfg.db_path).unwrap();
    let n: i64 = user
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 20);
}

#[test]
fn duration_threshold_rotates_on_commit_boundary() {
    let dir = TempDir::new("time");
    let mut cfg =
        DuckDbDeviceConfig::new(dir.path().join("user.duckdb"), dir.path().join("stage"));
    cfg.log_segment_switch_log_count_threshold = None;
    cfg.log_segment_switch_duration_threshold_ms = Some(1);

    let mut device: Box<dyn DbDevice> = Box::new(DuckDbDevice::open(cfg.clone()).unwrap());

    device
        .execute("CREATE TABLE t(k BIGINT, v TEXT)", &[])
        .unwrap();
    device.commit().unwrap();

    for i in 0..5 {
        std::thread::sleep(std::time::Duration::from_millis(5));
        device
            .execute(
                "INSERT INTO t VALUES (?, ?)",
                &[ArgValue::Int(i), ArgValue::Text(format!("v{i}"))],
            )
            .unwrap();
        device.commit().unwrap();
    }
    device.close().unwrap();

    assert!(
        main_segment_count(&cfg.segment_dir) > 1,
        "expected time-based rotation to produce multiple segments"
    );
    assert_commit_boundaries(&cfg.segment_dir);
}
