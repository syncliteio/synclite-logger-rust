//! Java-style commit-boundary segment auto-switch for the SQLite device.
//!
//! Verifies the (previously dead) `log-segment-switch-log-count-threshold`
//! and `log-segment-switch-duration-threshold-ms` options now drive
//! automatic segment rotation, and that rotation only ever happens on a
//! commit boundary (a transaction is never split across two segments).

use std::path::Path;

use logger_core::record::ArgValue;
use logger_db_sqlite::{SqliteDevice, SqliteDeviceConfig};
use logger_db_traits::DbDevice;
use rusqlite::Connection;
use tempfile::tempdir;

/// Every finalized log segment must end on a commit boundary: the record
/// with the largest `change_number` in a non-empty segment is a COMMIT.
fn assert_commit_boundaries(segment_dir: &Path) -> usize {
    let mut checked = 0;
    for entry in std::fs::read_dir(segment_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("sqllog") {
            continue;
        }
        let seg = Connection::open(&path).unwrap();
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

fn segment_count(segment_dir: &Path) -> usize {
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
    let dir = tempdir().unwrap();
    let mut cfg =
        SqliteDeviceConfig::new(dir.path().join("user.db"), dir.path().join("stage"));
    // Rotate aggressively on record count; disable time-based switching so
    // the test is deterministic.
    cfg.log_segment_switch_log_count_threshold = Some(4);
    cfg.log_segment_switch_duration_threshold_ms = None;

    let mut device: Box<dyn DbDevice> = Box::new(SqliteDevice::open(cfg.clone()).unwrap());

    device
        .execute("CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    device.commit().unwrap();

    for i in 0..20 {
        device
            .execute(
                "INSERT INTO t(k, v) VALUES (?, ?)",
                &[ArgValue::Int(i), ArgValue::Text(format!("v{i}"))],
            )
            .unwrap();
        device.commit().unwrap();
    }
    device.close().unwrap();

    // Multiple segments must have been produced by count-based rotation.
    assert!(
        segment_count(&cfg.segment_dir) > 1,
        "expected count-based rotation to produce multiple segments"
    );
    // And every one of them ends on a commit boundary.
    let checked = assert_commit_boundaries(&cfg.segment_dir);
    assert!(checked > 1, "expected several non-empty segments");

    // All 21 rows are present in the user DB.
    let user = Connection::open(&cfg.db_path).unwrap();
    let n: i64 = user
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 20);
}

#[test]
fn duration_threshold_rotates_on_commit_boundary() {
    let dir = tempdir().unwrap();
    let mut cfg =
        SqliteDeviceConfig::new(dir.path().join("user.db"), dir.path().join("stage"));
    // Rotate on age; disable count-based switching.
    cfg.log_segment_switch_log_count_threshold = None;
    cfg.log_segment_switch_duration_threshold_ms = Some(1);

    let mut device: Box<dyn DbDevice> = Box::new(SqliteDevice::open(cfg.clone()).unwrap());

    device
        .execute("CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    device.commit().unwrap();

    for i in 0..5 {
        // Ensure the active segment ages past the 1ms threshold.
        std::thread::sleep(std::time::Duration::from_millis(5));
        device
            .execute(
                "INSERT INTO t(k, v) VALUES (?, ?)",
                &[ArgValue::Int(i), ArgValue::Text(format!("v{i}"))],
            )
            .unwrap();
        device.commit().unwrap();
    }
    device.close().unwrap();

    assert!(
        segment_count(&cfg.segment_dir) > 1,
        "expected time-based rotation to produce multiple segments"
    );
    assert_commit_boundaries(&cfg.segment_dir);
}

#[test]
fn disabled_thresholds_keep_single_segment() {
    let dir = tempdir().unwrap();
    let mut cfg =
        SqliteDeviceConfig::new(dir.path().join("user.db"), dir.path().join("stage"));
    // Both switches off: no auto-rotation regardless of workload.
    cfg.log_segment_switch_log_count_threshold = None;
    cfg.log_segment_switch_duration_threshold_ms = None;

    let mut device: Box<dyn DbDevice> = Box::new(SqliteDevice::open(cfg.clone()).unwrap());

    device
        .execute("CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    device.commit().unwrap();
    for i in 0..50 {
        device
            .execute(
                "INSERT INTO t(k, v) VALUES (?, ?)",
                &[ArgValue::Int(i), ArgValue::Text(format!("v{i}"))],
            )
            .unwrap();
        device.commit().unwrap();
    }
    device.close().unwrap();

    assert_eq!(
        segment_count(&cfg.segment_dir),
        1,
        "no rotation expected when both thresholds are disabled"
    );
}
