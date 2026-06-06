//! End-to-end test: a SQLite device produces two segments via
//! `roll_segment` + `close`; the shipper carries both to a destination
//! directory through an `FsArchiver`.

use std::sync::Arc;

use rusqlite::Connection;
use logger_archiver::{Archiver, FsArchiver};
use logger_core::record::ArgValue;
use logger_db_sqlite::{SegmentReadyCallback, SqliteDevice, SqliteDeviceConfig};
use logger_db_traits::DbDevice;
use logger_shipper::LogShipper;
use tempfile::tempdir;

#[test]
fn device_rolls_two_segments_and_both_are_shipped() {
    let work = tempdir().unwrap();
    let dst = tempdir().unwrap();

    let archiver: Arc<dyn Archiver> = Arc::new(FsArchiver::new(dst.path()));
    let shipper = Arc::new(LogShipper::spawn(vec![archiver]).unwrap());

    // Wire the device's "segment ready" hook into the shipper.
    let shipper_for_cb = shipper.clone();
    let cb: SegmentReadyCallback = Arc::new(move |path| {
        shipper_for_cb.submit(path).unwrap();
    });

    let mut cfg = SqliteDeviceConfig::new(work.path().join("user.db"), work.path().join("stage"));
    cfg.on_segment_ready = Some(cb);

    let mut device = SqliteDevice::open(cfg).unwrap();

    // Segment 0
    device.execute("CREATE TABLE t(x INTEGER)", &[]).unwrap();
    device
        .execute("INSERT INTO t(x) VALUES (?)", &[ArgValue::Int(1)])
        .unwrap();
    device.commit().unwrap();
    // Roll → segment 0 finalized + submitted to shipper.
    device.roll_segment().unwrap();

    // Segment 1
    device
        .execute("INSERT INTO t(x) VALUES (?)", &[ArgValue::Int(2)])
        .unwrap();
    device.commit().unwrap();
    Box::new(device).close().unwrap();

    // Stop the shipper so we know the queue is drained before we assert.
    Arc::try_unwrap(shipper)
        .ok()
        .expect("shipper still referenced")
        .shutdown()
        .unwrap();

    // Both segments landed in the destination dir.
    let s0 = dst.path().join("0.sqllog");
    let s1 = dst.path().join("1.sqllog");
    assert!(s0.exists(), "segment 0 missing: {}", s0.display());
    assert!(s1.exists(), "segment 1 missing: {}", s1.display());

    // Segment 0 carries BEGIN + DDL + INSERT + COMMIT.
    // Segment 1 carries BEGIN + INSERT + COMMIT.
    let c0: i64 = Connection::open(&s0)
        .unwrap()
        .query_row("SELECT count(*) FROM commandlog", [], |r| r.get(0))
        .unwrap();
    let c1: i64 = Connection::open(&s1)
        .unwrap()
        .query_row("SELECT count(*) FROM commandlog", [], |r| r.get(0))
        .unwrap();
    assert_eq!(c0, 4);
    assert_eq!(c1, 3);
}


