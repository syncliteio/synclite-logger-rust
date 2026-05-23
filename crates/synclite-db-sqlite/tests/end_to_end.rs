//! End-to-end: open a SQLite device, run a small workload, close it.
//! Asserts the produced log segment captures every mutation, excludes
//! SELECTs, and is marked `READY_TO_APPLY`.

use rusqlite::Connection;
use synclite_core::record::ArgValue;
use synclite_db_sqlite::{SqliteDevice, SqliteDeviceConfig};
use synclite_db_traits::DbDevice;
use tempfile::tempdir;

#[test]
fn write_workload_produces_ready_segment() {
    let dir = tempdir().unwrap();
    let cfg = SqliteDeviceConfig::new(dir.path().join("user.db"), dir.path().join("stage"));

    let mut device: Box<dyn DbDevice> = Box::new(SqliteDevice::open(cfg.clone()).unwrap());

    device
        .execute("CREATE TABLE memories(k TEXT PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    device
        .execute(
            "INSERT INTO memories(k, v) VALUES (?, ?)",
            &[ArgValue::Text("ctx1".into()), ArgValue::Text("hello".into())],
        )
        .unwrap();
    device
        .execute(
            "INSERT INTO memories(k, v) VALUES (?, ?)",
            &[ArgValue::Text("ctx2".into()), ArgValue::Text("world".into())],
        )
        .unwrap();
    let rows = device.query("SELECT v FROM memories ORDER BY k", &[]).unwrap();
    assert_eq!(rows.len(), 2);

    device.commit().unwrap();
    device.close().unwrap();

    // User DB has the two rows.
    let user = Connection::open(cfg.db_path).unwrap();
    let n: i64 = user
        .query_row("SELECT count(*) FROM memories", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2);

    // Log segment 0 was finalized.
    let seg_path = cfg.segment_dir.join("0.sqllog");
    let seg = Connection::open(&seg_path).unwrap();

    let status: String = seg
        .query_row("SELECT value FROM metadata WHERE key='status'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(status, "READY_TO_APPLY");

    let logged: i64 = seg
        .query_row("SELECT count(*) FROM commandlog", [], |r| r.get(0))
        .unwrap();
    assert_eq!(logged, 3);

    let (a1, a2): (String, String) = seg
        .query_row(
            "SELECT arg1, arg2 FROM commandlog WHERE change_number = 2",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((a1.as_str(), a2.as_str()), ("ctx2", "world"));
}
