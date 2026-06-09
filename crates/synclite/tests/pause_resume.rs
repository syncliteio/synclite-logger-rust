//! Pause / resume sync integration tests.
//!
//! Verifies that `synclite::pause_sync` halts shipping +
//! consolidation while the logger keeps writing locally, and that
//! `synclite::resume_sync` drains everything queued during the pause.
//! Trigger-file variant is covered too.

use std::path::{Path, PathBuf};
use std::time::Duration;

use logger_core::DeviceType;
use logger_db_traits::Value;
use rusqlite::Connection as RawSqlite;
use synclite::pause::{TRIGGER_PAUSE, TRIGGER_RESUME};
use synclite::rusqlite::Connection as SlConn;
use synclite::{DestinationOptions, DstSyncMode, DstType, SyncLiteOptions};

fn fresh_workspace(tag: &str) -> (PathBuf, PathBuf) {
    let base = std::env::temp_dir()
        .join("synclite-pause-it")
        .join(format!("{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&base).unwrap();
    (base.join("src.db"), base.join("dst.db"))
}

fn init_session(name: &str, db: &Path, dst: &Path) {
    synclite::initialize(
        DeviceType::SQLITE,
        name,
        db,
        Some(DestinationOptions {
            dst_type: DstType::Sqlite,
            dst_connection_string: format!("jdbc:sqlite:{}", dst.display()),
            dst_database: None,
            dst_schema: None,
            dst_sync_mode: DstSyncMode::Consolidation,
        }),
        SyncLiteOptions::default(),
    )
    .expect("initialize");
}

fn write_rows(db: &Path, rows: &[(i64, &str)]) {
    let mut conn = SlConn::open(db).expect("open sqlite");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .expect("create items");
    for (id, name) in rows {
        conn.execute(
            "INSERT INTO items(id, name) VALUES (?, ?)",
            &[Value::Int(*id), Value::Text((*name).to_string())],
        )
        .expect("insert");
    }
    conn.flush().expect("flush");
    conn.close().expect("close");
}

/// Like `write_rows` but operates on an already-open connection so the
/// caller can keep the logger + consolidator alive across pause/resume.
fn write_rows_on(conn: &mut SlConn, rows: &[(i64, &str)]) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .expect("create items");
    for (id, name) in rows {
        conn.execute(
            "INSERT INTO items(id, name) VALUES (?, ?)",
            &[Value::Int(*id), Value::Text((*name).to_string())],
        )
        .expect("insert");
    }
    conn.flush().expect("flush");
}

fn dst_count(dst: &Path, table: &str) -> i64 {
    if !dst.exists() {
        return 0;
    }
    let c = RawSqlite::open(dst).expect("open dst");
    c.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| {
        r.get::<_, i64>(0)
    })
    .unwrap_or(0)
}

fn dst_table_exists(dst: &Path, table: &str) -> bool {
    if !dst.exists() {
        return false;
    }
    RawSqlite::open(dst)
        .expect("open dst")
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
            [table],
            |_| Ok(()),
        )
        .is_ok()
}

#[test]
fn pause_blocks_destination_apply_resume_drains() {
    let (db, dst) = fresh_workspace("pause-drain");
    let name = "pausedev";

    init_session(name, &db, &dst);
    write_rows(&db, &[(1, "a"), (2, "b")]);
    synclite::await_sync(&db, Duration::from_secs(60)).expect("seed sync");
    assert_eq!(dst_count(&dst, "items"), 2, "seed must land on dst");

    // Keep a single connection open across pause/resume so the
    // logger + consolidator stay alive. Otherwise close() would tear
    // down the consolidator (and its in-memory pause buffer) before
    // we ever call resume_sync.
    let mut conn = SlConn::open(&db).expect("open sqlite long-lived");

    // Pause and write more. Writes should land in local segments but
    // not on the destination.
    synclite::pause_sync(&db).expect("pause");
    assert!(
        synclite::is_sync_paused(&db).expect("is_paused"),
        "device must report paused"
    );

    write_rows_on(&mut conn, &[(3, "c"), (4, "d"), (5, "e")]);
    // Give the pipeline a moment to confirm nothing leaks past the
    // pause gate while still paused.
    std::thread::sleep(Duration::from_millis(2_000));
    assert_eq!(
        dst_count(&dst, "items"),
        2,
        "paused writes must not reach dst"
    );

    // Resume; the queued segments must drain.
    synclite::resume_sync(&db).expect("resume");
    assert!(
        !synclite::is_sync_paused(&db).expect("is_paused"),
        "device must report unpaused"
    );
    synclite::await_sync(&db, Duration::from_secs(60)).expect("resume sync");
    assert_eq!(
        dst_count(&dst, "items"),
        5,
        "resume must drain queued segments"
    );
    conn.close().expect("close");
}

#[test]
fn pause_resume_idempotent() {
    let (db, dst) = fresh_workspace("idempotent");
    let name = "pauseidem";

    init_session(name, &db, &dst);
    write_rows(&db, &[(1, "a")]);
    synclite::await_sync(&db, Duration::from_secs(60)).expect("seed sync");

    synclite::pause_sync(&db).expect("pause 1");
    synclite::pause_sync(&db).expect("pause 2 (idempotent)");
    assert!(synclite::is_sync_paused(&db).expect("paused"));

    synclite::resume_sync(&db).expect("resume 1");
    synclite::resume_sync(&db).expect("resume 2 (idempotent)");
    assert!(!synclite::is_sync_paused(&db).expect("unpaused"));
}

#[test]
fn pause_trigger_files_consumed_by_initialize() {
    let (db, dst) = fresh_workspace("trigger");
    let name = "pausetrigger";

    init_session(name, &db, &dst);
    write_rows(&db, &[(1, "a")]);
    synclite::await_sync(&db, Duration::from_secs(60)).expect("seed sync");

    // Drop a pause trigger file alongside the db.
    let parent = db.parent().unwrap();
    let pause_trigger = parent.join(format!("{TRIGGER_PAUSE}.{name}"));
    std::fs::write(&pause_trigger, b"").expect("write pause trigger");
    init_session(name, &db, &dst);
    assert!(
        !pause_trigger.exists(),
        "pause trigger must be removed by initialize"
    );
    assert!(
        synclite::is_sync_paused(&db).expect("paused"),
        "pause trigger must set paused state"
    );

    // Now drop a resume trigger.
    let resume_trigger = parent.join(format!("{TRIGGER_RESUME}.{name}"));
    std::fs::write(&resume_trigger, b"").expect("write resume trigger");
    init_session(name, &db, &dst);
    assert!(
        !resume_trigger.exists(),
        "resume trigger must be removed by initialize"
    );
    assert!(
        !synclite::is_sync_paused(&db).expect("unpaused"),
        "resume trigger must clear paused state"
    );

    // Destination table should still exist and survive the trigger
    // round-trip.
    assert!(dst_table_exists(&dst, "items"), "items table must survive");
}
