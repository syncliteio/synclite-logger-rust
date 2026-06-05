//! Integration tests for sync_status / sync_statistics / sync_latency.

use std::path::{Path, PathBuf};
use std::time::Duration;

use logger_core::DeviceType;
use logger_db_traits::Value;
use synclite::rusqlite::Connection as SlConn;
use synclite::{
    DestinationOptions, DstSyncMode, DstType, SyncLiteOptions, SyncState,
};

fn fresh_workspace(tag: &str) -> (PathBuf, PathBuf) {
    let base = std::env::temp_dir()
        .join("synclite-status-it")
        .join(format!("{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&base).unwrap();
    (base.join("src.db"), base.join("dst.db"))
}

fn init_session(name: &str, db: &Path, dst: &Path) {
    synclite::initialize(
        DeviceType::Sqlite,
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

#[test]
fn sync_status_reports_not_initialized_before_first_init() {
    let (db, _dst) = fresh_workspace("not-init");
    let st = synclite::sync_status(&db).expect("status");
    assert_eq!(st.state, SyncState::NotInitialized);
}

#[test]
fn sync_status_running_then_paused_then_running() {
    let (db, dst) = fresh_workspace("state-flip");
    let name = "statusdev";
    init_session(name, &db, &dst);
    write_rows(&db, &[(1, "a")]);
    synclite::await_sync(&db, Duration::from_secs(60)).expect("seed sync");

    let st = synclite::sync_status(&db).expect("status");
    assert_eq!(st.state, SyncState::Running, "should be running by default");

    synclite::pause_sync(&db).expect("pause");
    let st = synclite::sync_status(&db).expect("status");
    assert_eq!(st.state, SyncState::Paused);

    synclite::resume_sync(&db).expect("resume");
    let st = synclite::sync_status(&db).expect("status");
    assert_eq!(st.state, SyncState::Running);
}

#[test]
fn sync_statistics_reflect_applied_segments() {
    let (db, dst) = fresh_workspace("stats");
    let name = "statsdev";
    init_session(name, &db, &dst);
    write_rows(&db, &[(1, "a"), (2, "b"), (3, "c")]);
    synclite::await_sync(&db, Duration::from_secs(60)).expect("sync");

    let s = synclite::sync_statistics(&db).expect("stats");
    assert!(
        s.log_segments_applied >= 1,
        "expected at least one segment applied, got {}",
        s.log_segments_applied
    );
    assert!(
        s.processed_oper_count >= 3,
        "expected at least 3 ops applied, got {}",
        s.processed_oper_count
    );
    assert!(
        s.last_consolidated_commit_id > 0,
        "last_consolidated_commit_id should be set after sync"
    );
    assert!(
        s.last_heartbeat_time_ms > 0,
        "last_heartbeat_time_ms should be set after sync"
    );
}

#[test]
fn sync_latency_zero_after_drain() {
    let (db, dst) = fresh_workspace("latency");
    let name = "latencydev";
    init_session(name, &db, &dst);
    write_rows(&db, &[(1, "a"), (2, "b")]);
    synclite::await_sync(&db, Duration::from_secs(60)).expect("sync");

    let l = synclite::sync_latency(&db).expect("latency");
    assert!(l.source_commit_id > 0, "source_commit_id should be set");
    assert!(
        l.applied_commit_id.is_some(),
        "applied_commit_id must be known after a successful drain"
    );
    // After await_sync source == applied, so latency_ms == 0.
    assert_eq!(
        l.latency_ms, 0,
        "drained device should report zero latency, got {}",
        l.latency_ms
    );
}
