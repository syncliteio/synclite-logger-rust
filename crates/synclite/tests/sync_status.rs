//! Integration tests for sync_status / sync_statistics / sync_latency.

use std::path::{Path, PathBuf};
use std::time::Duration;

use logger_core::DeviceType;
use logger_db_traits::Value;
use synclite::rusqlite::Connection as SlConn;
use synclite::{DestinationOptions, DstSyncMode, DstType, SyncLiteOptions, SyncState};

fn fresh_workspace(tag: &str) -> (PathBuf, PathBuf) {
    let base = std::env::temp_dir()
        .join("synclite-status-it")
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

#[test]
fn sync_status_reports_not_initialized_before_first_init() {
    let (db, _dst) = fresh_workspace("not-init");
    let st = synclite::sync_status(&db).expect("status");
    assert_eq!(st.state, SyncState::NotInitialized);
}

#[test]
#[ignore = "requires SYNCLITE_TEST_POSTGRES_URL pointing to an isolated test server"]
fn postgres_initialize_reopen_await_uses_one_worker_and_one_snapshot() {
    let (db, _) = fresh_workspace("postgres-reopen");
    let root = db.parent().unwrap();
    let work = root.join("work");
    let schema = format!("synclite_facade_{}", uuid::Uuid::new_v4().simple());
    let url = std::env::var("SYNCLITE_TEST_POSTGRES_URL").expect("test PostgreSQL URL");
    let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    let database: String = client.query_one("SELECT current_database()", &[]).unwrap().get(0);
    let mut config = logger_config::SyncLiteConfig::default();
    config.local_stage_dir = Some(root.join("stage"));
    config.extra.insert(synclite::DEVICE_DATA_ROOT_KEY.into(), work.to_string_lossy().into_owned());
    config.extra.insert("device-polling-interval-ms".into(), "20".into());
    synclite::initialize(DeviceType::SQLITE, "pgreopen", &db, Some(DestinationOptions {
        dst_type: DstType::Postgres,
        dst_connection_string: url.clone(),
        dst_database: Some(database),
        dst_schema: Some(schema.clone()),
        dst_sync_mode: DstSyncMode::Replication,
    }), SyncLiteOptions { config: Some(config), ..SyncLiteOptions::default() }).unwrap();
    write_rows(&db, &[(1, "first"), (2, "second")]);
    synclite::await_sync(&db, Duration::from_secs(30)).expect("first PostgreSQL sync");
    write_rows(&db, &[(3, "after reopen")]);
    synclite::await_sync(&db, Duration::from_secs(30)).expect("reopened PostgreSQL sync");
    let latency = synclite::sync_latency(&db).unwrap();
    assert_eq!(latency.latency_ms, 0);
    assert_eq!(client.query_one(&format!("SELECT COUNT(*) FROM {schema}.items"), &[]).unwrap().get::<_, i64>(0), 3);
    let trace_path = std::fs::read_dir(&work).unwrap().filter_map(Result::ok)
        .map(|entry| entry.path().join("synclite_device.trace")).find(|path| path.is_file()).expect("worker trace");
    let trace = std::fs::read_to_string(&trace_path).unwrap();
    assert_eq!(trace.matches("Worker started/bootstrap pending").count(), 1, "{trace}");
    assert_eq!(trace.matches("Starting to consolidate device snapshot :").count(), 1, "{trace}");
    assert_eq!(trace.matches("Finished consolidating device snapshot :").count(), 1, "{trace}");
    assert_eq!(trace.matches("Device status : INITIALIZED").count(), 1, "{trace}");
    assert!(!trace.contains("step="), "INFO must not contain polling steps: {trace}");
    assert!(!trace.contains("ALTER TABLE"), "fresh schema must not be promoted: {trace}");
    assert!(!trace.contains("ERROR"), "{trace}");
    println!("Trace analysis: initialize + two open/close/await cycles = one worker, one committed snapshot, three destination rows, zero pending latency.");
    println!("Trace retained at {}", trace_path.display());
    for line in trace.lines().filter(|line| line.contains("DeviceDstInitializer") || line.contains("Worker started") || line.contains("Device status :")) {
        println!("{line}");
    }
    client.batch_execute(&format!("DROP SCHEMA {schema} CASCADE")).unwrap();
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

    // `await_sync` follows checkpoint progress; device_status counters
    // can lag slightly behind checkpoint updates.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut s = synclite::sync_statistics(&db).expect("stats");
    while std::time::Instant::now() < deadline
        && (s.processed_oper_count < 3 || s.last_consolidated_commit_id <= 0)
    {
        std::thread::sleep(Duration::from_millis(50));
        s = synclite::sync_statistics(&db).expect("stats");
    }

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
