//! Reinitialize integration tests.
//!
//! Drives `synclite::reinitialize` end-to-end for all five
//! `DeviceType` values (Sqlite, SqliteStore, DuckDb, DuckDbStore,
//! Streaming) under both `clean_destination={false,true}`, plus a
//! trigger-file variant. After each reinit the test resumes the
//! *normal* initialize → write → flush → await_sync → close flow
//! and confirms the destination receives the new rows. SQLite-backed
//! devices ship to a SQLite destination; DuckDB-backed devices ship
//! to a DuckDB destination.

use std::path::{Path, PathBuf};
use std::time::Duration;

use duckdb::Connection as RawDuck;
use logger_core::DeviceType;
use logger_db_traits::Value;
use rusqlite::Connection as RawSqlite;
use synclite::duckdb::Connection as DkConn;
use synclite::rusqlite::Connection as SlConn;
use synclite::{DestinationOptions, DstSyncMode, DstType, SyncLiteOptions};

#[derive(Copy, Clone, PartialEq, Eq)]
enum Backend {
    Sqlite,
    DuckDb,
}

fn backend_for(dt: DeviceType) -> Backend {
    match dt {
        DeviceType::Sqlite | DeviceType::SqliteStore | DeviceType::Streaming => Backend::Sqlite,
        DeviceType::DuckDb | DeviceType::DuckDbStore => Backend::DuckDb,
    }
}

fn fresh_workspace(tag: &str, dt: DeviceType) -> (PathBuf, PathBuf) {
    let (src_ext, dst_ext) = match backend_for(dt) {
        Backend::Sqlite => ("db", "db"),
        Backend::DuckDb => ("duckdb", "duckdb"),
    };
    let base = std::env::temp_dir()
        .join("synclite-reinit-it")
        .join(format!("{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&base).unwrap();
    let db = base.join(format!("src.{src_ext}"));
    let dst = base.join(format!("dst.{dst_ext}"));
    (db, dst)
}

fn init_session_with_mode(
    dt: DeviceType,
    device_name: &str,
    db: &Path,
    dst: &Path,
    mode: DstSyncMode,
) {
    let (dst_type, conn_str, dst_database) = match backend_for(dt) {
        Backend::Sqlite => (
            DstType::Sqlite,
            format!("jdbc:sqlite:{}", dst.display()),
            None,
        ),
        Backend::DuckDb => (
            DstType::DuckDb,
            format!("jdbc:duckdb:{}", dst.display()),
            Some("main".to_string()),
        ),
    };
    synclite::initialize(
        dt,
        device_name,
        db,
        Some(DestinationOptions {
            dst_type,
            dst_connection_string: conn_str,
            dst_database,
            dst_schema: None,
            dst_sync_mode: mode,
        }),
        SyncLiteOptions::default(),
    )
    .expect("initialize");
}

fn init_session(dt: DeviceType, device_name: &str, db: &Path, dst: &Path) {
    init_session_with_mode(dt, device_name, db, dst, DstSyncMode::Consolidation);
}

fn write_rows(dt: DeviceType, db: &Path, rows: &[(i64, &str)]) {
    match backend_for(dt) {
        Backend::Sqlite => {
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
            synclite::await_sync(db, Duration::from_secs(60)).expect("await_sync");
            conn.close().expect("close");
        }
        Backend::DuckDb => {
            let mut conn = DkConn::open(db).expect("open duckdb");
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
            synclite::await_sync(db, Duration::from_secs(60)).expect("await_sync");
            conn.close().expect("close");
        }
    }
}

fn read_uuid(db: &Path) -> String {
    let meta = PathBuf::from(format!("{}.synclite", db.display())).join(format!(
        "{}.synclite.metadata",
        db.file_name().unwrap().to_string_lossy()
    ));
    let c = RawSqlite::open(&meta).expect("open metadata");
    c.query_row("SELECT value FROM metadata WHERE key='uuid'", [], |r| {
        r.get::<_, String>(0)
    })
    .expect("uuid")
}

fn dst_table_exists(dt: DeviceType, dst: &Path, table: &str) -> bool {
    match backend_for(dt) {
        Backend::Sqlite => RawSqlite::open(dst)
            .expect("open dst")
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |_| Ok(()),
            )
            .is_ok(),
        Backend::DuckDb => RawDuck::open(dst)
            .expect("open dst duck")
            .query_row(
                "SELECT 1 FROM information_schema.tables WHERE table_name=?",
                [table],
                |_| Ok::<(), duckdb::Error>(()),
            )
            .is_ok(),
    }
}

fn dst_count(dt: DeviceType, dst: &Path, table: &str) -> i64 {
    match backend_for(dt) {
        Backend::Sqlite => RawSqlite::open(dst)
            .expect("open dst")
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| {
                r.get::<_, i64>(0)
            })
            .expect("count"),
        Backend::DuckDb => RawDuck::open(dst)
            .expect("open dst duck")
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| {
                r.get::<_, i64>(0)
            })
            .expect("count duck"),
    }
}

/// Seed → reinit(preserve) → re-run normal flow with disjoint ids.
fn run_preserve(dt: DeviceType, name: &str) {
    let (db, dst) = fresh_workspace(&format!("preserve-{name}"), dt);

    init_session(dt, name, &db, &dst);
    write_rows(dt, &db, &[(1, "a"), (2, "b")]);
    let uuid_before = read_uuid(&db);
    assert_eq!(dst_count(dt, &dst, "items"), 2, "{name} initial seed");

    synclite::reinitialize(&db, false).expect("reinitialize");
    assert_eq!(read_uuid(&db), uuid_before, "{name} uuid must survive");
    assert!(dst_table_exists(dt, &dst, "items"), "{name} table preserved");

    init_session(dt, name, &db, &dst);
    write_rows(dt, &db, &[(5, "x"), (6, "y")]);
    let total = dst_count(dt, &dst, "items");
    assert!(
        total >= 4,
        "{name} post-reinit re-seed: expected >=4 rows in dst, got {total}"
    );
}

/// Seed → reinit(clean) in REPLICATION mode → owned user tables gone
/// → re-run normal flow. In replication mode the destination belongs
/// to a single device, so dropping is safe and expected.
fn run_clean(dt: DeviceType, name: &str) {
    let (db, dst) = fresh_workspace(&format!("clean-{name}"), dt);

    init_session_with_mode(dt, name, &db, &dst, DstSyncMode::Replication);
    write_rows(dt, &db, &[(1, "a"), (2, "b")]);
    let uuid_before = read_uuid(&db);

    synclite::reinitialize(&db, true).expect("reinitialize");
    assert_eq!(read_uuid(&db), uuid_before, "{name} uuid must survive");
    assert!(
        !dst_table_exists(dt, &dst, "items"),
        "{name} items must be dropped in replication mode"
    );

    init_session_with_mode(dt, name, &db, &dst, DstSyncMode::Replication);
    write_rows(dt, &db, &[(10, "x"), (11, "y")]);
    assert_eq!(dst_count(dt, &dst, "items"), 2, "{name} clean re-seed");
}

/// In CONSOLIDATION mode the destination is shared across multiple
/// devices, so `clean_destination=true` MUST NOT drop user tables —
/// that would be catastrophic for sibling devices. The metadata
/// rows for this device are still cleaned.
fn run_clean_consolidation_is_noop(dt: DeviceType, name: &str) {
    let (db, dst) = fresh_workspace(&format!("clean-cons-{name}"), dt);

    init_session(dt, name, &db, &dst);
    write_rows(dt, &db, &[(1, "a"), (2, "b")]);

    synclite::reinitialize(&db, true).expect("reinitialize");
    assert!(
        dst_table_exists(dt, &dst, "items"),
        "{name} items must survive a clean reinit in consolidation mode"
    );
}

#[test]
fn sqlite_reinit_preserve() {
    run_preserve(DeviceType::Sqlite, "sqlitedev");
}

#[test]
fn sqlite_reinit_clean() {
    run_clean(DeviceType::Sqlite, "sqlitedev");
}

#[test]
fn sqlitestore_reinit_preserve() {
    run_preserve(DeviceType::SqliteStore, "sqlitestoredev");
}

#[test]
fn sqlitestore_reinit_clean() {
    run_clean(DeviceType::SqliteStore, "sqlitestoredev");
}

#[test]
fn duckdb_reinit_preserve() {
    run_preserve(DeviceType::DuckDb, "duckdbdev");
}

#[test]
fn duckdb_reinit_clean() {
    run_clean(DeviceType::DuckDb, "duckdbdev");
}

#[test]
fn duckdbstore_reinit_preserve() {
    run_preserve(DeviceType::DuckDbStore, "duckdbstoredev");
}

#[test]
fn duckdbstore_reinit_clean() {
    run_clean(DeviceType::DuckDbStore, "duckdbstoredev");
}

#[test]
fn streaming_reinit_preserve() {
    run_preserve(DeviceType::Streaming, "streamingdev");
}

#[test]
fn streaming_reinit_clean() {
    run_clean(DeviceType::Streaming, "streamingdev");
}

#[test]
fn trigger_file_runs_clean_reinit_on_next_initialize() {
    let dt = DeviceType::Sqlite;
    let name = "trigdev";
    let (db, dst) = fresh_workspace("trigger", dt);

    init_session_with_mode(dt, name, &db, &dst, DstSyncMode::Replication);
    write_rows(dt, &db, &[(1, "a"), (2, "b")]);
    let uuid_before = read_uuid(&db);
    assert!(dst_table_exists(dt, &dst, "items"));

    let trigger = db
        .parent()
        .unwrap()
        .join(format!("reinitialize_with_clean_destination.{name}"));
    std::fs::write(&trigger, b"").unwrap();

    init_session_with_mode(dt, name, &db, &dst, DstSyncMode::Replication);
    assert!(!trigger.exists(), "trigger file must be removed");
    assert_eq!(read_uuid(&db), uuid_before);
    assert!(!dst_table_exists(dt, &dst, "items"));

    write_rows(dt, &db, &[(42, "z")]);
    assert_eq!(dst_count(dt, &dst, "items"), 1);
}

#[test]
fn sqlite_reinit_clean_consolidation_is_noop() {
    run_clean_consolidation_is_noop(DeviceType::Sqlite, "sqlitedevcons");
}

/// Disambiguator: drive a DuckDbStore device but ship to a SQLite
/// destination. If user tables show up here we know the gap is
/// "DuckDbStore + DuckDb destination" specifically; if they don't,
/// the gap is on the store-device pipeline regardless of destination.
#[test]
fn duckdbstore_to_sqlite_dst_preserve() {
    let dt = DeviceType::DuckDbStore;
    let name = "duckdbstoredevsql";
    let base = std::env::temp_dir()
        .join("synclite-reinit-it")
        .join(format!("dkstore-to-sql-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&base).unwrap();
    let db = base.join("src.duckdb");
    let dst = base.join("dst.db");

    synclite::initialize(
        dt,
        name,
        &db,
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

    write_rows(dt, &db, &[(1, "a"), (2, "b")]);

    let exists = RawSqlite::open(&dst)
        .expect("open dst sqlite")
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='items'",
            [],
            |_| Ok(()),
        )
        .is_ok();
    assert!(
        exists,
        "DuckDbStore->SQLite: items must materialize on destination"
    );
}
