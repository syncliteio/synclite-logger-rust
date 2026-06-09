//! Reinitialize integration tests.
//!
//! Drives `synclite::reinitialize` end-to-end for all five
//! `DeviceType` values under both `DstSyncMode::Replication` and
//! `DstSyncMode::Consolidation`. Reinit wipes synclite footprint and
//! drops a `.reinit` sentinel that forces the next initialize to
//! re-seed with `dst-object-init-mode-1=OVERWRITE_OBJECT` —
//! REPLICATION drops and recreates destination tables, CONSOLIDATION
//! truncates this device's rows. Either way the post-reinit re-seed
//! is duplicate-free. Each test seeds, reinits, then resumes the
//! normal initialize → write → flush → await_sync → close flow and
//! confirms the destination contains exactly the source rows.
//! SQLite-backed devices ship to SQLite; DuckDB-backed devices ship
//! to DuckDB.

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
        DeviceType::SQLITE | DeviceType::SQLITE_STORE | DeviceType::STREAMING => Backend::Sqlite,
        DeviceType::DUCKDB | DeviceType::DUCKDB_STORE => Backend::DuckDb,
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

/// Seed (2 rows) → reinit → re-init → write (2 more rows). The
/// user's source DB still holds the original 2 rows, so the
/// post-reinit re-seed pushes them through the initial backup again
/// (`OVERWRITE_OBJECT` ensures no duplicates against any residue),
/// then the 2 new writes flow over CDC. Final destination row count
/// is the full 4-row source snapshot in both modes.
fn run_reinit(dt: DeviceType, name: &str, mode: DstSyncMode) {
    let (db, dst) = fresh_workspace(&format!("reinit-{name}"), dt);

    init_session_with_mode(dt, name, &db, &dst, mode);
    write_rows(dt, &db, &[(1, "a"), (2, "b")]);
    let uuid_before = read_uuid(&db);
    assert_eq!(dst_count(dt, &dst, "items"), 2, "{name} initial seed");

    synclite::reinitialize(&db).expect("reinitialize");
    assert_eq!(read_uuid(&db), uuid_before, "{name} uuid must survive");

    init_session_with_mode(dt, name, &db, &dst, mode);
    write_rows(dt, &db, &[(5, "x"), (6, "y")]);
    assert!(
        dst_table_exists(dt, &dst, "items"),
        "{name} items must be present after re-seed"
    );
    assert_eq!(
        dst_count(dt, &dst, "items"),
        4,
        "{name} post-reinit re-seed should land exactly the 4-row source snapshot"
    );
}

#[test]
fn sqlite_reinit_consolidation() {
    run_reinit(DeviceType::SQLITE, "sqlitedev", DstSyncMode::Consolidation);
}

#[test]
fn sqlite_reinit_replication() {
    run_reinit(
        DeviceType::SQLITE,
        "sqlitedevrep",
        DstSyncMode::Replication,
    );
}

#[test]
fn sqlitestore_reinit_consolidation() {
    run_reinit(
        DeviceType::SQLITE_STORE,
        "sqlitestoredev",
        DstSyncMode::Consolidation,
    );
}

#[test]
fn sqlitestore_reinit_replication() {
    run_reinit(
        DeviceType::SQLITE_STORE,
        "sqlitestoredevrep",
        DstSyncMode::Replication,
    );
}

#[test]
fn duckdb_reinit_consolidation() {
    run_reinit(DeviceType::DUCKDB, "duckdbdev", DstSyncMode::Consolidation);
}

#[test]
fn duckdb_reinit_replication() {
    run_reinit(
        DeviceType::DUCKDB,
        "duckdbdevrep",
        DstSyncMode::Replication,
    );
}

#[test]
fn duckdbstore_reinit_consolidation() {
    run_reinit(
        DeviceType::DUCKDB_STORE,
        "duckdbstoredev",
        DstSyncMode::Consolidation,
    );
}

#[test]
fn duckdbstore_reinit_replication() {
    run_reinit(
        DeviceType::DUCKDB_STORE,
        "duckdbstoredevrep",
        DstSyncMode::Replication,
    );
}

#[test]
fn streaming_reinit_consolidation() {
    run_reinit(
        DeviceType::STREAMING,
        "streamingdev",
        DstSyncMode::Consolidation,
    );
}

#[test]
fn streaming_reinit_replication() {
    run_reinit(
        DeviceType::STREAMING,
        "streamingdevrep",
        DstSyncMode::Replication,
    );
}

/// A trigger file `reinitialize.<device-name>` dropped alongside the
/// device DB must cause the next `synclite::initialize` to fire
/// reinit before bringing the logger up, then remove the trigger.
#[test]
fn trigger_file_runs_reinit_on_next_initialize() {
    let dt = DeviceType::SQLITE;
    let name = "trigdev";
    let (db, dst) = fresh_workspace("trigger", dt);

    init_session_with_mode(dt, name, &db, &dst, DstSyncMode::Replication);
    write_rows(dt, &db, &[(1, "a"), (2, "b")]);
    let uuid_before = read_uuid(&db);
    assert!(dst_table_exists(dt, &dst, "items"));

    let trigger = db.parent().unwrap().join(format!("reinitialize.{name}"));
    std::fs::write(&trigger, b"").unwrap();

    init_session_with_mode(dt, name, &db, &dst, DstSyncMode::Replication);
    assert!(!trigger.exists(), "trigger file must be removed");
    assert_eq!(read_uuid(&db), uuid_before);

    write_rows(dt, &db, &[(42, "z")]);
    assert_eq!(
        dst_count(dt, &dst, "items"),
        3,
        "post-trigger re-seed should land the full 3-row source snapshot"
    );
}

/// Disambiguator: drive a DuckDbStore device but ship to a SQLite
/// destination. If user tables show up here we know the gap is
/// "DuckDbStore + DuckDb destination" specifically; if they don't,
/// the gap is on the store-device pipeline regardless of destination.
#[test]
fn duckdbstore_to_sqlite_dst_preserve() {
    let dt = DeviceType::DUCKDB_STORE;
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
