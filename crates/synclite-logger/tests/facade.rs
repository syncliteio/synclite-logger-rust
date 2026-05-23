//! End-to-end test of the top-level `Logger` facade. Verifies that:
//!   - parsing a properties file picks the right backend,
//!   - segments are produced under the device home,
//!     moved into the stage subdir, then shipped + cleaned.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use synclite_core::record::ArgValue;
use synclite::{duckdb as sl_duckdb, rusqlite as sl_rusqlite};
use synclite::Logger;

struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let n = N.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("synclite-logger-{tag}-{nanos:x}-{n}"));
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

/// Locate the `synclite-<device>-<uuid>/` stage subdirectory created by
/// `Logger::open` for the given `device_name`.
fn find_stage_subdir(stage: &Path, device_name: &str) -> PathBuf {
    let prefix = format!("synclite-{device_name}-");
    for entry in fs::read_dir(stage).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(&prefix) {
            return entry.path();
        }
    }
    panic!("no stage subdir matching {prefix}* under {}", stage.display());
}

#[test]
fn sqlite_logger_ships_and_cleans_segments() {
    let work = TempDir::new("sqlite");
    let stage = work.path().join("stage");
    let target = work.path().join("target");
    let db = work.path().join("db").join("demo.db");
    let conf = work.path().join("synclite_logger.conf");

    fs::write(
        &conf,
        format!(
            "device-name=demo\n\
             db-engine=sqlite\n\
             db-path={}\n\
             local-data-stage-directory={}\n\
             destination-fs-target-dir={}\n",
            db.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/"),
            target.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();

    Logger::initialize_with_config_path(synclite_core::DeviceType::Sqlite, &db, &conf).unwrap();
    let mut logger = Logger::open(&conf).unwrap();
    logger.execute("CREATE TABLE t(x INTEGER)", &[]).unwrap();
    logger
        .execute("INSERT INTO t(x) VALUES (?)", &[ArgValue::Int(7)])
        .unwrap();
    logger.close().unwrap();

    // Segment 0 was shipped to the target dir.
    let shipped = target.join("0.sqllog");
    assert!(shipped.exists(), "shipped segment missing: {}", shipped.display());

    // Local stage segment was cleaned by the cleaner after shipping.
    let stage_subdir = find_stage_subdir(&stage, "demo");
    let staged = stage_subdir.join("0.sqllog");
    assert!(!staged.exists(), "staged segment should be cleaned: {}", staged.display());

    // Device home exists with metadata file.
    let device_home: PathBuf = format!("{}.synclite", db.display()).into();
    assert!(device_home.exists(), "device home missing: {}", device_home.display());
    assert!(device_home.join("demo.db.synclite.metadata").exists());

    // The shipped segment carries the user SQL rows; initialize may also
    // contribute setup rows, so avoid asserting an exact global count.
    let seg = Connection::open(&shipped).unwrap();
    let create_count: i64 = seg
        .query_row(
            "SELECT count(*) FROM commandlog WHERE sql LIKE 'CREATE TABLE t(%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let insert_count: i64 = seg
        .query_row(
            "SELECT count(*) FROM commandlog WHERE sql LIKE 'INSERT INTO t(x) VALUES (%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(create_count, 1);
    assert_eq!(insert_count, 1);
}

#[test]
fn duckdb_logger_ships_and_cleans_segments() {
    let work = TempDir::new("duckdb");
    let stage = work.path().join("stage");
    let target = work.path().join("target");
    let db = work.path().join("db").join("demo.duckdb");
    let conf = work.path().join("synclite_logger.conf");

    fs::write(
        &conf,
        format!(
            "device-name=demo\n\
             db-engine=duckdb\n\
             db-path={}\n\
             local-data-stage-directory={}\n\
             destination-fs-target-dir={}\n",
            db.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/"),
            target.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();

    Logger::initialize_with_config_path(synclite_core::DeviceType::DuckDb, &db, &conf).unwrap();
    let mut logger = Logger::open(&conf).unwrap();
    logger
        .execute("CREATE TABLE m(ts BIGINT, v DOUBLE)", &[])
        .unwrap();
    logger
        .execute(
            "INSERT INTO m VALUES (?, ?)",
            &[ArgValue::Int(1), ArgValue::Real(1.5)],
        )
        .unwrap();
    logger.close().unwrap();

    let shipped = target.join("0.sqllog");
    assert!(shipped.exists());
    let stage_subdir = find_stage_subdir(&stage, "demo");
    let staged = stage_subdir.join("0.sqllog");
    assert!(!staged.exists());

    let n: i64 = Connection::open(&shipped)
        .unwrap()
        .query_row("SELECT count(*) FROM commandlog", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2);
}

#[test]
fn logger_without_destination_still_works() {
    let work = TempDir::new("nodest");
    let stage = work.path().join("stage");
    let db = work.path().join("db").join("demo.db");
    let conf = work.path().join("synclite_logger.conf");

    fs::write(
        &conf,
        format!(
            "device-name=demo\n\
             db-engine=sqlite\n\
             db-path={}\n\
             local-data-stage-directory={}\n",
            db.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();

    Logger::initialize_with_config_path(synclite_core::DeviceType::Sqlite, &db, &conf).unwrap();
    let mut logger = Logger::open(&conf).unwrap();
    logger.execute("CREATE TABLE t(x INTEGER)", &[]).unwrap();
    logger.close().unwrap();

    // No shipping → segment lives under the stage subdir.
    let stage_subdir = find_stage_subdir(&stage, "demo");
    let staged = stage_subdir.join("0.sqllog");
    assert!(staged.exists(), "staged segment missing: {}", staged.display());
}

#[test]
fn logger_fans_out_to_multiple_fs_destinations_from_indexed_keys() {
    let work = TempDir::new("multifs");
    let stage = work.path().join("stage");
    let target1 = work.path().join("target-1");
    let target2 = work.path().join("target-2");
    let db = work.path().join("db").join("demo.db");
    let conf = work.path().join("synclite_logger.conf");

    fs::write(
        &conf,
        format!(
            "device-name=demo\n\
             db-engine=sqlite\n\
             db-path={}\n\
             local-data-stage-directory={}\n\
             destination-type-1=FS\n\
             local-data-stage-directory-1={}\n\
             destination-type-2=FS\n\
             local-data-stage-directory-2={}\n",
            db.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/"),
            target1.display().to_string().replace('\\', "/"),
            target2.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();

    Logger::initialize_with_config_path(synclite_core::DeviceType::Sqlite, &db, &conf).unwrap();
    let mut logger = Logger::open(&conf).unwrap();
    logger.execute("CREATE TABLE t(x INTEGER)", &[]).unwrap();
    logger.close().unwrap();

    assert!(target1.join("0.sqllog").exists());
    assert!(target2.join("0.sqllog").exists());
}

#[test]
fn logger_uses_native_lock_file_to_block_second_open() {
    let work = TempDir::new("applock");
    let stage = work.path().join("stage");
    let db = work.path().join("db").join("demo.db");
    let conf = work.path().join("synclite_logger.conf");

    fs::write(
        &conf,
        format!(
            "device-name=demo\n\
             db-engine=sqlite\n\
             db-path={}\n\
             local-data-stage-directory={}\n",
            db.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();

    Logger::initialize_with_config_path(synclite_core::DeviceType::Sqlite, &db, &conf).unwrap();
    let logger = Logger::open(&conf).unwrap();

    let second = Logger::open(&conf);
    assert!(second.is_err());

    logger.close().unwrap();
    Logger::open(&conf).unwrap().close().unwrap();
}

#[test]
fn logger_rejects_non_alphanumeric_device_name() {
    let work = TempDir::new("bad-device-name");
    let stage = work.path().join("stage");
    let db = work.path().join("db").join("demo.db");
    let conf = work.path().join("synclite_logger.conf");

    fs::write(
        &conf,
        format!(
            "device-name=bad-name\n\
             db-engine=sqlite\n\
             db-path={}\n\
             local-data-stage-directory={}\n",
            db.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();

    assert!(Logger::open(&conf).is_err());
}

#[test]
fn rusqlite_wrapper_supports_prepare_and_batch() {
    let work = TempDir::new("rsql-wrapper");
    let db = work.path().join("db").join("demo.db");

    let mut conn = sl_rusqlite::Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t(x INTEGER)", &[]).unwrap();
    {
        let mut stmt = conn.prepare("INSERT INTO t(x) VALUES (?)").unwrap();
        stmt.add_batch(&[ArgValue::Int(1)]);
        stmt.add_batch(&[ArgValue::Int(2)]);
        stmt.execute_batch().unwrap();
    }
    conn.commit().unwrap();

    let rows = conn.query("SELECT COUNT(*) FROM t", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], ArgValue::Int(2));
    conn.close().unwrap();
}

#[test]
fn duckdb_wrapper_supports_prepare_and_batch() {
    let work = TempDir::new("duck-wrapper");
    let db = work.path().join("db").join("demo.duckdb");

    let mut conn = sl_duckdb::Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t(x INTEGER)", &[]).unwrap();
    {
        let mut stmt = conn.prepare("INSERT INTO t(x) VALUES (?)").unwrap();
        stmt.add_batch(&[ArgValue::Int(1)]);
        stmt.add_batch(&[ArgValue::Int(2)]);
        stmt.execute_batch().unwrap();
    }
    conn.commit().unwrap();

    let rows = conn.query("SELECT COUNT(*) FROM t", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], ArgValue::Int(2));
    conn.close().unwrap();
}
