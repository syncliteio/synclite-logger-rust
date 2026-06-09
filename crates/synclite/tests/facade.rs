//! End-to-end test of the top-level `Logger` facade. Verifies that:
//!   - parsing a properties file picks the right backend,
//!   - segments are produced under the device home,
//!     moved into the stage subdir, then shipped + cleaned.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use logger_core::record::ArgValue;
use rusqlite::Connection;
use synclite::Logger;
use synclite::{duckdb as sl_duckdb, rusqlite as sl_rusqlite};

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let n = N.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("synclite-{tag}-{nanos:x}-{n}"));
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

fn user_home_dir() -> PathBuf {
    if let Some(home) = std::env::var_os("USERPROFILE") {
        if !home.is_empty() {
            return PathBuf::from(home);
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        if !home.is_empty() {
            return PathBuf::from(home);
        }
    }
    panic!("unable to resolve user home directory from HOME/USERPROFILE");
}

fn test_home() -> PathBuf {
    user_home_dir().join("synclite").join("test").join("rustruntime")
}

fn rm_rf(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

fn wipe_stage_subdirs(stage: &Path, device_name: &str) {
    if !stage.exists() {
        return;
    }
    let prefix = format!("synclite-{device_name}-");
    if let Ok(entries) = fs::read_dir(stage) {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                rm_rf(&entry.path());
            }
        }
    }
}

fn wait_for_row_count(db_path: &Path, table: &str, expected: i64) {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let deadline = SystemTime::now() + Duration::from_secs(20);
    loop {
        if let Ok(conn) = Connection::open(db_path) {
            let count = conn.query_row(&sql, [], |r| r.get::<_, i64>(0));
            if let Ok(n) = count {
                if n == expected {
                    return;
                }
            }
        }

        if SystemTime::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    let conn = Connection::open(db_path).unwrap();
    let n: i64 = conn.query_row(&sql, [], |r| r.get(0)).unwrap_or(-1);
    panic!(
        "timed out waiting for row count in table {table}; expected {expected}, got {n}"
    );
}

fn wait_for_min_row_count(db_path: &Path, table: &str, min_expected: i64) {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let deadline = SystemTime::now() + Duration::from_secs(20);
    loop {
        if let Ok(conn) = Connection::open(db_path) {
            let count = conn.query_row(&sql, [], |r| r.get::<_, i64>(0));
            if let Ok(n) = count {
                if n >= min_expected {
                    return;
                }
            }
        }

        if SystemTime::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    let conn = Connection::open(db_path).unwrap();
    let n: i64 = conn.query_row(&sql, [], |r| r.get(0)).unwrap_or(-1);
    panic!(
        "timed out waiting for min row count in table {table}; expected at least {min_expected}, got {n}"
    );
}

fn wait_for_min_i64(db_path: &Path, sql: &str, min_expected: i64) -> i64 {
    let deadline = SystemTime::now() + Duration::from_secs(20);
    let mut last = -1i64;
    loop {
        if let Ok(conn) = Connection::open(db_path) {
            if let Ok(n) = conn.query_row(sql, [], |r| r.get::<_, i64>(0)) {
                last = n;
                if n >= min_expected {
                    return n;
                }
            }
        }
        if SystemTime::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out waiting for `{sql}` >= {min_expected}; last={last}");
}

fn remove_path_retry(path: &Path) {
    for _ in 0..200 {
        let result = if path.is_dir() {
            fs::remove_dir_all(path)
        } else {
            fs::remove_file(path)
        };

        match result {
            Ok(()) => return,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e)
                if e.kind() == std::io::ErrorKind::PermissionDenied
                    || e.kind() == std::io::ErrorKind::WouldBlock
                    || e.raw_os_error() == Some(32) =>
            {
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("failed to remove {}: {e}", path.display()),
        }
    }
    eprintln!(
        "warning: failed to remove path after retries (continuing): {}",
        path.display()
    );
}

#[test]
#[allow(non_snake_case)]
fn testrustSqliteSqliteShip() {
    let name = "testrustSqliteSqliteShip";
    let test_root = test_home();
    let stage = test_root.join("stageDir");
    let target = test_root.join("workDir").join(name);
    let db = test_root
        .join("db").join("rustologger")
        .join(name)
        .join(format!("{name}.db"));
    let conf = db.parent().unwrap().join("synclite.conf");

    remove_path_retry(&db.parent().unwrap());
    remove_path_retry(&target);
    wipe_stage_subdirs(&stage, name);
    fs::create_dir_all(&stage).unwrap();
    fs::create_dir_all(&db.parent().unwrap()).unwrap();
    fs::create_dir_all(&target).unwrap();

    fs::write(
        &conf,
        format!(
            "device-name={name}\n\
             db-engine=sqlite\n\
             db-path={}\n\
             local-data-stage-directory={}\n\
             device-stage-type=FS\n\
             device-upload-root={}\n",
            db.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/"),
            target.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();

    synclite::initialize(
        logger_core::DeviceType::SQLITE,
        "testdevice",
        &db,
        None,
        synclite::SyncLiteOptions {
            config_path: Some(conf.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let mut logger = Logger::open(&conf).unwrap();
    let table = name;
    logger
        .execute(&format!("CREATE TABLE {table}(x INTEGER)"), &[])
        .unwrap();
    logger
        .execute(&format!("INSERT INTO {table}(x) VALUES (?)"), &[ArgValue::Int(7)])
        .unwrap();
    logger.close().unwrap();

    // Segment 0 was shipped to the target dir.
    let shipped = target.join("0.sqllog");
    assert!(shipped.exists(), "shipped segment missing: {}", shipped.display());

    // Local stage segment was cleaned by the cleaner after shipping.
    let stage_subdir = find_stage_subdir(&stage, name);
    let staged = stage_subdir.join("0.sqllog");
    assert!(!staged.exists(), "staged segment should be cleaned: {}", staged.display());

    // Device home exists with metadata file.
    let device_home: PathBuf = format!("{}.synclite", db.display()).into();
    assert!(device_home.exists(), "device home missing: {}", device_home.display());
    let db_file = db.file_name().unwrap().to_string_lossy().into_owned();
    assert!(device_home.join(format!("{db_file}.synclite.metadata")).exists());

    // The shipped segment carries the user SQL rows; initialize may also
    // contribute setup rows, so avoid asserting an exact global count.
    let seg = Connection::open(&shipped).unwrap();
    let create_count: i64 = seg
        .query_row(
            &format!("SELECT count(*) FROM commandlog WHERE sql LIKE 'CREATE TABLE {table}(%'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    let insert_count: i64 = seg
        .query_row(
            &format!("SELECT count(*) FROM commandlog WHERE sql LIKE 'INSERT INTO {table}(x) VALUES (%'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(create_count, 1);
    assert_eq!(insert_count, 1);
}

#[test]
#[allow(non_snake_case)]
fn testrustDuckdbDuckdbShip() {
    let name = "testrustDuckdbDuckdbShip";
    let test_root = test_home();
    let stage = test_root.join("stageDir");
    let target = test_root.join("workDir").join(name);
    let db = test_root
        .join("db").join("rustologger")
        .join(name)
        .join(format!("{name}.duckdb"));
    let conf = db.parent().unwrap().join("synclite.conf");

    remove_path_retry(&db.parent().unwrap());
    remove_path_retry(&target);
    wipe_stage_subdirs(&stage, name);
    fs::create_dir_all(&stage).unwrap();
    fs::create_dir_all(&db.parent().unwrap()).unwrap();
    fs::create_dir_all(&target).unwrap();

    fs::write(
        &conf,
        format!(
            "device-name={name}\n\
             db-engine=duckdb\n\
             db-path={}\n\
             local-data-stage-directory={}\n\
             device-stage-type=FS\n\
             device-upload-root={}\n",
            db.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/"),
            target.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();

    synclite::initialize(
        logger_core::DeviceType::DUCKDB,
        "testdevice",
        &db,
        None,
        synclite::SyncLiteOptions {
            config_path: Some(conf.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let mut logger = Logger::open(&conf).unwrap();
    let table = name;
    logger
        .execute(&format!("CREATE TABLE {table}(ts BIGINT, v DOUBLE)"), &[])
        .unwrap();
    logger
        .execute(&format!("INSERT INTO {table} VALUES (?, ?)"), &[ArgValue::Int(1), ArgValue::Real(1.5)])
        .unwrap();
    logger.close().unwrap();

    let shipped = target.join("0.sqllog");
    assert!(shipped.exists());
    let stage_subdir = find_stage_subdir(&stage, name);
    let staged = stage_subdir.join("0.sqllog");
    assert!(!staged.exists());

    let n: i64 = Connection::open(&shipped)
        .unwrap()
        .query_row("SELECT count(*) FROM commandlog", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2);
}

#[test]
#[allow(non_snake_case)]
fn testrustSqliteSqliteNoDestination() {
    let name = "testrustSqliteSqliteNoDestination";
    let test_root = test_home();
    let stage = test_root.join("stageDir");
    let db = test_root
        .join("db").join("rustologger")
        .join(name)
        .join(format!("{name}.db"));
    let conf = db.parent().unwrap().join("synclite.conf");

    remove_path_retry(&db.parent().unwrap());
    wipe_stage_subdirs(&stage, name);
    fs::create_dir_all(&stage).unwrap();
    fs::create_dir_all(&db.parent().unwrap()).unwrap();

    fs::write(
        &conf,
        format!(
            "device-name={name}\n\
             db-engine=sqlite\n\
             db-path={}\n\
             local-data-stage-directory={}\n",
            db.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();

    synclite::initialize(
        logger_core::DeviceType::SQLITE,
        "testdevice",
        &db,
        None,
        synclite::SyncLiteOptions {
            config_path: Some(conf.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let mut logger = Logger::open(&conf).unwrap();
    let table = name;
    logger.execute(&format!("CREATE TABLE {table}(x INTEGER)"), &[]).unwrap();
    logger.close().unwrap();

    // No shipping ? segment lives under the stage subdir.
    let stage_subdir = find_stage_subdir(&stage, name);
    let staged = stage_subdir.join("0.sqllog");
    assert!(staged.exists(), "staged segment missing: {}", staged.display());
}

#[test]
#[allow(non_snake_case)]
fn testrustSqlitestoreSqliteConsolidate() {
    let name = "testrustSqlitestoreSqliteConsolidate";
    let test_root = test_home();
    let stage = test_root.join("stageDir");
    let work_dir = test_root.join("workDir");
    let db_dir = test_root.join("db").join("rustologger").join(name);
    let db = db_dir.join(format!("{name}.db"));
    let dest_db = work_dir.join("consolidated_db.sqlite");
    let conf = db_dir.join("synclite.conf");

    remove_path_retry(&db_dir);
    wipe_stage_subdirs(&stage, name);
    fs::create_dir_all(&stage).unwrap();
    fs::create_dir_all(&work_dir).unwrap();
    fs::create_dir_all(&db_dir).unwrap();
    fs::write(
        &conf,
        format!(
            "device-name={name}\n\
             device-type=SQLITE_STORE\n\
             device-stage-type=FS\n\
             db-path={}\n\
             device-data-root={}\n\
             local-data-stage-directory={}\n\
             \n\
             # ---------------- logger options catalog ----------------\n\
             # db-engine=SQLITE (default: SQLITE)\n\
             # device-type=SQLITE_STORE (default: inferred from db-engine when omitted)\n\
             # work-dir=<alias for local-data-stage-directory> (default: unset)\n\
             # device-data-root=<alias for local-data-stage-directory/dst-work-dir> (default: unset)\n\
             # log-segment-page-size=<bytes> (default: internal DEFAULT_LOG_SEGMENT_PAGE_SIZE)\n\
             # log-segment-flush-batch-size=<rows> (default: internal DEFAULT_LOG_SEGMENT_FLUSH_BATCH_SIZE)\n\
             # log-segment-switch-log-count-threshold=<count> (default: internal engine default)\n\
             # log-segment-switch-duration-threshold-ms=<ms> (default: internal engine default)\n\
             # log-segment-shipping-frequency-ms=<ms> (default: internal DEFAULT_LOG_SEGMENT_SHIPPING_FREQUENCY_MS)\n\
             # log-max-inlined-arg-count=<count> (default: 16)\n\
             # log-queue-size=<count> (default: unset)\n\
             # use-precreated-data-backup=true|false (default: false)\n\
             # vacuum-data-backup=true|false (default: false)\n\
             # skip-restart-recovery=true|false (default: false)\n\
             # disable-async-logging-for-transactional-device=true|false (default: false)\n\
             # enable-async-logging-for-appender-device=true|false (default: false)\n\
             # include-tables=t1,t2 (default: unset, include all)\n\
             # exclude-tables=t3,t4 (default: unset, exclude none)\n\
             # enable-command-handler=true|false (default: false)\n\
             # command-handler-type=EXTERNAL (default: unset)\n\
             # external-command-handler=<path-to-script> (default: unset)\n\
             # command-handler-frequency-ms=<ms> (default: unset)\n\
             # device-stage-type=FS|MS_ONEDRIVE|GOOGLE_DRIVE|SFTP|LOCAL_SFTP|REMOTE_SFTP|MINIO|LOCAL_MINIO|REMOTE_MINIO|S3 (default: unset)\n\
             # local-stage-directory=<path> (default: unset)\n\
             # device-stage-fs-target-dir=<path> (default: unset)\n\
             # sftp:host=<host> (default: unset)\n\
             # sftp:port=<port> (default: 22)\n\
             # sftp:user-name=<user> (default: unset)\n\
             # sftp:password=<password> (default: unset)\n\
             # sftp:remote-stage-directory=<remote-dir> (default: unset)\n\
             # minio:endpoint=<url> (default: unset)\n\
             # minio:bucket-name=<bucket> (default: unset)\n\
             # minio:access-key=<key> (default: unset)\n\
             # minio:secret-key=<secret> (default: unset)\n\
             # s3:key-prefix=<prefix> (default: unset)\n\
             # s3:region=<region> (default: us-east-1)\n\
             # s3:endpoint=<url> (default: unset)\n\
             # s3:path-style=true|false (default: false)\n\
             # s3:access-key-id=<key> (default: unset)\n\
             # s3:secret-access-key=<secret> (default: unset)\n\
             # s3:session-token=<token> (default: unset)\n\
             # sftp:host=<host> (default: unset)\n\
             # sftp:port=<port> (default: 22)\n\
             # sftp:user-name=<user> (default: unset)\n\
             # sftp:password=<password> (default: unset)\n\
             # sftp:private-key-path=<path> (default: unset)\n\
             # sftp:private-key-passphrase=<passphrase> (default: unset)\n\
             # sftp:remote-stage-directory=<remote-dir> (default: .)\n\
             # sftp:accept-any-host-key=true|false (default: false)\n\
             # indexed shipper aliases: device-stage-type-N / device-stage-fs-target-dir-N / s3:...-N / sftp:...-N (default: unset)\n\
             \n\
             # ---------------- consolidator options catalog ----------------\n\
             # dst-sync-mode=CONSOLIDATION|REPLICATION (default: CONSOLIDATION)\n\
             # destination-sync-mode=CONSOLIDATION|REPLICATION (default: CONSOLIDATION)\n\
             # dst-enable-event-streaming-consolidation-1=true|false (default: true)\n\
             # dst-database-type-1=SQLITE|DUCKDB|POSTGRESQL (default: SQLITE)\n\
             # dst-type-1=SQLITE|DUCKDB|POSTGRESQL (default: SQLITE)\n\
             # metadata-store-1=LOCAL|DESTINATION (default: DESTINATION)\n\
             # dst-connection-string-1=jdbc:sqlite:<path> or jdbc:duckdb:<path> or jdbc:postgresql://... (default: unset)\n\
             # dst-work-dir-1=<path> (default: <device_home>/synclite-consolidator[/dst-1])\n\
             # destination-work-dir-1=<path> (default: <device_home>/synclite-consolidator[/dst-1])\n\
             # dst-oper-retry-count-1=3 (default: 3)\n\
             # dst-oper-retry-interval-ms-1=1000 (default: 1000)\n\
             # dst-idempotent-data-ingestion-1=true|false (default: true for REPLICATION, false for CONSOLIDATION)\n\
             # dst-insert-batch-size-1=1000 (default: 1000)\n\
             # dst-update-batch-size-1=1000 (default: 1000)\n\
             # dst-delete-batch-size-1=1000 (default: 1000)\n\
             # dst-connection-timeout-s-1=<seconds> (default: backend-specific)\n\
             # dst-object-init-mode-1=TRY_CREATE_APPEND_DATA|APPEND_DATA|TRY_CREATE_DELETE_DATA|DELETE_DATA|OVERWRITE_OBJECT (default: TRY_CREATE_APPEND_DATA)\n\
             # dst-oper-predicate-optimization-1=true|false (default: true)\n\
             # dst-idempotent-data-ingestion-method-1=NATIVE_UPSERT|NATIVE_REPLACE|DELETE_INSERT (default: NATIVE_UPSERT)\n\
             # dst-skip-failed-log-files-1=true|false (default: false)\n\
             # dst-set-unparsable-values-to-null-1=true|false (default: false)\n\
             # dst-quote-object-names-1=true|false (default: false)\n\
             # dst-quote-column-names-1=true|false (default: false)\n\
             # dst-use-catalog-scope-resolution-1=true|false (default: false)\n\
             # dst-use-schema-scope-resolution-1=true|false (default: true)\n\
             # dst-data-type-mapping-1=ALL_TEXT|BEST_EFFORT|CUSTOMIZED|EXACT (default: ALL_TEXT)\n\
             # dst-enable-filter-mapper-rules-1=true|false (default: false)\n\
             # dst-allow-unspecified-tables-1=true|false (default: true)\n\
             # dst-allow-unspecified-columns-1=true|false (default: true)\n\
             # dst-filter-mapper-rules-file-1=<path-to-json> (required when dst-enable-filter-mapper-rules-1=true)\n\
             # dst-enable-value-mapper-1=true|false (default: false)\n\
             # dst-value-mappings-file-1=<path-to-json> (required when dst-enable-value-mapper-1=true)\n\
             # dst-enable-triggers-1=true|false (default: false)\n\
             # dst-triggers-file-1=<path-to-sql/json> (required when dst-enable-triggers-1=true)\n\
             # map-devices-to-dst-pattern-type=DEVICE_NAME_PATTERN|DEVICE_ID_PATTERN (required when multiple destinations are configured)\n\
             # map-devices-to-dst-pattern-1=<regex> (required when multiple destinations are configured)\n\
             # default-dst-index-for-unmapped-devices=<index> (required when multiple destinations are configured)\n\
             # update-statistics-interval-s=1 (default: 1)\n\
             # synclite-statistics-file-path=<device-data-root>/synclite_consolidator_statistics.db (default: unset)\n\
             # enable-prometheus-statistics-publisher=true|false (default: false)\n\
             # prometheus-push-gateway-url=http://localhost:9091 (default: unset)\n\
             # prometheus-statistics-publisher-interval-s=60 (default: 60)\n\
             \n\
             dst-sync-mode=CONSOLIDATION\n\
             dst-type-1=SQLITE\n\
             metadata-store-1=DESTINATION\n\
             dst-oper-retry-count-1=100\n\
             dst-oper-retry-interval-ms-1=10000\n\
             dst-idempotent-data-ingestion-1=false\n\
             dst-insert-batch-size-1=100000\n\
             dst-update-batch-size-1=100000\n\
             dst-delete-batch-size-1=100000\n\
             dst-connection-string-1=jdbc:sqlite:{}?journal_mode=WAL\n",
            db.display().to_string().replace('\\', "/"),
            work_dir.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/"),
            dest_db.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();

    synclite::initialize(
        logger_core::DeviceType::SQLITE_STORE,
        "testdevice",
        &db,
        None,
        synclite::SyncLiteOptions {
            config_path: Some(conf.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let mut logger = Logger::open(&conf).unwrap();
    let table = name;
    logger.execute(&format!("CREATE TABLE {table}(id INTEGER PRIMARY KEY, v TEXT)"), &[]).unwrap();
    logger
        .execute(&format!("INSERT INTO {table}(id, v) VALUES (?, ?)",), &[ArgValue::Int(1), ArgValue::Text("a".into())],)
        .unwrap();
    logger
        .execute(&format!("INSERT INTO {table}(id, v) VALUES (?, ?)",), &[ArgValue::Int(2), ArgValue::Text("b".into())],)
        .unwrap();
    logger.close().unwrap();

    wait_for_row_count(&dest_db, table, 2);

    let stage_subdir = find_stage_subdir(&stage, name);
    let stage_device_dir_name = stage_subdir.file_name().unwrap();
    let work_device_dir = work_dir.join(stage_device_dir_name);
    assert!(work_device_dir.exists(), "work mirror device dir missing: {}", work_device_dir.display());
    assert!(
        work_device_dir.join(format!("{name}.db.synclite.backup")).exists(),
        "work mirror backup missing"
    );
    assert!(
        work_device_dir.join(format!("{name}.db.synclite.metadata")).exists(),
        "work mirror metadata missing"
    );

    let device_stats_db = work_device_dir.join("synclite_device_statistics.db");
    let consolidator_stats_db = work_dir.join("synclite_consolidator_statistics.db");
    assert!(
        device_stats_db.exists(),
        "device statistics db missing: {}",
        device_stats_db.display()
    );
    assert!(
        consolidator_stats_db.exists(),
        "consolidator statistics db missing: {}",
        consolidator_stats_db.display()
    );

    let src = Connection::open(&db).unwrap();
    let dst = Connection::open(&dest_db).unwrap();

    let src_rows: Vec<(i64, String)> = src
        .prepare(&format!("SELECT id, v FROM {table} ORDER BY id"))
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let dst_rows: Vec<(i64, String)> = dst
        .prepare(&format!("SELECT id, v FROM {table} ORDER BY id"))
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(src_rows, dst_rows);

    let n: i64 = dst
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2);

    let checkpoint_seq: i64 = dst
        .query_row(
            "SELECT cdc_log_segment_sequence_number FROM synclite_checkpoint",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(checkpoint_seq >= 0);

    wait_for_min_i64(
        &dest_db,
        "SELECT COUNT(*) FROM synclite_consolidator_table_metadata WHERE prop_key = 'create_sql'",
        1,
    );

    let init_status: i64 = dst
        .query_row(
            "SELECT initialization_status FROM synclite_checkpoint ORDER BY commit_id DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(init_status, 1);

    let device_stats_conn = Connection::open(&device_stats_db).unwrap();
    let checkpoint_alias: String = device_stats_conn
        .query_row("SELECT dst_alias FROM checkpoint LIMIT 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(checkpoint_alias, "DB-1");

    let checkpoint_oper_count = wait_for_min_i64(
        &device_stats_db,
        "SELECT processed_oper_count FROM checkpoint WHERE dst_alias='DB-1'",
        2,
    );
    assert!(checkpoint_oper_count >= 2);

    let checkpoint_txn_count = wait_for_min_i64(
        &device_stats_db,
        "SELECT processed_txn_count FROM checkpoint WHERE dst_alias='DB-1'",
        1,
    );
    assert!(checkpoint_txn_count >= 1);

    let checkpoint_log_size = wait_for_min_i64(
        &device_stats_db,
        "SELECT processed_log_size FROM checkpoint WHERE dst_alias='DB-1'",
        1,
    );
    assert!(checkpoint_log_size > 0);

    let table_stats_count = wait_for_min_i64(
        &device_stats_db,
        "SELECT COUNT(*) FROM table_statistics",
        1,
    );
    assert!(table_stats_count >= 1);

    let dashboard_segments = wait_for_min_i64(
        &consolidator_stats_db,
        "SELECT total_log_segments_applied FROM dashboard",
        1,
    );
    assert!(dashboard_segments >= 1);

    let dashboard_oper_count = wait_for_min_i64(
        &consolidator_stats_db,
        "SELECT total_processed_oper_count FROM dashboard",
        2,
    );
    assert!(dashboard_oper_count >= 2);

    let dashboard_txn_count = wait_for_min_i64(
        &consolidator_stats_db,
        "SELECT total_processed_txn_count FROM dashboard",
        1,
    );
    assert!(dashboard_txn_count >= 1);

    let dashboard_log_size = wait_for_min_i64(
        &consolidator_stats_db,
        "SELECT total_processed_log_size FROM dashboard",
        1,
    );
    assert!(dashboard_log_size > 0);

    let consolidator_stats_conn = Connection::open(&consolidator_stats_db).unwrap();
    let dashboard_job_start: i64 = consolidator_stats_conn
        .query_row("SELECT last_job_start_time FROM dashboard", [], |r| r.get(0))
        .unwrap();
    assert!(dashboard_job_start > 0);

    let dashboard_last_heartbeat: i64 = consolidator_stats_conn
        .query_row("SELECT last_heartbeat_time FROM dashboard", [], |r| r.get(0))
        .unwrap();
    assert!(dashboard_last_heartbeat >= dashboard_job_start);

    let dashboard_latency: i64 = consolidator_stats_conn
        .query_row("SELECT latency FROM dashboard", [], |r| r.get(0))
        .unwrap();
    assert!(dashboard_latency >= 0);

    let status_last_commit: i64 = consolidator_stats_conn
        .query_row("SELECT COALESCE(MAX(last_consolidated_commit_id), 0) FROM device_status", [], |r| r.get(0))
        .unwrap();
    assert!(status_last_commit > 0);

}

#[test]
#[allow(non_snake_case)]
fn testrustSqliteSqliteFanoutIndexed() {
    let name = "testrustSqliteSqliteFanoutIndexed";
    let test_root = test_home();
    let stage = test_root.join("stageDir");
    let target1 = test_root.join("workDir").join(format!("{name}-1"));
    let target2 = test_root.join("workDir").join(format!("{name}-2"));
    let db = test_root
        .join("db").join("rustologger")
        .join(name)
        .join(format!("{name}.db"));
    let conf = db.parent().unwrap().join("synclite.conf");

    remove_path_retry(&db.parent().unwrap());
    remove_path_retry(&target1);
    remove_path_retry(&target2);
    wipe_stage_subdirs(&stage, name);
    fs::create_dir_all(&stage).unwrap();
    fs::create_dir_all(&target1).unwrap();
    fs::create_dir_all(&target2).unwrap();
    fs::create_dir_all(&db.parent().unwrap()).unwrap();

    fs::write(
        &conf,
        format!(
            "device-name={name}\n\
             db-engine=sqlite\n\
             db-path={}\n\
             local-data-stage-directory={}\n\
             device-stage-type-1=FS\n\
             device-upload-root-1={}\n\
             device-stage-type-2=FS\n\
             device-upload-root-2={}\n",
            db.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/"),
            target1.display().to_string().replace('\\', "/"),
            target2.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();

    synclite::initialize(
        logger_core::DeviceType::SQLITE,
        "testdevice",
        &db,
    None,
    synclite::SyncLiteOptions {
            config_path: Some(conf.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let mut logger = Logger::open(&conf).unwrap();
    let table = name;
    logger.execute(&format!("CREATE TABLE {table}(x INTEGER)"), &[]).unwrap();
    logger.close().unwrap();

    assert!(target1.join("0.sqllog").exists());
    assert!(target2.join("0.sqllog").exists());
}

#[test]
#[allow(non_snake_case)]
fn testrustSqliteSqliteNativeLock() {
    let name = "testrustSqliteSqliteNativeLock";
    let test_root = test_home();
    let stage = test_root.join("stageDir");
    let db = test_root
        .join("db").join("rustologger")
        .join(name)
        .join(format!("{name}.db"));
    let conf = db.parent().unwrap().join("synclite.conf");

    remove_path_retry(&db.parent().unwrap());
    wipe_stage_subdirs(&stage, name);
    fs::create_dir_all(&stage).unwrap();
    fs::create_dir_all(&db.parent().unwrap()).unwrap();

    fs::write(
        &conf,
        format!(
            "device-name={name}\n\
             db-engine=sqlite\n\
             db-path={}\n\
             local-data-stage-directory={}\n",
            db.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();

    synclite::initialize(
        logger_core::DeviceType::SQLITE,
        "testdevice",
        &db,
        None,
        synclite::SyncLiteOptions {
            config_path: Some(conf.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let logger = Logger::open(&conf).unwrap();

    let second = Logger::open(&conf);
    assert!(second.is_err());

    logger.close().unwrap();
    Logger::open(&conf).unwrap().close().unwrap();
}

#[test]
#[allow(non_snake_case)]
fn testrustSqliteSqliteRejectBadName() {
    let name = "testrustSqliteSqliteRejectBadName";
    let test_root = test_home();
    let stage = test_root.join("stageDir");
    let db = test_root
        .join("db").join("rustologger")
        .join(name)
        .join(format!("{name}.db"));
    let conf = db.parent().unwrap().join("synclite.conf");

    remove_path_retry(&db.parent().unwrap());
    wipe_stage_subdirs(&stage, name);
    fs::create_dir_all(&stage).unwrap();
    fs::create_dir_all(&db.parent().unwrap()).unwrap();

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
    let test_root = test_home();
    let db = test_root
        .join("db").join("rustologger")
        .join("rsqlite_wrapper_supports_prepare_and_batch")
        .join("demo.db");
    remove_path_retry(db.parent().unwrap());
    fs::create_dir_all(db.parent().unwrap()).unwrap();

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
    let test_root = test_home();
    let db = test_root
        .join("db").join("rustologger")
        .join("duckdb_wrapper_supports_prepare_and_batch")
        .join("demo.duckdb");
    remove_path_retry(db.parent().unwrap());
    fs::create_dir_all(db.parent().unwrap()).unwrap();

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

#[test]
fn rusqlite_wrapper_close_commits_pending_manual_txn() {
    let test_root = test_home();
    let db = test_root
        .join("db").join("rustologger")
        .join("rsqlite_wrapper_close_commits_pending_manual_txn")
        .join("demo.db");
    remove_path_retry(db.parent().unwrap());
    fs::create_dir_all(db.parent().unwrap()).unwrap();

    {
        let mut conn = sl_rusqlite::Connection::open(&db).unwrap();
        conn.set_auto_commit(false);
        conn.execute("CREATE TABLE t_close(x INTEGER)", &[]).unwrap();
        conn.execute("INSERT INTO t_close(x) VALUES (1)", &[]).unwrap();
        conn.close().unwrap();
    }

    let mut reopened = sl_rusqlite::Connection::open(&db).unwrap();
    let rows = reopened.query("SELECT COUNT(*) FROM t_close", &[]).unwrap();
    assert_eq!(rows[0][0], ArgValue::Int(1));
    reopened.close().unwrap();
}

#[test]
fn duckdb_wrapper_close_commits_pending_manual_txn() {
    let test_root = test_home();
    let db = test_root
        .join("db").join("rustologger")
        .join("duckdb_wrapper_close_commits_pending_manual_txn")
        .join("demo.duckdb");
    remove_path_retry(db.parent().unwrap());
    fs::create_dir_all(db.parent().unwrap()).unwrap();

    {
        let mut conn = sl_duckdb::Connection::open(&db).unwrap();
        conn.set_auto_commit(false);
        conn.execute("CREATE TABLE t_close(x INTEGER)", &[]).unwrap();
        conn.execute("INSERT INTO t_close VALUES (1)", &[]).unwrap();
        conn.close().unwrap();
    }

    let mut reopened = sl_duckdb::Connection::open(&db).unwrap();
    let rows = reopened.query("SELECT COUNT(*) FROM t_close", &[]).unwrap();
    assert_eq!(rows[0][0], ArgValue::Int(1));
    reopened.close().unwrap();
}



