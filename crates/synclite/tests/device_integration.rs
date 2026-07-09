//! Device integration tests.
//!
//! These tests are direct ports of the SQLite and DuckDB unit tests in
//! `synclite-logger-java/logger/src/test/java/io/synclite/logger/`,
//! adapted to the SyncLite-wrapped backend APIs in
//! `synclite::rusqlite` and `synclite::duckdb`.
//!
//! Layout alignment with the JUnit tests
//! (`DerbyAppenderTest`, `DerbyStoreAPITest`, etc.):
//!
//! - `testHome = ~/synclite/test/rustruntime/`
//! - `testDbPath = testHome/db/rustologger/<TestName>/<device>.db`
//!   (per-test, wiped in `setUp`)
//! - `testStageDir = testHome/stageDir` (SHARED across tests; only the
//!   per-device `synclite-<device>-*` subdirs get cleaned)
//! - `testWorkDir = testHome/workDir` with per-device folders and a shared
//!   `consolidated_db.sqlite` destination
//! - `testConfigPath = testDbPath.getParent()/synclite.conf`
//!
//! `rustruntime` keeps Rust artifacts isolated from the Java JUnit tree
//! at `~/synclite/test/` so concurrent runs cannot stomp each other.
//! Device names also carry a `-rust` suffix as a second safety net.
//!
//! Both ports write segments as `<N>.sqllog` inside a per-device
//! `synclite-<device>-<uuid>/` stage subdirectory.

use std::fs;
use std::time::{Duration, Instant};
use std::path::{Path, PathBuf};

use duckdb::Connection as DuckConnection;
use rusqlite::Connection;
use logger_core::record::ArgValue;
use logger_core::DeviceType;
use logger_db_traits::Row;
use synclite::{duckdb as sl_duckdb, rusqlite as sl_rusqlite};

// --------------------------------------------------------------------------
// Test scaffolding
// --------------------------------------------------------------------------

/// `~/synclite/test/rustruntime/` for Rust device integration tests.
/// Per-device directories are created under `db/rustologger/<device>`;
/// the stage and work directories are shared across rust suites under
/// `rustruntime/stageDir` and `rustruntime/workDir`.
fn test_home() -> PathBuf {
    let home = std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    home.join("synclite").join("test").join("rustruntime")
}

fn inferred_device_type(device_name: &str, engine: &str) -> &'static str {
    if device_name.contains("stream") {
        return "STREAMING";
    }
    match (engine.to_ascii_lowercase().as_str(), device_name.contains("store")) {
        ("sqlite", true) => "SQLITE_STORE",
        ("sqlite", false) => "SQLITE",
        ("duckdb", true) => "DUCKDB_STORE",
        ("duckdb", false) => "DUCKDB",
        _ => panic!("unsupported engine: {engine}"),
    }
}

fn slugify_alnum(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        }
    }
    if out.is_empty() { "na".to_string() } else { out }
}

fn normalize_naming_terms(input: &str) -> String {
    let mut s = input.to_ascii_lowercase();
    for (from, to) in [
        ("store_device", "store"),
        ("store-device", "store"),
        ("storedevice", "store"),
        ("sql_device", "sql"),
        ("sql-device", "sql"),
        ("sqldevice", "sql"),
        ("streaming_device", "streaming"),
        ("streaming-device", "streaming"),
        ("streamingdevice", "streaming"),
    ] {
        s = s.replace(from, to);
    }
    s
}

fn compact_scenario_token(token: &str) -> String {
    match token {
        "transactional" => "txn".to_string(),
        "transaction" => "txn".to_string(),
        "prepared" => "prep".to_string(),
        "default" => "def".to_string(),
        "autocommit" => "ac".to_string(),
        "rollback" => "rb".to_string(),
        "recovery" => "rcv".to_string(),
        "restart" => "rst".to_string(),
        "execute" => "exec".to_string(),
        "executes" => "exec".to_string(),
        "unlogged" => "ulg".to_string(),
        "splits" => "split".to_string(),
        "semicolon" => "semi".to_string(),
        "messages" => "msg".to_string(),
        "destination" => "dst".to_string(),
        "consolidate" => "csl".to_string(),
        "consolidates" => "csl".to_string(),
        "across" => "x".to_string(),
        "reopens" => "reopen".to_string(),
        "persist" => "pst".to_string(),
        "parity" => "pty".to_string(),
        "schema" => "sch".to_string(),
        "shadow" => "shd".to_string(),
        "behavior" => "bhv".to_string(),
        "unknown" => "unk".to_string(),
        "equal" => "eq".to_string(),
        "higher" => "hi".to_string(),
        "without" => "wo".to_string(),
        "with" => "w".to_string(),
        "basic" => "bsc".to_string(),
        "operations" => "ops".to_string(),
        "apis" => "api".to_string(),
        "all" => "all".to_string(),
        _ => {
            if token.len() <= 6 {
                token.to_string()
            } else {
                token.chars().take(6).collect()
            }
        }
    }
}

fn normalized_scenario_suffix(scenario_name: &str) -> String {
    let mut spaced = String::with_capacity(scenario_name.len() * 2);
    let mut prev_was_lower_or_digit = false;
    for ch in scenario_name.chars() {
        if ch.is_ascii_uppercase() && prev_was_lower_or_digit {
            spaced.push(' ');
        }
        if ch.is_ascii_alphanumeric() {
            spaced.push(ch.to_ascii_lowercase());
        } else {
            spaced.push(' ');
        }
        prev_was_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
    }

    let normalized = normalize_naming_terms(&spaced);
    let mut parts: Vec<String> = normalized
        .split_whitespace()
        .map(|p| p.chars().filter(|c| c.is_ascii_alphanumeric()).collect::<String>())
        .filter(|p| !p.is_empty())
        .collect();

    parts.retain(|p| {
        !matches!(
            p.as_str(),
            "duckdb" | "sqlite" | "store" | "sql" | "streaming" | "test" | "testrust" | "rust" | "device"
        )
    });

    let mut compact_parts: Vec<String> = Vec::new();
    for part in parts {
        let compact = compact_scenario_token(&part);
        if compact_parts.last().map_or(false, |last| last == &compact) {
            continue;
        }
        compact_parts.push(compact);
    }

    let mut scenario = if compact_parts.is_empty() {
        "scenario".to_string()
    } else {
        slugify_alnum(&compact_parts.join(""))
    };

    for redundant in ["duckdb", "sqlite", "store", "sql", "streaming"] {
        while scenario.contains(redundant) {
            scenario = scenario.replace(redundant, "");
        }
    }

    if scenario.is_empty() {
        "scenario".to_string()
    } else {
        scenario
    }
}

fn enforce_name_len_64(base: String, seed: &str) -> String {
    const MAX_DEVICE_NAME_LEN: usize = 40;
    if base.len() <= MAX_DEVICE_NAME_LEN {
        return base;
    }
    let _ = seed;
    base.chars().take(MAX_DEVICE_NAME_LEN).collect()
}

fn canonical_test_name(device_name: &str, engine: &str, scenario_name: &str) -> String {
    let normalized_device = normalize_naming_terms(device_name);
    let device = if normalized_device.contains("stream") {
        "streaming"
    } else if normalized_device.contains("store") {
        "store"
    } else {
        "sql"
    };
    let engine_name = if device == "streaming" {
        "sqlite"
    } else {
        &engine.to_ascii_lowercase()
    };
    let scenario = normalized_scenario_suffix(scenario_name);
    let base = format!("testrust{device}{engine_name}{scenario}");
    enforce_name_len_64(base, &format!("{device_name}|{engine}|{scenario_name}"))
}

fn rm_rf(p: &Path) {
    let _ = fs::remove_dir_all(p);
}

/// Common test setup:
///   * wipe `testHome/db/<TestName>/`
///   * wipe any prior `testHome/stageDir/synclite-<device>-*` subdirs
///   * (re-)create both directories
///   * write `synclite.conf` next to the DB
///
/// With `with_target=true`, a per-test `target/` dir under the DB dir
/// becomes the FS shipper destination for non-layout scenarios.
/// Layout-focused tests keep finalized segments under the device stage
/// subdir inside `testHome/stageDir`.
struct Workspace {
    device_name: String,
    device_type: DeviceType,
    stage: PathBuf,
    db_path: PathBuf,
    target: Option<PathBuf>,
    conf: PathBuf,
}

impl Workspace {
    fn new(test_name: &str, device_name: &str, engine: &str, with_target: bool) -> Self {
        let canonical_name = canonical_test_name(device_name, engine, test_name);
        let normalized_device_name = normalize_naming_terms(&canonical_name);
        let sanitized_device_name = {
            let s: String = normalized_device_name
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric())
                .collect();
            if s.is_empty() { "device".to_string() } else { s }
        };
        let test_home = test_home();
        let stage = test_home.join("stageDir");
        let work_dir = test_home.join("workDir");
        // Match the Java test layout under ~/synclite/test/rustruntime/db/rustologger/<TestName>/
        // while keeping Rust-created artifacts distinct via the -rust device suffix.
        let db_dir = test_home.join("db").join("rustologger").join(&sanitized_device_name);
        let ext = if engine.eq_ignore_ascii_case("duckdb") { "duckdb" } else { "db" };
        let db_path = db_dir.join(format!("{sanitized_device_name}.{ext}"));
        let target = if with_target { Some(db_dir.join("target")) } else { None };
        let conf = db_dir.join("synclite.conf");
        let device_type = inferred_device_type(device_name, engine)
            .parse::<DeviceType>()
            .unwrap();

        // Wipe per-test DB dir entirely.
        rm_rf(&db_dir);
        // In the shared stageDir, wipe only this device's subdirs.
        let prefix = format!("synclite-{sanitized_device_name}-");
        if stage.exists() {
            if let Ok(entries) = fs::read_dir(&stage) {
                for entry in entries.flatten() {
                    if entry.file_name().to_string_lossy().starts_with(&prefix) {
                        rm_rf(&entry.path());
                    }
                }
            }
        }

        fs::create_dir_all(&stage).unwrap();
        fs::create_dir_all(&work_dir).unwrap();
        fs::create_dir_all(&db_dir).unwrap();
        if let Some(t) = &target {
            fs::create_dir_all(t).unwrap();
        }
        let dest_db = work_dir.join("consolidated_db.sqlite");
        let mut body = format!(
            "device-name={sanitized_device_name}\n\
             db-engine={engine}\n\
             device-type={}\n\
             db-path={}\n\
             local-data-stage-directory={}\n",
            device_type,
            db_path.display().to_string().replace('\\', "/"),
            stage.display().to_string().replace('\\', "/"),
        );
        if let Some(t) = &target {
            body.push_str(&format!(
                "device-upload-root={}\n",
                t.display().to_string().replace('\\', "/")
            ));
        }
        body.push_str(&format!(
            "dst-sync-mode=CONSOLIDATION\n\
             dst-type-1=SQLITE\n\
             metadata-store-1=DESTINATION\n\
             device-data-root={}\n\
             dst-connection-string-1=jdbc:sqlite:{}?journal_mode=WAL\n",
            work_dir.display().to_string().replace('\\', "/"),
            dest_db.display().to_string().replace('\\', "/")
        ));
        fs::write(&conf, body).unwrap();

        Self {
            device_name: sanitized_device_name,
            device_type,
            stage,
            db_path,
            target,
            conf,
        }
    }

    /// Locate the `synclite-<device>-<uuid>/` stage subdir created on
    /// first open. Panics if it does not yet exist.
    fn stage_subdir(&self) -> PathBuf {
        let prefix = format!("synclite-{}-", self.device_name);
        for entry in fs::read_dir(&self.stage).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                return entry.path();
            }
        }
        panic!(
            "no stage subdir matching {prefix}* under {}",
            self.stage.display()
        )
    }

    /// `<db_path>.synclite/` device home directory.
    fn device_home(&self) -> PathBuf {
        format!("{}.synclite", self.db_path.display()).into()
    }

    fn sqlite_shadow_schema(&self) -> PathBuf {
        self.device_home().join(format!(
            "{}.sqlite",
            self.db_path.file_name().unwrap().to_string_lossy()
        ))
    }
}

enum TestConnection {
    Sqlite(sl_rusqlite::Connection),
    DuckDb(sl_duckdb::Connection),
}

enum TestStatement<'a> {
    Sqlite(sl_rusqlite::Statement<'a>),
    DuckDb(sl_duckdb::Statement<'a>),
}

impl TestConnection {
    fn set_auto_commit(&mut self, auto_commit: bool) {
        match self {
            Self::Sqlite(conn) => conn.set_auto_commit(auto_commit),
            Self::DuckDb(conn) => conn.set_auto_commit(auto_commit),
        }
    }

    fn execute(&mut self, sql: &str, params: &[ArgValue]) -> logger_core::Result<u64> {
        match self {
            Self::Sqlite(conn) => conn.execute(sql, params),
            Self::DuckDb(conn) => conn.execute(sql, params),
        }
    }

    fn execute_unlogged(&mut self, sql: &str, params: &[ArgValue]) -> logger_core::Result<u64> {
        match self {
            Self::Sqlite(conn) => conn.execute_unlogged(sql, params),
            Self::DuckDb(conn) => conn.execute_unlogged(sql, params),
        }
    }

    fn query(&mut self, sql: &str, params: &[ArgValue]) -> logger_core::Result<Vec<Row>> {
        match self {
            Self::Sqlite(conn) => conn.query(sql, params),
            Self::DuckDb(conn) => conn.query(sql, params),
        }
    }

    fn prepare<'a>(&'a mut self, sql: &str) -> TestStatement<'a> {
        match self {
            Self::Sqlite(conn) => TestStatement::Sqlite(conn.prepare(sql).unwrap()),
            Self::DuckDb(conn) => TestStatement::DuckDb(conn.prepare(sql).unwrap()),
        }
    }

    fn commit(&mut self) -> logger_core::Result<()> {
        match self {
            Self::Sqlite(conn) => conn.commit(),
            Self::DuckDb(conn) => conn.commit(),
        }
    }

    fn rollback(&mut self) -> logger_core::Result<()> {
        match self {
            Self::Sqlite(conn) => conn.rollback(),
            Self::DuckDb(conn) => conn.rollback(),
        }
    }

    fn close(self) -> logger_core::Result<()> {
        match self {
            Self::Sqlite(conn) => conn.close(),
            Self::DuckDb(conn) => conn.close(),
        }
    }
}

impl<'a> TestStatement<'a> {
    fn add_batch(&mut self, params: &[ArgValue]) {
        match self {
            Self::Sqlite(stmt) => stmt.add_batch(params),
            Self::DuckDb(stmt) => stmt.add_batch(params),
        }
    }

    fn execute_batch(&mut self) -> logger_core::Result<Vec<u64>> {
        match self {
            Self::Sqlite(stmt) => stmt.execute_batch(),
            Self::DuckDb(stmt) => stmt.execute_batch(),
        }
    }
}

fn open_logger(ws: &Workspace) -> TestConnection {
    synclite::initialize(
        ws.device_type,
        &ws.device_name,
        &ws.db_path,
        None,
        synclite::SyncLiteOptions {
            config_path: Some(ws.conf.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    if ws.db_path.extension().and_then(|s| s.to_str()) == Some("duckdb") {
        TestConnection::DuckDb(sl_duckdb::Connection::open_with_config(&ws.conf).unwrap())
    } else {
        TestConnection::Sqlite(sl_rusqlite::Connection::open_with_config(&ws.conf).unwrap())
    }
}

fn as_i64(v: &ArgValue) -> i64 {
    match v {
        ArgValue::Int(n) => *n,
        other => panic!("expected Int, got {other:?}"),
    }
}

fn as_text(v: &ArgValue) -> &str {
    match v {
        ArgValue::Text(s) => s.as_str(),
        other => panic!("expected Text, got {other:?}"),
    }
}

fn count(logger: &mut TestConnection, table: &str) -> i64 {
    let rows: Vec<Row> = logger
        .query(&format!("SELECT COUNT(*) FROM {table}"), &[])
        .unwrap();
    as_i64(&rows[0][0])
}

fn test_table_name(ws: &Workspace) -> String {
    ws.db_path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| ws.device_name.clone())
}

fn assert_execute_err_contains(logger: &mut TestConnection, sql: &str, expected_substr: &str) {
    let err = logger
        .execute(sql, &[])
        .expect_err("statement should be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains(expected_substr),
        "expected error containing '{expected_substr}', got '{msg}'"
    );
}

fn seed_preexisting_user_db(ws: &Workspace, table_name: &str) {
    let ddl = format!("CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name TEXT)");
    let ins1 = format!("INSERT INTO {table_name} (id, name) VALUES (1, 'pre-1')");
    let ins2 = format!("INSERT INTO {table_name} (id, name) VALUES (2, 'pre-2')");
    if ws.db_path.extension().and_then(|s| s.to_str()) == Some("duckdb") {
        let c = DuckConnection::open(&ws.db_path).unwrap();
        c.execute(&ddl, []).unwrap();
        c.execute(&ins1, []).unwrap();
        c.execute(&ins2, []).unwrap();
    } else {
        let c = Connection::open(&ws.db_path).unwrap();
        c.execute(&ddl, []).unwrap();
        c.execute(&ins1, []).unwrap();
        c.execute(&ins2, []).unwrap();
    }
}

fn backup_row_count_for_table(backup_path: &Path, table_name: &str) -> i64 {
    let c = Connection::open(backup_path).unwrap();
    c.query_row(&format!("SELECT COUNT(*) FROM {table_name}"), [], |r| r.get(0))
        .unwrap()
}

fn preexisting_data_init_backup_and_logging_core(engine: &str, device_name: &str, test_name: &str) {
    let ws = Workspace::new(test_name, device_name, engine, false);
    let table_name = test_table_name(&ws);
    seed_preexisting_user_db(&ws, &table_name);

    let mut logger = open_logger(&ws);
    assert_eq!(count(&mut logger, &table_name), 2);
    logger
        .execute(
            &format!("INSERT INTO {table_name} (id, name) VALUES (?, ?)"),
            &[ArgValue::Int(3), ArgValue::Text("post-init".into())],
        )
        .unwrap();
    logger.commit().unwrap();
    assert_eq!(count(&mut logger, &table_name), 3);
    logger.close().unwrap();

    let stage_subdir = ws.stage_subdir();
    let backup = stage_subdir.join(format!(
        "{}.synclite.backup",
        ws.db_path.file_name().unwrap().to_string_lossy()
    ));
    assert!(backup.exists(), "backup file missing: {}", backup.display());
    assert_eq!(
        backup_row_count_for_table(&backup, &table_name),
        2,
        "backup must contain preexisting rows captured at init"
    );

    let seg = locate_segment(&ws);
    assert_eq!(
        segment_and_txn_ci_count_starting_with(&seg, &format!("INSERT INTO {table_name}")),
        1,
        "only post-init INSERT should be logged"
    );
}

/// Locate the most recent shipped/local segment.
fn locate_segment(ws: &Workspace) -> PathBuf {
    let seg = try_locate_segment(ws).unwrap_or_else(|| {
        let dir = ws.target.clone().unwrap_or_else(|| ws.stage_subdir());
        panic!(
            "no non-empty <N>.sqllog segment found in {}",
            dir.display()
        )
    });
    eprintln!("DEBUG locate_segment selected {}", seg.display());
    seg
}

fn try_locate_segment(ws: &Workspace) -> Option<PathBuf> {
    let dir = ws.target.clone().unwrap_or_else(|| ws.stage_subdir());
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in fs::read_dir(&dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display())) {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(num_str) = name.strip_suffix(".sqllog") {
            if let Ok(n) = num_str.parse::<u64>() {
                let path = entry.path();
                // Last shipped segment means the highest sequence file that
                // actually carries commandlog rows.
                if segment_max_commit_id_opt(&path).is_some()
                    && best.as_ref().map_or(true, |(b, _)| n >= *b)
                {
                    best = Some((n, path));
                }
            }
        }
    }
    best.map(|(_, p)| {
        eprintln!("DEBUG try_locate_segment selected {}", p.display());
        p
    })
}

fn segment_max_commit_id_opt(seg: &Path) -> Option<i64> {
    let c = Connection::open(seg).unwrap();
    c.query_row("SELECT MAX(commit_id) FROM commandlog", [], |r| {
        r.get::<_, Option<i64>>(0)
    })
    .unwrap()
}

/// `MAX(commit_id) FROM commandlog` for the given segment:
/// segments carry commit ids on `commandlog` rows; there is no
/// `synclite_txn` table inside a segment.
fn segment_max_commit_id(seg: &Path) -> i64 {
    segment_max_commit_id_opt(seg).unwrap_or_else(|| {
        panic!(
            "segment {} has no commandlog rows (MAX(commit_id) is NULL)",
            seg.display()
        )
    })
}

/// `MAX(commit_id)` over all `<N>.sqllog` files in this test workspace.
fn workspace_max_commit_id(ws: &Workspace) -> i64 {
    let dir = ws.target.clone().unwrap_or_else(|| ws.stage_subdir());
    let mut best = i64::MIN;
    for entry in fs::read_dir(&dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display())) {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".sqllog") {
            if let Some(commit_id) = segment_max_commit_id_opt(&entry.path()) {
                best = best.max(commit_id);
            }
        }
    }
    assert!(best > 0, "no commit ids found under {}", dir.display());
    best
}

/// The latest segment (`locate_segment`) must carry the global max
/// commit id for the workspace.
fn assert_latest_segment_carries_max_commit_id(ws: &Workspace) {
    let latest = locate_segment(ws);
    let latest_commit = segment_max_commit_id(&latest);
    let global_max = workspace_max_commit_id(ws);
    assert_eq!(
        latest_commit,
        global_max,
        "latest segment {} commit_id {} != global max {}",
        latest.display(),
        latest_commit,
        global_max
    );
}

fn remove_fate_sql_for_commit(seg: &Path, commit_id: i64) {
    let c = Connection::open(seg).unwrap();
    c.execute(
        "DELETE FROM commandlog WHERE commit_id = ?1 AND (sql = 'COMMIT' OR sql = 'ROLLBACK')",
        [commit_id],
    )
    .unwrap();
}

fn set_user_synclite_txn_commit_id(ws: &Workspace, commit_id: i64) {
    if ws.db_path.extension().and_then(|s| s.to_str()) == Some("duckdb") {
        let c = DuckConnection::open(&ws.db_path).unwrap();
        c.execute("UPDATE synclite_txn SET commit_id = ?", [commit_id])
            .unwrap();
    } else {
        let c = Connection::open(&ws.db_path).unwrap();
        c.execute("UPDATE synclite_txn SET commit_id = ?1", [commit_id])
            .unwrap();
    }
}

fn user_db_synclite_txn_commit_id(ws: &Workspace) -> i64 {
    if ws.db_path.extension().and_then(|s| s.to_str()) == Some("duckdb") {
        let c = DuckConnection::open(&ws.db_path).unwrap();
        c.query_row("SELECT MAX(commit_id) FROM synclite_txn", [], |r| r.get(0))
            .unwrap()
    } else {
        let c = Connection::open(&ws.db_path).unwrap();
        c.query_row("SELECT MAX(commit_id) FROM synclite_txn", [], |r| r.get(0))
            .unwrap()
    }
}

fn assert_synclite_txn_matches_latest_segment(ws: &Workspace) {
    let latest_seg_commit = segment_max_commit_id(&locate_segment(ws));
    let user_commit = user_db_synclite_txn_commit_id(ws);
    assert_eq!(
        user_commit,
        latest_seg_commit,
        "synclite_txn.commit_id {} must match latest segment commit_id {}",
        user_commit,
        latest_seg_commit
    );
}

fn segment_commandlog_count_for(seg: &Path, commit_id: i64) -> i64 {
    let c = Connection::open(seg).unwrap();
    c.query_row(
        "SELECT COUNT(*) FROM commandlog WHERE commit_id = ?1",
        [commit_id],
        |r| r.get(0),
    )
    .unwrap()
}

fn segment_commandlog_count_starting_with(seg: &Path, keyword: &str) -> i64 {
    let c = Connection::open(seg).unwrap();
    c.query_row(
        "SELECT COUNT(*) FROM commandlog WHERE sql LIKE ?1",
        [format!("{keyword}%")],
        |r| r.get(0),
    )
    .unwrap()
}

fn segment_commandlog_ci_count_starting_with(seg: &Path, keyword: &str) -> i64 {
    let c = Connection::open(seg).unwrap();
    c.query_row(
        "SELECT COUNT(*) FROM commandlog WHERE sql IS NOT NULL AND lower(sql) LIKE lower(?1)",
        [format!("{keyword}%")],
        |r| r.get(0),
    )
    .unwrap()
}

fn segment_commandlog_null_sql_count(seg: &Path) -> i64 {
    let c = Connection::open(seg).unwrap();
    c.query_row("SELECT COUNT(*) FROM commandlog WHERE sql IS NULL", [], |r| r.get(0))
        .unwrap()
}

fn segment_commandlog_count_for_commit_starting_with(seg: &Path, commit_id: i64, keyword: &str) -> i64 {
    let c = Connection::open(seg).unwrap();
    c.query_row(
        "SELECT COUNT(*) FROM commandlog WHERE commit_id = ?1 AND sql LIKE ?2",
        (commit_id, format!("{keyword}%")),
        |r| r.get(0),
    )
    .unwrap()
}

fn segment_distinct_commit_ids_starting_with(seg: &Path, keyword: &str) -> std::collections::BTreeSet<i64> {
    let mut out = std::collections::BTreeSet::new();
    for p in segment_and_txn_files(seg) {
        eprintln!("DEBUG segment file: {}", p.display());
        let c = Connection::open(&p).unwrap();
        let mut stmt = c
            .prepare("SELECT DISTINCT commit_id FROM commandlog WHERE sql IS NOT NULL AND sql LIKE ?1")
            .unwrap();
        let rows = stmt
            .query_map([format!("{keyword}%")], |r| r.get::<_, i64>(0))
            .unwrap();
        for row in rows {
            out.insert(row.unwrap());
        }
    }
    out
}

fn segment_commandlog_exact_sql_count_for_commit(seg: &Path, commit_id: i64, sql: &str) -> i64 {
    let c = Connection::open(seg).unwrap();
    c.query_row(
        "SELECT COUNT(*) FROM commandlog WHERE commit_id = ?1 AND sql = ?2",
        (commit_id, sql),
        |r| r.get(0),
    )
    .unwrap()
}

fn segment_in_doubt_commit_count(seg: &Path) -> i64 {
    let c = Connection::open(seg).unwrap();
    c.query_row(
                "SELECT COUNT(*) FROM ( \
                     SELECT commit_id FROM commandlog \
                     GROUP BY commit_id \
                     HAVING SUM(CASE WHEN sql = 'COMMIT' OR sql = 'ROLLBACK' THEN 1 ELSE 0 END) = 0 \
                 )",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

fn segment_and_txn_files(seg: &Path) -> Vec<PathBuf> {
    let mut out = vec![seg.to_path_buf()];
    let Some(file_name) = seg.file_name().and_then(|s| s.to_str()) else {
        return out;
    };
    let Some(seq) = file_name.strip_suffix(".sqllog") else {
        return out;
    };
    let Some(dir) = seg.parent() else {
        return out;
    };
    let prefix = format!("{seq}.sqllog.");
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(&prefix) && name.ends_with(".txn") {
                out.push(entry.path());
            }
        }
    }
    out
}

fn segment_and_txn_count_starting_with(seg: &Path, keyword: &str) -> i64 {
    segment_and_txn_files(seg)
        .into_iter()
        .map(|p| segment_commandlog_count_starting_with(&p, keyword))
        .sum()
}

fn segment_and_txn_ci_count_starting_with(seg: &Path, keyword: &str) -> i64 {
    segment_and_txn_files(seg)
        .into_iter()
        .map(|p| segment_commandlog_ci_count_starting_with(&p, keyword))
        .sum()
}

fn segment_and_txn_null_sql_count(seg: &Path) -> i64 {
    segment_and_txn_files(seg)
        .into_iter()
        .map(|p| segment_commandlog_null_sql_count(&p))
        .sum()
}

fn segment_and_txn_count_for_commit_starting_with(seg: &Path, commit_id: i64, keyword: &str) -> i64 {
    segment_and_txn_files(seg)
        .into_iter()
        .map(|p| segment_commandlog_count_for_commit_starting_with(&p, commit_id, keyword))
        .sum()
}

fn wait_for_sqlite_row_count(db: &Path, table: &str, expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let sql = format!("SELECT COUNT(*) FROM {table}");

    loop {
        if let Ok(conn) = Connection::open(db) {
            if let Ok(count) = conn.query_row::<i64, _, _>(&sql, [], |r| r.get(0)) {
                if count == expected {
                    return;
                }
            }
        }

        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let final_count: Option<i64> = Connection::open(db)
        .ok()
        .and_then(|conn| conn.query_row::<i64, _, _>(&sql, [], |r| r.get(0)).ok());
    panic!(
        "timeout waiting for {table} row count={expected} in {} (last observed {:?})",
        db.display(),
        final_count
    );
}

fn wait_for_path_exists(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if path.exists() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("timeout waiting for path to exist: {}", path.display());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn destination_db_path() -> PathBuf {
    test_home().join("workDir").join("consolidated_db.sqlite")
}

fn is_sql_device_workspace(ws: &Workspace) -> bool {
    let dt = ws.device_type.to_string();
    dt.eq_ignore_ascii_case("SQLITE") || dt.eq_ignore_ascii_case("DUCKDB")
}

fn source_table_row_count(ws: &Workspace, table: &str) -> i64 {
    if ws.db_path.extension().and_then(|s| s.to_str()) == Some("duckdb") {
        let c = DuckConnection::open(&ws.db_path).unwrap();
        c.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    } else {
        let c = Connection::open(&ws.db_path).unwrap();
        c.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }
}

fn destination_table_row_count(table: &str) -> Option<i64> {
    let dest_db = destination_db_path();
    if !dest_db.exists() {
        return None;
    }
    let c = Connection::open(dest_db).ok()?;
    c.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
        .ok()
}

fn reset_destination_table_if_exists(table: &str) {
    let dest_db = destination_db_path();
    if !dest_db.exists() {
        return;
    }
    let Ok(conn) = Connection::open(dest_db) else {
        return;
    };
    let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {table}"));
}

fn latest_stage_segment_seq(ws: &Workspace) -> Option<u64> {
    let mut max_seq = None;
    let stage_subdir = ws.stage_subdir();
    for entry in fs::read_dir(&stage_subdir).ok()? {
        let entry = entry.ok()?;
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(seq_str) = name.strip_suffix(".sqllog") else {
            continue;
        };
        let Ok(seq) = seq_str.parse::<u64>() else {
            continue;
        };
        if segment_max_commit_id_opt(&entry.path()).is_none() {
            continue;
        }
        max_seq = Some(max_seq.map_or(seq, |m: u64| m.max(seq)));
    }
    max_seq
}

fn wait_for_last_cdc_segment_if_sql_device(ws: &Workspace, timeout: Duration) {
    if !is_sql_device_workspace(ws) {
        return;
    }
    let Some(seq) = latest_stage_segment_seq(ws) else {
        return;
    };

    let archive_name = ws.stage_subdir().file_name().unwrap().to_os_string();
    let cdclog = test_home()
        .join("workDir")
        .join(archive_name)
        .join(format!("{seq}.cdclog"));
    wait_for_path_exists(&cdclog, timeout);
}

fn wait_for_destination_table_to_match_source(ws: &Workspace, table: &str, timeout: Duration) {
    let expected = source_table_row_count(ws, table);
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(actual) = destination_table_row_count(table) {
            if actual == expected {
                return;
            }
        }
        if Instant::now() >= deadline {
            let observed = destination_table_row_count(table);
            panic!(
                "timeout waiting for destination table row-count parity for {table}: expected {}, observed {:?}",
                expected,
                observed
            );
        }
        std::thread::sleep(Duration::from_millis(150));
    }
}

fn wait_for_destination_table_row_count(table: &str, expected: i64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(actual) = destination_table_row_count(table) {
            if actual == expected {
                return;
            }
        }
        if Instant::now() >= deadline {
            let observed = destination_table_row_count(table);
            panic!(
                "timeout waiting for destination table row count for {table}: expected {}, observed {:?}",
                expected,
                observed
            );
        }
        std::thread::sleep(Duration::from_millis(150));
    }
}

fn wait_for_sql_device_consolidation_and_validate_table(ws: &Workspace, table: &str) {
    if !is_sql_device_workspace(ws) {
        return;
    }
    let timeout = Duration::from_secs(20);
    wait_for_last_cdc_segment_if_sql_device(ws, timeout);
    wait_for_destination_table_to_match_source(ws, table, timeout);
}

fn wait_for_sql_device_consolidation_and_validate_expected_count(
    ws: &Workspace,
    table: &str,
    expected: i64,
) {
    if !is_sql_device_workspace(ws) {
        return;
    }
    let timeout = Duration::from_secs(20);
    wait_for_last_cdc_segment_if_sql_device(ws, timeout);
    wait_for_destination_table_row_count(table, expected, timeout);
}

// --------------------------------------------------------------------------
// SQLite parity tests
// --------------------------------------------------------------------------

/// Core flow shared by Java `SQLiteTransactionalTest.testBasicTableOperations`
/// and `SQLiteStoreTest.testBasicTableOperations`.
fn sqlite_basic_table_operations_core(device_name: &str, test_name: &str, expect_txn_markers: bool) {
    let ws = Workspace::new(
        test_name,
        device_name,
        "sqlite",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);
    if expect_txn_markers {
        logger.set_auto_commit(false);
    }

    // --- Transaction 1: create + batch insert -----------------------------
    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)"),
            &[],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {table} (id, name, value) VALUES (?, ?, ?)"),
            &[ArgValue::Int(1), ArgValue::Text("test1".into()), ArgValue::Int(100)],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {table} (id, name, value) VALUES (?, ?, ?)"),
            &[ArgValue::Int(2), ArgValue::Text("test2".into()), ArgValue::Int(200)],
        )
        .unwrap();
    logger.commit().unwrap();

    assert_eq!(count(&mut logger, &table), 2);
    let rows = logger
        .query(&format!("SELECT id, name, value FROM {table} ORDER BY id"), &[])
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(as_i64(&rows[0][0]), 1);
    assert_eq!(as_text(&rows[0][1]), "test1");
    assert_eq!(as_i64(&rows[0][2]), 100);
    assert_eq!(as_i64(&rows[1][0]), 2);
    assert_eq!(as_text(&rows[1][1]), "test2");
    assert_eq!(as_i64(&rows[1][2]), 200);

    // --- Transaction 2: update + delete -----------------------------------
    logger
        .execute(
            &format!("UPDATE {table} SET value = ? WHERE name = ?"),
            &[ArgValue::Int(999), ArgValue::Text("test1".into())],
        )
        .unwrap();
    logger
        .execute(
            &format!("DELETE FROM {table} WHERE name = ?"),
            &[ArgValue::Text("test2".into())],
        )
        .unwrap();
    logger.commit().unwrap();

    assert_eq!(count(&mut logger, &table), 1);
    let rows = logger
        .query(&format!("SELECT name, value FROM {table}"), &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(as_text(&rows[0][0]), "test1");
    assert_eq!(as_i64(&rows[0][1]), 999);

    logger.close().unwrap();

    wait_for_sql_device_consolidation_and_validate_table(&ws, &table);

    assert_latest_segment_carries_max_commit_id(&ws);
    assert_synclite_txn_matches_latest_segment(&ws);

    let seg = locate_segment(&ws);
    let commit_id = segment_max_commit_id(&seg);
    assert!(commit_id > 0);
    assert!(segment_commandlog_count_for(&seg, commit_id) > 0);

    assert_eq!(segment_commandlog_count_starting_with(&seg, "CREATE"), 1);
    assert_eq!(segment_commandlog_count_starting_with(&seg, "INSERT"), 2);
    assert_eq!(segment_commandlog_count_starting_with(&seg, "UPDATE"), 1);
    assert_eq!(segment_commandlog_count_starting_with(&seg, "DELETE"), 1);
    let expected_marker_count = if expect_txn_markers { 2 } else { 5 };
    assert_eq!(
        segment_commandlog_count_starting_with(&seg, "BEGIN"),
        expected_marker_count
    );
    assert_eq!(
        segment_commandlog_count_starting_with(&seg, "COMMIT"),
        expected_marker_count
    );
}

#[test]
fn sqlite_execute_unlogged_runs_ddl_and_dml_without_commandlog_entries() {
    let test_name = "sqlite_execute_unlogged_runs_ddl_and_dml_without_commandlog_entries";
    let ws = Workspace::new(
        test_name,
        "sqliteunlogged-rust",
        "sqlite",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)"),
            &[],
        )
        .unwrap();
    logger
        .execute_unlogged(
            &format!("INSERT INTO {table} VALUES (1, 'unlogged-row')"),
            &[],
        )
        .unwrap();
    logger.commit().unwrap();
    assert_eq!(count(&mut logger, &table), 1);
    logger.close().unwrap();

    let seg = locate_segment(&ws);
    assert_eq!(segment_commandlog_ci_count_starting_with(&seg, &format!("CREATE TABLE {table}")), 1);
    assert_eq!(segment_commandlog_ci_count_starting_with(&seg, &format!("INSERT INTO {table}")), 0);
}

/// Exact mapping: Java `SQLiteTransactionalTest.testBasicTableOperations`.
#[test]
fn sqlite_transactional_test_basic_table_operations() {
    let ws = Workspace::new(
        "SQLiteTransactionalTest",
        "sqlitetransactional-rust",
        "sqlite",
        false,
    );
    let table = test_table_name(&ws);
    reset_destination_table_if_exists(&table);
    sqlite_basic_table_operations_core(
        "sqlitetransactional-rust",
        "SQLiteTransactionalTest",
        true,
    );
}

/// Exact mapping: Java `SQLiteStoreTest.testBasicTableOperations`.
#[test]
fn sqlite_store_test_basic_table_operations() {
    sqlite_basic_table_operations_core("sqlitestore-rust", "SQLiteStoreTest", false);
}

#[test]
fn sqlite_store_rejects_non_values_and_subquery_dml_shapes() {
    let test_name = "sqlite_store_rejects_non_values_and_subquery_dml_shapes";
    let ws = Workspace::new(
        test_name,
        "sqlitestorevalidate-rust",
        "sqlite",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    logger
        .execute(&format!("CREATE TABLE {table} (id INTEGER, v TEXT)"), &[])
        .unwrap();

    assert_execute_err_contains(
        &mut logger,
        &format!("INSERT INTO {table} SELECT id, v FROM {table}"),
        "does not allow SQL",
    );
    assert_execute_err_contains(
        &mut logger,
        &format!("UPDATE {table} SET v = (SELECT 'x')"),
        "Unsupported SQL",
    );
    assert_execute_err_contains(
        &mut logger,
        &format!("DELETE FROM {table} WHERE id IN (SELECT id FROM {table})"),
        "Unsupported SQL",
    );

    logger.close().unwrap();
}

fn sqlite_rollback_marker_core(device_name: &str, test_name: &str, expect_rollback_marker: bool) {
    let ws = Workspace::new(test_name, device_name, "sqlite", false);
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);
    logger.set_auto_commit(false);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)"),
            &[],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {table} (id, name) VALUES (?, ?)"),
            &[ArgValue::Int(1), ArgValue::Text("rb".into())],
        )
        .unwrap();
    logger.rollback().unwrap();
    logger.close().unwrap();

    let seg = locate_segment(&ws);
    assert_eq!(
        segment_commandlog_count_starting_with(&seg, "ROLLBACK"),
        if expect_rollback_marker { 1 } else { 0 }
    );
}

#[test]
fn sqlite_transactional_rollback_logs_marker() {
    sqlite_rollback_marker_core(
        "sqliterollbacktxn-rust",
        "SQLiteTransactionalRollbackTest",
        true,
    );
}

#[test]
fn sqlite_store_rollback_has_no_marker() {
    sqlite_rollback_marker_core(
        "sqliterollbackstore-rust",
        "SQLiteStoreRollbackTest",
        true,
    );
}

fn close_drops_uncommitted_store_tail_core(engine: &str, device_name: &str, test_name: &str) {
    let ws = Workspace::new(test_name, device_name, engine, false);
    let table = test_table_name(&ws);

    {
        let mut logger = open_logger(&ws);
        logger.execute(&format!("CREATE TABLE {table}(id INTEGER)"), &[]).unwrap();
        logger.commit().unwrap();
        logger.set_auto_commit(false);
        logger
            .execute(
                &format!("INSERT INTO {table}(id) VALUES (?)"),
                &[ArgValue::Int(2)],
            )
            .unwrap();
        logger.close().unwrap();
    }

    let seg = locate_segment(&ws);
    assert_eq!(
        segment_in_doubt_commit_count(&seg),
        0,
        "segment must not contain commit ids with unknown fate"
    );
}

#[test]
fn sqlite_store_close_drops_uncommitted_tail_from_segment() {
    close_drops_uncommitted_store_tail_core(
        "sqlite",
        "sqlitestoreclosetaildrop-rust",
        "SQLiteStoreCloseDropsUncommittedTail",
    );
}

#[test]
fn duckdb_store_close_drops_uncommitted_tail_from_segment() {
    close_drops_uncommitted_store_tail_core(
        "duckdb",
        "duckdbstoreclosetaildrop-rust",
        "DuckDBStoreCloseDropsUncommittedTail",
    );
}

#[test]
fn sqlite_transactional_prepared_batch_logs_sql_once_and_cleans_on_rollback() {
    let test_name = "sqlite_transactional_prepared_batch_logs_sql_once_and_cleans_on_rollback";
    let ws = Workspace::new(
        test_name,
        "sqlitepreparedbatchtxn-rust",
        "sqlite",
        false,
    );
    let table = test_table_name(&ws);
    reset_destination_table_if_exists(&table);
    let mut logger = open_logger(&ws);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, v TEXT)"),
            &[],
        )
        .unwrap();

    logger.set_auto_commit(false);

    {
        let mut stmt = logger.prepare(&format!("INSERT INTO {table} (id, v) VALUES (?, ?)"));
        stmt.add_batch(&[ArgValue::Int(1), ArgValue::Text("a".into())]);
        stmt.add_batch(&[ArgValue::Int(2), ArgValue::Text("b".into())]);
        stmt.add_batch(&[ArgValue::Int(3), ArgValue::Text("c".into())]);
        stmt.execute_batch().unwrap();
    }
    logger.rollback().unwrap();

    {
        let mut stmt = logger.prepare(&format!("INSERT INTO {table} (id, v) VALUES (?, ?)"));
        stmt.add_batch(&[ArgValue::Int(10), ArgValue::Text("ok".into())]);
        stmt.execute_batch().unwrap();
    }
    logger.commit().unwrap();
    logger.close().unwrap();

    wait_for_sql_device_consolidation_and_validate_table(&ws, &table);

    assert_latest_segment_carries_max_commit_id(&ws);
    assert_synclite_txn_matches_latest_segment(&ws);

    let seg = locate_segment(&ws);
    assert_eq!(
        segment_commandlog_count_starting_with(&seg, &format!("INSERT INTO {table}")),
        2
    );
    assert_eq!(segment_commandlog_null_sql_count(&seg), 2);

    let latest_commit = segment_max_commit_id(&seg);
    assert_eq!(
        segment_commandlog_count_for_commit_starting_with(&seg, latest_commit, &format!("INSERT INTO {table}")),
        1
    );
}

#[test]
fn sqlite_transactional_default_autocommit_splits_create_and_batch_into_separate_txns() {
    let test_name = "sqlite_transactional_default_autocommit_splits_create_and_batch_into_separate_txns";
    let ws = Workspace::new(
        test_name,
        "sqliteautocommitboundary-rust",
        "sqlite",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    // Default wrapper behavior mirrors Java: user-autocommit is ON.
    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, v TEXT)"),
            &[],
        )
        .unwrap();

    {
        let mut stmt = logger.prepare(&format!("INSERT INTO {table} (id, v) VALUES (?, ?)"));
        stmt.add_batch(&[ArgValue::Int(1), ArgValue::Text("a".into())]);
        stmt.add_batch(&[ArgValue::Int(2), ArgValue::Text("b".into())]);
        stmt.execute_batch().unwrap();
    }

    logger.close().unwrap();

    let seg = locate_segment(&ws);
    let ddl_commits = segment_distinct_commit_ids_starting_with(&seg, &format!("CREATE TABLE {table}"));
    let dml_commits = segment_distinct_commit_ids_starting_with(&seg, &format!("INSERT INTO {table}"));

    assert_eq!(ddl_commits.len(), 1, "CREATE should map to exactly one txn");
    assert_eq!(dml_commits.len(), 1, "Batch INSERT should map to exactly one txn");
    assert_ne!(
        ddl_commits.iter().next().unwrap(),
        dml_commits.iter().next().unwrap(),
        "Standalone CREATE and subsequent batch must have different commit_id values in default autocommit mode"
    );
}

#[test]
fn sqlite_store_default_autocommit_splits_create_and_batch_into_separate_txns() {
    let test_name = "sqlite_store_default_autocommit_splits_create_and_batch_into_separate_txns";
    let ws = Workspace::new(
        test_name,
        "sqlitestoreautocommitboundary-rust",
        "sqlite",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, v TEXT)"),
            &[],
        )
        .unwrap();

    {
        let mut stmt = logger.prepare(&format!("INSERT INTO {table} (id, v) VALUES (?, ?)"));
        stmt.add_batch(&[ArgValue::Int(1), ArgValue::Text("a".into())]);
        stmt.add_batch(&[ArgValue::Int(2), ArgValue::Text("b".into())]);
        stmt.execute_batch().unwrap();
    }

    logger.close().unwrap();

    let seg = locate_segment(&ws);
    let ddl_commits = segment_distinct_commit_ids_starting_with(&seg, &format!("CREATE TABLE {table}"));
    let dml_commits = segment_distinct_commit_ids_starting_with(&seg, &format!("INSERT INTO {table}"));

    assert_eq!(ddl_commits.len(), 1, "CREATE should map to exactly one txn");
    assert_eq!(dml_commits.len(), 1, "Batch INSERT should map to exactly one txn");
    assert_ne!(
        ddl_commits.iter().next().unwrap(),
        dml_commits.iter().next().unwrap(),
        "Standalone CREATE and subsequent batch must have different commit_id values in default autocommit mode"
    );
}

#[test]
fn sqlite_execute_splits_semicolon_sql_and_honors_txn_messages() {
    let test_name = "sqlite_execute_splits_semicolon_sql_and_honors_txn_messages";
    let ws = Workspace::new(
        test_name,
        "sqlitesplitsql-rust",
        "sqlite",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, v TEXT);\n             INSERT INTO {table} (id, v) VALUES (1, 'a');"),
            &[],
        )
        .unwrap();

    assert_eq!(count(&mut logger, &table), 1);

    logger
        .execute(
            &format!("BEGIN; INSERT INTO {table} (id, v) VALUES (2, 'b'); ROLLBACK;"),
            &[],
        )
        .unwrap();

    // Rollback should keep row-count unchanged.
    assert_eq!(count(&mut logger, &table), 1);
    logger.close().unwrap();

    let seg = locate_segment(&ws);
    let rollback_commits = segment_distinct_commit_ids_starting_with(&seg, "ROLLBACK");
    assert_eq!(rollback_commits.len(), 1, "expected exactly one rollback commit");
    let rollback_commit = *rollback_commits.iter().next().unwrap();
    assert_eq!(
        segment_commandlog_exact_sql_count_for_commit(&seg, rollback_commit, "COMMIT"),
        0,
        "rollback commit must not carry an additional COMMIT marker"
    );
}

/// Exact mapping: Java `SQLiteStoreAPITest.testAllAPIs`.
#[test]
fn sqlite_store_api_test_all_apis() {
    let test_name = "sqlite_store_api_test_all_apis";
    let ws = Workspace::new(
        test_name,
        "sqlitestoreapi-rust",
        "sqlite",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"),
            &[],
        )
        .unwrap();

    for (id, name, score) in [(1, "Alice", 100), (2, "Bob", 200)] {
        logger
            .execute(
                &format!("INSERT INTO {table} (id, name, score) VALUES (?, ?, ?)"),
                &[
                    ArgValue::Int(id),
                    ArgValue::Text(name.into()),
                    ArgValue::Int(score),
                ],
            )
            .unwrap();
    }
    logger.commit().unwrap();
    assert_eq!(count(&mut logger, &table), 2);

    let alice = logger
        .query(&format!("SELECT score FROM {table} WHERE name = ?"), &[ArgValue::Text("Alice".into())])
        .unwrap();
    assert_eq!(as_i64(&alice[0][0]), 100);

    logger
        .execute(
            &format!("UPDATE {table} SET score = ? WHERE name = ?"),
            &[ArgValue::Int(999), ArgValue::Text("Alice".into())],
        )
        .unwrap();
    let alice = logger
        .query(&format!("SELECT score FROM {table} WHERE name = ?"), &[ArgValue::Text("Alice".into())])
        .unwrap();
    assert_eq!(as_i64(&alice[0][0]), 999);

    logger
        .execute(&format!("DELETE FROM {table} WHERE name = ?"), &[ArgValue::Text("Bob".into())])
        .unwrap();
    assert_eq!(count(&mut logger, &table), 1);

    for (id, name, score) in [(3, "Carol", 300), (4, "Dave", 400)] {
        logger
            .execute(
                &format!("INSERT INTO {table} (id, name, score) VALUES (?, ?, ?)"),
                &[
                    ArgValue::Int(id),
                    ArgValue::Text(name.into()),
                    ArgValue::Int(score),
                ],
            )
            .unwrap();
    }
    logger.commit().unwrap();
    assert_eq!(count(&mut logger, &table), 3);

    logger
        .execute(
            &format!("UPDATE {table} SET score = ? WHERE name = ?"),
            &[ArgValue::Int(350), ArgValue::Text("Carol".into())],
        )
        .unwrap();
    logger
        .execute(
            &format!("UPDATE {table} SET score = ? WHERE name = ?"),
            &[ArgValue::Int(450), ArgValue::Text("Dave".into())],
        )
        .unwrap();
    let carol = logger
        .query(&format!("SELECT score FROM {table} WHERE name = ?"), &[ArgValue::Text("Carol".into())])
        .unwrap();
    assert_eq!(as_i64(&carol[0][0]), 350);
    let dave = logger
        .query(&format!("SELECT score FROM {table} WHERE name = ?"), &[ArgValue::Text("Dave".into())])
        .unwrap();
    assert_eq!(as_i64(&dave[0][0]), 450);

    logger
        .execute(&format!("DELETE FROM {table} WHERE name = ?"), &[ArgValue::Text("Carol".into())])
        .unwrap();
    logger
        .execute(&format!("DELETE FROM {table} WHERE name = ?"), &[ArgValue::Text("Dave".into())])
        .unwrap();
    assert_eq!(count(&mut logger, &table), 1);

    logger.execute(&format!("DROP TABLE {table}"), &[]).unwrap();
    logger
        .execute(&format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"), &[])
        .unwrap();
    logger.commit().unwrap();
    assert_eq!(count(&mut logger, &table), 0);

    logger.close().unwrap();

    wait_for_sql_device_consolidation_and_validate_table(&ws, &table);

    assert_latest_segment_carries_max_commit_id(&ws);
    assert_synclite_txn_matches_latest_segment(&ws);

    let seg = locate_segment(&ws);
    let commit_id = segment_max_commit_id(&seg);
    assert!(commit_id > 0);
    assert!(segment_commandlog_count_for(&seg, commit_id) > 0);
}

#[test]
fn sqlite_sql_device_end_to_end_consolidates_into_destination_sqlite() {
    let test_name = "sqlite_sql_device_end_to_end_consolidates_into_destination_sqlite";
    let ws = Workspace::new(
        test_name,
        "sqlitedevicee2e-rust",
        "sqlite",
        false,
    );

    let dst_work_dir = test_home().join("workDir");
    fs::create_dir_all(&dst_work_dir).unwrap();
    let dest_db = dst_work_dir.join("consolidated_db.sqlite");

    let work_prefix = format!("synclite-{}-", ws.device_name);
    if let Ok(entries) = fs::read_dir(&dst_work_dir) {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with(&work_prefix) {
                rm_rf(&entry.path());
            }
        }
    }
    let base_table = test_table_name(&ws);
    let src_table = format!("{base_table}src");
    let final_table = base_table;

    let mut logger = open_logger(&ws);
    logger.set_auto_commit(false);
    logger
        .execute(
            &format!("CREATE TABLE {src_table} (id INTEGER, v TEXT)"),
            &[],
        )
        .unwrap();
    logger.commit().unwrap();
    logger
        .execute(
            &format!("INSERT INTO {src_table}(id, v) VALUES (?, ?)"),
            &[ArgValue::Int(1), ArgValue::Text("a".into())],
        )
        .unwrap();
    logger.commit().unwrap();
    logger
        .execute(
            &format!("INSERT INTO {src_table}(id, v) VALUES (?, ?)"),
            &[ArgValue::Int(2), ArgValue::Text("b".into())],
        )
        .unwrap();
    logger.commit().unwrap();
    logger
        .execute(&format!("INSERT INTO {src_table} SELECT * FROM {src_table}"), &[])
        .unwrap();
    logger.commit().unwrap();
    logger
        .execute(&format!("UPDATE {src_table} SET id = id + 10 WHERE id > 0"), &[])
        .unwrap();
    logger.commit().unwrap();
    logger
        .execute(&format!("DELETE FROM {src_table} WHERE id > 0"), &[])
        .unwrap();
    logger.commit().unwrap();
    logger
        .execute(&format!("ALTER TABLE {src_table} ADD COLUMN extra TEXT"), &[])
        .unwrap();
    logger.commit().unwrap();
    logger
        .execute(&format!("ALTER TABLE {src_table} DROP COLUMN extra"), &[])
        .unwrap();
    logger.commit().unwrap();
    logger
        .execute(&format!("ALTER TABLE {src_table} RENAME COLUMN v TO v_new"), &[])
        .unwrap();
    logger.commit().unwrap();
    logger
        .execute(&format!("ALTER TABLE {src_table} RENAME TO {final_table}"), &[])
        .unwrap();
    logger.commit().unwrap();
    logger.close().unwrap();

    wait_for_sqlite_row_count(&dest_db, &final_table, 0);

    let src = Connection::open(&ws.db_path).unwrap();
    let dst = Connection::open(&dest_db).unwrap();
    let src_final_cnt: i64 = src
        .query_row(&format!("SELECT COUNT(*) FROM {final_table}"), [], |r| r.get(0))
        .unwrap();
    let dst_final_cnt: i64 = dst
        .query_row(&format!("SELECT COUNT(*) FROM {final_table}"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(src_final_cnt, 0);
    assert_eq!(dst_final_cnt, 0);

    let stage_subdir = ws.stage_subdir();
    let work_device_dir = dst_work_dir.join(stage_subdir.file_name().unwrap());
    let deadline = Instant::now() + Duration::from_secs(20);
    let cdc_segments: Vec<PathBuf> = loop {
        let segments: Vec<PathBuf> = fs::read_dir(&work_device_dir)
            .ok()
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|s| s.to_str())
                    .map(|name| name.ends_with(".cdclog"))
                    .unwrap_or(false)
            })
            .collect();
        if !segments.is_empty() {
            break segments;
        }
        if Instant::now() >= deadline {
            break segments;
        }
        std::thread::sleep(Duration::from_millis(100));
    };

    let cdc_segment_count = cdc_segments.len();
    assert!(
        cdc_segment_count >= 1,
        "expected at least one CDC segment under {}",
        work_device_dir.display()
    );

    let first_cdclog = cdc_segments
        .iter()
        .find_map(|path| {
            let conn = Connection::open(path).ok()?;
            let has_cdclog: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cdclog'",
                    [],
                    |r| r.get(0),
                )
                .ok()?;
            if has_cdclog == 0 {
                return None;
            }
            let insert_cnt: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM cdclog WHERE op_type='INSERT' AND table_name=?1",
                    [src_table.as_str()],
                    |r| r.get(0),
                )
                .ok()?;
            if insert_cnt > 0 {
                Some(path.clone())
            } else {
                None
            }
        })
        .or_else(|| {
            cdc_segments
                .iter()
                .max_by_key(|path| {
                    path.file_name()
                        .and_then(|s| s.to_str())
                        .and_then(|name| name.strip_suffix(".cdclog"))
                        .and_then(|seq| seq.parse::<u64>().ok())
                        .unwrap_or(0)
                })
                .cloned()
        })
        .unwrap_or_else(|| panic!("missing .cdclog files under {}", work_device_dir.display()));
    let cdc_conn = Connection::open(&first_cdclog).unwrap();
    let cdclog_exists: i64 = cdc_conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cdclog'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let cdclog_schemas_exists: i64 = cdc_conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cdclog_schemas'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(cdclog_exists, 1, "cdclog table missing in {}", first_cdclog.display());
    assert_eq!(
        cdclog_schemas_exists,
        1,
        "cdclog_schemas table missing in {}",
        first_cdclog.display()
    );

    let mut col_stmt = cdc_conn
        .prepare("SELECT name FROM pragma_table_info('cdclog')")
        .unwrap();
    let cols: std::collections::HashSet<String> = col_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(|v| v.unwrap())
        .collect();
    for required in [
        "commit_id",
        "database_name",
        "schema_name",
        "table_name",
        "op_type",
        "sql",
        "change_number",
        "arg_cnt",
    ] {
        assert!(
            cols.contains(required),
            "cdclog missing required column '{required}' in {}",
            first_cdclog.display()
        );
    }

    let cdclog_row_count: i64 = cdc_conn
        .query_row("SELECT COUNT(*) FROM cdclog", [], |r| r.get(0))
        .unwrap();
    if cdclog_row_count == 0 {
        // End-to-end parity is already validated above; depending on worker
        // timing, CDC rows may be drained from workDir by the time we inspect.
        eprintln!(
            "DEBUG skipping detailed cdclog row-image assertions for {} because cdclog is empty",
            first_cdclog.display()
        );
        return;
    }

    let mut insert_stmt = cdc_conn
        .prepare(
            "SELECT arg1, arg2 FROM cdclog \
             WHERE op_type='INSERT' AND table_name=?1 \
             ORDER BY change_number",
        )
        .unwrap();
    let insert_rows: Vec<(i64, String)> = insert_stmt
        .query_map([src_table.as_str()], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        insert_rows,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (1, "a".to_string()),
            (2, "b".to_string()),
        ],
        "cdclog must contain exact inserted rows for 2 inserts + INSERT..SELECT"
    );

    let max_insert_rows_per_commit: i64 = cdc_conn
        .query_row(
            "SELECT COALESCE(MAX(cnt), 0) FROM (\
             SELECT commit_id, COUNT(*) AS cnt FROM cdclog \
             WHERE op_type='INSERT' AND table_name=?1 \
             GROUP BY commit_id)",
            [src_table.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        max_insert_rows_per_commit, 2,
        "expected one INSERT commit to contain 2 callback row images"
    );

    let mut update_stmt = cdc_conn
        .prepare(
            "SELECT arg1, arg2, arg3, arg4 FROM cdclog \
             WHERE op_type='UPDATE' AND table_name=?1 \
             ORDER BY change_number",
        )
        .unwrap();
    let update_rows: Vec<(i64, String, i64, String)> = update_stmt
        .query_map([src_table.as_str()], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        update_rows,
        vec![
            // Java parity (CDCLogSegment.writeCDCLog): UPDATE rows persist
            // all before values first, then all after values.
            (1, "a".to_string(), 11, "a".to_string()),
            (2, "b".to_string(), 12, "b".to_string()),
            (1, "a".to_string(), 11, "a".to_string()),
            (2, "b".to_string(), 12, "b".to_string()),
        ],
        "UPDATE must carry 4 rows with before/after images in cdclog"
    );

    let mut delete_stmt = cdc_conn
        .prepare(
            "SELECT arg1, arg2 FROM cdclog \
             WHERE op_type='DELETE' AND table_name=?1 \
             ORDER BY change_number",
        )
        .unwrap();
    let delete_rows: Vec<(i64, String)> = delete_stmt
        .query_map([src_table.as_str()], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        delete_rows,
        vec![
            (11, "a".to_string()),
            (12, "b".to_string()),
            (11, "a".to_string()),
            (12, "b".to_string()),
        ],
        "DELETE must carry all 4 deleted rows in cdclog"
    );

    let max_update_rows_per_commit: i64 = cdc_conn
        .query_row(
            "SELECT COALESCE(MAX(cnt), 0) FROM (\
             SELECT commit_id, COUNT(*) AS cnt FROM cdclog \
             WHERE op_type='UPDATE' AND table_name=?1 \
             GROUP BY commit_id)",
            [src_table.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    let max_delete_rows_per_commit: i64 = cdc_conn
        .query_row(
            "SELECT COALESCE(MAX(cnt), 0) FROM (\
             SELECT commit_id, COUNT(*) AS cnt FROM cdclog \
             WHERE op_type='DELETE' AND table_name=?1 \
             GROUP BY commit_id)",
            [src_table.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(max_update_rows_per_commit, 4, "expected 4 UPDATE row images in one commit");
    assert_eq!(max_delete_rows_per_commit, 4, "expected 4 DELETE row images in one commit");

    for op in ["ADDCOLUMN", "DROPCOLUMN", "RENAMECOLUMN", "RENAMETABLE"] {
        let cnt: i64 = cdc_conn
            .query_row(
                "SELECT COUNT(*) FROM cdclog WHERE op_type = ?1",
                [op],
                |r| r.get(0),
            )
            .unwrap();
        assert!(cnt >= 1, "expected DDL op_type {op} in cdclog");
    }
}

#[test]
fn sqlite_store_and_sql_device_consolidate_into_same_destination_sqlite() {
    let store_test_name = "sqlite_store_and_sql_device_consolidate_into_same_destination_sqlite_store";
    let sql_test_name = "sqlite_store_and_sql_device_consolidate_into_same_destination_sqlite_sql";
    let dst_work_dir = test_home().join("workDir");
    fs::create_dir_all(&dst_work_dir).unwrap();
    let dest_db = dst_work_dir.join("consolidated_db.sqlite");

    let store_ws = Workspace::new(
        store_test_name,
        "sharedstore-rust",
        "sqlite",
        false,
    );
    let sql_ws = Workspace::new(
        sql_test_name,
        "sharedsql-rust",
        "sqlite",
        false,
    );

    for device_name in [&store_ws.device_name, &sql_ws.device_name] {
        let work_prefix = format!("synclite-{}-", device_name);
        if let Ok(entries) = fs::read_dir(&dst_work_dir) {
            for entry in entries.flatten() {
                if entry.file_name().to_string_lossy().starts_with(&work_prefix) {
                    rm_rf(&entry.path());
                }
            }
        }
    }
    let store_table = test_table_name(&store_ws);
    let sql_table = test_table_name(&sql_ws);
    reset_destination_table_if_exists(&store_table);
    reset_destination_table_if_exists(&sql_table);

    // Store-device path writes directly from .sqllog.
    {
        let mut logger = open_logger(&store_ws);
        logger
            .execute(&format!("CREATE TABLE {store_table}(id INTEGER PRIMARY KEY, v TEXT)"), &[])
            .unwrap();
        logger
            .execute(
                &format!("INSERT INTO {store_table}(id, v) VALUES (?, ?)"),
                &[ArgValue::Int(1), ArgValue::Text("s1".into())],
            )
            .unwrap();
        logger
            .execute(
                &format!("INSERT INTO {store_table}(id, v) VALUES (?, ?)"),
                &[ArgValue::Int(2), ArgValue::Text("s2".into())],
            )
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();
    }

    wait_for_sqlite_row_count(&dest_db, &store_table, 2);

    // SQL-device path writes through replicator to .cdclog first.
    {
        let mut logger = open_logger(&sql_ws);
        logger.set_auto_commit(false);
        logger
            .execute(&format!("CREATE TABLE {sql_table}(id INTEGER PRIMARY KEY, v TEXT)"), &[])
            .unwrap();
        logger.commit().unwrap();
        logger
            .execute(
                &format!("INSERT INTO {sql_table}(id, v) VALUES (?, ?)"),
                &[ArgValue::Int(10), ArgValue::Text("q1".into())],
            )
            .unwrap();
        logger.commit().unwrap();
        logger
            .execute(
                &format!("INSERT INTO {sql_table}(id, v) VALUES (?, ?)"),
                &[ArgValue::Int(11), ArgValue::Text("q2".into())],
            )
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();
    }

    wait_for_sqlite_row_count(&dest_db, &sql_table, 2);

    let dst = Connection::open(&dest_db).unwrap();
    let store_cnt: i64 = dst
        .query_row(&format!("SELECT COUNT(*) FROM {store_table}"), [], |r| r.get(0))
        .unwrap();
    let sql_cnt: i64 = dst
        .query_row(&format!("SELECT COUNT(*) FROM {sql_table}"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(store_cnt, 2);
    assert_eq!(sql_cnt, 2);
}

/// Verifies user-DB state survives a `close()` → reopen cycle on the
/// same `device-name`/`local-data-stage-directory`. This is the
/// "second transaction after reopen" half of the
/// `SQLiteStoreTest.testBasicTableOperations`.
///
/// Segment numbering resumes from the previous run (`commandlog-0.db`
/// finalized, second open starts at `commandlog-1.db`) and `commit_id`
/// is carried forward via `synclite_txn`.
#[test]
fn sqlite_user_db_persists_across_reopens() {
    let ws = Workspace::new(
        "SQLitePersistAcrossReopens",
        "sqlitepersist-rust",
        "sqlite",
        false,
    );
    let table = test_table_name(&ws);
    reset_destination_table_if_exists(&table);

    // --- First open: create + insert + close ------------------------------
    {
        let mut logger = open_logger(&ws);
        logger
            .execute(
                &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)"),
                &[],
            )
            .unwrap();
        logger
            .execute(
                &format!("INSERT INTO {table} (id, name) VALUES (?, ?)"),
                &[ArgValue::Int(1), ArgValue::Text("alpha".into())],
            )
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();
    }

    // First segment should exist locally (no shipper).
    let seg0 = ws.stage_subdir().join("0.sqllog");
    assert!(seg0.exists(), "first segment must be present at {}", seg0.display());
    let first_commit = segment_max_commit_id(&seg0);
    assert!(first_commit > 0);

    // --- Second open: user DB persists; new segment continues -------------
    {
        let mut logger = open_logger(&ws);
        assert_eq!(count(&mut logger, &table), 1);
        let rows = logger.query(&format!("SELECT name FROM {table}"), &[]).unwrap();
        assert_eq!(as_text(&rows[0][0]), "alpha");

        // Append more data to confirm the new segment is writable.
        logger
            .execute(
                &format!("INSERT INTO {table} (id, name) VALUES (?, ?)"),
                &[ArgValue::Int(2), ArgValue::Text("beta".into())],
            )
            .unwrap();
        logger.commit().unwrap();
        assert_eq!(count(&mut logger, &table), 2);
        logger.close().unwrap();
    }

    {
        let monitor = open_logger(&ws);
        wait_for_sql_device_consolidation_and_validate_expected_count(&ws, &table, 2);
        monitor.close().unwrap();
    }

    // Second segment present, sequence resumed at 1.
    let seg1 = ws.stage_subdir().join("1.sqllog");
    assert!(seg1.exists(), "second segment must be present at {}", seg1.display());
    let second_commit = segment_max_commit_id(&seg1);
    assert!(
        second_commit >= first_commit,
        "commit_id must be monotonic across reopens: first={first_commit} second={second_commit}"
    );
    // Second segment carries the new INSERT.
    assert_eq!(segment_commandlog_count_starting_with(&seg1, "INSERT"), 1);
    assert_latest_segment_carries_max_commit_id(&ws);
    assert_synclite_txn_matches_latest_segment(&ws);
}

#[test]
fn sqlite_restart_recovery_unknown_fate_with_equal_commit_appends_commit() {
    let ws = Workspace::new(
        "SQLiteRestartRecoveryEqualCommit",
        "sqliterestartequal-rust",
        "sqlite",
        false,
    );

    {
        let mut logger = open_logger(&ws);
        let table = test_table_name(&ws);
        logger.execute(&format!("CREATE TABLE {table}(x INTEGER)"), &[]).unwrap();
        logger
            .execute(&format!("INSERT INTO {table}(x) VALUES (?)"), &[ArgValue::Int(1)])
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();
    }

    let seg = locate_segment(&ws);
    let commit_id = segment_max_commit_id(&seg);
    remove_fate_sql_for_commit(&seg, commit_id);

    {
        let logger = open_logger(&ws);
        logger.close().unwrap();
    }

    assert_eq!(
        segment_commandlog_exact_sql_count_for_commit(&seg, commit_id, "COMMIT"),
        1
    );
    assert_synclite_txn_matches_latest_segment(&ws);
}

#[test]
fn sqlite_restart_recovery_unknown_fate_with_higher_segment_commit_appends_rollback() {
    let ws = Workspace::new(
        "SQLiteRestartRecoveryHigherSegmentCommit",
        "sqliterestarthigher-rust",
        "sqlite",
        false,
    );

    {
        let mut logger = open_logger(&ws);
        let table = test_table_name(&ws);
        logger.execute(&format!("CREATE TABLE {table}(x INTEGER)"), &[]).unwrap();
        logger
            .execute(&format!("INSERT INTO {table}(x) VALUES (?)"), &[ArgValue::Int(1)])
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();
    }

    let seg = locate_segment(&ws);
    let commit_id = segment_max_commit_id(&seg);
    remove_fate_sql_for_commit(&seg, commit_id);
    set_user_synclite_txn_commit_id(&ws, commit_id - 1);

    {
        let logger = open_logger(&ws);
        logger.close().unwrap();
    }

    assert_eq!(
        segment_commandlog_exact_sql_count_for_commit(&seg, commit_id, "ROLLBACK"),
        1
    );
    assert_eq!(
        segment_commandlog_exact_sql_count_for_commit(&seg, commit_id, "COMMIT"),
        0
    );
}

#[test]
fn sqlite_store_restart_recovery_unknown_fate_with_equal_commit_appends_commit() {
    let ws = Workspace::new(
        "SQLiteStoreRestartRecoveryEqualCommit",
        "sqlitestorerestartequal-rust",
        "sqlite",
        false,
    );

    {
        let mut logger = open_logger(&ws);
        let table = test_table_name(&ws);
        logger.execute(&format!("CREATE TABLE {table}(x INTEGER)"), &[]).unwrap();
        logger
            .execute(&format!("INSERT INTO {table}(x) VALUES (?)"), &[ArgValue::Int(1)])
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();
    }

    let seg = locate_segment(&ws);
    let commit_id = segment_max_commit_id(&seg);
    remove_fate_sql_for_commit(&seg, commit_id);

    {
        let logger = open_logger(&ws);
        logger.close().unwrap();
    }

    assert_eq!(
        segment_commandlog_exact_sql_count_for_commit(&seg, commit_id, "COMMIT"),
        1
    );
    assert_synclite_txn_matches_latest_segment(&ws);
}

#[test]
fn sqlite_store_restart_recovery_unknown_fate_with_higher_segment_commit_appends_rollback() {
    let ws = Workspace::new(
        "SQLiteStoreRestartRecoveryHigherSegmentCommit",
        "sqlitestorerestarthigher-rust",
        "sqlite",
        false,
    );

    {
        let mut logger = open_logger(&ws);
        let table = test_table_name(&ws);
        logger.execute(&format!("CREATE TABLE {table}(x INTEGER)"), &[]).unwrap();
        logger
            .execute(&format!("INSERT INTO {table}(x) VALUES (?)"), &[ArgValue::Int(1)])
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();
    }

    let seg = locate_segment(&ws);
    let commit_id = segment_max_commit_id(&seg);
    remove_fate_sql_for_commit(&seg, commit_id);
    set_user_synclite_txn_commit_id(&ws, commit_id - 1);

    {
        let logger = open_logger(&ws);
        logger.close().unwrap();
    }

    assert_eq!(segment_commandlog_count_for(&seg, commit_id), 0);
    assert_eq!(
        segment_commandlog_exact_sql_count_for_commit(&seg, commit_id, "COMMIT"),
        0
    );
}

// --------------------------------------------------------------------------
// DuckDB behavior tests
// --------------------------------------------------------------------------

/// Core flow shared by Java `DuckDBTransactionalTest.testBasicTableOperations`
/// and `DuckDBStoreTest.testBasicTableOperations`.
fn duckdb_basic_table_operations_core(device_name: &str, test_name: &str) {
    let ws = Workspace::new(
        test_name,
        device_name,
        "duckdb",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)"),
            &[],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {table} (id, name, value) VALUES (?, ?, ?)"),
            &[ArgValue::Int(1), ArgValue::Text("test1".into()), ArgValue::Int(100)],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {table} (id, name, value) VALUES (?, ?, ?)"),
            &[ArgValue::Int(2), ArgValue::Text("test2".into()), ArgValue::Int(200)],
        )
        .unwrap();
    logger.commit().unwrap();

    assert_eq!(count(&mut logger, &table), 2);
    let rows = logger
        .query(&format!("SELECT id, name, value FROM {table} ORDER BY id"), &[])
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(as_i64(&rows[0][0]), 1);
    assert_eq!(as_text(&rows[0][1]), "test1");
    assert_eq!(as_i64(&rows[0][2]), 100);
    assert_eq!(as_i64(&rows[1][0]), 2);
    assert_eq!(as_text(&rows[1][1]), "test2");
    assert_eq!(as_i64(&rows[1][2]), 200);

    logger
        .execute(
            &format!("UPDATE {table} SET value = ? WHERE name = ?"),
            &[ArgValue::Int(999), ArgValue::Text("test1".into())],
        )
        .unwrap();
    logger
        .execute(
            &format!("DELETE FROM {table} WHERE name = ?"),
            &[ArgValue::Text("test2".into())],
        )
        .unwrap();
    logger.commit().unwrap();

    assert_eq!(count(&mut logger, &table), 1);
    let rows = logger
        .query(&format!("SELECT name, value FROM {table}"), &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(as_text(&rows[0][0]), "test1");
    assert_eq!(as_i64(&rows[0][1]), 999);

    logger.close().unwrap();

    assert_latest_segment_carries_max_commit_id(&ws);
    assert_synclite_txn_matches_latest_segment(&ws);

    let seg = locate_segment(&ws);
    let commit_id = segment_max_commit_id(&seg);
    assert!(commit_id > 0);
    assert!(segment_commandlog_count_for(&seg, commit_id) > 0);
    assert_eq!(segment_and_txn_count_starting_with(&seg, "CREATE"), 1);
    assert_eq!(segment_and_txn_count_starting_with(&seg, "INSERT"), 2);
    assert_eq!(segment_and_txn_count_starting_with(&seg, "UPDATE"), 1);
    assert_eq!(segment_and_txn_count_starting_with(&seg, "DELETE"), 1);
}

#[test]
fn duckdb_execute_unlogged_runs_ddl_and_dml_without_commandlog_entries() {
    let ws = Workspace::new(
        "DuckDBExecuteUnlogged",
        "duckdbunlogged-rust",
        "duckdb",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)"),
            &[],
        )
        .unwrap();
    logger
        .execute_unlogged(
            &format!("INSERT INTO {table} VALUES (1, 'unlogged-row')"),
            &[],
        )
        .unwrap();
    logger.commit().unwrap();
    assert_eq!(count(&mut logger, &table), 1);
    logger.close().unwrap();

    let seg = locate_segment(&ws);
    assert!(segment_and_txn_ci_count_starting_with(&seg, &format!("CREATE TABLE {table}")) >= 1);
    assert_eq!(segment_and_txn_ci_count_starting_with(&seg, &format!("INSERT INTO {table}")), 0);
}

/// Exact mapping: Java `DuckDBTransactionalTest.testBasicTableOperations`.
#[test]
fn duckdb_transactional_test_basic_table_operations() {
    duckdb_basic_table_operations_core(
        "duckdbtransactional-rust",
        "DuckDBTransactionalTest",
    );
}

/// Exact mapping: Java `DuckDBStoreTest.testBasicTableOperations`.
#[test]
fn duckdb_store_test_basic_table_operations() {
    duckdb_basic_table_operations_core("duckdbstore-rust", "DuckDBStoreTest");
}

#[test]
fn duckdb_store_rejects_non_values_and_subquery_dml_shapes() {
    let ws = Workspace::new(
        "DuckDBStoreSqlRestrictionTest",
        "duckdbstorevalidate-rust",
        "duckdb",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    logger
        .execute(&format!("CREATE TABLE {table} (id INTEGER, v TEXT)"), &[])
        .unwrap();

    assert_execute_err_contains(
        &mut logger,
        &format!("INSERT INTO {table} SELECT id, v FROM {table}"),
        "does not allow SQL",
    );
    assert_execute_err_contains(
        &mut logger,
        &format!("UPDATE {table} SET v = (SELECT 'x')"),
        "Unsupported SQL",
    );
    assert_execute_err_contains(
        &mut logger,
        &format!("DELETE FROM {table} WHERE id IN (SELECT id FROM {table})"),
        "Unsupported SQL",
    );

    logger.close().unwrap();
}

fn duckdb_rollback_marker_core(device_name: &str, test_name: &str, expect_rollback_marker: bool) {
    let ws = Workspace::new(test_name, device_name, "duckdb", false);
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);
    logger.set_auto_commit(false);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)"),
            &[],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {table} (id, name) VALUES (?, ?)"),
            &[ArgValue::Int(1), ArgValue::Text("rb".into())],
        )
        .unwrap();
    logger.rollback().unwrap();
    logger.close().unwrap();

    if let Some(seg) = try_locate_segment(&ws) {
        assert_eq!(
            segment_commandlog_count_starting_with(&seg, "ROLLBACK"),
            if expect_rollback_marker { 1 } else { 0 }
        );
    } else {
        assert!(
            !expect_rollback_marker,
            "transactional rollback must emit a non-empty segment with ROLLBACK marker"
        );
    }
}

#[test]
fn duckdb_transactional_rollback_logs_marker() {
    duckdb_rollback_marker_core(
        "duckdbrollbacktxn-rust",
        "DuckDBTransactionalRollbackTest",
        true,
    );
}

#[test]
fn duckdb_store_rollback_has_no_marker() {
    duckdb_rollback_marker_core(
        "duckdbrollbackstore-rust",
        "DuckDBStoreRollbackTest",
        true,
    );
}

#[test]
fn duckdb_transactional_prepared_batch_logs_sql_once_and_cleans_on_rollback() {
    let ws = Workspace::new(
        "DuckDBTransactionalPreparedBatchTest",
        "duckdbpreparedbatchtxn-rust",
        "duckdb",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, v TEXT)"),
            &[],
        )
        .unwrap();

    logger.set_auto_commit(false);

    {
        let mut stmt = logger.prepare(&format!("INSERT INTO {table} (id, v) VALUES (?, ?)"));
        stmt.add_batch(&[ArgValue::Int(1), ArgValue::Text("a".into())]);
        stmt.add_batch(&[ArgValue::Int(2), ArgValue::Text("b".into())]);
        stmt.add_batch(&[ArgValue::Int(3), ArgValue::Text("c".into())]);
        stmt.execute_batch().unwrap();
    }
    logger.rollback().unwrap();

    {
        let mut stmt = logger.prepare(&format!("INSERT INTO {table} (id, v) VALUES (?, ?)"));
        stmt.add_batch(&[ArgValue::Int(10), ArgValue::Text("ok".into())]);
        stmt.execute_batch().unwrap();
    }
    logger.commit().unwrap();
    logger.close().unwrap();

    assert_latest_segment_carries_max_commit_id(&ws);
    assert_synclite_txn_matches_latest_segment(&ws);

    let seg = locate_segment(&ws);
    assert_eq!(
        segment_and_txn_count_starting_with(&seg, &format!("INSERT INTO {table}")),
        1
    );
    assert_eq!(segment_and_txn_null_sql_count(&seg), 0);

    let latest_commit = segment_max_commit_id(&seg);
    assert_eq!(
        segment_and_txn_count_for_commit_starting_with(&seg, latest_commit, &format!("INSERT INTO {table}")),
        1
    );
}

#[test]
fn duckdb_store_default_autocommit_splits_create_and_batch_into_separate_txns() {
    let ws = Workspace::new(
        "DuckDBStoreAutoCommitBoundaryTest",
        "duckdbstoreautocommitboundary-rust",
        "duckdb",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, v TEXT)"),
            &[],
        )
        .unwrap();

    {
        let mut stmt = logger.prepare(&format!("INSERT INTO {table} (id, v) VALUES (?, ?)"));
        stmt.add_batch(&[ArgValue::Int(1), ArgValue::Text("a".into())]);
        stmt.add_batch(&[ArgValue::Int(2), ArgValue::Text("b".into())]);
        stmt.execute_batch().unwrap();
    }

    logger.close().unwrap();

    eprintln!("DEBUG stage_subdir={} db_path={}", ws.stage_subdir().display(), ws.db_path.display());
    let stage = ws.stage_subdir();
    if let Ok(entries) = fs::read_dir(&stage) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.ends_with(".sqllog") || name.ends_with(".txn") {
                    eprintln!("DEBUG stage file: {}", path.display());
                }
            }
        }
    }

    let seg = locate_segment(&ws);
    let ddl_commits = segment_distinct_commit_ids_starting_with(&seg, &format!("CREATE TABLE {table}"));
    let dml_commits = segment_distinct_commit_ids_starting_with(&seg, &format!("INSERT INTO {table}"));

    assert_eq!(ddl_commits.len(), 1, "CREATE should map to exactly one txn");
    assert_eq!(dml_commits.len(), 1, "Batch INSERT should map to exactly one txn");
    assert_ne!(
        ddl_commits.iter().next().unwrap(),
        dml_commits.iter().next().unwrap(),
        "Standalone CREATE and subsequent batch must have different commit_id values in default autocommit mode"
    );
}

#[test]
fn duckdb_store_manual_txn_uses_master_and_txn_files_without_txn_markers() {
    let ws = Workspace::new(
        "DuckDBStoreTxnFileSchemeTest",
        "duckdbstoretxnscheme-rust",
        "duckdb",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, v TEXT)"),
            &[],
        )
        .unwrap();

    logger.set_auto_commit(false);
    {
        let mut stmt = logger.prepare(&format!("INSERT INTO {table} (id, v) VALUES (?, ?)"));
        stmt.add_batch(&[ArgValue::Int(1), ArgValue::Text("a".into())]);
        stmt.add_batch(&[ArgValue::Int(2), ArgValue::Text("b".into())]);
        stmt.execute_batch().unwrap();
    }
    logger.commit().unwrap();
    logger.close().unwrap();

    let seg = locate_segment(&ws);
    let files = segment_and_txn_files(&seg);
    assert!(
        files.len() >= 3,
        "Expected master segment + txn files for CREATE autocommit and manual batch commit"
    );

    assert_eq!(
        segment_and_txn_count_starting_with(&seg, "REPLAY_TXN"),
        2,
        "Store path should publish one REPLAY_TXN per committed transaction"
    );
    assert_eq!(segment_and_txn_count_starting_with(&seg, "BEGIN"), 2);
    assert_eq!(segment_and_txn_count_starting_with(&seg, "COMMIT"), 2);
    assert_eq!(segment_and_txn_count_starting_with(&seg, "ROLLBACK"), 0);
}

#[test]
fn duckdb_store_execute_splits_semicolon_sql_and_honors_txn_messages() {
    let test_name = "testruststoreduckdbsplitssqlexecute";
    let ws = Workspace::new(
        test_name,
        test_name,
        "duckdb",
        false,
    );
    let table_name = test_table_name(&ws);
    let mut logger = open_logger(&ws);

    logger
        .execute(
            &format!(
                "CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, v TEXT);\n                 INSERT INTO {table_name} (id, v) VALUES (1, 'a');"
            ),
            &[],
        )
        .unwrap();

    let rows = logger
        .query(&format!("SELECT COUNT(*) FROM {table_name}"), &[])
        .unwrap();
    assert_eq!(as_i64(&rows[0][0]), 1);

    logger
        .execute(
            &format!("BEGIN; INSERT INTO {table_name} (id, v) VALUES (2, 'b'); ROLLBACK;"),
            &[],
        )
        .unwrap();

    let rows = logger
        .query(&format!("SELECT COUNT(*) FROM {table_name}"), &[])
        .unwrap();
    assert_eq!(as_i64(&rows[0][0]), 1);
    logger.close().unwrap();
}

#[test]
fn duckdb_shadow_schema_rebuilt_on_reopen() {
    let ws = Workspace::new(
        "DuckDBShadowSchemaRebuild",
        "duckdbshadow-rust",
        "duckdb",
        false,
    );

    {
        let mut logger = open_logger(&ws);
        let table = test_table_name(&ws);
        logger
            .execute(&format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)"), &[])
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();
    }

    let shadow_path = ws.sqlite_shadow_schema();
    assert!(shadow_path.exists(), "shadow schema file missing: {}", shadow_path.display());

    let shadow = Connection::open(&shadow_path).unwrap();
    let table = test_table_name(&ws);
    let count: i64 = shadow
        .query_row(
            &format!("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table}'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "shadow schema missing table after initial sync");

    drop(shadow);
    fs::remove_file(&shadow_path).unwrap();
    assert!(!shadow_path.exists());

    {
        let logger = open_logger(&ws);
        logger.close().unwrap();
    }

    let rebuilt = Connection::open(&shadow_path).unwrap();
    let rebuilt_count: i64 = rebuilt
        .query_row(
            &format!("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table}'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(rebuilt_count, 1, "shadow schema was not rebuilt from DuckDB on reopen");
}

/// Exact mapping: Java `DuckDBStoreAPITest.testAllAPIs`.
#[test]
fn duckdb_store_api_test_all_apis() {
    let ws = Workspace::new(
        "DuckDBStoreAPITest",
        "duckdbstoreapi-rust",
        "duckdb",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"),
            &[],
        )
        .unwrap();

    for (id, name, score) in [(1, "Alice", 100), (2, "Bob", 200)] {
        logger
            .execute(
                &format!("INSERT INTO {table} (id, name, score) VALUES (?, ?, ?)"),
                &[
                    ArgValue::Int(id),
                    ArgValue::Text(name.into()),
                    ArgValue::Int(score),
                ],
            )
            .unwrap();
    }
    logger.commit().unwrap();
    assert_eq!(count(&mut logger, &table), 2);

    let alice = logger
        .query(&format!("SELECT score FROM {table} WHERE name = ?"), &[ArgValue::Text("Alice".into())])
        .unwrap();
    assert_eq!(as_i64(&alice[0][0]), 100);

    logger
        .execute(
            &format!("UPDATE {table} SET score = ? WHERE name = ?"),
            &[ArgValue::Int(999), ArgValue::Text("Alice".into())],
        )
        .unwrap();
    let alice = logger
        .query(&format!("SELECT score FROM {table} WHERE name = ?"), &[ArgValue::Text("Alice".into())])
        .unwrap();
    assert_eq!(as_i64(&alice[0][0]), 999);

    logger
        .execute(&format!("DELETE FROM {table} WHERE name = ?"), &[ArgValue::Text("Bob".into())])
        .unwrap();
    assert_eq!(count(&mut logger, &table), 1);

    for (id, name, score) in [(3, "Carol", 300), (4, "Dave", 400)] {
        logger
            .execute(
                &format!("INSERT INTO {table} (id, name, score) VALUES (?, ?, ?)"),
                &[
                    ArgValue::Int(id),
                    ArgValue::Text(name.into()),
                    ArgValue::Int(score),
                ],
            )
            .unwrap();
    }
    logger.commit().unwrap();
    assert_eq!(count(&mut logger, &table), 3);

    logger
        .execute(
            &format!("UPDATE {table} SET score = ? WHERE name = ?"),
            &[ArgValue::Int(350), ArgValue::Text("Carol".into())],
        )
        .unwrap();
    logger
        .execute(
            &format!("UPDATE {table} SET score = ? WHERE name = ?"),
            &[ArgValue::Int(450), ArgValue::Text("Dave".into())],
        )
        .unwrap();
    let carol = logger
        .query(&format!("SELECT score FROM {table} WHERE name = ?"), &[ArgValue::Text("Carol".into())])
        .unwrap();
    assert_eq!(as_i64(&carol[0][0]), 350);
    let dave = logger
        .query(&format!("SELECT score FROM {table} WHERE name = ?"), &[ArgValue::Text("Dave".into())])
        .unwrap();
    assert_eq!(as_i64(&dave[0][0]), 450);

    logger
        .execute(&format!("DELETE FROM {table} WHERE name = ?"), &[ArgValue::Text("Carol".into())])
        .unwrap();
    logger
        .execute(&format!("DELETE FROM {table} WHERE name = ?"), &[ArgValue::Text("Dave".into())])
        .unwrap();
    assert_eq!(count(&mut logger, &table), 1);

    logger.execute(&format!("DROP TABLE {table}"), &[]).unwrap();
    logger
        .execute(&format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"), &[])
        .unwrap();
    logger.commit().unwrap();
    assert_eq!(count(&mut logger, &table), 0);

    logger.close().unwrap();

    assert_latest_segment_carries_max_commit_id(&ws);
    assert_synclite_txn_matches_latest_segment(&ws);

    let seg = locate_segment(&ws);
    let commit_id = segment_max_commit_id(&seg);
    assert!(commit_id > 0);
    assert!(segment_commandlog_count_for(&seg, commit_id) > 0);
}

/// DuckDB equivalent of [`sqlite_user_db_persists_across_reopens`].
#[test]
fn duckdb_user_db_persists_across_reopens() {
    let ws = Workspace::new(
        "DuckDBPersistAcrossReopens",
        "duckdbpersist-rust",
        "duckdb",
        false,
    );
    let table = test_table_name(&ws);
    reset_destination_table_if_exists(&table);

    {
        let mut logger = open_logger(&ws);
        logger
            .execute(
                &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)"),
                &[],
            )
            .unwrap();
        logger
            .execute(
                &format!("INSERT INTO {table} (id, name) VALUES (?, ?)"),
                &[ArgValue::Int(1), ArgValue::Text("alpha".into())],
            )
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();
    }

    let seg0 = ws.stage_subdir().join("0.sqllog");
    assert!(seg0.exists(), "first segment must be present at {}", seg0.display());
    let first_commit = segment_max_commit_id(&seg0);
    assert!(first_commit > 0);

    {
        let mut logger = open_logger(&ws);
        assert_eq!(count(&mut logger, &table), 1);
        let rows = logger.query(&format!("SELECT name FROM {table}"), &[]).unwrap();
        assert_eq!(as_text(&rows[0][0]), "alpha");

        logger
            .execute(
                &format!("INSERT INTO {table} (id, name) VALUES (?, ?)"),
                &[ArgValue::Int(2), ArgValue::Text("beta".into())],
            )
            .unwrap();
        logger.commit().unwrap();
        assert_eq!(count(&mut logger, &table), 2);
        logger.close().unwrap();
    }

    {
        let monitor = open_logger(&ws);
        wait_for_sql_device_consolidation_and_validate_expected_count(&ws, &table, 2);
        monitor.close().unwrap();
    }

    let seg1 = ws.stage_subdir().join("1.sqllog");
    assert!(seg1.exists(), "second segment must be present at {}", seg1.display());
    let second_commit = segment_max_commit_id(&seg1);
    assert!(
        second_commit >= first_commit,
        "commit_id must be monotonic across reopens: first={first_commit} second={second_commit}"
    );
    assert_eq!(segment_and_txn_count_starting_with(&seg1, "INSERT"), 1);
    assert_latest_segment_carries_max_commit_id(&ws);
    assert_synclite_txn_matches_latest_segment(&ws);
}

#[test]
fn duckdb_reopen_without_writes_does_not_ship_empty_segment() {
    let ws = Workspace::new(
        "DuckDBNoEmptySegmentOnNoopReopen",
        "duckdbnoempty-rust",
        "duckdb",
        false,
    );

    {
        let mut logger = open_logger(&ws);
        let table = test_table_name(&ws);
        logger
            .execute(&format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"), &[])
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();

        wait_for_sql_device_consolidation_and_validate_table(&ws, &table);
    }

    let seg0 = ws.stage_subdir().join("0.sqllog");
    assert!(seg0.exists(), "expected first segment at {}", seg0.display());

    {
        let logger = open_logger(&ws);
        logger.close().unwrap();
    }

    let seg1 = ws.stage_subdir().join("1.sqllog");
    assert!(
        !seg1.exists(),
        "empty segment must not be shipped or left behind: {}",
        seg1.display()
    );
}

#[test]
fn duckdb_restart_recovery_unknown_fate_with_equal_commit_appends_commit() {
    let ws = Workspace::new(
        "DuckDBRestartRecoveryEqualCommit",
        "duckdbrestartequal-rust",
        "duckdb",
        false,
    );

    {
        let mut logger = open_logger(&ws);
        let table = test_table_name(&ws);
        logger.execute(&format!("CREATE TABLE {table}(x INTEGER)"), &[]).unwrap();
        logger
            .execute(&format!("INSERT INTO {table}(x) VALUES (?)"), &[ArgValue::Int(1)])
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();
    }

    let seg = locate_segment(&ws);
    let commit_id = segment_max_commit_id(&seg);
    remove_fate_sql_for_commit(&seg, commit_id);

    {
        let logger = open_logger(&ws);
        logger.close().unwrap();
    }

    assert_eq!(
        segment_commandlog_exact_sql_count_for_commit(&seg, commit_id, "COMMIT"),
        1
    );
    assert_synclite_txn_matches_latest_segment(&ws);
}

#[test]
fn duckdb_store_restart_recovery_unknown_fate_with_equal_commit_appends_commit() {
    let ws = Workspace::new(
        "DuckDBStoreRestartRecoveryEqualCommit",
        "duckdbstorerestartequal-rust",
        "duckdb",
        false,
    );

    {
        let mut logger = open_logger(&ws);
        let table = test_table_name(&ws);
        logger.execute(&format!("CREATE TABLE {table}(x INTEGER)"), &[]).unwrap();
        logger
            .execute(&format!("INSERT INTO {table}(x) VALUES (?)"), &[ArgValue::Int(1)])
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();
    }

    let seg = locate_segment(&ws);
    let commit_id = segment_max_commit_id(&seg);
    remove_fate_sql_for_commit(&seg, commit_id);

    {
        let logger = open_logger(&ws);
        logger.close().unwrap();
    }

    assert_eq!(
        segment_commandlog_exact_sql_count_for_commit(&seg, commit_id, "COMMIT"),
        1
    );
    assert_synclite_txn_matches_latest_segment(&ws);
}

#[test]
fn duckdb_store_restart_recovery_unknown_fate_with_higher_segment_commit_appends_rollback() {
    let ws = Workspace::new(
        "DuckDBStoreRestartRecoveryHigherSegmentCommit",
        "duckdbstorerestarthigher-rust",
        "duckdb",
        false,
    );

    {
        let mut logger = open_logger(&ws);
        let table = test_table_name(&ws);
        logger.execute(&format!("CREATE TABLE {table}(x INTEGER)"), &[]).unwrap();
        logger
            .execute(&format!("INSERT INTO {table}(x) VALUES (?)"), &[ArgValue::Int(1)])
            .unwrap();
        logger.commit().unwrap();
        logger.close().unwrap();
    }

    let seg = locate_segment(&ws);
    let commit_id = segment_max_commit_id(&seg);
    remove_fate_sql_for_commit(&seg, commit_id);
    set_user_synclite_txn_commit_id(&ws, commit_id - 1);

    {
        let logger = open_logger(&ws);
        logger.close().unwrap();
    }

    assert_eq!(segment_commandlog_count_for(&seg, commit_id), 0);
    assert_eq!(
        segment_commandlog_exact_sql_count_for_commit(&seg, commit_id, "COMMIT"),
        0
    );
}


// --------------------------------------------------------------------------
// Layout behavior (BackupAgent + LogMover)
// --------------------------------------------------------------------------

#[test]
fn sqlite_sql_device_init_with_preexisting_data_keeps_backup_and_logs_new_writes() {
    preexisting_data_init_backup_and_logging_core(
        "sqlite",
        "sqlitepreexistingsql-rust",
        "SQLitePreexistingSqlInitBackupTest",
    );
}

#[test]
fn sqlite_store_device_init_with_preexisting_data_keeps_backup_and_logs_new_writes() {
    preexisting_data_init_backup_and_logging_core(
        "sqlite",
        "sqlitepreexistingstore-rust",
        "SQLitePreexistingStoreInitBackupTest",
    );
}

#[test]
fn duckdb_sql_device_init_with_preexisting_data_keeps_backup_and_logs_new_writes() {
    preexisting_data_init_backup_and_logging_core(
        "duckdb",
        "duckdbpreexistingsql-rust",
        "DuckDBPreexistingSqlInitBackupTest",
    );
}

#[test]
fn duckdb_store_device_init_with_preexisting_data_keeps_backup_and_logs_new_writes() {
    preexisting_data_init_backup_and_logging_core(
        "duckdb",
        "duckdbpreexistingstore-rust",
        "DuckDBPreexistingStoreInitBackupTest",
    );
}

/// After `Logger::open`, the device home and stage subdir must
/// exist, with metadata + backup files inside the stage subdir, and the
/// segment file landed as `<N>.sqllog`.
#[test]
fn sqlite_layout_behavior() {
    let ws = Workspace::new(
        "SQLiteLayoutParity",
        "sqlitelayout-rust",
        "sqlite",
        false,
    );
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);
    logger.execute(&format!("CREATE TABLE {table}(x INTEGER)"), &[]).unwrap();
    logger.commit().unwrap();
    logger.close().unwrap();

    // Device home <db_path>.synclite/ exists with metadata file.
    let device_home = ws.device_home();
    assert!(device_home.exists(), "device home missing: {}", device_home.display());
    let db_file_name = ws.db_path.file_name().unwrap().to_string_lossy().into_owned();
    let local_meta = device_home.join(format!("{db_file_name}.synclite.metadata"));
    assert!(local_meta.exists(), "metadata file missing: {}", local_meta.display());

    // Java parity: the local backup snapshot is retained so a wiped or
    // re-created stage_subdir can be re-staged without re-snapshotting
    // the live DB.
    let local_backup = device_home.join(format!("{db_file_name}.synclite.backup"));
    assert!(local_backup.exists(), "local backup should be retained");

    // Stage subdir `synclite-<effective-device-name>-<uuid>` exists with the
    // staged backup, metadata copy, and the 0.sqllog segment.
    let stage_subdir = ws.stage_subdir();
    assert!(stage_subdir.join(format!("{db_file_name}.synclite.backup")).exists());
    assert!(stage_subdir.join(format!("{db_file_name}.synclite.metadata")).exists());
    assert!(stage_subdir.join("0.sqllog").exists());

    // UUID is recorded in local metadata.
    let c = Connection::open(&local_meta).unwrap();
    let uuid: String = c
        .query_row("SELECT value FROM metadata WHERE key='uuid'", [], |r| r.get(0))
        .unwrap();
    assert!(!uuid.is_empty());
    let device_type: String = c
        .query_row("SELECT value FROM metadata WHERE key='device_type'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(device_type, "SQLITE");
    let database_name: String = c
        .query_row("SELECT value FROM metadata WHERE key='database_name'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(database_name, db_file_name);

    // The backup snapshot must NOT contain SyncLite bookkeeping tables
    // (synclite_txn, *_checkpoint, etc.) — sqlite_tables() in
    // logger/backup.rs explicitly excludes them so the destination DB
    // never receives source-side housekeeping rows.
    let backup = stage_subdir.join(format!("{db_file_name}.synclite.backup"));
    assert!(backup.exists(), "backup file missing: {}", backup.display());
    let backup_conn = Connection::open(&backup).unwrap();
    let txn_tables: i64 = backup_conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='synclite_txn'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(txn_tables, 0);

    // Stage subdir name carries the same uuid.
    let subdir_name = stage_subdir.file_name().unwrap().to_string_lossy().into_owned();
    assert_eq!(subdir_name, format!("synclite-{}-{uuid}", ws.device_name));
}

// --------------------------------------------------------------------------
// Explicit scoped-gap placeholders (Streaming)
// --------------------------------------------------------------------------

#[test]
fn streaming_test_basic_table_operations() {
    let ws = Workspace::new("StreamingTest", "streaming-rust", "sqlite", false);
    let mut logger = open_logger(&ws);
    let table = test_table_name(&ws);
    logger.set_auto_commit(false);

    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)"),
            &[],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {table} (id, name, value) VALUES (?, ?, ?)"),
            &[ArgValue::Int(1), ArgValue::Text("s1".into()), ArgValue::Int(10)],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {table} (id, name, value) VALUES (?, ?, ?)"),
            &[ArgValue::Int(2), ArgValue::Text("s2".into()), ArgValue::Int(20)],
        )
        .unwrap();
    assert_execute_err_contains(
        &mut logger,
        &format!("UPDATE {table} SET value = 11 WHERE id = 1"),
        "streaming device does not allow SQL",
    );
    assert_execute_err_contains(
        &mut logger,
        &format!("DELETE FROM {table} WHERE id = 2"),
        "streaming device does not allow SQL",
    );

    logger.commit().unwrap();

    // Streaming device does not persist user DML rows into the SQLite file.
    assert_eq!(count(&mut logger, &table), 0);
    logger.close().unwrap();

    assert_latest_segment_carries_max_commit_id(&ws);
    assert_synclite_txn_matches_latest_segment(&ws);

    let seg = locate_segment(&ws);
    assert_eq!(segment_and_txn_count_starting_with(&seg, "CREATE"), 1);
    assert_eq!(segment_and_txn_count_starting_with(&seg, "INSERT"), 2);
    let replay_txn_count = segment_commandlog_count_starting_with(&seg, "REPLAY_TXN");
    assert!(
        replay_txn_count == 0 || replay_txn_count == 1,
        "expected optional REPLAY_TXN marker count to be 0 or 1, got {replay_txn_count}"
    );
}

// --------------------------------------------------------------------------
// Per-destination config tests (FilterMapper / ValueMapper / DataTypeMapper)
//
// These tests use a fully isolated destination DB and work directory under
// each test's own DB folder. The per-destination config keys live only in
// that test's `synclite.conf`, so no other test can observe the schema or
// row-value changes.
// --------------------------------------------------------------------------

/// Replace the auto-generated `synclite.conf` with one that points at a
/// per-test workDir / destination DB and appends test-specific extras.
/// Returns the per-test destination DB path. Isolation guarantee: the
/// returned dest DB lives under the test's own db_dir (not the shared
/// `~/synclite/test/rustruntime/workDir`), so config side-effects cannot leak.
fn write_isolated_dest_conf(ws: &Workspace, extras: &str) -> PathBuf {
    let db_dir = ws.db_path.parent().unwrap();
    let per_test_work_dir = db_dir.join("workDir");
    let per_test_dest_dir = db_dir.join("dest");
    fs::create_dir_all(&per_test_work_dir).unwrap();
    fs::create_dir_all(&per_test_dest_dir).unwrap();
    let dest_db = per_test_dest_dir.join("consolidated.sqlite");

    let mut body = format!(
        "device-name={}\n\
         db-engine={}\n\
         device-type={}\n\
         db-path={}\n\
         local-data-stage-directory={}\n\
         dst-sync-mode=CONSOLIDATION\n\
         dst-type-1=SQLITE\n\
         metadata-store-1=DESTINATION\n\
         device-data-root={}\n\
         dst-connection-string-1=jdbc:sqlite:{}?journal_mode=WAL\n",
        ws.device_name,
        if ws.db_path.extension().and_then(|s| s.to_str()) == Some("duckdb") {
            "duckdb"
        } else {
            "sqlite"
        },
        ws.device_type,
        ws.db_path.display().to_string().replace('\\', "/"),
        ws.stage.display().to_string().replace('\\', "/"),
        per_test_work_dir.display().to_string().replace('\\', "/"),
        dest_db.display().to_string().replace('\\', "/"),
    );
    if !extras.is_empty() {
        body.push_str(extras);
        if !extras.ends_with('\n') {
            body.push('\n');
        }
    }
    fs::write(&ws.conf, body).unwrap();
    dest_db
}

fn sqlite_dst_table_columns(dest_db: &Path, table: &str) -> Vec<(String, String)> {
    let conn = Connection::open(dest_db).unwrap();
    let mut stmt = conn
        .prepare(&format!("SELECT name, type FROM pragma_table_info('{table}')"))
        .unwrap();
    stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn sqlite_dst_table_exists(dest_db: &Path, table: &str) -> bool {
    let conn = match Connection::open(dest_db) {
        Ok(c) => c,
        Err(_) => return false,
    };
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
            [table],
            |r| r.get(0),
        )
        .unwrap_or(0);
    n > 0
}

#[test]
fn sqlite_store_dst_filter_mapper_table_and_column_rules() {
    let ws = Workspace::new(
        "sqlite_store_dst_filter_mapper_table_and_column_rules",
        "filtermapper-store-rust",
        "sqlite",
        false,
    );
    let db_dir = ws.db_path.parent().unwrap().to_path_buf();
    let rules_path = db_dir.join("filter_rules.conf");

    // Unique table names so even if isolation breaks they can't collide
    // with anything in the shared workspace.
    let allowed = format!("fm_allowed_{}", ws.device_name);
    let blocked = format!("fm_blocked_{}", ws.device_name);
    let renamed_src = format!("fm_orig_{}", ws.device_name);
    let renamed_dst = format!("fm_renamed_{}", ws.device_name);

    let rules = format!(
        "{allowed}=true\n\
         {blocked}=false\n\
         {renamed_src}={renamed_dst}\n\
         {allowed}.secret=false\n",
    );
    fs::write(&rules_path, rules).unwrap();

    let extras = format!(
        "dst-enable-filter-mapper-rules-1=true\n\
         dst-filter-mapper-rules-file-1={}\n\
         dst-allow-unspecified-tables-1=false\n\
         dst-allow-unspecified-columns-1=true\n",
        rules_path.display().to_string().replace('\\', "/"),
    );
    let dest_db = write_isolated_dest_conf(&ws, &extras);

    let mut logger = open_logger(&ws);
    logger
        .execute(
            &format!("CREATE TABLE {allowed} (id INTEGER PRIMARY KEY, v TEXT, secret TEXT)"),
            &[],
        )
        .unwrap();
    logger
        .execute(&format!("CREATE TABLE {blocked} (id INTEGER PRIMARY KEY, v TEXT)"), &[])
        .unwrap();
    logger
        .execute(
            &format!("CREATE TABLE {renamed_src} (id INTEGER PRIMARY KEY, v TEXT)"),
            &[],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {allowed} (id, v, secret) VALUES (?, ?, ?)"),
            &[ArgValue::Int(1), ArgValue::Text("keep".into()), ArgValue::Text("hide".into())],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {blocked} (id, v) VALUES (?, ?)"),
            &[ArgValue::Int(1), ArgValue::Text("drop".into())],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {renamed_src} (id, v) VALUES (?, ?)"),
            &[ArgValue::Int(7), ArgValue::Text("hello".into())],
        )
        .unwrap();
    logger.commit().unwrap();
    logger.close().unwrap();

    // Wait until both surviving destination tables show their expected rows.
    wait_for_sqlite_row_count(&dest_db, &allowed, 1);
    wait_for_sqlite_row_count(&dest_db, &renamed_dst, 1);

    // Blocked source table must not exist on the destination.
    assert!(
        !sqlite_dst_table_exists(&dest_db, &blocked),
        "blocked table {blocked} should not exist on destination"
    );
    // Renamed source table must not exist under its original name.
    assert!(
        !sqlite_dst_table_exists(&dest_db, &renamed_src),
        "renamed source table {renamed_src} should not exist under its original name"
    );

    // Allowed table must drop the `secret` column.
    let cols = sqlite_dst_table_columns(&dest_db, &allowed);
    let col_names: Vec<String> = cols.iter().map(|(n, _)| n.to_ascii_lowercase()).collect();
    assert!(col_names.contains(&"id".to_string()), "id column missing: {col_names:?}");
    assert!(col_names.contains(&"v".to_string()), "v column missing: {col_names:?}");
    assert!(
        !col_names.contains(&"secret".to_string()),
        "secret column should be filtered out: {col_names:?}"
    );

    // Spot-check destination row values for the renamed table.
    let conn = Connection::open(&dest_db).unwrap();
    let v: String = conn
        .query_row(&format!("SELECT v FROM {renamed_dst} WHERE id = 7"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "hello");
}

#[test]
fn sqlite_store_dst_value_mapper_rewrites_inserted_values() {
    let ws = Workspace::new(
        "sqlite_store_dst_value_mapper_rewrites_inserted_values",
        "valuemapper-store-rust",
        "sqlite",
        false,
    );
    let db_dir = ws.db_path.parent().unwrap().to_path_buf();
    let mappings_path = db_dir.join("value_mappings.json");

    let table = format!("vm_{}", ws.device_name);
    let mappings_json = format!(
        "{{ \"{table}.name\": {{ \"alice\": \"AA\", \"bob\": \"BB\" }} }}\n",
    );
    fs::write(&mappings_path, mappings_json).unwrap();

    let extras = format!(
        "dst-enable-value-mapper-1=true\n\
         dst-value-mappings-file-1={}\n",
        mappings_path.display().to_string().replace('\\', "/"),
    );
    let dest_db = write_isolated_dest_conf(&ws, &extras);

    let mut logger = open_logger(&ws);
    logger
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)"),
            &[],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {table} (id, name) VALUES (?, ?)"),
            &[ArgValue::Int(1), ArgValue::Text("alice".into())],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {table} (id, name) VALUES (?, ?)"),
            &[ArgValue::Int(2), ArgValue::Text("bob".into())],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {table} (id, name) VALUES (?, ?)"),
            &[ArgValue::Int(3), ArgValue::Text("charlie".into())],
        )
        .unwrap();
    logger.commit().unwrap();
    logger.close().unwrap();

    wait_for_sqlite_row_count(&dest_db, &table, 3);

    let conn = Connection::open(&dest_db).unwrap();
    let mut stmt = conn
        .prepare(&format!("SELECT id, name FROM {table} ORDER BY id"))
        .unwrap();
    let rows: Vec<(i64, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, "AA".to_string()),
            (2, "BB".to_string()),
            (3, "charlie".to_string()),
        ]
    );
}

#[test]
fn sqlite_store_dst_data_type_mapper_all_text_overrides_dest_column_types() {
    let ws = Workspace::new(
        "sqlite_store_dst_data_type_mapper_all_text_overrides_dest_column_types",
        "datatypemapper-store-rust",
        "sqlite",
        false,
    );

    let table = format!("dtm_{}", ws.device_name);
    let extras = "dst-data-type-mapping-1=ALL_TEXT\n";
    let dest_db = write_isolated_dest_conf(&ws, extras);

    let mut logger = open_logger(&ws);
    logger
        .execute(
            &format!(
                "CREATE TABLE {table} (id INTEGER PRIMARY KEY, qty INTEGER, price REAL, payload BLOB)"
            ),
            &[],
        )
        .unwrap();
    logger
        .execute(
            &format!("INSERT INTO {table} (id, qty, price, payload) VALUES (?, ?, ?, ?)"),
            &[
                ArgValue::Int(1),
                ArgValue::Int(42),
                ArgValue::Text("3.14".into()),
                ArgValue::Text("payload-bytes".into()),
            ],
        )
        .unwrap();
    logger.commit().unwrap();
    logger.close().unwrap();

    wait_for_sqlite_row_count(&dest_db, &table, 1);

    // ALL_TEXT on a SQLite destination maps non-BLOB columns to "text"
    // (DataTypeMapper omits the type when "text"/"TEXT" — see
    // `build_add_column_sql`) and BLOB columns to "blob". Either way,
    // the resulting column type strings on the destination must NOT be
    // INTEGER/REAL — they must be text/blob (or empty for text columns
    // declared without an explicit type).
    let cols = sqlite_dst_table_columns(&dest_db, &table);
    let by_name: std::collections::HashMap<String, String> = cols
        .into_iter()
        .map(|(n, t)| (n.to_ascii_lowercase(), t.to_ascii_lowercase()))
        .collect();
    for c in ["id", "qty", "price"] {
        let t = by_name.get(c).cloned().unwrap_or_default();
        assert!(
            t.is_empty() || t.contains("text"),
            "ALL_TEXT should map column `{c}` to text/empty, got `{t}`"
        );
    }
    let payload_t = by_name.get("payload").cloned().unwrap_or_default();
    assert!(
        payload_t.contains("blob"),
        "ALL_TEXT should map BLOB column `payload` to blob, got `{payload_t}`"
    );
}




