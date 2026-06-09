//! DuckDB-backed [`DbDevice`](logger_db_traits::DbDevice) implementation.
//!
//! Mirrors [`logger_db_sqlite::SqliteDevice`] but the user-facing database
//! is a DuckDB file. Log segments are still written as SQLite files via
//! [`LogSegmentWriter`] so the existing consolidator can consume them
//! unchanged regardless of which backend produced them.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use duckdb::{appender_params_from_iter, params_from_iter, types::Value as DuckValue, Connection};
use rusqlite::Connection as SqliteConnection;
use logger_core::{
    record::{ArgValue, CommandLogRecord},
    sql_policy::{is_ddl, parse_insert_shape, validate_sql_policy, InsertShape, SqlPolicyMode, SqlShape},
    Backend, CommitId, DeviceType, Error, OperationId, Result, SegmentSequence,
};
use logger_db_traits::{DbDevice, Row, TxnLogRuntime, Value};
use logger_log::{
    scan_segment_dir,
    LogSegmentWriter,
    DEFAULT_LOG_SEGMENT_FLUSH_BATCH_SIZE,
    DEFAULT_LOG_SEGMENT_PAGE_SIZE,
};

/// Callback invoked with the path of every finalized log segment.
///
/// Kept structurally identical to the SQLite device so a shipper can be
/// wired into either backend with the same code.
pub type SegmentReadyCallback = Arc<dyn Fn(&Path) + Send + Sync>;

/// Configuration for opening a [`DuckDbDevice`].
#[derive(Clone)]
pub struct DuckDbDeviceConfig {
    /// Path to the user-facing DuckDB database file.
    pub db_path: PathBuf,
    /// Directory in which log segments are written. Created on first use.
    /// Segment files are named `<seq>.sqllog` inside this directory.
    pub segment_dir: PathBuf,
    /// Optional directory to scan for resume state. When `None`, scans
    /// `segment_dir`. Useful when finalized segments are moved out of
    /// `segment_dir` into a stage subdirectory.
    pub resume_dir: Option<PathBuf>,
    /// Optional hook called every time a segment is finalized.
    pub on_segment_ready: Option<SegmentReadyCallback>,
    /// SQLite page size for generated log-segment files.
    pub log_segment_page_size: u32,
    /// Number of log rows to buffer in segment transaction before COMMIT.
    pub log_segment_flush_batch_size: u64,
    /// Java-compatible device mode.
    pub device_type: DeviceType,
    /// Maximum number of inlined log args for generated segment files.
    pub max_inlined_log_args: u32,
    /// Skip in-doubt restart recovery on open.
    pub skip_restart_recovery: bool,
}

impl DuckDbDeviceConfig {
    /// Convenience constructor with no segment-ready hook.
    pub fn new<P: Into<PathBuf>, S: Into<PathBuf>>(db_path: P, segment_dir: S) -> Self {
        Self {
            db_path: db_path.into(),
            segment_dir: segment_dir.into(),
            resume_dir: None,
            on_segment_ready: None,
            log_segment_page_size: DEFAULT_LOG_SEGMENT_PAGE_SIZE,
            log_segment_flush_batch_size: DEFAULT_LOG_SEGMENT_FLUSH_BATCH_SIZE,
            device_type: DeviceType::DUCKDB,
            max_inlined_log_args: 16,
            skip_restart_recovery: false,
        }
    }
}

impl std::fmt::Debug for DuckDbDeviceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDbDeviceConfig")
            .field("db_path", &self.db_path)
            .field("segment_dir", &self.segment_dir)
            .field("resume_dir", &self.resume_dir)
            .field("on_segment_ready", &self.on_segment_ready.is_some())
            .field("log_segment_page_size", &self.log_segment_page_size)
            .field("log_segment_flush_batch_size", &self.log_segment_flush_batch_size)
            .field("device_type", &self.device_type)
            .finish()
    }
}

/// DuckDB device: user DuckDB DB + rolling SQLite log segments.
pub struct DuckDbDevice {
    cfg: DuckDbDeviceConfig,
    conn: Connection,
    shadow_conn: SqliteConnection,
    user_txn_open: bool,
    log: LogSegmentWriter,
    current_segment_seq: SegmentSequence,
    next_change_number: u64,
    next_commit_id: u64,
    last_committed_commit_id: u64,
    next_operation_id: u64,
    txn_runtime: TxnLogRuntime,
    txn_stage: Option<TxnStage>,
    restart_segment_path: Option<PathBuf>,
    restart_slave_commit_id: u64,
    restart_txn_fate: Option<TxnFate>,
}

/// Per-transaction staging log used by concurrent-writer parity paths.
///
/// Java creates one txn file per transaction for DuckDB-family devices.
/// We mirror that by writing transaction records to a temporary stage file,
/// then serializing them into the main segment on commit.
struct TxnStage {
    commit_id: u64,
    path: PathBuf,
    log: LogSegmentWriter,
    next_change_number: u64,
}

impl TxnStage {
    fn create(
        segment_dir: &Path,
        page_size: u32,
        flush_batch_size: u64,
        commit_id: u64,
    ) -> Result<Self> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = segment_dir.join(format!("txn-{commit_id}-{now}.sqllog"));
        let log = LogSegmentWriter::create_with_page_size_and_flush_batch(
            path.clone(),
            page_size,
            flush_batch_size,
        )?;
        Ok(Self {
            commit_id,
            path,
            log,
            next_change_number: 0,
        })
    }

    fn append_record(&mut self, rec: &CommandLogRecord) -> Result<()> {
        let mut staged = rec.clone();
        staged.change_number = self.next_change_number;
        self.log.append(&staged)?;
        self.next_change_number += 1;
        Ok(())
    }

    fn cleanup(self) {
        let TxnStage { path, log, .. } = self;
        drop(log);
        std::fs::remove_file(path).ok();
    }

    fn publish(self, segment_dir: &Path, seq: SegmentSequence) -> Result<PathBuf> {
        let TxnStage {
            commit_id,
            path,
            log,
            ..
        } = self;
        // Finalize without automatic COMMIT marker since main segment already has it
        log.finalize_without_commit_marker()?;
        drop(log);
        let published = txn_published_path(segment_dir, seq, commit_id);
        std::fs::rename(&path, &published)?;
        Ok(published)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxnFate {
    Commit,
    Rollback,
    Unknown,
}

impl DuckDbDevice {
    fn uses_staged_txn_scheme(&self) -> bool {
        // DuckDB Store is event-style (no txn markers) but still needs
        // per-transaction staging for concurrent-writer parity.
        self.cfg.device_type.is_transactional() || self.cfg.device_type.is_store()
    }

    fn logs_txn_markers(&self) -> bool {
        self.uses_staged_txn_scheme()
    }

    fn try_execute_prepared_batch_with_appender(
        &mut self,
        sql: &str,
        batch_params: &[Vec<Value>],
    ) -> Result<Option<Vec<u64>>> {
        let Some(insert_shape) = parse_insert_shape(sql) else {
            return Ok(None);
        };
        if batch_params
            .iter()
            .any(|params| params.len() != insert_shape.value_count)
        {
            return Ok(None);
        }

        self.ensure_txn_open_for_user_sql()?;
        let mut app = self
            .conn
            .appender(&insert_shape.table_name)
            .map_err(db_err)?;

        for params in batch_params.iter() {
            let bound: Vec<DuckValue> = params.iter().map(arg_to_duckvalue).collect();
            app.append_row(appender_params_from_iter(bound.iter()))
                .map_err(db_err)?;
        }
        app.flush().map_err(db_err)?;
        drop(app);
        self.post_user_execute_batch(sql)?;

        Ok(Some(vec![0; batch_params.len()]))
    }

    /// Open (or create) the user database and start a fresh segment.
    ///
    /// Resumes segment numbering and `commit_id` from any prior segments
    /// already present in `segment_dir`. See [`SqliteDevice::open`] for
    /// details.
    pub fn open(cfg: DuckDbDeviceConfig) -> Result<Self> {
        std::fs::create_dir_all(&cfg.segment_dir)?;
        let conn = Connection::open(&cfg.db_path).map_err(db_err)?;
        let shadow_path = shadow_schema_path(&cfg);
        rebuild_shadow_schema(&conn, &shadow_path)?;
        let shadow_conn = SqliteConnection::open(&shadow_path).map_err(sqlite_err)?;
        let resume_dir = cfg.resume_dir.as_deref().unwrap_or(cfg.segment_dir.as_path());
        let resume = scan_segment_dir(resume_dir)?;
        let restart_state = Self::scan_last_segment_txn_state(resume_dir)?;
        let log = if cfg.max_inlined_log_args == 16 {
            LogSegmentWriter::create_with_page_size_and_flush_batch(
                segment_path(&cfg, resume.next_seq),
                cfg.log_segment_page_size,
                cfg.log_segment_flush_batch_size,
            )?
        } else {
            LogSegmentWriter::create_with_page_size_and_arg_columns_and_flush_batch(
                segment_path(&cfg, resume.next_seq),
                cfg.log_segment_page_size,
                cfg.max_inlined_log_args,
                cfg.log_segment_flush_batch_size,
            )?
        };
        let initial_commit = next_monotonic_commit_id(resume.max_commit_id);
        let mut dev = Self {
            cfg,
            conn,
            shadow_conn,
            user_txn_open: false,
            log,
            current_segment_seq: resume.next_seq,
            next_change_number: 0,
            next_commit_id: initial_commit,
            last_committed_commit_id: 0,
            next_operation_id: 0,
            txn_runtime: TxnLogRuntime::new(),
            txn_stage: None,
            restart_segment_path: restart_state.as_ref().map(|s| s.0.clone()),
            restart_slave_commit_id: restart_state.as_ref().map(|s| s.1).unwrap_or(0),
            restart_txn_fate: restart_state.as_ref().map(|s| s.2),
        };
        dev.ensure_synclite_txn_table()?;
        if !dev.cfg.skip_restart_recovery {
            dev.resolve_in_doubt_txn_on_open()?;
        }
        Ok(dev)
    }

    /// Path of the segment the device is currently writing to.
    pub fn current_segment_path(&self) -> &Path {
        self.log.path()
    }

    /// Sequence number of the active segment.
    pub fn current_segment_sequence(&self) -> SegmentSequence {
        self.current_segment_seq
    }

    /// Finalize the active segment, notify the segment-ready hook if any,
    /// and open the next segment.
    pub fn roll_segment(&mut self) -> Result<()> {
        self.flush_txn_records(false, false)?;
        // Same swap-then-drop ordering as the SQLite backend: drop the
        // finalized segment's file handle before notifying the shipper.
        let next_seq = SegmentSequence(self.current_segment_seq.0 + 1);
        let new_log = LogSegmentWriter::create_with_page_size_and_flush_batch(
            segment_path(&self.cfg, next_seq),
            self.cfg.log_segment_page_size,
            self.cfg.log_segment_flush_batch_size,
        )?;
        let old_log = std::mem::replace(&mut self.log, new_log);
        let finalized_path = old_log.path().to_path_buf();
        let should_ship = !old_log.is_empty()? && old_log.last_commit_fate_decided()?;
        if should_ship {
            old_log.finalize()?;
        }
        drop(old_log);
        if should_ship {
            if let Some(cb) = &self.cfg.on_segment_ready {
                cb(&finalized_path);
            }
        } else if !finalized_path.exists() {
            // Nothing to do.
        } else if rusqlite::Connection::open(&finalized_path)
            .ok()
            .and_then(|c| c.query_row::<i64, _, _>("SELECT COUNT(*) FROM commandlog", [], |r| r.get(0)).ok())
            .unwrap_or(1)
            == 0
        {
            std::fs::remove_file(&finalized_path).ok();
        }
        self.current_segment_seq = next_seq;
        self.next_change_number = 0;
        Ok(())
    }

    fn append_record(&mut self, sql: Option<&str>, params: &[Value]) -> Result<()> {
        let rec = self.make_record(sql, params);
        self.log.append(&rec)
    }

    fn queue_record(&mut self, sql: Option<&str>, params: &[Value]) -> Result<()> {
        if !self.txn_runtime.is_open() {
            if !self.user_txn_open {
                let mut begin = self.conn.prepare("BEGIN").map_err(db_err)?;
                let mut rows = begin.query([]).map_err(db_err)?;
                while rows.next().map_err(db_err)?.is_some() {}
                self.user_txn_open = true;
            }
            if self.logs_txn_markers() {
                // Java multiwriter parity: BEGIN belongs to main segment, not
                // to the per-transaction staged txn file.
                self.append_record(Some("BEGIN"), &[])?;
            }
            self.txn_runtime.open_if_needed();
        }
        let rec = self.make_record(sql, params);
        self.txn_runtime.push(rec);
        Ok(())
    }

    fn ensure_txn_open_for_user_sql(&mut self) -> Result<()> {
        if !self.uses_staged_txn_scheme() || self.txn_runtime.is_open() {
            return Ok(());
        }
        if !self.user_txn_open {
            let mut begin = self.conn.prepare("BEGIN").map_err(db_err)?;
            let mut rows = begin.query([]).map_err(db_err)?;
            while rows.next().map_err(db_err)?.is_some() {}
            self.user_txn_open = true;
        }
        if self.logs_txn_markers() {
            // Java multiwriter parity: BEGIN belongs to main segment.
            self.append_record(Some("BEGIN"), &[])?;
        }
        self.txn_runtime.open_if_needed();
        Ok(())
    }

    fn make_record(&mut self, sql: Option<&str>, params: &[Value]) -> CommandLogRecord {
        make_record(
            &mut self.next_change_number,
            self.next_commit_id,
            &mut self.next_operation_id,
            sql,
            params,
        )
    }

    fn flush_txn_records(&mut self, publish: bool, include_replay_txn: bool) -> Result<()> {
        if !self.uses_staged_txn_scheme() {
            return Ok(());
        }
        self.spill_pending_to_stage()?;
        let Some(stage) = self.txn_stage.take() else {
            return Ok(());
        };
        if !publish {
            stage.cleanup();
            return Ok(());
        }
        if include_replay_txn {
            self.append_record(Some("REPLAY_TXN"), &[])?;
        }
        let _published = stage.publish(&self.cfg.segment_dir, self.current_segment_seq)?;
        Ok(())
    }

    fn spill_pending_to_stage(&mut self) -> Result<()> {
        let records = self.txn_runtime.drain();
        if records.is_empty() {
            return Ok(());
        }
        // Txn body rows are written into per-transaction files, so they
        // should not consume the master segment's change-number sequence.
        self.next_change_number = self
            .next_change_number
            .saturating_sub(records.len() as u64);
        let commit_id = records[0].commit_id.0;
        if self.txn_stage.is_none() {
            self.txn_stage = Some(TxnStage::create(
                &self.cfg.segment_dir,
                self.cfg.log_segment_page_size,
                self.cfg.log_segment_flush_batch_size,
                commit_id,
            )?);
        }
        let stage = self.txn_stage.as_mut().expect("txn_stage initialized");
        for rec in &records {
            stage.append_record(rec)?;
        }
        Ok(())
    }

    fn policy_mode(&self) -> Option<SqlPolicyMode> {
        if self.cfg.device_type.is_store() {
            Some(SqlPolicyMode::Store)
        } else {
            None
        }
    }

    fn validate_insert_all_values(&self, insert: &InsertShape) -> Result<()> {
        let table_col_count = read_duckdb_table_info(&self.conn, &insert.table_name)
            .map(|cols| cols.len())
            .unwrap_or(0);
        if table_col_count == 0 {
            return Ok(());
        }
        match insert.column_count {
            Some(col_count) if col_count != table_col_count => Err(Error::Db(format!(
                "INSERT column count ({col_count}) does not match table '{}' column count ({}). All table columns must be specified.",
                insert.table_name, table_col_count
            ))),
            None if insert.value_count != table_col_count => Err(Error::Db(format!(
                "INSERT value count ({}) does not match table '{}' column count ({}). A value must be supplied for every table column.",
                insert.value_count, insert.table_name, table_col_count
            ))),
            _ => Ok(()),
        }
    }

    fn enforce_sql_policy(&self, sql: &str) -> Result<()> {
        let Some(mode) = self.policy_mode() else {
            return Ok(());
        };
        let shape = validate_sql_policy(sql, mode)?;
        if let SqlShape::Insert(insert) = shape {
            self.validate_insert_all_values(&insert)?;
        }
        Ok(())
    }

    fn validate_shadow_sql(&self, sql: &str, params: &[Value]) -> Result<()> {
        let stmt = self.shadow_conn.prepare(sql).map_err(sqlite_err)?;
        let expected = stmt.parameter_count();
        if expected != params.len() {
            return Err(Error::Db(format!(
                "sqlite shadow validation failed for SQL parameter count: expected {expected}, got {}",
                params.len()
            )));
        }
        Ok(())
    }

    fn refresh_shadow_schema(&mut self) -> Result<()> {
        let shadow_path = shadow_schema_path(&self.cfg);
        let replacement = SqliteConnection::open_in_memory().map_err(sqlite_err)?;
        let old_shadow = std::mem::replace(&mut self.shadow_conn, replacement);
        drop(old_shadow);
        rebuild_shadow_schema(&self.conn, &shadow_path)?;
        self.shadow_conn = SqliteConnection::open(&shadow_path).map_err(sqlite_err)?;
        Ok(())
    }

    fn ensure_synclite_txn_table(&mut self) -> Result<()> {
        self.conn
            .prepare("CREATE TABLE IF NOT EXISTS synclite_txn(commit_id BIGINT PRIMARY KEY, operation_id BIGINT)")
            .map_err(db_err)?
            .execute([])
            .map_err(db_err)?;

        let rows = self.query("SELECT COUNT(*) FROM synclite_txn", &[])?;
        let count = match rows.first().and_then(|r| r.first()) {
            Some(ArgValue::Int(v)) => *v,
            _ => 0,
        };
        if count == 0 {
            let vals = [DuckValue::BigInt(0), DuckValue::BigInt(0)];
            self.conn
                .prepare("INSERT INTO synclite_txn(commit_id, operation_id) VALUES(?, ?)")
                .map_err(db_err)?
                .execute(params_from_iter(vals.iter()))
                .map_err(db_err)?;
        }
        Ok(())
    }

    fn update_synclite_txn(&mut self, commit_id: u64, operation_id: u64) -> Result<()> {
        let vals = [
            DuckValue::BigInt(commit_id as i64),
            DuckValue::BigInt(operation_id as i64),
        ];
        let updated = self
            .conn
            .prepare("UPDATE synclite_txn SET commit_id = ?, operation_id = ?")
            .map_err(db_err)?
            .execute(params_from_iter(vals.iter()))
            .map_err(db_err)?;
        if updated == 0 {
            let vals = [
                DuckValue::BigInt(commit_id as i64),
                DuckValue::BigInt(operation_id as i64),
            ];
            self.conn
                .prepare("INSERT INTO synclite_txn(commit_id, operation_id) VALUES(?, ?)")
                .map_err(db_err)?
                .execute(params_from_iter(vals.iter()))
                .map_err(db_err)?;
        }
        Ok(())
    }

    fn flush_user_db_state(&mut self) -> Result<()> {
        // Make checkpoint state visible across immediate close/reopen cycles,
        // matching Java restart-recovery expectations around synclite_txn.
        let mut stmt = self.conn.prepare("CHECKPOINT").map_err(db_err)?;
        let mut rows = stmt.query([]).map_err(db_err)?;
        while rows.next().map_err(db_err)?.is_some() {
            // Drain any checkpoint result rows.
        }
        Ok(())
    }

    fn synclite_txn_commit_id(&mut self) -> Result<u64> {
        let rows = self.query("SELECT MAX(commit_id) FROM synclite_txn", &[])?;
        let commit = rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| match v {
                ArgValue::Int(i) => Some(*i),
                _ => None,
            })
            .unwrap_or(0);
        if commit < 0 {
            return Err(Error::Db(format!(
                "invalid synclite_txn.commit_id {commit} in {}",
                self.cfg.db_path.display()
            )));
        }
        Ok(commit as u64)
    }

    fn segment_last_txn_state(path: &Path) -> Result<Option<(u64, TxnFate)>> {
        let c = SqliteConnection::open(path).map_err(sqlite_err)?;
        let max_commit: Option<i64> = c
            .query_row("SELECT MAX(commit_id) FROM commandlog", [], |r| r.get(0))
            .map_err(sqlite_err)?;
        let Some(max_commit) = max_commit else {
            return Ok(None);
        };
        if max_commit < 0 {
            return Err(Error::Db(format!(
                "invalid commit_id {max_commit} in segment {}",
                path.display()
            )));
        }
        let commit_id = max_commit as u64;
        let commit_count: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM commandlog WHERE commit_id = ?1 AND sql = 'COMMIT'",
                rusqlite::params![max_commit],
                |r| r.get(0),
            )
            .map_err(sqlite_err)?;
        let rollback_count: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM commandlog WHERE commit_id = ?1 AND sql = 'ROLLBACK'",
                rusqlite::params![max_commit],
                |r| r.get(0),
            )
            .map_err(sqlite_err)?;
        let fate = if commit_count > 0 {
            TxnFate::Commit
        } else if rollback_count > 0 {
            TxnFate::Rollback
        } else {
            TxnFate::Unknown
        };
        Ok(Some((commit_id, fate)))
    }

    fn scan_last_segment_txn_state(dir: &Path) -> Result<Option<(PathBuf, u64, TxnFate)>> {
        if !dir.exists() {
            return Ok(None);
        }
        let mut best: Option<(u64, PathBuf)> = None;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            let Some(num) = name.strip_suffix(".sqllog") else {
                continue;
            };
            let Ok(n) = num.parse::<u64>() else {
                continue;
            };
            if best.as_ref().map_or(true, |(b, _)| n > *b) {
                best = Some((n, entry.path()));
            }
        }
        let Some((_, path)) = best else {
            return Ok(None);
        };
        let Some((commit_id, fate)) = Self::segment_last_txn_state(&path)? else {
            return Ok(None);
        };
        Ok(Some((path, commit_id, fate)))
    }

    fn append_recovery_fate(path: &Path, commit_id: u64, fate_sql: &'static str) -> Result<()> {
        let c = SqliteConnection::open(path).map_err(sqlite_err)?;
        let next_change: i64 = c
            .query_row(
                "SELECT COALESCE(MAX(change_number), -1) + 1 FROM commandlog",
                [],
                |r| r.get(0),
            )
            .map_err(sqlite_err)?;
        c.execute(
            "INSERT INTO commandlog(change_number, commit_id, sql, arg_cnt) VALUES(?1, ?2, ?3, 0)",
            rusqlite::params![next_change, commit_id as i64, fate_sql],
        )
        .map_err(sqlite_err)?;
        Ok(())
    }

    fn remove_recovery_commit(path: &Path, commit_id: u64) -> Result<()> {
        let c = SqliteConnection::open(path).map_err(sqlite_err)?;
        c.execute(
            "DELETE FROM commandlog WHERE commit_id = ?1",
            rusqlite::params![commit_id as i64],
        )
        .map_err(sqlite_err)?;
        Ok(())
    }

    fn resolve_in_doubt_txn_on_open(&mut self) -> Result<()> {
        if !self.cfg.device_type.participates_in_restart_recovery() {
            return Ok(());
        }
        let master_commit = self.synclite_txn_commit_id()?;
        if master_commit == 0 {
            return Ok(());
        }
        let slave_commit = self.restart_slave_commit_id;
        let fate = self.restart_txn_fate.unwrap_or(TxnFate::Unknown);
        if slave_commit == 0 {
            return Ok(());
        }
        if slave_commit < master_commit {
            return Err(Error::Db(format!(
                "restart recovery failed for {}: database commit_id {} is larger than last logged commit_id {}",
                self.cfg.db_path.display(),
                master_commit,
                slave_commit
            )));
        }
        if slave_commit > master_commit {
            match fate {
                TxnFate::Commit => {
                    return Err(Error::Db(format!(
                        "restart recovery failed for {}: commit_id {} has COMMIT fate but synclite_txn commit_id is {}",
                        self.cfg.db_path.display(),
                        slave_commit,
                        master_commit
                    )));
                }
                TxnFate::Unknown => {
                    if let Some(path) = &self.restart_segment_path {
                        // Java SyncTxnLogger rollback recovery removes the
                        // in-doubt commit rows instead of appending ROLLBACK.
                        Self::remove_recovery_commit(path, slave_commit)?;
                    }
                }
                TxnFate::Rollback => {}
            }
            return Ok(());
        }
        match fate {
            TxnFate::Rollback => Err(Error::Db(format!(
                "restart recovery failed for {}: commit_id {} has ROLLBACK fate but synclite_txn commit_id matches it",
                self.cfg.db_path.display(),
                slave_commit
            ))),
            TxnFate::Unknown => {
                if let Some(path) = &self.restart_segment_path {
                    Self::append_recovery_fate(path, slave_commit, "COMMIT")
                } else {
                    Ok(())
                }
            }
            TxnFate::Commit => Ok(()),
        }
    }
}

fn make_record(
    next_change_number: &mut u64,
    next_commit_id: u64,
    next_operation_id: &mut u64,
    sql: Option<&str>,
    params: &[Value],
) -> CommandLogRecord {
    let rec = CommandLogRecord {
        change_number: *next_change_number,
        commit_id: CommitId(next_commit_id),
        operation_id: OperationId(*next_operation_id),
        sql: sql.map(|s| s.to_string()),
        arg_count: params.len() as u32,
        args: params.to_vec(),
    };
    *next_change_number += 1;
    *next_operation_id += 1;
    rec
}

fn segment_path(cfg: &DuckDbDeviceConfig, seq: SegmentSequence) -> PathBuf {
    cfg.segment_dir.join(format!("{}.sqllog", seq.0))
}

fn txn_published_path(dir: &Path, seq: SegmentSequence, commit_id: u64) -> PathBuf {
    dir.join(format!("{}.sqllog.{}.txn", seq.0, commit_id))
}

fn shadow_schema_path(cfg: &DuckDbDeviceConfig) -> PathBuf {
    let file_name = cfg
        .db_path
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("device.duckdb"));
    cfg.segment_dir.join(format!("{}.sqlite", file_name.to_string_lossy()))
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn rebuild_shadow_schema(conn: &Connection, shadow_path: &Path) -> Result<()> {
    if shadow_path.exists() {
        std::fs::remove_file(shadow_path)?;
    }
    let shadow = SqliteConnection::open(shadow_path).map_err(sqlite_err)?;
    let table_names = read_duckdb_table_names(conn)?;
    for table_name in table_names {
        let columns = read_duckdb_table_info(conn, &table_name)?;
        if columns.is_empty() {
            continue;
        }
        let create_sql = build_sqlite_create_table(&table_name, &columns);
        shadow.execute_batch(&create_sql).map_err(sqlite_err)?;
    }
    Ok(())
}

fn read_duckdb_table_names(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn
        .prepare(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema NOT IN ('information_schema', 'pg_catalog') \
             AND table_type = 'BASE TABLE' ORDER BY table_name",
        )
        .map_err(db_err)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(db_err)?;
    let mut tables = Vec::new();
    for row in rows {
        tables.push(row.map_err(db_err)?);
    }
    Ok(tables)
}

#[derive(Debug)]
struct DuckColumn {
    name: String,
    declared_type: String,
    nullable: bool,
    default_value: Option<String>,
    primary_key_ordinal: i64,
}

fn read_duckdb_table_info(conn: &Connection, table_name: &str) -> Result<Vec<DuckColumn>> {
    let escaped = table_name.replace('\'', "''");
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info('{escaped}')"))
        .map_err(db_err)?;
    let rows = stmt
        .query_map([], |row| {
            Ok(DuckColumn {
                name: row.get(1)?,
                declared_type: row.get::<_, Option<String>>(2)?.unwrap_or_else(|| "TEXT".into()),
                nullable: row.get::<_, bool>(3).map(|not_null| !not_null).unwrap_or(true),
                default_value: row.get(4)?,
                primary_key_ordinal: row.get::<_, i64>(5).unwrap_or(0),
            })
        })
        .map_err(db_err)?;
    let mut cols = Vec::new();
    for row in rows {
        cols.push(row.map_err(db_err)?);
    }
    Ok(cols)
}

fn build_sqlite_create_table(table_name: &str, columns: &[DuckColumn]) -> String {
    let mut parts = Vec::new();
    let mut primary_keys: Vec<(i64, String)> = Vec::new();
    for column in columns {
        let mut part = format!("{} {}", quote_ident(&column.name), column.declared_type);
        if !column.nullable {
            part.push_str(" NOT NULL");
        }
        if let Some(default_value) = &column.default_value {
            part.push_str(" DEFAULT ");
            part.push_str(default_value);
        }
        if column.primary_key_ordinal > 0 {
            primary_keys.push((column.primary_key_ordinal, quote_ident(&column.name)));
        }
        parts.push(part);
    }
    if primary_keys.len() == 1 {
        let primary_key = &primary_keys[0].1;
        for part in &mut parts {
            if part.starts_with(primary_key) {
                part.push_str(" PRIMARY KEY");
                primary_keys.clear();
                break;
            }
        }
    }
    if !primary_keys.is_empty() {
        primary_keys.sort_by_key(|(ordinal, _)| *ordinal);
        let joined = primary_keys
            .into_iter()
            .map(|(_, name)| name)
            .collect::<Vec<_>>()
            .join(", ");
        parts.push(format!("PRIMARY KEY({joined})"));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {}({})",
        quote_ident(table_name),
        parts.join(", ")
    )
}

/// See `logger_db_sqlite::next_monotonic_commit_id` — same algorithm.
fn next_monotonic_commit_id(prior: u64) -> u64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    if now_ms > prior {
        now_ms
    } else {
        prior + 1
    }
}

impl DbDevice for DuckDbDevice {
    fn backend(&self) -> Backend {
        Backend::DuckDb
    }

    fn pre_user_execute(&mut self, sql: &str, params: &[Value]) -> Result<()> {
        self.enforce_sql_policy(sql)?;
        if !is_ddl(sql) {
            self.validate_shadow_sql(sql, params)?;
        }
        Ok(())
    }

    fn post_user_execute(&mut self, sql: &str) -> Result<()> {
        if is_ddl(sql) {
            self.refresh_shadow_schema()?;
        }
        Ok(())
    }

    fn pre_user_execute_batch(&mut self, sql: &str, batch_params: &[Vec<Value>]) -> Result<()> {
        self.enforce_sql_policy(sql)?;
        if !is_ddl(sql) {
            if let Some(params) = batch_params.first() {
                // SQL shape/policy is batch-invariant; validating once avoids
                // O(batch_size) shadow prepare/plan overhead.
                self.validate_shadow_sql(sql, params)?;
            }
        }
        Ok(())
    }

    fn post_user_execute_batch(&mut self, sql: &str) -> Result<()> {
        if is_ddl(sql) {
            self.refresh_shadow_schema()?;
        }
        Ok(())
    }

    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64> {
        self.pre_user_execute(sql, params)?;
        self.ensure_txn_open_for_user_sql()?;
        // duckdb-rs's `Statement::execute` panics in `rows_changed()` for
        // some statement shapes (DDL, certain INSERTs) because the result
        // handle isn't populated. We sidestep that by going through the
        // query path and draining the result, then returning 0. Callers
        // that need an accurate affected-row count should use a dedicated
        // `SELECT changes()`-style probe.
        let bound: Vec<DuckValue> = params.iter().map(arg_to_duckvalue).collect();
        {
            let mut stmt = self.conn.prepare(sql).map_err(db_err)?;
            let mut rows = stmt
                .query(params_from_iter(bound.iter()))
                .map_err(db_err)?;
            while rows.next().map_err(db_err)?.is_some() {
                // drain — DuckDB requires the result to be consumed.
            }
        }
        self.post_user_execute(sql)?;
        self.log_record(Some(sql), params)?;
        Ok(0)
    }

    fn execute_unlogged(&mut self, sql: &str, params: &[Value]) -> Result<u64> {
        self.pre_user_execute(sql, params)?;
        self.ensure_txn_open_for_user_sql()?;
        let bound: Vec<DuckValue> = params.iter().map(arg_to_duckvalue).collect();
        {
            let mut stmt = self.conn.prepare(sql).map_err(db_err)?;
            let mut rows = stmt
                .query(params_from_iter(bound.iter()))
                .map_err(db_err)?;
            while rows.next().map_err(db_err)?.is_some() {
                // drain — DuckDB requires the result to be consumed.
            }
        }
        self.post_user_execute(sql)?;
        Ok(0)
    }

    fn log_record(&mut self, sql: Option<&str>, params: &[Value]) -> Result<()> {
        if self.uses_staged_txn_scheme() {
            self.queue_record(sql, params)?;
            Ok(())
        } else {
            self.append_record(sql, params)
        }
    }

    fn query(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        let bound: Vec<DuckValue> = params.iter().map(arg_to_duckvalue).collect();
        let mut stmt = self.conn.prepare(sql).map_err(db_err)?;
        // NOTE: in duckdb-rs, `column_count` requires the statement to have
        // been executed (it inspects the result handle), so we read it from
        // each materialized row instead of from the bare statement.
        let rows = stmt
            .query_map(params_from_iter(bound.iter()), |row| {
                let col_count = row.as_ref().column_count();
                let mut out = Vec::with_capacity(col_count);
                for i in 0..col_count {
                    let v: DuckValue = row.get(i)?;
                    out.push(duckvalue_to_arg(v));
                }
                Ok(out)
            })
            .map_err(db_err)?;
        let mut collected = Vec::new();
        for r in rows {
            collected.push(r.map_err(db_err)?);
        }
        Ok(collected)
    }

    fn execute_prepared_batch(&mut self, sql: &str, batch_params: &[Vec<Value>]) -> Result<Vec<u64>> {
        self.pre_user_execute_batch(sql, batch_params)?;
        if let Some(out) = self.try_execute_prepared_batch_with_appender(sql, batch_params)? {
            return Ok(out);
        }
        self.ensure_txn_open_for_user_sql()?;
        let mut out = Vec::with_capacity(batch_params.len());
        let mut stmt = self.conn.prepare(sql).map_err(db_err)?;
        for params in batch_params.iter() {
            for (param_idx, param) in params.iter().enumerate() {
                stmt.raw_bind_parameter(param_idx + 1, arg_to_duckvalue(param))
                    .map_err(db_err)?;
            }
            let res = stmt.raw_execute().map_err(db_err)? as u64;
            out.push(res);
        }
        self.post_user_execute_batch(sql)?;
        Ok(out)
    }

    fn commit(&mut self) -> Result<()> {
        let has_pending_work = self.next_operation_id > 0
            || self.txn_runtime.is_open()
            || self.user_txn_open
            || self.txn_stage.is_some();
        if !has_pending_work {
            return Ok(());
        }

        let committed_id = self.next_commit_id;
        // Java SyncLiteConnection.commit(): recordCommit() happens before
        // log flush / DB commit / logCommitAndFlush.
        self.update_synclite_txn(committed_id, 0)?;
        if self.uses_staged_txn_scheme() {
            self.txn_runtime.close_with_tail(None);
            self.flush_txn_records(true, true)?;
            if self.user_txn_open {
                let mut commit = self.conn.prepare("COMMIT").map_err(db_err)?;
                let mut rows = commit.query([]).map_err(db_err)?;
                while rows.next().map_err(db_err)?.is_some() {}
                self.user_txn_open = false;
            }
            if self.logs_txn_markers() {
                self.append_record(Some("COMMIT"), &[])?;
            }
        }
        self.flush_user_db_state()?;
        self.last_committed_commit_id = committed_id;
        self.next_commit_id = next_monotonic_commit_id(self.next_commit_id);
        self.next_operation_id = 0;
        Ok(())
    }

    fn flush_log(&mut self) -> Result<()> {
        if self.uses_staged_txn_scheme() {
            self.spill_pending_to_stage()?;
        }
        Ok(())
    }

    fn roll_segment(&mut self) -> Result<()> {
        DuckDbDevice::roll_segment(self)
    }

    fn rollback(&mut self) -> Result<()> {
        let has_pending_work = self.next_operation_id > 0
            || self.txn_runtime.is_open()
            || self.user_txn_open
            || self.txn_stage.is_some();
        if !has_pending_work {
            return Ok(());
        }

        if self.uses_staged_txn_scheme() {
            self.txn_runtime.close_with_tail(None);
            // Rollback fate: staged txn body must not be published.
            self.flush_txn_records(false, false)?;
            if self.user_txn_open {
                let mut rollback = self.conn.prepare("ROLLBACK").map_err(db_err)?;
                let mut rows = rollback.query([]).map_err(db_err)?;
                while rows.next().map_err(db_err)?.is_some() {}
                self.user_txn_open = false;
            }
            if self.logs_txn_markers() {
                self.append_record(Some("ROLLBACK"), &[])?;
            }
        }
        self.next_commit_id = next_monotonic_commit_id(self.next_commit_id);
        self.next_operation_id = 0;
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<()> {
        // Take ownership and finalize+drop the segment writer before
        // notifying the shipper. On Windows the shipper can otherwise
        // see a sharing violation reading a segment whose SQLite handle
        // is still open here.
        let mut me = *self;
        let dangling_commit_id = me.next_commit_id;
        let has_dangling_txn = me.next_operation_id > 0 || me.txn_runtime.is_open() || me.txn_stage.is_some();
        if me.uses_staged_txn_scheme() && me.txn_runtime.is_open() {
            me.txn_runtime.close_with_tail(None);
            me.flush_txn_records(false, false)?;
        }
        if has_dangling_txn
            && me.logs_txn_markers()
            && !me.log.commit_fate_decided(dangling_commit_id)?
        {
            // Autocommit-style close: finalize pending implicit transaction
            // so the segment ships instead of being dropped.
            me.append_record(Some("COMMIT"), &[])?;
            me.next_commit_id = next_monotonic_commit_id(me.next_commit_id);
            me.next_operation_id = 0;
        }
        me.flush_txn_records(false, false)?;
        let logs_markers = me.logs_txn_markers();
        let DuckDbDevice {
            cfg,
            conn: _,
            log,
            ..
        } = me;
        let path = log.path().to_path_buf();
        // When the device doesn't log txn markers, segments don't carry
        // COMMIT rows, so the fate check is not applicable.
        let should_ship = !log.is_empty()? && (!logs_markers || log.last_commit_fate_decided()?);
        if should_ship {
            log.finalize()?;
        }
        drop(log);
        if should_ship {
            if let Some(cb) = &cfg.on_segment_ready {
                cb(&path);
            }
        } else if rusqlite::Connection::open(&path)
            .ok()
            .and_then(|c| c.query_row::<i64, _, _>("SELECT COUNT(*) FROM commandlog", [], |r| r.get(0)).ok())
            .unwrap_or(1)
            == 0
        {
            std::fs::remove_file(&path).ok();
        }
        Ok(())
    }
}

fn arg_to_duckvalue(v: &ArgValue) -> DuckValue {
    match v {
        ArgValue::Null => DuckValue::Null,
        ArgValue::Int(i) => DuckValue::BigInt(*i),
        ArgValue::Real(f) => DuckValue::Double(*f),
        ArgValue::Text(s) => DuckValue::Text(s.clone()),
        ArgValue::Blob(b) => DuckValue::Blob(b.clone()),
    }
}

fn duckvalue_to_arg(v: DuckValue) -> ArgValue {
    match v {
        DuckValue::Null => ArgValue::Null,
        DuckValue::Boolean(b) => ArgValue::Int(if b { 1 } else { 0 }),
        DuckValue::TinyInt(i) => ArgValue::Int(i as i64),
        DuckValue::SmallInt(i) => ArgValue::Int(i as i64),
        DuckValue::Int(i) => ArgValue::Int(i as i64),
        DuckValue::BigInt(i) => ArgValue::Int(i),
        DuckValue::UTinyInt(i) => ArgValue::Int(i as i64),
        DuckValue::USmallInt(i) => ArgValue::Int(i as i64),
        DuckValue::UInt(i) => ArgValue::Int(i as i64),
        DuckValue::UBigInt(i) => ArgValue::Int(i as i64),
        DuckValue::Float(f) => ArgValue::Real(f as f64),
        DuckValue::Double(f) => ArgValue::Real(f),
        DuckValue::Text(s) => ArgValue::Text(s),
        DuckValue::Blob(b) => ArgValue::Blob(b),
        // Anything we don't model explicitly is rendered as text via Debug,
        // preserving information while keeping our Value type small.
        other => ArgValue::Text(format!("{other:?}")),
    }
}

fn db_err(e: duckdb::Error) -> Error {
    Error::Db(e.to_string())
}

fn sqlite_err(e: rusqlite::Error) -> Error {
    Error::Db(e.to_string())
}


