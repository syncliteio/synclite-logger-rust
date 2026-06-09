//! SQLite-backed [`DbDevice`](logger_db_traits::DbDevice) implementation.
//!
//! Wraps a user-facing SQLite database file *and* a rolling sequence of log
//! segments. Every mutating call is forwarded to the underlying database
//! **and** appended to the active segment so a downstream consumer can
//! replay the workload.
//!
//! Segments are named `<prefix>-<seq>.db` inside `segment_dir`. Call
//! [`SqliteDevice::roll_segment`] (or rely on [`DbDevice::close`]) to
//! finalize the active segment so a downstream archiver can ship it.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, params_from_iter, types::Value as SqlValue, Connection};
use logger_core::{
    record::{ArgValue, CommandLogRecord},
    sql_policy::{is_ddl, validate_sql_policy, InsertShape, SqlPolicyMode, SqlShape},
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
pub type SegmentReadyCallback = Arc<dyn Fn(&Path) + Send + Sync>;

/// Configuration for opening a [`SqliteDevice`].
#[derive(Clone)]
pub struct SqliteDeviceConfig {
    /// Path to the user-facing SQLite database file.
    pub db_path: PathBuf,
    /// Directory in which log segments are written. Created on first use.
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

impl SqliteDeviceConfig {
    /// Convenience constructor with no segment-ready hook.
    pub fn new<P: Into<PathBuf>, S: Into<PathBuf>>(db_path: P, segment_dir: S) -> Self {
        Self {
            db_path: db_path.into(),
            segment_dir: segment_dir.into(),
            resume_dir: None,
            on_segment_ready: None,
            log_segment_page_size: DEFAULT_LOG_SEGMENT_PAGE_SIZE,
            log_segment_flush_batch_size: DEFAULT_LOG_SEGMENT_FLUSH_BATCH_SIZE,
            device_type: DeviceType::SQLITE,
            max_inlined_log_args: 16,
            skip_restart_recovery: false,
        }
    }
}

impl std::fmt::Debug for SqliteDeviceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteDeviceConfig")
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

/// SQLite device: user DB + rolling log segments.
pub struct SqliteDevice {
    cfg: SqliteDeviceConfig,
    conn: Connection,
    user_txn_open: bool,
    log: LogSegmentWriter,
    current_segment_seq: SegmentSequence,
    next_change_number: u64,
    next_commit_id: u64,
    last_committed_commit_id: u64,
    next_operation_id: u64,
    txn_runtime: TxnLogRuntime,
    restart_segment_path: Option<PathBuf>,
    restart_slave_commit_id: u64,
    restart_txn_fate: Option<TxnFate>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxnFate {
    Commit,
    Rollback,
    Unknown,
}

impl SqliteDevice {
    /// Open (or create) the user database and start a fresh segment.
    ///
    /// If `segment_dir` already contains finalized segments from a previous
    /// run (typical when no shipper is configured and segments stay local),
    /// the new segment's sequence number continues from `max + 1`. The
    /// initial `next_commit_id` is `max(System.currentTimeMillis(),
    /// MAX(commit_id) FROM prior commandlog + 1)` so commit ids stay
    /// monotonic across `close()` / reopen cycles.
    pub fn open(cfg: SqliteDeviceConfig) -> Result<Self> {
        std::fs::create_dir_all(&cfg.segment_dir)?;
        let conn = Connection::open(&cfg.db_path).map_err(db_err)?;
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
            user_txn_open: false,
            log,
            current_segment_seq: resume.next_seq,
            next_change_number: 0,
            next_commit_id: initial_commit,
            last_committed_commit_id: 0,
            next_operation_id: 0,
            txn_runtime: TxnLogRuntime::new(),
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
    /// and open the next segment. The new segment continues the
    /// `commit_id` sequence so the consolidator sees a continuous stream.
    pub fn roll_segment(&mut self) -> Result<()> {
        self.flush_txn_records(false, false)?;
        // Prepare the new segment first, then swap it into `self.log` so we
        // can finalize+drop the old writer (releasing its SQLite file
        // handles) BEFORE notifying the shipper. On Windows the shipper
        // would otherwise hit a sharing violation reading the finalized
        // segment file.
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
        } else if Connection::open(&finalized_path)
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
                self.conn.execute_batch("BEGIN").map_err(db_err)?;
                self.user_txn_open = true;
            }
            let begin = self.make_record(Some("BEGIN"), &[]);
            self.txn_runtime.begin_if_needed(begin);
        }
        let rec = self.make_record(sql, params);
        self.txn_runtime.push(rec);
        Ok(())
    }

    fn ensure_txn_open_for_user_sql(&mut self) -> Result<()> {
        if !self.cfg.device_type.is_transactional() || self.txn_runtime.is_open() {
            return Ok(());
        }
        if !self.user_txn_open {
            self.conn.execute_batch("BEGIN").map_err(db_err)?;
            self.user_txn_open = true;
        }
        let begin = self.make_record(Some("BEGIN"), &[]);
        self.txn_runtime.begin_if_needed(begin);
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

    fn flush_txn_records(&mut self, _publish: bool, _include_replay_txn: bool) -> Result<()> {
        if !self.cfg.device_type.is_transactional() {
            return Ok(());
        }

        for rec in self.txn_runtime.drain() {
            self.log.append(&rec)?;
        }
        Ok(())
    }

    fn policy_mode(&self) -> Option<SqlPolicyMode> {
        if self.cfg.device_type.is_store() {
            Some(SqlPolicyMode::Store)
        } else if self.cfg.device_type.is_streaming() {
            Some(SqlPolicyMode::Streaming)
        } else {
            None
        }
    }

    fn validate_insert_all_values(&self, insert: &InsertShape) -> Result<()> {
        let mut stmt = match self
            .conn
            .prepare(&format!("PRAGMA table_info({})", insert.table_name))
        {
            Ok(stmt) => stmt,
            Err(_) => return Ok(()),
        };
        let mut rows = stmt.query([]).map_err(db_err)?;
        let mut table_col_count = 0_i64;
        while rows.next().map_err(db_err)?.is_some() {
            table_col_count += 1;
        }

        if table_col_count <= 0 {
            return Ok(());
        }

        match insert.column_count {
            Some(col_count) if col_count as i64 != table_col_count => Err(Error::Db(format!(
                "INSERT column count ({col_count}) does not match table '{}' column count ({}). All table columns must be specified.",
                insert.table_name, table_col_count
            ))),
            None if insert.value_count as i64 != table_col_count => Err(Error::Db(format!(
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

    fn ensure_synclite_txn_table(&mut self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS synclite_txn(commit_id BIGINT PRIMARY KEY, operation_id BIGINT)",
            )
            .map_err(db_err)?;
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM synclite_txn", [], |r| r.get(0))
            .map_err(db_err)?;
        if count == 0 {
            self.conn
                .execute(
                    "INSERT INTO synclite_txn(commit_id, operation_id) VALUES(0, 0)",
                    [],
                )
                .map_err(db_err)?;
        }
        Ok(())
    }

    fn update_synclite_txn(&mut self, commit_id: u64, operation_id: u64) -> Result<()> {
        let updated = self
            .conn
            .execute(
                "UPDATE synclite_txn SET commit_id = ?1, operation_id = ?2",
                params![commit_id as i64, operation_id as i64],
            )
            .map_err(db_err)?;
        if updated == 0 {
            self.conn
                .execute(
                    "INSERT INTO synclite_txn(commit_id, operation_id) VALUES(?1, ?2)",
                    params![commit_id as i64, operation_id as i64],
                )
                .map_err(db_err)?;
        }
        Ok(())
    }

    fn synclite_txn_commit_id(&self) -> Result<u64> {
        let commit: i64 = self
            .conn
            .query_row("SELECT MAX(commit_id) FROM synclite_txn", [], |r| r.get(0))
            .map_err(db_err)?;
        if commit < 0 {
            return Err(Error::Db(format!(
                "invalid synclite_txn.commit_id {commit} in {}",
                self.cfg.db_path.display()
            )));
        }
        Ok(commit as u64)
    }

    fn segment_last_txn_state(path: &Path) -> Result<Option<(u64, TxnFate)>> {
        let c = Connection::open(path).map_err(db_err)?;
        let max_commit: Option<i64> = c
            .query_row("SELECT MAX(commit_id) FROM commandlog", [], |r| r.get(0))
            .map_err(db_err)?;
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
                params![max_commit],
                |r| r.get(0),
            )
            .map_err(db_err)?;
        let rollback_count: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM commandlog WHERE commit_id = ?1 AND sql = 'ROLLBACK'",
                params![max_commit],
                |r| r.get(0),
            )
            .map_err(db_err)?;
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
        let c = Connection::open(path).map_err(db_err)?;
        let next_change: i64 = c
            .query_row(
                "SELECT COALESCE(MAX(change_number), -1) + 1 FROM commandlog",
                [],
                |r| r.get(0),
            )
            .map_err(db_err)?;
        c.execute(
            "INSERT INTO commandlog(change_number, commit_id, sql, arg_cnt) VALUES(?1, ?2, ?3, 0)",
            params![next_change, commit_id as i64, fate_sql],
        )
        .map_err(db_err)?;
        Ok(())
    }

    fn remove_recovery_commit(path: &Path, commit_id: u64) -> Result<()> {
        let c = Connection::open(path).map_err(db_err)?;
        c.execute(
            "DELETE FROM commandlog WHERE commit_id = ?1",
            params![commit_id as i64],
        )
        .map_err(db_err)?;
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
                        if self.cfg.device_type.is_transactional() {
                            Self::append_recovery_fate(path, slave_commit, "ROLLBACK")?;
                        } else {
                            // Java SyncTxnLogger rollback recovery removes the
                            // in-doubt commit rows instead of appending ROLLBACK.
                            Self::remove_recovery_commit(path, slave_commit)?;
                        }
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

fn segment_path(cfg: &SqliteDeviceConfig, seq: SegmentSequence) -> PathBuf {
    cfg.segment_dir.join(format!("{}.sqllog", seq.0))
}

#[allow(dead_code)]
fn txn_published_path(dir: &Path, seq: SegmentSequence, commit_id: u64) -> PathBuf {
    dir.join(format!("{}.sqllog.{}.txn", seq.0, commit_id))
}

/// Return the next commit id given the previously-issued one.
/// Matches `io.synclite.logger.SQLLogger.getNextCommitID` — jump to
/// `System.currentTimeMillis()` when it's larger than the prior id,
/// otherwise increment by one so the sequence stays strictly monotonic
/// even when wall-clock time stalls or moves backwards.
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

impl DbDevice for SqliteDevice {
    fn backend(&self) -> Backend {
        Backend::Sqlite
    }

    fn last_committed_commit_id(&self) -> i64 {
        self.last_committed_commit_id as i64
    }

    fn pre_user_execute(&mut self, sql: &str, _params: &[Value]) -> Result<()> {
        self.enforce_sql_policy(sql)
    }

    fn pre_user_execute_batch(&mut self, sql: &str, _batch_params: &[Vec<Value>]) -> Result<()> {
        self.enforce_sql_policy(sql)
    }

    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64> {
        self.pre_user_execute(sql, params)?;
        self.ensure_txn_open_for_user_sql()?;
        let affected = if self.cfg.device_type.is_streaming() && !is_ddl(sql) {
            0
        } else {
            let bound: Vec<SqlValue> = params.iter().map(arg_to_sqlvalue).collect();
            self.conn
                .execute(sql, params_from_iter(bound.iter()))
                .map_err(db_err)? as u64
        };
        self.log_record(Some(sql), params)?;
        Ok(affected)
    }

    fn execute_unlogged(&mut self, sql: &str, params: &[Value]) -> Result<u64> {
        self.pre_user_execute(sql, params)?;
        self.ensure_txn_open_for_user_sql()?;
        let affected = if self.cfg.device_type.is_streaming() && !is_ddl(sql) {
            0
        } else {
            let bound: Vec<SqlValue> = params.iter().map(arg_to_sqlvalue).collect();
            self.conn
                .execute(sql, params_from_iter(bound.iter()))
                .map_err(db_err)? as u64
        };
        Ok(affected)
    }

    fn log_record(&mut self, sql: Option<&str>, params: &[Value]) -> Result<()> {
        if self.cfg.device_type.is_transactional() {
            self.queue_record(sql, params)
        } else {
            if self.next_operation_id == 0 {
                self.append_record(Some("BEGIN"), &[])?;
            }
            self.append_record(sql, params)
        }
    }

    fn query(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        let bound: Vec<SqlValue> = params.iter().map(arg_to_sqlvalue).collect();
        let mut stmt = self.conn.prepare(sql).map_err(db_err)?;
        let col_count = stmt.column_count();
        let rows = stmt
            .query_map(params_from_iter(bound.iter()), |row| {
                let mut out = Vec::with_capacity(col_count);
                for i in 0..col_count {
                    let v: SqlValue = row.get(i)?;
                    out.push(sqlvalue_to_arg(v));
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
        let device_type = self.cfg.device_type;
        self.ensure_txn_open_for_user_sql()?;

        let mut out = Vec::with_capacity(batch_params.len());

        {
            let mut stmt_opt = if device_type.is_streaming() && !is_ddl(sql) {
                None
            } else {
                Some(self.conn.prepare(sql).map_err(db_err)?)
            };

            for params in batch_params.iter() {
                let affected = if let Some(stmt) = stmt_opt.as_mut() {
                    for (param_idx, param) in params.iter().enumerate() {
                        stmt.raw_bind_parameter(param_idx + 1, arg_to_sqlvalue(param))
                            .map_err(db_err)?;
                    }
                    stmt.raw_execute().map_err(db_err)? as u64
                } else {
                    0
                };
                out.push(affected);
            }
        }

        Ok(out)
    }

    fn commit(&mut self) -> Result<()> {
        let has_pending_work = self.next_operation_id > 0
            || self.txn_runtime.is_open()
            || self.user_txn_open;
        if !has_pending_work {
            return Ok(());
        }

        let committed_id = self.next_commit_id;
        // Java SyncLiteConnection.commit(): recordCommit() happens before
        // log flush / DB commit / logCommitAndFlush.
        self.update_synclite_txn(committed_id, 0)?;
        if self.cfg.device_type.is_transactional() {
            self.txn_runtime.close_with_tail(None);
            self.flush_txn_records(true, false)?;
            if self.user_txn_open {
                self.conn.execute_batch("COMMIT").map_err(db_err)?;
                self.user_txn_open = false;
            }
            self.append_record(Some("COMMIT"), &[])?;
        } else {
            // STORE and STREAMING (SyncTxnLogger style): append COMMIT directly
            self.append_record(Some("COMMIT"), &[])?;
        }
        // Java's SQLLogger.getNextCommitID: jump to wall-clock ms when
        // that's ahead, otherwise increment by one. Keeps ids strictly
        // monotonic AND roughly sortable by wall time across devices.
        self.last_committed_commit_id = committed_id;
        self.next_commit_id = next_monotonic_commit_id(self.next_commit_id);
        self.next_operation_id = 0;
        Ok(())
    }

    fn flush_log(&mut self) -> Result<()> {
        if self.cfg.device_type.is_transactional() {
            self.flush_txn_records(false, false)?;
        }
        Ok(())
    }

    fn roll_segment(&mut self) -> Result<()> {
        SqliteDevice::roll_segment(self)
    }

    fn rollback(&mut self) -> Result<()> {
        let has_pending_work = self.next_operation_id > 0
            || self.txn_runtime.is_open()
            || self.user_txn_open;
        if !has_pending_work {
            return Ok(());
        }

        if self.cfg.device_type.is_transactional() {
            self.txn_runtime.close_with_tail(None);
            self.flush_txn_records(true, false)?;
            if self.user_txn_open {
                self.conn.execute_batch("ROLLBACK").map_err(db_err)?;
                self.user_txn_open = false;
            }
            self.append_record(Some("ROLLBACK"), &[])?;
        } else {
            // STORE and STREAMING (SyncTxnLogger style): append ROLLBACK directly
            self.append_record(Some("ROLLBACK"), &[])?;
        }
        self.next_commit_id = next_monotonic_commit_id(self.next_commit_id);
        self.next_operation_id = 0;
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<()> {
        // Take ownership of the segment writer first so we can finalize
        // it AND release its file handles before notifying the shipper.
        // On Windows the shipper may otherwise see a sharing violation if
        // it tries to copy a segment whose SQLite connection is still open
        // here.
        let mut me = *self;
        let dangling_commit_id = me.next_commit_id;
        if me.cfg.device_type.is_transactional() {
            let tail = if me.txn_runtime.is_open() {
                Some(me.make_record(Some("ROLLBACK"), &[]))
            } else {
                None
            };
            me.txn_runtime.close_with_tail(tail);
        }
        me.flush_txn_records(false, false)?;
        if !me.cfg.device_type.is_transactional()
            && me.next_operation_id > 0
            && !me.log.commit_fate_decided(dangling_commit_id)?
        {
            // Autocommit-style close: finalize the pending implicit
            // transaction so the segment ships instead of being dropped.
            me.append_record(Some("COMMIT"), &[])?;
            me.next_commit_id = next_monotonic_commit_id(me.next_commit_id);
            me.next_operation_id = 0;
        }
        let SqliteDevice {
            cfg,
            conn: _,
            log,
            ..
        } = me;
        let path = log.path().to_path_buf();
        let should_ship = !log.is_empty()? && log.last_commit_fate_decided()?;
        if should_ship {
            log.finalize()?;
        }
        drop(log);
        if should_ship {
            if let Some(cb) = &cfg.on_segment_ready {
                cb(&path);
            }
        } else if Connection::open(&path)
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

fn arg_to_sqlvalue(v: &ArgValue) -> SqlValue {
    match v {
        ArgValue::Null => SqlValue::Null,
        ArgValue::Int(i) => SqlValue::Integer(*i),
        ArgValue::Real(f) => SqlValue::Real(*f),
        ArgValue::Text(s) => SqlValue::Text(s.clone()),
        ArgValue::Blob(b) => SqlValue::Blob(b.clone()),
    }
}

fn sqlvalue_to_arg(v: SqlValue) -> ArgValue {
    match v {
        SqlValue::Null => ArgValue::Null,
        SqlValue::Integer(i) => ArgValue::Int(i),
        SqlValue::Real(f) => ArgValue::Real(f),
        SqlValue::Text(s) => ArgValue::Text(s),
        SqlValue::Blob(b) => ArgValue::Blob(b),
    }
}

fn db_err(e: rusqlite::Error) -> Error {
    Error::Db(e.to_string())
}


