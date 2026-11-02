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
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use logger_core::{
    record::{ArgValue, CommandLogRecord},
    sql_policy::{
        classify_sql_statement, is_ddl, validate_sql_policy, InsertShape, SqlPolicyMode, SqlShape,
        SqlStatementKind,
    },
    Backend, CommitId, DeviceType, Error, OperationId, Result, SegmentSequence,
};
use logger_db_traits::{ColumnInfo, DbDevice, ExecutionResult, Row, TxnLogRuntime, Value};
use logger_log::{
    recovery, scan_segment_dir, LogSegmentWriter, DEFAULT_LOG_SEGMENT_FLUSH_BATCH_SIZE,
    DEFAULT_LOG_SEGMENT_PAGE_SIZE,
};
use rusqlite::{params, params_from_iter, types::Value as SqlValue, Connection};

/// Callback invoked with the path of every finalized log segment.
pub type SegmentReadyCallback = Arc<dyn Fn(&Path) + Send + Sync>;

/// Callback invoked after the active segment sequence changes.
pub type ActiveSegmentCallback = Arc<dyn Fn(SegmentSequence) + Send + Sync>;

/// Configuration for opening a [`SqliteDevice`].
#[derive(Clone)]
pub struct SqliteDeviceConfig {
    /// Path to the user-facing SQLite database file.
    pub db_path: PathBuf,
    /// Directory in which log segments are written. Created on first use.
    pub segment_dir: PathBuf,
    /// Directory used by the shipper/consolidator for staged segments. It is
    /// never scanned for logger restart recovery.
    pub resume_dir: Option<PathBuf>,
    /// Metadata sequence identifying the active segment on restart. When it
    /// is absent, the active device home is scanned as a legacy fallback.
    pub resume_segment_sequence: Option<u64>,
    /// Optional hook called every time a segment is finalized.
    pub on_segment_ready: Option<SegmentReadyCallback>,
    /// Optional hook called after the active segment sequence is established.
    pub on_active_segment_changed: Option<ActiveSegmentCallback>,
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
    /// Auto-switch the active segment once it has accumulated this many log
    /// records. Checked only at commit boundaries so a transaction never
    /// spans two segments. `None` disables count-based switching.
    /// Java default: 1_000_000.
    pub log_segment_switch_log_count_threshold: Option<u64>,
    /// Auto-switch the active segment once it is older than this many
    /// milliseconds. Checked only at commit boundaries. `None` disables
    /// time-based switching. Java default: 5_000.
    pub log_segment_switch_duration_threshold_ms: Option<u64>,
}

impl SqliteDeviceConfig {
    /// Convenience constructor with no segment-ready hook.
    pub fn new<P: Into<PathBuf>, S: Into<PathBuf>>(db_path: P, segment_dir: S) -> Self {
        Self {
            db_path: db_path.into(),
            segment_dir: segment_dir.into(),
            resume_dir: None,
            resume_segment_sequence: None,
            on_segment_ready: None,
            on_active_segment_changed: None,
            log_segment_page_size: DEFAULT_LOG_SEGMENT_PAGE_SIZE,
            log_segment_flush_batch_size: DEFAULT_LOG_SEGMENT_FLUSH_BATCH_SIZE,
            device_type: DeviceType::SQLITE,
            max_inlined_log_args: 16,
            skip_restart_recovery: false,
            log_segment_switch_log_count_threshold: Some(1_000_000),
            log_segment_switch_duration_threshold_ms: Some(5_000),
        }
    }
}

impl std::fmt::Debug for SqliteDeviceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteDeviceConfig")
            .field("db_path", &self.db_path)
            .field("segment_dir", &self.segment_dir)
            .field("resume_dir", &self.resume_dir)
            .field("resume_segment_sequence", &self.resume_segment_sequence)
            .field("on_segment_ready", &self.on_segment_ready.is_some())
            .field(
                "on_active_segment_changed",
                &self.on_active_segment_changed.is_some(),
            )
            .field("log_segment_page_size", &self.log_segment_page_size)
            .field(
                "log_segment_flush_batch_size",
                &self.log_segment_flush_batch_size,
            )
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
    current_segment_created_at: Instant,
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
    /// If metadata identifies an active segment in `segment_dir`, that
    /// segment is reopened. Legacy devices without metadata scan only the
    /// device home as a migration fallback; staged segments are never used
    /// for restart recovery. The
    /// initial `next_commit_id` is `max(System.currentTimeMillis(),
    /// MAX(commit_id) FROM prior commandlog + 1)` so commit ids stay
    /// monotonic across `close()` / reopen cycles.
    pub fn open(cfg: SqliteDeviceConfig) -> Result<Self> {
        std::fs::create_dir_all(&cfg.segment_dir)?;
        let conn = Connection::open(&cfg.db_path).map_err(db_err)?;
        let active =
            recovery::resolve_active_segment(&cfg.segment_dir, cfg.resume_segment_sequence)?;
        let active_resume = scan_segment_dir(&cfg.segment_dir)?;
        // Only used when there is no writable active segment, i.e. the
        // persisted segment was finalized (Java `doSwitchLogSegment`
        // crashed before creating `seq + 1`, or `close()` shipped it).
        // Continue strictly with `seq + 1`; a brand-new device starts at 0.
        let next_seq = if active.is_none() {
            // Occupied slots are shipped below by the finalized sweep.
            let seq =
                recovery::next_segment_sequence(&cfg.segment_dir, cfg.resume_segment_sequence)?;
            recovery::claim_segment_slot(&cfg.segment_dir, seq)?.0
        } else {
            SegmentSequence(0)
        };
        let restart_state = if let Some((_, path)) = &active {
            Self::segment_last_txn_state(path)?.map(|(commit, fate)| (path.clone(), commit, fate))
        } else {
            None
        };
        let log = if let Some((_, path)) = active.as_ref() {
            LogSegmentWriter::open_existing_with_page_size_and_arg_columns_and_flush_batch(
                path,
                cfg.log_segment_page_size,
                cfg.max_inlined_log_args,
                cfg.log_segment_flush_batch_size,
            )?
        } else if cfg.max_inlined_log_args == 16 {
            LogSegmentWriter::create_with_page_size_and_flush_batch(
                segment_path(&cfg, next_seq),
                cfg.log_segment_page_size,
                cfg.log_segment_flush_batch_size,
            )?
        } else {
            LogSegmentWriter::create_with_page_size_and_arg_columns_and_flush_batch(
                segment_path(&cfg, next_seq),
                cfg.log_segment_page_size,
                cfg.max_inlined_log_args,
                cfg.log_segment_flush_batch_size,
            )?
        };
        let initial_commit = next_monotonic_commit_id(active_resume.max_commit_id);
        let next_change_number = active
            .as_ref()
            .map(|(_, path)| Self::next_change_number(path))
            .transpose()?
            .unwrap_or(0);
        let mut dev = Self {
            cfg,
            conn,
            user_txn_open: false,
            log,
            current_segment_seq: active.map_or(next_seq, |(seq, _)| seq),
            next_change_number,
            next_commit_id: initial_commit,
            last_committed_commit_id: 0,
            next_operation_id: 0,
            txn_runtime: TxnLogRuntime::new(),
            current_segment_created_at: Instant::now(),
            restart_segment_path: restart_state.as_ref().map(|s| s.0.clone()),
            restart_slave_commit_id: restart_state.as_ref().map(|s| s.1).unwrap_or(0),
            restart_txn_fate: restart_state.as_ref().map(|s| s.2),
        };
        dev.ensure_synclite_txn_table()?;
        if !dev.cfg.skip_restart_recovery {
            dev.resolve_in_doubt_txn_on_open()?;
            dev.log.flush()?;
        }
        // Java `restartMasterCommitID`: the last commit recorded in the user DB.
        dev.last_committed_commit_id = dev.synclite_txn_commit_id()?;
        if let Some(cb) = &dev.cfg.on_active_segment_changed {
            cb(dev.current_segment_seq);
        }
        // Java `LogMover` parity: a segment finalized just before a crash
        // (before the mover took it) is still in the device home. Hand every
        // such segment to the mover in sequence order so stage stays gap-free.
        if let Some(cb) = &dev.cfg.on_segment_ready {
            for path in
                recovery::finalized_segments_below(&dev.cfg.segment_dir, dev.current_segment_seq)?
            {
                cb(&path);
            }
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
        // Java parity (`checkAndSwitchLogSegment`): only switch when the
        // active segment holds log records and sits at a commit boundary.
        // Empty segments are never finalized, shipped or deleted, so the
        // staged sequence stays gap-free (0, 1, 2, ...).
        if self.log.is_empty()? || !self.log.last_commit_fate_decided()? {
            return Ok(());
        }
        // Java `doSwitchLogSegment`: finish current, create `seq + 1`,
        // then persist the new sequence. Finalizing first means a crash
        // mid-switch leaves either the old segment active (NEW) or the
        // old one finalized with `seq + 1` still to be created on restart.
        let next_seq = SegmentSequence(self.current_segment_seq.0 + 1);
        let (next_seq, skipped) = recovery::claim_segment_slot(&self.cfg.segment_dir, next_seq)?;
        let next_path = segment_path(&self.cfg, next_seq);
        self.log.finalize()?;
        let new_log = LogSegmentWriter::create_with_page_size_and_flush_batch(
            &next_path,
            self.cfg.log_segment_page_size,
            self.cfg.log_segment_flush_batch_size,
        )?;
        // Drop the finalized writer (releasing its SQLite file handles)
        // BEFORE notifying the shipper; on Windows the shipper would
        // otherwise hit a sharing violation reading the segment.
        let old_log = std::mem::replace(&mut self.log, new_log);
        let finalized_path = old_log.path().to_path_buf();
        drop(old_log);
        self.current_segment_seq = next_seq;
        self.next_change_number = 0;
        self.current_segment_created_at = Instant::now();
        if let Some(cb) = &self.cfg.on_active_segment_changed {
            cb(self.current_segment_seq);
        }
        if let Some(cb) = &self.cfg.on_segment_ready {
            cb(&finalized_path);
            for path in &skipped {
                cb(path);
            }
        }
        Ok(())
    }

    /// Java-parity segment auto-switch, evaluated only at commit boundaries
    /// so a transaction is never split across two segment files. Rolls the
    /// active segment once it has accumulated enough log records (count)
    /// or has aged past the duration threshold (time).
    fn maybe_auto_switch_segment(&mut self) -> Result<()> {
        // `next_change_number` is the number of log records written to the
        // current segment; it resets to 0 on every roll.
        if self.next_change_number == 0 {
            return Ok(());
        }
        let count_hit = self
            .cfg
            .log_segment_switch_log_count_threshold
            .is_some_and(|t| self.next_change_number >= t);
        let time_hit = self
            .cfg
            .log_segment_switch_duration_threshold_ms
            .is_some_and(|ms| self.current_segment_created_at.elapsed().as_millis() as u64 >= ms);
        if count_hit || time_hit {
            self.roll_segment()?;
        }
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

    fn next_change_number(path: &Path) -> Result<u64> {
        let c = Connection::open(path).map_err(db_err)?;
        let next: i64 = c
            .query_row(
                "SELECT COALESCE(MAX(change_number), -1) + 1 FROM commandlog",
                [],
                |r| r.get(0),
            )
            .map_err(db_err)?;
        if next < 0 {
            return Err(Error::Db(format!(
                "invalid change number in segment {}",
                path.display()
            )));
        }
        Ok(next as u64)
    }

    fn append_recovery_fate(&mut self, commit_id: u64, fate_sql: &'static str) -> Result<()> {
        // The active writer holds an IMMEDIATE transaction. Reusing it avoids
        // locking the active home segment or accidentally opening a staged
        // copy after the mover has finalized it.
        let record = CommandLogRecord {
            change_number: self.next_change_number,
            commit_id: CommitId(commit_id),
            operation_id: OperationId(self.next_operation_id),
            sql: Some(fate_sql.to_string()),
            arg_count: 0,
            args: Vec::new(),
        };
        self.log.append(&record)?;
        self.next_change_number += 1;
        Ok(())
    }

    fn remove_recovery_commit(&mut self, commit_id: u64) -> Result<()> {
        self.log.delete_commit_rows(commit_id)
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
                    if self.restart_segment_path.is_some() {
                        if self.cfg.device_type.is_transactional() {
                            self.append_recovery_fate(slave_commit, "ROLLBACK")?;
                        } else {
                            // Java SyncTxnLogger rollback recovery removes the
                            // in-doubt commit rows instead of appending ROLLBACK.
                            self.remove_recovery_commit(slave_commit)?;
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
                if self.restart_segment_path.is_some() {
                    self.append_recovery_fate(slave_commit, "COMMIT")
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
    recovery::segment_file_path(&cfg.segment_dir, seq)
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

    fn execute_with_result(&mut self, sql: &str, params: &[Value]) -> Result<ExecutionResult> {
        let readonly = self.conn.prepare(sql).map_err(db_err)?.readonly();
        let bound: Vec<SqlValue> = params.iter().map(arg_to_sqlvalue).collect();

        if readonly {
            let mut stmt = self.conn.prepare(sql).map_err(db_err)?;
            let columns = stmt
                .columns()
                .into_iter()
                .map(|column| ColumnInfo {
                    name: column.name().to_string(),
                    type_name: column.decl_type().map(str::to_string),
                })
                .collect::<Vec<_>>();
            let col_count = columns.len();
            let rows = stmt
                .query_map(params_from_iter(bound.iter()), |row| {
                    let mut out = Vec::with_capacity(col_count);
                    for index in 0..col_count {
                        let value: SqlValue = row.get(index)?;
                        out.push(sqlvalue_to_arg(value));
                    }
                    Ok(out)
                })
                .map_err(db_err)?;
            let mut collected = Vec::new();
            for row in rows {
                collected.push(row.map_err(db_err)?);
            }
            return Ok(ExecutionResult {
                rows: Some(collected),
                columns,
                row_count: None,
                last_insert_rowid: None,
                is_mutating: false,
            });
        }

        self.pre_user_execute(sql, params)?;
        self.ensure_txn_open_for_user_sql()?;
        let kind = classify_sql_statement(sql);

        let (rows, columns, affected) = if self.cfg.device_type.is_streaming() && !is_ddl(sql) {
            (None, Vec::new(), 0)
        } else {
            let mut stmt = self.conn.prepare(sql).map_err(db_err)?;
            let columns = stmt
                .columns()
                .into_iter()
                .map(|column| ColumnInfo {
                    name: column.name().to_string(),
                    type_name: column.decl_type().map(str::to_string),
                })
                .collect::<Vec<_>>();
            if columns.is_empty() {
                let affected = stmt
                    .execute(params_from_iter(bound.iter()))
                    .map_err(db_err)? as u64;
                (None, columns, affected)
            } else {
                let col_count = columns.len();
                let mapped = stmt
                    .query_map(params_from_iter(bound.iter()), |row| {
                        let mut out = Vec::with_capacity(col_count);
                        for index in 0..col_count {
                            let value: SqlValue = row.get(index)?;
                            out.push(sqlvalue_to_arg(value));
                        }
                        Ok(out)
                    })
                    .map_err(db_err)?;
                let mut collected = Vec::new();
                for row in mapped {
                    collected.push(row.map_err(db_err)?);
                }
                (Some(collected), columns, self.conn.changes())
            }
        };

        let row_count = matches!(
            kind,
            SqlStatementKind::Insert | SqlStatementKind::Update | SqlStatementKind::Delete
        )
        .then_some(affected);
        let last_insert_rowid = (kind == SqlStatementKind::Insert && affected > 0)
            .then(|| self.conn.last_insert_rowid());
        self.log_record(Some(sql), params)?;
        Ok(ExecutionResult {
            rows,
            columns,
            row_count,
            last_insert_rowid,
            is_mutating: true,
        })
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

    fn execute_prepared_batch(
        &mut self,
        sql: &str,
        batch_params: &[Vec<Value>],
    ) -> Result<Vec<u64>> {
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
        let has_pending_work =
            self.next_operation_id > 0 || self.txn_runtime.is_open() || self.user_txn_open;
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
            // Java `flush(commitId)`: the log must be durable before the
            // user database commits.
            self.log.flush()?;
            if self.user_txn_open {
                self.conn.execute_batch("COMMIT").map_err(db_err)?;
                self.user_txn_open = false;
            }
            self.append_record(Some("COMMIT"), &[])?;
        } else {
            // STORE and STREAMING (SyncTxnLogger style): append COMMIT directly
            self.append_record(Some("COMMIT"), &[])?;
        }
        // Java `logCommitAndFlush`.
        self.log.flush()?;
        // Java's SQLLogger.getNextCommitID: jump to wall-clock ms when
        // that's ahead, otherwise increment by one. Keeps ids strictly
        // monotonic AND roughly sortable by wall time across devices.
        self.last_committed_commit_id = committed_id;
        self.next_commit_id = next_monotonic_commit_id(self.next_commit_id);
        self.next_operation_id = 0;
        // Commit boundary: safe to roll the segment without splitting a txn.
        self.maybe_auto_switch_segment()?;
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

    fn roll_idle_segment_if_needed(&mut self) -> Result<()> {
        // An open user/log transaction must remain wholly in its current
        // segment. The caller may flush its buffered records, but only its
        // eventual COMMIT/ROLLBACK is allowed to create a roll boundary.
        if self.user_txn_open || self.txn_runtime.is_open() || self.next_operation_id != 0 {
            return Ok(());
        }
        self.maybe_auto_switch_segment()
    }

    fn idle_segment_switch_interval(&self) -> Option<std::time::Duration> {
        self.cfg.log_segment_switch_duration_threshold_ms.map(|ms| {
            // Mirror Java SyncTxnLogger.startLogging(): zero is invalid, so
            // use its 1-second safety fallback rather than spinning.
            std::time::Duration::from_millis(if ms == 0 { 1_000 } else { ms })
        })
    }

    fn rollback(&mut self) -> Result<()> {
        let has_pending_work =
            self.next_operation_id > 0 || self.txn_runtime.is_open() || self.user_txn_open;
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
        // Java `logRollbackAndFlush`.
        self.log.flush()?;
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
            cfg, conn: _, log, ..
        } = me;
        let path = log.path().to_path_buf();
        let should_ship = !log.is_empty()? && log.last_commit_fate_decided()?;
        if should_ship {
            log.finalize()?;
        }
        drop(log);
        // An empty (or in-doubt) active segment stays in the device home
        // and is reopened on restart under the same sequence number, so no
        // sequence is ever skipped. A shipped segment is followed by
        // `seq + 1` on the next open.
        if should_ship {
            if let Some(cb) = &cfg.on_segment_ready {
                cb(&path);
            }
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
