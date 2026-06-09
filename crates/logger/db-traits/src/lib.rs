//! Backend-agnostic abstractions for SyncLite database devices.
//!
//! A [`DbDevice`] is the runtime handle the user holds: it forwards SQL to an
//! embedded database (SQLite, DuckDB, ...) **and** captures every mutation to
//! the SyncLite command log. Adding a new backend means implementing this
//! trait in a fresh crate; the rest of the workspace stays untouched.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use logger_core::{record::{ArgValue, CommandLogRecord}, Backend, Result};

/// A SQL parameter / bound argument.
///
/// Re-exported here as the public input type for [`DbDevice::execute`] so
/// callers don't have to import from `synclite-core::record`.
pub type Value = ArgValue;

/// One row's worth of selected values (used by simple read paths).
pub type Row = Vec<Value>;

/// Runtime-owned transactional log queue state.
///
/// Backends use this shared state to keep transactional queue semantics
/// out of ad-hoc API method logic.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct TxnLogRuntime {
    open: bool,
    pending: Vec<CommandLogRecord>,
}

impl TxnLogRuntime {
    /// Create an empty runtime queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether a logical transaction is currently open.
    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Mark the logical transaction as open if it is not already open.
    pub fn open_if_needed(&mut self) {
        if !self.open {
            self.open = true;
        }
    }

    /// Ensure the transaction is open, injecting a BEGIN record if needed.
    pub fn begin_if_needed(&mut self, begin: CommandLogRecord) {
        if !self.open {
            self.pending.push(begin);
            self.open = true;
        }
    }

    /// Queue one record into the active transactional buffer.
    pub fn push(&mut self, rec: CommandLogRecord) {
        self.pending.push(rec);
    }

    /// Close the current logical transaction and optionally append a tail
    /// marker record (COMMIT/ROLLBACK).
    pub fn close_with_tail(&mut self, tail: Option<CommandLogRecord>) {
        if self.open {
            if let Some(rec) = tail {
                self.pending.push(rec);
            }
            self.open = false;
        }
    }

    /// Drain all queued transactional records.
    pub fn drain(&mut self) -> Vec<CommandLogRecord> {
        std::mem::take(&mut self.pending)
    }
}

/// The runtime handle to one open SyncLite device.
///
/// Implementations must be `Send + Sync` so a device can be passed across
/// threads. Methods take `&mut self` because they mutate internal state
/// (transaction status, log writer position).
pub trait DbDevice: Send {
    /// Identifies the underlying backend.
    fn backend(&self) -> Backend;

    /// Execute a non-query statement (INSERT / UPDATE / DELETE / DDL).
    ///
    /// Returns the number of rows affected, where the backend reports it.
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64>;

    /// Execute a non-query statement without writing to commandlog.
    ///
    /// This still applies backend SQL-policy checks and mutates the user DB,
    /// but intentionally bypasses SyncLite log capture.
    fn execute_unlogged(&mut self, sql: &str, params: &[Value]) -> Result<u64>;

    /// Runtime pre-hook before user DB executes one mutating statement.
    ///
    /// DuckDB uses this to validate user SQL against its shadow schema.
    fn pre_user_execute(&mut self, _sql: &str, _params: &[Value]) -> Result<()> {
        Ok(())
    }

    /// Runtime post-hook after user DB executes one mutating statement.
    ///
    /// DuckDB uses this to refresh shadow schema after DDL.
    fn post_user_execute(&mut self, _sql: &str) -> Result<()> {
        Ok(())
    }

    /// Runtime pre-hook before user DB executes a prepared batch.
    fn pre_user_execute_batch(&mut self, sql: &str, batch_params: &[Vec<Value>]) -> Result<()> {
        for params in batch_params {
            self.pre_user_execute(sql, params)?;
        }
        Ok(())
    }

    /// Runtime post-hook after user DB executes a prepared batch.
    fn post_user_execute_batch(&mut self, sql: &str) -> Result<()> {
        self.post_user_execute(sql)
    }

    /// Append one logical log record without executing SQL on the user DB.
    fn log_record(&mut self, sql: Option<&str>, params: &[Value]) -> Result<()>;

    /// Execute a query and collect all rows eagerly.
    ///
    /// Phase 1 keeps the API simple; streaming iterators arrive in a later
    /// phase once the surface stabilizes.
    fn query(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Row>>;

    /// Execute one prepared SQL statement for multiple argument sets.
    ///
    /// SyncLite behavior note: implementations may log SQL only for the first
    /// batch row and log subsequent rows as args-only (`sql = NULL`).
    fn execute_prepared_batch(&mut self, sql: &str, batch_params: &[Vec<Value>]) -> Result<Vec<u64>> {
        let mut out = Vec::with_capacity(batch_params.len());
        for params in batch_params {
            out.push(self.execute(sql, params)?);
        }
        Ok(out)
    }

    /// Commit the current transaction (if any) and flush pending log records
    /// to the active segment.
    fn commit(&mut self) -> Result<()>;

    /// Latest commit-id that has been successfully recorded by this device
    /// (advanced inside `commit()`). Returns 0 if no user commit has
    /// landed yet. Used by `synclite::await_sync` so it does not have to
    /// crack open the source DB — important for backends like DuckDB
    /// where the file is not a SQLite database.
    fn last_committed_commit_id(&self) -> i64 {
        0
    }

    /// Flush buffered log records without committing/rolling back fate.
    ///
    /// Transactional backends may use this to durably stage queued records.
    fn flush_log(&mut self) -> Result<()> {
        Ok(())
    }

    /// Roll back the current transaction.
    ///
    /// Transactional devices log rollback fate; store/event devices may
    /// implement this as a no-op parity hook.
    fn rollback(&mut self) -> Result<()>;

    /// Finalize the active log segment and start a fresh one. Lets callers
    /// force the shipper to see in-flight work without closing the device.
    /// Backends that don't write segments may leave this as a no-op.
    fn roll_segment(&mut self) -> Result<()> {
        Ok(())
    }

    /// Close the device, finalizing the in-flight log segment so a consumer
    /// can ship it. After this call the device is unusable.
    fn close(self: Box<Self>) -> Result<()>;
}


