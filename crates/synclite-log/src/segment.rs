//! `LogSegmentWriter` — single-segment, append-only writer.
//!
//! Schema parity with the Java logger
//! (`io.synclite.logger.SQLLogger.createLogTableSqlTemplate`):
//!
//! ```sql
//! CREATE TABLE commandlog(
//!     change_number INTEGER PRIMARY KEY,
//!     commit_id     INTEGER,
//!     sql           TEXT,
//!     arg_cnt       INTEGER,
//!     arg1, arg2, ..., arg16   -- pre-allocated inlined-arg columns
//! );
//! CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT);
//! ```
//!
//! Records with more than 16 args trigger `ALTER TABLE ... ADD COLUMN`
//! to widen the table, matching Java's `addNewInlinedArgCols` path.
//! There is intentionally **no `synclite_txn` table** in the log
//! segment: Java keeps that table in the user DB only; the log segment
//! carries commit ids on the `commandlog` rows themselves and consumers
//! read `MAX(commit_id) FROM commandlog` to determine state.

use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use rusqlite::{types::Null, Connection};
use synclite_core::{
    record::{ArgValue, CommandLogRecord},
    Error, Result,
};

use crate::status::LogSegmentStatus;

/// Java's `SyncLiteOptions.maxInlinedLogArgs` default.
const DEFAULT_INLINED_ARG_COLUMNS: u32 = 16;
/// Java's default `logSegmentPageSize`.
pub const DEFAULT_LOG_SEGMENT_PAGE_SIZE: u32 = 512;
/// Java-typical flush cadence used to amortize fsync overhead.
pub const DEFAULT_LOG_SEGMENT_FLUSH_BATCH_SIZE: u64 = 10_000;

/// Single-segment, append-only writer.
pub struct LogSegmentWriter {
    path: PathBuf,
    inner: Mutex<Inner>,
}

struct Inner {
    conn: Connection,
    /// Current count of `argN` columns the `commandlog` table exposes.
    /// Starts at [`DEFAULT_INLINED_ARG_COLUMNS`]; grows on demand.
    arg_columns: u32,
    /// Cached INSERT statement matching `arg_columns`. Rebuilt whenever
    /// the table widens.
    insert_sql: String,
    /// Whether `finalize` has been called.
    finalized: bool,
    /// Number of appended rows since last SQLite COMMIT.
    pending_since_commit: u64,
    /// Commit frequency for batched segment writes.
    flush_batch_size: u64,
}

impl LogSegmentWriter {
    /// Create a new segment file at `path` and initialize its schema.
    ///
    /// Fails if `path` already exists — segments are write-once.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_with_page_size_and_arg_columns(
            path,
            DEFAULT_LOG_SEGMENT_PAGE_SIZE,
            DEFAULT_INLINED_ARG_COLUMNS,
        )
    }

    /// Create a new segment file with an explicit SQLite page size.
    pub fn create_with_page_size<P: AsRef<Path>>(path: P, page_size: u32) -> Result<Self> {
        Self::create_with_page_size_and_flush_batch(
            path,
            page_size,
            DEFAULT_LOG_SEGMENT_FLUSH_BATCH_SIZE,
        )
    }

    /// Create a new segment file with explicit SQLite page size and flush batch size.
    pub fn create_with_page_size_and_flush_batch<P: AsRef<Path>>(
        path: P,
        page_size: u32,
        flush_batch_size: u64,
    ) -> Result<Self> {
        Self::create_with_page_size_and_arg_columns_and_flush_batch(
            path,
            page_size,
            DEFAULT_INLINED_ARG_COLUMNS,
            flush_batch_size,
        )
    }

    /// Create a new segment file with explicit page size and inlined-arg count.
    pub fn create_with_page_size_and_arg_columns<P: AsRef<Path>>(
        path: P,
        page_size: u32,
        arg_columns: u32,
    ) -> Result<Self> {
        Self::create_with_page_size_and_arg_columns_and_flush_batch(
            path,
            page_size,
            arg_columns,
            DEFAULT_LOG_SEGMENT_FLUSH_BATCH_SIZE,
        )
    }

    /// Create a new segment file with explicit page size, inlined-arg count,
    /// and flush batch size.
    pub fn create_with_page_size_and_arg_columns_and_flush_batch<P: AsRef<Path>>(
        path: P,
        page_size: u32,
        arg_columns: u32,
        flush_batch_size: u64,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            return Err(Error::Log(format!(
                "segment file already exists: {}",
                path.display()
            )));
        }
        let conn = Connection::open(&path).map_err(db_err)?;
        let arg_columns = arg_columns.max(1);
        let arg_cols_decl = arg_columns_decl(arg_columns);
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS commandlog(\
                 change_number INTEGER PRIMARY KEY,\
                 commit_id     INTEGER,\
                 sql           TEXT,\
                 arg_cnt       INTEGER,\
                 {arg_cols_decl}\
             )"
        );
        conn.execute_batch(&format!(
            "PRAGMA journal_mode = DELETE;\n\
             PRAGMA synchronous = FULL;\n\
             PRAGMA locking_mode = EXCLUSIVE;\n\
             PRAGMA temp_store = MEMORY;\n\
             PRAGMA mmap_size = 0;\n\
             PRAGMA page_size = {page_size};"
        ))
        .map_err(db_err)?;

        conn.execute_batch(&format!(
            "BEGIN;\n\
             {create_sql};\n\
             CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT);\n\
             INSERT INTO metadata(key, value) VALUES('status', 'NEW');\n\
             COMMIT;"
        ))
        .map_err(db_err)?;

        // Keep an explicit write transaction open and flush periodically.
        conn.execute_batch("BEGIN IMMEDIATE;").map_err(db_err)?;

        let insert_sql = build_insert_sql(arg_columns);
        Ok(Self {
            path,
            inner: Mutex::new(Inner {
                conn,
                arg_columns,
                insert_sql,
                finalized: false,
                pending_since_commit: 0,
                flush_batch_size: flush_batch_size.max(1),
            }),
        })
    }

    /// Path of the underlying segment file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one [`CommandLogRecord`] to the segment.
    pub fn append(&self, record: &CommandLogRecord) -> Result<()> {
        if record.args.len() as u32 != record.arg_count {
            return Err(Error::Log(format!(
                "record arg_count={} but args.len()={}",
                record.arg_count,
                record.args.len()
            )));
        }
        let mut inner = self.inner.lock();
        if inner.finalized {
            return Err(Error::Log("segment already finalized".into()));
        }
        ensure_arg_columns(&mut inner, record.arg_count)?;

        {
            let mut stmt = inner
                .conn
                .prepare_cached(&inner.insert_sql)
                .map_err(db_err)?;
            stmt.raw_bind_parameter(1, record.change_number as i64)
                .map_err(db_err)?;
            stmt.raw_bind_parameter(2, record.commit_id.0 as i64)
                .map_err(db_err)?;
            if let Some(sql) = &record.sql {
                stmt.raw_bind_parameter(3, sql.as_str()).map_err(db_err)?;
            } else {
                stmt.raw_bind_parameter(3, Null).map_err(db_err)?;
            }
            stmt.raw_bind_parameter(4, record.arg_count as i64)
                .map_err(db_err)?;

            for i in 0..inner.arg_columns as usize {
                let bind_idx = i + 5;
                if i < record.args.len() {
                    match &record.args[i] {
                        ArgValue::Null => stmt.raw_bind_parameter(bind_idx, Null).map_err(db_err)?,
                        ArgValue::Int(v) => stmt.raw_bind_parameter(bind_idx, *v).map_err(db_err)?,
                        ArgValue::Real(v) => stmt.raw_bind_parameter(bind_idx, *v).map_err(db_err)?,
                        ArgValue::Text(v) => stmt
                            .raw_bind_parameter(bind_idx, v.as_str())
                            .map_err(db_err)?,
                        ArgValue::Blob(v) => stmt
                            .raw_bind_parameter(bind_idx, v.as_slice())
                            .map_err(db_err)?,
                    }
                } else {
                    stmt.raw_bind_parameter(bind_idx, Null).map_err(db_err)?;
                }
            }
            stmt
                .raw_execute()
                .map_err(db_err)?;
        }
        inner.pending_since_commit += 1;
        if inner.pending_since_commit >= inner.flush_batch_size {
            inner
                .conn
                .execute_batch("COMMIT; BEGIN IMMEDIATE;")
                .map_err(db_err)?;
            inner.pending_since_commit = 0;
        }
        Ok(())
    }

    /// Whether the segment currently has zero commandlog rows.
    pub fn is_empty(&self) -> Result<bool> {
        let inner = self.inner.lock();
        let count: i64 = inner
            .conn
            .query_row("SELECT COUNT(*) FROM commandlog", [], |r| r.get(0))
            .map_err(db_err)?;
        Ok(count == 0)
    }

    /// Mark the segment ready for shipping. Flips `metadata.status` to
    /// `READY_TO_APPLY`. Subsequent `append` calls fail. Unlike the
    /// earlier prototype, no `synclite_txn` row is written — Java keeps
    /// commit-id state on `commandlog` rows only.
    pub fn finalize(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.finalized {
            return Ok(());
        }

        // Ensure all commandlog rows are durably committed before status flip.
        inner.conn.execute_batch("COMMIT;").map_err(db_err)?;

        inner
            .conn
            .execute(
                "UPDATE metadata SET value = ?1 WHERE key = 'status'",
                [LogSegmentStatus::ReadyToApply.as_str()],
            )
            .map_err(db_err)?;
        inner.pending_since_commit = 0;
        inner.finalized = true;
        Ok(())
    }
}

fn ensure_arg_columns(inner: &mut Inner, needed: u32) -> Result<()> {
    let mut widened = false;
    while inner.arg_columns < needed {
        let next = inner.arg_columns + 1;
        let sql = format!("ALTER TABLE commandlog ADD COLUMN arg{next}");
        inner.conn.execute(&sql, []).map_err(db_err)?;
        inner.arg_columns = next;
        widened = true;
    }
    if widened {
        inner.insert_sql = build_insert_sql(inner.arg_columns);
    }
    Ok(())
}

fn arg_columns_decl(n: u32) -> String {
    (1..=n)
        .map(|i| format!("arg{i}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn build_insert_sql(arg_columns: u32) -> String {
    let cols = arg_columns_decl(arg_columns);
    let placeholders = std::iter::repeat("?")
        .take(4 + arg_columns as usize)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO commandlog(change_number, commit_id, sql, arg_cnt, {cols}) VALUES ({placeholders})"
    )
}

fn db_err(e: rusqlite::Error) -> Error {
    Error::Log(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use synclite_core::{CommitId, OperationId};
    use tempfile::tempdir;

    fn rec(change: u64, sql: &str, args: Vec<ArgValue>) -> CommandLogRecord {
        CommandLogRecord {
            change_number: change,
            commit_id: CommitId(1),
            operation_id: OperationId(change),
                sql: Some(sql.into()),
            arg_count: args.len() as u32,
            args,
        }
    }

    #[test]
    fn writes_and_finalizes_segment() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segment-0.db");

        let w = LogSegmentWriter::create(&path).unwrap();
        w.append(&rec(0, "CREATE TABLE t(x INT)", vec![])).unwrap();
        w.append(&rec(
            1,
            "INSERT INTO t(x) VALUES (?)",
            vec![ArgValue::Int(42)],
        ))
        .unwrap();
        w.append(&rec(
            2,
            "INSERT INTO t(x) VALUES (?)",
            vec![ArgValue::Text("hello".into())],
        ))
        .unwrap();
        w.finalize().unwrap();

        let conn = Connection::open(&path).unwrap();
        let count: i64 = conn
            .query_row("SELECT count(*) FROM commandlog", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 3);

        let status: String = conn
            .query_row("SELECT value FROM metadata WHERE key = 'status'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(status, "READY_TO_APPLY");

        // arg1..arg16 columns exist pre-allocated.
        let arg16: Option<i64> = conn
            .query_row(
                "SELECT arg16 FROM commandlog WHERE change_number = 0",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(arg16.is_none());

        // synclite_txn must NOT exist in the segment file.
        let exists: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='synclite_txn'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(exists, 0);
    }

    #[test]
    fn rejects_append_after_finalize() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segment-1.db");
        let w = LogSegmentWriter::create(&path).unwrap();
        w.finalize().unwrap();
        let err = w.append(&rec(0, "SELECT 1", vec![])).unwrap_err();
        assert!(matches!(err, Error::Log(_)));
    }

    #[test]
    fn widens_beyond_default_arg_columns() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segment-wide.db");
        let w = LogSegmentWriter::create(&path).unwrap();
        let many: Vec<ArgValue> = (0..20).map(|i| ArgValue::Int(i as i64)).collect();
        w.append(&rec(0, "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", many))
            .unwrap();
        w.finalize().unwrap();
        let conn = Connection::open(&path).unwrap();
        let arg20: i64 = conn
            .query_row("SELECT arg20 FROM commandlog WHERE change_number = 0", [], |r| r.get(0))
            .unwrap();
        assert_eq!(arg20, 19);
    }
}
