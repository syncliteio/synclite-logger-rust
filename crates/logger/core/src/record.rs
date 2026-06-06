//! Wire-level data types for the command log.
//!
//! These types model the rows of the `commandlog` table written by the
//! Java logger. Preserving this schema is what lets the existing Java
//! consolidator consume log segments produced by the Rust logger.

use crate::ids::{CommitId, OperationId};
use serde::{Deserialize, Serialize};

/// A single logged SQL command together with its bound arguments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandLogRecord {
    /// Per-segment monotonic change number (PRIMARY KEY of `commandlog`).
    pub change_number: u64,
    /// Commit this command belongs to.
    pub commit_id: CommitId,
    /// Operation index within the commit.
    pub operation_id: OperationId,
    /// The SQL text as issued by the application.
    ///
    /// Java prepared-statement batching logs SQL for the first entry and
    /// stores `NULL` for subsequent entries that reuse the same statement.
    pub sql: Option<String>,
    /// Number of bound arguments (0 if none).
    pub arg_count: u32,
    /// Bound argument values, length must equal `arg_count`.
    pub args: Vec<ArgValue>,
}

/// A bound SQL argument value. Mirrors the types the Java logger inlines into
/// the `commandlog` table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ArgValue {
    /// SQL NULL.
    Null,
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating point.
    Real(f64),
    /// UTF-8 text.
    Text(String),
    /// Raw bytes (BLOB).
    Blob(Vec<u8>),
}

