//! SyncLite log-segment writer.
//!
//! A *log segment* is a small SQLite file that records the sequence of SQL
//! commands a device has executed since the previous segment. The schema is
//! intentionally identical to the one written by the Java logger so the
//! existing Java SyncLite Consolidator can consume Rust-produced segments
//! unchanged:
//!
//! ```text
//! commandlog(change_number PRIMARY KEY, commit_id, sql, arg_cnt, arg1, arg2, ..., arg16)
//! metadata(key TEXT PRIMARY KEY, value TEXT)
//! ```
//!
//! 16 `argN` columns are pre-allocated at create time (Java's
//! `SyncLiteOptions.maxInlinedLogArgs` default); records carrying more
//! args grow the table via `ALTER TABLE`. The `synclite_txn` table that
//! Java keeps in the *user DB* is intentionally absent here — log
//! segments carry commit ids on the `commandlog` rows themselves.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod scan;
mod segment;
mod status;

pub use scan::{scan_segment_dir, ResumeState};
pub use segment::{
	LogSegmentWriter,
	DEFAULT_LOG_SEGMENT_FLUSH_BATCH_SIZE,
	DEFAULT_LOG_SEGMENT_PAGE_SIZE,
};
pub use status::LogSegmentStatus;

