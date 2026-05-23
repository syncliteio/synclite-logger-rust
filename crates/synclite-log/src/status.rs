//! Lifecycle status persisted on each log segment via its `metadata` table.
//!
//! Mirrors the Java `LogSegmentStatus` enum. The string values must stay in
//! lock-step with the Java side because the consolidator reads them
//! verbatim.

use std::fmt;

/// Lifecycle status of a log segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogSegmentStatus {
    /// Newly created, still being written.
    New,
    /// Closed and ready to ship to staging / be applied by the consolidator.
    ReadyToApply,
}

impl LogSegmentStatus {
    /// Wire representation written into the segment's `metadata.status` row.
    pub const fn as_str(self) -> &'static str {
        match self {
            LogSegmentStatus::New => "NEW",
            LogSegmentStatus::ReadyToApply => "READY_TO_APPLY",
        }
    }
}

impl fmt::Display for LogSegmentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
