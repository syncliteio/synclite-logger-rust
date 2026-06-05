//! Resume helpers: scan an existing segment directory.

use std::path::{Path, PathBuf};

use rusqlite::Connection;
use logger_core::{Error, Result, SegmentSequence};

/// State recovered from a previously-used segment directory.
#[derive(Debug, Clone)]
pub struct ResumeState {
    /// Sequence number for the *next* segment to create.
    pub next_seq: SegmentSequence,
    /// `MAX(commit_id)` observed across all prior segments in the dir.
    /// `0` when no prior segment was found (fresh stage directory).
    /// Callers seed their own `next_commit_id` source — typically
    /// `max(System.currentTimeMillis(), max_commit_id + 1)` to keep
    /// commit ids monotonic across restarts.
    pub max_commit_id: u64,
}

impl Default for ResumeState {
    fn default() -> Self {
        Self {
            next_seq: SegmentSequence(0),
            max_commit_id: 0,
        }
    }
}

/// Scan `dir` for files named `<N>.sqllog` (matching the SyncLite logger's
/// `SyncLite.getLogSegmentPath` formula). Returns the [`ResumeState`] a
/// fresh device should adopt to continue the segment stream produced by
/// a previous run on the same directory.
///
/// - When `dir` is missing, returns [`ResumeState::default`].
/// - When no matching files exist, returns [`ResumeState::default`].
/// - Otherwise picks the file with the highest `N` and reads
///   `MAX(commit_id) FROM commandlog` to carry the value forward.
pub fn scan_segment_dir(dir: &Path) -> Result<ResumeState> {
    if !dir.exists() {
        return Ok(ResumeState::default());
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
    let Some((n, path)) = best else {
        return Ok(ResumeState::default());
    };
    let conn = Connection::open(&path).map_err(|e| Error::Log(e.to_string()))?;
    let max_commit: Option<i64> = conn
        .query_row("SELECT MAX(commit_id) FROM commandlog", [], |r| r.get(0))
        .map_err(|e| Error::Log(e.to_string()))?;
    let max_commit_id = match max_commit {
        Some(v) if v > 0 => v as u64,
        _ => 0,
    };
    Ok(ResumeState {
        next_seq: SegmentSequence(n + 1),
        max_commit_id,
    })
}


