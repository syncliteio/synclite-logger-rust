//! Restart-recovery helpers shared by every device type.
//!
//! Java parity (`SQLLogger.restartRecovery` / `doSwitchLogSegment`): the
//! persisted `log_segment_sequence_number` names the active segment in the
//! device home, segments are numbered strictly `0, 1, 2, ...`, and a switch
//! finalizes segment `N` before creating `N + 1`. These helpers make that
//! model tolerant of a crash at any point without ever failing restart or
//! leaving a gap in the staged sequence.

use std::path::{Path, PathBuf};

use logger_core::{Error, Result, SegmentSequence};
use rusqlite::Connection;

use crate::LogSegmentStatus;

fn log_err(e: rusqlite::Error) -> Error {
    Error::Log(e.to_string())
}

/// `<dir>/<seq>.sqllog`.
pub fn segment_file_path(dir: &Path, seq: SegmentSequence) -> PathBuf {
    dir.join(format!("{}.sqllog", seq.0))
}

fn segment_seq_of(path: &Path) -> Option<u64> {
    path.file_name()?
        .to_str()?
        .strip_suffix(".sqllog")?
        .parse()
        .ok()
}

fn numbered_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut found = Vec::new();
    if !dir.exists() {
        return Ok(found);
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if let Some(seq) = segment_seq_of(&path) {
            found.push((seq, path));
        }
    }
    found.sort_unstable_by_key(|(seq, _)| *seq);
    Ok(found)
}

/// Lifecycle status stored inside a segment; `None` when the file has no
/// initialized `metadata` table (a segment whose creation never completed).
pub fn segment_status(path: &Path) -> Result<Option<String>> {
    let conn = Connection::open(path).map_err(log_err)?;
    match conn.query_row("SELECT value FROM metadata WHERE key = 'status'", [], |r| {
        r.get::<_, String>(0)
    }) {
        Ok(status) => Ok(Some(status)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(rusqlite::Error::SqliteFailure(e, _))
            if e.code == rusqlite::ErrorCode::NotADatabase =>
        {
            Ok(None)
        }
        Err(rusqlite::Error::SqliteFailure(_, Some(msg))) if msg.contains("no such table") => {
            Ok(None)
        }
        Err(e) => Err(log_err(e)),
    }
}

fn is_ready(status: Option<&str>) -> bool {
    status == Some(LogSegmentStatus::ReadyToApply.as_str())
}

/// Segment creation is atomic (schema and `status` row are committed
/// together), so a file without them holds no log records and is safe to drop.
fn remove_unusable_segment(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-journal", path.display()));
}

/// Resolve the writable (`NEW`) active segment in the device home.
///
/// The persisted sequence wins when its segment is still writable. A crash
/// can leave it stale (e.g. the process died after creating `N + 1` but
/// before persisting it), so fall back to the highest writable segment on
/// disk. Half-created files are removed on the way.
pub fn resolve_active_segment(
    dir: &Path,
    persisted_seq: Option<u64>,
) -> Result<Option<(SegmentSequence, PathBuf)>> {
    if let Some(seq) = persisted_seq {
        let path = segment_file_path(dir, SegmentSequence(seq));
        if path.exists() {
            match segment_status(&path)? {
                Some(status) if !is_ready(Some(&status)) => {
                    return Ok(Some((SegmentSequence(seq), path)));
                }
                Some(_) => {}
                None => remove_unusable_segment(&path),
            }
        }
    }
    let mut best = None;
    for (seq, path) in numbered_segments(dir)? {
        match segment_status(&path)? {
            None => remove_unusable_segment(&path),
            Some(status) if !is_ready(Some(&status)) => best = Some((SegmentSequence(seq), path)),
            Some(_) => {}
        }
    }
    Ok(best)
}

/// Sequence for a brand-new segment when no writable active segment exists:
/// one past the persisted sequence and every segment still in the device
/// home. A brand-new device starts at `0`.
pub fn next_segment_sequence(dir: &Path, persisted_seq: Option<u64>) -> Result<SegmentSequence> {
    let after_disk = numbered_segments(dir)?
        .last()
        .map_or(0, |(seq, _)| seq.saturating_add(1));
    let after_persisted = persisted_seq.map_or(0, |seq| seq.saturating_add(1));
    Ok(SegmentSequence(after_disk.max(after_persisted)))
}

/// Finalized (`READY_TO_APPLY`) segments below `active` still sitting in the
/// device home (finalized just before a crash, before the mover took them),
/// in ascending sequence order.
pub fn finalized_segments_below(dir: &Path, active: SegmentSequence) -> Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    for (seq, path) in numbered_segments(dir)? {
        if seq < active.0 && is_ready(segment_status(&path)?.as_deref()) {
            found.push(path);
        }
    }
    Ok(found)
}

/// Claim the slot for a new segment starting at `seq`, never failing the
/// logger and never overwriting log data:
/// - a file with no log records (half-created by a crash) is removed and its
///   slot reused;
/// - a file that holds log records is kept, marked `READY_TO_APPLY` and
///   returned so the caller ships it in order; the next slot is tried.
///
/// Sequence numbers therefore stay contiguous in stage.
pub fn claim_segment_slot(
    dir: &Path,
    mut seq: SegmentSequence,
) -> Result<(SegmentSequence, Vec<PathBuf>)> {
    let mut occupied = Vec::new();
    loop {
        let path = segment_file_path(dir, seq);
        if !path.exists() {
            return Ok((seq, occupied));
        }
        let status = segment_status(&path)?;
        let records = match status {
            None => 0,
            Some(_) => Connection::open(&path)
                .and_then(|c| {
                    c.query_row("SELECT COUNT(*) FROM commandlog", [], |r| {
                        r.get::<_, i64>(0)
                    })
                })
                .unwrap_or(0),
        };
        if records == 0 {
            remove_unusable_segment(&path);
            return Ok((seq, occupied));
        }
        if !is_ready(status.as_deref()) {
            Connection::open(&path)
                .and_then(|c| {
                    c.execute(
                        "UPDATE metadata SET value = ?1 WHERE key = 'status'",
                        [LogSegmentStatus::ReadyToApply.as_str()],
                    )
                })
                .map_err(log_err)?;
        }
        occupied.push(path);
        seq = SegmentSequence(seq.0 + 1);
    }
}
