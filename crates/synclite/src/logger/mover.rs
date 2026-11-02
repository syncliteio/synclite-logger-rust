//! Segment-mover callback factory.
//!
//! When a segment finalizes in the device home, it must be
//! moved (copy + delete) into the per-device stage subdirectory before
//! the shipper sees it. This mirrors `LogMover.copyToWriteArchive` +
//! source delete in the Java logger.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use logger_shipper::LogShipper;
use synclite_observability::{tracer_info, Tracer};

use crate::consolidator::Consolidator;

/// Build a segment-ready callback that:
///   1. Copies the finalized segment from device home → `stage_subdir`,
///   2. Deletes the source file,
///   3. Submits the staged path to `shipper` if one is configured,
///   4. Notifies each `consolidator` about the staged main segment.
///
/// Step 5 is the *path-only* notifier and is intended for deployments
/// without a shipper. When a shipper is wired up the caller should pass
/// `consolidators = vec![]`: notification then flows through the
/// shipper's `on_shipped` hook so the consolidator only sees a segment
/// after every archiver has shipped it (this prevents the
/// consolidator's post-apply cleanup from deleting a segment before it
/// reaches all stages). The `notify_consolidator_for_txn_files` toggle
/// still routes MultiWriter sidecar `.txn` files directly through the
/// mover regardless of shipper presence — txn files are shipper-only
/// for the main on_shipped path.
///
/// All steps are best-effort with error tracing — the callback is fire
/// and forget.
pub fn make_callback(
    stage_subdir: PathBuf,
    _metadata_path: PathBuf,
    shipper: Option<Arc<LogShipper>>,
    consolidators: Vec<Arc<Consolidator>>,
    notify_consolidator_for_txn_files: bool,
    tracer: Arc<Tracer>,
) -> Arc<dyn Fn(&Path) + Send + Sync> {
    Arc::new(move |src: &Path| {
        let Some(name) = src.file_name() else {
            tracing::error!(segment = %src.display(), "segment path has no file name");
            return;
        };
        // Java parity: emit "Log segment closed" INFO when the finalized
        // segment is observed (Java `SQLLogger.finishCurrentLogSegment`).
        if let Some(seq) = parse_seq(name.to_string_lossy().as_ref()) {
            tracer_info!(
                tracer,
                "SQLLogger",
                "Log segment closed : seqNum={} path={}",
                seq,
                src.display()
            );
        }
        if let Err(e) = std::fs::create_dir_all(&stage_subdir) {
            tracing::error!(
                stage = %stage_subdir.display(),
                error = %e,
                "failed to create stage subdir"
            );
            return;
        }
        let dst = stage_subdir.join(name);
        // Per-transaction files published for this master segment
        // (MultiWriter devices, e.g. DuckDB): `<seq>.sqllog.<commit>.txn`.
        let txn_srcs: Vec<PathBuf> = parse_seq(name.to_string_lossy().as_ref())
            .and_then(|seq| {
                let parent = src.parent()?;
                let entries = std::fs::read_dir(parent).ok()?;
                Some(
                    entries
                        .flatten()
                        .filter(|e| is_txn_file_for_seq(seq, &e.file_name().to_string_lossy()))
                        .map(|e| e.path())
                        .collect(),
                )
            })
            .unwrap_or_default();
        // Java `LogShipper` parity: txn files reach stage ahead of their
        // master segment. Sources are deleted only after every copy
        // succeeded (txn files first, master last), so a crash at any point
        // leaves the master segment in the device home and the whole move is
        // redone on restart.
        let mut txn_dsts = Vec::with_capacity(txn_srcs.len());
        for txn_src in &txn_srcs {
            let txn_dst = stage_subdir.join(txn_src.file_name().unwrap_or_default());
            if let Err(e) = std::fs::copy(txn_src, &txn_dst) {
                tracing::error!(
                    src = %txn_src.display(),
                    dst = %txn_dst.display(),
                    error = %e,
                    "failed to copy txn file into stage"
                );
                return;
            }
            txn_dsts.push(txn_dst);
        }
        if let Err(e) = std::fs::copy(src, &dst) {
            tracing::error!(
                src = %src.display(),
                dst = %dst.display(),
                error = %e,
                "failed to copy segment into stage"
            );
            return;
        }
        for txn_src in &txn_srcs {
            if let Err(e) = std::fs::remove_file(txn_src) {
                tracing::warn!(
                    src = %txn_src.display(),
                    error = %e,
                    "failed to delete source txn file after move"
                );
            }
        }
        if let Err(e) = std::fs::remove_file(src) {
            tracing::warn!(
                src = %src.display(),
                error = %e,
                "failed to delete source segment after move"
            );
        }
        for txn_dst in &txn_dsts {
            if let Some(sh) = &shipper {
                if let Err(e) = sh.submit(txn_dst) {
                    tracing::error!(
                        segment = %txn_dst.display(),
                        error = %e,
                        "failed to enqueue txn file for shipping"
                    );
                }
            }
            if notify_consolidator_for_txn_files {
                for consolidator in &consolidators {
                    if let Err(e) = consolidator.notify_stage_path(txn_dst.clone()) {
                        tracing::error!(
                            segment = %txn_dst.display(),
                            error = %e,
                            "failed to notify consolidator about staged txn file"
                        );
                    }
                }
            }
        }
        if let Some(sh) = &shipper {
            if let Err(e) = sh.submit(&dst) {
                tracing::error!(
                    segment = %dst.display(),
                    error = %e,
                    "failed to enqueue segment for shipping"
                );
            }
        }
        for consolidator in &consolidators {
            if let Err(e) = consolidator.notify_stage_path(dst.clone()) {
                tracing::error!(
                    segment = %dst.display(),
                    error = %e,
                    "failed to notify consolidator about staged segment"
                );
            }
        }
    })
}

fn parse_seq(name: &str) -> Option<u64> {
    name.strip_suffix(".sqllog")
        .and_then(|n| n.parse::<u64>().ok())
}

fn is_txn_file_for_seq(seq: u64, name: &str) -> bool {
    let prefix = format!("{seq}.sqllog.");
    name.starts_with(&prefix) && name.ends_with(".txn")
}
