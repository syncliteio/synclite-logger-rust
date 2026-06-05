//! Integration test for shipper retry + cleaner: a flaky archiver causes
//! transient failures, the shipper retries, and after every archiver
//! ultimately accepts the segment the cleaner deletes the local file.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use logger_archiver::{Archiver, FsArchiver};
use logger_core::{Error, Result};
use logger_shipper::{LogCleaner, LogShipper, RetryPolicy, ShipperConfig};
use tempfile::tempdir;

struct FlakyFs {
    inner: FsArchiver,
    fail_first: usize,
    attempts: AtomicUsize,
    seen: Mutex<Vec<PathBuf>>,
}

impl FlakyFs {
    fn new(target: &Path, fail_first: usize) -> Self {
        Self {
            inner: FsArchiver::new(target),
            fail_first,
            attempts: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
        }
    }
}

impl Archiver for FlakyFs {
    fn name(&self) -> &str {
        "flaky-fs"
    }
    fn ship(&self, segment_path: &Path) -> Result<()> {
        let n = self.attempts.fetch_add(1, Ordering::SeqCst);
        if n < self.fail_first {
            return Err(Error::Internal("transient".into()));
        }
        self.seen.lock().unwrap().push(segment_path.to_path_buf());
        self.inner.ship(segment_path)
    }
}

#[test]
fn retries_then_cleans_local_segment() {
    let src = tempdir().unwrap();
    let dst = tempdir().unwrap();

    // Local "stage" segment that the shipper will be told about.
    let seg = src.path().join("0.sqllog");
    std::fs::write(&seg, b"fake-segment-bytes").unwrap();

    let flaky = Arc::new(FlakyFs::new(dst.path(), 2));
    let cfg = ShipperConfig {
        archivers: vec![flaky.clone() as Arc<dyn Archiver>],
        retry: RetryPolicy {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            multiplier: 1.0,
        },
        on_shipped: Some(LogCleaner::new().as_callback()),
        stage_dir: Some(src.path().to_path_buf()),
        scan_interval: Duration::from_millis(10),
    };

    let shipper = LogShipper::spawn_with(cfg).unwrap();
    shipper.submit(&seg).unwrap();
    shipper.shutdown().unwrap();

    // Two transient failures + one success.
    assert_eq!(flaky.attempts.load(Ordering::SeqCst), 3);
    assert_eq!(flaky.seen.lock().unwrap().len(), 1);

    // Destination got the file.
    let shipped = dst.path().join("0.sqllog");
    assert!(shipped.exists(), "shipped file missing");

    // Local stage segment was cleaned.
    assert!(!seg.exists(), "local segment was not cleaned");
}

#[test]
fn permanent_failure_does_not_clean_local_segment() {
    let src = tempdir().unwrap();
    let dst = tempdir().unwrap();

    let seg = src.path().join("0.sqllog");
    std::fs::write(&seg, b"fake").unwrap();

    // fail_first larger than max_attempts → never succeeds.
    let flaky = Arc::new(FlakyFs::new(dst.path(), 99));
    let cfg = ShipperConfig {
        archivers: vec![flaky.clone() as Arc<dyn Archiver>],
        retry: RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            multiplier: 1.0,
        },
        on_shipped: Some(LogCleaner::new().as_callback()),
        stage_dir: Some(src.path().to_path_buf()),
        scan_interval: Duration::from_millis(10),
    };

    let shipper = LogShipper::spawn_with(cfg).unwrap();
    shipper.submit(&seg).unwrap();
    shipper.shutdown().unwrap();

    assert_eq!(flaky.attempts.load(Ordering::SeqCst), 3);
    // The local segment must remain so the user can retry later.
    assert!(seg.exists(), "segment was wrongly cleaned despite failures");
}


