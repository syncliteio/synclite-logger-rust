//! Background log-segment shipper.
//!
//! Owns a worker thread that tracks the latest finalized segment sequence
//! and ships sequentially from the last shipped sequence up to that latest
//! sequence:
//!
//! - **Fan-out per segment:** every archiver gets every segment.
//! - **Bounded retry with exponential backoff:** transient failures are
//!   retried up to [`RetryPolicy::max_attempts`] times per archiver. After
//!   exhausting retries, the failure is logged and that archiver is
//!   skipped for the segment.
//! - **Catch-up on startup:** if a stage directory is configured, existing
//!   `<N>.sqllog` files are discovered and shipped without waiting for new
//!   notifications.
//! - **Shipped callback:** when *every* archiver has accepted a segment
//!   (no retries left over), an optional [`ShippedCallback`] fires. This
//!   is the hook a [`LogCleaner`] uses to delete fully shipped local
//!   segments.
//! - **Drain-on-drop:** dropping the [`LogShipper`] stops the worker after
//!   in-flight shipping work completes.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use synclite_archiver::Archiver;
use synclite_core::{Error, Result};

pub mod cleaner;
pub use cleaner::LogCleaner;

/// Callback fired exactly once per segment, after every archiver has
/// successfully accepted it. The argument is the local segment path.
pub type ShippedCallback = Arc<dyn Fn(&Path) + Send + Sync>;

/// Bounded exponential-backoff retry policy applied per archiver, per
/// segment. With defaults: 5 attempts, starting at 100 ms, doubling, capped
/// at 30 s.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total number of attempts including the first. Must be >= 1.
    pub max_attempts: u32,
    /// Initial backoff after attempt #1 fails.
    pub initial_backoff: Duration,
    /// Upper bound on any single backoff sleep.
    pub max_backoff: Duration,
    /// Multiplier applied to `initial_backoff` between attempts.
    pub multiplier: f64,
}

impl RetryPolicy {
    /// No-retry policy: a single attempt, no backoff.
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff: Duration::from_millis(0),
            max_backoff: Duration::from_millis(0),
            multiplier: 1.0,
        }
    }

    fn backoff_for(&self, attempt: u32) -> Duration {
        // attempt is 1-based; we sleep *after* attempt N fails to wait
        // before attempt N+1.
        let exp = (attempt as i32 - 1).max(0);
        let mut d = self.initial_backoff.as_secs_f64() * self.multiplier.powi(exp);
        let cap = self.max_backoff.as_secs_f64();
        if d > cap {
            d = cap;
        }
        Duration::from_secs_f64(d.max(0.0))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            multiplier: 2.0,
        }
    }
}

/// Configuration for a [`LogShipper`].
#[derive(Clone)]
pub struct ShipperConfig {
    /// Archivers to fan each segment out to. Must be non-empty.
    pub archivers: Vec<Arc<dyn Archiver>>,
    /// Retry policy applied per archiver, per segment.
    pub retry: RetryPolicy,
    /// Fired after a segment is fully shipped to every archiver.
    pub on_shipped: Option<ShippedCallback>,
    /// Optional stage directory containing `<N>.sqllog` files.
    pub stage_dir: Option<PathBuf>,
    /// Poll interval for catch-up scanning and shipping.
    pub scan_interval: Duration,
}

impl std::fmt::Debug for ShipperConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShipperConfig")
            .field("archivers", &self.archivers.len())
            .field("retry", &self.retry)
            .field("on_shipped", &self.on_shipped.is_some())
            .field("stage_dir", &self.stage_dir)
            .field("scan_interval", &self.scan_interval)
            .finish()
    }
}

impl ShipperConfig {
    /// Build a config with the given archivers and default retry/no callback.
    pub fn new(archivers: Vec<Arc<dyn Archiver>>) -> Self {
        Self {
            archivers,
            retry: RetryPolicy::default(),
            on_shipped: None,
            stage_dir: None,
            scan_interval: Duration::from_millis(1000),
        }
    }
}

#[derive(Debug)]
enum Msg {
    Observe(PathBuf),
    Shutdown,
}

/// Background shipper. Drop to stop the worker (after it drains the queue).
#[derive(Debug)]
pub struct LogShipper {
    tx: Sender<Msg>,
    join: Option<JoinHandle<()>>,
}

impl LogShipper {
    /// Spawn a shipper with one or more archivers, using the default retry
    /// policy and no shipped callback. Returns an error if `archivers` is
    /// empty.
    pub fn spawn(archivers: Vec<Arc<dyn Archiver>>) -> Result<Self> {
        Self::spawn_with(ShipperConfig::new(archivers))
    }

    /// Spawn a shipper with full configuration.
    pub fn spawn_with(cfg: ShipperConfig) -> Result<Self> {
        if cfg.archivers.is_empty() {
            return Err(Error::Internal("LogShipper requires >= 1 archiver".into()));
        }
        if cfg.retry.max_attempts == 0 {
            return Err(Error::Internal(
                "RetryPolicy.max_attempts must be >= 1".into(),
            ));
        }
        let (tx, rx) = mpsc::channel::<Msg>();
        let join = thread::Builder::new()
            .name("synclite-shipper".into())
            .spawn(move || worker_loop(rx, cfg))
            .map_err(|e| Error::Internal(format!("spawn shipper thread: {e}")))?;
        Ok(Self {
            tx,
            join: Some(join),
        })
    }

    /// Inform the shipper about a finalized segment path.
    ///
    /// This updates the shipper's notion of "latest"
    /// segment; the worker then ships sequentially from its last shipped
    /// sequence up to the latest known sequence.
    pub fn submit<P: AsRef<Path>>(&self, segment_path: P) -> Result<()> {
        self.tx
            .send(Msg::Observe(segment_path.as_ref().to_path_buf()))
            .map_err(|_| Error::Internal("shipper worker has exited".into()))
    }

    /// Stop the worker and wait for it to drain. Called automatically on
    /// drop; exposed so callers can observe the result deterministically.
    pub fn shutdown(mut self) -> Result<()> {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> Result<()> {
        if let Some(join) = self.join.take() {
            let _ = self.tx.send(Msg::Shutdown);
            join.join()
                .map_err(|_| Error::Internal("shipper worker panicked".into()))?;
        }
        Ok(())
    }
}

impl Drop for LogShipper {
    fn drop(&mut self) {
        let _ = self.shutdown_inner();
    }
}

fn worker_loop(rx: mpsc::Receiver<Msg>, cfg: ShipperConfig) {
    let mut stage_dir = cfg.stage_dir.clone();
    let mut latest_seq = stage_dir
        .as_deref()
        .and_then(scan_max_seq)
        .unwrap_or(0);
    let mut shipped_seq: i64 = -1;

    loop {
        match rx.recv_timeout(cfg.scan_interval) {
            Ok(Msg::Shutdown) => break,
            Ok(Msg::Observe(path)) => {
                if stage_dir.is_none() {
                    stage_dir = path.parent().map(|p| p.to_path_buf());
                }
                if let Some(seq) = parse_seq(path.file_name().and_then(|s| s.to_str())) {
                    latest_seq = latest_seq.max(seq);
                } else {
                    ship_one(&cfg, &path);
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                // Periodic catch-up tick.
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }

        let Some(dir) = stage_dir.as_deref() else {
            continue;
        };
        if let Some(max_seq) = scan_max_seq(dir) {
            latest_seq = latest_seq.max(max_seq);
        }

        let mut next = shipped_seq + 1;
        while (next as u64) <= latest_seq {
            let segment = dir.join(format!("{next}.sqllog"));
            if !segment.exists() {
                // Segment already cleaned or missing; don't get stuck.
                shipped_seq = next;
                next += 1;
                continue;
            }
            if !ship_one(&cfg, &segment) {
                break;
            }
            shipped_seq = next;
            next += 1;
        }
    }
}

fn ship_one(cfg: &ShipperConfig, path: &Path) -> bool {
    let mut all_ok = true;
    for archiver in &cfg.archivers {
        if !ship_with_retry(archiver.as_ref(), path, &cfg.retry) {
            all_ok = false;
            break;
        }
    }
    if all_ok {
        if let Some(cb) = &cfg.on_shipped {
            cb(path);
        }
    }
    all_ok
}

fn parse_seq(name: Option<&str>) -> Option<u64> {
    name.and_then(|n| n.strip_suffix(".sqllog"))
        .and_then(|n| n.parse::<u64>().ok())
}

fn scan_max_seq(dir: &Path) -> Option<u64> {
    let mut best: Option<u64> = None;
    let entries = std::fs::read_dir(dir).ok()?;
    for entry in entries.flatten() {
        let seq = parse_seq(entry.file_name().to_str());
        if let Some(seq) = seq {
            best = Some(best.map_or(seq, |b| b.max(seq)));
        }
    }
    best
}

/// Returns `true` if `archiver` accepted `path` within the retry budget.
fn ship_with_retry(archiver: &dyn Archiver, path: &Path, retry: &RetryPolicy) -> bool {
    for attempt in 1..=retry.max_attempts {
        match archiver.ship(path) {
            Ok(()) => return true,
            Err(e) => {
                if attempt == retry.max_attempts {
                    tracing::error!(
                        archiver = archiver.name(),
                        segment = %path.display(),
                        attempt,
                        error = %e,
                        "giving up on segment after exhausting retries"
                    );
                    return false;
                }
                let sleep = retry.backoff_for(attempt);
                tracing::warn!(
                    archiver = archiver.name(),
                    segment = %path.display(),
                    attempt,
                    next_backoff_ms = sleep.as_millis() as u64,
                    error = %e,
                    "ship failed; will retry"
                );
                if !sleep.is_zero() {
                    thread::sleep(sleep);
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use synclite_archiver::FsArchiver;
    use tempfile::tempdir;

    #[test]
    fn ships_submitted_segments() {
        let src = tempdir().unwrap();
        let dst = tempdir().unwrap();
        let s1 = src.path().join("0.sqllog");
        let s2 = src.path().join("1.sqllog");
        fs::write(&s1, b"first").unwrap();
        fs::write(&s2, b"second").unwrap();

        let archiver: Arc<dyn Archiver> = Arc::new(FsArchiver::new(dst.path()));
        let mut cfg = ShipperConfig::new(vec![archiver]);
        cfg.stage_dir = Some(src.path().to_path_buf());
        cfg.scan_interval = Duration::from_millis(10);
        let shipper = LogShipper::spawn_with(cfg).unwrap();
        shipper.submit(&s1).unwrap();
        shipper.submit(&s2).unwrap();
        shipper.shutdown().unwrap();

        assert_eq!(fs::read(dst.path().join("0.sqllog")).unwrap(), b"first");
        assert_eq!(fs::read(dst.path().join("1.sqllog")).unwrap(), b"second");
    }

    #[test]
    fn rejects_empty_archiver_list() {
        let err = LogShipper::spawn(vec![]).unwrap_err();
        assert!(matches!(err, Error::Internal(_)));
    }

    /// Archiver that fails the first `fail_first` calls and then succeeds.
    struct FlakyArchiver {
        attempts: AtomicUsize,
        fail_first: usize,
        seen: Mutex<Vec<PathBuf>>,
    }

    impl Archiver for FlakyArchiver {
        fn name(&self) -> &str {
            "flaky"
        }
        fn ship(&self, segment_path: &Path) -> Result<()> {
            let n = self.attempts.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_first {
                return Err(Error::Internal("simulated transient failure".into()));
            }
            self.seen.lock().unwrap().push(segment_path.to_path_buf());
            Ok(())
        }
    }

    #[test]
    fn retries_transient_failures_and_succeeds() {
        let src = tempdir().unwrap();
        let p = src.path().join("0.sqllog");
        fs::write(&p, b"x").unwrap();

        let flaky = Arc::new(FlakyArchiver {
            attempts: AtomicUsize::new(0),
            fail_first: 2,
            seen: Mutex::new(Vec::new()),
        });

        let shipped_count = Arc::new(AtomicUsize::new(0));
        let counter = shipped_count.clone();

        let cfg = ShipperConfig {
            archivers: vec![flaky.clone() as Arc<dyn Archiver>],
            retry: RetryPolicy {
                max_attempts: 3,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
                multiplier: 1.0,
            },
            on_shipped: Some(Arc::new(move |_p: &Path| {
                counter.fetch_add(1, Ordering::SeqCst);
            })),
            stage_dir: Some(src.path().to_path_buf()),
            scan_interval: Duration::from_millis(10),
        };

        let shipper = LogShipper::spawn_with(cfg).unwrap();
        shipper.submit(&p).unwrap();
        shipper.shutdown().unwrap();

        assert_eq!(flaky.attempts.load(Ordering::SeqCst), 3);
        assert_eq!(flaky.seen.lock().unwrap().len(), 1);
        assert_eq!(shipped_count.load(Ordering::SeqCst), 1);
    }

    /// Always-failing archiver.
    struct BrokenArchiver(AtomicUsize);
    impl Archiver for BrokenArchiver {
        fn name(&self) -> &str {
            "broken"
        }
        fn ship(&self, _segment_path: &Path) -> Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(Error::Internal("permanent failure".into()))
        }
    }

    #[test]
    fn gives_up_after_max_attempts_and_skips_callback() {
        let src = tempdir().unwrap();
        let p = src.path().join("0.sqllog");
        fs::write(&p, b"x").unwrap();

        let broken = Arc::new(BrokenArchiver(AtomicUsize::new(0)));
        let shipped_count = Arc::new(AtomicUsize::new(0));
        let counter = shipped_count.clone();

        let cfg = ShipperConfig {
            archivers: vec![broken.clone() as Arc<dyn Archiver>],
            retry: RetryPolicy {
                max_attempts: 3,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
                multiplier: 1.0,
            },
            on_shipped: Some(Arc::new(move |_p: &Path| {
                counter.fetch_add(1, Ordering::SeqCst);
            })),
            stage_dir: Some(src.path().to_path_buf()),
            scan_interval: Duration::from_millis(10),
        };

        let shipper = LogShipper::spawn_with(cfg).unwrap();
        shipper.submit(&p).unwrap();
        shipper.shutdown().unwrap();

        assert_eq!(broken.0.load(Ordering::SeqCst), 3);
        assert_eq!(shipped_count.load(Ordering::SeqCst), 0);
    }
}
