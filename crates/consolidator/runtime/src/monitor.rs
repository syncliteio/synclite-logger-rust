//! Java parity: `com.synclite.consolidator.watchdog.Monitor` + its inner
//! `PrometheusDumper`.
//!
//! Holds a process-global singleton that owns 14 Prometheus gauges (exact
//! Java metric names and help strings) plus the atomic counters used by
//! per-device workers to increment them. A background pusher thread sends
//! the registry to the configured push-gateway every
//! `prometheus-statistics-publisher-interval-s` seconds, with job name
//! `SyncLiteConsolidator` -- matching `PrometheusDumper.dump()` exactly.
//!
//! Activation: `start_prometheus_publisher` is called once from the synclite
//! facade after parsing `enable-prometheus-statistics-publisher` +
//! `prometheus-push-gateway-url` from the config. If the publisher is
//! disabled, the counters still increment harmlessly so workers do not
//! need to branch.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prometheus::{labels, Encoder, Gauge, Registry, TextEncoder};

/// Process-global singleton. Created lazily on first access so all worker
/// threads share the same gauges + counters regardless of init order.
static MONITOR: OnceLock<Arc<Monitor>> = OnceLock::new();
static PUBLISHER_STARTED: AtomicBool = AtomicBool::new(false);
static JOB_START_TIME_MS: OnceLock<i64> = OnceLock::new();

/// Java parity: `Monitor` + `PrometheusDumper` state.
pub struct Monitor {
    registry: Registry,

    // Java fields (`AtomicLong`) -- workers increment these.
    detected_device_cnt: AtomicI64,
    registered_device_cnt: AtomicI64,
    initialized_device_cnt: AtomicI64,
    failed_device_cnt: AtomicI64,
    total_consolidated_table_cnt: AtomicI64,
    total_cdc_log_segment_cnt: AtomicI64,
    total_processed_oper_count: AtomicI64,
    total_dst_txn_cnt: AtomicI64,
    total_processed_log_size: AtomicI64,
    total_initialization_cnt: AtomicI64,
    total_resynchronization_cnt: AtomicI64,
    latency_ms: AtomicI64,

    // Java `Gauge` instances (one per metric).
    g_detected: Gauge,
    g_registered: Gauge,
    g_initialized: Gauge,
    g_failed: Gauge,
    g_consolidated_tables: Gauge,
    g_log_segments: Gauge,
    g_processed_opers: Gauge,
    g_processed_txns: Gauge,
    g_processed_log_size: Gauge,
    g_initializations: Gauge,
    g_resynchronizations: Gauge,
    g_latency: Gauge,
    g_last_heartbeat_time: Gauge,
    g_last_job_start_time: Gauge,
}

impl Monitor {
    fn new() -> Arc<Self> {
        let registry = Registry::new();

        // Java PrometheusDumper.init(): exact name + help strings preserved.
        let g_detected = make_gauge(&registry, "Detected_Devices", "Detected Devices");
        let g_registered = make_gauge(&registry, "Registered_Devices", "Registered Devices");
        let g_initialized = make_gauge(&registry, "Initialized_Devices", "Initialized Devices");
        let g_failed = make_gauge(&registry, "Failed_Devices", "Failed Devices");
        let g_consolidated_tables = make_gauge(
            &registry,
            "Total_Consolidated_Tables",
            "Total_Consolidated_Tables",
        );
        let g_log_segments = make_gauge(
            &registry,
            "Total_Log_Segments_Applied",
            "Total_Log_Segments_Applied",
        );
        let g_processed_opers = make_gauge(
            &registry,
            "Total_Processed_Operation_Count",
            "Total_Processed_Operation_Count",
        );
        let g_processed_txns = make_gauge(
            &registry,
            "Total_Processed_Transaction_Count",
            "Total_Processed_Transaction_Count",
        );
        let g_processed_log_size = make_gauge(
            &registry,
            "Total_Processed_Log_Size",
            "Total_Processed_Log_Size",
        );
        let g_initializations = make_gauge(
            &registry,
            "Total_Device_Initializations",
            "Total_Device_Initializations",
        );
        let g_resynchronizations = make_gauge(
            &registry,
            "Total_Device_Resynchronizations",
            "Total_Device_Resynchronizations",
        );
        let g_latency = make_gauge(&registry, "Latency", "Latency");
        // Java spells "Hearthbeat_Time" in the help string for
        // Job_Last_Heartbeat_Time; mirror it byte-for-byte.
        let g_last_heartbeat_time =
            make_gauge(&registry, "Job_Last_Heartbeat_Time", "Job_Last_Hearthbeat_Time");
        let g_last_job_start_time =
            make_gauge(&registry, "Job_Last_Start_Time", "Job_Last_Start_Time");

        // Capture process start time (Java Main.jobStartTime).
        let _ = JOB_START_TIME_MS.set(now_millis());

        Arc::new(Self {
            registry,
            detected_device_cnt: AtomicI64::new(0),
            registered_device_cnt: AtomicI64::new(0),
            initialized_device_cnt: AtomicI64::new(0),
            failed_device_cnt: AtomicI64::new(0),
            total_consolidated_table_cnt: AtomicI64::new(0),
            total_cdc_log_segment_cnt: AtomicI64::new(0),
            total_processed_oper_count: AtomicI64::new(0),
            total_dst_txn_cnt: AtomicI64::new(0),
            total_processed_log_size: AtomicI64::new(0),
            total_initialization_cnt: AtomicI64::new(0),
            total_resynchronization_cnt: AtomicI64::new(0),
            latency_ms: AtomicI64::new(0),

            g_detected,
            g_registered,
            g_initialized,
            g_failed,
            g_consolidated_tables,
            g_log_segments,
            g_processed_opers,
            g_processed_txns,
            g_processed_log_size,
            g_initializations,
            g_resynchronizations,
            g_latency,
            g_last_heartbeat_time,
            g_last_job_start_time,
        })
    }

    // ----- Java setters / incrementers ---------------------------------

    pub fn set_detected_device_cnt(&self, v: i64) {
        self.detected_device_cnt.store(v, Ordering::Relaxed);
    }
    pub fn set_registered_device_cnt(&self, v: i64) {
        self.registered_device_cnt.store(v, Ordering::Relaxed);
    }
    pub fn incr_registered_device_cnt(&self, v: i64) {
        self.registered_device_cnt.fetch_add(v, Ordering::Relaxed);
    }
    pub fn set_initialized_device_cnt(&self, v: i64) {
        self.initialized_device_cnt.store(v, Ordering::Relaxed);
    }
    pub fn incr_initialized_device_cnt(&self, v: i64) {
        self.initialized_device_cnt.fetch_add(v, Ordering::Relaxed);
    }
    pub fn set_failed_device_cnt(&self, v: i64) {
        self.failed_device_cnt.store(v, Ordering::Relaxed);
    }
    pub fn set_total_consolidated_table_cnt(&self, v: i64) {
        self.total_consolidated_table_cnt.store(v, Ordering::Relaxed);
    }
    pub fn incr_total_cdc_log_segment_cnt(&self, v: i64) {
        self.total_cdc_log_segment_cnt.fetch_add(v, Ordering::Relaxed);
    }
    pub fn incr_total_processed_oper_count(&self, v: i64) {
        self.total_processed_oper_count.fetch_add(v, Ordering::Relaxed);
    }
    pub fn incr_total_dst_txn_cnt(&self, v: i64) {
        self.total_dst_txn_cnt.fetch_add(v, Ordering::Relaxed);
    }
    pub fn incr_total_processed_log_size(&self, v: i64) {
        self.total_processed_log_size.fetch_add(v, Ordering::Relaxed);
    }
    pub fn incr_initialization_cnt(&self, v: i64) {
        self.total_initialization_cnt.fetch_add(v, Ordering::Relaxed);
    }
    pub fn incr_resynchronization_cnt(&self, v: i64) {
        self.total_resynchronization_cnt.fetch_add(v, Ordering::Relaxed);
    }
    pub fn set_latency_ms(&self, v: i64) {
        self.latency_ms.store(v, Ordering::Relaxed);
    }

    // ----- Java PrometheusDumper.dump() --------------------------------

    /// Copy counters into gauges. Mirrors `PrometheusDumper.dump()` lines
    /// 148-167 (without the network push, which is the caller's job).
    fn refresh_gauges(&self) {
        self.g_detected.set(self.detected_device_cnt.load(Ordering::Relaxed) as f64);
        self.g_registered.set(self.registered_device_cnt.load(Ordering::Relaxed) as f64);
        self.g_initialized.set(self.initialized_device_cnt.load(Ordering::Relaxed) as f64);
        self.g_failed.set(self.failed_device_cnt.load(Ordering::Relaxed) as f64);
        self.g_consolidated_tables
            .set(self.total_consolidated_table_cnt.load(Ordering::Relaxed) as f64);
        self.g_log_segments
            .set(self.total_cdc_log_segment_cnt.load(Ordering::Relaxed) as f64);
        self.g_processed_opers
            .set(self.total_processed_oper_count.load(Ordering::Relaxed) as f64);
        self.g_processed_txns.set(self.total_dst_txn_cnt.load(Ordering::Relaxed) as f64);
        self.g_processed_log_size
            .set(self.total_processed_log_size.load(Ordering::Relaxed) as f64);
        self.g_initializations
            .set(self.total_initialization_cnt.load(Ordering::Relaxed) as f64);
        self.g_resynchronizations
            .set(self.total_resynchronization_cnt.load(Ordering::Relaxed) as f64);
        self.g_latency.set(self.latency_ms.load(Ordering::Relaxed) as f64);
        self.g_last_heartbeat_time.set(now_millis() as f64);
        self.g_last_job_start_time
            .set(JOB_START_TIME_MS.get().copied().unwrap_or(0) as f64);
    }

    /// Java parity: `PrometheusDumper.dump()` network push. Pushes the full
    /// registry to `<host>:<port>` under job `SyncLiteConsolidator` via
    /// HTTP POST (PushGateway `pushAdd` semantics).
    fn push(&self, gateway_url: &str) -> Result<(), String> {
        self.refresh_gauges();
        let (host_port, scheme) = parse_gateway_host(gateway_url)?;
        let endpoint = format!("{scheme}://{host_port}/metrics/job/SyncLiteConsolidator");
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder
            .encode(&metric_families, &mut buf)
            .map_err(|e| format!("encode failed: {e}"))?;

        // Minimal HTTP POST without pulling in a heavy client. Java's
        // PushGateway.pushAdd issues PUT (replace) by default but the
        // `prometheus` Rust crate's pusher uses POST -- both are accepted
        // by the pushgateway. We use POST for parity with the Rust pusher.
        send_http_post(&endpoint, &buf, &labels! {})
    }
}

/// Get (or lazily create) the global Monitor. Workers should call this and
/// then invoke incrementer methods. Cheap after first call.
pub fn monitor() -> Arc<Monitor> {
    MONITOR.get_or_init(Monitor::new).clone()
}

/// Java parity: start the PrometheusDumper background thread. Idempotent --
/// only the first call wins, mirroring the `INSTANCE` singleton check on the
/// Java side. `interval_secs` defaults to 60 when 0 is passed (Java default).
pub fn start_prometheus_publisher(gateway_url: String, interval_secs: u64) {
    if PUBLISHER_STARTED.swap(true, Ordering::SeqCst) {
        return;
    }
    let interval = Duration::from_secs(if interval_secs == 0 { 60 } else { interval_secs });
    let m = monitor();
    thread::Builder::new()
        .name("synclite-prometheus".to_string())
        .spawn(move || loop {
            if let Err(e) = m.push(&gateway_url) {
                tracing::warn!("prometheus push failed: {e}");
            }
            thread::sleep(interval);
        })
        .expect("spawn prometheus pusher");
}

// ----- helpers -------------------------------------------------------

fn make_gauge(registry: &Registry, name: &str, help: &str) -> Gauge {
    let g = Gauge::new(name, help).expect("valid gauge");
    registry.register(Box::new(g.clone())).expect("register gauge");
    g
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Parse `http://host:port[/...]` or `host:port` into `(host_port, scheme)`.
fn parse_gateway_host(url: &str) -> Result<(String, &'static str), String> {
    let (scheme, rest) = if let Some(r) = url.strip_prefix("https://") {
        ("https", r)
    } else if let Some(r) = url.strip_prefix("http://") {
        ("http", r)
    } else {
        ("http", url)
    };
    let host_port = rest.split('/').next().unwrap_or(rest).to_string();
    if host_port.is_empty() {
        return Err(format!("invalid prometheus-push-gateway-url: {url}"));
    }
    Ok((host_port, scheme))
}

/// Minimal HTTP POST via `std::net::TcpStream`. Avoids pulling in a full
/// HTTP client (reqwest/hyper) for what is essentially a fire-and-forget
/// metrics push.
fn send_http_post(
    endpoint: &str,
    body: &[u8],
    _labels: &std::collections::HashMap<&str, &str>,
) -> Result<(), String> {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let (scheme, rest) = if let Some(r) = endpoint.strip_prefix("https://") {
        ("https", r)
    } else if let Some(r) = endpoint.strip_prefix("http://") {
        ("http", r)
    } else {
        return Err(format!("unsupported scheme in endpoint: {endpoint}"));
    };
    if scheme == "https" {
        return Err("https push-gateway not supported in this build".into());
    }

    let (host_port, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let mut iter = host_port.rsplitn(2, ':');
    let port = iter.next().and_then(|s| s.parse::<u16>().ok()).unwrap_or(9091);
    let host = iter.next().unwrap_or(host_port);

    let mut stream =
        TcpStream::connect((host, port)).map_err(|e| format!("connect {host}:{port}: {e}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| e.to_string())?;

    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("write headers: {e}"))?;
    stream.write_all(body).map_err(|e| format!("write body: {e}"))?;

    let mut resp = String::new();
    stream
        .read_to_string(&mut resp)
        .map_err(|e| format!("read response: {e}"))?;
    // First line: "HTTP/1.1 200 OK"
    let status_ok = resp
        .lines()
        .next()
        .map(|l| l.contains(" 200 ") || l.contains(" 202 "))
        .unwrap_or(false);
    if !status_ok {
        return Err(format!("push-gateway rejected: {}", resp.lines().next().unwrap_or("")));
    }
    Ok(())
}
