//! Sync status / latency / statistics inspection APIs.
//!
//! These are read-only helpers a host application can call at any
//! time to inspect what the in-process consolidator is doing for a
//! given device. They do not start workers or retain connections.
//! Progress is read from each configured metadata store: LOCAL reads
//! consolidator SQLite files; DESTINATION contacts the destination DB.
//!
//! - [`sync_status`] returns the device's current run state
//!   (`Running` / `Paused` / `NotInitialized`).
//! - [`sync_statistics`] returns counters maintained by the
//!   consolidator (segments applied, ops, txns, bytes, last commit
//!   id, last heartbeat).
//! - [`sync_latency`] returns `latency_ms = source − applied` where
//!   both sides are `System.currentTimeMillis()`-style commit ids
//!   the logger emits. If the destination side hasn't been seen
//!   (consolidator hasn't started, destination unreachable, etc.)
//!   `applied_commit_id` is `None` and `latency_ms` is `-1`.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use duckdb::{
    params_from_iter as duck_params_from_iter, types::Value as DuckValue,
    Connection as DuckConnection,
};
use logger_core::{Error, Result};
use postgres::{Config as PgConfig, NoTls};
use rusqlite::{Connection, OpenFlags};

use crate::layout::DeviceLayout;
use crate::metadata::Metadata;
use crate::pause;
use crate::{default_device_data_root, normalize_db_path, SyncLiteConfig};
use consolidator_core::{ConsolidatorLayout, DstType, MetadataStore};

/// Run state of a device's sync pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Device has never been initialized (no `.synclite` metadata).
    NotInitialized,
    /// `pause_sync` has been called and no `resume_sync` since.
    Paused,
    /// Default — consolidator is processing segments as they arrive.
    Running,
}

/// Snapshot of the consolidator's run state and last-heartbeat row.
#[derive(Debug, Clone)]
pub struct SyncStatus {
    /// Derived run state — `NotInitialized` / `Paused` / `Running`.
    pub state: SyncState,
    /// Raw `status` string from the consolidator's `device_status`
    /// row (e.g. `"SYNCING"`). Empty when the consolidator has not
    /// yet written any heartbeat.
    pub status: String,
    /// Raw `status_description` string.
    pub status_description: String,
    /// `device_status.last_heartbeat_time` (epoch ms). 0 if absent.
    pub last_heartbeat_time_ms: i64,
}

/// Snapshot of consolidator counters for a device.
#[derive(Debug, Clone, Default)]
pub struct SyncStatistics {
    /// Number of log segments that have been applied to the
    /// destination so far.
    pub log_segments_applied: i64,
    /// Total operations applied (insert + update + delete rows etc.).
    pub processed_oper_count: i64,
    /// Total transactions applied.
    pub processed_txn_count: i64,
    /// Total log-segment bytes processed.
    pub processed_log_size: i64,
    /// Last commit id applied at the destination.
    pub last_consolidated_commit_id: i64,
    /// Epoch ms of the consolidator's last heartbeat update.
    pub last_heartbeat_time_ms: i64,
}

/// Snapshot of sync lag between the device and the destination.
#[derive(Debug, Clone)]
pub struct SyncLatency {
    /// `MAX(commit_id)` from the device's `synclite_txn` table.
    /// 0 if no user writes have committed yet.
    pub source_commit_id: i64,
    /// Minimum commit id applied across all configured destinations.
    /// `None` if any checkpoint is missing or unreadable (including an
    /// unreachable destination using DESTINATION metadata).
    pub applied_commit_id: Option<i64>,
    /// `source_commit_id − applied_commit_id`. Because both sides
    /// are wall-clock millisecond timestamps, this is the wall-clock
    /// sync lag in milliseconds. `-1` when `applied_commit_id` is
    /// unknown. Clamped at `0` (a negative diff is treated as
    /// caught-up).
    pub latency_ms: i64,
}

/// Return the device's current sync run state.
pub fn sync_status<P: AsRef<Path>>(db_path: P) -> Result<SyncStatus> {
    let normalized = normalize_db_path(db_path.as_ref())?;
    let layout = DeviceLayout::new(normalized);
    if !layout.metadata_path.exists() {
        return Ok(SyncStatus {
            state: SyncState::NotInitialized,
            status: String::new(),
            status_description: String::new(),
            last_heartbeat_time_ms: 0,
        });
    }
    let paused = pause::pause_sentinel_path(&layout.device_home).exists();
    let state = if paused {
        SyncState::Paused
    } else {
        SyncState::Running
    };

    let (status, status_description, last_heartbeat_time_ms) =
        read_device_status_row(&layout).unwrap_or_default();

    Ok(SyncStatus {
        state,
        status,
        status_description,
        last_heartbeat_time_ms,
    })
}

/// Return per-device consolidator counters.
pub fn sync_statistics<P: AsRef<Path>>(db_path: P) -> Result<SyncStatistics> {
    let normalized = normalize_db_path(db_path.as_ref())?;
    let layout = DeviceLayout::new(normalized);
    if !layout.metadata_path.exists() {
        return Err(Error::Config(format!(
            "sync_statistics: device metadata not found at {}",
            layout.metadata_path.display()
        )));
    }
    Ok(read_device_status_stats(&layout).unwrap_or_default())
}

/// Return wall-clock sync lag in milliseconds between the device's
/// last committed write and the consolidator's last applied commit.
pub fn sync_latency<P: AsRef<Path>>(db_path: P) -> Result<SyncLatency> {
    let normalized = normalize_db_path(db_path.as_ref())?;
    let layout = DeviceLayout::new(normalized.clone());
    if !layout.metadata_path.exists() {
        return Err(Error::Config(format!(
            "sync_latency: device metadata not found at {}",
            layout.metadata_path.display()
        )));
    }
    let source_commit_id = read_source_commit_id(&normalized).unwrap_or(0);
    let applied_commit_id = read_applied_commit_id(&layout).filter(|v| *v > 0);
    let latency_ms = match applied_commit_id {
        Some(applied) => (source_commit_id - applied).max(0),
        None => -1,
    };
    Ok(SyncLatency {
        source_commit_id,
        applied_commit_id,
        latency_ms,
    })
}

fn consolidator_stats_db_path(layout: &DeviceLayout) -> PathBuf {
    // Mirror consolidator::ConsolidatorLayout::new path selection.
    // Prefer the global default work-dir (Java-parity) since that's
    // what `initialize()` uses unless the caller overrides it.
    // Fall back to the device-home-local `synclite-consolidator/`
    // directory the legacy embedded layout may have used.
    let global = default_device_data_root().join("synclite_consolidator_statistics.db");
    if global.exists() {
        return global;
    }
    let legacy = layout.device_home.join("synclite-syncer");
    let work_dir = if legacy.exists() {
        legacy
    } else {
        layout.device_home.join("synclite-consolidator")
    };
    work_dir.join("synclite_consolidator_statistics.db")
}

fn read_device_id_name(layout: &DeviceLayout) -> Result<(String, String)> {
    let md = Metadata::open_or_create(&layout.metadata_path)?;
    let uuid = md
        .get("uuid")?
        .ok_or_else(|| Error::Config("device uuid missing from metadata".to_string()))?;
    let name = md.get("device_name")?.unwrap_or_default();
    Ok((uuid, name))
}

fn open_ro(path: &Path) -> Option<Connection> {
    open_ro_with_timeout(path, None)
}

fn open_ro_with_timeout(path: &Path, timeout: Option<Duration>) -> Option<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .ok()?;
    if let Some(timeout) = timeout {
        // A zero-duration await still performs one immediate checkpoint read.
        // Keep SQLite lock waits bounded without turning that probe off.
        conn.busy_timeout(timeout.max(Duration::from_millis(1)))
            .ok()?;
    }
    Some(conn)
}

fn read_device_status_row(layout: &DeviceLayout) -> Option<(String, String, i64)> {
    let (uuid, name) = read_device_id_name(layout).ok()?;
    let stats_db = consolidator_stats_db_path(layout);
    if !stats_db.exists() {
        return None;
    }
    let conn = open_ro(&stats_db)?;
    conn.query_row(
        "SELECT status, status_description, last_heartbeat_time \
         FROM device_status WHERE synclite_device_id = ?1 AND synclite_device_name = ?2",
        rusqlite::params![uuid, name],
        |row| {
            Ok((
                row.get::<_, String>(0).unwrap_or_default(),
                row.get::<_, String>(1).unwrap_or_default(),
                row.get::<_, i64>(2).unwrap_or(0),
            ))
        },
    )
    .ok()
}

fn read_device_status_stats(layout: &DeviceLayout) -> Option<SyncStatistics> {
    let (uuid, name) = read_device_id_name(layout).ok()?;
    let stats_db = consolidator_stats_db_path(layout);
    if !stats_db.exists() {
        return None;
    }
    let conn = open_ro(&stats_db)?;
    conn.query_row(
        "SELECT log_segments_applied, processed_oper_count, processed_txn_count, \
                processed_log_size, last_consolidated_commit_id, last_heartbeat_time \
         FROM device_status WHERE synclite_device_id = ?1 AND synclite_device_name = ?2",
        rusqlite::params![uuid, name],
        |row| {
            Ok(SyncStatistics {
                log_segments_applied: row.get::<_, i64>(0).unwrap_or(0),
                processed_oper_count: row.get::<_, i64>(1).unwrap_or(0),
                processed_txn_count: row.get::<_, i64>(2).unwrap_or(0),
                processed_log_size: row.get::<_, i64>(3).unwrap_or(0),
                last_consolidated_commit_id: row.get::<_, i64>(4).unwrap_or(0),
                last_heartbeat_time_ms: row.get::<_, i64>(5).unwrap_or(0),
            })
        },
    )
    .ok()
}

pub(crate) fn read_source_commit_id(db_path: &Path) -> Option<i64> {
    let conn = open_ro(db_path)?;
    let v: rusqlite::Result<Option<i64>> =
        conn.query_row("SELECT MAX(commit_id) FROM synclite_txn", [], |row| {
            row.get::<_, Option<i64>>(0)
        });
    match v {
        Ok(Some(v)) => Some(v),
        _ => Some(0),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataStoreMode {
    Local,
    Destination,
}

fn persisted_config(layout: &DeviceLayout) -> Result<SyncLiteConfig> {
    let mut cfg = SyncLiteConfig::default();
    cfg.extra = crate::read_persisted_initialize_extra(&layout.db_path)?;
    cfg.device_type = crate::persisted_device_type(&layout.db_path);
    Ok(cfg)
}

pub(crate) fn sync_configured(layout: &DeviceLayout) -> Result<bool> {
    let cfg = persisted_config(layout)?;
    let md = Metadata::open_or_create(&layout.metadata_path)?;
    // Older metadata can have indexed configuration without the marker.
    // Conversely, retain a true marker when old persistence omitted keys:
    // missing progress must time out, not masquerade as an unconfigured device.
    Ok(crate::has_any_destination_config(&cfg) || md.get_i64("sync_configured")?.unwrap_or(0) != 0)
}

fn metadata_store(cfg: &SyncLiteConfig, dst_index: usize) -> MetadataStoreMode {
    // Match Logger::open_with, including the legacy "false" alias and
    // unsuffixed fallback for index 1 only.
    match crate::parse_cfg_string_for_index(
        cfg,
        &["metadata-store", "dst-metadata-store"],
        dst_index,
    ) {
        Some(v) if v.eq_ignore_ascii_case("local") || v.eq_ignore_ascii_case("false") => {
            MetadataStoreMode::Local
        }
        _ => MetadataStoreMode::Destination,
    }
}

fn quote_pg_ident(raw: &str) -> String {
    format!("\"{}\"", raw.replace('"', "\"\""))
}

fn destination_alias(cfg: &SyncLiteConfig, dst_index: usize) -> String {
    crate::parse_cfg_string_for_index(cfg, &["dst-alias"], dst_index)
        .unwrap_or_else(|| format!("DB-{dst_index}"))
}

fn device_data_root(cfg: &SyncLiteConfig) -> PathBuf {
    cfg.extra
        .get(crate::DEVICE_DATA_ROOT_KEY)
        .map(PathBuf::from)
        .unwrap_or_else(default_device_data_root)
}

fn local_checkpoint_db_candidates(
    layout: &DeviceLayout,
    device_id: &str,
    device_name: &str,
    cfg: &SyncLiteConfig,
    dst_index: usize,
    multi_destination: bool,
) -> Vec<PathBuf> {
    let root = device_data_root(cfg);
    let work_dir = if multi_destination {
        root.join(destination_alias(cfg, dst_index))
    } else {
        root
    };
    let device_dir_name = format!("synclite-{}-{}", device_name, device_id);
    // Runtime stores destination indices as i32 in ConsolidatorLayout.
    let file_name = format!("synclite_consolidator_metadata_{}.db", dst_index as i32);
    let mut out = vec![work_dir.join(&device_dir_name).join(&file_name)];

    // Only old single-destination configurations without an explicit root
    // may use the legacy embedded directories. Never scan sibling aliases
    // or fall back from a custom root to a stale checkpoint elsewhere.
    if multi_destination || cfg.extra.contains_key(crate::DEVICE_DATA_ROOT_KEY) {
        return out;
    }

    let legacy_work_dir = layout.device_home.join("synclite-syncer");
    if legacy_work_dir.exists() {
        out.push(legacy_work_dir.join(&device_dir_name).join(&file_name));
    }
    out.push(
        layout
            .device_home
            .join("synclite-consolidator")
            .join(&device_dir_name)
            .join(&file_name),
    );

    out
}

fn read_applied_commit_id_from_local_metadata(
    layout: &DeviceLayout,
    cfg: &SyncLiteConfig,
    dst_index: usize,
    multi_destination: bool,
    timeout: Option<Duration>,
) -> Option<i64> {
    let (device_id, device_name) = read_device_id_name(layout).ok()?;
    let db_path = local_checkpoint_db_candidates(
        layout,
        &device_id,
        &device_name,
        cfg,
        dst_index,
        multi_destination,
    )
    .into_iter()
    .find(|p| p.exists())?;
    read_applied_commit_id_from_local_path_with_timeout(&db_path, timeout)
}

fn read_applied_commit_id_from_local_path_with_timeout(
    db_path: &Path,
    timeout: Option<Duration>,
) -> Option<i64> {
    let conn = open_ro_with_timeout(db_path, timeout)?;
    // LOCAL checkpoints belong to a single device/destination, selected by
    // the file path. Unlike destination checkpoints, the production state
    // schema has no device identity columns and maintains one progress row.
    let row: rusqlite::Result<Option<i64>> =
        conn.query_row("SELECT MAX(commit_id) FROM synclite_checkpoint", [], |r| {
            r.get::<_, Option<i64>>(0)
        });
    row.ok().flatten()
}

fn read_applied_commit_id_from_destination(
    layout: &DeviceLayout,
    cfg: &SyncLiteConfig,
    dst_index: usize,
    timeout: Option<Duration>,
) -> Option<i64> {
    let dst_type = crate::parse_cfg_destination_backend_for_index(cfg, dst_index);
    let dst_conn = crate::parse_cfg_destination_connection_for_index(
        cfg,
        dst_index,
        dst_type,
        device_data_root(cfg)
            .join(destination_alias(cfg, dst_index))
            .join(format!("synclite_destination_apply_{dst_index}.db")),
    );
    let dst_schema = crate::parse_cfg_string_for_index(cfg, &["dst-schema"], dst_index);
    let (device_id, device_name) = read_device_id_name(layout).ok()?;
    read_applied_commit_id_from_destination_values(
        dst_type,
        dst_conn,
        dst_schema,
        device_id,
        device_name,
        timeout,
    )
}

fn read_applied_commit_id_from_destination_values(
    dst_type: DstType,
    dst_conn: String,
    dst_schema: Option<String>,
    device_id: String,
    device_name: String,
    timeout: Option<Duration>,
) -> Option<i64> {
    let dst_table = dst_schema
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            format!(
                "{}.{}",
                quote_pg_ident(s),
                quote_pg_ident("synclite_checkpoint")
            )
        })
        .unwrap_or_else(|| "synclite_checkpoint".to_string());

    match dst_type {
        DstType::Sqlite => {
            let conn = open_ro_with_timeout(Path::new(&dst_conn), timeout)?;
            let row: rusqlite::Result<Option<i64>> = conn.query_row(
                "SELECT MAX(commit_id)
                 FROM synclite_checkpoint
                 WHERE synclite_device_id = ?1 AND synclite_device_name = ?2",
                rusqlite::params![device_id, device_name],
                |r| r.get::<_, Option<i64>>(0),
            );
            row.ok().flatten()
        }
        DstType::DuckDb => {
            let conn = DuckConnection::open(&dst_conn).ok()?;
            let sql = format!(
                "SELECT MAX(commit_id)
                 FROM {dst_table}
                 WHERE synclite_device_id = ? AND synclite_device_name = ?"
            );
            let mut stmt = conn.prepare(&sql).ok()?;
            let mut rows = stmt
                .query(duck_params_from_iter(
                    [DuckValue::Text(device_id), DuckValue::Text(device_name)].iter(),
                ))
                .ok()?;
            let row = rows.next().ok()??;
            row.get::<_, Option<i64>>(0).ok().flatten()
        }
        DstType::Postgres => {
            // Do not let one unreachable destination consume more than the
            // shared await budget. The startup option bounds server-side query
            // execution; connect_timeout bounds socket establishment.
            let mut config = dst_conn.parse::<PgConfig>().ok()?;
            if let Some(timeout) = timeout {
                if timeout.is_zero() {
                    return None;
                }
                config.connect_timeout(timeout);
                let timeout_ms = timeout.as_millis().clamp(1, i32::MAX as u128);
                let statement_timeout = format!("-c statement_timeout={timeout_ms}");
                let options = match config.get_options().map(str::trim) {
                    Some(existing) if !existing.is_empty() => {
                        format!("{existing} {statement_timeout}")
                    }
                    _ => statement_timeout,
                };
                config.options(&options);
            }
            let mut client = config.connect(NoTls).ok()?;
            let sql = format!(
                "SELECT MAX(commit_id)
                 FROM {dst_table}
                 WHERE synclite_device_id = $1 AND synclite_device_name = $2"
            );
            let row = client.query_one(&sql, &[&device_id, &device_name]).ok()?;
            row.try_get::<_, Option<i64>>(0).ok().flatten()
        }
    }
}

/// Read the applied commit id for one live embedded consolidator layout.
///
/// This is used by the Java JNI bridge, whose workers are spawned directly
/// from `ConsolidatorLayout` values rather than through `SyncLiteConfig`.
/// Returning `None` means that this destination is missing, unreachable, or
/// has not seeded its checkpoint yet and therefore must not count as caught up.
pub(crate) fn read_applied_commit_id_for_layout_with_timeout(
    layout: &ConsolidatorLayout,
    timeout: Option<Duration>,
) -> Option<i64> {
    match layout.metadata_store {
        MetadataStore::Local => {
            read_applied_commit_id_from_local_path_with_timeout(&layout.state_db_path, timeout)
        }
        MetadataStore::Destination => read_applied_commit_id_from_destination_values(
            layout.dst_type,
            layout.dst_connection_string.clone(),
            if layout.dst_use_schema_scope_resolution {
                layout.dst_schema.clone()
            } else {
                None
            },
            layout.device_id.clone(),
            layout.device_name.clone(),
            timeout,
        ),
    }
}

/// Read the minimum applied `commit_id` across all enabled destinations.
///
/// Use the same destination discovery as worker creation, without a fixed
/// index limit. Source is selected strictly by each metadata-store mode:
/// - `DESTINATION`: read `synclite_checkpoint` from the destination DB.
/// - `LOCAL`: read `synclite_checkpoint` from local consolidator metadata DB.
/// Any missing/unreadable checkpoint makes overall progress unknown. In
/// particular, never substitute a healthy destination's heartbeat or temp DB.
pub(crate) fn read_applied_commit_id(layout: &DeviceLayout) -> Option<i64> {
    read_applied_commit_id_before(layout, None)
}

pub(crate) fn read_applied_commit_id_before(
    layout: &DeviceLayout,
    deadline: Option<Instant>,
) -> Option<i64> {
    let cfg = persisted_config(layout).ok()?;
    if !crate::has_any_destination_config(&cfg) {
        return None;
    }
    let indices = crate::parse_cfg_destination_indices(&cfg);
    let multi_destination = indices.len() > 1;
    let mut minimum: Option<i64> = None;
    for dst_index in indices {
        let timeout = deadline.map(|value| value.saturating_duration_since(Instant::now()));
        let applied = match metadata_store(&cfg, dst_index) {
            MetadataStoreMode::Destination => {
                read_applied_commit_id_from_destination(layout, &cfg, dst_index, timeout)
            }
            MetadataStoreMode::Local => read_applied_commit_id_from_local_metadata(
                layout,
                &cfg,
                dst_index,
                multi_destination,
                timeout,
            ),
        }?;
        minimum = Some(minimum.map_or(applied, |previous| previous.min(applied)));
    }
    minimum
}

// Reuse the production initializer without adding a manifest dependency:
// these schema regression tests must not maintain their own LOCAL DDL.
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../consolidator/state/src/lib.rs"]
mod test_state;

#[cfg(test)]
mod tests {
    use super::*;
    use consolidator_core::{ConsolidatorLayout, DestinationSyncMode, MetadataStore};
    use std::time::Duration;

    // No workers, sleeps, network, environment mutation, or shared work roots.
    // Checkpoint fixtures use the same device identity and layout as runtime.
    struct Fixture {
        _dir: tempfile::TempDir,
        layout: DeviceLayout,
        cfg: SyncLiteConfig,
        uuid: String,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let layout = DeviceLayout::new(dir.path().join("source.db"));
            let uuid = uuid::Uuid::new_v4().to_string();
            let md = Metadata::open_or_create(&layout.metadata_path).unwrap();
            md.put("uuid", &uuid).unwrap();
            md.put("device_name", "status-test").unwrap();
            md.put("device_type", "SQLITE").unwrap();
            let source = Connection::open(&layout.db_path).unwrap();
            source
                .execute_batch(
                    "CREATE TABLE synclite_txn(commit_id INTEGER);
                 INSERT INTO synclite_txn VALUES(100);",
                )
                .unwrap();
            let mut cfg = SyncLiteConfig::default();
            cfg.extra.insert(
                crate::DEVICE_DATA_ROOT_KEY.to_string(),
                dir.path()
                    .join("custom-work")
                    .to_string_lossy()
                    .into_owned(),
            );
            Self {
                _dir: dir,
                layout,
                cfg,
                uuid,
            }
        }

        fn set(&mut self, key: &str, value: &str) {
            self.cfg.extra.insert(key.to_string(), value.to_string());
        }

        fn destination(&mut self, suffix: &str) -> PathBuf {
            let path = self._dir.path().join(format!("destination{suffix}.db"));
            self.set(&format!("dst-type{suffix}"), "SQLITE");
            self.set(
                &format!("dst-connection-string{suffix}"),
                &format!("jdbc:sqlite:{}", path.display()),
            );
            path
        }

        fn persist(&self) {
            crate::persist_initialize_config_to_metadata(&self.layout.db_path, &self.cfg).unwrap();
        }

        fn local_checkpoint(&self, path: &Path, commit: i64) {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            test_state::initialize_state_db(path).unwrap();
            let conn = Connection::open(path).unwrap();
            conn.execute("DELETE FROM synclite_checkpoint", []).unwrap();
            test_state::ensure_synclite_checkpoint_seeded(&conn, &self.layout.db_path).unwrap();
            conn.execute("UPDATE synclite_checkpoint SET commit_id = ?1", [commit])
                .unwrap();
        }

        fn checkpoint(&self, path: &Path, commit: i64) {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let conn = Connection::open(path).unwrap();
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS synclite_checkpoint(
                    synclite_device_id TEXT, synclite_device_name TEXT, commit_id INTEGER);
                 DELETE FROM synclite_checkpoint;",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO synclite_checkpoint VALUES(?1, 'status-test', ?2)",
                rusqlite::params![self.uuid, commit],
            )
            .unwrap();
            // Other devices must not affect this device's progress.
            conn.execute(
                "INSERT INTO synclite_checkpoint VALUES('other-device', 'status-test', 9999)",
                [],
            )
            .unwrap();
        }

        fn runtime_local_path(&self, index: usize, multi: bool) -> PathBuf {
            let root = PathBuf::from(self.cfg.extra.get(crate::DEVICE_DATA_ROOT_KEY).unwrap());
            let work = if multi {
                root.join(
                    crate::parse_cfg_string_for_index(&self.cfg, &["dst-alias"], index)
                        .unwrap_or_else(|| format!("DB-{index}")),
                )
            } else {
                root
            };
            ConsolidatorLayout::new(
                &self.layout.device_home,
                Some(work),
                self.uuid.clone(),
                "status-test",
                "SQLITE",
                "source.db",
                index as i32,
                true,
                MetadataStore::Local,
                DstType::Sqlite,
                DestinationSyncMode::Consolidation,
                String::new(),
                1,
                1,
                false,
                1,
                1,
                1,
                true,
            )
            .consolidator_metadata_path(index as i32)
        }

        fn runtime_destination_layout(
            &self,
            index: usize,
            destination: &Path,
        ) -> ConsolidatorLayout {
            let root = PathBuf::from(self.cfg.extra.get(crate::DEVICE_DATA_ROOT_KEY).unwrap());
            ConsolidatorLayout::new(
                &self.layout.device_home,
                Some(root.join(format!("DB-{index}"))),
                self.uuid.clone(),
                "status-test",
                "SQLITE",
                "source.db",
                index as i32,
                true,
                MetadataStore::Destination,
                DstType::Sqlite,
                DestinationSyncMode::Consolidation,
                destination.to_string_lossy().into_owned(),
                1,
                1,
                false,
                1,
                1,
                1,
                true,
            )
        }

        fn assert_not_complete(&self) {
            assert!(crate::await_sync(&self.layout.db_path, Duration::ZERO).is_err());
            assert!(
                crate::await_applied_commit(&self.layout.db_path, 100, Duration::ZERO).is_err()
            );
        }

        fn assert_complete(&self) {
            crate::await_sync(&self.layout.db_path, Duration::ZERO).unwrap();
            crate::await_applied_commit(&self.layout.db_path, 100, Duration::ZERO).unwrap();
        }
    }

    #[test]
    fn all_destinations_must_catch_up_before_await_completes() {
        let mut f = Fixture::new();
        let first = f.destination("-1");
        let second = f.destination("-2");
        f.persist();
        f.checkpoint(&first, 100);
        f.checkpoint(&second, 40);
        assert_eq!(read_applied_commit_id(&f.layout), Some(40));
        let latency = sync_latency(&f.layout.db_path).unwrap();
        assert_eq!(latency.applied_commit_id, Some(40));
        assert_eq!(latency.latency_ms, 60);
        f.assert_not_complete();

        f.checkpoint(&second, 100);
        assert_eq!(read_applied_commit_id(&f.layout), Some(100));
        f.assert_complete();

        // Ordering cannot change completion: the first destination may lag too.
        f.checkpoint(&first, 30);
        assert_eq!(read_applied_commit_id(&f.layout), Some(30));
        f.assert_not_complete();
    }

    #[test]
    fn destination_checkpoint_reads_use_max_for_duplicate_device_rows() {
        let mut f = Fixture::new();
        let destination = f.destination("-1");
        f.persist();
        f.checkpoint(&destination, 40);

        let conn = Connection::open(&destination).unwrap();
        conn.execute(
            "INSERT INTO synclite_checkpoint VALUES(?1, 'status-test', 100)",
            [&f.uuid],
        )
        .unwrap();
        drop(conn);

        assert_eq!(read_applied_commit_id(&f.layout), Some(100));
        f.assert_complete();
    }

    #[test]
    fn live_layout_wait_used_by_java_requires_every_destination() {
        let f = Fixture::new();
        let first = f._dir.path().join("java-first.db");
        let second = f._dir.path().join("java-second.db");
        let layouts = vec![
            f.runtime_destination_layout(1, &first),
            f.runtime_destination_layout(2, &second),
        ];

        f.checkpoint(&first, 100);
        f.checkpoint(&second, 40);
        let error = crate::await_applied_commit_for_destinations(&layouts, 100, Duration::ZERO)
            .unwrap_err()
            .to_string();
        assert!(error.contains("destination_checkpoints=[1=100, 2=40]"));

        f.checkpoint(&second, 100);
        crate::await_applied_commit_for_destinations(&layouts, 100, Duration::ZERO).unwrap();
    }

    #[test]
    fn missing_unreadable_or_unseeded_destination_never_counts_as_complete() {
        let mut f = Fixture::new();
        let first = f.destination("-1");
        let second = f.destination("-2");
        f.persist();
        f.checkpoint(&first, 100);
        assert_eq!(read_applied_commit_id(&f.layout), None);
        assert!(
            !second.exists(),
            "read-only inspection must not create the destination"
        );
        f.assert_not_complete();

        // A directory is an unavailable SQLite DB on every supported OS.
        std::fs::create_dir(&second).unwrap();
        assert_eq!(read_applied_commit_id(&f.layout), None);
        f.assert_not_complete();
        std::fs::remove_dir(&second).unwrap();

        f.checkpoint(&second, 100);
        let conn = Connection::open(&second).unwrap();
        conn.execute(
            "DELETE FROM synclite_checkpoint WHERE synclite_device_id = ?1",
            [&f.uuid],
        )
        .unwrap();
        drop(conn);
        assert_eq!(read_applied_commit_id(&f.layout), None);
        assert_eq!(sync_latency(&f.layout.db_path).unwrap().latency_ms, -1);
        f.assert_not_complete();

        f.checkpoint(&second, 100);
        f.assert_complete();
    }

    #[test]
    fn indexed_only_config_without_sync_marker_does_not_short_circuit() {
        let mut f = Fixture::new();
        let first = f.destination("-3");
        let second = f.destination("-97");
        f.persist();
        f.checkpoint(&first, 100);
        f.checkpoint(&second, 20);
        Metadata::open_or_create(&f.layout.metadata_path)
            .unwrap()
            .put_i64("sync_configured", 0)
            .unwrap();
        assert!(sync_configured(&f.layout).unwrap());
        assert_eq!(read_applied_commit_id(&f.layout), Some(20));
        f.assert_not_complete();
        f.checkpoint(&second, 100);
        f.assert_complete();
    }

    #[test]
    fn indexed_and_unsuffixed_keys_follow_runtime_precedence() {
        let mut f = Fixture::new();
        let first = f.destination("");
        // Index 1 participates because runtime discovers this indexed key.
        f.set("metadata-store-1", "DESTINATION");
        let second = f.destination("-7");
        f.persist();
        f.checkpoint(&first, 100);
        f.checkpoint(&second, 50);
        assert_eq!(read_applied_commit_id(&f.layout), Some(50));
        f.assert_not_complete();

        let override_path = f.destination("-1");
        f.persist();
        f.checkpoint(&override_path, 10);
        assert_eq!(read_applied_commit_id(&f.layout), Some(10));

        // No unsuffixed fallback is allowed for other destination indices.
        f.set("dst-connection-string-7", "");
        f.persist();
        assert_eq!(read_applied_commit_id(&f.layout), None);
    }

    #[test]
    fn discovery_matches_runtime_when_only_nonfirst_index_is_present() {
        let mut f = Fixture::new();
        let unsuffixed = f.destination("");
        let indexed = f.destination("-7");
        f.persist();
        f.checkpoint(&unsuffixed, 10);
        f.checkpoint(&indexed, 100);
        // Runtime does not union an implicit index 1 into a nonempty index set.
        assert_eq!(crate::parse_cfg_destination_indices(&f.cfg), vec![7]);
        assert_eq!(read_applied_commit_id(&f.layout), Some(100));
        f.assert_complete();
    }

    #[test]
    fn local_metadata_uses_custom_root_alias_and_noncontiguous_indices() {
        let mut f = Fixture::new();
        f.destination("-3");
        f.destination("-97");
        f.set("metadata-store-3", "LOCAL");
        f.set("dst-metadata-store-97", "false");
        f.set("dst-alias-3", "first-alias");
        f.set("dst-alias-97", "second-alias");
        f.persist();
        let first = f.runtime_local_path(3, true);
        let second = f.runtime_local_path(97, true);
        f.local_checkpoint(&first, 100);
        // A checkpoint for the same index under a different alias is stale.
        let wrong = first
            .parent()
            .unwrap()
            .join("synclite_consolidator_metadata_97.db");
        f.local_checkpoint(&wrong, 100);
        assert_eq!(read_applied_commit_id(&f.layout), None);
        f.assert_not_complete();
        f.local_checkpoint(&second, 25);
        assert_eq!(read_applied_commit_id(&f.layout), Some(25));
        f.assert_not_complete();
        f.local_checkpoint(&second, 100);
        f.assert_complete();
    }

    #[test]
    fn mixed_local_and_destination_modes_do_not_use_each_others_checkpoints() {
        let mut f = Fixture::new();
        let first_destination = f.destination("-1");
        let second_destination = f.destination("-8");
        f.set("metadata-store", "LOCAL");
        // Unsuffixed LOCAL is for index 1 only; index 8 defaults to DESTINATION.
        f.persist();
        f.checkpoint(&first_destination, 999);
        f.checkpoint(&second_destination, 100);
        assert_eq!(read_applied_commit_id(&f.layout), None);
        let first_local = f.runtime_local_path(1, true);
        f.local_checkpoint(&first_local, 60);
        assert_eq!(read_applied_commit_id(&f.layout), Some(60));
        f.assert_not_complete();
        f.local_checkpoint(&first_local, 100);
        f.assert_complete();
        std::fs::remove_file(&second_destination).unwrap();
        f.local_checkpoint(&f.runtime_local_path(8, true), 999);
        assert_eq!(read_applied_commit_id(&f.layout), None);
        f.assert_not_complete();
    }

    #[test]
    fn single_unsuffixed_destination_remains_compatible() {
        let mut f = Fixture::new();
        let destination = f.destination("");
        f.persist();
        f.checkpoint(&destination, 80);
        assert_eq!(read_applied_commit_id(&f.layout), Some(80));
        f.assert_not_complete();
        f.checkpoint(&destination, 100);
        f.assert_complete();
    }

    #[test]
    fn single_local_destination_uses_root_without_alias_even_at_high_index() {
        let mut f = Fixture::new();
        f.destination("-97");
        f.set("metadata-store-97", "LOCAL");
        f.set("dst-alias-97", "unused-for-single-work-dir");
        f.persist();
        let path = f.runtime_local_path(97, false);
        f.local_checkpoint(&path, 100);
        assert_eq!(read_applied_commit_id(&f.layout), Some(100));
        f.assert_complete();
    }

    #[test]
    fn single_local_legacy_path_is_allowed_only_without_explicit_root() {
        let mut f = Fixture::new();
        f.destination("");
        f.set("metadata-store", "LOCAL");
        let legacy = f
            .layout
            .device_home
            .join("synclite-syncer")
            .join(format!("synclite-status-test-{}", f.uuid))
            .join("synclite_consolidator_metadata_1.db");
        f.local_checkpoint(&legacy, 100);
        f.persist();
        assert_eq!(read_applied_commit_id(&f.layout), None);
        f.assert_not_complete();
        let conn = Connection::open(&f.layout.metadata_path).unwrap();
        conn.execute(
            "DELETE FROM metadata WHERE key = ?1",
            [crate::DEVICE_DATA_ROOT_KEY],
        )
        .unwrap();
        drop(conn);
        assert_eq!(read_applied_commit_id(&f.layout), Some(100));
        f.assert_complete();
    }

    #[test]
    fn default_destination_path_matches_runtime_for_discovered_index() {
        let mut f = Fixture::new();
        f.set("dst-type-97", "SQLITE");
        f.set("dst-alias-97", "default-destination");
        f.persist();
        let destination = PathBuf::from(f.cfg.extra.get(crate::DEVICE_DATA_ROOT_KEY).unwrap())
            .join("default-destination")
            .join("synclite_destination_apply_97.db");
        f.checkpoint(&destination, 100);
        assert_eq!(read_applied_commit_id(&f.layout), Some(100));
        f.assert_complete();
    }

    #[test]
    fn initialize_config_roundtrips_every_destination_discovery_key() {
        let mut f = Fixture::new();
        f.destination("-3");
        f.destination("-97");
        // This key alone can discover destination 1 and must not disappear
        // on reload. The transient reinit key is object-init-mode, not this.
        f.set("dst-idempotent-data-ingestion-1", "true");
        for (base, value) in [
            ("dst-alias", "remote"),
            ("metadata-store", "LOCAL"),
            ("dst-metadata-store", "DESTINATION"),
            ("dst-oper-retry-count", "4"),
            ("dst-oper-retry-interval-ms", "50"),
            ("dst-idempotent-data-ingestion", "true"),
            ("dst-insert-batch-size", "200"),
            ("dst-update-batch-size", "200"),
            ("dst-delete-batch-size", "200"),
            ("dst-database", "destination_db"),
            ("dst-schema", "destination_schema"),
            ("dst-enable-filter-mapper-rules", "true"),
            ("dst-filter-mapper-rules-file", "mapper.csv"),
            ("dst-allow-unspecified-tables", "false"),
            ("dst-allow-unspecified-columns", "false"),
        ] {
            f.set(&format!("{base}-97"), value);
        }
        // A destination can also be discovered only through a retry key.
        f.set("dst-oper-retry-count-105", "2");
        f.persist();
        let reloaded =
            crate::default_config_for_backend(f.layout.db_path.clone(), crate::Backend::Sqlite);
        assert_eq!(reloaded.extra, f.cfg.extra);
        assert_eq!(
            crate::parse_cfg_destination_indices(&reloaded),
            vec![1, 3, 97, 105]
        );
        assert_eq!(
            Metadata::open_or_create(&f.layout.metadata_path)
                .unwrap()
                .get_i64("sync_configured")
                .unwrap(),
            Some(1)
        );

        let mut explicit = SyncLiteConfig::default();
        explicit
            .extra
            .insert("dst-schema-97".into(), "caller_schema".into());
        crate::hydrate_initialize_config_from_metadata(&f.layout.db_path, &mut explicit);
        assert_eq!(explicit.extra["dst-schema-97"], "caller_schema");
        assert_eq!(
            explicit.extra["dst-connection-string-3"],
            f.cfg.extra["dst-connection-string-3"]
        );
        // Re-init without a destination preserves the existing destinations.
        crate::persist_initialize_config_to_metadata(&f.layout.db_path, &SyncLiteConfig::default())
            .unwrap();
        assert_eq!(
            crate::read_persisted_initialize_extra(&f.layout.db_path).unwrap(),
            f.cfg.extra
        );
        f.set("dst-object-init-mode-1", "OVERWRITE_OBJECT");
        f.persist();
        assert!(!crate::read_persisted_initialize_extra(&f.layout.db_path)
            .unwrap()
            .contains_key("dst-object-init-mode-1"));
    }

    #[test]
    fn local_production_schema_handles_missing_empty_and_zero_checkpoints() {
        let mut f = Fixture::new();
        f.destination("-1");
        f.set("metadata-store-1", "LOCAL");
        f.persist();
        let path = f.runtime_local_path(1, false);
        assert_eq!(read_applied_commit_id(&f.layout), None);
        assert!(!path.exists(), "inspection must not create local state");
        f.assert_not_complete();

        std::fs::create_dir_all(&path).unwrap();
        assert_eq!(read_applied_commit_id(&f.layout), None);
        f.assert_not_complete();
        std::fs::remove_dir(&path).unwrap();
        test_state::initialize_state_db(&path).unwrap();
        assert_eq!(read_applied_commit_id(&f.layout), None);
        assert_eq!(sync_latency(&f.layout.db_path).unwrap().latency_ms, -1);
        f.assert_not_complete();

        f.local_checkpoint(&path, 0);
        assert_eq!(read_applied_commit_id(&f.layout), Some(0));
        assert_eq!(
            sync_latency(&f.layout.db_path).unwrap().applied_commit_id,
            None
        );
        f.assert_not_complete();
        f.local_checkpoint(&path, 40);
        assert_eq!(sync_latency(&f.layout.db_path).unwrap().latency_ms, 60);
        f.assert_not_complete();
        f.local_checkpoint(&path, 100);
        f.assert_complete();
    }

    #[test]
    fn explicit_reopen_shrinks_local_destination_set_and_changes_layout() {
        let mut f = Fixture::new();
        f.destination("-1");
        f.destination("-2");
        f.set("metadata-store-1", "LOCAL");
        f.set("dst-metadata-store-2", "LOCAL");
        f.set("dst-alias-1", "old-first");
        f.set("dst-alias-2", "old-second");
        f.set("dst-oper-retry-count-2", "4");
        f.set("dst-sync-mode", "REPLICATION");
        f.persist();
        f.local_checkpoint(&f.runtime_local_path(1, true), 10);
        f.local_checkpoint(&f.runtime_local_path(2, true), 20);
        assert_eq!(read_applied_commit_id(&f.layout), Some(10));

        // This is the effective config a direct Logger::open_with receives.
        // Only destination 1 remains, with a new root and no alias or mode.
        f.cfg.extra.clear();
        f.destination("-1");
        f.set("metadata-store-1", "LOCAL");
        let root = f._dir.path().join("replacement-work");
        f.set(crate::DEVICE_DATA_ROOT_KEY, &root.to_string_lossy());
        f.persist();
        let hydrated =
            crate::default_config_for_backend(f.layout.db_path.clone(), crate::Backend::Sqlite);
        assert_eq!(hydrated.extra, f.cfg.extra);
        assert_eq!(crate::parse_cfg_destination_indices(&hydrated), vec![1]);
        assert!(matches!(
            crate::parse_cfg_destination_sync_mode_for_index(&hydrated, 1),
            crate::DstSyncMode::Consolidation
        ));
        assert_eq!(read_applied_commit_id(&f.layout), None);
        f.assert_not_complete();
        f.local_checkpoint(&f.runtime_local_path(1, false), 100);
        assert_eq!(read_applied_commit_id(&f.layout), Some(100));
        f.assert_complete();
    }

    #[test]
    fn explicit_destination_snapshot_drops_omitted_root_and_metadata_mode() {
        let mut f = Fixture::new();
        f.destination("-1");
        f.destination("-2");
        f.set("metadata-store", "LOCAL");
        f.set("dst-metadata-store-1", "LOCAL");
        f.set("dst-alias-1", "old-alias");
        f.set("dst-sync-mode", "REPLICATION");
        f.persist();
        f.local_checkpoint(&f.runtime_local_path(1, true), 5);
        let md = Metadata::open_or_create(&f.layout.metadata_path).unwrap();
        md.put_i64("backup_taken", 1).unwrap();
        md.put("unrelated-option", "keep").unwrap();

        f.cfg.extra.clear();
        let destination = f.destination("-1");
        // initialize() hydrates before persisting. Explicit destination config
        // must not pick up an omitted root, LOCAL mode, or destination 2 here.
        let mut explicit = f.cfg.clone();
        crate::hydrate_initialize_config_from_metadata(&f.layout.db_path, &mut explicit);
        assert_eq!(explicit.extra, f.cfg.extra);
        crate::persist_initialize_config_to_metadata(&f.layout.db_path, &explicit).unwrap();
        let hydrated =
            crate::default_config_for_backend(f.layout.db_path.clone(), crate::Backend::Sqlite);
        assert_eq!(hydrated.extra, f.cfg.extra);
        assert_eq!(crate::parse_cfg_destination_indices(&hydrated), vec![1]);
        assert_eq!(metadata_store(&hydrated, 1), MetadataStoreMode::Destination);
        assert_eq!(device_data_root(&hydrated), default_device_data_root());
        assert_eq!(md.get("uuid").unwrap(), Some(f.uuid.clone()));
        assert_eq!(md.get_i64("backup_taken").unwrap(), Some(1));
        assert_eq!(md.get("unrelated-option").unwrap().as_deref(), Some("keep"));
        assert_eq!(md.get_i64("sync_configured").unwrap(), Some(1));
        assert_eq!(read_applied_commit_id(&f.layout), None);
        f.checkpoint(&destination, 100);
        f.assert_complete();
    }

    #[test]
    fn destination_snapshot_and_sync_marker_roll_back_together_on_failure() {
        let mut f = Fixture::new();
        f.destination("-1");
        f.destination("-2");
        f.set("metadata-store", "LOCAL");
        f.persist();
        let original = f.cfg.extra.clone();
        let md = Metadata::open_or_create(&f.layout.metadata_path).unwrap();
        md.put_i64("sync_configured", 0).unwrap();
        let conn = Connection::open(&f.layout.metadata_path).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER reject_snapshot_marker BEFORE INSERT ON metadata
             WHEN NEW.key = 'sync_configured'
             BEGIN SELECT RAISE(ABORT, 'test snapshot failure'); END;",
        )
        .unwrap();
        f.cfg.extra.clear();
        f.destination("-1");
        assert!(crate::persist_initialize_config_to_metadata(&f.layout.db_path, &f.cfg).is_err());
        assert_eq!(
            crate::read_persisted_initialize_extra(&f.layout.db_path).unwrap(),
            original
        );
        assert_eq!(md.get_i64("sync_configured").unwrap(), Some(0));
        conn.execute_batch("DROP TRIGGER reject_snapshot_marker;")
            .unwrap();
        f.persist();
        assert_eq!(
            crate::read_persisted_initialize_extra(&f.layout.db_path).unwrap(),
            f.cfg.extra
        );
        assert_eq!(md.get_i64("sync_configured").unwrap(), Some(1));
    }

    #[test]
    fn no_destination_config_keeps_merge_behavior_and_sync_marker() {
        let mut f = Fixture::new();
        f.destination("-1");
        f.destination("-2");
        f.persist();
        let mut partial = SyncLiteConfig::default();
        partial
            .extra
            .insert("dst-sync-mode".into(), "REPLICATION".into());
        crate::persist_initialize_config_to_metadata(&f.layout.db_path, &partial).unwrap();
        f.set("dst-sync-mode", "REPLICATION");
        assert_eq!(
            crate::read_persisted_initialize_extra(&f.layout.db_path).unwrap(),
            f.cfg.extra
        );
        crate::hydrate_initialize_config_from_metadata(&f.layout.db_path, &mut partial);
        assert_eq!(partial.extra, f.cfg.extra);
        assert_eq!(
            Metadata::open_or_create(&f.layout.metadata_path)
                .unwrap()
                .get_i64("sync_configured")
                .unwrap(),
            Some(1)
        );
    }

    #[test]
    fn unconfigured_device_and_no_user_commits_keep_short_circuits() {
        let mut f = Fixture::new();
        f.persist();
        assert!(!sync_configured(&f.layout).unwrap());
        f.assert_complete();
        f.destination("-97");
        f.persist();
        Connection::open(&f.layout.db_path)
            .unwrap()
            .execute("DELETE FROM synclite_txn", [])
            .unwrap();
        crate::await_sync(&f.layout.db_path, Duration::ZERO).unwrap();
        crate::await_applied_commit(&f.layout.db_path, 0, Duration::ZERO).unwrap();
    }
}
