//! Sync status / latency / statistics inspection APIs.
//!
//! These are read-only helpers a host application can call at any
//! time to inspect what the in-process consolidator is doing for a
//! given device. They do not start workers, hold connections, or
//! contact destinations — they read SQLite files the consolidator
//! has already produced under `<db_path>.synclite/`.
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

use duckdb::{params_from_iter as duck_params_from_iter, types::Value as DuckValue, Connection as DuckConnection};
use logger_core::{Error, Result};
use postgres::{Client as PgClient, NoTls};
use rusqlite::{Connection, OpenFlags};

use crate::layout::DeviceLayout;
use crate::metadata::Metadata;
use crate::{default_device_data_root, normalize_db_path};
use crate::pause;

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
    /// Last commit id known to be applied at the destination, as
    /// recorded by the consolidator. `None` when the consolidator
    /// has not yet produced a heartbeat (destination unreachable or
    /// consolidator not running).
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
    let state = if paused { SyncState::Paused } else { SyncState::Running };

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
    let applied_commit_id = read_applied_commit_id(&layout)
        .filter(|v| *v > 0);
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
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .ok()
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
    let v: rusqlite::Result<Option<i64>> = conn.query_row(
        "SELECT MAX(commit_id) FROM synclite_txn",
        [],
        |row| row.get::<_, Option<i64>>(0),
    );
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

fn discover_metadata_store(layout: &DeviceLayout) -> Option<(MetadataStoreMode, i32)> {
    let md = Metadata::open_or_create(&layout.metadata_path).ok()?;
    for idx in 1..=64 {
        let v = md
            .get(&format!("metadata-store-{idx}"))
            .ok()
            .flatten()
            .or_else(|| md.get(&format!("dst-metadata-store-{idx}")).ok().flatten());
        if let Some(raw) = v {
            let mode = match raw.trim().to_ascii_uppercase().as_str() {
                "LOCAL" => MetadataStoreMode::Local,
                "DESTINATION" => MetadataStoreMode::Destination,
                _ => continue,
            };
            return Some((mode, idx));
        }
    }

    Some((MetadataStoreMode::Destination, 1))
}

fn quote_pg_ident(raw: &str) -> String {
    format!("\"{}\"", raw.replace('"', "\"\""))
}

fn metadata_value(layout: &DeviceLayout, key: &str) -> Option<String> {
    let md = Metadata::open_or_create(&layout.metadata_path).ok()?;
    md.get(key).ok().flatten()
}

fn local_checkpoint_db_candidates(
    layout: &DeviceLayout,
    device_id: &str,
    device_name: &str,
    dst_index: i32,
) -> Vec<PathBuf> {
    let mut out = Vec::new();

    let default_root = default_device_data_root();
    let device_dir_name = format!("synclite-{}-{}", device_name, device_id);
    let file_name = format!("synclite_consolidator_metadata_{dst_index}.db");

    out.push(
        default_root
            .join(&device_dir_name)
            .join(&file_name),
    );

    // Multi-destination layouts use <device_data_root>/<dst_alias>/... .
    if let Ok(entries) = std::fs::read_dir(&default_root) {
        for entry in entries.flatten() {
            let p = entry.path();
            if !p.is_dir() {
                continue;
            }
            out.push(
                p.join(&device_dir_name)
                    .join(&file_name),
            );
        }
    }

    let legacy_work_dir = layout.device_home.join("synclite-syncer");
    if legacy_work_dir.exists() {
        out.push(
            legacy_work_dir
                .join(&device_dir_name)
                .join(&file_name),
        );
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

fn read_applied_commit_id_from_local_metadata(layout: &DeviceLayout) -> Option<i64> {
    let (_, dst_index) = discover_metadata_store(layout)?;
    let (device_id, device_name) = read_device_id_name(layout).ok()?;
    let db_path = local_checkpoint_db_candidates(layout, &device_id, &device_name, dst_index)
        .into_iter()
        .find(|p| p.exists())?;
    let conn = open_ro(&db_path)?;
    let row: rusqlite::Result<Option<i64>> = conn.query_row(
        "SELECT commit_id
         FROM synclite_checkpoint
         WHERE synclite_device_id = ?1 AND synclite_device_name = ?2",
        rusqlite::params![device_id, device_name],
        |r| r.get::<_, Option<i64>>(0),
    );
    row.ok().flatten()
}

fn read_applied_commit_id_from_destination(layout: &DeviceLayout) -> Option<i64> {
    let dst_type = metadata_value(layout, "dst-type")?.trim().to_ascii_uppercase();
    let raw_dst_conn = metadata_value(layout, "dst-connection-string")?;
    let dst_conn = match dst_type.as_str() {
        "SQLITE" => crate::parse_sqlite_path_from_connection(Some(&raw_dst_conn))
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or(raw_dst_conn.clone()),
        "DUCKDB" => crate::parse_duckdb_path_from_connection(Some(&raw_dst_conn))
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or(raw_dst_conn.clone()),
        "POSTGRES" => crate::translate_postgres_connection_string(&raw_dst_conn)
            .map(|s| s.to_string())
            .unwrap_or(raw_dst_conn.clone()),
        _ => raw_dst_conn.clone(),
    };
    let dst_schema = metadata_value(layout, "dst-schema");
    let (device_id, device_name) = read_device_id_name(layout).ok()?;

    let dst_table = dst_schema
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| format!("{}.{}", quote_pg_ident(s), quote_pg_ident("synclite_checkpoint")))
        .unwrap_or_else(|| "synclite_checkpoint".to_string());

    match dst_type.as_str() {
        "SQLITE" => {
            let conn = open_ro(Path::new(&dst_conn))?;
            let row: rusqlite::Result<Option<i64>> = conn.query_row(
                "SELECT commit_id
                 FROM synclite_checkpoint
                 WHERE synclite_device_id = ?1 AND synclite_device_name = ?2",
                rusqlite::params![device_id, device_name],
                |r| r.get::<_, Option<i64>>(0),
            );
            row.ok().flatten()
        }
        "DUCKDB" => {
            let conn = DuckConnection::open(&dst_conn).ok()?;
            let sql = format!(
                "SELECT commit_id
                 FROM {dst_table}
                 WHERE synclite_device_id = ? AND synclite_device_name = ?"
            );
            let mut stmt = conn.prepare(&sql).ok()?;
            let mut rows = stmt
                .query(duck_params_from_iter(
                    [
                        DuckValue::Text(device_id),
                        DuckValue::Text(device_name),
                    ]
                    .iter(),
                ))
                .ok()?;
            let row = rows.next().ok()??;
            row.get::<_, Option<i64>>(0).ok().flatten()
        }
        "POSTGRES" => {
            let mut client = PgClient::connect(&dst_conn, NoTls).ok()?;
            let sql = format!(
                "SELECT commit_id
                 FROM {dst_table}
                 WHERE synclite_device_id = $1 AND synclite_device_name = $2"
            );
            let row = client.query_opt(&sql, &[&device_id, &device_name]).ok()?;
            let row = row?;
            row.try_get::<_, Option<i64>>(0).ok().flatten()
        }
        _ => None,
    }
}

/// Read the consolidator's applied `commit_id`.
///
/// Source is selected strictly by metadata-store mode:
/// - `DESTINATION`: read `synclite_checkpoint` from the destination DB.
/// - `LOCAL`: read `synclite_checkpoint` from local consolidator metadata DB.
pub(crate) fn read_applied_commit_id(layout: &DeviceLayout) -> Option<i64> {
    match discover_metadata_store(layout)?.0 {
        MetadataStoreMode::Destination => read_applied_commit_id_from_destination(layout),
        MetadataStoreMode::Local => read_applied_commit_id_from_local_metadata(layout),
    }
}

