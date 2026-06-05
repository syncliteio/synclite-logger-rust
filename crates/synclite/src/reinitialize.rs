//! In-place device reinitialize.
//!
//! `reinitialize(db_path, clean_destination)` wipes every piece of
//! per-device local state — device home, stage subdir, work subdir,
//! all segment counters — and (when reachable) deletes this device's
//! rows from the destination metadata tables. When
//! `clean_destination=true` it additionally drops every user table
//! previously owned by this device on the destination.
//!
//! The device UUID, device-name, device-type and destination wiring
//! are preserved so the next `synclite::initialize` brings the device
//! back up as the *same logical device* but with a fresh initial
//! backup and segment sequence starting at 0. `reinitialize` also
//! flips `dst-idempotent-data-ingestion-1=true` in the persisted
//! device config so the re-seed tolerates any rows the destination
//! may still hold (REPLICATION or CONSOLIDATION mode).
//!
//! Trigger-file protocol: dropping a file named
//! `reinitialize.<device-name>` or
//! `reinitialize_with_clean_destination.<device-name>` alongside the
//! database file causes the next `synclite::initialize` call to fire
//! `reinitialize` before bringing the logger up; the trigger file is
//! removed on success.

use std::path::{Path, PathBuf};

use consolidator_core::{FilterMapperRules, retry_op};
use duckdb::Connection as DuckConn;
use logger_core::{Error, Result};
use postgres::{Client as PgClient, NoTls};
use rusqlite::Connection as SqlConn;

use crate::app_lock::AppLock;
use crate::layout::{ArchiveLayout, DeviceLayout};
use crate::metadata::Metadata;
use crate::{default_device_data_root, default_local_stage_dir, normalize_db_path};

/// Trigger-file basename for a preserve-destination reinit.
pub const TRIGGER_PRESERVE: &str = "reinitialize";
/// Trigger-file basename for a clean-destination reinit.
pub const TRIGGER_CLEAN: &str = "reinitialize_with_clean_destination";

/// Wipe local state and (where applicable) destination metadata so the
/// next `synclite::initialize` re-seeds the device from scratch. See
/// module documentation for the exact contract.
pub fn reinitialize<P: AsRef<Path>>(db_path: P, clean_destination: bool) -> Result<()> {
    let db_path = normalize_db_path(db_path.as_ref())?;
    let layout = DeviceLayout::new(db_path.clone());
    if !layout.metadata_path.exists() {
        return Err(Error::Config(format!(
            "reinitialize: device metadata not found at {}",
            layout.metadata_path.display()
        )));
    }

    // Block any concurrent Logger / Connection on this device.
    let _lock = AppLock::try_lock(&db_path)?;

    // Snapshot identity + destination wiring before we wipe.
    let snap = {
        let md = Metadata::open_or_create(&layout.metadata_path)?;
        Snapshot::read(&md, &layout)?
    };

    // Destination cleanup first: if it fails the local state is still
    // intact so the caller can retry once the destination is reachable.
    //
    // The cleanup queries `synclite_consolidator_table_metadata` for the
    // SOURCE table names this device owns (LOCAL file first, falling
    // back to the destination), applies the persisted FilterMapperRules
    // to derive the actual DESTINATION names, and then drops them +
    // deletes every metadata row for this (uuid, name, dst_index) — all
    // inside a single per-backend transaction so a mid-flight failure
    // leaves the destination unchanged and the operation safe to retry.
    //
    // In CONSOLIDATION mode the destination is shared across many
    // devices, so user-table drops are unsafe: silently downgrade to a
    // metadata-only cleanup even when `clean_destination=true`.
    if snap.dst_type.is_some() && snap.dst_conn_str.is_some() {
        clean_destination_for_device(&snap, &db_path, clean_destination)?;
    }

    // Tear down local state: stage/work subdir for this (name, uuid)
    // and the whole device home. We then rebuild a minimal metadata
    // file preserving identity + destination wiring.
    let stage_subdir = ArchiveLayout::new(
        &default_local_stage_dir(),
        &snap.device_name,
        &snap.uuid,
        &layout.db_file_name,
    )
    .stage_subdir;
    let work_subdir = work_subdir_for(&snap.device_name, &snap.uuid);
    remove_dir_all_if_exists(&stage_subdir)?;
    remove_dir_all_if_exists(&work_subdir)?;
    remove_dir_all_if_exists(&layout.device_home)?;

    std::fs::create_dir_all(&layout.device_home)?;
    let md = Metadata::open_or_create(&layout.metadata_path)?;
    snap.write_minimal(&md)?;
    Ok(())
}

/// Trigger-file check called at the top of `synclite::initialize`.
/// Fires `reinitialize` then deletes the trigger file. Returns `Ok(())`
/// when no trigger is present.
pub(crate) fn maybe_run_trigger(db_path: &Path, device_name: &str) -> Result<()> {
    let parent = db_path.parent().unwrap_or_else(|| Path::new("."));
    let clean = parent.join(format!("{TRIGGER_CLEAN}.{device_name}"));
    let preserve = parent.join(format!("{TRIGGER_PRESERVE}.{device_name}"));
    if clean.exists() {
        reinitialize(db_path, true)?;
        let _ = std::fs::remove_file(&clean);
    } else if preserve.exists() {
        reinitialize(db_path, false)?;
        let _ = std::fs::remove_file(&preserve);
    }
    Ok(())
}

struct Snapshot {
    uuid: String,
    device_name: String,
    device_type: String,
    database_name: String,
    database_id: i64,
    allow_concurrent_writers: i64,
    dst_type: Option<String>,
    dst_conn_str: Option<String>,
    dst_database: Option<String>,
    dst_schema: Option<String>,
    dst_sync_mode: Option<String>,
    /// 1-based destination index. Currently the device-side reinit
    /// only handles dst_index=1; the persisted snapshot keys are
    /// suffixed `-1` accordingly. Multi-destination expansion is a
    /// follow-up.
    dst_index: i32,
    /// Path to the local consolidator metadata SQLite file when
    /// `metadata-store-1=LOCAL` and the file exists. Used to query
    /// the device's owned destination tables without going through
    /// the destination connection. `None` ⇒ consult the destination
    /// (DESTINATION metadata-store mode, or LOCAL mode but the file
    /// is not present yet).
    local_md_path: Option<PathBuf>,
    /// Filter-mapper rules used by the consolidator when it created
    /// destination tables. Required to translate the source-side
    /// `table_name` values stored in
    /// `synclite_consolidator_table_metadata` into the actual
    /// destination identifiers we must drop.
    filter_mapper: FilterMapperRules,
    /// Destination retry policy lifted from the persisted device metadata
    /// (`dst-oper-retry-count-1` / `dst-oper-retry-interval-ms-1`).
    /// SQLite local metadata files can hit `SQLITE_BUSY`; remote
    /// destinations can hit transient network errors. Defaults
    /// match `ConsolidatorLayout` (3 attempts, 1000 ms backoff).
    retry_count: u32,
    retry_interval_ms: u64,
}

impl Snapshot {
    fn read(md: &Metadata, layout: &DeviceLayout) -> Result<Self> {
        let uuid = md.get("uuid")?.ok_or_else(|| {
            Error::Config("reinitialize: device uuid missing from metadata".into())
        })?;
        let device_name = md.get("device_name")?.ok_or_else(|| {
            Error::Config("reinitialize: device_name missing from metadata".into())
        })?;
        let dst_index = 1i32;
        let metadata_store_local = md
            .get("metadata-store-1")?
            .or(md.get("dst-metadata-store-1")?)
            .map(|v| {
                let t = v.trim();
                t.eq_ignore_ascii_case("local") || t.eq_ignore_ascii_case("false")
            })
            .unwrap_or(false);
        let local_md_path = if metadata_store_local {
            let work_subdir = work_subdir_for(&device_name, &uuid);
            let p = work_subdir.join(format!(
                "synclite_consolidator_metadata_{dst_index}.db"
            ));
            if p.exists() { Some(p) } else { None }
        } else {
            None
        };
        let filter_mapper = load_filter_mapper(md)?;
        let retry_count = md
            .get("dst-oper-retry-count-1")?
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(3);
        let retry_interval_ms = md
            .get("dst-oper-retry-interval-ms-1")?
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(1000);
        Ok(Self {
            uuid,
            device_name,
            device_type: md.get("device_type")?.unwrap_or_default(),
            database_name: md
                .get("database_name")?
                .unwrap_or_else(|| layout.db_file_name.clone()),
            database_id: md.get_i64("database_id")?.unwrap_or(0),
            allow_concurrent_writers: md.get_i64("allow_concurrent_writers")?.unwrap_or(0),
            dst_type: md.get("dst-type")?.map(|s| s.trim().to_ascii_uppercase()),
            dst_conn_str: md.get("dst-connection-string")?,
            dst_database: md.get("dst-database")?,
            dst_schema: md.get("dst-schema")?,
            dst_sync_mode: md.get("dst-sync-mode")?,
            dst_index,
            local_md_path,
            filter_mapper,
            retry_count,
            retry_interval_ms,
        })
    }

    fn write_minimal(&self, md: &Metadata) -> Result<()> {
        md.put("uuid", &self.uuid)?;
        md.put("device_name", &self.device_name)?;
        if !self.device_type.is_empty() {
            md.put("device_type", &self.device_type)?;
        }
        md.put("database_name", &self.database_name)?;
        md.put_i64("database_id", self.database_id)?;
        md.put_i64("allow_concurrent_writers", self.allow_concurrent_writers)?;
        if let Some(v) = &self.dst_type {
            md.put("dst-type", v)?;
        }
        if let Some(v) = &self.dst_conn_str {
            md.put("dst-connection-string", v)?;
        }
        if let Some(v) = &self.dst_database {
            md.put("dst-database", v)?;
        }
        if let Some(v) = &self.dst_schema {
            md.put("dst-schema", v)?;
        }
        if let Some(v) = &self.dst_sync_mode {
            md.put("dst-sync-mode", v)?;
        }
        // Force idempotent ingestion ON for the re-seed: the destination
        // may still hold rows / tables that would otherwise PK-collide
        // with the fresh initial backup.
        md.put("dst-idempotent-data-ingestion-1", "true")?;
        Ok(())
    }
}

fn work_subdir_for(device_name: &str, uuid: &str) -> PathBuf {
    let archive_name = if device_name.is_empty() {
        format!("synclite-{uuid}")
    } else {
        format!("synclite-{device_name}-{uuid}")
    };
    default_device_data_root().join(archive_name)
}

/// Best-effort reload of the consolidator's filter-mapper rules from
/// the keys the device persisted at `initialize` time. A missing or
/// disabled config — or any parse error — yields a
/// `FilterMapperRules::disabled()`, which makes the rest of the reinit
/// pipeline treat every source name as its own destination name.
fn load_filter_mapper(md: &Metadata) -> Result<FilterMapperRules> {
    let enabled = md
        .get("dst-enable-filter-mapper-rules-1")?
        .map(|v| v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if !enabled {
        return Ok(FilterMapperRules::disabled());
    }
    let Some(path) = md.get("dst-filter-mapper-rules-file-1")? else {
        return Ok(FilterMapperRules::disabled());
    };
    let p = std::path::PathBuf::from(path);
    if !p.exists() {
        return Ok(FilterMapperRules::disabled());
    }
    let allow_tables = md
        .get("dst-allow-unspecified-tables-1")?
        .map(|v| v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    let allow_cols = md
        .get("dst-allow-unspecified-columns-1")?
        .map(|v| v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    Ok(FilterMapperRules::parse_rules_file(&p, allow_tables, allow_cols)
        .unwrap_or_else(|_| FilterMapperRules::disabled()))
}

fn remove_dir_all_if_exists(dir: &Path) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    std::fs::remove_dir_all(dir).map_err(|e| {
        Error::Config(format!(
            "reinitialize: failed to remove {}: {e}",
            dir.display()
        ))
    })
}

fn clean_destination_for_device(
    snap: &Snapshot,
    source_db: &Path,
    clean_destination: bool,
) -> Result<()> {
    let Some(dst_type) = snap.dst_type.as_deref() else { return Ok(()); };
    let Some(conn_str) = snap.dst_conn_str.as_deref() else { return Ok(()); };
    let sync_mode = snap.dst_sync_mode.as_deref().unwrap_or("CONSOLIDATION");
    // CONSOLIDATION mode: the destination is shared across many devices,
    // so user-table drops are unsafe even if the caller asked for them.
    // Metadata-row deletes for this (uuid, name) remain safe.
    let drop_user = clean_destination && sync_mode.eq_ignore_ascii_case("REPLICATION");

    match dst_type {
        "SQLITE" => clean_sqlite(conn_str, snap, source_db, drop_user),
        "DUCKDB" => clean_duckdb(conn_str, snap, source_db, drop_user),
        "POSTGRES" | "POSTGRESQL" => clean_postgres(conn_str, snap, source_db, drop_user),
        other => Err(Error::Config(format!(
            "reinitialize: unsupported dst-type for cleanup: {other}"
        ))),
    }
}

/// Source-side fallback: enumerate user tables from the device's
/// source DB. Used only when neither the LOCAL metadata file nor the
/// destination has a `synclite_consolidator_table_metadata` to query
/// (e.g., reinit fired before the consolidator ever ran a cycle).
/// Streaming devices materialize nothing locally, so the list is
/// legitimately empty there.
fn collect_source_user_tables(db_path: &Path, device_type: &str) -> Result<Vec<String>> {
    let dt = device_type.trim().to_ascii_uppercase();
    let backend_is_duck = matches!(dt.as_str(), "DUCKDB" | "DUCKDB_STORE");
    if backend_is_duck {
        let conn = DuckConn::open(db_path)
            .map_err(|e| Error::Config(format!("reinitialize: open source duckdb: {e}")))?;
        let mut stmt = conn
            .prepare(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema='main' \
                   AND table_name NOT LIKE 'synclite\\_%' ESCAPE '\\'",
            )
            .map_err(|e| Error::Config(format!("reinitialize: list source tables: {e}")))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| Error::Config(format!("reinitialize: query source tables: {e}")))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| Error::Config(format!("reinitialize: row source tables: {e}")))?);
        }
        Ok(out)
    } else {
        let conn = SqlConn::open(db_path)
            .map_err(|e| Error::Config(format!("reinitialize: open source sqlite: {e}")))?;
        let mut stmt = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' \
                 AND name NOT LIKE 'synclite\\_%' ESCAPE '\\' \
                 AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'",
            )
            .map_err(|e| Error::Config(format!("reinitialize: list source tables: {e}")))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| Error::Config(format!("reinitialize: query source tables: {e}")))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| Error::Config(format!("reinitialize: row source tables: {e}")))?);
        }
        Ok(out)
    }
}

/// Apply the persisted `FilterMapperRules` to a list of source-side
/// table names from `synclite_consolidator_table_metadata`, yielding
/// the actual destination identifiers we need to drop. Blocked tables
/// (filter `false` with no `allow_unspecified_tables`) are dropped
/// from the list — they were never created on the destination.
fn map_to_dst_names(src_names: Vec<String>, rules: &FilterMapperRules) -> Vec<String> {
    let mut out = Vec::with_capacity(src_names.len());
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for name in src_names {
        if let Some(mapped) = rules.mapped_table_name(&name) {
            // Skip SyncLite-owned bookkeeping tables: they're cleared
            // by the metadata-row DELETEs / explicit DROP later.
            if mapped.to_ascii_lowercase().starts_with("synclite_") {
                continue;
            }
            if seen.insert(mapped.to_ascii_uppercase()) {
                out.push(mapped);
            }
        }
    }
    out
}

// ---- SQLite destination ---------------------------------------------------

fn clean_sqlite(
    conn_str: &str,
    snap: &Snapshot,
    source_db: &Path,
    drop_user: bool,
) -> Result<()> {
    retry_op(
        snap.retry_count,
        snap.retry_interval_ms,
        snap.dst_index,
        "reinit_clean_sqlite",
        || {
            let path = crate::parse_sqlite_path_from_connection(Some(conn_str))
                .unwrap_or_else(|| std::path::PathBuf::from(conn_str));
            let mut conn = SqlConn::open(&path)
                .map_err(|e| Error::Config(format!("reinitialize: open sqlite dst: {e}")))?;

            let dst_tables = if drop_user {
                resolve_dst_user_tables_sqlite(&conn, snap, source_db)?
            } else {
                Vec::new()
            };

            with_sqlite_tx(&mut conn, |c| {
                for t in &dst_tables {
                    drop_table_sqlite(c, t)?;
                }
                delete_by_device_sqlite(c, "synclite_consolidator_table_metadata", snap, true)?;
                delete_by_device_sqlite(c, "synclite_consolidator_metadata", snap, true)?;
                delete_by_device_sqlite_md(c, snap)?;
                drop_table_sqlite(c, "synclite_txn")?;
                Ok(())
            })
        },
    )
}

/// Resolve the device's destination user tables for SQLite backends.
/// Order of precedence: LOCAL consolidator metadata file → destination
/// `synclite_consolidator_table_metadata` → source-DB enumeration
/// fallback. The resolved list is then run through the FilterMapper
/// to translate source names into the actual destination identifiers.
fn resolve_dst_user_tables_sqlite(
    dst: &SqlConn,
    snap: &Snapshot,
    source_db: &Path,
) -> Result<Vec<String>> {
    if let Some(p) = snap.local_md_path.as_deref() {
        if let Ok(local) = SqlConn::open(p) {
            if let Ok(names) = query_owned_src_names_sqlite(&local, snap) {
                if !names.is_empty() {
                    return Ok(map_to_dst_names(names, &snap.filter_mapper));
                }
            }
        }
    }
    if table_exists_sqlite(dst, "synclite_consolidator_table_metadata") {
        let names = query_owned_src_names_sqlite(dst, snap)?;
        if !names.is_empty() {
            return Ok(map_to_dst_names(names, &snap.filter_mapper));
        }
    }
    // Last-resort fallback: enumerate the source DB. Useful when
    // reinit fires before the consolidator ever processed a segment.
    let src = collect_source_user_tables(source_db, &snap.device_type).unwrap_or_default();
    Ok(map_to_dst_names(src, &snap.filter_mapper))
}

fn query_owned_src_names_sqlite(conn: &SqlConn, snap: &Snapshot) -> Result<Vec<String>> {
    let sql = "SELECT DISTINCT table_name FROM synclite_consolidator_table_metadata \
               WHERE device_uuid=?1 AND device_name=?2 AND dst_index=?3";
    let mut stmt = conn.prepare(sql).map_err(|e| {
        Error::Config(format!("reinitialize: prepare owned-tables: {e}"))
    })?;
    let rows = stmt
        .query_map(
            rusqlite::params![&snap.uuid, &snap.device_name, snap.dst_index],
            |r| r.get::<_, String>(0),
        )
        .map_err(|e| Error::Config(format!("reinitialize: query owned-tables: {e}")))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r.map_err(|e| Error::Config(format!("reinitialize: row owned-tables: {e}")))?);
    }
    Ok(out)
}

/// SQLite supports `DROP TABLE` inside `BEGIN/COMMIT`. On any error
/// inside `body` we ROLLBACK so the destination remains untouched and
/// the operation is safe to retry.
fn with_sqlite_tx<F>(conn: &mut SqlConn, body: F) -> Result<()>
where
    F: FnOnce(&SqlConn) -> Result<()>,
{
    conn.execute_batch("BEGIN")
        .map_err(|e| Error::Config(format!("reinitialize: sqlite BEGIN: {e}")))?;
    match body(conn) {
        Ok(()) => conn
            .execute_batch("COMMIT")
            .map_err(|e| Error::Config(format!("reinitialize: sqlite COMMIT: {e}"))),
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

fn drop_table_sqlite(conn: &SqlConn, table: &str) -> Result<()> {
    let sql = format!("DROP TABLE IF EXISTS \"{}\"", table.replace('"', "\"\""));
    conn.execute_batch(&sql)
        .map_err(|e| Error::Config(format!("reinitialize: drop {table}: {e}")))?;
    Ok(())
}

fn table_exists_sqlite(conn: &SqlConn, table: &str) -> bool {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
        [table],
        |_| Ok(()),
    )
    .is_ok()
}

fn delete_by_device_sqlite(
    conn: &SqlConn,
    table: &str,
    snap: &Snapshot,
    with_dst_index: bool,
) -> Result<()> {
    if !table_exists_sqlite(conn, table) {
        return Ok(());
    }
    if with_dst_index {
        let sql = format!(
            "DELETE FROM {table} WHERE device_uuid=?1 AND device_name=?2 AND dst_index=?3"
        );
        conn.execute(
            &sql,
            rusqlite::params![&snap.uuid, &snap.device_name, snap.dst_index],
        )
        .map_err(|e| Error::Config(format!("reinitialize: delete from {table}: {e}")))?;
    } else {
        let sql = format!("DELETE FROM {table} WHERE device_uuid=?1 AND device_name=?2");
        conn.execute(&sql, [&snap.uuid, &snap.device_name])
            .map_err(|e| Error::Config(format!("reinitialize: delete from {table}: {e}")))?;
    }
    Ok(())
}

fn delete_by_device_sqlite_md(conn: &SqlConn, snap: &Snapshot) -> Result<()> {
    if !table_exists_sqlite(conn, "synclite_metadata") {
        return Ok(());
    }
    conn.execute(
        "DELETE FROM synclite_metadata WHERE synclite_device_id=?1 AND synclite_device_name=?2",
        [&snap.uuid, &snap.device_name],
    )
    .map_err(|e| Error::Config(format!("reinitialize: delete from synclite_metadata: {e}")))?;
    Ok(())
}

// ---- DuckDB destination ---------------------------------------------------

fn clean_duckdb(
    conn_str: &str,
    snap: &Snapshot,
    source_db: &Path,
    drop_user: bool,
) -> Result<()> {
    retry_op(
        snap.retry_count,
        snap.retry_interval_ms,
        snap.dst_index,
        "reinit_clean_duckdb",
        || {
            let path = crate::parse_duckdb_path_from_connection(Some(conn_str))
                .unwrap_or_else(|| std::path::PathBuf::from(conn_str));
            let conn = DuckConn::open(&path)
                .map_err(|e| Error::Config(format!("reinitialize: open duckdb dst: {e}")))?;

            let dst_tables = if drop_user {
                resolve_dst_user_tables_duck(&conn, snap, source_db)?
            } else {
                Vec::new()
            };

            with_duck_tx(&conn, |c| {
                for t in &dst_tables {
                    drop_table_duck(c, t)?;
                }
                delete_by_device_duck(c, "synclite_consolidator_table_metadata", snap, true)?;
                delete_by_device_duck(c, "synclite_consolidator_metadata", snap, true)?;
                delete_by_device_duck_md(c, snap)?;
                drop_table_duck(c, "synclite_txn")?;
                Ok(())
            })
        },
    )
}

fn resolve_dst_user_tables_duck(
    dst: &DuckConn,
    snap: &Snapshot,
    source_db: &Path,
) -> Result<Vec<String>> {
    // The LOCAL consolidator metadata DB is always SQLite, regardless
    // of destination backend.
    if let Some(p) = snap.local_md_path.as_deref() {
        if let Ok(local) = SqlConn::open(p) {
            if let Ok(names) = query_owned_src_names_sqlite(&local, snap) {
                if !names.is_empty() {
                    return Ok(map_to_dst_names(names, &snap.filter_mapper));
                }
            }
        }
    }
    if table_exists_duck(dst, "synclite_consolidator_table_metadata") {
        let names = query_owned_src_names_duck(dst, snap)?;
        if !names.is_empty() {
            return Ok(map_to_dst_names(names, &snap.filter_mapper));
        }
    }
    let src = collect_source_user_tables(source_db, &snap.device_type).unwrap_or_default();
    Ok(map_to_dst_names(src, &snap.filter_mapper))
}

fn query_owned_src_names_duck(conn: &DuckConn, snap: &Snapshot) -> Result<Vec<String>> {
    let sql = "SELECT DISTINCT table_name FROM synclite_consolidator_table_metadata \
               WHERE device_uuid=? AND device_name=? AND dst_index=?";
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| Error::Config(format!("reinitialize: duck prepare owned-tables: {e}")))?;
    let rows = stmt
        .query_map(
            duckdb::params![&snap.uuid, &snap.device_name, snap.dst_index],
            |r| r.get::<_, String>(0),
        )
        .map_err(|e| Error::Config(format!("reinitialize: duck query owned-tables: {e}")))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(
            r.map_err(|e| Error::Config(format!("reinitialize: duck row owned-tables: {e}")))?,
        );
    }
    Ok(out)
}

fn with_duck_tx<F>(conn: &DuckConn, body: F) -> Result<()>
where
    F: FnOnce(&DuckConn) -> Result<()>,
{
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Error::Config(format!("reinitialize: duckdb BEGIN: {e}")))?;
    match body(conn) {
        Ok(()) => conn
            .execute_batch("COMMIT")
            .map_err(|e| Error::Config(format!("reinitialize: duckdb COMMIT: {e}"))),
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

fn drop_table_duck(conn: &DuckConn, table: &str) -> Result<()> {
    let sql = format!("DROP TABLE IF EXISTS \"{}\"", table.replace('"', "\"\""));
    conn.execute_batch(&sql)
        .map_err(|e| Error::Config(format!("reinitialize: drop {table}: {e}")))?;
    Ok(())
}

fn table_exists_duck(conn: &DuckConn, table: &str) -> bool {
    let sql = format!(
        "SELECT 1 FROM information_schema.tables WHERE table_name = '{}' LIMIT 1",
        table.replace('\'', "''")
    );
    conn.query_row(&sql, [], |_| Ok::<(), duckdb::Error>(())).is_ok()
}

fn delete_by_device_duck(
    conn: &DuckConn,
    table: &str,
    snap: &Snapshot,
    with_dst_index: bool,
) -> Result<()> {
    if !table_exists_duck(conn, table) {
        return Ok(());
    }
    if with_dst_index {
        let sql = format!(
            "DELETE FROM {table} WHERE device_uuid=? AND device_name=? AND dst_index=?"
        );
        conn.execute(
            &sql,
            duckdb::params![&snap.uuid, &snap.device_name, snap.dst_index],
        )
        .map_err(|e| Error::Config(format!("reinitialize: delete from {table}: {e}")))?;
    } else {
        let sql = format!("DELETE FROM {table} WHERE device_uuid=? AND device_name=?");
        conn.execute(&sql, [&snap.uuid, &snap.device_name])
            .map_err(|e| Error::Config(format!("reinitialize: delete from {table}: {e}")))?;
    }
    Ok(())
}

fn delete_by_device_duck_md(conn: &DuckConn, snap: &Snapshot) -> Result<()> {
    if !table_exists_duck(conn, "synclite_metadata") {
        return Ok(());
    }
    conn.execute(
        "DELETE FROM synclite_metadata WHERE synclite_device_id=? AND synclite_device_name=?",
        [&snap.uuid, &snap.device_name],
    )
    .map_err(|e| Error::Config(format!("reinitialize: delete from synclite_metadata: {e}")))?;
    Ok(())
}

// ---- Postgres destination -------------------------------------------------

fn clean_postgres(
    conn_str: &str,
    snap: &Snapshot,
    source_db: &Path,
    drop_user: bool,
) -> Result<()> {
    retry_op(
        snap.retry_count,
        snap.retry_interval_ms,
        snap.dst_index,
        "reinit_clean_postgres",
        || {
            let translated = crate::translate_postgres_connection_string(conn_str).unwrap_or(conn_str);
            let mut client = PgClient::connect(translated.trim(), NoTls)
                .map_err(|e| Error::Config(format!("reinitialize: connect postgres: {e}")))?;
            let schema = snap.dst_schema.as_deref().unwrap_or("public");

            let dst_tables = if drop_user {
                resolve_dst_user_tables_postgres(&mut client, schema, snap, source_db)?
            } else {
                Vec::new()
            };

            with_pg_tx(&mut client, |c| {
                for t in &dst_tables {
                    pg_drop_table(c, schema, t)?;
                }
                pg_delete_by_device(c, schema, "synclite_consolidator_table_metadata", snap, true)?;
                pg_delete_by_device(c, schema, "synclite_consolidator_metadata", snap, true)?;
                pg_delete_by_device_md(c, schema, snap)?;
                pg_drop_table(c, schema, "synclite_txn")?;
                Ok(())
            })
        },
    )
}

fn resolve_dst_user_tables_postgres(
    client: &mut PgClient,
    schema: &str,
    snap: &Snapshot,
    source_db: &Path,
) -> Result<Vec<String>> {
    if let Some(p) = snap.local_md_path.as_deref() {
        if let Ok(local) = SqlConn::open(p) {
            if let Ok(names) = query_owned_src_names_sqlite(&local, snap) {
                if !names.is_empty() {
                    return Ok(map_to_dst_names(names, &snap.filter_mapper));
                }
            }
        }
    }
    if pg_table_exists(client, schema, "synclite_consolidator_table_metadata")? {
        let names = query_owned_src_names_pg(client, schema, snap)?;
        if !names.is_empty() {
            return Ok(map_to_dst_names(names, &snap.filter_mapper));
        }
    }
    let src = collect_source_user_tables(source_db, &snap.device_type).unwrap_or_default();
    Ok(map_to_dst_names(src, &snap.filter_mapper))
}

fn query_owned_src_names_pg(
    client: &mut PgClient,
    schema: &str,
    snap: &Snapshot,
) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT DISTINCT table_name FROM {}.{} \
         WHERE device_uuid=$1 AND device_name=$2 AND dst_index=$3",
        quote_pg_ident(schema),
        quote_pg_ident("synclite_consolidator_table_metadata")
    );
    let rows = client
        .query(
            sql.as_str(),
            &[&snap.uuid, &snap.device_name, &snap.dst_index],
        )
        .map_err(|e| Error::Config(format!("reinitialize: pg query owned-tables: {e}")))?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let name: String = r
            .try_get(0)
            .map_err(|e| Error::Config(format!("reinitialize: pg row owned-tables: {e}")))?;
        out.push(name);
    }
    Ok(out)
}

fn with_pg_tx<F>(client: &mut PgClient, body: F) -> Result<()>
where
    F: FnOnce(&mut PgClient) -> Result<()>,
{
    client
        .batch_execute("BEGIN")
        .map_err(|e| Error::Config(format!("reinitialize: postgres BEGIN: {e}")))?;
    match body(client) {
        Ok(()) => client
            .batch_execute("COMMIT")
            .map_err(|e| Error::Config(format!("reinitialize: postgres COMMIT: {e}"))),
        Err(e) => {
            let _ = client.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}

fn pg_drop_table(client: &mut PgClient, schema: &str, table: &str) -> Result<()> {
    let sql = format!(
        "DROP TABLE IF EXISTS {}.{}",
        quote_pg_ident(schema),
        quote_pg_ident(table)
    );
    client
        .batch_execute(&sql)
        .map_err(|e| Error::Config(format!("reinitialize: drop {table}: {e}")))?;
    Ok(())
}

fn pg_table_exists(client: &mut PgClient, schema: &str, table: &str) -> Result<bool> {
    let row = client
        .query_opt(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2",
            &[&schema, &table],
        )
        .map_err(|e| Error::Config(format!("reinitialize: pg table_exists({table}): {e}")))?;
    Ok(row.is_some())
}

fn pg_delete_by_device(
    client: &mut PgClient,
    schema: &str,
    table: &str,
    snap: &Snapshot,
    with_dst_index: bool,
) -> Result<()> {
    if !pg_table_exists(client, schema, table)? {
        return Ok(());
    }
    if with_dst_index {
        let sql = format!(
            "DELETE FROM {}.{} WHERE device_uuid=$1 AND device_name=$2 AND dst_index=$3",
            quote_pg_ident(schema),
            quote_pg_ident(table)
        );
        client
            .execute(
                sql.as_str(),
                &[&snap.uuid, &snap.device_name, &snap.dst_index],
            )
            .map_err(|e| Error::Config(format!("reinitialize: delete from {table}: {e}")))?;
    } else {
        let sql = format!(
            "DELETE FROM {}.{} WHERE device_uuid=$1 AND device_name=$2",
            quote_pg_ident(schema),
            quote_pg_ident(table)
        );
        client
            .execute(sql.as_str(), &[&snap.uuid, &snap.device_name])
            .map_err(|e| Error::Config(format!("reinitialize: delete from {table}: {e}")))?;
    }
    Ok(())
}

fn pg_delete_by_device_md(client: &mut PgClient, schema: &str, snap: &Snapshot) -> Result<()> {
    if !pg_table_exists(client, schema, "synclite_metadata")? {
        return Ok(());
    }
    let sql = format!(
        "DELETE FROM {}.{} WHERE synclite_device_id=$1 AND synclite_device_name=$2",
        quote_pg_ident(schema),
        quote_pg_ident("synclite_metadata")
    );
    client
        .execute(sql.as_str(), &[&snap.uuid, &snap.device_name])
        .map_err(|e| Error::Config(format!("reinitialize: delete from synclite_metadata: {e}")))?;
    Ok(())
}

fn quote_pg_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

