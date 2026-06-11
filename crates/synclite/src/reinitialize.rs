//! In-place device reinitialize.
//!
//! `reinitialize(db_path)` wipes every piece of per-device SyncLite
//! footprint — device home, stage subdir, work subdir, all segment
//! counters — and (when reachable) deletes this device's rows from
//! the destination metadata tables. The user's source DB file and
//! any destination user tables are left untouched.
//!
//! The device UUID, device-name, device-type and destination wiring
//! are preserved so the next `synclite::initialize` brings the device
//! back up as the *same logical device* but with a fresh initial
//! backup and segment sequence starting at 0. To keep the user's
//! configuration untouched, `reinitialize` does **not** mutate any
//! `dst-*` keys in the device metadata file. Instead it drops a
//! single sentinel file `<device_home>/.reinit` which the next
//! `synclite::initialize` consumes once — forcing
//! `dst-object-init-mode-1=OVERWRITE_OBJECT` for the post-reinit
//! re-seed only — and then deletes. In REPLICATION mode this drops
//! the destination tables and recreates them; in CONSOLIDATION mode
//! it truncates this device's rows on the shared destination, so the
//! re-seed never duplicates data.
//!
//! Trigger-file protocol: dropping a file named
//! `reinitialize.<device-name>` alongside the database file causes
//! the next `synclite::initialize` call to fire `reinitialize` before
//! bringing the logger up; the trigger file is removed on success.

use std::path::{Path, PathBuf};

use consolidator_core::retry_op;
use duckdb::Connection as DuckConn;
use logger_core::{Error, Result};
use postgres::{Client as PgClient, NoTls};
use rusqlite::Connection as SqlConn;

use crate::app_lock::AppLock;
use crate::layout::{ArchiveLayout, DeviceLayout};
use crate::metadata::Metadata;
use crate::{default_device_data_root, default_local_stage_dir, normalize_db_path};

/// Trigger-file basename for a reinit dropped alongside the device DB.
pub const TRIGGER: &str = "reinitialize";

/// Sentinel file dropped under the device home so the next
/// `synclite::initialize` knows to force
/// `dst-object-init-mode-1=OVERWRITE_OBJECT` for that one run only —
/// without persisting that flag into user-visible device metadata.
/// Consumed and deleted by `synclite::initialize`.
pub(crate) const REINIT_SENTINEL: &str = ".reinit";

/// Wipe local SyncLite state and (where applicable) this device's
/// metadata rows on the destination so the next `synclite::initialize`
/// re-seeds the device from scratch. See module documentation for
/// the exact contract.
pub fn reinitialize<P: AsRef<Path>>(db_path: P) -> Result<()> {
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

    // Destination metadata cleanup first: if it fails the local state
    // is still intact so the caller can retry once the destination is
    // reachable. We delete every row this device owns in the
    // consolidator metadata tables and in `synclite_checkpoint`; the
    // user's destination tables are not touched. Drop+recreate (or
    // truncate, in CONSOLIDATION mode) happens during the post-reinit
    // re-seed driven by the `.reinit` sentinel below.
    if snap.dst_type.is_some() && snap.dst_conn_str.is_some() {
        clean_destination_metadata(&snap)?;
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
    snap.write_minimal(&md, &layout)?;
    Ok(())
}

/// Trigger-file check called at the top of `synclite::initialize`.
/// Fires `reinitialize` then deletes the trigger file. Returns `Ok(())`
/// when no trigger is present.
pub(crate) fn maybe_run_trigger(db_path: &Path, device_name: &str) -> Result<()> {
    let parent = db_path.parent().unwrap_or_else(|| Path::new("."));
    let trigger = parent.join(format!("{TRIGGER}.{device_name}"));
    if trigger.exists() {
        reinitialize(db_path)?;
        let _ = std::fs::remove_file(&trigger);
    }
    Ok(())
}

struct Snapshot {
    uuid: String,
    device_name: String,
    device_type: String,
    database_name: String,
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
            dst_type: md.get("dst-type")?.map(|s| s.trim().to_ascii_uppercase()),
            dst_conn_str: md.get("dst-connection-string")?,
            dst_database: md.get("dst-database")?,
            dst_schema: md.get("dst-schema")?,
            dst_sync_mode: md.get("dst-sync-mode")?,
            dst_index,
            retry_count,
            retry_interval_ms,
        })
    }

    fn write_minimal(&self, md: &Metadata, layout: &DeviceLayout) -> Result<()> {
        md.put("uuid", &self.uuid)?;
        md.put("device_name", &self.device_name)?;
        if !self.device_type.is_empty() {
            md.put("device_type", &self.device_type)?;
        }
        md.put("database_name", &self.database_name)?;
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
        // Drop a sentinel file (no metadata mutation) so the next
        // `synclite::initialize` knows to force `OVERWRITE_OBJECT`
        // mode for the post-reinit re-seed only. Sentinel sits inside
        // `device_home`, which `reinitialize` recreated just before
        // this call, so it is naturally cleared on a follow-up reinit.
        let sentinel = layout.device_home.join(REINIT_SENTINEL);
        std::fs::write(&sentinel, b"").map_err(|e| {
            Error::Config(format!(
                "reinitialize: failed to write sentinel {}: {e}",
                sentinel.display()
            ))
        })?;
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

fn clean_destination_metadata(snap: &Snapshot) -> Result<()> {
    let Some(dst_type) = snap.dst_type.as_deref() else { return Ok(()); };
    let Some(conn_str) = snap.dst_conn_str.as_deref() else { return Ok(()); };
    match dst_type {
        "SQLITE" => clean_sqlite(conn_str, snap),
        "DUCKDB" => clean_duckdb(conn_str, snap),
        "POSTGRES" | "POSTGRESQL" => clean_postgres(conn_str, snap),
        other => Err(Error::Config(format!(
            "reinitialize: unsupported dst-type for cleanup: {other}"
        ))),
    }
}

// ---- SQLite destination ---------------------------------------------------

fn clean_sqlite(conn_str: &str, snap: &Snapshot) -> Result<()> {
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
            with_sqlite_tx(&mut conn, |c| {
                delete_by_device_sqlite(c, "synclite_consolidator_table_metadata", snap)?;
                delete_by_device_sqlite(c, "synclite_consolidator_metadata", snap)?;
                delete_by_device_sqlite_md(c, snap)?;
                drop_table_sqlite(c, "synclite_txn")?;
                Ok(())
            })
        },
    )
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
) -> Result<()> {
    if !table_exists_sqlite(conn, table) {
        return Ok(());
    }
    let sql = format!("DELETE FROM {table} WHERE device_uuid=?1 AND device_name=?2");
    conn.execute(&sql, [&snap.uuid, &snap.device_name])
        .map_err(|e| Error::Config(format!("reinitialize: delete from {table}: {e}")))?;
    Ok(())
}

fn delete_by_device_sqlite_md(conn: &SqlConn, snap: &Snapshot) -> Result<()> {
    if !table_exists_sqlite(conn, "synclite_checkpoint") {
        return Ok(());
    }
    conn.execute(
        "DELETE FROM synclite_checkpoint WHERE synclite_device_id=?1 AND synclite_device_name=?2",
        [&snap.uuid, &snap.device_name],
    )
    .map_err(|e| Error::Config(format!("reinitialize: delete from synclite_checkpoint: {e}")))?;
    Ok(())
}

// ---- DuckDB destination ---------------------------------------------------

fn clean_duckdb(conn_str: &str, snap: &Snapshot) -> Result<()> {
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
            with_duck_tx(&conn, |c| {
                delete_by_device_duck(c, "synclite_consolidator_table_metadata", snap)?;
                delete_by_device_duck(c, "synclite_consolidator_metadata", snap)?;
                delete_by_device_duck_md(c, snap)?;
                drop_table_duck(c, "synclite_txn")?;
                Ok(())
            })
        },
    )
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
) -> Result<()> {
    if !table_exists_duck(conn, table) {
        return Ok(());
    }
    let sql = format!("DELETE FROM {table} WHERE device_uuid=? AND device_name=?");
    conn.execute(&sql, [&snap.uuid, &snap.device_name])
        .map_err(|e| Error::Config(format!("reinitialize: delete from {table}: {e}")))?;
    Ok(())
}

fn delete_by_device_duck_md(conn: &DuckConn, snap: &Snapshot) -> Result<()> {
    if !table_exists_duck(conn, "synclite_checkpoint") {
        return Ok(());
    }
    conn.execute(
        "DELETE FROM synclite_checkpoint WHERE synclite_device_id=? AND synclite_device_name=?",
        [&snap.uuid, &snap.device_name],
    )
    .map_err(|e| Error::Config(format!("reinitialize: delete from synclite_checkpoint: {e}")))?;
    Ok(())
}

// ---- Postgres destination -------------------------------------------------

fn clean_postgres(conn_str: &str, snap: &Snapshot) -> Result<()> {
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
            with_pg_tx(&mut client, |c| {
                pg_delete_by_device(c, schema, "synclite_consolidator_table_metadata", snap)?;
                pg_delete_by_device(c, schema, "synclite_consolidator_metadata", snap)?;
                pg_delete_by_device_md(c, schema, snap)?;
                pg_drop_table(c, schema, "synclite_txn")?;
                Ok(())
            })
        },
    )
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
) -> Result<()> {
    if !pg_table_exists(client, schema, table)? {
        return Ok(());
    }
    let sql = format!(
        "DELETE FROM {}.{} WHERE device_uuid=$1 AND device_name=$2",
        quote_pg_ident(schema),
        quote_pg_ident(table)
    );
    client
        .execute(sql.as_str(), &[&snap.uuid, &snap.device_name])
        .map_err(|e| Error::Config(format!("reinitialize: delete from {table}: {e}")))?;
    Ok(())
}

fn pg_delete_by_device_md(client: &mut PgClient, schema: &str, snap: &Snapshot) -> Result<()> {
    if !pg_table_exists(client, schema, "synclite_checkpoint")? {
        return Ok(());
    }
    let sql = format!(
        "DELETE FROM {}.{} WHERE synclite_device_id=$1 AND synclite_device_name=$2",
        quote_pg_ident(schema),
        quote_pg_ident("synclite_checkpoint")
    );
    client
        .execute(sql.as_str(), &[&snap.uuid, &snap.device_name])
        .map_err(|e| Error::Config(format!("reinitialize: delete from synclite_checkpoint: {e}")))?;
    Ok(())
}

fn quote_pg_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
