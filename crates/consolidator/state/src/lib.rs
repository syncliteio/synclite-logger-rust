//! Consolidator state, bootstrap, and checkpoint helpers.
//!
//! This module owns the device-local state DB and stats DB pieces that are
//! shared across sync modes and destination backends.

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use duckdb::{params_from_iter as duck_params_from_iter, types::Value as DuckValue, Connection as DuckConnection};
use postgres::{Client as PgClient, NoTls};
use rusqlite::{params, Connection, OptionalExtension};
use consolidator_core::{ApplyProgress, ConsolidatorLayout, DstType, DestinationSyncMode, MetadataStore, retry_dst};
use logger_core::{Error, Result};

const DEFAULT_DST_ALIAS: &str = "DB-1";

/// Key under which the on-disk metadata schema version is stored — both
/// in the local `metadata` key/value table and in
/// `synclite_consolidator_metadata` (LOCAL state DB and DESTINATION bookkeeping).
pub const SYNCLITE_METADATA_VERSION_KEY: &str = "synclite_metadata_version";

/// Current consolidator-side metadata schema/semantics version. Bump in
/// lockstep with any migration routine so a future consolidator version
/// can detect an older store and upgrade it on initialization.
pub const SYNCLITE_METADATA_VERSION: i64 = 1;

pub fn initialize_state_db(path: &Path) -> Result<()> {
    let conn = Connection::open(path).map_err(map_sql_err)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS synclite_checkpoint(\n\
             commit_id LONG NOT NULL PRIMARY KEY,\n\
             cdc_change_number LONG NOT NULL,\n\
             cdc_log_segment_sequence_number LONG NOT NULL,\n\
             initialization_status LONG NOT NULL DEFAULT 0,\n\
             txn_count LONG NOT NULL\n\
         );\n\
         CREATE TABLE IF NOT EXISTS schema(\n\
             database_name TEXT,\n\
             schema_name TEXT,\n\
             table_name TEXT,\n\
             column_index LONG,\n\
             column_name TEXT,\n\
             column_type TEXT,\n\
             column_not_null INTEGER,\n\
             column_default_value BLOB,\n\
             column_primary_key INTEGER,\n\
             column_auto_increment INTEGER\n\
         );\n\
         CREATE TABLE IF NOT EXISTS table_metadata(\n\
             database_name TEXT,\n\
             schema_name TEXT,\n\
             table_name TEXT,\n\
             key TEXT,\n\
             value TEXT\n\
         );\n\
         CREATE TABLE IF NOT EXISTS metadata(\n\
             key TEXT,\n\
             value TEXT\n\
         );\n\
         CREATE TABLE IF NOT EXISTS synclite_consolidator_metadata(\n\
             synclite_device_id VARCHAR(64) NOT NULL,\n\
             synclite_device_name VARCHAR(255) NOT NULL,\n\
             synclite_update_timestamp TEXT,\n\
             prop_key VARCHAR(255) NOT NULL,\n\
             prop_value TEXT,\n\
             PRIMARY KEY(synclite_device_id, synclite_device_name, prop_key)\n\
         );\n\
         CREATE TABLE IF NOT EXISTS synclite_consolidator_table_metadata(\n\
             synclite_device_id VARCHAR(64) NOT NULL,\n\
             synclite_device_name VARCHAR(255) NOT NULL,\n\
             synclite_update_timestamp TEXT,\n\
             database_name VARCHAR(255) NOT NULL,\n\
             table_name VARCHAR(255) NOT NULL,\n\
             prop_key VARCHAR(255) NOT NULL,\n\
             prop_value TEXT,\n\
             PRIMARY KEY(synclite_device_id, synclite_device_name, database_name, table_name, prop_key)\n\
         );",
    )
    .map_err(map_sql_err)?;

    // Seed metadata-schema version row if absent. The `metadata` table has
    // no PRIMARY KEY in this layout (Java parity) so we cannot use
    // `INSERT OR IGNORE`; do an explicit SELECT-then-INSERT.
    let has_version: bool = conn
        .query_row(
            "SELECT 1 FROM metadata WHERE key = ?1 LIMIT 1",
            params![SYNCLITE_METADATA_VERSION_KEY],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(map_sql_err)?
        .is_some();
    if !has_version {
        conn.execute(
            "INSERT INTO metadata(key, value) VALUES(?1, ?2)",
            params![
                SYNCLITE_METADATA_VERSION_KEY,
                SYNCLITE_METADATA_VERSION.to_string()
            ],
        )
        .map_err(map_sql_err)?;
    }
    Ok(())
}

pub fn initialize_device_stats_db(layout: &ConsolidatorLayout) -> Result<()> {
    let conn = Connection::open(&layout.stats_db_path).map_err(map_sql_err)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS checkpoint(\n\
             dst_alias TEXT PRIMARY KEY,\n\
             cdc_log_segment_sequence_number LONG,\n\
             is_initialization_stats_collected LONG,\n\
             processed_oper_count LONG,\n\
             processed_txn_count LONG,\n\
             processed_log_size LONG\n\
         );\n\
         CREATE TABLE IF NOT EXISTS table_statistics(\n\
             dst_alias TEXT,\n\
             database_name TEXT,\n\
             schema_name TEXT,\n\
             table_name TEXT,\n\
             initial_rows LONG,\n\
             insert_rows LONG,\n\
             update_rows LONG,\n\
             delete_rows LONG,\n\
             add_column LONG,\n\
             drop_column LONG,\n\
             rename_column LONG,\n\
             create_table LONG,\n\
             drop_table LONG,\n\
             rename_table LONG,\n\
             PRIMARY KEY(dst_alias, database_name, schema_name, table_name)\n\
         );",
    )
    .map_err(map_sql_err)?;
    conn.execute(
        "INSERT INTO checkpoint(dst_alias, cdc_log_segment_sequence_number, is_initialization_stats_collected, processed_oper_count, processed_txn_count, processed_log_size)\n\
         SELECT ?1, -1, 0, 0, 0, 0\n\
         WHERE NOT EXISTS (SELECT 1 FROM checkpoint WHERE dst_alias=?1)",
        params![DEFAULT_DST_ALIAS],
    )
    .map_err(map_sql_err)?;
    conn.execute(
        "UPDATE checkpoint SET dst_alias=?1 WHERE dst_alias='DEFAULT'",
        params![DEFAULT_DST_ALIAS],
    )
    .map_err(map_sql_err)?;
    Ok(())
}

fn destination_backend_name(layout: &ConsolidatorLayout) -> &'static str {
    match layout.dst_type {
        DstType::Sqlite => "SQLite",
        DstType::DuckDb => "DuckDB",
        DstType::Postgres => "PostgreSQL",
    }
}

/// Postgres-only: schema-qualify a system metadata table name when
/// `dst_schema` is set, so metadata always lands in the configured schema.
fn pg_meta_table(layout: &ConsolidatorLayout, name: &str) -> String {
    if let Some(schema) = layout.dst_schema.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        let q_schema = schema.replace('"', "\"\"");
        let q_name = name.replace('"', "\"\"");
        return format!("\"{q_schema}\".\"{q_name}\"");
    }
    name.to_string()
}

/// Postgres-only: double-quote an identifier, escaping any embedded `"`.
/// Used when we need to emit a bare quoted identifier (e.g. for
/// `CREATE SCHEMA IF NOT EXISTS <ident>`).
fn pg_quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

/// Returns the PG schema to use when filtering `information_schema` queries,
/// matching the same logic used to qualify metadata tables.
fn pg_meta_schema(layout: &ConsolidatorLayout) -> Option<String> {
    layout
        .dst_schema
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn pg_create_dst_prop_table_sql(layout: &ConsolidatorLayout) -> String {
    let tbl = pg_meta_table(layout, "synclite_consolidator_metadata");
    format!(
        "CREATE TABLE IF NOT EXISTS {tbl}(\
         synclite_device_id VARCHAR(64) NOT NULL,\
         synclite_device_name VARCHAR(255) NOT NULL,\
         synclite_update_timestamp TEXT,\
         prop_key VARCHAR(255) NOT NULL,\
         prop_value TEXT,\
         PRIMARY KEY(synclite_device_id, synclite_device_name, prop_key))"
    )
}

fn pg_create_dst_table_metadata_sql(layout: &ConsolidatorLayout) -> String {
    let tbl = pg_meta_table(layout, "synclite_consolidator_table_metadata");
    format!(
        "CREATE TABLE IF NOT EXISTS {tbl}(\
         synclite_device_id VARCHAR(64) NOT NULL,\
         synclite_device_name VARCHAR(255) NOT NULL,\
         synclite_update_timestamp TEXT,\
         database_name VARCHAR(255) NOT NULL,\
         table_name VARCHAR(255) NOT NULL,\
         prop_key VARCHAR(255) NOT NULL,\
         prop_value TEXT,\
         PRIMARY KEY(synclite_device_id, synclite_device_name, database_name, table_name, prop_key))"
    )
}

fn pg_create_dst_checkpoint_sql(layout: &ConsolidatorLayout) -> String {
    let tbl = pg_meta_table(layout, "synclite_checkpoint");
    format!(
        "CREATE TABLE IF NOT EXISTS {tbl}(\
         synclite_device_id TEXT NOT NULL,\
         synclite_device_name TEXT NOT NULL,\
         synclite_update_timestamp TEXT,\
         commit_id BIGINT NOT NULL,\
         cdc_change_number BIGINT NOT NULL,\
         cdc_log_segment_sequence_number BIGINT NOT NULL,\
         initialization_status INTEGER NOT NULL DEFAULT 0,\
         txn_count BIGINT NOT NULL,\
         PRIMARY KEY(synclite_device_id, synclite_device_name, commit_id))"
    )
}

static DESTINATION_METADATA_BOOTSTRAP_GUARD: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn destination_metadata_bootstrap_key(layout: &ConsolidatorLayout, dst_index: i32) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        format!("{:?}", layout.dst_type),
        dst_index,
        layout.dst_connection_string,
        layout.dst_database.as_deref().unwrap_or(""),
        layout.dst_schema.as_deref().unwrap_or("")
    )
}

fn has_destination_metadata_bootstrapped(key: &str) -> bool {
    DESTINATION_METADATA_BOOTSTRAP_GUARD
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .map(|g| g.contains(key))
        .unwrap_or(false)
}

fn mark_destination_metadata_bootstrapped(key: String) {
    if let Ok(mut guard) = DESTINATION_METADATA_BOOTSTRAP_GUARD
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
    {
        guard.insert(key);
    }
}

/// One-time destination metadata-table bootstrap for DESTINATION metadata mode.
///
/// This is intentionally idempotent and process-local guarded so each device
/// does not repeatedly issue table-create DDL during steady-state metadata reads/writes.
pub fn bootstrap_destination_metadata(layout: &ConsolidatorLayout, dst_index: i32) -> Result<()> {
    if layout.metadata_store != MetadataStore::Destination {
        return Ok(());
    }

    let key = destination_metadata_bootstrap_key(layout, dst_index);
    if has_destination_metadata_bootstrapped(&key) {
        tracing::debug!(
            dst_index = dst_index,
            dst_type = ?layout.dst_type,
            "[BOOT] Destination metadata tables already bootstrapped, skipping"
        );
        return Ok(());
    }

    tracing::debug!(
        dst_index = dst_index,
        dst_type = ?layout.dst_type,
        "[BOOT] Ensuring destination metadata tables exist"
    );

    match layout.dst_type {
        DstType::Sqlite => {
            let conn = Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
            conn.execute_batch(CREATE_DST_PROP_TBL_SQL).map_err(map_sql_err)?;
            conn.execute_batch(CREATE_DST_TABLE_METADATA_TBL_SQL)
                .map_err(map_sql_err)?;
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS synclite_checkpoint(\n\
                 synclite_device_id TEXT NOT NULL,\n\
                 synclite_device_name TEXT NOT NULL,\n\
                 synclite_update_timestamp TEXT,\n\
                 commit_id BIGINT NOT NULL,\n\
                 cdc_change_number BIGINT NOT NULL,\n\
                 cdc_log_segment_sequence_number BIGINT NOT NULL,\n\
                 initialization_status INTEGER NOT NULL DEFAULT 0,\n\
                 txn_count BIGINT NOT NULL,\n\
                 PRIMARY KEY(synclite_device_id, synclite_device_name, commit_id));",
            )
            .map_err(map_sql_err)?;
        }
        DstType::DuckDb => {
            let conn = DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
            conn.execute_batch(CREATE_DST_PROP_TBL_SQL).map_err(map_duck_err)?;
            conn.execute_batch(CREATE_DST_TABLE_METADATA_TBL_SQL)
                .map_err(map_duck_err)?;
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS synclite_checkpoint(\n\
                 synclite_device_id TEXT NOT NULL,\n\
                 synclite_device_name TEXT NOT NULL,\n\
                 synclite_update_timestamp TEXT,\n\
                 commit_id BIGINT NOT NULL,\n\
                 cdc_change_number BIGINT NOT NULL,\n\
                 cdc_log_segment_sequence_number BIGINT NOT NULL,\n\
                 initialization_status INTEGER NOT NULL DEFAULT 0,\n\
                 txn_count BIGINT NOT NULL,\n\
                 PRIMARY KEY(synclite_device_id, synclite_device_name, commit_id));",
            )
            .map_err(map_duck_err)?;
        }
        DstType::Postgres => {
            let mut client = PgClient::connect(&layout.dst_connection_string, NoTls).map_err(map_pg_err)?;
            // The consolidator metadata tables (and later, the user
            // tables) live in dst_schema when one is configured. Pre-
            // ensure it now so the very first CREATE TABLE below doesn't
            // fail with `3F000 schema does not exist`. Mirrors the
            // ensure_pg_dst_schema call in apply_to_postgres_destination,
            // which would otherwise only run after bootstrap.
            if let Some(schema) = pg_meta_schema(layout) {
                let create_schema_sql =
                    format!("CREATE SCHEMA IF NOT EXISTS {}", pg_quote_ident(&schema));
                client
                    .batch_execute(create_schema_sql.as_str())
                    .map_err(|e| map_pg_err_with_sql(e, &create_schema_sql))?;
            }
            let create_prop_sql = pg_create_dst_prop_table_sql(layout);
            client
                .batch_execute(create_prop_sql.as_str())
                .map_err(|e| map_pg_err_with_sql(e, &create_prop_sql))?;
            let create_tblmeta_sql = pg_create_dst_table_metadata_sql(layout);
            client
                .batch_execute(create_tblmeta_sql.as_str())
                .map_err(|e| map_pg_err_with_sql(e, &create_tblmeta_sql))?;
            let create_checkpoint_sql = pg_create_dst_checkpoint_sql(layout);
            client
                .batch_execute(create_checkpoint_sql.as_str())
                .map_err(|e| map_pg_err_with_sql(e, &create_checkpoint_sql))?;
        }
    }

    mark_destination_metadata_bootstrapped(key);
    tracing::info!(
        dst_index = dst_index,
        dst_type = ?layout.dst_type,
        "[BOOT] Destination metadata tables ensured"
    );
    Ok(())
}

pub fn initialize_consolidator_stats_db(layout: &ConsolidatorLayout) -> Result<()> {
    let conn = Connection::open(&layout.consolidator_stats_db_path).map_err(map_sql_err)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS dashboard(\n\
             header TEXT,\n\
             detected_devices LONG,\n\
             registered_devices LONG,\n\
             initialized_devices LONG,\n\
             failed_devices LONG,\n\
             total_device_initializations LONG,\n\
             total_device_resynchronizations LONG,\n\
             total_consolidated_tables LONG,\n\
             total_log_segments_applied LONG,\n\
             total_processed_log_size LONG,\n\
             total_processed_oper_count LONG,\n\
             total_processed_txn_count LONG,\n\
             latency LONG,\n\
             last_heartbeat_time LONG,\n\
             last_job_start_time LONG\n\
         );\n\
         CREATE TABLE IF NOT EXISTS device_status(\n\
             synclite_device_id TEXT,\n\
             synclite_device_name TEXT,\n\
             synclite_device_type TEXT,\n\
             status TEXT,\n\
             status_description TEXT,\n\
             path TEXT,\n\
             database_name TEXT,\n\
             destination_database_alias TEXT,\n\
             log_segments_applied LONG,\n\
             processed_log_size LONG,\n\
             processed_oper_count LONG,\n\
             processed_txn_count LONG,\n\
             latency LONG,\n\
             last_heartbeat_time LONG,\n\
             last_consolidated_commit_id LONG,\n\
             PRIMARY KEY(synclite_device_id, synclite_device_name)\n\
         );",
    )
    .map_err(map_sql_err)?;
    let now = now_epoch_ms();
    conn.execute(
        "INSERT INTO dashboard(\n\
             header, detected_devices, registered_devices, initialized_devices, failed_devices,\n\
             total_device_initializations, total_device_resynchronizations, total_consolidated_tables,\n\
             total_log_segments_applied, total_processed_log_size, total_processed_oper_count,\n\
             total_processed_txn_count, latency, last_heartbeat_time, last_job_start_time\n\
         )\n\
         SELECT ?1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?2, ?2\n\
         WHERE NOT EXISTS (SELECT 1 FROM dashboard)",
        params![
            format!(
                "Consolidating devices from data directory {} into {} ({})...",
                layout.work_dir.to_string_lossy(),
                DEFAULT_DST_ALIAS,
                destination_backend_name(layout)
            ),
            now,
        ],
    )
    .map_err(map_sql_err)?;
    upsert_device_status(&conn, layout, "SYNCING", "event consolidator active")?;
    Ok(())
}

pub fn record_bootstrap(
    state_conn: &Connection,
    device_stats_conn: &Connection,
    backup_path: &Path,
    metadata_path: &Path,
) -> Result<()> {
    let _ = state_conn;
    let _ = backup_path;
    let _ = metadata_path;
    put_metric_i64(device_stats_conn, "bootstrap_ready", 1)?;
    Ok(())
}

pub fn initialize_from_backup(
    layout: &ConsolidatorLayout,
    state_conn: &Connection,
    device_stats_conn: &Connection,
    consolidator_stats_conn: &Connection,
    backup_path: &Path,
    metadata_path: &Path,
) -> Result<bool> {
    if is_initialized(state_conn)? {
        return Ok(true);
    }

    if !backup_path.exists() {
        return Ok(false);
    }

    ensure_synclite_checkpoint_seeded(state_conn, backup_path)?;

    let _ = metadata_path;

    put_metric_i64(device_stats_conn, "destination_initialized", 1)?;
    update_dashboard(consolidator_stats_conn, "initialized_devices", 1)?;
    update_dashboard(consolidator_stats_conn, "total_device_initializations", 1)?;
    update_dashboard(consolidator_stats_conn, "last_heartbeat_time", now_epoch_ms())?;
    update_device_status_commit(
        consolidator_stats_conn,
        device_stats_conn,
        layout,
        "SYNCING",
        "initialized from backup",
        get_commit_id(state_conn)?,
        0,
    )?;

    if let Ok(meta) = std::fs::metadata(backup_path) {
        put_metric_i64(device_stats_conn, "initialization_backup_size_bytes", meta.len() as i64)?;
        update_dashboard(consolidator_stats_conn, "total_processed_log_size", meta.len() as i64)?;
    }

    Ok(true)
}

pub fn is_initialized(state_conn: &Connection) -> Result<bool> {
    Ok(state_conn
        .query_row(
            "SELECT commit_id FROM synclite_checkpoint LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|_| true)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(false),
            _ => Err(e),
        })
        .map_err(map_sql_err)?)
}

pub fn record_stage_path<F>(
    layout: &ConsolidatorLayout,
    state_conn: &Connection,
    device_stats_conn: &Connection,
    consolidator_stats_conn: &Connection,
    path: &Path,
    apply_segment: F,
) -> Result<()>
where
    F: Fn(&ConsolidatorLayout, &Path) -> Result<ApplyProgress>,
{
    let heartbeat_now = now_epoch_ms();
    let latency_ms = compute_segment_latency_ms(path, heartbeat_now);

    let mut apply_progress: Option<ApplyProgress> = None;
    if layout.destination_apply_enabled {
        apply_progress = Some(apply_segment(layout, path)?);
        update_table_statistics_from_segment(layout, device_stats_conn, path)?;
    }

    record_event(state_conn, "stage-path-ready", path)?;

    let oper_inc = apply_progress.map(|p| p.applied_txn_cnt.max(0)).unwrap_or(0);
    let txn_inc = if oper_inc > 0 { 1 } else { 0 };
    let log_size_inc = std::fs::metadata(path).map(|m| m.len() as i64).unwrap_or(0);
    let updated_rows = device_stats_conn
        .execute(
            "UPDATE checkpoint\n\
             SET processed_oper_count = processed_oper_count + ?1,\n\
                 processed_txn_count = processed_txn_count + ?2,\n\
                 processed_log_size = processed_log_size + ?3\n\
             WHERE dst_alias = ?4",
            params![oper_inc, txn_inc, log_size_inc, DEFAULT_DST_ALIAS],
        )
        .map_err(map_sql_err)?;
    let _ = updated_rows;

    let total_segments = get_dashboard_counter(consolidator_stats_conn, "total_log_segments_applied")? + 1;
    let total_oper = get_dashboard_counter(consolidator_stats_conn, "total_processed_oper_count")? + oper_inc;
    let total_txn = get_dashboard_counter(consolidator_stats_conn, "total_processed_txn_count")? + txn_inc;
    let total_size = get_dashboard_counter(consolidator_stats_conn, "total_processed_log_size")? + log_size_inc;
    update_dashboard(consolidator_stats_conn, "total_log_segments_applied", total_segments)?;
    update_dashboard(consolidator_stats_conn, "total_processed_oper_count", total_oper)?;
    update_dashboard(consolidator_stats_conn, "total_processed_txn_count", total_txn)?;
    update_dashboard(consolidator_stats_conn, "total_processed_log_size", total_size)?;
    update_dashboard(consolidator_stats_conn, "latency", latency_ms)?;
    update_dashboard(consolidator_stats_conn, "last_heartbeat_time", heartbeat_now)?;

    if let Some(seq) = parse_segment_seq(path) {
        put_metric_i64(device_stats_conn, "last_segment_sequence", seq as i64)?;
        if let Some(progress) = apply_progress {
            // Keep local progress metadata current for status/reporting parity,
            // irrespective of destination metadata-store mode.
            map_local_checkpoint(state_conn, seq as i64, progress)?;
        } else {
            update_synclite_checkpoint_for_segment(state_conn, seq as i64)?;
        }
    }

    let commit_id = get_commit_id(state_conn)?;
    update_device_status_commit(
        consolidator_stats_conn,
        device_stats_conn,
        layout,
        "SYNCING",
        "processing staged segments",
        commit_id,
        latency_ms,
    )?;

    Ok(())
}

pub fn ensure_synclite_checkpoint_seeded(state_conn: &Connection, backup_db_path: &Path) -> Result<()> {
    let existing = state_conn.query_row(
        "SELECT commit_id FROM synclite_checkpoint LIMIT 1",
        [],
        |row| row.get::<_, i64>(0),
    );
    if existing.is_ok() {
        return Ok(());
    }
    if let Err(e) = existing {
        if !matches!(e, rusqlite::Error::QueryReturnedNoRows) {
            return Err(map_sql_err(e));
        }
    }

    let backup_conn = Connection::open(backup_db_path).map_err(map_sql_err)?;
    let first_commit_id = backup_conn
        .query_row("SELECT commit_id FROM synclite_txn", [], |row| row.get::<_, i64>(0))
        .unwrap_or(0);

    state_conn
        .execute(
            "INSERT INTO synclite_checkpoint(\n\
                 commit_id, cdc_change_number,\n\
                 cdc_log_segment_sequence_number, txn_count\n\
             ) VALUES(?1, -1, 0, 0)",
            params![first_commit_id],
        )
        .map_err(map_sql_err)?;
    Ok(())
}

pub fn update_synclite_checkpoint_for_segment(state_conn: &Connection, seq: i64) -> Result<()> {
    state_conn
        .execute(
            "UPDATE synclite_checkpoint\n\
             SET cdc_log_segment_sequence_number = CASE\n\
                     WHEN cdc_log_segment_sequence_number < ?1 THEN ?1\n\
                     ELSE cdc_log_segment_sequence_number\n\
                 END",
            params![seq],
        )
        .map_err(map_sql_err)?;
    Ok(())
}

pub fn get_commit_id(state_conn: &Connection) -> Result<i64> {
    let commit_id = state_conn
        .query_row("SELECT commit_id FROM synclite_checkpoint LIMIT 1", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap_or(0);
    Ok(commit_id)
}

pub fn upsert_device_status(conn: &Connection, layout: &ConsolidatorLayout, status: &str, status_description: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO device_status(\n\
             synclite_device_id, synclite_device_name, synclite_device_type, status, status_description,\n\
             path, database_name, destination_database_alias, log_segments_applied, processed_log_size,\n\
             processed_oper_count, processed_txn_count, latency, last_heartbeat_time, last_consolidated_commit_id\n\
         ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, '', 0, 0, 0, 0, 0, ?8, 0)\n\
         ON CONFLICT(synclite_device_id, synclite_device_name) DO UPDATE SET\n\
             synclite_device_type=excluded.synclite_device_type,\n\
             status=excluded.status,\n\
             status_description=excluded.status_description,\n\
             path=excluded.path,\n\
             database_name=excluded.database_name,\n\
             last_heartbeat_time=excluded.last_heartbeat_time",
        params![
            layout.device_id,
            layout.device_name,
            layout.device_type,
            status,
            status_description,
            layout.work_dir.to_string_lossy().as_ref(),
            layout.database_name,
            now_epoch_ms(),
        ],
    )
    .map_err(map_sql_err)?;

    Ok(())
}

pub fn update_device_status_commit(
    conn: &Connection,
    device_stats_conn: &Connection,
    layout: &ConsolidatorLayout,
    status: &str,
    status_description: &str,
    commit_id: i64,
    latency_ms: i64,
) -> Result<()> {
    let segments = get_dashboard_counter(conn, "total_log_segments_applied")?;
    let processed_size = device_stats_conn
        .query_row(
            "SELECT processed_log_size FROM checkpoint WHERE dst_alias = ?1",
            params![DEFAULT_DST_ALIAS],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let processed_oper_count = device_stats_conn
        .query_row(
            "SELECT processed_oper_count FROM checkpoint WHERE dst_alias = ?1",
            params![DEFAULT_DST_ALIAS],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let processed_txn_count = device_stats_conn
        .query_row(
            "SELECT processed_txn_count FROM checkpoint WHERE dst_alias = ?1",
            params![DEFAULT_DST_ALIAS],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    conn.execute(
        "UPDATE device_status\n\
         SET status = ?3, status_description = ?4, log_segments_applied = ?5,\n\
             processed_log_size = ?6, processed_oper_count = ?7,\n\
             processed_txn_count = ?8, latency = ?9, last_heartbeat_time = ?10, last_consolidated_commit_id = ?11\n\
         WHERE synclite_device_id = ?1 AND synclite_device_name = ?2",
        params![
            layout.device_id,
            layout.device_name,
            status,
            status_description,
            segments,
            processed_size,
            processed_oper_count,
            processed_txn_count,
            latency_ms,
            now_epoch_ms(),
            commit_id,
        ],
    )
    .map_err(map_sql_err)?;
    Ok(())
}

pub fn update_dashboard(conn: &Connection, field: &str, value: i64) -> Result<()> {
    let sql = match field {
        "initialized_devices" => "UPDATE dashboard SET initialized_devices = ?1",
        "total_device_initializations" => "UPDATE dashboard SET total_device_initializations = ?1",
        "total_log_segments_applied" => "UPDATE dashboard SET total_log_segments_applied = ?1",
        "total_processed_log_size" => "UPDATE dashboard SET total_processed_log_size = ?1",
        "total_processed_oper_count" => "UPDATE dashboard SET total_processed_oper_count = ?1",
        "total_processed_txn_count" => "UPDATE dashboard SET total_processed_txn_count = ?1",
        "latency" => "UPDATE dashboard SET latency = ?1",
        "last_heartbeat_time" => "UPDATE dashboard SET last_heartbeat_time = ?1",
        _ => return Ok(()),
    };
    conn.execute(sql, params![value]).map_err(map_sql_err)?;
    Ok(())
}

fn get_dashboard_counter(conn: &Connection, field: &str) -> Result<i64> {
    let sql = match field {
        "total_log_segments_applied" => "SELECT total_log_segments_applied FROM dashboard LIMIT 1",
        "total_processed_log_size" => "SELECT total_processed_log_size FROM dashboard LIMIT 1",
        "total_processed_oper_count" => "SELECT total_processed_oper_count FROM dashboard LIMIT 1",
        "total_processed_txn_count" => "SELECT total_processed_txn_count FROM dashboard LIMIT 1",
        _ => return Ok(0),
    };
    let value = conn.query_row(sql, [], |row| row.get::<_, i64>(0));
    match value {
        Ok(v) => Ok(v),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
        Err(e) => Err(map_sql_err(e)),
    }
}

fn compute_segment_latency_ms(path: &Path, now_ms: i64) -> i64 {
    let modified_ms = std::fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(now_ms);
    (now_ms - modified_ms).max(0)
}

fn has_table(conn: &Connection, table_name: &str) -> Result<bool> {
    let found = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1 LIMIT 1",
            [table_name],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(map_sql_err)?;
    Ok(found.is_some())
}

fn update_table_statistics_from_segment(
    layout: &ConsolidatorLayout,
    device_stats_conn: &Connection,
    segment_path: &Path,
) -> Result<()> {
    let seg_conn = Connection::open(segment_path).map_err(map_sql_err)?;
    let sql_select = if has_table(&seg_conn, "commandlog")? {
        "SELECT sql FROM commandlog ORDER BY change_number"
    } else if has_table(&seg_conn, "cdclog")? {
        "SELECT sql FROM cdclog ORDER BY change_number"
    } else {
        return Ok(());
    };

    let mut stmt = seg_conn.prepare(sql_select).map_err(map_sql_err)?;
    let mut rows = stmt.query([]).map_err(map_sql_err)?;

    let mut last_sql: Option<String> = None;
    while let Some(row) = rows.next().map_err(map_sql_err)? {
        let sql_opt: Option<String> = row.get(0).map_err(map_sql_err)?;
        let sql = match sql_opt {
            Some(s) => {
                last_sql = Some(s.clone());
                s
            }
            None => match &last_sql {
                Some(s) => s.clone(),
                None => continue,
            },
        };

        let Some((column_name, table_name)) = classify_table_stat_update(layout, &sql) else {
            continue;
        };

        let update_sql = match column_name {
            "insert_rows" => {
                "UPDATE table_statistics SET insert_rows = insert_rows + 1 WHERE dst_alias = ?1 AND database_name = ?2 AND schema_name = ?3 AND table_name = ?4"
            }
            "update_rows" => {
                "UPDATE table_statistics SET update_rows = update_rows + 1 WHERE dst_alias = ?1 AND database_name = ?2 AND schema_name = ?3 AND table_name = ?4"
            }
            "delete_rows" => {
                "UPDATE table_statistics SET delete_rows = delete_rows + 1 WHERE dst_alias = ?1 AND database_name = ?2 AND schema_name = ?3 AND table_name = ?4"
            }
            "create_table" => {
                "UPDATE table_statistics SET create_table = create_table + 1 WHERE dst_alias = ?1 AND database_name = ?2 AND schema_name = ?3 AND table_name = ?4"
            }
            "drop_table" => {
                "UPDATE table_statistics SET drop_table = drop_table + 1 WHERE dst_alias = ?1 AND database_name = ?2 AND schema_name = ?3 AND table_name = ?4"
            }
            "add_column" => {
                "UPDATE table_statistics SET add_column = add_column + 1 WHERE dst_alias = ?1 AND database_name = ?2 AND schema_name = ?3 AND table_name = ?4"
            }
            "drop_column" => {
                "UPDATE table_statistics SET drop_column = drop_column + 1 WHERE dst_alias = ?1 AND database_name = ?2 AND schema_name = ?3 AND table_name = ?4"
            }
            "rename_column" => {
                "UPDATE table_statistics SET rename_column = rename_column + 1 WHERE dst_alias = ?1 AND database_name = ?2 AND schema_name = ?3 AND table_name = ?4"
            }
            "rename_table" => {
                "UPDATE table_statistics SET rename_table = rename_table + 1 WHERE dst_alias = ?1 AND database_name = ?2 AND schema_name = ?3 AND table_name = ?4"
            }
            _ => continue,
        };

        let schema_name = "";
        let inserted = device_stats_conn
            .execute(
                "INSERT INTO table_statistics(
                     dst_alias, database_name, schema_name, table_name,
                     initial_rows, insert_rows, update_rows, delete_rows,
                     add_column, drop_column, rename_column, create_table, drop_table, rename_table
                 )
                 SELECT ?1, ?2, ?3, ?4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                 WHERE NOT EXISTS (
                     SELECT 1 FROM table_statistics WHERE dst_alias = ?1 AND database_name = ?2 AND schema_name = ?3 AND table_name = ?4
                 )",
                params![DEFAULT_DST_ALIAS, layout.database_name, schema_name, table_name],
            )
            .map_err(map_sql_err)?;
        let _ = inserted;

        device_stats_conn
            .execute(
                update_sql,
                params![DEFAULT_DST_ALIAS, layout.database_name, schema_name, table_name],
            )
            .map_err(map_sql_err)?;
    }

    Ok(())
}

fn classify_table_stat_update(
    layout: &ConsolidatorLayout,
    sql: &str,
) -> Option<(&'static str, String)> {
    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    let consolidation_mode = layout.destination_sync_mode == DestinationSyncMode::Consolidation;

    if upper.starts_with("INSERT") {
        extract_table_after_keyword(trimmed, "INTO").map(|t| ("insert_rows", t))
    } else if upper.starts_with("UPDATE") {
        let token = trimmed["UPDATE".len()..]
            .trim_start()
            .split_whitespace()
            .next()?;
        Some(("update_rows", normalize_table_token(token)))
    } else if upper.starts_with("DELETE") {
        extract_table_after_keyword(trimmed, "FROM").map(|t| ("delete_rows", t))
    } else if upper.starts_with("CREATE TABLE") {
        extract_table_after_create_drop(trimmed, "CREATE TABLE").map(|t| ("create_table", t))
    } else if upper.starts_with("DROP TABLE") {
        if consolidation_mode {
            None
        } else {
            extract_table_after_create_drop(trimmed, "DROP TABLE").map(|t| ("drop_table", t))
        }
    } else if upper.starts_with("ALTER TABLE") && upper.contains(" ADD COLUMN ") {
        extract_table_after_create_drop(trimmed, "ALTER TABLE").map(|t| ("add_column", t))
    } else if upper.starts_with("ALTER TABLE") && upper.contains(" DROP COLUMN ") {
        if consolidation_mode {
            None
        } else {
            extract_table_after_create_drop(trimmed, "ALTER TABLE").map(|t| ("drop_column", t))
        }
    } else if upper.starts_with("ALTER TABLE") && upper.contains(" RENAME COLUMN ") {
        if consolidation_mode {
            extract_table_after_create_drop(trimmed, "ALTER TABLE").map(|t| ("add_column", t))
        } else {
            extract_table_after_create_drop(trimmed, "ALTER TABLE").map(|t| ("rename_column", t))
        }
    } else if upper.starts_with("ALTER TABLE") && upper.contains(" RENAME TO ") {
        if consolidation_mode {
            extract_table_after_keyword(trimmed, "RENAME TO").map(|t| ("create_table", t))
        } else {
            extract_table_after_create_drop(trimmed, "ALTER TABLE").map(|t| ("rename_table", t))
        }
    } else {
        None
    }
}

fn extract_table_after_keyword(sql: &str, keyword: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let idx = upper.find(keyword)?;
    let rest = sql[idx + keyword.len()..].trim_start();
    let token = rest.split_whitespace().next()?;
    Some(normalize_table_token(token))
}

fn extract_table_after_create_drop(sql: &str, phrase: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let idx = upper.find(phrase)?;
    let mut rest = sql[idx + phrase.len()..].trim_start();
    if rest.to_ascii_uppercase().starts_with("IF NOT EXISTS") {
        rest = rest["IF NOT EXISTS".len()..].trim_start();
    }
    if rest.to_ascii_uppercase().starts_with("IF EXISTS") {
        rest = rest["IF EXISTS".len()..].trim_start();
    }
    let token = rest.split_whitespace().next()?;
    Some(normalize_table_token(token))
}

fn normalize_table_token(token: &str) -> String {
    token
        .split('(')
        .next()
        .unwrap_or(token)
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .trim_end_matches(';')
        .trim_end_matches(',')
        .to_string()
}

pub fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn current_update_timestamp() -> String {
    format_timestamp_utc(now_epoch_ms())
}

fn format_timestamp_utc(epoch_ms: i64) -> String {
    let secs = epoch_ms.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400) as u32;
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 }.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096).div_euclid(365);
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2).div_euclid(153);
    let d = doy - (153 * mp + 2).div_euclid(5) + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year as i32, m as u32, d as u32)
}

pub fn record_event(conn: &Connection, kind: &str, path: &Path) -> Result<()> {
    let _ = conn;
    let _ = kind;
    let _ = path;
    Ok(())
}

pub fn put_state_text(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO metadata(key, value)
         SELECT ?1, ?2
         WHERE NOT EXISTS (SELECT 1 FROM metadata WHERE key = ?1)",
        params![key, value],
    )
    .map_err(map_sql_err)?;
    conn.execute(
        "UPDATE metadata SET value = ?2 WHERE key = ?1",
        params![key, value],
    )
    .map_err(map_sql_err)?;
    Ok(())
}

pub fn get_state_i64(conn: &Connection, key: &str, default: i64) -> Result<i64> {
    let value = conn.query_row(
        "SELECT value FROM metadata WHERE key = ?1 LIMIT 1",
        params![key],
        |row| row.get::<_, String>(0),
    );
    match value {
        Ok(v) => Ok(v.parse::<i64>().unwrap_or(default)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(default),
        Err(e) => Err(map_sql_err(e)),
    }
}

pub fn put_state_i64(conn: &Connection, key: &str, value: i64) -> Result<()> {
    put_state_text(conn, key, &value.to_string())
}

pub fn put_metric_i64(conn: &Connection, key: &str, value: i64) -> Result<()> {
    let sql = match key {
        "destination_initialized" => {
            "UPDATE checkpoint SET is_initialization_stats_collected = ?1 WHERE dst_alias='DB-1'"
        }
        "processed_stage_event_count" => {
            "UPDATE checkpoint SET processed_oper_count = ?1 WHERE dst_alias='DB-1'"
        }
        "last_segment_sequence" => {
            "UPDATE checkpoint SET cdc_log_segment_sequence_number = ?1 WHERE dst_alias='DB-1'"
        }
        "initialization_backup_size_bytes" => {
            "UPDATE checkpoint SET processed_log_size = ?1 WHERE dst_alias='DB-1'"
        }
        _ => return Ok(()),
    };
    conn.execute(sql, params![value]).map_err(map_sql_err)?;
    Ok(())
}

pub fn get_counter(conn: &Connection, key: &str) -> Result<i64> {
    let sql = match key {
        "processed_stage_event_count" => {
            "SELECT processed_oper_count FROM checkpoint WHERE dst_alias='DB-1'"
        }
        "last_segment_sequence" => {
            "SELECT cdc_log_segment_sequence_number FROM checkpoint WHERE dst_alias='DB-1'"
        }
        "initialization_backup_size_bytes" => {
            "SELECT processed_log_size FROM checkpoint WHERE dst_alias='DB-1'"
        }
        _ => return Ok(0),
    };
    let value = conn.query_row(sql, [], |row| row.get::<_, i64>(0));
    match value {
        Ok(v) => Ok(v),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
        Err(e) => Err(map_sql_err(e)),
    }
}

pub fn parse_segment_seq(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    if let Some(seq) = name.strip_suffix(".sqllog") {
        return seq.parse::<u64>().ok();
    }
    if let Some(seq) = name.strip_suffix(".cdclog") {
        return seq.parse::<u64>().ok();
    }
    None
}

pub fn map_sql_err(e: rusqlite::Error) -> Error {
    match &e {
        rusqlite::Error::SqliteFailure(code, msg) => Error::Config(format!(
            "consolidator: sqlite[{:#?}] {}",
            code,
            msg.clone().unwrap_or_else(|| e.to_string())
        )),
        _ => Error::Config(format!("consolidator: {e}")),
    }
}

pub fn map_duck_err(e: duckdb::Error) -> Error {
    Error::Config(format!("consolidator: {e}"))
}

pub fn map_pg_err(e: postgres::Error) -> Error {
    map_pg_err_ctx(e, None)
}

/// Same as `map_pg_err` but prepends a short SQL/context label so the
/// device trace shows which statement failed. Truncates very long SQL.
pub fn map_pg_err_with_sql(e: postgres::Error, sql: &str) -> Error {
    map_pg_err_ctx(e, Some(sql))
}

fn map_pg_err_ctx(e: postgres::Error, sql: Option<&str>) -> Error {
    let code = e
        .code()
        .map(|c| c.code().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    // `postgres::Error`'s top-level Display is famously opaque
    // ("db error"). The actual server message (with SQLSTATE name,
    // detail, hint, table, column, etc.) lives on the chained source.
    // Walk the source chain so the trace shows the real cause.
    let mut cause_chain = String::new();
    let mut src: Option<&dyn std::error::Error> = std::error::Error::source(&e);
    while let Some(s) = src {
        if !cause_chain.is_empty() {
            cause_chain.push_str(" :: ");
        }
        cause_chain.push_str(&s.to_string());
        src = s.source();
    }
    // Also pull structured server fields when available (DbError).
    let server = e.as_db_error().map(|db| {
        let mut s = format!("severity={} message={}", db.severity(), db.message());
        if let Some(d) = db.detail() {
            s.push_str(&format!(" detail={d}"));
        }
        if let Some(h) = db.hint() {
            s.push_str(&format!(" hint={h}"));
        }
        if let Some(t) = db.table() {
            s.push_str(&format!(" table={t}"));
        }
        if let Some(sc) = db.schema() {
            s.push_str(&format!(" schema={sc}"));
        }
        if let Some(c) = db.column() {
            s.push_str(&format!(" column={c}"));
        }
        if let Some(w) = db.where_() {
            s.push_str(&format!(" where={w}"));
        }
        s
    });
    let detail = match (server, cause_chain.is_empty()) {
        (Some(s), _) => s,
        (None, false) => cause_chain,
        (None, true) => e.to_string(),
    };
    let sql_ctx = sql.map(|s| {
        let trimmed = s.trim();
        if trimmed.len() > 240 {
            format!(" sql=`{}...`", &trimmed[..240])
        } else {
            format!(" sql=`{}`", trimmed)
        }
    }).unwrap_or_default();
    Error::Config(format!("consolidator: pg[{code}]{sql_ctx} {detail}"))
}

pub fn map_io_err(e: std::io::Error) -> Error {
    Error::Config(format!("consolidator: {e}"))
}

fn duck_exec_with_params(conn: &DuckConnection, sql: &str, vals: &[DuckValue]) -> Result<()> {
    let mut stmt = conn.prepare(sql).map_err(map_duck_err)?;
    let mut rows = stmt
        .query(duck_params_from_iter(vals.iter()))
        .map_err(map_duck_err)?;
    while rows.next().map_err(map_duck_err)?.is_some() {}
    Ok(())
}

fn sqlite_has_checkpoint_device_columns(conn: &Connection) -> Result<bool> {
    let mut stmt = conn
        .prepare("PRAGMA table_info('synclite_checkpoint')")
        .map_err(map_sql_err)?;
    let mut rows = stmt.query([]).map_err(map_sql_err)?;
    let mut has_device_id = false;
    let mut has_device_name = false;
    while let Some(row) = rows.next().map_err(map_sql_err)? {
        let name: String = row.get(1).map_err(map_sql_err)?;
        if name.eq_ignore_ascii_case("synclite_device_id") {
            has_device_id = true;
        } else if name.eq_ignore_ascii_case("synclite_device_name") {
            has_device_name = true;
        }
    }
    Ok(has_device_id && has_device_name)
}

fn duckdb_has_checkpoint_device_columns(conn: &DuckConnection) -> Result<bool> {
    let mut stmt = conn
        .prepare("PRAGMA table_info('synclite_checkpoint')")
        .map_err(map_duck_err)?;
    let mut rows = stmt.query([]).map_err(map_duck_err)?;
    let mut has_device_id = false;
    let mut has_device_name = false;
    while let Some(row) = rows.next().map_err(map_duck_err)? {
        let name: String = row.get(1).map_err(map_duck_err)?;
        if name.eq_ignore_ascii_case("synclite_device_id") {
            has_device_id = true;
        } else if name.eq_ignore_ascii_case("synclite_device_name") {
            has_device_name = true;
        }
    }
    Ok(has_device_id && has_device_name)
}

fn postgres_has_checkpoint_device_columns(client: &mut PgClient, layout: &ConsolidatorLayout) -> Result<bool> {
    let count: i64 = if let Some(schema) = pg_meta_schema(layout) {
        client
            .query_one(
                "SELECT COUNT(DISTINCT column_name)
                 FROM information_schema.columns
                 WHERE table_schema = $1
                   AND table_name = 'synclite_checkpoint'
                   AND column_name IN ('synclite_device_id', 'synclite_device_name')",
                &[&schema],
            )
            .map_err(map_pg_err)?
            .get(0)
    } else {
        client
            .query_one(
                "SELECT COUNT(DISTINCT column_name)
                 FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name = 'synclite_checkpoint'
                   AND column_name IN ('synclite_device_id', 'synclite_device_name')",
                &[],
            )
            .map_err(map_pg_err)?
            .get(0)
    };
    Ok(count == 2)
}

pub fn map_destination_checkpoint(
    layout: &ConsolidatorLayout,
    seq: i64,
    progress: ApplyProgress,
) -> Result<()> {
    let update_ts = current_update_timestamp();
    match layout.dst_type {
        DstType::Sqlite => {
            let dst_conn = Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
            if !sqlite_has_checkpoint_device_columns(&dst_conn)? {
                return Err(Error::Config(
                    "consolidator: destination synclite_checkpoint must contain synclite_device_id and synclite_device_name".to_string(),
                ));
            }
            let updated = dst_conn
                .execute(
                    "UPDATE synclite_checkpoint
                     SET commit_id = ?1,
                         synclite_update_timestamp = ?2,
                         cdc_change_number = CASE WHEN ?3 >= 0 THEN ?3 ELSE cdc_change_number END,
                         cdc_log_segment_sequence_number = CASE
                             WHEN cdc_log_segment_sequence_number < ?4 THEN ?4
                             ELSE cdc_log_segment_sequence_number
                         END,
                         txn_count = txn_count + ?5
                     WHERE synclite_device_id = ?6
                       AND synclite_device_name = ?7
                       AND commit_id = (
                            SELECT MAX(commit_id)
                            FROM synclite_checkpoint
                            WHERE synclite_device_id = ?8 AND synclite_device_name = ?9
                       )",
                    params![
                        progress.applied_commit,
                        update_ts,
                        progress.applied_change,
                        seq,
                        progress.applied_txn_cnt,
                        layout.device_id,
                        layout.device_name,
                        layout.device_id,
                        layout.device_name,
                    ],
                )
                .map_err(map_sql_err)?;
            if updated == 0 {
                dst_conn
                    .execute(
                        "INSERT INTO synclite_checkpoint(
                             synclite_device_id, synclite_device_name, synclite_update_timestamp, commit_id,
                             cdc_change_number, cdc_log_segment_sequence_number, txn_count
                         ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![
                            layout.device_id,
                            layout.device_name,
                            current_update_timestamp(),
                            progress.applied_commit,
                            progress.applied_change,
                            seq,
                            progress.applied_txn_cnt,
                        ],
                    )
                    .map_err(map_sql_err)?;
            }
        }
        DstType::DuckDb => {
            let dst_conn = DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
            if !duckdb_has_checkpoint_device_columns(&dst_conn)? {
                return Err(Error::Config(
                    "consolidator: destination synclite_checkpoint must contain synclite_device_id and synclite_device_name".to_string(),
                ));
            }
            let vals = [
                DuckValue::BigInt(progress.applied_commit),
                DuckValue::Text(update_ts.clone()),
                DuckValue::BigInt(progress.applied_change),
                DuckValue::BigInt(seq),
                DuckValue::BigInt(seq),
                DuckValue::BigInt(progress.applied_txn_cnt),
                DuckValue::Text(layout.device_id.clone()),
                DuckValue::Text(layout.device_name.clone()),
                DuckValue::Text(layout.device_id.clone()),
                DuckValue::Text(layout.device_name.clone()),
            ];
            let mut stmt = dst_conn
                .prepare(
                    "UPDATE synclite_checkpoint
                     SET commit_id = ?,
                         synclite_update_timestamp = ?,
                         cdc_change_number = CASE WHEN ? >= 0 THEN ? ELSE cdc_change_number END,
                         cdc_log_segment_sequence_number = CASE
                             WHEN cdc_log_segment_sequence_number < ? THEN ?
                             ELSE cdc_log_segment_sequence_number
                         END,
                         txn_count = txn_count + ?
                     WHERE synclite_device_id = ?
                       AND synclite_device_name = ?
                       AND commit_id = (
                            SELECT MAX(commit_id)
                            FROM synclite_checkpoint
                            WHERE synclite_device_id = ? AND synclite_device_name = ?
                       )",
                )
                .map_err(map_duck_err)?;
            let updated = stmt
                .execute(duck_params_from_iter(vals.iter()))
                .map_err(map_duck_err)?;
            if updated == 0 {
                let insert_vals = [
                    DuckValue::Text(layout.device_id.clone()),
                    DuckValue::Text(layout.device_name.clone()),
                    DuckValue::Text(current_update_timestamp()),
                    DuckValue::BigInt(progress.applied_commit),
                    DuckValue::BigInt(progress.applied_change),
                    DuckValue::BigInt(seq),
                    DuckValue::BigInt(progress.applied_txn_cnt),
                ];
                duck_exec_with_params(
                    &dst_conn,
                    "INSERT INTO synclite_checkpoint(
                         synclite_device_id, synclite_device_name, synclite_update_timestamp, commit_id,
                         cdc_change_number, cdc_log_segment_sequence_number, txn_count
                     ) VALUES(?, ?, ?, ?, ?, ?, ?)",
                    &insert_vals,
                )?;
            }
        }
        DstType::Postgres => {
            let conn_str = &layout.dst_connection_string;
            let mut client = PgClient::connect(conn_str, NoTls).map_err(map_pg_err)?;
            if !postgres_has_checkpoint_device_columns(&mut client, layout)? {
                return Err(Error::Config(
                    "consolidator: destination synclite_checkpoint must contain synclite_device_id and synclite_device_name".to_string(),
                ));
            }
            let meta = pg_meta_table(layout, "synclite_checkpoint");
            let update_sql = format!(
                    "UPDATE {meta}
                     SET commit_id = $1,
                         synclite_update_timestamp = $2,
                         cdc_change_number = CASE WHEN $3 >= 0::bigint THEN $3 ELSE cdc_change_number END,
                         cdc_log_segment_sequence_number = CASE
                             WHEN cdc_log_segment_sequence_number < $4 THEN $4
                             ELSE cdc_log_segment_sequence_number
                         END,
                         txn_count = txn_count + $5
                     WHERE synclite_device_id = $6
                       AND synclite_device_name = $7
                       AND commit_id = (
                            SELECT MAX(commit_id)
                            FROM {meta}
                            WHERE synclite_device_id = $8 AND synclite_device_name = $9
                       )"
                );
            let updated = client
                .execute(
                    update_sql.as_str(),
                    &[
                        &progress.applied_commit,
                        &update_ts,
                        &progress.applied_change,
                        &seq,
                        &progress.applied_txn_cnt,
                        &layout.device_id,
                        &layout.device_name,
                        &layout.device_id,
                        &layout.device_name,
                    ],
                )
                .map_err(|e| map_pg_err_with_sql(e, &update_sql))?;
            if updated == 0 {
                let insert_sql = format!(
                    "INSERT INTO {meta}(
                         synclite_device_id, synclite_device_name, synclite_update_timestamp, commit_id,
                         cdc_change_number, cdc_log_segment_sequence_number, txn_count
                     ) VALUES($1, $2, $3, $4, $5, $6, $7)"
                );
                client
                    .execute(
                        insert_sql.as_str(),
                        &[
                            &layout.device_id,
                            &layout.device_name,
                            &current_update_timestamp(),
                            &progress.applied_commit,
                            &progress.applied_change,
                            &seq,
                            &progress.applied_txn_cnt,
                        ],
                    )
                    .map_err(|e| map_pg_err_with_sql(e, &insert_sql))?;
            }
        }
    }
    Ok(())
}

pub fn map_local_checkpoint(state_conn: &Connection, seq: i64, progress: ApplyProgress) -> Result<()> {
    state_conn
        .execute(
            "UPDATE synclite_checkpoint\n\
             SET commit_id = ?1,\n\
                 cdc_change_number = CASE WHEN ?2 >= 0 THEN ?2 ELSE cdc_change_number END,\n\
                 cdc_log_segment_sequence_number = CASE\n\
                     WHEN cdc_log_segment_sequence_number < ?3 THEN ?3\n\
                     ELSE cdc_log_segment_sequence_number\n\
                 END,\n\
                 txn_count = txn_count + ?4",
            params![progress.applied_commit, progress.applied_change, seq, progress.applied_txn_cnt],
        )
        .map_err(map_sql_err)?;
    Ok(())
}

// =============================================================================
// Java-compatible metadata stores: synclite_device_metadata.db (per-device
// identity) and synclite_consolidator_metadata_<dstIndex>.db / destination
// synclite_consolidator_metadata (per-destination state).
//
// The schemas, file names, table names, and property keys are byte-for-byte
// identical to the Java consolidator (see ConsolidatorMetadataManager.java)
// so a device's persistent state survives handover Java<->Rust seamlessly.
// =============================================================================

const CREATE_LOCAL_PROP_TBL_SQL: &str =
    "CREATE TABLE IF NOT EXISTS metadata(key TEXT, value TEXT);";

const CREATE_LOCAL_TABLE_METADATA_TBL_SQL: &str =
    "CREATE TABLE IF NOT EXISTS table_metadata(database_name TEXT, schema_name TEXT, table_name TEXT, key TEXT, value TEXT);";

const CREATE_DST_PROP_TBL_SQL: &str =
    "CREATE TABLE IF NOT EXISTS synclite_consolidator_metadata(\
    synclite_device_id VARCHAR(64) NOT NULL, synclite_device_name VARCHAR(255) NOT NULL, synclite_update_timestamp TEXT, \
     prop_key VARCHAR(255) NOT NULL, prop_value TEXT, \
    PRIMARY KEY(synclite_device_id, synclite_device_name, prop_key))";

const CREATE_DST_TABLE_METADATA_TBL_SQL: &str =
    "CREATE TABLE IF NOT EXISTS synclite_consolidator_table_metadata(\
    synclite_device_id VARCHAR(64) NOT NULL, synclite_device_name VARCHAR(255) NOT NULL, synclite_update_timestamp TEXT, \
     database_name VARCHAR(255) NOT NULL, table_name VARCHAR(255) NOT NULL, prop_key VARCHAR(255) NOT NULL, prop_value TEXT, \
    PRIMARY KEY(synclite_device_id, synclite_device_name, database_name, table_name, prop_key))";

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(map_io_err)?;
    }
    Ok(())
}

fn open_local_props(path: &Path) -> Result<Connection> {
    ensure_parent_dir(path)?;
    let conn = Connection::open(path).map_err(map_sql_err)?;
    conn.execute_batch(CREATE_LOCAL_PROP_TBL_SQL).map_err(map_sql_err)?;
    Ok(conn)
}

fn open_local_table_metadata(path: &Path) -> Result<Connection> {
    ensure_parent_dir(path)?;
    let conn = Connection::open(path).map_err(map_sql_err)?;
    conn.execute_batch(CREATE_LOCAL_TABLE_METADATA_TBL_SQL)
        .map_err(map_sql_err)?;
    Ok(conn)
}

fn local_get_string(conn: &Connection, key: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT value FROM metadata WHERE key = ?1",
        params![key],
        |r| r.get::<_, String>(0),
    )
    .optional()
    .map_err(map_sql_err)
}

fn local_upsert(conn: &mut Connection, key: &str, value: &str) -> Result<()> {
    let tx = conn.transaction().map_err(map_sql_err)?;
    tx.execute("DELETE FROM metadata WHERE key = ?1", params![key])
        .map_err(map_sql_err)?;
    tx.execute(
        "INSERT INTO metadata(key, value) VALUES(?1, ?2)",
        params![key, value],
    )
    .map_err(map_sql_err)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(())
}

fn local_upsert_many(conn: &mut Connection, kvs: &[(&str, String)]) -> Result<()> {
    let tx = conn.transaction().map_err(map_sql_err)?;
    for (k, v) in kvs {
        tx.execute("DELETE FROM metadata WHERE key = ?1", params![k])
            .map_err(map_sql_err)?;
        tx.execute(
            "INSERT INTO metadata(key, value) VALUES(?1, ?2)",
            params![k, v],
        )
        .map_err(map_sql_err)?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(())
}

fn local_delete(conn: &Connection, key: &str) -> Result<()> {
    conn.execute("DELETE FROM metadata WHERE key = ?1", params![key])
        .map_err(map_sql_err)?;
    Ok(())
}

fn local_table_meta_get(
    conn: &Connection,
    db: &str,
    table: &str,
    key: &str,
) -> Result<Option<String>> {
    conn.query_row(
        "SELECT value FROM table_metadata WHERE database_name = ?1 AND table_name = ?2 AND key = ?3",
        params![db, table, key],
        |r| r.get::<_, String>(0),
    )
    .optional()
    .map_err(map_sql_err)
}

fn local_table_meta_upsert(
    conn: &mut Connection,
    db: &str,
    schema: &str,
    table: &str,
    key: &str,
    value: &str,
) -> Result<()> {
    let tx = conn.transaction().map_err(map_sql_err)?;
    tx.execute(
        "DELETE FROM table_metadata WHERE database_name = ?1 AND table_name = ?2 AND key = ?3",
        params![db, table, key],
    )
    .map_err(map_sql_err)?;
    tx.execute(
        "INSERT INTO table_metadata(database_name, schema_name, table_name, key, value) VALUES(?1, ?2, ?3, ?4, ?5)",
        params![db, schema, table, key, value],
    )
    .map_err(map_sql_err)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(())
}

// ---------- Destination-mode helpers ----------

fn dst_props_sqlite_get(
    conn: &Connection,
    device_uuid: &str,
    device_name: &str,
    key: &str,
) -> Result<Option<String>> {
    conn.execute_batch(CREATE_DST_PROP_TBL_SQL).map_err(map_sql_err)?;
    conn.query_row(
        "SELECT prop_value FROM synclite_consolidator_metadata \
         WHERE synclite_device_id = ?1 AND synclite_device_name = ?2 AND prop_key = ?3",
        params![device_uuid, device_name, key],
        |r| r.get::<_, String>(0),
    )
    .optional()
    .map_err(map_sql_err)
}

fn dst_props_sqlite_upsert(
    conn: &mut Connection,
    device_uuid: &str,
    device_name: &str,
    kvs: &[(&str, String)],
) -> Result<()> {
    conn.execute_batch(CREATE_DST_PROP_TBL_SQL).map_err(map_sql_err)?;
    let tx = conn.transaction().map_err(map_sql_err)?;
    for (k, v) in kvs {
        tx.execute(
            "DELETE FROM synclite_consolidator_metadata \
             WHERE synclite_device_id = ?1 AND synclite_device_name = ?2 AND prop_key = ?3",
            params![device_uuid, device_name, k],
        )
        .map_err(map_sql_err)?;
        tx.execute(
            "INSERT INTO synclite_consolidator_metadata(synclite_device_id, synclite_device_name, synclite_update_timestamp, prop_key, prop_value) \
             VALUES(?1, ?2, ?3, ?4, ?5)",
            params![device_uuid, device_name, current_update_timestamp(), k, v],
        )
        .map_err(map_sql_err)?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(())
}

fn dst_props_sqlite_delete(
    conn: &Connection,
    device_uuid: &str,
    device_name: &str,
    key: &str,
) -> Result<()> {
    conn.execute_batch(CREATE_DST_PROP_TBL_SQL).map_err(map_sql_err)?;
    conn.execute(
        "DELETE FROM synclite_consolidator_metadata \
         WHERE synclite_device_id = ?1 AND synclite_device_name = ?2 AND prop_key = ?3",
        params![device_uuid, device_name, key],
    )
    .map_err(map_sql_err)?;
    Ok(())
}

fn dst_props_duck_get(
    conn: &DuckConnection,
    device_uuid: &str,
    device_name: &str,
    key: &str,
) -> Result<Option<String>> {
    conn.execute_batch(CREATE_DST_PROP_TBL_SQL).map_err(map_duck_err)?;
    let res: duckdb::Result<String> = conn.query_row(
        "SELECT prop_value FROM synclite_consolidator_metadata \
         WHERE synclite_device_id = ? AND synclite_device_name = ? AND prop_key = ?",
        duck_params_from_iter(
            [
                DuckValue::Text(device_uuid.to_string()),
                DuckValue::Text(device_name.to_string()),
                DuckValue::Text(key.to_string()),
            ]
            .iter(),
        ),
        |r| r.get::<_, String>(0),
    );
    match res {
        Ok(v) => Ok(Some(v)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(map_duck_err(e)),
    }
}

fn dst_props_duck_upsert(
    conn: &DuckConnection,
    device_uuid: &str,
    device_name: &str,
    kvs: &[(&str, String)],
) -> Result<()> {
    conn.execute_batch(CREATE_DST_PROP_TBL_SQL).map_err(map_duck_err)?;
    for (k, v) in kvs {
        let del_vals = [
            DuckValue::Text(device_uuid.to_string()),
            DuckValue::Text(device_name.to_string()),
            DuckValue::Text((*k).to_string()),
        ];
        duck_exec_with_params(
            conn,
            "DELETE FROM synclite_consolidator_metadata \
             WHERE synclite_device_id = ? AND synclite_device_name = ? AND prop_key = ?",
            &del_vals,
        )?;
        let ins_vals = [
            DuckValue::Text(device_uuid.to_string()),
            DuckValue::Text(device_name.to_string()),
            DuckValue::Text(current_update_timestamp()),
            DuckValue::Text((*k).to_string()),
            DuckValue::Text(v.clone()),
        ];
        duck_exec_with_params(
            conn,
            "INSERT INTO synclite_consolidator_metadata(synclite_device_id, synclite_device_name, synclite_update_timestamp, prop_key, prop_value) \
             VALUES(?, ?, ?, ?, ?)",
            &ins_vals,
        )?;
    }
    Ok(())
}

fn dst_props_duck_delete(
    conn: &DuckConnection,
    device_uuid: &str,
    device_name: &str,
    key: &str,
) -> Result<()> {
    conn.execute_batch(CREATE_DST_PROP_TBL_SQL).map_err(map_duck_err)?;
    let vals = [
        DuckValue::Text(device_uuid.to_string()),
        DuckValue::Text(device_name.to_string()),
        DuckValue::Text(key.to_string()),
    ];
    duck_exec_with_params(
        conn,
        "DELETE FROM synclite_consolidator_metadata \
         WHERE synclite_device_id = ? AND synclite_device_name = ? AND prop_key = ?",
        &vals,
    )?;
    Ok(())
}

fn dst_props_pg_get(
    client: &mut PgClient,
    layout: &ConsolidatorLayout,
    device_uuid: &str,
    device_name: &str,
    key: &str,
) -> Result<Option<String>> {
    let meta = pg_meta_table(layout, "synclite_consolidator_metadata");
    let select_sql = format!(
        "SELECT prop_value FROM {meta} \
         WHERE synclite_device_id = $1 AND synclite_device_name = $2 AND prop_key = $3"
    );
    let rows = client
        .query(
            select_sql.as_str(),
            &[&device_uuid, &device_name, &key],
        )
        .map_err(|e| map_pg_err_with_sql(e, &select_sql))?;
    if let Some(row) = rows.into_iter().next() {
        let v: Option<String> = row.get(0);
        Ok(v)
    } else {
        Ok(None)
    }
}

fn dst_props_pg_upsert(
    client: &mut PgClient,
    layout: &ConsolidatorLayout,
    device_uuid: &str,
    device_name: &str,
    kvs: &[(&str, String)],
) -> Result<()> {
    let meta = pg_meta_table(layout, "synclite_consolidator_metadata");
    let delete_sql = format!(
        "DELETE FROM {meta} \
         WHERE synclite_device_id = $1 AND synclite_device_name = $2 AND prop_key = $3"
    );
    let insert_sql = format!(
        "INSERT INTO {meta}(synclite_device_id, synclite_device_name, synclite_update_timestamp, prop_key, prop_value) \
         VALUES($1, $2, $3, $4, $5)"
    );
    let mut tx = client.transaction().map_err(map_pg_err)?;
    for (k, v) in kvs {
        tx.execute(
            delete_sql.as_str(),
            &[&device_uuid, &device_name, k],
        )
        .map_err(|e| map_pg_err_with_sql(e, &delete_sql))?;
        tx.execute(
            insert_sql.as_str(),
            &[&device_uuid, &device_name, &current_update_timestamp(), k, v],
        )
        .map_err(|e| map_pg_err_with_sql(e, &insert_sql))?;
    }
    tx.commit().map_err(map_pg_err)?;
    Ok(())
}

fn dst_props_pg_delete(
    client: &mut PgClient,
    layout: &ConsolidatorLayout,
    device_uuid: &str,
    device_name: &str,
    key: &str,
) -> Result<()> {
    let meta = pg_meta_table(layout, "synclite_consolidator_metadata");
    let delete_sql = format!(
        "DELETE FROM {meta} \
         WHERE synclite_device_id = $1 AND synclite_device_name = $2 AND prop_key = $3"
    );
    client
        .execute(
            delete_sql.as_str(),
            &[&device_uuid, &device_name, &key],
        )
        .map_err(|e| map_pg_err_with_sql(e, &delete_sql))?;
    Ok(())
}

// ---------- Per-table metadata destination helpers ----------

fn dst_tblmeta_sqlite_upsert(
    conn: &mut Connection,
    device_uuid: &str,
    device_name: &str,
    db: &str,
    table: &str,
    key: &str,
    value: &str,
) -> Result<()> {
    conn.execute_batch(CREATE_DST_TABLE_METADATA_TBL_SQL)
        .map_err(map_sql_err)?;
    let tx = conn.transaction().map_err(map_sql_err)?;
    tx.execute(
        "DELETE FROM synclite_consolidator_table_metadata \
         WHERE synclite_device_id = ?1 AND synclite_device_name = ?2 \
           AND database_name = ?3 AND table_name = ?4 AND prop_key = ?5",
        params![device_uuid, device_name, db, table, key],
    )
    .map_err(map_sql_err)?;
    tx.execute(
        "INSERT INTO synclite_consolidator_table_metadata(synclite_device_id, synclite_device_name, synclite_update_timestamp, database_name, table_name, prop_key, prop_value) \
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![device_uuid, device_name, current_update_timestamp(), db, table, key, value],
    )
    .map_err(map_sql_err)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(())
}

fn dst_tblmeta_duck_upsert(
    conn: &DuckConnection,
    device_uuid: &str,
    device_name: &str,
    db: &str,
    table: &str,
    key: &str,
    value: &str,
) -> Result<()> {
    conn.execute_batch(CREATE_DST_TABLE_METADATA_TBL_SQL)
        .map_err(map_duck_err)?;
    let del_vals = [
        DuckValue::Text(device_uuid.to_string()),
        DuckValue::Text(device_name.to_string()),
        DuckValue::Text(db.to_string()),
        DuckValue::Text(table.to_string()),
        DuckValue::Text(key.to_string()),
    ];
    duck_exec_with_params(
        conn,
        "DELETE FROM synclite_consolidator_table_metadata \
         WHERE synclite_device_id = ? AND synclite_device_name = ? AND database_name = ? AND table_name = ? AND prop_key = ?",
        &del_vals,
    )?;
    let ins_vals = [
        DuckValue::Text(device_uuid.to_string()),
        DuckValue::Text(device_name.to_string()),
        DuckValue::Text(current_update_timestamp()),
        DuckValue::Text(db.to_string()),
        DuckValue::Text(table.to_string()),
        DuckValue::Text(key.to_string()),
        DuckValue::Text(value.to_string()),
    ];
    duck_exec_with_params(
        conn,
        "INSERT INTO synclite_consolidator_table_metadata(synclite_device_id, synclite_device_name, synclite_update_timestamp, database_name, table_name, prop_key, prop_value) \
         VALUES(?, ?, ?, ?, ?, ?, ?)",
        &ins_vals,
    )?;
    Ok(())
}

fn dst_tblmeta_pg_upsert(
    client: &mut PgClient,
    layout: &ConsolidatorLayout,
    device_uuid: &str,
    device_name: &str,
    db: &str,
    table: &str,
    key: &str,
    value: &str,
) -> Result<()> {
    let meta = pg_meta_table(layout, "synclite_consolidator_table_metadata");
    let delete_sql = format!(
        "DELETE FROM {meta} \
         WHERE synclite_device_id = $1 AND synclite_device_name = $2 AND database_name = $3 AND table_name = $4 AND prop_key = $5"
    );
    let insert_sql = format!(
        "INSERT INTO {meta}(synclite_device_id, synclite_device_name, synclite_update_timestamp, database_name, table_name, prop_key, prop_value) \
         VALUES($1, $2, $3, $4, $5, $6, $7)"
    );
    let mut tx = client.transaction().map_err(map_pg_err)?;
    tx.execute(
        delete_sql.as_str(),
        &[&device_uuid, &device_name, &db, &table, &key],
    )
    .map_err(|e| map_pg_err_with_sql(e, &delete_sql))?;
    tx.execute(
        insert_sql.as_str(),
        &[&device_uuid, &device_name, &current_update_timestamp(), &db, &table, &key, &value],
    )
    .map_err(|e| map_pg_err_with_sql(e, &insert_sql))?;
    tx.commit().map_err(map_pg_err)?;
    Ok(())
}

// ---------- Public Java-parity API ----------

/// Opens (creating if missing) the per-device identity metadata file and
/// returns a rusqlite Connection. Java equivalent:
/// `MetadataManager.getInstance(<rootPath>/synclite_device_metadata.db)`.
pub fn open_device_metadata(layout: &ConsolidatorLayout) -> Result<Connection> {
    open_local_props(&layout.device_metadata_path())
}

/// Writes the device-identity properties Java reads on every startup. Safe to
/// call repeatedly: each call upserts and never deletes other keys.
pub fn seed_device_metadata(layout: &ConsolidatorLayout) -> Result<()> {
    let mut conn = open_device_metadata(layout)?;
    let kvs: Vec<(&str, String)> = vec![
        ("database_name", layout.database_name.clone()),
        ("device_name", layout.device_name.clone()),
        ("device_type", layout.device_type.clone()),
    ];
    local_upsert_many(&mut conn, &kvs)?;
    // Only set status if not already present (preserve handover state from Java).
    if local_get_string(&conn, "status")?.is_none() {
        local_upsert(&mut conn, "status", "SYNCING")?;
    }
    Ok(())
}

/// Reads a string property from the device-metadata file.
pub fn get_device_metadata_string(layout: &ConsolidatorLayout, key: &str) -> Result<Option<String>> {
    let conn = open_device_metadata(layout)?;
    local_get_string(&conn, key)
}

/// Upserts a string property in the device-metadata file.
pub fn put_device_metadata_string(
    layout: &ConsolidatorLayout,
    key: &str,
    value: &str,
) -> Result<()> {
    let mut conn = open_device_metadata(layout)?;
    local_upsert(&mut conn, key, value)
}

/// Reads a string property from the per-destination consolidator metadata
/// store. Mirrors Java's `ConsolidatorMetadataManager.getStringProperty`,
/// branching LOCAL (local SQLite file) vs DESTINATION (destination DB
/// `synclite_consolidator_metadata` table) per `layout.metadata_store`.
pub fn get_consolidator_property(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    key: &str,
) -> Result<Option<String>> {
    retry_dst(layout, "get_consolidator_property", || match layout.metadata_store {
        MetadataStore::Local => {
            let conn = open_local_props(&layout.consolidator_metadata_path(dst_index))?;
            local_get_string(&conn, key)
        }
        MetadataStore::Destination => {
            bootstrap_destination_metadata(layout, dst_index)?;
            match layout.dst_type {
            DstType::Sqlite => {
                let conn = Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
                dst_props_sqlite_get(
                    &conn,
                    &layout.device_id,
                    &layout.device_name,
                    key,
                )
            }
            DstType::DuckDb => {
                let conn = DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
                dst_props_duck_get(
                    &conn,
                    &layout.device_id,
                    &layout.device_name,
                    key,
                )
            }
            DstType::Postgres => {
                let conn_str = &layout.dst_connection_string;
                let mut client = PgClient::connect(conn_str, NoTls).map_err(map_pg_err)?;
                dst_props_pg_get(
                    &mut client,
                    layout,
                    &layout.device_id,
                    &layout.device_name,
                    key,
                )
            }
            }
        }
    })
}

/// Reads a long property (parses string value).
pub fn get_consolidator_property_long(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    key: &str,
) -> Result<Option<i64>> {
    match get_consolidator_property(layout, dst_index, key)? {
        Some(s) => s
            .parse::<i64>()
            .map(Some)
            .map_err(|e| Error::Config(format!("consolidator: bad long property {}: {}", key, e))),
        None => Ok(None),
    }
}

/// Upserts (key, value) into the per-destination consolidator metadata store.
pub fn upsert_consolidator_property(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    key: &str,
    value: &str,
) -> Result<()> {
    upsert_consolidator_properties(layout, dst_index, &[(key, value.to_string())])
}

/// Batch upsert; matches Java `upsertProperties(HashMap)` semantics.
pub fn upsert_consolidator_properties(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    kvs: &[(&str, String)],
) -> Result<()> {
    if kvs.is_empty() {
        return Ok(());
    }
    retry_dst(layout, "upsert_consolidator_properties", || match layout.metadata_store {
        MetadataStore::Local => {
            let mut conn = open_local_props(&layout.consolidator_metadata_path(dst_index))?;
            local_upsert_many(&mut conn, kvs)
        }
        MetadataStore::Destination => {
            bootstrap_destination_metadata(layout, dst_index)?;
            match layout.dst_type {
            DstType::Sqlite => {
                let mut conn =
                    Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
                dst_props_sqlite_upsert(
                    &mut conn,
                    &layout.device_id,
                    &layout.device_name,
                    kvs,
                )
            }
            DstType::DuckDb => {
                let conn =
                    DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
                dst_props_duck_upsert(
                    &conn,
                    &layout.device_id,
                    &layout.device_name,
                    kvs,
                )
            }
            DstType::Postgres => {
                let conn_str = &layout.dst_connection_string;
                let mut client = PgClient::connect(conn_str, NoTls).map_err(map_pg_err)?;
                dst_props_pg_upsert(
                    &mut client,
                    layout,
                    &layout.device_id,
                    &layout.device_name,
                    kvs,
                )
            }
            }
        }
    })
}

/// Deletes a property from the per-destination consolidator metadata store.
pub fn delete_consolidator_property(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    key: &str,
) -> Result<()> {
    retry_dst(layout, "delete_consolidator_property", || match layout.metadata_store {
        MetadataStore::Local => {
            let conn = open_local_props(&layout.consolidator_metadata_path(dst_index))?;
            local_delete(&conn, key)
        }
        MetadataStore::Destination => {
            bootstrap_destination_metadata(layout, dst_index)?;
            match layout.dst_type {
            DstType::Sqlite => {
                let conn = Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
                dst_props_sqlite_delete(
                    &conn,
                    &layout.device_id,
                    &layout.device_name,
                    key,
                )
            }
            DstType::DuckDb => {
                let conn = DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
                dst_props_duck_delete(
                    &conn,
                    &layout.device_id,
                    &layout.device_name,
                    key,
                )
            }
            DstType::Postgres => {
                let conn_str = &layout.dst_connection_string;
                let mut client = PgClient::connect(conn_str, NoTls).map_err(map_pg_err)?;
                dst_props_pg_delete(
                    &mut client,
                    layout,
                    &layout.device_id,
                    &layout.device_name,
                    key,
                )
            }
            }
        }
    })
}

/// Upserts per-table metadata (e.g. `initial_rows`) into the appropriate store
/// based on metadata-store mode.
pub fn upsert_consolidator_table_metadata(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    db: &str,
    schema: &str,
    table: &str,
    key: &str,
    value: &str,
) -> Result<()> {
    retry_dst(layout, "upsert_consolidator_table_metadata", || match layout.metadata_store {
        MetadataStore::Local => {
            let mut conn = open_local_table_metadata(&layout.consolidator_metadata_path(dst_index))?;
            local_table_meta_upsert(&mut conn, db, schema, table, key, value)
        }
        MetadataStore::Destination => {
            bootstrap_destination_metadata(layout, dst_index)?;
            match layout.dst_type {
            DstType::Sqlite => {
                let mut conn =
                    Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
                dst_tblmeta_sqlite_upsert(
                    &mut conn,
                    &layout.device_id,
                    &layout.device_name,
                    db,
                    table,
                    key,
                    value,
                )
            }
            DstType::DuckDb => {
                let conn =
                    DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
                dst_tblmeta_duck_upsert(
                    &conn,
                    &layout.device_id,
                    &layout.device_name,
                    db,
                    table,
                    key,
                    value,
                )
            }
            DstType::Postgres => {
                let conn_str = &layout.dst_connection_string;
                let mut client = PgClient::connect(conn_str, NoTls).map_err(map_pg_err)?;
                dst_tblmeta_pg_upsert(
                    &mut client,
                    layout,
                    &layout.device_id,
                    &layout.device_name,
                    db,
                    table,
                    key,
                    value,
                )
            }
            }
        }
    })
}

/// Reads a per-table metadata value (LOCAL or DESTINATION mode).
pub fn get_consolidator_table_metadata(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    db: &str,
    table: &str,
    key: &str,
) -> Result<Option<String>> {
    retry_dst(layout, "get_consolidator_table_metadata", || match layout.metadata_store {
        MetadataStore::Local => {
            let conn = open_local_table_metadata(&layout.consolidator_metadata_path(dst_index))?;
            local_table_meta_get(&conn, db, table, key)
        }
        MetadataStore::Destination => {
            bootstrap_destination_metadata(layout, dst_index)?;
            match layout.dst_type {
            DstType::Sqlite => {
                let conn = Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
                conn.query_row(
                    "SELECT prop_value FROM synclite_consolidator_table_metadata \
                     WHERE synclite_device_id = ?1 AND synclite_device_name = ?2 \
                       AND database_name = ?3 AND table_name = ?4 AND prop_key = ?5",
                    params![
                        &layout.device_id,
                        &layout.device_name,
                        db,
                        table,
                        key
                    ],
                    |r| r.get::<_, String>(0),
                )
                .optional()
                .map_err(map_sql_err)
            }
            DstType::DuckDb => {
                let conn = DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
                let res: duckdb::Result<String> = conn.query_row(
                    "SELECT prop_value FROM synclite_consolidator_table_metadata \
                     WHERE synclite_device_id = ? AND synclite_device_name = ? AND database_name = ? AND table_name = ? AND prop_key = ?",
                    duck_params_from_iter([
                        DuckValue::Text(layout.device_id.clone()),
                        DuckValue::Text(layout.device_name.clone()),
                        DuckValue::Text(db.to_string()),
                        DuckValue::Text(table.to_string()),
                        DuckValue::Text(key.to_string()),
                    ].iter()),
                    |r| r.get::<_, String>(0),
                );
                match res {
                    Ok(v) => Ok(Some(v)),
                    Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
                    Err(e) => Err(map_duck_err(e)),
                }
            }
            DstType::Postgres => {
                let conn_str = &layout.dst_connection_string;
                let mut client = PgClient::connect(conn_str, NoTls).map_err(map_pg_err)?;
                let meta = pg_meta_table(layout, "synclite_consolidator_table_metadata");
                let select_sql = format!(
                    "SELECT prop_value FROM {meta} \
                     WHERE synclite_device_id = $1 AND synclite_device_name = $2 \
                       AND database_name = $3 AND table_name = $4 AND prop_key = $5"
                );
                let rows = client
                    .query(
                        select_sql.as_str(),
                        &[&layout.device_id, &layout.device_name, &db, &table, &key],
                    )
                    .map_err(|e| map_pg_err_with_sql(e, &select_sql))?;
                Ok(rows.into_iter().next().and_then(|r| r.get(0)))
            }
            }
        }
    })
}

// ---------- Lifecycle helpers (semantic operations Java performs) ----------

/// Java parity: `ConsolidatorMetadataManager.updateInitializedSnapshotName`.
/// Atomically marks the device as initialized for the given destination,
/// recording the snapshot name and bumping initialization_count.
pub fn mark_initialization_complete(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    snapshot_name: &str,
) -> Result<()> {
    let new_count = get_consolidator_property_long(layout, dst_index, "initialization_count")?
        .unwrap_or(0)
        + 1;
    let kvs: Vec<(&str, String)> = vec![
        ("initialized_snapshot_name", snapshot_name.to_string()),
        ("initialization_status", "1".to_string()),
        ("initialization_count", new_count.to_string()),
    ];
    upsert_consolidator_properties(layout, dst_index, &kvs)
}

/// Java parity: `ConsolidatorMetadataManager.updateLastConsolidatedCDCLogSegmentSeqNum`.
pub fn record_consolidated_cdc_seq(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    seq: i64,
) -> Result<()> {
    upsert_consolidator_property(
        layout,
        dst_index,
        "last_consolidated_cdc_log_segment_seq_num",
        &seq.to_string(),
    )
}

/// Java parity: device-metadata identity duplicated into consolidator
/// properties (`Device.persistDeviceMetadataToConsolidatorMgr`).
pub fn seed_consolidator_identity(layout: &ConsolidatorLayout, dst_index: i32) -> Result<()> {
    let kvs: Vec<(&str, String)> = vec![
        ("database_name", layout.database_name.clone()),
        ("device_name", layout.device_name.clone()),
        ("device_type", layout.device_type.clone()),
        ("database_id", "0".to_string()),
    ];
    upsert_consolidator_properties(layout, dst_index, &kvs)
}

/// Seeds the on-disk metadata schema version under
/// [`SYNCLITE_METADATA_VERSION_KEY`] into the per-destination consolidator
/// metadata store, but only if absent. Skipping when present preserves an
/// older version stamp so future migration routines can detect the gap
/// (current code version vs. stored version) and run an upgrade.
pub fn seed_consolidator_metadata_version(
    layout: &ConsolidatorLayout,
    dst_index: i32,
) -> Result<()> {
    if get_consolidator_property(layout, dst_index, SYNCLITE_METADATA_VERSION_KEY)?.is_some() {
        return Ok(());
    }
    upsert_consolidator_property(
        layout,
        dst_index,
        SYNCLITE_METADATA_VERSION_KEY,
        &SYNCLITE_METADATA_VERSION.to_string(),
    )
}

/// Java parity: `DeviceDstInitializer.markTableInitialized` — after a snapshot
/// is applied, persist per-table `initialization_status=INITIALIZED` and
/// `initial_rows=<rowCount>` for every user table present in the backup file.
/// Skips logger bookkeeping tables (synclite_txn, synclite_dbreader_checkpoint,
/// synclite_logreader_checkpoint) and sqlite_* internal tables. Honors
/// `ConsolidatorLayout::is_table_allowed` (Java's `ConfLoader.isAllowedTable`).
pub fn record_initial_table_stats(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    backup_path: &Path,
) -> Result<()> {
    let tables = enumerate_user_tables(backup_path)?;
    for (table, count) in tables {
        if !layout.is_table_allowed(&table) {
            continue;
        }
        upsert_consolidator_table_metadata(
            layout,
            dst_index,
            &layout.database_name,
            "",
            &table,
            "initialization_status",
            "INITIALIZED",
        )?;
        upsert_consolidator_table_metadata(
            layout,
            dst_index,
            &layout.database_name,
            "",
            &table,
            "initial_rows",
            &count.to_string(),
        )?;
    }
    Ok(())
}

/// Java parity: `DeviceDstInitializer.markTableInitializing` — before applying
/// per-table data, persist `initialization_status=INITIALIZING` for every user
/// table present in the backup. Crash-recovery semantics match Java.
pub fn mark_tables_initializing(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    backup_path: &Path,
) -> Result<()> {
    let tables = enumerate_user_tables(backup_path)?;
    for (table, _count) in tables {
        if !layout.is_table_allowed(&table) {
            continue;
        }
        upsert_consolidator_table_metadata(
            layout,
            dst_index,
            &layout.database_name,
            "",
            &table,
            "initialization_status",
            "INITIALIZING",
        )?;
    }
    Ok(())
}

/// Java parity: `ConsolidatorMetadataManager.upsertSchema` (LOCAL mode) — for
/// every user table in the backup, reads `PRAGMA table_info` and inserts one
/// row per column into the local `schema` table. No-op in DESTINATION mode
/// (Java's destination path persists schema separately via DeviceSyncProcessor).
pub fn record_initial_table_schemas(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    backup_path: &Path,
) -> Result<()> {
    if !backup_path.exists() {
        return Ok(());
    }
    if layout.metadata_store != MetadataStore::Local {
        return Ok(());
    }
    retry_dst(layout, "record_initial_table_schemas", || {
        let local_path = layout.consolidator_metadata_path(dst_index);
        ensure_parent_dir(&local_path)?;
        let mut local_conn = Connection::open(&local_path).map_err(map_sql_err)?;
        local_conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS schema(database_name TEXT, schema_name TEXT, table_name TEXT, \
                 column_index LONG, column_name TEXT, column_type TEXT, column_not_null INTEGER, \
                 column_default_value BLOB, column_primary_key INTEGER, column_auto_increment INTEGER);",
            )
            .map_err(map_sql_err)?;

        let backup_conn = Connection::open_with_flags(
            backup_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .map_err(map_sql_err)?;

        let mut stmt = backup_conn
            .prepare(
                "SELECT name, sql FROM sqlite_master \
                 WHERE type='table' \
                   AND name NOT IN ( \
                       'synclite_txn', \
                       'synclite_dbreader_checkpoint', \
                       'synclite_logreader_checkpoint', \
                       'replay_checkpoint', \
                       'sqlite_sequence' \
                   ) \
                 ORDER BY name",
            )
            .map_err(map_sql_err)?;
        let rows: Vec<(String, Option<String>)> = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)))
            .map_err(map_sql_err)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sql_err)?;
        drop(stmt);

        for (table, create_sql) in rows {
            if !layout.is_table_allowed(&table) {
                continue;
            }
            let autoincrement = create_sql
                .as_deref()
                .map(|s| s.to_ascii_uppercase().contains("AUTOINCREMENT"))
                .unwrap_or(false);

            let pragma = format!("PRAGMA table_info(\"{}\")", table.replace('"', "\"\""));
            let mut info_stmt = backup_conn.prepare(&pragma).map_err(map_sql_err)?;
            let cols: Vec<(i64, String, String, i64, Option<String>, i64)> = info_stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,            // cid
                        r.get::<_, String>(1)?,         // name
                        r.get::<_, String>(2)?,         // type
                        r.get::<_, i64>(3)?,            // notnull
                        r.get::<_, Option<String>>(4)?, // dflt_value
                        r.get::<_, i64>(5)?,            // pk
                    ))
                })
                .map_err(map_sql_err)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(map_sql_err)?;
            drop(info_stmt);

            let tx = local_conn.transaction().map_err(map_sql_err)?;
            tx.execute(
                "DELETE FROM schema WHERE database_name = ?1 AND table_name = ?2",
                params![layout.database_name, table],
            )
            .map_err(map_sql_err)?;
            for (cid, cname, ctype, notnull, default, pk) in cols {
                let is_auto = if autoincrement && pk == 1 { 1i64 } else { 0i64 };
                tx.execute(
                    "INSERT INTO schema(database_name, schema_name, table_name, column_index, \
                     column_name, column_type, column_not_null, column_default_value, \
                     column_primary_key, column_auto_increment) \
                     VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        layout.database_name,
                        Option::<String>::None,
                        table,
                        cid,
                        cname,
                        ctype,
                        notnull,
                        default,
                        pk,
                        is_auto,
                    ],
                )
                .map_err(map_sql_err)?;
            }
            tx.commit().map_err(map_sql_err)?;
        }
        let _ = dst_index;
        Ok(())
    })
}

fn enumerate_user_tables(backup_path: &Path) -> Result<Vec<(String, i64)>> {
    if !backup_path.exists() {
        return Ok(Vec::new());
    }
    let backup_conn = Connection::open_with_flags(
        backup_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(map_sql_err)?;
    let mut stmt = backup_conn
        .prepare(
            "SELECT name FROM sqlite_master \
             WHERE type='table' \
               AND name NOT IN ( \
                   'synclite_txn', \
                   'synclite_dbreader_checkpoint', \
                   'synclite_logreader_checkpoint', \
                   'replay_checkpoint', \
                   'sqlite_sequence' \
               ) \
             ORDER BY name",
        )
        .map_err(map_sql_err)?;
    let names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(map_sql_err)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(map_sql_err)?;
    drop(stmt);

    let mut out = Vec::with_capacity(names.len());
    for table in names {
        let count: i64 = backup_conn
            .query_row(
                &format!("SELECT COUNT(*) FROM \"{}\"", table.replace('"', "\"\"")),
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        out.push((table, count));
    }
    Ok(out)
}

// ---------- Read-side helpers (Java parity for bidirectional consumption) ----------

/// Java parity: `ConsolidatorMetadataManager.getInitializedTables`. Returns a
/// map of table_name -> initial_rows, sourced from the property store
/// (LOCAL or DESTINATION).
pub fn get_initialized_tables(
    layout: &ConsolidatorLayout,
    dst_index: i32,
) -> Result<std::collections::HashMap<String, i64>> {
    retry_dst(layout, "get_initialized_tables", || {
        let mut out = std::collections::HashMap::new();
        match layout.metadata_store {
            MetadataStore::Local => {
                let path = layout.consolidator_metadata_path(dst_index);
                if !path.exists() {
                    return Ok(out);
                }
                let conn = Connection::open(&path).map_err(map_sql_err)?;
                conn.execute_batch(CREATE_LOCAL_TABLE_METADATA_TBL_SQL)
                    .map_err(map_sql_err)?;
                let mut stmt = conn
                    .prepare(
                        "SELECT table_name, value FROM table_metadata WHERE key = 'initial_rows'",
                    )
                    .map_err(map_sql_err)?;
                let mut rows = stmt.query([]).map_err(map_sql_err)?;
                while let Some(row) = rows.next().map_err(map_sql_err)? {
                    let t: String = row.get(0).map_err(map_sql_err)?;
                    let v: String = row.get(1).map_err(map_sql_err)?;
                    if let Ok(n) = v.parse::<i64>() {
                        out.insert(t, n);
                    }
                }
            }
            MetadataStore::Destination => match layout.dst_type {
                DstType::Sqlite => {
                    let conn = Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
                    conn.execute_batch(CREATE_DST_TABLE_METADATA_TBL_SQL)
                        .map_err(map_sql_err)?;
                    let mut stmt = conn
                        .prepare(
                            "SELECT table_name, prop_value FROM synclite_consolidator_table_metadata \
                             WHERE synclite_device_id = ?1 AND synclite_device_name = ?2 AND prop_key = 'initial_rows'",
                        )
                        .map_err(map_sql_err)?;
                    let mut rows = stmt
                        .query(params![layout.device_id, layout.device_name])
                        .map_err(map_sql_err)?;
                    while let Some(row) = rows.next().map_err(map_sql_err)? {
                        let t: String = row.get(0).map_err(map_sql_err)?;
                        let v: String = row.get(1).map_err(map_sql_err)?;
                        if let Ok(n) = v.parse::<i64>() {
                            out.insert(t, n);
                        }
                    }
                }
                DstType::DuckDb => {
                    let conn = DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
                    conn.execute(CREATE_DST_TABLE_METADATA_TBL_SQL, []).map_err(map_duck_err)?;
                    let mut stmt = conn
                        .prepare(
                            "SELECT table_name, prop_value FROM synclite_consolidator_table_metadata \
                             WHERE synclite_device_id = ? AND synclite_device_name = ? AND prop_key = 'initial_rows'",
                        )
                        .map_err(map_duck_err)?;
                    let params: Vec<DuckValue> = vec![
                        DuckValue::Text(layout.device_id.clone()),
                        DuckValue::Text(layout.device_name.clone()),
                    ];
                    let mut rows = stmt
                        .query(duck_params_from_iter(params.iter()))
                        .map_err(map_duck_err)?;
                    while let Some(row) = rows.next().map_err(map_duck_err)? {
                        let t: String = row.get(0).map_err(map_duck_err)?;
                        let v: String = row.get(1).map_err(map_duck_err)?;
                        if let Ok(n) = v.parse::<i64>() {
                            out.insert(t, n);
                        }
                    }
                }
                DstType::Postgres => {
                    let conn_str = &layout.dst_connection_string;
                    let mut client = PgClient::connect(conn_str, NoTls).map_err(map_pg_err)?;
                    let create_tblmeta_sql = pg_create_dst_table_metadata_sql(layout);
                    client
                        .batch_execute(create_tblmeta_sql.as_str())
                        .map_err(|e| map_pg_err_with_sql(e, &create_tblmeta_sql))?;
                    let meta = pg_meta_table(layout, "synclite_consolidator_table_metadata");
                    let select_sql = format!(
                        "SELECT table_name, prop_value FROM {meta} \
                         WHERE synclite_device_id = $1 AND synclite_device_name = $2 AND prop_key = 'initial_rows'"
                    );
                    let rows = client
                        .query(select_sql.as_str(), &[&layout.device_id, &layout.device_name])
                        .map_err(|e| map_pg_err_with_sql(e, &select_sql))?;
                    for r in rows {
                        let t: String = r.get(0);
                        let v: String = r.get(1);
                        if let Ok(n) = v.parse::<i64>() {
                            out.insert(t, n);
                        }
                    }
                }
            },
        }
        Ok(out)
    })
}

/// One column of a loaded local schema row.
#[derive(Debug, Clone)]
pub struct LocalSchemaColumn {
    pub database_name: String,
    pub schema_name: Option<String>,
    pub table_name: String,
    pub column_index: i64,
    pub column_name: String,
    pub column_type: String,
    pub column_not_null: i64,
    pub column_default_value: Option<String>,
    pub column_primary_key: i64,
    pub column_auto_increment: i64,
}

/// Java parity: `ConsolidatorMetadataManager.loadSchemas` (LOCAL mode). Returns
/// raw rows from the local `schema` table; caller groups by table as needed.
/// Returns empty vec in DESTINATION mode (Java loads from destination separately).
pub fn load_local_schemas(
    layout: &ConsolidatorLayout,
    dst_index: i32,
) -> Result<Vec<LocalSchemaColumn>> {
    if layout.metadata_store != MetadataStore::Local {
        return Ok(Vec::new());
    }
    let path = layout.consolidator_metadata_path(dst_index);
    if !path.exists() {
        return Ok(Vec::new());
    }
    retry_dst(layout, "load_local_schemas", || {
        let conn = Connection::open(&path).map_err(map_sql_err)?;
        let mut stmt = conn
            .prepare(
                "SELECT database_name, schema_name, table_name, column_index, column_name, \
                 column_type, column_not_null, column_default_value, column_primary_key, \
                 column_auto_increment FROM schema \
                 ORDER BY database_name, schema_name, table_name, column_index",
            )
            .map_err(map_sql_err)?;
        let rows = stmt
            .query_map([], |row| {
                Ok(LocalSchemaColumn {
                    database_name: row.get(0)?,
                    schema_name: row.get(1)?,
                    table_name: row.get(2)?,
                    column_index: row.get(3)?,
                    column_name: row.get(4)?,
                    column_type: row.get(5)?,
                    column_not_null: row.get(6)?,
                    column_default_value: row.get(7)?,
                    column_primary_key: row.get(8)?,
                    column_auto_increment: row.get(9)?,
                })
            })
            .map_err(map_sql_err)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sql_err)?;
        Ok(rows)
    })
}

/// Java parity: `ConsolidatorMetadataManager.getConsolidatedSnapshotSize`.
/// Looks up `initialized_snapshot_name` in the property store and returns the
/// size of that snapshot file under `device_data_root`. Zero on any failure.
pub fn get_consolidated_snapshot_size(layout: &ConsolidatorLayout, dst_index: i32) -> u64 {
    let name = match get_consolidator_property(layout, dst_index, "initialized_snapshot_name") {
        Ok(Some(n)) => n,
        _ => return 0,
    };
    if name.is_empty() {
        return 0;
    }
    let p = layout.device_data_root.join(&name);
    std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0)
}

// ---------- Reset / delete operations (Java parity) ----------

/// Java parity: `ConsolidatorMetadataManager.resetSchemas` (LOCAL branch).
pub fn reset_local_schemas(layout: &ConsolidatorLayout, dst_index: i32) -> Result<()> {
    if layout.metadata_store != MetadataStore::Local {
        return Ok(());
    }
    let path = layout.consolidator_metadata_path(dst_index);
    if !path.exists() {
        return Ok(());
    }
    retry_dst(layout, "reset_local_schemas", || {
        let conn = Connection::open(&path).map_err(map_sql_err)?;
        conn.execute("DELETE FROM schema", []).map_err(map_sql_err)?;
        Ok(())
    })
}

/// Java parity: `ConsolidatorMetadataManager.resetTableMetadata` (LOCAL branch).
pub fn reset_local_table_metadata(layout: &ConsolidatorLayout, dst_index: i32) -> Result<()> {
    if layout.metadata_store != MetadataStore::Local {
        return Ok(());
    }
    let path = layout.consolidator_metadata_path(dst_index);
    if !path.exists() {
        return Ok(());
    }
    retry_dst(layout, "reset_local_table_metadata", || {
        let conn = Connection::open(&path).map_err(map_sql_err)?;
        conn.execute("DELETE FROM table_metadata", []).map_err(map_sql_err)?;
        Ok(())
    })
}

/// Java parity: `ConsolidatorMetadataManager.resetSchemas` (DESTINATION branch).
/// Deletes all per-table schema rows for this device — these now live in
/// `synclite_consolidator_table_metadata` with `prop_key = 'create_sql'`.
pub fn reset_dst_schemas(layout: &ConsolidatorLayout, _dst_index: i32) -> Result<()> {
    if layout.metadata_store != MetadataStore::Destination {
        return Ok(());
    }
    retry_dst(layout, "reset_dst_schemas", || match layout.dst_type {
        DstType::Sqlite => {
            let conn = Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
            conn.execute(
                "DELETE FROM synclite_consolidator_table_metadata WHERE synclite_device_id = ?1 AND synclite_device_name = ?2 AND prop_key = 'create_sql'",
                params![layout.device_id, layout.device_name],
            )
            .map_err(map_sql_err)?;
            Ok(())
        }
        DstType::DuckDb => {
            let conn = DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
            let p: Vec<DuckValue> = vec![
                DuckValue::Text(layout.device_id.clone()),
                DuckValue::Text(layout.device_name.clone()),
            ];
            conn.execute(
                "DELETE FROM synclite_consolidator_table_metadata WHERE synclite_device_id = ? AND synclite_device_name = ? AND prop_key = 'create_sql'",
                duck_params_from_iter(p.iter()),
            )
            .map_err(map_duck_err)?;
            Ok(())
        }
        DstType::Postgres => {
            let conn_str = &layout.dst_connection_string;
            let mut client = PgClient::connect(conn_str, NoTls).map_err(map_pg_err)?;
            client
                .execute(
                    "DELETE FROM synclite_consolidator_table_metadata WHERE synclite_device_id = $1 AND synclite_device_name = $2 AND prop_key = 'create_sql'",
                    &[&layout.device_id, &layout.device_name],
                )
                .map_err(map_pg_err)?;
            Ok(())
        }
    })
}

/// Java parity: `ConsolidatorMetadataManager.resetTableMetadata` (DESTINATION branch).
pub fn reset_dst_table_metadata(layout: &ConsolidatorLayout, _dst_index: i32) -> Result<()> {
    if layout.metadata_store != MetadataStore::Destination {
        return Ok(());
    }
    retry_dst(layout, "reset_dst_table_metadata", || match layout.dst_type {
        DstType::Sqlite => {
            let conn = Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
            conn.execute_batch(CREATE_DST_TABLE_METADATA_TBL_SQL).map_err(map_sql_err)?;
            conn.execute(
                "DELETE FROM synclite_consolidator_table_metadata WHERE synclite_device_id = ?1 AND synclite_device_name = ?2",
                params![layout.device_id, layout.device_name],
            )
            .map_err(map_sql_err)?;
            Ok(())
        }
        DstType::DuckDb => {
            let conn = DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
            conn.execute(CREATE_DST_TABLE_METADATA_TBL_SQL, []).map_err(map_duck_err)?;
            let p: Vec<DuckValue> = vec![
                DuckValue::Text(layout.device_id.clone()),
                DuckValue::Text(layout.device_name.clone()),
            ];
            conn.execute(
                "DELETE FROM synclite_consolidator_table_metadata WHERE synclite_device_id = ? AND synclite_device_name = ?",
                duck_params_from_iter(p.iter()),
            )
            .map_err(map_duck_err)?;
            Ok(())
        }
        DstType::Postgres => {
            let conn_str = &layout.dst_connection_string;
            let mut client = PgClient::connect(conn_str, NoTls).map_err(map_pg_err)?;
            client.batch_execute(CREATE_DST_TABLE_METADATA_TBL_SQL).map_err(map_pg_err)?;
            client
                .execute(
                    "DELETE FROM synclite_consolidator_table_metadata WHERE synclite_device_id = $1 AND synclite_device_name = $2",
                    &[&layout.device_id, &layout.device_name],
                )
                .map_err(map_pg_err)?;
            Ok(())
        }
    })
}

/// Java parity: `ConsolidatorMetadataManager.deleteSchema(srcTable)` (LOCAL branch).
pub fn delete_local_schema_for_table(
    layout: &ConsolidatorLayout,
    dst_index: i32,
    db: &str,
    table: &str,
) -> Result<()> {
    if layout.metadata_store != MetadataStore::Local {
        return Ok(());
    }
    let path = layout.consolidator_metadata_path(dst_index);
    if !path.exists() {
        return Ok(());
    }
    retry_dst(layout, "delete_local_schema_for_table", || {
        let conn = Connection::open(&path).map_err(map_sql_err)?;
        conn.execute(
            "DELETE FROM schema WHERE database_name = ?1 AND table_name = ?2",
            params![db, table],
        )
        .map_err(map_sql_err)?;
        Ok(())
    })
}



