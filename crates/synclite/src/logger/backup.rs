//! Eager backup-at-init (matching `BackupAgent` +
//! `MultiWriterDBProcessor.backupDB`).
//!
//! The Java logger snapshots the user DB the very first time a device
//! opens by **rebuilding** the schema in a fresh SQLite file and
//! copying rows table-by-table — it does **not** do an `fs::copy`.
//! That keeps the backup portable (always SQLite) and avoids fighting
//! the writer's exclusive file lock (especially DuckDB's).
//!
//! After the local backup is taken, it is copied into the stage subdir,
//! the metadata file is copied next to it (with `backup_shipped=1` set
//! in the staged copy), and finally the local backup file is deleted.
//! Flags `backup_taken` and `backup_shipped` in the live metadata file
//! gate subsequent runs so the work happens exactly once.

use std::path::Path;

use duckdb::Connection as DuckConnection;
use rusqlite::{params_from_iter, types::Value as SqlValue, Connection as SqlConn};
use logger_config::SyncLiteConfig;
use logger_core::{Backend, Error, Result};

use crate::layout::{ArchiveLayout, DeviceLayout};
use crate::metadata::Metadata;

/// Ensure the user DB already contains the Java-style `synclite_txn`
/// table before we take the first backup snapshot.
///
/// Java creates this table during DB initialization and only then invokes
/// the backup agent; the backup snapshot must therefore include it.
pub fn bootstrap_synclite_txn_table(db_path: &Path, dst_type: Backend) -> Result<()> {
    match dst_type {
        Backend::Sqlite => bootstrap_sqlite_txn_table(db_path),
        Backend::DuckDb => bootstrap_duckdb_txn_table(db_path),
    }
}

fn bootstrap_sqlite_txn_table(db_path: &Path) -> Result<()> {
    let conn = SqlConn::open(db_path).map_err(map_sql_err)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS synclite_txn(commit_id BIGINT PRIMARY KEY, operation_id BIGINT);",
    )
    .map_err(map_sql_err)?;
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM synclite_txn", [], |r| r.get(0))
        .map_err(map_sql_err)?;
    if count == 0 {
        conn.execute(
            "INSERT INTO synclite_txn(commit_id, operation_id) VALUES(0, 0)",
            [],
        )
        .map_err(map_sql_err)?;
    }
    Ok(())
}

fn bootstrap_duckdb_txn_table(db_path: &Path) -> Result<()> {
    let conn = DuckConnection::open(db_path).map_err(map_duck_err)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS synclite_txn(commit_id BIGINT PRIMARY KEY, operation_id BIGINT)",
        [],
    )
    .map_err(map_duck_err)?;
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM synclite_txn", [], |r| r.get(0))
        .map_err(map_duck_err)?;
    if count == 0 {
        conn.execute(
            "INSERT INTO synclite_txn(commit_id, operation_id) VALUES(0, 0)",
            [],
        )
        .map_err(map_duck_err)?;
    }
    Ok(())
}

/// Run the backup-and-ship sequence if it has not completed yet.
///
/// Must be called **before** the device opens the user DB so the
/// source-side reader connection isn't blocked by the writer's lock.
///
/// Java parity: each step is gated by the on-disk artifact, not just
/// the persisted flag. If the stage_subdir was wiped (or a new archive
/// folder is being created for the current device_name/UUID combo), we
/// re-stage the backup + metadata from the local snapshot — and the
/// local snapshot is kept for that reason.
pub fn run_initial_backup_if_needed(
    layout: &DeviceLayout,
    archive: &ArchiveLayout,
    metadata: &Metadata,
    dst_type: Backend,
    cfg: &SyncLiteConfig,
) -> Result<()> {
    std::fs::create_dir_all(&archive.stage_subdir)?;

    let backup_taken = metadata.get_i64("backup_taken")?.unwrap_or(0);

    // 1. Take the local backup via schema+row copy. Only if neither the
    //    flag nor the file says we already have one.
    if backup_taken != 1 || !layout.backup_local_path.exists() {
        if !cfg.use_precreated_data_backup.unwrap_or(false) {
            take_backup(&layout.db_path, &layout.backup_local_path, dst_type, cfg)?;
        }
        metadata.put_i64("backup_taken", 1)?;
    }

    // 2. Copy the backup into the stage subdir (idempotent by file
    //    existence so a fresh archive folder gets re-populated).
    if !archive.stage_backup_path.exists() {
        std::fs::copy(&layout.backup_local_path, &archive.stage_backup_path)?;
    }

    // 3. Copy the metadata file into the stage subdir; flag the copy
    //    shipped. Mutating the live metadata in place would race with
    //    subsequent updates from the mover callback.
    if !archive.stage_metadata_path.exists() {
        std::fs::copy(&layout.metadata_path, &archive.stage_metadata_path)?;
        let staged = Metadata::open_or_create(&archive.stage_metadata_path)?;
        staged.put_i64("backup_shipped", 1)?;
        drop(staged);
    }

    metadata.put_i64("backup_shipped", 1)?;
    Ok(())
}

/// Create the destination as a fresh SQLite file and copy
/// the schema + rows of every user table from `src` into it.
/// Mirrors `MultiWriterDBProcessor.backupDB`.
fn take_backup(src: &Path, dst: &Path, dst_type: Backend, cfg: &SyncLiteConfig) -> Result<()> {
    if let Some(p) = dst.parent() {
        std::fs::create_dir_all(p)?;
    }
    if dst.exists() {
        std::fs::remove_file(dst)
            .map_err(|e| Error::Config(format!("backup: remove existing dst: {e}")))?;
    }
    let dst_conn = SqlConn::open(dst).map_err(map_sql_err)?;
    let copied_any_tables = match dst_type {
        Backend::Sqlite => copy_from_sqlite(src, &dst_conn, cfg)?,
        Backend::DuckDb => copy_from_duckdb(src, &dst_conn, cfg)?,
    };
    if !copied_any_tables {
        materialize_empty_sqlite_backup(&dst_conn)?;
    } else if cfg.vacuum_data_backup.unwrap_or(true) {
        dst_conn
            .execute_batch("VACUUM;")
            .map_err(map_sql_err)?;
    }
    Ok(())
}

fn materialize_empty_sqlite_backup(dst: &SqlConn) -> Result<()> {
    // Ensure an empty initial backup is materialized as a valid SQLite file.
    dst.execute_batch(
        "BEGIN; \
         CREATE TABLE IF NOT EXISTS __synclite_backup_init__(x INTEGER); \
         DROP TABLE __synclite_backup_init__; \
         COMMIT; \
         VACUUM;",
    )
    .map_err(map_sql_err)?;
    Ok(())
}

#[derive(Debug, Clone)]
struct ColumnInfo {
    name: String,
    ty: String,
    notnull: bool,
    dflt: Option<String>,
    pk: bool,
}

fn build_create_table(table: &str, cols: &[ColumnInfo]) -> String {
    let mut parts = Vec::with_capacity(cols.len());
    for c in cols {
        let nn = if c.notnull { "NOT NULL" } else { "NULL" };
        let dflt = c
            .dflt
            .as_deref()
            .map(|v| format!("DEFAULT({v})"))
            .unwrap_or_default();
        let pk = if c.pk { "PRIMARY KEY" } else { "" };
        let piece = format!("{} {} {} {} {}", c.name, c.ty, nn, dflt, pk);
        parts.push(piece.split_whitespace().collect::<Vec<_>>().join(" "));
    }
    format!("CREATE TABLE IF NOT EXISTS {} ({})", table, parts.join(", "))
}

// ----- SQLite source --------------------------------------------------------

fn copy_from_sqlite(src: &Path, dst: &SqlConn, cfg: &SyncLiteConfig) -> Result<bool> {
    let src_conn = SqlConn::open(src).map_err(map_sql_err)?;
    let tables = filtered_tables(sqlite_tables(&src_conn)?, cfg);
    let copied_any_tables = !tables.is_empty();
    for table in tables {
        let cols = sqlite_columns(&src_conn, &table)?;
        if cols.is_empty() {
            continue;
        }
        dst.execute(&build_create_table(&table, &cols), [])
            .map_err(map_sql_err)?;
        let col_list: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        let select = format!("SELECT {} FROM {}", col_list.join(","), table);
        let placeholders = std::iter::repeat("?")
            .take(cols.len())
            .collect::<Vec<_>>()
            .join(",");
        let insert = format!(
            "INSERT INTO {}({}) VALUES ({})",
            table,
            col_list.join(","),
            placeholders
        );
        let mut sel = src_conn.prepare(&select).map_err(map_sql_err)?;
        let mut ins = dst.prepare(&insert).map_err(map_sql_err)?;
        let mut rows = sel.query([]).map_err(map_sql_err)?;
        while let Some(row) = rows.next().map_err(map_sql_err)? {
            let mut vals: Vec<SqlValue> = Vec::with_capacity(cols.len());
            for i in 0..cols.len() {
                let v: SqlValue = row.get(i).map_err(map_sql_err)?;
                vals.push(v);
            }
            ins.execute(params_from_iter(vals.iter()))
                .map_err(map_sql_err)?;
        }
    }
    Ok(copied_any_tables)
}

fn sqlite_tables(c: &SqlConn) -> Result<Vec<String>> {
    let mut s = c
        .prepare(
            "SELECT name FROM sqlite_master \
             WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        )
        .map_err(map_sql_err)?;
    let mapped = s
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(map_sql_err)?;
    let mut out = Vec::new();
    for r in mapped {
        out.push(r.map_err(map_sql_err)?);
    }
    Ok(out)
}

fn sqlite_columns(c: &SqlConn, t: &str) -> Result<Vec<ColumnInfo>> {
    let sql = format!("PRAGMA table_info({})", t);
    let mut s = c.prepare(&sql).map_err(map_sql_err)?;
    let mapped = s
        .query_map([], |r| {
            Ok(ColumnInfo {
                name: r.get::<_, String>(1)?,
                ty: r.get::<_, String>(2)?,
                notnull: r.get::<_, i64>(3)? != 0,
                dflt: r.get::<_, Option<String>>(4)?,
                pk: r.get::<_, i64>(5)? != 0,
            })
        })
        .map_err(map_sql_err)?;
    let mut out = Vec::new();
    for r in mapped {
        out.push(r.map_err(map_sql_err)?);
    }
    Ok(out)
}

// ----- DuckDB source --------------------------------------------------------

fn copy_from_duckdb(src: &Path, dst: &SqlConn, cfg: &SyncLiteConfig) -> Result<bool> {
    let src_conn = duckdb::Connection::open(src).map_err(map_duck_err)?;
    let tables = filtered_tables(duckdb_tables(&src_conn)?, cfg);
    let copied_any_tables = !tables.is_empty();
    for table in tables {
        let cols = duckdb_columns(&src_conn, &table)?;
        if cols.is_empty() {
            continue;
        }
        dst.execute(&build_create_table(&table, &cols), [])
            .map_err(map_sql_err)?;
        let col_list: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        let select = format!("SELECT {} FROM {}", col_list.join(","), table);
        let placeholders = std::iter::repeat("?")
            .take(cols.len())
            .collect::<Vec<_>>()
            .join(",");
        let insert = format!(
            "INSERT INTO {}({}) VALUES ({})",
            table,
            col_list.join(","),
            placeholders
        );
        let mut sel = src_conn.prepare(&select).map_err(map_duck_err)?;
        let mut ins = dst.prepare(&insert).map_err(map_sql_err)?;
        let mut rows = sel.query([]).map_err(map_duck_err)?;
        while let Some(row) = rows.next().map_err(map_duck_err)? {
            let mut vals: Vec<SqlValue> = Vec::with_capacity(cols.len());
            for i in 0..cols.len() {
                let dv: duckdb::types::Value = row.get(i).map_err(map_duck_err)?;
                vals.push(duck_to_sql(dv));
            }
            ins.execute(params_from_iter(vals.iter()))
                .map_err(map_sql_err)?;
        }
    }
    Ok(copied_any_tables)
}

fn duckdb_tables(c: &duckdb::Connection) -> Result<Vec<String>> {
    let mut s = c
        .prepare(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema='main'",
        )
        .map_err(map_duck_err)?;
    let mapped = s
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(map_duck_err)?;
    let mut out = Vec::new();
    for r in mapped {
        out.push(r.map_err(map_duck_err)?);
    }
    Ok(out)
}

fn duckdb_columns(c: &duckdb::Connection, t: &str) -> Result<Vec<ColumnInfo>> {
    let sql = format!("PRAGMA table_info({})", t);
    let mut s = c.prepare(&sql).map_err(map_duck_err)?;
    let mapped = s
        .query_map([], |r| {
            Ok(ColumnInfo {
                name: r.get::<_, String>(1)?,
                ty: r.get::<_, String>(2)?,
                notnull: r.get::<_, bool>(3).unwrap_or(false),
                dflt: r.get::<_, Option<String>>(4).unwrap_or(None),
                pk: r.get::<_, bool>(5).unwrap_or(false),
            })
        })
        .map_err(map_duck_err)?;
    let mut out = Vec::new();
    for r in mapped {
        out.push(r.map_err(map_duck_err)?);
    }
    Ok(out)
}

fn duck_to_sql(v: duckdb::types::Value) -> SqlValue {
    use duckdb::types::Value as DV;
    match v {
        DV::Null => SqlValue::Null,
        DV::Boolean(b) => SqlValue::Integer(if b { 1 } else { 0 }),
        DV::TinyInt(n) => SqlValue::Integer(n as i64),
        DV::SmallInt(n) => SqlValue::Integer(n as i64),
        DV::Int(n) => SqlValue::Integer(n as i64),
        DV::BigInt(n) => SqlValue::Integer(n),
        DV::Float(f) => SqlValue::Real(f as f64),
        DV::Double(f) => SqlValue::Real(f),
        DV::Text(s) => SqlValue::Text(s),
        DV::Blob(b) => SqlValue::Blob(b),
        other => SqlValue::Text(format!("{other:?}")),
    }
}

fn filtered_tables(mut tables: Vec<String>, cfg: &SyncLiteConfig) -> Vec<String> {
    if let Some(include) = &cfg.include_tables {
        tables.retain(|t| include.iter().any(|name| name.eq_ignore_ascii_case(t)));
    }
    if let Some(exclude) = &cfg.exclude_tables {
        tables.retain(|t| !exclude.iter().any(|name| name.eq_ignore_ascii_case(t)));
    }
    tables
}

fn map_sql_err(e: rusqlite::Error) -> Error {
    Error::Config(format!("backup: {e}"))
}

fn map_duck_err(e: duckdb::Error) -> Error {
    Error::Config(format!("backup: {e}"))
}




