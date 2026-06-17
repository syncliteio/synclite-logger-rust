//! Per-device background consolidator scaffold.
//!
//! This is the Rust-side entry point for translating consolidator behavior
//! into the logger runtime. It is intentionally scoped to device-local state
//! so multiple devices can run concurrently without a shared job workDir.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::env;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use consolidator_core::{ApplyProgress, ConsolidatorLayout, DstIdempotentDataIngestionMethod, DstType, DestinationSyncMode, MetadataStore};
use consolidator_state::*;
use duckdb::{params_from_iter as duck_params_from_iter, types::Value as DuckValue, Connection as DuckConnection};
use fs2::FileExt;
use libloading::Library;
use postgres::{Client as PgClient, NoTls};
use rusqlite::{params, params_from_iter, types::Value as SqlValue, Connection, OptionalExtension};
use logger_core::{Error, Result};
use synclite_observability::{tracer_debug, tracer_error, tracer_info, TraceLevel, Tracer};

pub mod monitor;

thread_local! {
    /// Per-worker Java-parity device tracer (writes
    /// `<deviceRoot>/synclite_device.trace`). Set once at the top of
    /// `worker_loop` so deep-call-site `exec_sql` etc. can emit DEBUG SQL
    /// traces without threading the tracer through every signature.
    static DEVICE_TRACER: std::cell::RefCell<Option<Arc<Tracer>>> =
        std::cell::RefCell::new(None);

    // Java parity (`DeviceSyncProcessor.inMemoryReplicaConn`): one long-lived
    // in-memory SQLite schema replica per consolidator worker thread (i.e.
    // per device-dst pair). Seeded lazily on first use, then mutated in place
    // as DDL ops flow through `derive_commandlog_ddl_columns`. Reused across
    // every segment apply for the lifetime of the worker thread so we never
    // pay the seed cost more than once.
    static SCHEMA_REPLICA: std::cell::RefCell<Option<Connection>> =
        const { std::cell::RefCell::new(None) };
}

fn with_device_tracer<F: FnOnce(&Arc<Tracer>)>(f: F) {
    DEVICE_TRACER.with(|cell| {
        if let Some(t) = cell.borrow().as_ref() {
            f(t);
        }
    });
}

/// Take-or-init the thread-local schema replica. On drop the connection is
/// returned to the thread-local so subsequent segment applies reuse it.
struct SchemaReplicaGuard {
    conn: Option<Connection>,
}

impl SchemaReplicaGuard {
    fn as_mut(&mut self) -> &mut Connection {
        self.conn.as_mut().expect("schema replica guard taken")
    }
}

impl Drop for SchemaReplicaGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            SCHEMA_REPLICA.with(|cell| {
                *cell.borrow_mut() = Some(conn);
            });
        }
    }
}

fn acquire_schema_replica(layout: &ConsolidatorLayout) -> Result<SchemaReplicaGuard> {
    let existing = SCHEMA_REPLICA.with(|cell| cell.borrow_mut().take());
    let conn = match existing {
        Some(c) => c,
        None => seed_commandlog_schema_replica(layout)?,
    };
    Ok(SchemaReplicaGuard { conn: Some(conn) })
}

// Java parity: `com.synclite.consolidator.processor.DeviceLogCleaner`
// persists its cleanup watermark in the per-device metadata DB under this
// key (see `DeviceLogCleaner.LAST_CLEANED_KEY`). Must match Java exactly.
const LAST_CLEANED_SEGMENT_KEY: &str = "last_cleaned_log_segment_seq_num";
const LAST_STAGE_CLEANED_SEGMENT_KEY: &str = "last_stage_cleaned_log_segment_sequence_number";
const DEVICE_PROCESSING_LOCK_FILE_NAME: &str = "synclite_device_processing.lock";
// Java parity: `SyncLiteDeviceInfo.getMetadataFileName()`. Per-device
// consolidator metadata DB, sibling of `state_db_path` inside
// `device_work_dir`. Holds device-scoped (not destination-scoped) state
// such as the DeviceLogCleaner watermark.
const DEVICE_METADATA_FILE_NAME: &str = "synclite_device_metadata.db";

struct DeviceProcessingLock {
    file: File,
}

impl DeviceProcessingLock {
    fn try_lock(layout: &ConsolidatorLayout) -> Result<Option<Self>> {
        std::fs::create_dir_all(&layout.device_work_dir)?;
        let lock_path = layout
            .device_work_dir
            .join(DEVICE_PROCESSING_LOCK_FILE_NAME);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)?;

        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(Self { file })),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(Error::Config(format!(
                "consolidator: failed to lock device work dir {}: {e}",
                layout.device_work_dir.display()
            ))),
        }
    }
}

impl Drop for DeviceProcessingLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DmlKind {
    Insert,
    Update,
    Delete,
    Other,
}

fn classify_sql_kind(sql: &str) -> DmlKind {
    let normalized = sql.trim_start().to_ascii_uppercase();
    if normalized.starts_with("INSERT") {
        DmlKind::Insert
    } else if normalized.starts_with("UPDATE") {
        DmlKind::Update
    } else if normalized.starts_with("DELETE") {
        DmlKind::Delete
    } else {
        DmlKind::Other
    }
}

#[cfg(test)]
fn batch_limit_for_sql(layout: &ConsolidatorLayout, sql: &str) -> usize {
    match classify_sql_kind(sql) {
        DmlKind::Insert => layout.dst_insert_batch_size.max(1) as usize,
        DmlKind::Update => layout.dst_update_batch_size.max(1) as usize,
        DmlKind::Delete => layout.dst_delete_batch_size.max(1) as usize,
        DmlKind::Other => 1,
    }
}

fn batch_limit_for_kind(layout: &ConsolidatorLayout, kind: DmlKind) -> usize {
    match kind {
        DmlKind::Insert => layout.dst_insert_batch_size.max(1) as usize,
        DmlKind::Update => layout.dst_update_batch_size.max(1) as usize,
        DmlKind::Delete => layout.dst_delete_batch_size.max(1) as usize,
        DmlKind::Other => 1,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MapperKind {
    Consolidation,
    Replication,
}

#[derive(Debug, Clone)]
struct MappedOperation {
    change_number: i64,
    commit_id: i64,
    kind: DmlKind,
    table_name: Option<String>,
    op_type: Option<String>,
    ddl_columns: Vec<CdcSchemaColumn>,
    #[allow(dead_code)]
    source: SegmentSource,
    mapper: MapperKind,
    is_system_table: bool,
    sql: String,
    args: Vec<SqlValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MappedOperationKey {
    mapper: MapperKind,
    kind: DmlKind,
    table_name: Option<String>,
    sql: String,
}

#[derive(Debug, Clone)]
struct CachedMappedOperation {
    sql: String,
    reusable_args: Vec<SqlValue>,
}

enum ApplyAction {
    Execute(MappedOperation),
    Skip,
}

enum GeneratedSql<'a> {
    Parameterized { sql: &'a str, args: &'a [SqlValue] },
    Text(String),
}

struct DestinationSqlGenerator {
    backend: DstType,
}

impl DestinationSqlGenerator {
    fn new(backend: DstType) -> Self {
        Self { backend }
    }

    fn generate<'a>(&self, cached: &'a CachedMappedOperation) -> GeneratedSql<'a> {
        match self.backend {
            DstType::Sqlite | DstType::DuckDb => GeneratedSql::Parameterized {
                sql: &cached.sql,
                args: &cached.reusable_args,
            },
            DstType::Postgres => {
                GeneratedSql::Text(render_sql_with_args(&cached.sql, &cached.reusable_args))
            }
        }
    }
}

fn user_mapper_for_mode(mode: DestinationSyncMode) -> MapperKind {
    match mode {
        DestinationSyncMode::Consolidation => MapperKind::Consolidation,
        DestinationSyncMode::Replication => MapperKind::Replication,
    }
}

fn mapper_for_entry(layout: &ConsolidatorLayout, entry: &SegmentEntry) -> MapperKind {
    if layout.destination_sync_mode == DestinationSyncMode::Consolidation {
        return MapperKind::Consolidation;
    }

    let user_mapper = user_mapper_for_mode(layout.destination_sync_mode);
    let table_name = operation_table_name(entry);
    if table_name
        .as_deref()
        .map(is_system_table_name)
        .unwrap_or(false)
    {
        MapperKind::Consolidation
    } else {
        user_mapper
    }
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

fn is_system_table_name(name: &str) -> bool {
    let n = name.trim().trim_matches('"').to_ascii_uppercase();
    n.starts_with("SYNCLITE_")
        || n == "CHECKPOINT"
        || n == "DASHBOARD"
        || n == "DEVICE_STATUS"
}

fn normalize_table_name(name: &str) -> String {
    name.trim()
        .trim_matches('"')
        .rsplit('.')
        .next()
        .unwrap_or("")
        .trim_matches('"')
        .to_ascii_uppercase()
}

fn is_blocked_table_name(name: &str) -> bool {
    let normalized = normalize_table_name(name);
    normalized == "SYNCLITE_TXN" || normalized == "REPLAY_CHECKPOINT"
}

fn extract_table_name_for_dml(sql: &str) -> Option<String> {
    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    let source = if upper.starts_with("INSERT") {
        extract_token_after_keyword(trimmed, "INTO")
    } else if upper.starts_with("UPDATE") {
        Some(trimmed["UPDATE".len()..].trim_start())
    } else if upper.starts_with("DELETE") {
        extract_token_after_keyword(trimmed, "FROM")
    } else {
        None
    }?;

    let token = source
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .trim_end_matches(';');
    let token = token.split('(').next().unwrap_or("").trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

fn extract_table_name_for_operation(sql: &str) -> Option<String> {
    if let Some(name) = extract_table_name_for_dml(sql) {
        return Some(name);
    }

    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    let source = if upper.starts_with("CREATE TABLE") {
        let rest = trimmed["CREATE TABLE".len()..].trim_start();
        Some(skip_if_not_exists(rest))
    } else if upper.starts_with("DROP TABLE") {
        let rest = trimmed["DROP TABLE".len()..].trim_start();
        Some(skip_if_exists(rest))
    } else if upper.starts_with("ALTER TABLE") {
        Some(trimmed["ALTER TABLE".len()..].trim_start())
    } else {
        None
    }?;

    let token = source
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .trim_end_matches(';');
    let token = token.split('(').next().unwrap_or("").trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

fn extract_token_after_keyword<'a>(sql: &'a str, keyword: &str) -> Option<&'a str> {
    let upper = sql.to_ascii_uppercase();
    let idx = upper.find(keyword)?;
    Some(sql[idx + keyword.len()..].trim_start())
}

fn skip_if_not_exists(s: &str) -> &str {
    let trimmed = s.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("IF NOT EXISTS ") {
        return trimmed["IF NOT EXISTS".len()..].trim_start();
    }
    trimmed
}

fn skip_if_exists(s: &str) -> &str {
    let trimmed = s.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("IF EXISTS ") {
        return trimmed["IF EXISTS".len()..].trim_start();
    }
    trimmed
}

fn operation_table_name(entry: &SegmentEntry) -> Option<String> {
    entry
        .table_name
        .clone()
        .or_else(|| extract_table_name_for_operation(&entry.sql))
}

fn operation_op_type(entry: &SegmentEntry) -> Option<String> {
    if let Some(op) = &entry.op_type {
        return Some(op.to_ascii_uppercase());
    }
    Some(map_cdclog_op_type(&entry.sql).to_string())
}

fn map_operation_for_destination(backend: DstType, entry: &SegmentEntry) -> ApplyAction {
    let normalized = entry.sql.trim_start().to_ascii_uppercase();
    if let Some(table_name) = operation_table_name(entry) {
        if is_blocked_table_name(&table_name) {
            return ApplyAction::Skip;
        }
    }
    if normalized.starts_with("BEGIN")
        || normalized.starts_with("COMMIT")
        || normalized.starts_with("ROLLBACK")
        || normalized.starts_with("CHECKPOINT")
        || normalized.starts_with("PRAGMA")
    {
        return ApplyAction::Skip;
    }
    if backend == DstType::Postgres
        && (normalized.starts_with("VACUUM") || normalized.starts_with("ANALYZE"))
    {
        return ApplyAction::Skip;
    }

    ApplyAction::Execute(MappedOperation {
        change_number: entry.change_number,
        commit_id: entry.commit_id,
        kind: classify_sql_kind(&entry.sql),
        table_name: operation_table_name(entry),
        op_type: operation_op_type(entry),
        ddl_columns: entry.ddl_columns.clone(),
        source: entry.source,
        mapper: MapperKind::Consolidation,
        is_system_table: operation_table_name(entry)
            .as_deref()
            .map(is_system_table_name)
            .unwrap_or(false),
        sql: entry.sql.clone(),
        args: entry.args.clone(),
    })
}

fn map_operation_using_mapper(layout: &ConsolidatorLayout, entry: &SegmentEntry) -> ApplyAction {
    match map_operation_for_destination(layout.dst_type, entry) {
        ApplyAction::Skip => ApplyAction::Skip,
        ApplyAction::Execute(mut mapped) => {
            mapped.mapper = mapper_for_entry(layout, entry);
            if mapped.mapper == MapperKind::Consolidation && should_skip_consolidation_ddl(&mapped) {
                return ApplyAction::Skip;
            }
            // Java parity: per-destination FilterMapper + ValueMapper run BEFORE
            // consolidation metadata wrapping. Mirrors DeviceConsolidator.java
            // lines 443-525 (filter/rename tables, filter/rename columns, value
            // mapping) and DeviceEventStreamer.java lines 789-820.
            mapped = match apply_filter_value_mappers(layout, mapped) {
                ApplyAction::Skip => return ApplyAction::Skip,
                ApplyAction::Execute(m) => m,
            };
            if mapped.mapper == MapperKind::Consolidation || mapped.is_system_table {
                mapped = apply_consolidation_mapping(layout, mapped);
            } else {
                // Replication mapper: still qualify table identifiers so DDL
                // and DML land in the configured dst schema, not session
                // search_path. Safer and simpler than relying on search_path.
                mapped = qualify_replication_sql(layout, mapped);
            }
            ApplyAction::Execute(mapped)
        }
    }
}

fn qualify_replication_sql(layout: &ConsolidatorLayout, mut mapped: MappedOperation) -> MappedOperation {
    if mapped.is_system_table {
        return mapped;
    }
    // Java parity: replication DDL must also be rebuilt from ddl_columns so
    // (a) filter-mapper column drops/renames take effect on CREATE TABLE /
    // ADD COLUMN, (b) identifiers are properly quoted, schema-qualified, and
    // (c) DataTypeMapper translates source types to dst-dialect types.
    // qualify-only would leave the placeholder sentinel SQL written by
    // apply_filter_value_mappers unchanged.
    if matches!(mapped.kind, DmlKind::Other) {
        let sql = mapped.sql.trim().to_string();
        let rewritten = rewrite_replication_ddl_sql(layout, &mapped, &sql);
        return MappedOperation { sql: rewritten, ..mapped };
    }
    let Some(tname) = mapped.table_name.as_deref().filter(|s| !s.is_empty()) else {
        return mapped;
    };
    let qualified = qualify_table(layout, tname);
    if qualified.is_empty() || qualified == quote_table(layout, tname) {
        return mapped;
    }
    mapped.sql = rename_table_in_sql(&mapped.sql, tname, &qualified);
    mapped
}

/// Java parity: apply per-destination FilterMapper (table allow/block/rename,
/// column allow/block/rename) and ValueMapper (per (table,column) value
/// substitution) to a mapped operation before consolidation metadata is added.
///
/// Mirrors com.synclite.consolidator.processor.DeviceConsolidator semantics:
/// * Table blocked by FilterMapper -> `ApplyAction::Skip`.
/// * Table renamed -> rewrite SQL to reference dst table name.
/// * For DML with parseable column list: drop blocked-column args, rename
///   surviving column names, transform each surviving value via ValueMapper.
/// * For CREATE TABLE / ADD COLUMN DDL: filter+rename columns; if all
///   columns are blocked or the column itself is blocked, skip the op.
///
/// System tables (synclite_*) and entries marked is_system_table are
/// pass-through (Java skips filter/value mappers for internal bookkeeping).
fn apply_filter_value_mappers(layout: &ConsolidatorLayout, mut mapped: MappedOperation) -> ApplyAction {
    if mapped.is_system_table {
        return ApplyAction::Execute(mapped);
    }
    let filter = &layout.filter_mapper;
    let value_mapper = &layout.value_mapper;
    if !filter.enabled && !value_mapper.enabled {
        return ApplyAction::Execute(mapped);
    }

    let src_table = match mapped.table_name.as_deref() {
        Some(t) if !t.is_empty() => t.to_string(),
        _ => return ApplyAction::Execute(mapped),
    };

    let dst_table = match filter.mapped_table_name(&src_table) {
        Some(t) => t,
        None => return ApplyAction::Skip,
    };
    let table_renamed = !dst_table.eq_ignore_ascii_case(&src_table);

    match mapped.kind {
        DmlKind::Insert => {
            let parsed = parse_insert_sql(&mapped.sql);
            if let Some((_, cols)) = parsed {
                let arg_cnt = mapped.args.len();
                if cols.len() != arg_cnt {
                    if table_renamed {
                        mapped.sql = rename_table_in_sql(&mapped.sql, &src_table, &dst_table);
                        mapped.table_name = Some(dst_table);
                    }
                    return ApplyAction::Execute(mapped);
                }
                let mut new_cols: Vec<String> = Vec::with_capacity(arg_cnt);
                let mut new_args: Vec<SqlValue> = Vec::with_capacity(arg_cnt);
                for (i, col) in cols.iter().enumerate() {
                    let mapped_col = match filter.mapped_column_name(&src_table, col) {
                        Some(c) => c,
                        None => continue, // blocked: drop arg
                    };
                    let v = apply_value_mapping(value_mapper, &src_table, col, &mapped.args[i]);
                    new_cols.push(mapped_col);
                    new_args.push(v);
                }
                if new_cols.is_empty() {
                    return ApplyAction::Skip;
                }
                let placeholders = (0..new_args.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
                let qcols: Vec<String> = new_cols.iter().map(|c| quote_col(layout, c)).collect();
                mapped.sql = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    qualify_table(layout, &dst_table),
                    qcols.join(", "),
                    placeholders
                );
                mapped.args = new_args;
                mapped.table_name = Some(dst_table);
            } else if table_renamed {
                mapped.sql = rename_table_in_sql(&mapped.sql, &src_table, &dst_table);
                mapped.table_name = Some(dst_table);
            }
            ApplyAction::Execute(mapped)
        }
        DmlKind::Update => {
            let parsed = parse_update_sql(&mapped.sql);
            if let Some((_, set_cols, where_cols)) = parsed {
                let n = mapped.args.len();
                if set_cols.len() + where_cols.len() != n {
                    if table_renamed {
                        mapped.sql = rename_table_in_sql(&mapped.sql, &src_table, &dst_table);
                        mapped.table_name = Some(dst_table);
                    }
                    return ApplyAction::Execute(mapped);
                }
                // Java DeviceConsolidator: build_callback_sql emits UPDATE with
                // SET <cols> = ? WHERE <cols> = ?, where both halves use the
                // same column list. Args are ordered set-image then where-image.
                let set_args = &mapped.args[..set_cols.len()];
                let where_args = &mapped.args[set_cols.len()..];
                let mut new_set_cols: Vec<String> = Vec::with_capacity(set_cols.len());
                let mut new_set_args: Vec<SqlValue> = Vec::with_capacity(set_cols.len());
                for (i, col) in set_cols.iter().enumerate() {
                    let mapped_col = match filter.mapped_column_name(&src_table, col) {
                        Some(c) => c,
                        None => continue,
                    };
                    let v = apply_value_mapping(value_mapper, &src_table, col, &set_args[i]);
                    new_set_cols.push(mapped_col);
                    new_set_args.push(v);
                }
                let mut new_where_cols: Vec<String> = Vec::with_capacity(where_cols.len());
                let mut new_where_args: Vec<SqlValue> = Vec::with_capacity(where_cols.len());
                for (i, col) in where_cols.iter().enumerate() {
                    let mapped_col = match filter.mapped_column_name(&src_table, col) {
                        Some(c) => c,
                        None => continue,
                    };
                    let v = apply_value_mapping(value_mapper, &src_table, col, &where_args[i]);
                    new_where_cols.push(mapped_col);
                    new_where_args.push(v);
                }
                if new_set_cols.is_empty() || new_where_cols.is_empty() {
                    return ApplyAction::Skip;
                }
                let set_clause = new_set_cols.iter().map(|c| format!("{} = ?", quote_col(layout, c))).collect::<Vec<_>>().join(", ");
                let where_clause = new_where_cols.iter().map(|c| format!("{} = ?", quote_col(layout, c))).collect::<Vec<_>>().join(" AND ");
                mapped.sql = format!("UPDATE {} SET {} WHERE {}", qualify_table(layout, &dst_table), set_clause, where_clause);
                let mut new_args = new_set_args;
                new_args.extend(new_where_args);
                mapped.args = new_args;
                mapped.table_name = Some(dst_table);
            } else if table_renamed {
                mapped.sql = rename_table_in_sql(&mapped.sql, &src_table, &dst_table);
                mapped.table_name = Some(dst_table);
            }
            ApplyAction::Execute(mapped)
        }
        DmlKind::Delete => {
            let parsed = parse_delete_sql(&mapped.sql);
            if let Some((_, where_cols)) = parsed {
                let arg_cnt = mapped.args.len();
                if where_cols.len() != arg_cnt {
                    if table_renamed {
                        mapped.sql = rename_table_in_sql(&mapped.sql, &src_table, &dst_table);
                        mapped.table_name = Some(dst_table);
                    }
                    return ApplyAction::Execute(mapped);
                }
                let mut new_cols: Vec<String> = Vec::with_capacity(arg_cnt);
                let mut new_args: Vec<SqlValue> = Vec::with_capacity(arg_cnt);
                for (i, col) in where_cols.iter().enumerate() {
                    let mapped_col = match filter.mapped_column_name(&src_table, col) {
                        Some(c) => c,
                        None => continue,
                    };
                    let v = apply_value_mapping(value_mapper, &src_table, col, &mapped.args[i]);
                    new_cols.push(mapped_col);
                    new_args.push(v);
                }
                if new_cols.is_empty() {
                    return ApplyAction::Skip;
                }
                let where_clause = new_cols.iter().map(|c| format!("{} = ?", quote_col(layout, c))).collect::<Vec<_>>().join(" AND ");
                mapped.sql = format!("DELETE FROM {} WHERE {}", qualify_table(layout, &dst_table), where_clause);
                mapped.args = new_args;
                mapped.table_name = Some(dst_table);
            } else if table_renamed {
                mapped.sql = rename_table_in_sql(&mapped.sql, &src_table, &dst_table);
                mapped.table_name = Some(dst_table);
            }
            ApplyAction::Execute(mapped)
        }
        DmlKind::Other => {
            // DDL: filter+rename columns/table in ddl_columns and let the
            // downstream DDL rewriter (consolidation or replication) emit
            // properly quoted, schema-qualified, type-mapped SQL. We avoid
            // hand-formatting CREATE/ALTER here — those paths know about
            // identifier quoting, schema scope, and DataTypeMapper.
            let op = mapped.op_type.as_deref().unwrap_or_default().to_ascii_uppercase();
            if op == "CREATETABLE" && !mapped.ddl_columns.is_empty() {
                let mut filtered: Vec<CdcSchemaColumn> = Vec::new();
                for c in &mapped.ddl_columns {
                    match filter.mapped_column_name(&src_table, &c.column_name) {
                        Some(new_name) => {
                            let mut nc = c.clone();
                            nc.column_name = new_name;
                            filtered.push(nc);
                        }
                        None => continue,
                    }
                }
                if filtered.is_empty() {
                    return ApplyAction::Skip;
                }
                mapped.ddl_columns = filtered;
                mapped.table_name = Some(dst_table.clone());
                // Sentinel; rewrite_{consolidation,replication}_ddl_sql
                // rebuild from ddl_columns and ignore this string.
                mapped.sql = format!("CREATE TABLE {} (...)", dst_table);
            } else if op == "ADDCOLUMN" {
                if let Some(col) = mapped.ddl_columns.iter().find(|c| !c.column_name.is_empty()) {
                    match filter.mapped_column_name(&src_table, &col.column_name) {
                        Some(new_name) => {
                            let renamed_col = !new_name.eq_ignore_ascii_case(&col.column_name);
                            if renamed_col || table_renamed {
                                let mut new_col = col.clone();
                                new_col.column_name = new_name;
                                mapped.ddl_columns = vec![new_col];
                                mapped.table_name = Some(dst_table.clone());
                                // Sentinel; downstream DDL rewriter emits
                                // the real ALTER TABLE ... ADD COLUMN with
                                // quoting + type mapping.
                                mapped.sql = format!("ALTER TABLE {} ADD COLUMN ...", dst_table);
                            }
                        }
                        None => return ApplyAction::Skip,
                    }
                } else if table_renamed {
                    mapped.sql = rename_table_in_sql(&mapped.sql, &src_table, &dst_table);
                    mapped.table_name = Some(dst_table);
                }
            } else if op == "RENAMETABLE" && table_renamed {
                mapped.sql = rename_table_in_sql(&mapped.sql, &src_table, &dst_table);
                mapped.table_name = Some(dst_table);
            } else if table_renamed {
                mapped.sql = rename_table_in_sql(&mapped.sql, &src_table, &dst_table);
                mapped.table_name = Some(dst_table);
            }
            ApplyAction::Execute(mapped)
        }
    }
}

/// Java parity: `ValueMapper.mapValue`. Stringifies value via
/// `String.valueOf` semantics (NULL -> "null"), looks up in the per-(table,
/// column) map, and returns either the mapped string as SqlValue::Text or
/// the original value unchanged.
fn apply_value_mapping(
    value_mapper: &consolidator_core::ValueMapperRules,
    table: &str,
    column: &str,
    value: &SqlValue,
) -> SqlValue {
    if !value_mapper.enabled {
        return value.clone();
    }
    let stringified = sql_value_for_value_mapper(value);
    if let Some(mapped) = value_mapper.mapped_value(table, column, &stringified) {
        return SqlValue::Text(mapped);
    }
    // SQL CHAR(n) values arrive blank-padded; per SQL semantics trailing
    // spaces are insignificant for comparison, so retry the lookup using
    // the right-trimmed key before falling through to the source value.
    let trimmed = stringified.trim_end_matches(' ');
    if trimmed.len() != stringified.len() {
        if let Some(mapped) = value_mapper.mapped_value(table, column, trimmed) {
            return SqlValue::Text(mapped);
        }
    }
    value.clone()
}

fn sql_value_for_value_mapper(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "null".to_string(),
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Real(f) => f.to_string(),
        SqlValue::Text(s) => s.clone(),
        SqlValue::Blob(b) => {
            // Java's String.valueOf(byte[]) yields an object-identity string;
            // unmappable in practice. Use hex so equality lookups are at
            // least deterministic if a user ever tried to map a blob.
            let mut s = String::with_capacity(b.len() * 2);
            for byte in b {
                s.push_str(&format!("{:02X}", byte));
            }
            s
        }
    }
}

/// Parse `INSERT INTO <table>(<col_list>) VALUES (?, ?, ?)` into
/// `(table, columns)`. Returns None when the SQL lacks an explicit column
/// list (e.g., `INSERT INTO t VALUES (...)`) -- Java's FilterMapper requires
/// the column list to position-map args.
fn parse_insert_sql(sql: &str) -> Option<(String, Vec<String>)> {
    let upper = sql.to_ascii_uppercase();
    if !upper.trim_start().starts_with("INSERT") {
        return None;
    }
    let into_idx = upper.find("INTO")?;
    let after_into = sql[into_idx + 4..].trim_start();
    let paren_open = after_into.find('(')?;
    let table = after_into[..paren_open]
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string();
    if table.is_empty() {
        return None;
    }
    let cols_start = paren_open + 1;
    let rest = &after_into[cols_start..];
    let paren_close = find_matching_close_paren(rest)?;
    let cols_body = &rest[..paren_close];
    let cols = split_csv_identifiers(cols_body);
    if cols.is_empty() {
        return None;
    }
    Some((table, cols))
}

/// Parse `UPDATE <table> SET <col> = ?, <col> = ? WHERE <col> = ? AND <col> = ?`
/// into `(table, set_cols, where_cols)`.
fn parse_update_sql(sql: &str) -> Option<(String, Vec<String>, Vec<String>)> {
    let upper = sql.to_ascii_uppercase();
    if !upper.trim_start().starts_with("UPDATE") {
        return None;
    }
    let update_pos = upper.find("UPDATE")?;
    let after_update = sql[update_pos + 6..].trim_start();
    let set_rel = after_update.to_ascii_uppercase().find(" SET ")?;
    let table = after_update[..set_rel]
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string();
    if table.is_empty() {
        return None;
    }
    let after_set = &after_update[set_rel + 5..];
    let where_rel = after_set.to_ascii_uppercase().find(" WHERE ");
    let (set_clause, where_clause) = match where_rel {
        Some(i) => (&after_set[..i], &after_set[i + 7..]),
        None => (after_set, ""),
    };
    let set_cols = parse_assignments_for_cols(set_clause, ",");
    let where_cols = if where_clause.is_empty() {
        Vec::new()
    } else {
        parse_assignments_for_cols(where_clause, " AND ")
    };
    if set_cols.is_empty() {
        return None;
    }
    Some((table, set_cols, where_cols))
}

/// Parse `DELETE FROM <table> WHERE <col> = ? AND <col> = ?` into
/// `(table, where_cols)`.
fn parse_delete_sql(sql: &str) -> Option<(String, Vec<String>)> {
    let upper = sql.to_ascii_uppercase();
    if !upper.trim_start().starts_with("DELETE") {
        return None;
    }
    let from_idx = upper.find("FROM")?;
    let after_from = sql[from_idx + 4..].trim_start();
    let where_rel = after_from.to_ascii_uppercase().find(" WHERE ");
    let (table_part, where_clause) = match where_rel {
        Some(i) => (&after_from[..i], &after_from[i + 7..]),
        None => (after_from, ""),
    };
    let table = table_part
        .trim()
        .trim_matches(';')
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string();
    if table.is_empty() {
        return None;
    }
    let where_cols = if where_clause.is_empty() {
        Vec::new()
    } else {
        parse_assignments_for_cols(where_clause, " AND ")
    };
    Some((table, where_cols))
}

fn parse_assignments_for_cols(clause: &str, sep: &str) -> Vec<String> {
    // Split on sep (case-insensitive for " AND "), then for each assignment
    // take the LHS of `=` as the column name.
    let parts: Vec<String> = if sep.eq_ignore_ascii_case(" AND ") {
        split_ignore_case(clause, " AND ")
    } else {
        clause.split(sep).map(|s| s.to_string()).collect()
    };
    parts
        .into_iter()
        .filter_map(|p| {
            let eq = p.find('=')?;
            let lhs = p[..eq]
                .trim()
                .trim_matches('"')
                .trim_matches('`')
                .trim_matches('[')
                .trim_matches(']');
            if lhs.is_empty() { None } else { Some(lhs.to_string()) }
        })
        .collect()
}

fn split_ignore_case(s: &str, sep: &str) -> Vec<String> {
    let sep_upper = sep.to_ascii_uppercase();
    let upper = s.to_ascii_uppercase();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut search_from = 0usize;
    while let Some(rel) = upper[search_from..].find(&sep_upper) {
        let idx = search_from + rel;
        out.push(s[start..idx].to_string());
        start = idx + sep.len();
        search_from = start;
    }
    out.push(s[start..].to_string());
    out
}

fn split_csv_identifiers(body: &str) -> Vec<String> {
    body.split(',')
        .map(|s| {
            s.trim()
                .trim_matches('"')
                .trim_matches('`')
                .trim_matches('[')
                .trim_matches(']')
                .to_string()
        })
        .filter(|s| !s.is_empty())
        .collect()
}

fn find_matching_close_paren(s: &str) -> Option<usize> {
    let mut depth = 1i32;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Rewrite the first occurrence of `src_table` as `dst_table` in `sql`,
/// matching `INSERT INTO <t>`, `UPDATE <t>`, `DELETE FROM <t>`,
/// `CREATE TABLE <t>`, `ALTER TABLE <t>`, and `DROP TABLE <t>` shapes.
fn rename_table_in_sql(sql: &str, src_table: &str, dst_table: &str) -> String {
    let needles = [
        ("INSERT INTO ", false),
        ("UPDATE ", false),
        ("DELETE FROM ", false),
        ("CREATE TABLE IF NOT EXISTS ", false),
        ("CREATE TABLE ", false),
        ("ALTER TABLE ", false),
        ("DROP TABLE IF EXISTS ", false),
        ("DROP TABLE ", false),
    ];
    let upper = sql.to_ascii_uppercase();
    for (needle, _) in needles.iter() {
        if let Some(start) = upper.find(needle) {
            let after = start + needle.len();
            let tail = &sql[after..];
            // Extract the existing table token.
            let mut end = 0usize;
            for (i, ch) in tail.char_indices() {
                if ch.is_whitespace() || ch == '(' || ch == ';' {
                    end = i;
                    break;
                }
                end = i + ch.len_utf8();
            }
            let existing = &tail[..end];
            let bare = existing
                .trim()
                .trim_matches('"')
                .trim_matches('`')
                .trim_matches('[')
                .trim_matches(']');
            if bare.eq_ignore_ascii_case(src_table) {
                let mut out = String::with_capacity(sql.len());
                out.push_str(&sql[..after]);
                out.push_str(dst_table);
                out.push_str(&sql[after + end..]);
                return out;
            }
        }
    }
    sql.to_string()
}

fn should_skip_consolidation_ddl(mapped: &MappedOperation) -> bool {
    let op = mapped.op_type.as_deref().unwrap_or_default().to_ascii_uppercase();
    if op == "DROPTABLE" {
        return true;
    }
    if op == "DROPCOLUMN" {
        return true;
    }

    let normalized = mapped.sql.trim_start().to_ascii_uppercase();
    if normalized.starts_with("DROP TABLE") {
        return true;
    }
    if normalized.starts_with("ALTER TABLE") && normalized.contains(" DROP COLUMN ") {
        return true;
    }
    false
}

fn apply_consolidation_mapping(layout: &ConsolidatorLayout, mapped: MappedOperation) -> MappedOperation {
    let device_id = SqlValue::Text(layout.device_id.clone());
    let device_name = SqlValue::Text(layout.device_name.clone());
    let ts_value = SqlValue::Text(current_update_timestamp());

    let sql = mapped.sql.trim().to_string();
    // Java parity: DML must target the same fully-qualified name the DDL
    // path emitted (db.schema.table), otherwise Postgres/DuckDB look in the
    // session default schema and fail with 42P01.
    let qualify_dml = |rewritten: String| -> String {
        if mapped.is_system_table {
            return rewritten;
        }
        let Some(tname) = mapped.table_name.as_deref().filter(|s| !s.is_empty()) else {
            return rewritten;
        };
        let qualified = qualify_table(layout, tname);
        if qualified.is_empty() || qualified == quote_table(layout, tname) {
            return rewritten;
        }
        rename_table_in_sql(&rewritten, tname, &qualified)
    };
    match mapped.kind {
        DmlKind::Insert => {
            let (prefix, values_clause) = split_values_clause(&sql).unwrap_or((sql.as_str(), ""));
            let rewritten_sql = qualify_dml(rewrite_insert_sql(prefix, values_clause, mapped.is_system_table));
            let mut args = Vec::with_capacity(mapped.args.len() + 3);
            args.push(device_id);
            args.push(device_name);
            args.push(ts_value);
            args.extend(mapped.args);
            MappedOperation { sql: rewritten_sql, args, ..mapped }
        }
        DmlKind::Update => {
            let rewritten_sql = qualify_dml(rewrite_update_sql(&sql, mapped.is_system_table));
            let mut args = Vec::with_capacity(mapped.args.len() + 5);
            args.push(device_id);
            args.push(device_name);
            args.push(ts_value);
            args.extend(mapped.args);
            args.push(SqlValue::Text(layout.device_id.clone()));
            args.push(SqlValue::Text(layout.device_name.clone()));
            MappedOperation { sql: rewritten_sql, args, ..mapped }
        }
        DmlKind::Delete => {
            let rewritten_sql = qualify_dml(rewrite_delete_sql(&sql));
            let mut args = Vec::with_capacity(mapped.args.len() + 2);
            args.push(device_id);
            args.push(device_name);
            args.extend(mapped.args);
            MappedOperation { sql: rewritten_sql, args, ..mapped }
        }
        DmlKind::Other => {
            let rewritten_sql = rewrite_consolidation_ddl_sql(layout, &mapped, &sql);
            MappedOperation {
                sql: rewritten_sql,
                ..mapped
            }
        }
    }
}

fn rewrite_consolidation_ddl_sql(layout: &ConsolidatorLayout, mapped: &MappedOperation, sql: &str) -> String {
    let op = mapped.op_type.as_deref().unwrap_or_default().to_ascii_uppercase();
    let fallback_table = extract_table_name_for_operation(sql);
    let table_name = mapped
        .table_name
        .as_deref()
        .or(fallback_table.as_deref())
        .unwrap_or("");

    // Java parity: rebuild CREATE TABLE from column metadata so
    // synclite_* bookkeeping columns are added and DataTypeMapper is
    // applied. CdcLog entries carry metadata directly; CommandLog
    // entries (store devices) now also carry metadata populated by
    // read_commandlog_entries (mirrors Java DeviceEventStreamer's
    // in-memory replica + PRAGMA table_info).
    if op == "CREATETABLE"
        && !table_name.is_empty()
        && !mapped.ddl_columns.is_empty()
    {
        if let Some(create_sql) = build_create_table_with_metadata(layout, table_name, &mapped.ddl_columns) {
            return create_sql;
        }
    }

    if op == "RENAMETABLE" {
        let inferred_new_table = mapped
            .ddl_columns
            .iter()
            .find_map(|c| c.old_table_name.as_ref().map(|_| table_name.to_string()));
        if let Some(new_table) = parse_alter_rename_table_name(sql).or(inferred_new_table) {
            if let Some(create_sql) = build_create_table_with_metadata(layout, &new_table, &mapped.ddl_columns) {
                return create_sql;
            }
            if !table_name.is_empty() {
                return format!(
                    "CREATE TABLE IF NOT EXISTS {new_table} AS SELECT * FROM {table_name} WHERE 1=0"
                );
            }
        }
    }

    if op == "RENAMECOLUMN" {
        if !table_name.is_empty() {
            let col_name = mapped
                .ddl_columns
                .iter()
                .find(|c| c.old_column_name.is_some())
                .map(|c| c.column_name.clone())
                .or_else(|| parse_alter_rename_column_names(sql).map(|(_, new_col)| new_col));
            if let Some(new_col) = col_name {
                let col_type = mapped
                    .ddl_columns
                    .iter()
                    .find(|c| c.column_name.eq_ignore_ascii_case(&new_col))
                    .and_then(|c| c.column_type.clone());
                return build_add_column_sql(layout, table_name, &new_col, col_type.as_deref());
            }
        }
    }

    let upper = sql.trim_start().to_ascii_uppercase();

    // Java consolidation mapper semantics: ADD COLUMN. The raw SQL from
    // the source backend (Derby / H2 / HSQL / DuckDB / SQLite multi-writer)
    // may carry destination-incompatible clauses such as `NOT NULL`
    // without a DEFAULT — SQLite rejects this. Rebuild via
    // build_add_column_sql which applies destination-dialect quoting,
    // schema qualification, DataTypeMapper, and drops unsupported
    // constraint clauses.
    if op == "ADDCOLUMN" && !table_name.is_empty() {
        let col_opt = mapped
            .ddl_columns
            .iter()
            .find(|c| !c.column_name.is_empty())
            .cloned()
            .or_else(|| parse_add_column_column(sql));
        if let Some(col) = col_opt {
            return build_add_column_sql(
                layout,
                table_name,
                &col.column_name,
                col.column_type.as_deref(),
            );
        }
    }

    // Java consolidation mapper semantics: RENAME COLUMN behaves like ADD COLUMN.
    if upper.starts_with("ALTER TABLE") && upper.contains(" RENAME COLUMN ") {
        if let Some(table_name) = extract_table_name_for_operation(sql) {
            if let Some((_old_col, new_col)) = parse_alter_rename_column_names(sql) {
                return build_add_column_sql(layout, &table_name, &new_col, None);
            }
        }
    }

    // Java consolidation mapper semantics: RENAME TABLE behaves like CREATE TABLE(new).
    if upper.starts_with("ALTER TABLE") && upper.contains(" RENAME TO ") {
        if let Some(old_table) = extract_table_name_for_operation(sql) {
            if let Some(new_table) = parse_alter_rename_table_name(sql) {
                return format!(
                    "CREATE TABLE IF NOT EXISTS {new_table} AS SELECT * FROM {old_table} WHERE 1=0"
                );
            }
        }
    }

    rewrite_create_table_sql(sql)
}

/// Java parity: replication-mode equivalent of `rewrite_consolidation_ddl_sql`.
/// Replication preserves the source schema 1:1 (no `synclite_device_id` /
/// `synclite_device_name` / `synclite_update_timestamp` bookkeeping columns)
/// but still needs to (a) apply filter-mapper column drops/renames to CREATE
/// TABLE and ADD COLUMN, (b) quote identifiers per dst dialect, (c) schema-
/// qualify table names, and (d) map source column types to dst-dialect types
/// via DataTypeMapper.
fn rewrite_replication_ddl_sql(layout: &ConsolidatorLayout, mapped: &MappedOperation, sql: &str) -> String {
    let op = mapped.op_type.as_deref().unwrap_or_default().to_ascii_uppercase();
    let fallback_table = extract_table_name_for_operation(sql);
    let table_name = mapped
        .table_name
        .as_deref()
        .or(fallback_table.as_deref())
        .unwrap_or("");

    if op == "CREATETABLE" && !table_name.is_empty() && !mapped.ddl_columns.is_empty() {
        if let Some(create_sql) = build_replication_create_table(layout, table_name, &mapped.ddl_columns) {
            return create_sql;
        }
    }

    if op == "ADDCOLUMN" && !table_name.is_empty() {
        if let Some(col) = mapped.ddl_columns.iter().find(|c| !c.column_name.is_empty()) {
            return build_add_column_sql(
                layout,
                table_name,
                &col.column_name,
                col.column_type.as_deref(),
            );
        }
    }

    // RENAME TABLE / RENAME COLUMN / DROP COLUMN: fall through to qualify-only
    // rewrite (preserves the original SQL shape but schema-qualifies the
    // table identifier so Postgres/DuckDB land in the configured dst schema).
    if !table_name.is_empty() {
        let qualified = qualify_table(layout, table_name);
        if !qualified.is_empty() && qualified != quote_table(layout, table_name) {
            return rename_table_in_sql(sql, table_name, &qualified);
        }
    }
    sql.to_string()
}

/// Replication-mode CREATE TABLE builder. Unlike `build_create_table_with_metadata`,
/// does not inject `synclite_device_id` / `synclite_device_name` /
/// `synclite_update_timestamp` columns and always emits a single CREATE
/// (no per-column ADD COLUMN fallback, no TRUNCATE/DELETE statement) since
/// replication mirrors the source schema verbatim.
fn build_replication_create_table(
    layout: &ConsolidatorLayout,
    table_name: &str,
    ddl_columns: &[CdcSchemaColumn],
) -> Option<String> {
    if table_name.trim().is_empty() {
        return None;
    }
    let mapper = layout.data_type_mapper();
    let mut sorted_cols = ddl_columns.to_vec();
    sorted_cols.sort_by_key(|c| c.column_index);

    let mut defs: Vec<String> = Vec::new();
    for col in &sorted_cols {
        let name = col.column_name.trim();
        if name.is_empty() {
            continue;
        }
        let src_type = col
            .column_type
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .unwrap_or("TEXT");
        let mapped_type = mapper.map_type(src_type);
        defs.push(format!("{} {mapped_type}", quote_col(layout, name)));
    }
    if defs.is_empty() {
        return None;
    }

    let suffix = layout.dst_create_table_suffix.trim();
    let suffix_clause = if suffix.is_empty() {
        String::new()
    } else {
        format!(" {suffix}")
    };
    let qtable = qualify_table(layout, table_name);
    Some(format!(
        "CREATE TABLE IF NOT EXISTS {qtable} ({}){}",
        defs.join(", "),
        suffix_clause
    ))
}

fn build_create_table_with_metadata(
    layout: &ConsolidatorLayout,
    table_name: &str,
    ddl_columns: &[CdcSchemaColumn],
) -> Option<String> {
    if table_name.trim().is_empty() {
        return None;
    }

    let init_mode = layout.dst_object_init_mode;
    let suffix = layout.dst_create_table_suffix.trim();
    let suffix_clause = if suffix.is_empty() {
        String::new()
    } else {
        format!(" {suffix}")
    };

    let mapper = layout.data_type_mapper();
    let mut sorted_cols = ddl_columns.to_vec();
    sorted_cols.sort_by_key(|c| c.column_index);

    let qtable = qualify_table(layout, table_name);

    let mut stmts: Vec<String> = Vec::new();

    // Java parity: ConsolidationTableMapper emits CreateTable + AddColumn
    // per non-system column when init mode is OVERWRITE_OBJECT /
    // TRY_CREATE_DELETE_DATA / TRY_CREATE_APPEND_DATA. CREATE is omitted
    // for DELETE_DATA / APPEND_DATA. Duplicate-column errors from the
    // redundant ADDs are swallowed by the apply path.
    if init_mode.emits_create() {
        let mut defs = Vec::new();
        defs.push(format!("{} TEXT", quote_col(layout, "synclite_device_id")));
        defs.push(format!("{} TEXT", quote_col(layout, "synclite_device_name")));
        defs.push(format!("{} TEXT", quote_col(layout, "synclite_update_timestamp")));
        for col in &sorted_cols {
            let name = col.column_name.trim();
            if name.is_empty() {
                continue;
            }
            let src_type = col
                .column_type
                .as_deref()
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .unwrap_or("TEXT");
            let mapped_type = mapper.map_type(src_type);
            defs.push(format!("{} {mapped_type}", quote_col(layout, name)));
        }
        stmts.push(format!(
            "CREATE TABLE IF NOT EXISTS {qtable} ({}){}",
            defs.join(", "),
            suffix_clause
        ));

        for col in &sorted_cols {
            let name = col.column_name.trim();
            if name.is_empty() {
                continue;
            }
            stmts.push(build_add_column_sql(
                layout,
                table_name,
                name,
                col.column_type.as_deref(),
            ));
        }
    }

    if init_mode.emits_truncate() {
        // SQLite/DuckDB/Postgres all accept DELETE FROM as a portable
        // truncate; Java emits TRUNCATE TABLE but SQLite does not
        // support it.
        stmts.push(format!("DELETE FROM {qtable}"));
    }

    if stmts.is_empty() {
        return None;
    }

    Some(stmts.join(";\n"))
}

fn build_add_column_sql(
    layout: &ConsolidatorLayout,
    table_name: &str,
    col_name: &str,
    col_type: Option<&str>,
) -> String {
    let mapper = layout.data_type_mapper();
    let src_type = col_type
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .unwrap_or("TEXT");
    let mapped_type = mapper.map_type(src_type);
    let qtable = qualify_table(layout, table_name);
    let qcol = quote_col(layout, col_name);

    match layout.dst_type {
        DstType::Sqlite => {
            if mapped_type.eq_ignore_ascii_case("TEXT") || mapped_type.eq_ignore_ascii_case("text") {
                format!("ALTER TABLE {qtable} ADD COLUMN {qcol}")
            } else {
                format!("ALTER TABLE {qtable} ADD COLUMN {qcol} {mapped_type}")
            }
        }
        DstType::DuckDb | DstType::Postgres => {
            format!("ALTER TABLE {qtable} ADD COLUMN {qcol} {mapped_type}")
        }
    }
}

/// Java parity: quote a single identifier (table or column) per the
/// destination dialect when `dst_quote_object_names` /
/// `dst_quote_column_names` is enabled. SQLite, DuckDB, and Postgres all
/// support standard double-quote identifier syntax; embedded quotes are
/// doubled. Caller decides whether to quote (table vs. column flag).
fn quote_ident_raw(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn quote_table(layout: &ConsolidatorLayout, name: &str) -> String {
    if layout.dst_quote_object_names && !name.is_empty() {
        quote_ident_raw(name)
    } else {
        name.to_string()
    }
}

/// Postgres-only: schema-qualify a system metadata table name when
/// `dst_use_schema_scope_resolution` is on and `dst_schema` is set.
/// Used for the consolidator-owned bookkeeping tables (`synclite_checkpoint`,
/// `synclite_consolidator_metadata`, `synclite_consolidator_table_metadata`)
/// so they live in the same destination schema as user tables and never
/// collide with stale objects in `public`.
fn pg_meta_table(layout: &ConsolidatorLayout, name: &str) -> String {
    if layout.dst_use_schema_scope_resolution {
        if let Some(schema) = layout.dst_schema.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            return format!("{}.{}", quote_ident_raw(schema), quote_ident_raw(name));
        }
    }
    name.to_string()
}

fn quote_col(layout: &ConsolidatorLayout, name: &str) -> String {
    if layout.dst_quote_column_names && !name.is_empty() {
        quote_ident_raw(name)
    } else {
        name.to_string()
    }
}

/// Java parity: prefix a table identifier with database/schema scope when
/// the destination backend supports them AND the corresponding
/// `dst_use_catalog_scope_resolution` / `dst_use_schema_scope_resolution`
/// flag is enabled AND a `dst_database` / `dst_schema` value is set.
///
/// Per-backend support (mirrors Java `SQLGenerator.isDatabaseAllowed` /
/// `isSchemaAllowed`):
/// - SQLite:   neither (single-DB engine).
/// - DuckDB:   both database and schema.
/// - Postgres: both database and schema.
fn qualify_table(layout: &ConsolidatorLayout, table_name: &str) -> String {
    if table_name.is_empty() {
        return String::new();
    }
    // Skip qualification when the caller already supplied a dotted path.
    if table_name.contains('.') {
        return quote_table(layout, table_name);
    }
    let base = quote_table(layout, table_name);
    let (db_allowed, schema_allowed) = match layout.dst_type {
        DstType::Sqlite => (false, false),
        DstType::DuckDb => (true, true),
        DstType::Postgres => (true, true),
    };
    if !db_allowed && !schema_allowed {
        return base;
    }
    let mut prefix = String::new();
    if db_allowed && layout.dst_use_catalog_scope_resolution {
        if let Some(db) = layout.dst_database.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            prefix.push_str(&quote_table(layout, db));
            prefix.push('.');
        }
    }
    if schema_allowed && layout.dst_use_schema_scope_resolution {
        if let Some(schema) = layout.dst_schema.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            prefix.push_str(&quote_table(layout, schema));
            prefix.push('.');
        }
    }
    if prefix.is_empty() {
        base
    } else {
        format!("{prefix}{base}")
    }
}

fn rewrite_create_table_sql(sql: &str) -> String {
    let upper = sql.to_ascii_uppercase();
    if !upper.starts_with("CREATE TABLE") {
        return sql.to_string();
    }

    let Some(open_idx) = sql.find('(') else {
        return sql.to_string();
    };
    let Some(close_idx) = sql.rfind(')') else {
        return sql.to_string();
    };
    if close_idx <= open_idx {
        return sql.to_string();
    }

    let head = sql[..open_idx].trim_end();
    let cols = sql[open_idx + 1..close_idx].trim();
    let tail = sql[close_idx + 1..].trim_start();

    let mut builder = String::new();
    builder.push_str(head);
    builder.push_str(" (");
    builder.push_str("synclite_device_id TEXT, synclite_device_name TEXT, synclite_update_timestamp TEXT");
    if !cols.is_empty() {
        builder.push_str(", ");
        builder.push_str(cols);
    }
    builder.push(')');
    if !tail.is_empty() {
        builder.push(' ');
        builder.push_str(tail);
    }
    builder
}

fn split_values_clause(sql: &str) -> Option<(&str, &str)> {
    let upper = sql.to_ascii_uppercase();
    let idx = upper.find("VALUES")?;
    Some((&sql[..idx], &sql[idx..]))
}

fn rewrite_insert_sql(sql_prefix: &str, values_clause: &str, is_system_table: bool) -> String {
    let mut builder = String::new();
    if let Some((head, tail)) = sql_prefix.split_once('(') {
        let table_part = head.trim_end();
        let tail = tail.rsplit_once(')').map(|(cols, _)| cols).unwrap_or(tail);
        builder.push_str(table_part);
        builder.push_str(" (");
        builder.push_str("synclite_device_id, synclite_device_name, synclite_update_timestamp");
        if !tail.trim().is_empty() {
            builder.push_str(", ");
            builder.push_str(tail.trim());
        }
        builder.push_str(") ");
    } else {
        builder.push_str(sql_prefix.trim_end());
        if !is_system_table {
            builder.push(' ');
        }
    }
    builder.push_str(values_clause.trim_start());
    let upper = builder.to_ascii_uppercase();
    if let Some(values_idx) = upper.find("VALUES") {
        if let Some(paren_rel_idx) = builder[values_idx..].find('(') {
            let insert_at = values_idx + paren_rel_idx + 1;
            builder.insert_str(insert_at, "?, ?, ?, ");
        }
    }
    builder
}

fn rewrite_update_sql(sql: &str, _is_system_table: bool) -> String {
    let upper = sql.to_ascii_uppercase();
    if let Some(set_idx) = upper.find(" SET ") {
        let after_set = &sql[set_idx + 5..];
        if let Some(where_idx) = after_set.to_ascii_uppercase().find(" WHERE ") {
            let set_clause = after_set[..where_idx].trim();
            let where_clause = after_set[where_idx + 7..].trim();
            return format!(
                "{} SET synclite_device_id = ?, synclite_device_name = ?, synclite_update_timestamp = ?, {} WHERE {} AND synclite_device_id = ? AND synclite_device_name = ?",
                sql[..set_idx].trim_end(),
                set_clause,
                where_clause
            );
        }
        let set_clause = after_set.trim();
        return format!(
            "{} SET synclite_device_id = ?, synclite_device_name = ?, synclite_update_timestamp = ?, {} WHERE synclite_device_id = ? AND synclite_device_name = ?",
            sql[..set_idx].trim_end(),
            set_clause
        );
    }
    sql.to_string()
}

fn rewrite_delete_sql(sql: &str) -> String {
    let upper = sql.to_ascii_uppercase();
    if let Some(where_idx) = upper.find(" WHERE ") {
        let where_clause = sql[where_idx + 7..].trim();
        return format!(
            "{} WHERE synclite_device_id = ? AND synclite_device_name = ? AND {}",
            sql[..where_idx].trim_end(),
            where_clause
        );
    }
    sql.to_string()
}

fn should_retry_apply_error(backend: DstType, err: &Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    if msg.contains("syntax") || msg.contains("no such table") || msg.contains("no such column") {
        return false;
    }
    if msg.contains("duplicate") || msg.contains("unique constraint") || msg.contains("primary key") {
        return false;
    }
    if backend == DstType::Postgres && msg.contains("pg[235") {
        return false;
    }
    msg.contains("busy")
        || msg.contains("locked")
        || msg.contains("deadlock")
        || msg.contains("timeout")
        || msg.contains("connection")
        || msg.contains("transient")
}

fn is_sqlite_duplicate_err(e: &rusqlite::Error) -> bool {
    match e {
        rusqlite::Error::SqliteFailure(_, msg) => msg
            .as_deref()
            .map(|m| {
                let lm = m.to_ascii_lowercase();
                lm.contains("unique") || lm.contains("primary key") || lm.contains("constraint")
            })
            .unwrap_or(false),
        _ => false,
    }
}

enum Msg {
    BootstrapReady {
        backup_path: PathBuf,
        metadata_path: PathBuf,
    },
    StagePathReady(PathBuf),
    Shutdown,
}

pub struct Consolidator {
    tx: Sender<Msg>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl Consolidator {
    pub fn spawn(layout: ConsolidatorLayout) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&layout.work_dir)?;
        std::fs::create_dir_all(&layout.device_work_dir)?;
        if let Some(parent) = layout.state_db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        migrate_legacy_state_db(&layout)?;
        initialize_state_db(&layout.state_db_path)?;
        initialize_device_stats_db(&layout)?;
        initialize_consolidator_stats_db(&layout)?;

        let (tx, rx) = mpsc::channel();
        let join = thread::Builder::new()
            .name("synclite-consolidator".into())
            .spawn(move || worker_loop(rx, layout))
            .map_err(|e| Error::Internal(format!("spawn consolidator thread: {e}")))?;

        Ok(Arc::new(Self {
            tx,
            join: Mutex::new(Some(join)),
        }))
    }

    pub fn notify_bootstrap_ready<P: Into<PathBuf>, Q: Into<PathBuf>>(
        &self,
        backup_path: P,
        metadata_path: Q,
    ) -> Result<()> {
        self.tx
            .send(Msg::BootstrapReady {
                backup_path: backup_path.into(),
                metadata_path: metadata_path.into(),
            })
            .map_err(|_| Error::Internal("consolidator worker has exited".into()))
    }

    pub fn notify_stage_path<P: Into<PathBuf>>(&self, path: P) -> Result<()> {
        self.tx
            .send(Msg::StagePathReady(path.into()))
            .map_err(|_| Error::Internal("consolidator worker has exited".into()))
    }

    pub fn catch_up_stage_dir(&self, stage_dir: &Path) -> Result<()> {
        let mut staged_paths = Vec::new();
        collect_stage_files_recursive(stage_dir, &mut staged_paths);

        staged_paths.sort();
        for path in staged_paths {
            if is_sqllog_txn_sidecar_path(&path) {
                continue;
            }
            // Only log-segment artifacts are valid apply candidates.
            // Device-level files in the same stage subdir (the
            // `.synclite.backup` snapshot and `.synclite.metadata`
            // marker the logger writes alongside segments) must NOT
            // go through `process_stage_path_ready` — they are not
            // sqllog/cdclog files and would otherwise produce
            // "undefined table" errors at apply time.
            if !is_apply_candidate_segment(&path) {
                continue;
            }
            self.notify_stage_path(path)?;
        }
        Ok(())
    }

    fn shutdown(&self) {
        let _ = self.tx.send(Msg::Shutdown);
        if let Ok(mut join) = self.join.lock() {
            if let Some(handle) = join.take() {
                let _ = handle.join();
            }
        }
    }
}

impl Drop for Consolidator {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn collect_stage_files_recursive(root: &Path, out: &mut Vec<PathBuf>) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file() {
                out.push(path);
            }
        }
    }
}

/// Locate a paired `<dbName>.synclite.backup` + `<dbName>.synclite.metadata`
/// in the per-device stage directory. The shipper writes both files using
/// the full DB filename (e.g. `test.db.synclite.backup`), which does not
/// always match `ConsolidatorLayout.database_name` (the Java helper
/// `databaseNameOf` strips the trailing extension). Returning `(backup,
/// metadata)` lets the worker synthesize a `Msg::BootstrapReady` without
/// the caller knowing the exact name.
fn find_stage_bootstrap_pair(stage_dir: &Path) -> Option<(PathBuf, PathBuf)> {
    let entries = std::fs::read_dir(stage_dir).ok()?;
    let mut backup: Option<PathBuf> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if name.ends_with(".synclite.backup") {
            backup = Some(path);
            break;
        }
    }
    let backup = backup?;
    let backup_name = backup.file_name()?.to_str()?;
    let stem = backup_name.strip_suffix(".synclite.backup")?;
    let metadata = stage_dir.join(format!("{}.synclite.metadata", stem));
    if metadata.is_file() {
        Some((backup, metadata))
    } else {
        None
    }
}

fn migrate_legacy_state_db(layout: &ConsolidatorLayout) -> Result<()> {
    if layout.metadata_store != MetadataStore::Local {
        return Ok(());
    }

    let legacy_path = layout
        .state_db_path
        .with_file_name("synclite_replicator_metadata.db");
    if legacy_path.exists() && !layout.state_db_path.exists() {
        std::fs::rename(&legacy_path, &layout.state_db_path).or_else(|_| {
            std::fs::copy(&legacy_path, &layout.state_db_path)
                .map(|_| ())
                .and_then(|_| std::fs::remove_file(&legacy_path))
        })?;
    }
    Ok(())
}

fn worker_loop(rx: mpsc::Receiver<Msg>, layout: ConsolidatorLayout) {
    // Java parity: open per-device trace file under the consolidator's per-device
    // work directory (`<workDir>/synclite-<device>-<id>/synclite_device.trace`).
    // Default level is INFO matching Java `ConfLoader.getTraceLevel`. Tracing
    // failures are non-fatal: observability, not correctness.
    let device_trace_level = layout
        .trace_level
        .as_deref()
        .and_then(|s| TraceLevel::parse(s).ok())
        .unwrap_or(TraceLevel::Info);
    match Tracer::for_consolidator_device(&layout.device_work_dir, device_trace_level) {
        Ok(tracer) => {
            tracer_info!(
                tracer,
                "Device",
                "Device initialized with status : INITIALIZED (device={} dst={} workDir={})",
                layout.device_name,
                layout.dst_index,
                layout.device_work_dir.display()
            );
            DEVICE_TRACER.with(|cell| *cell.borrow_mut() = Some(tracer));
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                path = %layout.device_work_dir.display(),
                "failed to open consolidator device trace file"
            );
        }
    }

    let state_conn = match Connection::open(&layout.state_db_path) {
        Ok(conn) => conn,
        Err(e) => {
            tracing::error!(error = %e, path = %layout.state_db_path.display(), "failed to open consolidator state db");
            return;
        }
    };
    let stats_conn = match Connection::open(&layout.stats_db_path) {
        Ok(conn) => conn,
        Err(e) => {
            tracing::error!(error = %e, path = %layout.stats_db_path.display(), "failed to open consolidator stats db");
            return;
        }
    };
    let consolidator_stats_conn = match Connection::open(&layout.consolidator_stats_db_path) {
        Ok(conn) => conn,
        Err(e) => {
            tracing::error!(error = %e, path = %layout.consolidator_stats_db_path.display(), "failed to open consolidator global stats db");
            return;
        }
    };

    let mut bootstrap_initialized = is_initialized(&state_conn).unwrap_or(false);
    // Java parity: every consolidator startup writes device identity into
    // synclite_device_metadata.db and reads consolidator init state from
    // the per-destination consolidator metadata store. This makes Java<->Rust
    // handover seamless for a given device.
    if let Err(e) = consolidator_state::bootstrap_destination_metadata(&layout, layout.dst_index) {
        tracing::warn!(error = %e, "failed to bootstrap destination metadata tables");
    }
    if let Err(e) = consolidator_state::seed_device_metadata(&layout) {
        tracing::warn!(error = %e, "failed to seed synclite_device_metadata.db");
    }
    if let Err(e) = consolidator_state::seed_consolidator_identity(&layout, layout.dst_index) {
        tracing::warn!(error = %e, "failed to seed consolidator identity metadata");
    }
    if let Err(e) = consolidator_state::seed_consolidator_metadata_version(&layout, layout.dst_index) {
        tracing::warn!(error = %e, "failed to seed consolidator metadata-schema version");
    }
    match consolidator_state::get_consolidator_property_long(&layout, layout.dst_index, "initialization_status") {
        Ok(Some(1)) => {
            bootstrap_initialized = true;
        }
        Ok(_) => {}
        Err(e) => tracing::warn!(error = %e, "failed to read consolidator initialization_status"),
    }
    let mut pending_stage_paths: Vec<PathBuf> = Vec::new();
    // Pause-buffer: while the pause sentinel exists we queue staged
    // segments here instead of applying them. On resume we drain in
    // arrival order through the normal apply path.
    let mut paused_stage_paths: Vec<PathBuf> = Vec::new();
    let poll_interval = std::time::Duration::from_millis(layout.device_polling_interval_ms.max(1));

    loop {
        let mut msg: Option<Msg> = match rx.recv_timeout(poll_interval) {
            Ok(msg) => Some(msg),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => break,
        };

        let paused = is_paused(&layout);
        if !paused && !paused_stage_paths.is_empty() && bootstrap_initialized {
            // Resume: drain queued segments in order before processing
            // the freshly received message.
            let drained: Vec<PathBuf> = paused_stage_paths.drain(..).collect();
            for path in drained {
                if let Err(e) = process_stage_path_ready(
                    &layout,
                    &state_conn,
                    &stats_conn,
                    &consolidator_stats_conn,
                    &path,
                ) {
                    tracing::error!(error = %e, path = %path.display(), "failed to apply paused-then-resumed stage path");
                }
            }
        }

        if msg.is_none() {
            // Periodic scan: walk the per-device stage directory and
            // re-apply any segment that hasn't been consumed yet.
            // `process_stage_path_ready` is idempotent against the
            // state DB (already-applied segments are no-ops), so this
            // is safe to run on every tick. Acts as the safety floor
            // for push notifications missed by the OS WatchService
            // or by Java shipping paths that don't fire the JNI
            // notify hook.
            if !paused {
                if let Some(stage_dir) = layout.stage_dir.as_deref() {
                    if stage_dir.is_dir() {
                        // Auto-detect bootstrap when the upstream
                        // logger has shipped both the snapshot and
                        // metadata into stage but nobody has called
                        // `notify_bootstrap_ready` yet (Java init
                        // happens before any files exist, and there
                        // is no post-ship hook that synthesizes the
                        // bootstrap notification). The shipped file
                        // names use the full DB filename
                        // (e.g. `test.db.synclite.backup`), which
                        // does not always match `database_name`
                        // (Java strips the extension), so we glob
                        // for any matching pair instead of computing
                        // a name.
                        if !bootstrap_initialized {
                            if let Some((backup_candidate, metadata_candidate)) =
                                find_stage_bootstrap_pair(stage_dir)
                            {
                                msg = Some(Msg::BootstrapReady {
                                    backup_path: backup_candidate,
                                    metadata_path: metadata_candidate,
                                });
                            }
                        }
                        if msg.is_none() && bootstrap_initialized {
                            let mut staged: Vec<PathBuf> = Vec::new();
                            collect_stage_files_recursive(stage_dir, &mut staged);
                            staged.sort();
                            for path in staged {
                                if is_sqllog_txn_sidecar_path(&path) {
                                    continue;
                                }
                                if !is_apply_candidate_segment(&path) {
                                    continue;
                                }
                                if let Err(e) = process_stage_path_ready(
                                    &layout,
                                    &state_conn,
                                    &stats_conn,
                                    &consolidator_stats_conn,
                                    &path,
                                ) {
                                    tracing::error!(
                                        error = %e,
                                        path = %path.display(),
                                        "periodic stage scan: failed to apply segment"
                                    );
                                }
                            }
                        }
                    }
                }
            }
            if msg.is_none() {
                continue;
            }
            // Fall through with synthesized BootstrapReady so the
            // existing match arm runs unchanged.
        }
        let msg = msg.expect("msg must be Some here");

        match msg {
            Msg::BootstrapReady {
                backup_path,
                metadata_path,
            } => {
                // Java parity (Device.initReplicas): non-transactional device
                // types (sqlite_store / duckdb_store / streaming) maintain one
                // replica file per destination, copied from the shared backup
                // the first time a destination sees it. Transactional devices
                // (sqlite / duckdb SQL) share a single replica across all
                // destinations.
                let bootstrap_backup_path = if !layout.is_transactional_device() {
                    let replica = layout.replica_path();
                    if let Some(parent) = replica.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    if !replica.exists() {
                        if let Err(e) = std::fs::copy(&backup_path, &replica) {
                            tracing::error!(
                                error = %e,
                                src = %backup_path.display(),
                                dst = %replica.display(),
                                "failed to create per-destination replica copy"
                            );
                            continue;
                        }
                    }
                    replica
                } else {
                    backup_path.clone()
                };

                // The store-device replica already lives in the per-device
                // work dir, so it doesn't need an extra mirror copy (which
                // would `copy(p, p)` and fail on Windows).
                let work_backup_path = if !layout.is_transactional_device() {
                    bootstrap_backup_path.clone()
                } else {
                    // Java parity: the device-shipped `.synclite.backup` is
                    // the INITIAL snapshot used to seed the destination at
                    // bootstrap time. After bootstrap the same file in
                    // workDir doubles as the replicator's working replica:
                    // sqllog replay (CREATE TABLE / INSERT / UPDATE /
                    // DELETE) commits into it so preupdate hooks can fire
                    // on subsequent segments. If we re-copy from stage on
                    // every BootstrapReady the accumulated replica state is
                    // wiped, which breaks UPDATE/DELETE replay for SQL
                    // devices. Only mirror the snapshot the first time;
                    // afterwards trust the work copy as the live replica.
                    let file_name = bootstrap_backup_path
                        .file_name()
                        .map(|n| n.to_owned())
                        .unwrap_or_default();
                    let stage_container = bootstrap_backup_path
                        .parent()
                        .and_then(|p| p.file_name())
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_else(|| format!("synclite-{}-{}", layout.device_name, layout.device_id));
                    let work_path = layout.work_dir.join(&stage_container).join(&file_name);
                    if work_path.exists() {
                        work_path
                    } else {
                        match mirror_stage_artifact_to_work(&layout, &bootstrap_backup_path) {
                            Ok(p) => p,
                            Err(e) => {
                                tracing::error!(error = %e, path = %bootstrap_backup_path.display(), "failed to mirror staged backup into consolidator work dir");
                                continue;
                            }
                        }
                    }
                };
                let work_metadata_path = match mirror_stage_artifact_to_work(&layout, &metadata_path) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!(error = %e, path = %metadata_path.display(), "failed to mirror staged metadata into consolidator work dir");
                        continue;
                    }
                };

                if let Err(e) = record_bootstrap(&state_conn, &stats_conn, &backup_path, &metadata_path) {
                    tracing::error!(error = %e, "failed to persist consolidator bootstrap state");
                }

                if !bootstrap_initialized {
                    // Java parity: DeviceDstInitializer.markTableInitializing —
                    // persist `initialization_status=INITIALIZING` per-table
                    // before applying so crash-recovery semantics match Java.
                    if let Err(e) = consolidator_state::mark_tables_initializing(
                        &layout,
                        layout.dst_index,
                        &work_backup_path,
                    ) {
                        tracing::warn!(error = %e, "failed to mark tables initializing");
                    }
                    // Java parity: ConsolidatorMetadataManager.upsertSchema —
                    // populate local `schema` table from PRAGMA table_info on
                    // the backup so a Java consolidator can loadSchemas() on
                    // handover. No-op in DESTINATION metadata mode.
                    if let Err(e) = consolidator_state::record_initial_table_schemas(
                        &layout,
                        layout.dst_index,
                        &work_backup_path,
                    ) {
                        tracing::warn!(error = %e, "failed to persist initial per-column schemas");
                    }
                    match initialize_from_backup(
                        &layout,
                        &state_conn,
                        &stats_conn,
                        &consolidator_stats_conn,
                        &work_backup_path,
                        &work_metadata_path,
                    ) {
                        Ok(true) => {
                            bootstrap_initialized = true;
                            // Java parity: mark this device initialized in the
                            // consolidator metadata store (LOCAL file or
                            // destination synclite_consolidator_metadata) so a
                            // future Java consolidator startup sees init_status=1.
                            let snapshot_name = backup_path
                                .file_name()
                                .map(|s| s.to_string_lossy().to_string())
                                .unwrap_or_default();
                            if let Err(e) = consolidator_state::mark_initialization_complete(
                                &layout,
                                layout.dst_index,
                                &snapshot_name,
                            ) {
                                tracing::warn!(error = %e, "failed to mark initialization complete");
                            }
                            // Java parity: DeviceDstInitializer.markTableInitialized —
                            // persist per-table initialization_status + initial_rows.
                            if let Err(e) = consolidator_state::record_initial_table_stats(
                                &layout,
                                layout.dst_index,
                                &work_backup_path,
                            ) {
                                tracing::warn!(error = %e, "failed to record initial per-table stats");
                            }
                            if let Err(e) = flush_pending_stage_paths(
                                &layout,
                                &state_conn,
                                &stats_conn,
                                &consolidator_stats_conn,
                                &mut pending_stage_paths,
                            ) {
                                tracing::error!(error = %e, "failed to flush pending stage paths");
                            }
                        }
                        Ok(false) => {
                            tracing::warn!(
                                backup = %backup_path.display(),
                                "consolidator bootstrap deferred; backup file not available yet"
                            );
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "failed to initialize destination state from backup");
                        }
                    }
                }
            }
            Msg::StagePathReady(path) => {
                if !bootstrap_initialized {
                    pending_stage_paths.push(path.clone());
                    if let Err(e) = record_event(&state_conn, "stage-path-deferred", &path) {
                        tracing::error!(error = %e, path = %path.display(), "failed to persist deferred stage event");
                    }
                    continue;
                }

                if paused {
                    // Capture a stable workdir copy now, before the
                    // shipper's cleaner deletes the stage file. The
                    // resume-drain path re-enters `process_stage_path_ready`
                    // which calls `mirror_stage_artifact_to_work` again;
                    // that call is tolerant of a missing stage file when
                    // the work copy already exists.
                    if let Err(e) = mirror_stage_artifact_to_work(&layout, &path) {
                        tracing::error!(error = %e, path = %path.display(), "failed to pre-mirror paused stage path");
                    }
                    paused_stage_paths.push(path.clone());
                    if let Err(e) = record_event(&state_conn, "stage-path-paused", &path) {
                        tracing::error!(error = %e, path = %path.display(), "failed to persist paused stage event");
                    }
                    continue;
                }

                if let Err(e) = process_stage_path_ready(
                    &layout,
                    &state_conn,
                    &stats_conn,
                    &consolidator_stats_conn,
                    &path,
                ) {
                    tracing::error!(error = %e, path = %path.display(), "failed to persist consolidator stage event");
                    // Mirror to the device trace file so users without
                    // a tracing subscriber installed still see why a
                    // segment failed to apply.
                    with_device_tracer(|t| {
                        tracer_error!(
                            t,
                            "DeviceConsolidator",
                            "Failed to apply cdc log derived from staged segment {} on dst {}: {}",
                            path.display(),
                            layout.dst_index,
                            e
                        )
                    });
                }
            }
            Msg::Shutdown => break,
        }
    }

    with_device_tracer(|t| tracer_info!(t, "Consolidator", "Job shutdown successfully."));
    DEVICE_TRACER.with(|cell| cell.borrow_mut().take());
}

fn flush_pending_stage_paths(
    layout: &ConsolidatorLayout,
    state_conn: &Connection,
    stats_conn: &Connection,
    consolidator_stats_conn: &Connection,
    pending_stage_paths: &mut Vec<PathBuf>,
) -> Result<()> {
    for path in pending_stage_paths.drain(..) {
        process_stage_path_ready(layout, state_conn, stats_conn, consolidator_stats_conn, &path)?;
    }
    Ok(())
}

fn is_paused(layout: &ConsolidatorLayout) -> bool {
    layout
        .pause_sentinel
        .as_deref()
        .map(|p| p.exists())
        .unwrap_or(false)
}

fn process_stage_path_ready(
    layout: &ConsolidatorLayout,
    state_conn: &Connection,
    stats_conn: &Connection,
    consolidator_stats_conn: &Connection,
    stage_path: &Path,
) -> Result<()> {
    if is_sqllog_txn_sidecar_path(stage_path) {
        return Ok(());
    }

    let _device_processing_lock = match DeviceProcessingLock::try_lock(layout)? {
        Some(lock) => lock,
        None => {
            record_event(state_conn, "stage-path-deferred-device-locked", stage_path)?;
            return Ok(());
        }
    };

    // Mirror to work dir first so subsequent inspections operate on a
    // stable copy that won't disappear if the shipper has already
    // cleaned the stage file (e.g. resume-from-pause path).
    let work_path = mirror_stage_artifact_to_work(layout, stage_path)?;

    // Idempotency: if the work copy is already marked APPLIED, the segment
    // (or the cdclog derived from it for SQL devices) has already been
    // consolidated into the destination. Skip re-applying — re-running
    // the apply pipeline would double-count rows. Still run the cleaner
    // so a re-notification (e.g. shipper `on_shipped` after the work copy
    // was applied earlier in the same job) can prune stale work artifacts.
    if segment_metadata_status_is_applied(&work_path) {
        record_event(state_conn, "stage-path-already-applied", stage_path)?;
        cleanup_processed_sqllog(layout, state_conn, stage_path, &work_path)?;
        return Ok(());
    }

    if is_sqllog_path(&work_path) && !last_txn_fate_decided(&work_path)? {
        record_event(state_conn, "stage-path-deferred-undecided-fate", stage_path)?;
        return Ok(());
    }
    let segment_seq = parse_segment_seq(&work_path)
        .or_else(|| parse_segment_seq(stage_path))
        .map(|v| v as i64);
    let destination_resume = if layout.metadata_store == MetadataStore::Destination
        && !(is_sql_device(layout) && is_sqllog_path(&work_path))
    {
        read_destination_resume_checkpoint(layout)?
    } else {
        None
    };

    if let Some(seq) = segment_seq {
        if should_skip_segment_by_destination_checkpoint(destination_resume, seq, &work_path)? {
            record_event(state_conn, "stage-path-skipped-recovered", stage_path)?;
            cleanup_processed_sqllog(layout, state_conn, stage_path, &work_path)?;
            return Ok(());
        }
    }

    // APPLIED status is written ONLY into the work-dir copy of the segment,
    // never into the stage-dir copy. The stage-dir file is owned by the
    // shipper until it has been shipped to every configured archiver;
    // mutating it here would race with the shipper and could result in a
    // partially-modified file being uploaded to remote stages.
    if is_store_or_streaming_device(layout) {
        let res = record_stage_path(
            layout,
            state_conn,
            stats_conn,
            consolidator_stats_conn,
            &work_path,
            apply_staged_segment_with_retry,
        );
        mark_segment_applied_best_effort(&work_path);
        res?;
    } else if is_sql_device(layout) {
        let cdc_log_path = run_device_replicator(layout, &work_path)?;
        let res = record_stage_path(
            layout,
            state_conn,
            stats_conn,
            consolidator_stats_conn,
            &cdc_log_path,
            apply_staged_segment_with_retry,
        );
        mark_segment_applied_best_effort(&cdc_log_path);
        mark_segment_applied_best_effort(&work_path);
        res?;
    } else {
        let res = record_stage_path(
            layout,
            state_conn,
            stats_conn,
            consolidator_stats_conn,
            &work_path,
            apply_staged_segment_with_retry,
        );
        mark_segment_applied_best_effort(&work_path);
        res?;
    }

    // Cleaner runs after successful apply, matching Java sequencing.
    cleanup_processed_sqllog(layout, state_conn, stage_path, &work_path)?;
    Ok(())
}

/// Java parity: `LogSegment.isApplied()` — read `metadata.status` from a
/// segment file (`.sqllog` or `.cdclog`) and return `true` iff it equals
/// `APPLIED`. Best-effort: a missing file, missing table, missing row, or
/// any SQLite error returns `false`, matching the Java side's tolerance
/// for in-flight segments. Used to make the apply pipeline idempotent
/// across re-notifications (mover, shipper `on_shipped`, restart catch-up).
fn segment_metadata_status_is_applied(path: &Path) -> bool {
    if !path.exists() {
        return false;
    }
    let conn = match Connection::open(path) {
        Ok(c) => c,
        Err(_) => return false,
    };
    let row: rusqlite::Result<String> = conn.query_row(
        "SELECT value FROM metadata WHERE key = 'status'",
        [],
        |r| r.get(0),
    );
    matches!(row, Ok(ref v) if v == "APPLIED")
}

/// Java parity: `LogSegment.markApplied()` — upsert
/// `metadata.status = 'APPLIED'` into the segment file's `metadata` table.
/// Best-effort: any IO/SQL error is logged (because the segment may be
/// about to be deleted by the cleaner, or may already be locked) but does
/// not fail the apply.
fn device_metadata_path(layout: &ConsolidatorLayout) -> PathBuf {
    layout.device_work_dir.join(DEVICE_METADATA_FILE_NAME)
}

/// Java parity: per-device metadata read. Best-effort — any failure
/// (missing file, locked DB, schema drift) returns `default`.
fn read_device_metadata_i64(layout: &ConsolidatorLayout, key: &str, default: i64) -> i64 {
    let path = device_metadata_path(layout);
    if !path.exists() {
        return default;
    }
    let conn = match Connection::open(&path) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, key, "device metadata read: open failed");
            return default;
        }
    };
    let row: rusqlite::Result<String> = conn.query_row(
        "SELECT value FROM metadata WHERE key = ?1",
        [key],
        |r| r.get(0),
    );
    match row {
        Ok(s) => s.parse::<i64>().unwrap_or(default),
        Err(rusqlite::Error::QueryReturnedNoRows) => default,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, key, "device metadata read: query failed");
            default
        }
    }
}

/// Java parity: per-device metadata upsert. Best-effort — a failure leaves
/// the in-memory caller state intact and logs a warning.
fn write_device_metadata_i64_best_effort(layout: &ConsolidatorLayout, key: &str, value: i64) {
    let path = device_metadata_path(layout);
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::warn!(path = %parent.display(), error = %e, key, "device metadata write: mkdir failed");
            return;
        }
    }
    let conn = match Connection::open(&path) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, key, value, "device metadata write: open failed");
            return;
        }
    };
    if let Err(e) = conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT);",
    ) {
        tracing::warn!(path = %path.display(), error = %e, key, value, "device metadata write: ensure-table failed");
        return;
    }
    let value_str = value.to_string();
    if let Err(e) = conn.execute(
        "INSERT INTO metadata(key, value) VALUES(?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value_str],
    ) {
        tracing::warn!(path = %path.display(), error = %e, key, value, "device metadata write: upsert failed");
    }
}

fn mark_segment_applied_best_effort(path: &Path) {
    if !path.exists() {
        with_device_tracer(|t| {
            tracer_info!(t, "LogSegment", "mark APPLIED skipped (missing): {}", path.display())
        });
        return;
    }
    let conn = match Connection::open(path) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "mark APPLIED: open failed");
            with_device_tracer(|t| {
                tracer_info!(t, "LogSegment", "mark APPLIED open failed: {} :: {}", path.display(), e)
            });
            return;
        }
    };
    match conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS metadata(key TEXT, value TEXT);\n\
         DELETE FROM metadata WHERE key = 'status';\n\
         INSERT INTO metadata(key, value) VALUES('status', 'APPLIED');",
    ) {
        Ok(()) => {
            with_device_tracer(|t| {
                tracer_info!(t, "LogSegment", "mark APPLIED OK : {}", path.display())
            });
        }
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "mark APPLIED: write failed");
            with_device_tracer(|t| {
                tracer_info!(t, "LogSegment", "mark APPLIED write failed: {} :: {}", path.display(), e)
            });
        }
    }
}

// Device-type classification mirrors Java's `SyncLiteLoggerInfo` taxonomy
// (see synclite-consolidator/.../SyncLiteLoggerInfo.java). The Rust logger
// only emits artifacts for SQLITE/DUCKDB (sqllog) and SQLITE_STORE/
// DUCKDB_STORE/STREAMING (cdclog), but the consolidator must accept every
// Java-recognized device type because shipped artifacts share the same
// on-disk format regardless of producer.
//
// Buckets:
//   * transactional/SQL  → sqllog, needs DeviceReplicator
//       SQLITE, DUCKDB, DERBY, H2, HYPERSQL
//   * appender/store/dblogger/streaming → cdclog (or equivalent),
//       fed directly to the consolidator with no replicator pass
//       SQLITE_APPENDER, DUCKDB_APPENDER, DERBY_APPENDER, H2_APPENDER,
//       HYPERSQL_APPENDER, SQLITE_STORE, DUCKDB_STORE, DERBY_STORE,
//       H2_STORE, HYPERSQL_STORE, DBLOGGER, STREAMING
fn is_store_or_streaming_device(layout: &ConsolidatorLayout) -> bool {
    let device_type = layout.device_type.trim().to_ascii_uppercase();
    matches!(
        device_type.as_str(),
        "SQLITE_STORE"
            | "DUCKDB_STORE"
            | "DERBY_STORE"
            | "H2_STORE"
            | "HYPERSQL_STORE"
            | "SQLITE_APPENDER"
            | "DUCKDB_APPENDER"
            | "DERBY_APPENDER"
            | "H2_APPENDER"
            | "HYPERSQL_APPENDER"
            | "DBLOGGER"
            | "STREAMING"
    )
}

#[allow(dead_code)]
fn is_store_device(layout: &ConsolidatorLayout) -> bool {
    let device_type = layout.device_type.trim().to_ascii_uppercase();
    matches!(
        device_type.as_str(),
        "SQLITE_STORE" | "DUCKDB_STORE" | "DERBY_STORE" | "H2_STORE" | "HYPERSQL_STORE"
    )
}

#[allow(dead_code)]
fn is_appender_device(layout: &ConsolidatorLayout) -> bool {
    let device_type = layout.device_type.trim().to_ascii_uppercase();
    matches!(
        device_type.as_str(),
        "SQLITE_APPENDER"
            | "DUCKDB_APPENDER"
            | "DERBY_APPENDER"
            | "H2_APPENDER"
            | "HYPERSQL_APPENDER"
    )
}

#[allow(dead_code)]
fn is_streaming_device(layout: &ConsolidatorLayout) -> bool {
    let device_type = layout.device_type.trim().to_ascii_uppercase();
    matches!(device_type.as_str(), "STREAMING" | "DBLOGGER")
}

fn is_sql_device(layout: &ConsolidatorLayout) -> bool {
    let device_type = layout.device_type.trim().to_ascii_uppercase();
    matches!(
        device_type.as_str(),
        "SQLITE" | "DUCKDB" | "DERBY" | "H2" | "HYPERSQL"
    )
}

fn run_device_replicator(layout: &ConsolidatorLayout, work_command_log_path: &Path) -> Result<PathBuf> {
    if !is_sqllog_path(work_command_log_path) {
        return Ok(work_command_log_path.to_path_buf());
    }

    // Apply command-log statements onto a per-device replica through native FFI.
    // CDC artifact emission remains on the same segment path contract for the
    // consolidator stage (one emitted segment per input segment).
    let replica_path = resolve_replicator_replica_path(layout)?;
    let seg_seq = parse_segment_seq(work_command_log_path).ok_or_else(|| {
        Error::Config(format!(
            "consolidator: invalid command log segment path {}",
            work_command_log_path.display()
        ))
    })?;

    let cdc_log_path = layout.device_work_dir.join(format!("{seg_seq}.cdclog"));

    // Idempotency: if the cdclog for this segment was already produced by an
    // earlier worker_loop poll, skip the replay. Re-running the replay against
    // the now-mutated replica produces zero events and would wipe a valid
    // cdclog (NativeCdcLogWriter::new unconditionally removes its target
    // file). Detect "already produced" by either (a) status=APPLIED metadata
    // (destination already consumed it) or (b) any rows in the cdclog table
    // (writer succeeded; either pending apply or apply in progress).
    let existing_rows = if cdc_log_path.exists() {
        Connection::open(&cdc_log_path)
            .and_then(|c| c.query_row("SELECT COUNT(*) FROM cdclog", [], |r| r.get::<_, i64>(0)))
            .ok()
    } else {
        None
    };

    if env::var("SYNCLITE_DEBUG_CDC_CALLBACK").ok().as_deref() == Some("1") {
        let exists = cdc_log_path.exists();
        let pre_rows = existing_rows
            .map(|n| n.to_string())
            .unwrap_or_else(|| if exists { "err".to_string() } else { "(no file)".to_string() });
        let line = format!("[replicator-call] seg={seg_seq} cdclog_exists={exists} pre_rows={pre_rows} cmdlog={}\n",
            work_command_log_path.display());
        eprintln!("{}", line.trim_end());
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open("C:\\tmp\\rust_debug.log") {
            use std::io::Write;
            let _ = f.write_all(line.as_bytes());
        }
    }

    if let Some(rows) = existing_rows {
        if rows > 0 {
            with_device_tracer(|t| {
                tracer_info!(
                    t,
                    "DeviceReplicator",
                    "Skipping replicate (cdclog already produced) segment={} rows={} cdcLogPath={}",
                    work_command_log_path.display(),
                    rows,
                    cdc_log_path.display()
                )
            });
            if env::var("SYNCLITE_DEBUG_CDC_CALLBACK").ok().as_deref() == Some("1") {
                let line = format!("[replicator-call] skip seg={seg_seq} rows={rows}\n");
                eprintln!("{}", line.trim_end());
                if let Ok(mut f) = OpenOptions::new().create(true).append(true).open("C:\\tmp\\rust_debug.log") {
                    use std::io::Write;
                    let _ = f.write_all(line.as_bytes());
                }
            }
            return Ok(cdc_log_path);
        }
    }

    // Java parity: when the native CDC library is present, we drive cdclog
    // writes through `NativeCdcLogWriter` (mirrors
    // DeviceReplicator+CDCLogger+CDCLogSegmentWriter using bindNativeValue
    // pointer pass-through). If the library is missing or initialization
    // fails, we fall back to the legacy rusqlite-based `write_cdclog_segment`.
    let replay = replay_segment_with_native_cdc(
        &replica_path,
        work_command_log_path,
        seg_seq as i64,
        layout,
        &cdc_log_path,
    )?;

    if !replay.writer_used {
        write_cdclog_segment(&cdc_log_path, &replay.entries, Some(&replay.events), &replica_path)?;
    }
    with_device_tracer(|t| {
        tracer_info!(
            t,
            "DeviceReplicator",
            "Replicated {} command logs from segment : {} (cdcLogPath={}, writer={})",
            replay.entries.len(),
            work_command_log_path.display(),
            cdc_log_path.display(),
            if replay.writer_used { "native" } else { "legacy" }
        )
    });
    Ok(cdc_log_path)
}

fn resolve_replicator_replica_path(layout: &ConsolidatorLayout) -> Result<PathBuf> {
    let preferred = layout
        .device_work_dir
        .join(format!("{}.synclite.backup", layout.database_name));
    if preferred.exists() {
        return Ok(preferred);
    }

    let legacy = layout.device_work_dir.join("synclite_replicator_replica.db");
    if legacy.exists() {
        if !preferred.exists() {
            std::fs::copy(&legacy, &preferred).map_err(|e| {
                Error::Internal(format!(
                    "consolidator: failed to migrate legacy replica {} to {}: {e}",
                    legacy.display(),
                    preferred.display()
                ))
            })?;
            return Ok(preferred);
        }
        return Ok(legacy);
    }

    if let Ok(entries) = std::fs::read_dir(&layout.device_work_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(".synclite.backup"))
                .unwrap_or(false)
            {
                return Ok(path);
            }
        }
    }

    Ok(preferred)
}

struct ReplayOutput {
    entries: Vec<SegmentEntry>,
    events: Vec<CdcEvent>,
    writer_used: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnFate {
    Commit,
    Rollback,
    Unknown,
}

#[derive(Debug, Clone, Copy)]
struct ReplicaResumeCheckpoint {
    command_log_segment_sequence_number: i64,
    commit_id: i64,
    cdc_log_segment_sequence_number: i64,
}

#[derive(Debug, Clone, Copy)]
struct DestinationResumeCheckpoint {
    cdc_log_segment_sequence_number: i64,
    cdc_change_number: i64,
}

#[derive(Debug, Clone)]
struct CdcEvent {
    change_number: i64,
    commit_id: i64,
    op_type: String,
    table_name: String,
    before_count: usize,
    after_count: usize,
    args: Vec<SqlValue>,
}

fn replay_segment_with_native_cdc(
    replica_path: &Path,
    segment_path: &Path,
    command_log_segment_seq: i64,
    layout: &ConsolidatorLayout,
    cdc_log_path: &Path,
) -> Result<ReplayOutput> {
    let seg_conn = Connection::open(segment_path).map_err(map_sql_err)?;
    // Java parity (`DeviceSyncProcessor.inMemoryReplicaConn`): acquire the
    // worker-owned schema replica. Held for the duration of this replay so
    // the outer segment read + every REPLAY_TXN sidecar share state, and
    // returned to the thread-local on drop for the next segment to reuse.
    let mut guard = acquire_schema_replica(layout)?;
    let schema_replica = guard.as_mut();
    let (_first_commit, mut entries) = read_segment_entries_with_replica(&seg_conn, schema_replica)?;

    if let Some(resume) = read_replica_resume_checkpoint(replica_path)? {
        with_device_tracer(|t| {
            tracer_info!(
                t,
                "DeviceReplicator",
                "Replay checkpoint recovered : commandlogSeq={} commitID={} cdclogSeq={}",
                resume.command_log_segment_sequence_number,
                resume.commit_id,
                resume.cdc_log_segment_sequence_number
            )
        });
        if env::var("SYNCLITE_DEBUG_CDC_CALLBACK").ok().as_deref() == Some("1") {
            let line = format!("[replay-resume] seg={command_log_segment_seq} resume.cmdSeq={} resume.commit={} resume.cdcSeq={}\n",
                resume.command_log_segment_sequence_number,
                resume.commit_id,
                resume.cdc_log_segment_sequence_number);
            eprintln!("{}", line.trim_end());
            if let Ok(mut f) = OpenOptions::new().create(true).append(true).open("C:\\tmp\\rust_debug.log") {
                use std::io::Write;
                let _ = f.write_all(line.as_bytes());
            }
        }
        // Resume by CDC segment sequence when available; fallback to command-log
        // sequence for legacy checkpoints.
        let resume_segment_seq = if resume.cdc_log_segment_sequence_number > 0 {
            resume.cdc_log_segment_sequence_number
        } else {
            resume.command_log_segment_sequence_number
        };

        if resume_segment_seq > command_log_segment_seq {
            return Ok(ReplayOutput {
                entries: Vec::new(),
                events: Vec::new(),
                writer_used: false,
            });
        }
        if resume_segment_seq == command_log_segment_seq {
            entries.retain(|entry| entry.commit_id > resume.commit_id);
        }
    }

    let raw_entries_len = entries.len();
    entries = retain_committed_entries(entries);
    let committed_len = entries.len();
    entries = expand_replay_txn_entries(entries, segment_path, command_log_segment_seq, schema_replica)?;
    let expanded_len = entries.len();
    let debug_callback = env::var("SYNCLITE_DEBUG_CDC_CALLBACK").ok().as_deref() == Some("1");
    if debug_callback {
        let line = format!("[replay-entries] seg={command_log_segment_seq} raw={raw_entries_len} committed={committed_len} expanded={expanded_len} segment={}\n",
            segment_path.display());
        eprintln!("{}", line.trim_end());
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open("C:\\tmp\\rust_debug.log") {
            use std::io::Write;
            let _ = f.write_all(line.as_bytes());
        }
    }
    if entries.is_empty() {
        return Ok(ReplayOutput {
            entries: Vec::new(),
            events: Vec::new(),
            writer_used: false,
        });
    }

    match NativeCdc::load() {
        Ok(native) => {
            if debug_callback {
                eprintln!("[synclite-rust] native CDC library loaded");
            }
            // Java parity: try to bring up a NativeCdcLogWriter so the cdclog
            // file is written via prepared statements + bindNativeValue. Fall
            // back to event-buffering + post-replay assembly if the writer
            // cannot be created (e.g. file permission or native error).
            let writer_attempt = NativeCdcLogWriter::new(native.stmt_api(), cdc_log_path);
            match writer_attempt {
                Ok(writer) => {
                    let writer_mutex = Mutex::new(writer);
                    let replay_res = native.replay(
                        replica_path,
                        &entries,
                        command_log_segment_seq,
                        segment_path,
                        layout,
                        Some(&writer_mutex),
                    );
                    // Always finalize the writer regardless of replay outcome.
                    let writer = writer_mutex.into_inner().map_err(|_| {
                        Error::Internal("cdclog writer mutex poisoned".into())
                    })?;
                    let close_res = writer.close();
                    let events = replay_res?;
                    close_res?;
                    if env::var("SYNCLITE_DEBUG_CDC_CALLBACK").ok().as_deref() == Some("1") {
                        let n = Connection::open(cdc_log_path)
                            .and_then(|c| c.query_row("SELECT COUNT(*) FROM cdclog", [], |r| r.get::<_, i64>(0)))
                            .map(|n| n.to_string())
                            .unwrap_or_else(|e| format!("err: {e}"));
                        let line = format!("[cdclog-after-close] cdclog rows={} ({})\n", n, cdc_log_path.display());
                        eprintln!("{}", line.trim_end());
                        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open("C:\\tmp\\rust_debug.log") {
                            use std::io::Write;
                            let _ = f.write_all(line.as_bytes());
                        }
                    }
                    Ok(ReplayOutput {
                        entries,
                        events,
                        writer_used: true,
                    })
                }
                Err(writer_err) => {
                    tracing::warn!(
                        error = %writer_err,
                        "NativeCdcLogWriter init failed; falling back to legacy cdclog writer"
                    );
                    let events = native.replay(
                        replica_path,
                        &entries,
                        command_log_segment_seq,
                        segment_path,
                        layout,
                        None,
                    )?;
                    Ok(ReplayOutput {
                        entries,
                        events,
                        writer_used: false,
                    })
                }
            }
        }
        Err(load_err) => {
            if debug_callback {
                eprintln!("[synclite-rust] native CDC library load failed: {}", load_err);
            }
            tracing::warn!(
                error = %load_err,
                "libsynclitecdc not loaded; SQL replicator replay skipped and segment will use compatibility CDC artifact"
            );
            Ok(ReplayOutput {
                entries,
                events: Vec::new(),
                writer_used: false,
            })
        }
    }
}

fn expand_replay_txn_entries(
    entries: Vec<SegmentEntry>,
    segment_path: &Path,
    segment_seq: i64,
    schema_replica: &mut Connection,
) -> Result<Vec<SegmentEntry>> {
    // Java parity (DeviceSyncProcessor.inMemoryReplicaConn): one schema
    // replica is shared across the entire segment apply (outer segment +
    // every sidecar). Each sidecar may contain ALTER TABLE / ADD COLUMN
    // that depends on a CREATE TABLE emitted by an earlier sidecar or by
    // the outer segment; using a fresh replica per sidecar would trip
    // PRAGMA table_info on a missing table.
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        if entry.sql.trim_start().eq_ignore_ascii_case("REPLAY_TXN") {
            let sidecar = replay_txn_sidecar_path(segment_path, segment_seq as u64, entry.commit_id as u64);
            if !sidecar.exists() {
                return Err(Error::Config(format!(
                    "consolidator: missing txn sidecar for REPLAY_TXN marker: {}",
                    sidecar.display()
                )));
            }
            let conn = Connection::open(&sidecar).map_err(map_sql_err)?;
            let (_, side_entries) = read_commandlog_entries_with_replica(&conn, schema_replica)?;
            // Java parity (CommandLogSegment.java L84-86, L168-169): sidecar
            // entries carry their own intra-sidecar change_number/commit_id
            // (starting at 0) but logically belong to the outer REPLAY_TXN
            // transaction. Re-stamp each entry with the outer marker's
            // commit_id and change_number so downstream filters
            // (retain_committed_entries) and per-entry checkpoint progress
            // see them as part of the wrapping outer commit, not as
            // orphan commit_id=0 rows that get dropped or never advance
            // the destination checkpoint.
            let outer_commit = entry.commit_id;
            let outer_change = entry.change_number;
            for side in side_entries {
                let sql_norm = side.sql.trim_start().to_ascii_uppercase();
                if sql_norm.starts_with("BEGIN")
                    || sql_norm.starts_with("COMMIT")
                    || sql_norm.starts_with("ROLLBACK")
                    || sql_norm.starts_with("CHECKPOINT")
                    || sql_norm.starts_with("PRAGMA")
                {
                    continue;
                }
                out.push(SegmentEntry {
                    commit_id: outer_commit,
                    change_number: outer_change,
                    ..side
                });
            }
            continue;
        }
        out.push(entry);
    }
    Ok(out)
}

fn txn_fates_by_commit_id(entries: &[SegmentEntry]) -> HashMap<i64, TxnFate> {
    let mut fates = HashMap::new();
    for entry in entries {
        let sql_norm = entry.sql.trim_start().to_ascii_uppercase();
        if sql_norm.starts_with("COMMIT") {
            fates.insert(entry.commit_id, TxnFate::Commit);
        } else if sql_norm.starts_with("ROLLBACK") {
            fates.insert(entry.commit_id, TxnFate::Rollback);
        } else {
            fates.entry(entry.commit_id).or_insert(TxnFate::Unknown);
        }
    }
    fates
}

fn retain_committed_entries(entries: Vec<SegmentEntry>) -> Vec<SegmentEntry> {
    let fates = txn_fates_by_commit_id(&entries);
    entries
        .into_iter()
        .filter(|entry| {
            matches!(
                fates.get(&entry.commit_id).copied().unwrap_or(TxnFate::Unknown),
                TxnFate::Commit
            )
        })
        .collect()
}

fn read_replica_resume_checkpoint(replica_path: &Path) -> Result<Option<ReplicaResumeCheckpoint>> {
    if !replica_path.exists() {
        return Ok(None);
    }

    let conn = Connection::open(replica_path).map_err(map_sql_err)?;
    let has_table = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='replay_checkpoint' LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(map_sql_err)?
        .is_some();
    if !has_table {
        return Ok(None);
    }

    // Try new schema with cdc_log_segment_sequence_number column first.
    let new_schema_result = conn
        .query_row(
            "SELECT commandlog_segment_sequence_number, commit_id, \
             cdc_log_segment_sequence_number \
             FROM replay_checkpoint ORDER BY commandlog_segment_sequence_number DESC, commit_id DESC LIMIT 1",
            [],
            |row| {
                Ok(ReplicaResumeCheckpoint {
                    command_log_segment_sequence_number: row.get(0)?,
                    commit_id: row.get(1)?,
                    cdc_log_segment_sequence_number: row.get(2)?,
                })
            },
        )
        .optional();

    match new_schema_result {
        Ok(row) => return Ok(row),
        Err(_) => {
            // Fallback for old schema without cdc_log_segment_sequence_number.
            let row = conn
                .query_row(
                    "SELECT commandlog_segment_sequence_number, commit_id \
                     FROM replay_checkpoint ORDER BY commandlog_segment_sequence_number DESC, commit_id DESC LIMIT 1",
                    [],
                    |row| {
                        let cmd_seq: i64 = row.get(0)?;
                        Ok(ReplicaResumeCheckpoint {
                            command_log_segment_sequence_number: cmd_seq,
                            commit_id: row.get(1)?,
                            cdc_log_segment_sequence_number: cmd_seq,
                        })
                    },
                )
                .optional()
                .map_err(map_sql_err)?;
            Ok(row)
        }
    }
}

fn read_destination_resume_checkpoint(layout: &ConsolidatorLayout) -> Result<Option<DestinationResumeCheckpoint>> {
    let sql = "SELECT cdc_log_segment_sequence_number, cdc_change_number \
               FROM synclite_checkpoint \
               WHERE synclite_device_id = ?1 AND synclite_device_name = ?2 \
               ORDER BY commit_id DESC LIMIT 1";
    match layout.dst_type {
        DstType::Sqlite => {
            if !std::path::Path::new(&layout.dst_connection_string).exists() {
                return Ok(None);
            }
            let conn = Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
            let has_table = conn
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='synclite_checkpoint' LIMIT 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(map_sql_err)?
                .is_some();
            if !has_table {
                return Ok(None);
            }
            let row = conn
                .query_row(sql, params![layout.device_id, layout.device_name], |row| {
                    Ok(DestinationResumeCheckpoint {
                        cdc_log_segment_sequence_number: row.get(0)?,
                        cdc_change_number: row.get(1)?,
                    })
                })
                .optional()
                .map_err(map_sql_err)?;
            Ok(row)
        }
        DstType::DuckDb => {
            if !std::path::Path::new(&layout.dst_connection_string).exists() {
                return Ok(None);
            }
            let conn = DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
            let has_table: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'synclite_checkpoint'",
                    [],
                    |row| row.get(0),
                )
                .map_err(map_duck_err)?;
            if has_table == 0 {
                return Ok(None);
            }
            let mut stmt = conn.prepare(sql).map_err(map_duck_err)?;
            let params = [
                DuckValue::Text(layout.device_id.clone()),
                DuckValue::Text(layout.device_name.clone()),
            ];
            let mut rows = stmt
                .query(duck_params_from_iter(params.iter()))
                .map_err(map_duck_err)?;
            if let Some(row) = rows.next().map_err(map_duck_err)? {
                Ok(Some(DestinationResumeCheckpoint {
                    cdc_log_segment_sequence_number: row.get(0).map_err(map_duck_err)?,
                    cdc_change_number: row.get(1).map_err(map_duck_err)?,
                }))
            } else {
                Ok(None)
            }
        }
        DstType::Postgres => {
            let conn_str = build_postgres_connection_string(layout);
            let mut client = PgClient::connect(&conn_str, NoTls).map_err(map_pg_err)?;
            // Brand-new destination: the consolidator-owned bookkeeping
            // tables (`synclite_checkpoint` et al.) only get CREATEd on
            // the first successful apply via `ensure_postgres_metadata_seeded`.
            // Probing before they exist would surface as `42P01
            // undefined_table` and abort the very first segment apply,
            // so guard the lookup the same way the SQLite / DuckDB
            // branches above do.
            let schema_opt: Option<String> = if layout.dst_use_schema_scope_resolution {
                layout
                    .dst_schema
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            } else {
                None
            };
            let has_table: bool = if let Some(schema) = schema_opt.as_deref() {
                client
                    .query_one(
                        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                         WHERE table_schema = $1 AND table_name = 'synclite_checkpoint')",
                        &[&schema],
                    )
                    .map_err(map_pg_err)?
                    .get(0)
            } else {
                client
                    .query_one(
                        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                         WHERE table_name = 'synclite_checkpoint')",
                        &[],
                    )
                    .map_err(map_pg_err)?
                    .get(0)
            };
            if !has_table {
                return Ok(None);
            }
            let meta = pg_meta_table(layout, "synclite_checkpoint");
            let select_sql = format!(
                "SELECT cdc_log_segment_sequence_number, cdc_change_number \
                 FROM {meta} \
                 WHERE synclite_device_id = $1 AND synclite_device_name = $2 \
                 ORDER BY commit_id DESC LIMIT 1"
            );
            let row = client
                .query_opt(&select_sql, &[&layout.device_id, &layout.device_name])
                .map_err(map_pg_err)?;
            Ok(row.map(|r| DestinationResumeCheckpoint {
                cdc_log_segment_sequence_number: r.get(0),
                cdc_change_number: r.get(1),
            }))
        }
    }
}

fn segment_max_change_number(segment_path: &Path) -> Result<Option<i64>> {
    let conn = Connection::open(segment_path).map_err(map_sql_err)?;
    if has_table(&conn, "cdclog")? {
        let v = conn
            .query_row("SELECT MAX(change_number) FROM cdclog", [], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .map_err(map_sql_err)?;
        return Ok(v);
    }
    if has_table(&conn, "commandlog")? {
        let v = conn
            .query_row("SELECT MAX(change_number) FROM commandlog", [], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .map_err(map_sql_err)?;
        return Ok(v);
    }
    Ok(None)
}

fn should_skip_segment_by_destination_checkpoint(
    resume: Option<DestinationResumeCheckpoint>,
    segment_seq: i64,
    segment_path: &Path,
) -> Result<bool> {
    let Some(resume) = resume else {
        return Ok(false);
    };
    let resume_seq = resume.cdc_log_segment_sequence_number;
    let resume_change = resume.cdc_change_number;

    if resume_seq > segment_seq {
        return Ok(true);
    }
    if resume_seq < segment_seq {
        return Ok(false);
    }

    let Some(max_change) = segment_max_change_number(segment_path)? else {
        return Ok(false);
    };
    Ok(resume_change >= max_change)
}

fn write_cdclog_segment(
    cdclog_path: &Path,
    entries: &[SegmentEntry],
    native_events: Option<&[CdcEvent]>,
    replica_path: &Path,
) -> Result<()> {
    if cdclog_path.exists() {
        std::fs::remove_file(cdclog_path)?;
    }

    let conn = Connection::open(cdclog_path).map_err(map_sql_err)?;
    let max_entry_arg_cnt = entries.iter().map(|e| e.args.len()).max().unwrap_or(0);
    let max_event_arg_cnt = native_events
        .map(|events| events.iter().map(|e| e.args.len()).max().unwrap_or(0))
        .unwrap_or(0);
    let max_arg_cnt = max_entry_arg_cnt.max(max_event_arg_cnt).max(16);

    let arg_cols = (1..=max_arg_cnt)
        .map(|i| format!("arg{i} BLOB"))
        .collect::<Vec<_>>()
        .join(", ");
    let create_cdclog_sql = format!(
        "CREATE TABLE IF NOT EXISTS cdclog(\
         commit_id LONG, database_name TEXT, schema_name TEXT, table_name TEXT,\
         op_type TEXT, sql TEXT, change_number INTEGER PRIMARY KEY, arg_cnt INTEGER, {arg_cols}\
         );"
    );

    conn.execute_batch(&create_cdclog_sql).map_err(map_sql_err)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS cdclog_schemas(\
         change_number INTEGER, database_name TEXT, schema_name TEXT, table_name TEXT,\
         column_index LONG, column_name TEXT, column_type TEXT, column_not_null INTEGER,\
         column_default_value BLOB, column_primary_key INTEGER, column_auto_increment INTEGER,\
         old_table_name TEXT, old_column_name TEXT\
         );\
         CREATE TABLE IF NOT EXISTS metadata(key TEXT, value TEXT);\
         CREATE INDEX IF NOT EXISTS commit_id_cdclog_index ON cdclog(commit_id);\
         CREATE INDEX IF NOT EXISTS change_number_cdclog_schemas_index ON cdclog_schemas(change_number);",
    )
    .map_err(map_sql_err)?;

    let mut events_by_change: HashMap<i64, Vec<CdcEvent>> = HashMap::new();
    let mut events_by_commit: HashMap<i64, VecDeque<CdcEvent>> = HashMap::new();
    if let Some(events) = native_events {
        for ev in events {
            events_by_change
                .entry(ev.change_number)
                .or_default()
                .push(ev.clone());
            events_by_commit
                .entry(ev.commit_id)
                .or_default()
                .push_back(ev.clone());
        }
    }

    let mut next_change_number: i64 = 0;
    let mut table_columns: HashMap<String, Vec<String>> = load_replica_table_columns(replica_path)?;
    let mut last_callback_sql: Option<String> = None;

    for entry in entries {
        if entry.sql.trim_start().eq_ignore_ascii_case("REPLAY_TXN") {
            continue;
        }
        let op_type = map_cdclog_op_type(&entry.sql);
        let table_name = extract_table_name_for_operation(&entry.sql);
        if op_type == "CREATETABLE" {
            if let Some(tbl) = table_name.as_deref() {
                let cols = parse_create_table_columns(&entry.sql);
                let pk_cols: Vec<&str> = cols
                    .iter()
                    .filter(|(_, _, pk)| *pk > 0)
                    .map(|(n, _, _)| n.as_str())
                    .collect();
                tracing::debug!(table = %tbl, columns = cols.len(), pk_cols = ?pk_cols, "cdclog CREATE TABLE parsed");
                table_columns.insert(
                    tbl.to_ascii_lowercase(),
                    cols.iter().map(|(c, _, _)| c.clone()).collect(),
                );
                for (idx, (col_name, col_type, pk_pos)) in cols.iter().enumerate() {
                    conn.execute(
                        "INSERT INTO cdclog_schemas(change_number, database_name, schema_name, table_name, \
                         column_index, column_name, column_type, column_not_null, column_default_value, \
                         column_primary_key, column_auto_increment, old_table_name, old_column_name) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, NULL, ?8, 0, NULL, NULL)",
                        params![entry.change_number, "main", SqlValue::Null, tbl, idx as i64, col_name, col_type, *pk_pos],
                    )
                    .map_err(map_sql_err)?;
                }
            }
        } else if op_type == "ADDCOLUMN" {
            if let Some(tbl) = table_name.as_deref() {
                if let Some(col) = parse_alter_add_column_name(&entry.sql) {
                    table_columns
                        .entry(tbl.to_ascii_lowercase())
                        .or_default()
                        .push(col);
                }
            }
        } else if op_type == "DROPCOLUMN" {
            if let Some(tbl) = table_name.as_deref() {
                if let Some(col) = parse_alter_drop_column_name(&entry.sql) {
                    if let Some(cols) = table_columns.get_mut(&tbl.to_ascii_lowercase()) {
                        cols.retain(|c| !c.eq_ignore_ascii_case(&col));
                    }
                }
            }
        } else if op_type == "RENAMECOLUMN" {
            if let Some(tbl) = table_name.as_deref() {
                if let Some((old_col, new_col)) = parse_alter_rename_column_names(&entry.sql) {
                    if let Some(cols) = table_columns.get_mut(&tbl.to_ascii_lowercase()) {
                        for c in cols.iter_mut() {
                            if c.eq_ignore_ascii_case(&old_col) {
                                *c = new_col.clone();
                            }
                        }
                    }
                }
            }
        } else if op_type == "RENAMETABLE" {
            if let Some(old_tbl) = table_name.as_deref() {
                if let Some(new_tbl) = parse_alter_rename_table_name(&entry.sql) {
                    let old_key = old_tbl.to_ascii_lowercase();
                    if let Some(cols) = table_columns.remove(&old_key) {
                        table_columns.insert(new_tbl.to_ascii_lowercase(), cols);
                    }
                }
            }
        }

        if !matches!(op_type, "INSERT" | "UPDATE" | "DELETE") {
            // Java parity: callback SQL de-dup cache resets around non-DML transitions.
            last_callback_sql = None;
        }

        if matches!(op_type, "INSERT" | "UPDATE" | "DELETE") {
            let mut matched_event: Option<CdcEvent> = None;
            if let Some(queue) = events_by_commit.get_mut(&entry.commit_id) {
                let desired_op = op_type.to_ascii_uppercase();
                let desired_table = table_name.clone().unwrap_or_default().to_ascii_lowercase();
                if let Some(front) = queue.front() {
                    let front_op = front.op_type.to_ascii_uppercase();
                    let front_table = front.table_name.to_ascii_lowercase();
                    if front_op == desired_op
                        && (desired_table.is_empty() || desired_table == front_table)
                    {
                        matched_event = queue.pop_front();
                    }
                }
                if matched_event.is_none() {
                    if let Some(pos) = queue.iter().position(|ev| {
                        ev.op_type.eq_ignore_ascii_case(op_type)
                            && (table_name.is_none()
                                || table_name
                                    .as_deref()
                                    .map(|t| ev.table_name.eq_ignore_ascii_case(t))
                                    .unwrap_or(false))
                    }) {
                        matched_event = queue.remove(pos);
                    }
                }
            }

            if let Some(ev) = matched_event {
                let mut rows_to_emit = vec![ev];
                if entry.args.is_empty() {
                    if let Some(queue) = events_by_commit.get_mut(&entry.commit_id) {
                        let mut idx = 0usize;
                        while idx < queue.len() {
                            let is_match = queue[idx].op_type.eq_ignore_ascii_case(op_type)
                                && (table_name.is_none()
                                    || table_name
                                        .as_deref()
                                        .map(|t| queue[idx].table_name.eq_ignore_ascii_case(t))
                                        .unwrap_or(false));
                            if is_match {
                                if let Some(extra) = queue.remove(idx) {
                                    rows_to_emit.push(extra);
                                    continue;
                                }
                            }
                            idx += 1;
                        }
                    }
                }

                for (idx, ev) in rows_to_emit.into_iter().enumerate() {
                    validate_callback_event_shape(&ev, &table_columns)?;
                    let cols = table_columns
                        .get(&ev.table_name.to_ascii_lowercase())
                        .cloned()
                        .unwrap_or_default();
                    let sql_for_event = build_callback_sql(&ev, &entry.sql, &cols);
                    let row_args = callback_args_java_order(&ev);
                    let sql_for_event_owned = if last_callback_sql
                        .as_deref()
                        .map(|s| s == sql_for_event)
                        .unwrap_or(false)
                    {
                        None
                    } else {
                        last_callback_sql = Some(sql_for_event.clone());
                        Some(sql_for_event)
                    };
                    let sql_for_event_ref = if idx == 0 {
                        sql_for_event_owned.as_deref()
                    } else {
                        None
                    };
                    emit_cdclog_row(
                        &conn,
                        ev.commit_id,
                        &ev.table_name,
                        &ev.op_type,
                        sql_for_event_ref,
                        next_change_number,
                        &row_args,
                    )?;
                    next_change_number += 1;
                }
                continue;
            }

            if let Some(ev_rows) = events_by_change.remove(&entry.change_number) {
                for (idx, ev) in ev_rows.into_iter().enumerate() {
                    validate_callback_event_shape(&ev, &table_columns)?;
                    let cols = table_columns
                        .get(&ev.table_name.to_ascii_lowercase())
                        .cloned()
                        .unwrap_or_default();
                    let sql_for_event = build_callback_sql(&ev, &entry.sql, &cols);
                    let row_args = callback_args_java_order(&ev);
                    let sql_for_event_owned = if last_callback_sql
                        .as_deref()
                        .map(|s| s == sql_for_event)
                        .unwrap_or(false)
                    {
                        None
                    } else {
                        last_callback_sql = Some(sql_for_event.clone());
                        Some(sql_for_event)
                    };
                    let sql_for_event_ref = if idx == 0 {
                        sql_for_event_owned.as_deref()
                    } else {
                        None
                    };
                    emit_cdclog_row(
                        &conn,
                        ev.commit_id,
                        &ev.table_name,
                        &ev.op_type,
                        sql_for_event_ref,
                        next_change_number,
                        &row_args,
                    )?;
                    next_change_number += 1;
                }
                continue;
            }
        }

        emit_cdclog_row(
            &conn,
            entry.commit_id,
            table_name.as_deref().unwrap_or(""),
            op_type,
            Some(&entry.sql),
            next_change_number,
            &entry.args,
        )?;
        next_change_number += 1;
    }

    Ok(())
}

fn emit_cdclog_row(
    conn: &Connection,
    commit_id: i64,
    table_name: &str,
    op_type: &str,
    sql_text: Option<&str>,
    change_number: i64,
    args: &[SqlValue],
) -> Result<()> {
    let mut columns = vec![
        "commit_id".to_string(),
        "database_name".to_string(),
        "schema_name".to_string(),
        "table_name".to_string(),
        "op_type".to_string(),
        "sql".to_string(),
        "change_number".to_string(),
        "arg_cnt".to_string(),
    ];
    for i in 1..=args.len() {
        columns.push(format!("arg{i}"));
    }

    let placeholders = (1..=columns.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT INTO cdclog({}) VALUES ({})",
        columns.join(", "),
        placeholders
    );

    let mut vals = Vec::with_capacity(columns.len());
    vals.push(SqlValue::Integer(commit_id));
    vals.push(SqlValue::Text("main".to_string()));
    vals.push(SqlValue::Null);
    vals.push(if table_name.is_empty() {
        SqlValue::Null
    } else {
        SqlValue::Text(table_name.to_string())
    });
    vals.push(SqlValue::Text(op_type.to_string()));
    vals.push(match sql_text {
        Some(text) => SqlValue::Text(text.to_string()),
        None => SqlValue::Null,
    });
    vals.push(SqlValue::Integer(change_number));
    vals.push(SqlValue::Integer(args.len() as i64));
    vals.extend(args.iter().cloned());

    conn.execute(&sql, params_from_iter(vals)).map_err(map_sql_err)?;
    Ok(())
}

fn parse_create_table_columns(sql: &str) -> Vec<(String, String, i64)> {
    let upper = sql.to_ascii_uppercase();
    let Some(create_idx) = upper.find("CREATE TABLE") else {
        return Vec::new();
    };
    let rest = &sql[create_idx + "CREATE TABLE".len()..];
    let Some(open_idx) = rest.find('(') else {
        return Vec::new();
    };
    let body = &rest[open_idx + 1..];
    let Some(close_idx) = body.rfind(')') else {
        return Vec::new();
    };
    let cols_body = &body[..close_idx];
    let parts = split_top_level_commas(cols_body);

    let mut cols: Vec<(String, String, i64)> = Vec::new();
    let mut table_level_pks: Vec<String> = Vec::new();
    let mut pos: i64 = 0;
    for part in parts {
        let p = part.trim();
        if p.is_empty() {
            continue;
        }
        let pu = p.to_ascii_uppercase();
        if pu.starts_with("PRIMARY KEY") {
            if let Some(po) = p.find('(') {
                if let Some(pc) = p.rfind(')') {
                    if pc > po {
                        for c in p[po + 1..pc].split(',') {
                            let n = c
                                .trim()
                                .trim_matches('"')
                                .trim_matches('`')
                                .trim_matches('[')
                                .trim_matches(']')
                                .to_string();
                            if !n.is_empty() {
                                table_level_pks.push(n);
                            }
                        }
                    }
                }
            }
            continue;
        }
        if pu.starts_with("UNIQUE")
            || pu.starts_with("CONSTRAINT")
            || pu.starts_with("FOREIGN KEY")
            || pu.starts_with("CHECK")
        {
            continue;
        }
        let mut toks = p.split_whitespace();
        let name = toks
            .next()
            .unwrap_or("")
            .trim_matches('"')
            .trim_matches('`')
            .trim_matches('[')
            .trim_matches(']')
            .to_string();
        if name.is_empty() {
            continue;
        }
        let col_type = toks.next().unwrap_or("TEXT").to_string();
        // Java parity: detect inline `PRIMARY KEY` in column tail tokens.
        let inline_pk = pu.contains(" PRIMARY KEY") || pu.ends_with(" PRIMARY KEY");
        let pk_pos = if inline_pk { pos + 1 } else { 0 };
        cols.push((name, col_type, pk_pos));
        pos += 1;
    }

    if !table_level_pks.is_empty() {
        for (i, pk_name) in table_level_pks.iter().enumerate() {
            if let Some(entry) = cols
                .iter_mut()
                .find(|(n, _, _)| n.eq_ignore_ascii_case(pk_name))
            {
                if entry.2 == 0 {
                    entry.2 = (i as i64) + 1;
                }
            }
        }
    }

    cols
}

fn parse_alter_add_column_name(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let idx = upper.find(" ADD COLUMN ")?;
    let rest = sql[idx + " ADD COLUMN ".len()..].trim();
    let col = rest
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .trim_end_matches(';');
    if col.is_empty() { None } else { Some(col.to_string()) }
}

fn parse_alter_drop_column_name(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let idx = upper.find(" DROP COLUMN ")?;
    let rest = sql[idx + " DROP COLUMN ".len()..].trim();
    let col = rest
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .trim_end_matches(';');
    if col.is_empty() { None } else { Some(col.to_string()) }
}

fn parse_alter_rename_column_names(sql: &str) -> Option<(String, String)> {
    let upper = sql.to_ascii_uppercase();
    let idx = upper.find(" RENAME COLUMN ")?;
    let rest = &sql[idx + " RENAME COLUMN ".len()..];
    let upper_rest = rest.to_ascii_uppercase();
    let to_idx = upper_rest.find(" TO ")?;
    let old_col = rest[..to_idx]
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string();
    let new_col = rest[to_idx + " TO ".len()..]
        .trim()
        .trim_end_matches(';')
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string();
    if old_col.is_empty() || new_col.is_empty() {
        None
    } else {
        Some((old_col, new_col))
    }
}

fn parse_alter_rename_table_name(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let idx = upper.find(" RENAME TO ")?;
    let rest = sql[idx + " RENAME TO ".len()..].trim();
    let new_tbl = rest
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .trim_end_matches(';');
    if new_tbl.is_empty() {
        None
    } else {
        Some(new_tbl.to_string())
    }
}

fn build_callback_sql(ev: &CdcEvent, fallback_sql: &str, columns: &[String]) -> String {
    let arg_len = ev.args.len();
    if ev.op_type.eq_ignore_ascii_case("INSERT") {
        let placeholders = (0..arg_len).map(|_| "?").collect::<Vec<_>>().join(", ");
        if !columns.is_empty() && columns.len() == arg_len {
            return format!(
                "INSERT INTO {} ({}) VALUES ({})",
                ev.table_name,
                columns.join(", "),
                placeholders
            );
        }
        return format!("INSERT INTO {} VALUES ({})", ev.table_name, placeholders);
    }
    if ev.op_type.eq_ignore_ascii_case("UPDATE") && arg_len % 2 == 0 && arg_len > 0 {
        let n = arg_len / 2;
        let cols = if columns.len() >= n { &columns[..n] } else { &[] };
        if !cols.is_empty() {
            let set_clause = cols.iter().map(|c| format!("{c} = ?")).collect::<Vec<_>>().join(", ");
            let where_clause = cols.iter().map(|c| format!("{c} = ?")).collect::<Vec<_>>().join(" AND ");
            return format!("UPDATE {} SET {} WHERE {}", ev.table_name, set_clause, where_clause);
        }
    }
    if ev.op_type.eq_ignore_ascii_case("DELETE") && arg_len > 0 {
        let cols = if columns.len() >= arg_len { &columns[..arg_len] } else { &[] };
        if !cols.is_empty() {
            let where_clause = cols.iter().map(|c| format!("{c} = ?")).collect::<Vec<_>>().join(" AND ");
            return format!("DELETE FROM {} WHERE {}", ev.table_name, where_clause);
        }
    }
    fallback_sql.to_string()
}

fn callback_args_java_order(ev: &CdcEvent) -> Vec<SqlValue> {
    // Java parity: UPDATE rows persist before-image values first, then after-image values.
    ev.args.clone()
}

fn validate_callback_event_shape(
    ev: &CdcEvent,
    table_columns: &HashMap<String, Vec<String>>,
) -> Result<()> {
    let table_key = ev.table_name.to_ascii_lowercase();
    let cols = table_columns.get(&table_key).ok_or_else(|| {
        Error::Internal(format!(
            "native CDC callback received changes for missing table '{}': schema not available",
            ev.table_name
        ))
    })?;

    let op = ev.op_type.to_ascii_uppercase();
    match op.as_str() {
        "INSERT" => {
            if ev.after_count != cols.len() {
                return Err(Error::Internal(format!(
                    "native CDC callback after-image width mismatch for table '{}': received {}, expected {}",
                    ev.table_name,
                    ev.after_count,
                    cols.len()
                )));
            }
            if ev.args.len() != ev.after_count {
                return Err(Error::Internal(format!(
                    "native CDC callback INSERT argument width mismatch for table '{}': received {}, expected {}",
                    ev.table_name,
                    ev.args.len(),
                    ev.after_count
                )));
            }
        }
        "UPDATE" => {
            if ev.before_count != cols.len() {
                return Err(Error::Internal(format!(
                    "native CDC callback before-image width mismatch for table '{}': received {}, expected {}",
                    ev.table_name,
                    ev.before_count,
                    cols.len()
                )));
            }
            if ev.after_count != cols.len() {
                return Err(Error::Internal(format!(
                    "native CDC callback after-image width mismatch for table '{}': received {}, expected {}",
                    ev.table_name,
                    ev.after_count,
                    cols.len()
                )));
            }
            let expected = ev.before_count + ev.after_count;
            if ev.args.len() != expected {
                return Err(Error::Internal(format!(
                    "native CDC callback UPDATE argument width mismatch for table '{}': received {}, expected {}",
                    ev.table_name,
                    ev.args.len(),
                    expected
                )));
            }
        }
        "DELETE" => {
            if ev.before_count != cols.len() {
                return Err(Error::Internal(format!(
                    "native CDC callback before-image width mismatch for table '{}': received {}, expected {}",
                    ev.table_name,
                    ev.before_count,
                    cols.len()
                )));
            }
            if ev.args.len() != ev.before_count {
                return Err(Error::Internal(format!(
                    "native CDC callback DELETE argument width mismatch for table '{}': received {}, expected {}",
                    ev.table_name,
                    ev.args.len(),
                    ev.before_count
                )));
            }
        }
        _ => {}
    }

    Ok(())
}

fn load_replica_table_columns(replica_path: &Path) -> Result<HashMap<String, Vec<String>>> {
    if !replica_path.exists() {
        return Ok(HashMap::new());
    }

    let conn = Connection::open(replica_path).map_err(map_sql_err)?;
    let mut out: HashMap<String, Vec<String>> = HashMap::new();

    // Exclude SyncLite/replicator internal tables only. Filtering by name
    // prefix (LIKE 'sqlite\_%') is fragile: `_` is a single-char LIKE
    // wildcard, so a pattern like 'sqlite_%' silently drops legitimate
    // user tables such as `sqlitetransactional_table`.
    let mut stmt = conn
        .prepare(
            "SELECT name FROM sqlite_master \
             WHERE type='table' \
               AND name NOT IN ( \
                   'synclite_txn', \
                   'synclite_dbreader_checkpoint', \
                   'synclite_logreader_checkpoint', \
                   'replay_checkpoint', \
                   'sqlite_sequence' \
               )",
        )
        .map_err(map_sql_err)?;
    let mut rows = stmt.query([]).map_err(map_sql_err)?;
    while let Some(row) = rows.next().map_err(map_sql_err)? {
        let table_name: String = row.get(0).map_err(map_sql_err)?;
        let pragma_sql = format!("PRAGMA table_info({})", quote_sqlite_ident(&table_name));
        let mut p = conn.prepare(&pragma_sql).map_err(map_sql_err)?;
        let mut pr = p.query([]).map_err(map_sql_err)?;
        let mut cols = Vec::new();
        while let Some(crow) = pr.next().map_err(map_sql_err)? {
            let col_name: String = crow.get(1).map_err(map_sql_err)?;
            cols.push(col_name);
        }
        out.insert(table_name.to_ascii_lowercase(), cols);
    }

    Ok(out)
}

fn quote_sqlite_ident(raw: &str) -> String {
    format!("\"{}\"", raw.replace('"', "\"\""))
}

fn map_cdclog_op_type(sql: &str) -> &'static str {
    let norm = sql.trim_start().to_ascii_uppercase();
    if norm.starts_with("BEGIN") {
        return "BEGINTRAN";
    }
    if norm.starts_with("COMMIT") {
        return "COMMITTRAN";
    }
    if norm.starts_with("ROLLBACK") {
        return "ROLLBACKTRAN";
    }
    if norm.starts_with("CHECKPOINT") {
        return "CHECKPOINTTRAN";
    }
    if norm.starts_with("INSERT") {
        return "INSERT";
    }
    if norm.starts_with("UPDATE") {
        return "UPDATE";
    }
    if norm.starts_with("DELETE") {
        return "DELETE";
    }
    if norm.starts_with("CREATE TABLE") {
        return "CREATETABLE";
    }
    if norm.starts_with("DROP TABLE") {
        return "DROPTABLE";
    }
    if norm.starts_with("ALTER TABLE") && norm.contains(" RENAME TO ") {
        return "RENAMETABLE";
    }
    if norm.starts_with("ALTER TABLE") && norm.contains(" ADD COLUMN ") {
        return "ADDCOLUMN";
    }
    if norm.starts_with("ALTER TABLE") && norm.contains(" DROP COLUMN ") {
        return "DROPCOLUMN";
    }
    if norm.starts_with("ALTER TABLE") && norm.contains(" RENAME COLUMN ") {
        return "RENAMECOLUMN";
    }
    if norm.starts_with("ALTER TABLE") {
        return "ALTERCOLUMN";
    }
    "NOOP"
}

struct NativeCdc {
    _lib: Library,
    open: unsafe extern "C" fn(*const c_char, *mut *mut c_void) -> c_int,
    close: unsafe extern "C" fn(*mut c_void) -> c_int,
    exec: unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int,
    errmsg: unsafe extern "C" fn(*mut c_void) -> *const c_char,
    set_change_callback:
        unsafe extern "C" fn(*mut c_void, Option<NativeChangeCallback>, *mut c_void) -> c_int,
    clear_change_callback: unsafe extern "C" fn(*mut c_void) -> c_int,
    value_type: unsafe extern "C" fn(*const c_void) -> c_int,
    value_int: unsafe extern "C" fn(*const c_void) -> c_int,
    value_int64: unsafe extern "C" fn(*const c_void) -> i64,
    value_double: unsafe extern "C" fn(*const c_void) -> f64,
    value_text: unsafe extern "C" fn(*const c_void) -> *const c_char,
    value_blob: unsafe extern "C" fn(*const c_void, *mut c_int) -> *const c_void,
    // Java-parity prepared-statement surface (mirrors
    // com.synclite.consolidator.nativedb.PreparedStatement JNI methods):
    prepare: unsafe extern "C" fn(*mut c_void, *const c_char, *mut *mut c_void) -> c_int,
    finalize_stmt: unsafe extern "C" fn(*mut c_void) -> c_int,
    reset: unsafe extern "C" fn(*mut c_void) -> c_int,
    clear_bindings: unsafe extern "C" fn(*mut c_void) -> c_int,
    step: unsafe extern "C" fn(*mut c_void) -> c_int,
    bind_int: unsafe extern "C" fn(*mut c_void, c_int, c_int) -> c_int,
    bind_int64: unsafe extern "C" fn(*mut c_void, c_int, i64) -> c_int,
    bind_double: unsafe extern "C" fn(*mut c_void, c_int, f64) -> c_int,
    bind_text: unsafe extern "C" fn(*mut c_void, c_int, *const c_char) -> c_int,
    bind_blob: unsafe extern "C" fn(*mut c_void, c_int, *const c_void, c_int) -> c_int,
    bind_null: unsafe extern "C" fn(*mut c_void, c_int) -> c_int,
    // Java parity: bindNativeValue(int index, long value) — sqlite3_bind_value(sqlite3_value*).
    bind_value: unsafe extern "C" fn(*mut c_void, c_int, *const c_void) -> c_int,
}

type NativeChangeCallback = unsafe extern "C" fn(
    *mut c_void,
    *const c_char,
    *const c_char,
    *const c_char,
    *const *const c_void,
    c_int,
    *const *const c_void,
    c_int,
) -> c_int;

#[derive(Clone, Copy)]
struct NativeValueApi {
    value_type: unsafe extern "C" fn(*const c_void) -> c_int,
    value_int: unsafe extern "C" fn(*const c_void) -> c_int,
    value_int64: unsafe extern "C" fn(*const c_void) -> i64,
    value_double: unsafe extern "C" fn(*const c_void) -> f64,
    value_text: unsafe extern "C" fn(*const c_void) -> *const c_char,
    value_blob: unsafe extern "C" fn(*const c_void, *mut c_int) -> *const c_void,
}

struct ReplayContext {
    api: NativeValueApi,
    current_commit: Mutex<i64>,
    current_change: Mutex<i64>,
    events: Mutex<Vec<CdcEvent>>,
    callback_invocations: Mutex<i64>,
    callback_ops: Mutex<HashMap<String, i64>>,
    last_error: Mutex<Option<String>>,

    // Java parity: native writer plumbing. When `writer_ptr` is non-null the
    // change callback writes cdclog rows directly using `bindNativeValue`
    // (pointer pass-through), exactly mirroring
    // `DeviceReplicator.getChanges -> CDCLogger.log -> CDCLogSegment.writeCDCLog`.
    //
    // The pointer references a `Mutex<NativeCdcLogWriter>` owned by the caller
    // of `NativeCdc::replay`; lifetime safety is enforced by the caller (the
    // writer outlives the replay).
    writer_ptr: *mut Mutex<NativeCdcLogWriter>,
    table_columns: Mutex<HashMap<String, Vec<String>>>,
    last_callback_sql: Mutex<Option<String>>,
    next_change_number: Mutex<i64>,
}

unsafe extern "C" fn native_change_callback(
    user_ctx: *mut c_void,
    database_name: *const c_char,
    table_name: *const c_char,
    operation: *const c_char,
    before_image: *const *const c_void,
    before_count: c_int,
    after_image: *const *const c_void,
    after_count: c_int,
) -> c_int {
    if user_ctx.is_null() {
        return 1;
    }
    // SAFETY: user_ctx is set from Box<ReplayContext> and lives through callback registration.
    let ctx = unsafe { &*(user_ctx as *mut ReplayContext) };

    let op = cstr_to_string(operation).unwrap_or_else(|| "UNKNOWN".to_string());
    let table = cstr_to_string(table_name).unwrap_or_default();
    let database = cstr_to_string(database_name).unwrap_or_else(|| "main".to_string());

    if table.eq_ignore_ascii_case("replay_checkpoint") {
        return 0;
    }

    let commit_id = match ctx.current_commit.lock() {
        Ok(v) => *v,
        Err(_) => return 1,
    };

    if let Ok(mut invocations) = ctx.callback_invocations.lock() {
        *invocations += 1;
    }
    if let Ok(mut op_counts) = ctx.callback_ops.lock() {
        *op_counts.entry(op.clone()).or_insert(0) += 1;
    }

    // Java parity: when a writer is configured, write the cdclog row directly
    // from this callback using pointer pass-through. Values are sqlite3_value*
    // duped by the native preupdate hook and freed immediately after this
    // callback returns, so the write must complete before we return.
    if !ctx.writer_ptr.is_null() {
        // SAFETY: writer_ptr points to a Mutex<NativeCdcLogWriter> owned by
        // the caller of replay() and guaranteed to outlive this callback.
        let writer_mtx = unsafe { &*ctx.writer_ptr };

        // Build raw pointer slices viewing the native arrays directly.
        let before_slice: &[*const c_void] = if before_image.is_null() || before_count <= 0 {
            &[]
        } else {
            // SAFETY: native preupdate hook hands us an array of before_count entries.
            unsafe { std::slice::from_raw_parts(before_image, before_count as usize) }
        };
        let after_slice: &[*const c_void] = if after_image.is_null() || after_count <= 0 {
            &[]
        } else {
            // SAFETY: native preupdate hook hands us an array of after_count entries.
            unsafe { std::slice::from_raw_parts(after_image, after_count as usize) }
        };

        // Width validation against current table schema (Java parity).
        let cols_for_table: Vec<String> = match ctx.table_columns.lock() {
            Ok(g) => g
                .get(&table.to_ascii_lowercase())
                .cloned()
                .unwrap_or_default(),
            Err(_) => Vec::new(),
        };

        let op_upper = op.to_ascii_uppercase();
        if !cols_for_table.is_empty() {
            let width = cols_for_table.len();
            let ok = match op_upper.as_str() {
                "INSERT" => after_slice.len() == width,
                "UPDATE" => before_slice.len() == width && after_slice.len() == width,
                "DELETE" => before_slice.len() == width,
                _ => true,
            };
            if !ok {
                if let Ok(mut err) = ctx.last_error.lock() {
                    *err = Some(format!(
                        "native CDC callback width mismatch table='{}' op={} before={} after={} expected={}",
                        table, op_upper, before_slice.len(), after_slice.len(), width
                    ));
                }
                return 1;
            }
        }

        // Synthesized SQL (mirrors DeviceReplicator.buildCallbackSql).
        let sql_for_event = build_callback_sql_from_pointers(
            &op_upper,
            &table,
            &cols_for_table,
            before_slice.len(),
            after_slice.len(),
        );

        // De-dup: only persist sql when it differs from the prior callback
        // (Java's lastCallbackSql cache).
        let sql_text_to_persist: Option<String> = match ctx.last_callback_sql.lock() {
            Ok(mut last) => {
                if last.as_deref() == Some(sql_for_event.as_str()) {
                    None
                } else {
                    *last = Some(sql_for_event.clone());
                    Some(sql_for_event.clone())
                }
            }
            Err(_) => Some(sql_for_event.clone()),
        };

        let change_number = match ctx.next_change_number.lock() {
            Ok(mut cn) => {
                let v = *cn;
                *cn = v + 1;
                v
            }
            Err(_) => return 1,
        };

        let mut writer = match writer_mtx.lock() {
            Ok(w) => w,
            Err(_) => return 1,
        };
        if let Err(e) = writer.write_dml_record(
            commit_id,
            Some(&database),
            None,
            Some(&table),
            &op_upper,
            sql_text_to_persist.as_deref(),
            change_number,
            before_slice,
            after_slice,
        ) {
            if let Ok(mut err) = ctx.last_error.lock() {
                *err = Some(format!("native CDC writer failed: {e}"));
            }
            return 1;
        }
        // Stats-only event push so legacy callers/diagnostics still see counts.
        if let Ok(mut events) = ctx.events.lock() {
            events.push(CdcEvent {
                change_number,
                commit_id,
                op_type: op_upper,
                table_name: table,
                before_count: before_slice.len(),
                after_count: after_slice.len(),
                args: Vec::new(),
            });
        }
        return 0;
    }

    // Legacy path (no writer): materialize values and buffer events for
    // post-replay reconciliation by `write_cdclog_segment`.
    let change_number = match ctx.current_change.lock() {
        Ok(v) => *v,
        Err(_) => return 1,
    };

    let args = match op.as_str() {
        "INSERT" => read_native_values(&ctx.api, after_image, after_count),
        "DELETE" => read_native_values(&ctx.api, before_image, before_count),
        "UPDATE" => {
            let mut vals = read_native_values(&ctx.api, before_image, before_count);
            vals.extend(read_native_values(&ctx.api, after_image, after_count));
            vals
        }
        _ => Vec::new(),
    };

    match ctx.events.lock() {
        Ok(mut events) => events.push(CdcEvent {
            change_number,
            commit_id,
            op_type: op,
            table_name: table,
            before_count: if before_count > 0 { before_count as usize } else { 0 },
            after_count: if after_count > 0 { after_count as usize } else { 0 },
            args,
        }),
        Err(_) => return 1,
    }

    0
}

/// Java parity: build the synthesized SQL recorded in cdclog from the table
/// schema, mirroring `DeviceReplicator.buildCallbackSql`. Returns the same
/// fallback (qualified table reference) Java falls back to when no schema is
/// available so de-dup behavior matches.
fn build_callback_sql_from_pointers(
    op_upper: &str,
    table: &str,
    columns: &[String],
    before_len: usize,
    after_len: usize,
) -> String {
    if op_upper == "INSERT" && after_len > 0 {
        let placeholders = (0..after_len).map(|_| "?").collect::<Vec<_>>().join(", ");
        if !columns.is_empty() && columns.len() == after_len {
            return format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table,
                columns.join(", "),
                placeholders
            );
        }
        return format!("INSERT INTO {} VALUES ({})", table, placeholders);
    }
    if op_upper == "UPDATE" && before_len > 0 && after_len > 0 {
        let cols = if columns.len() >= after_len { &columns[..after_len] } else { &[][..] };
        if !cols.is_empty() {
            let set_clause = cols.iter().map(|c| format!("{c} = ?")).collect::<Vec<_>>().join(", ");
            let where_clause = cols.iter().map(|c| format!("{c} = ?")).collect::<Vec<_>>().join(" AND ");
            return format!("UPDATE {} SET {} WHERE {}", table, set_clause, where_clause);
        }
    }
    if op_upper == "DELETE" && before_len > 0 {
        let cols = if columns.len() >= before_len { &columns[..before_len] } else { &[][..] };
        if !cols.is_empty() {
            let where_clause = cols.iter().map(|c| format!("{c} = ?")).collect::<Vec<_>>().join(" AND ");
            return format!("DELETE FROM {} WHERE {}", table, where_clause);
        }
    }
    format!("{} {}", op_upper, table)
}

fn cstr_to_string(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: pointer is expected to reference a null-terminated UTF-8/bytes string.
    Some(unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned())
}

fn read_native_values(
    api: &NativeValueApi,
    arr_ptr: *const *const c_void,
    count: c_int,
) -> Vec<SqlValue> {
    if arr_ptr.is_null() || count <= 0 {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(count as usize);
    for i in 0..(count as isize) {
        // SAFETY: native callback provides array of count pointers.
        let v_ptr = unsafe { *arr_ptr.offset(i) };
        if v_ptr.is_null() {
            out.push(SqlValue::Null);
            continue;
        }

        // sqlite type affinity: 1=int,2=float,3=text,4=blob,5=null.
        let v_type = unsafe { (api.value_type)(v_ptr) };
        let val = match v_type {
            1 => SqlValue::Integer(unsafe { (api.value_int64)(v_ptr) }),
            2 => SqlValue::Real(unsafe { (api.value_double)(v_ptr) }),
            3 => {
                let txt = unsafe { (api.value_text)(v_ptr) };
                if txt.is_null() {
                    SqlValue::Null
                } else {
                    SqlValue::Text(unsafe { CStr::from_ptr(txt) }.to_string_lossy().into_owned())
                }
            }
            4 => {
                let mut len: c_int = 0;
                let blob_ptr = unsafe { (api.value_blob)(v_ptr, &mut len as *mut c_int) };
                if blob_ptr.is_null() || len <= 0 {
                    SqlValue::Blob(Vec::new())
                } else {
                    // SAFETY: value_blob returns pointer valid for callback duration.
                    let bytes = unsafe {
                        std::slice::from_raw_parts(blob_ptr as *const u8, len as usize)
                    };
                    SqlValue::Blob(bytes.to_vec())
                }
            }
            _ => {
                let int_val = unsafe { (api.value_int)(v_ptr) };
                if int_val == 0 {
                    SqlValue::Null
                } else {
                    SqlValue::Integer(int_val as i64)
                }
            }
        };
        out.push(val);
    }
    out
}

impl NativeCdc {
    fn load() -> Result<Self> {
        for candidate in candidate_native_library_paths() {
            // SAFETY: Library is kept alive in the struct while symbols are used.
            let lib = unsafe { Library::new(&candidate) };
            let Ok(lib) = lib else {
                continue;
            };

            // SAFETY: Symbol signatures match the C ABI contract in RustBindings.java.
            unsafe {
                let open = *lib
                    .get::<unsafe extern "C" fn(*const c_char, *mut *mut c_void) -> c_int>(b"synclite_rs_open\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_open: {e}")))?;
                let close = *lib
                    .get::<unsafe extern "C" fn(*mut c_void) -> c_int>(b"synclite_rs_close\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_close: {e}")))?;
                let exec = *lib
                    .get::<unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int>(b"synclite_rs_exec\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_exec: {e}")))?;
                let errmsg = *lib
                    .get::<unsafe extern "C" fn(*mut c_void) -> *const c_char>(b"synclite_rs_errmsg\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_errmsg: {e}")))?;
                let set_change_callback = *lib
                    .get::<unsafe extern "C" fn(*mut c_void, Option<NativeChangeCallback>, *mut c_void) -> c_int>(
                        b"synclite_rs_set_change_callback\0",
                    )
                    .map_err(|e| Error::Internal(format!("load synclite_rs_set_change_callback: {e}")))?;
                let clear_change_callback = *lib
                    .get::<unsafe extern "C" fn(*mut c_void) -> c_int>(b"synclite_rs_clear_change_callback\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_clear_change_callback: {e}")))?;
                let value_type = *lib
                    .get::<unsafe extern "C" fn(*const c_void) -> c_int>(b"synclite_rs_value_type\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_value_type: {e}")))?;
                let value_int = *lib
                    .get::<unsafe extern "C" fn(*const c_void) -> c_int>(b"synclite_rs_value_int\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_value_int: {e}")))?;
                let value_int64 = *lib
                    .get::<unsafe extern "C" fn(*const c_void) -> i64>(b"synclite_rs_value_int64\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_value_int64: {e}")))?;
                let value_double = *lib
                    .get::<unsafe extern "C" fn(*const c_void) -> f64>(b"synclite_rs_value_double\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_value_double: {e}")))?;
                let value_text = *lib
                    .get::<unsafe extern "C" fn(*const c_void) -> *const c_char>(b"synclite_rs_value_text\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_value_text: {e}")))?;
                let value_blob = *lib
                    .get::<unsafe extern "C" fn(*const c_void, *mut c_int) -> *const c_void>(b"synclite_rs_value_blob\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_value_blob: {e}")))?;

                // Prepared-statement / bind surface (Java-parity write path).
                let prepare = *lib
                    .get::<unsafe extern "C" fn(*mut c_void, *const c_char, *mut *mut c_void) -> c_int>(b"synclite_rs_prepare\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_prepare: {e}")))?;
                let finalize_stmt = *lib
                    .get::<unsafe extern "C" fn(*mut c_void) -> c_int>(b"synclite_rs_finalize\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_finalize: {e}")))?;
                let reset = *lib
                    .get::<unsafe extern "C" fn(*mut c_void) -> c_int>(b"synclite_rs_reset\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_reset: {e}")))?;
                let clear_bindings = *lib
                    .get::<unsafe extern "C" fn(*mut c_void) -> c_int>(b"synclite_rs_clear_bindings\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_clear_bindings: {e}")))?;
                let step = *lib
                    .get::<unsafe extern "C" fn(*mut c_void) -> c_int>(b"synclite_rs_step\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_step: {e}")))?;
                let bind_int = *lib
                    .get::<unsafe extern "C" fn(*mut c_void, c_int, c_int) -> c_int>(b"synclite_rs_bind_int\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_bind_int: {e}")))?;
                let bind_int64 = *lib
                    .get::<unsafe extern "C" fn(*mut c_void, c_int, i64) -> c_int>(b"synclite_rs_bind_int64\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_bind_int64: {e}")))?;
                let bind_double = *lib
                    .get::<unsafe extern "C" fn(*mut c_void, c_int, f64) -> c_int>(b"synclite_rs_bind_double\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_bind_double: {e}")))?;
                let bind_text = *lib
                    .get::<unsafe extern "C" fn(*mut c_void, c_int, *const c_char) -> c_int>(b"synclite_rs_bind_text\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_bind_text: {e}")))?;
                let bind_blob = *lib
                    .get::<unsafe extern "C" fn(*mut c_void, c_int, *const c_void, c_int) -> c_int>(b"synclite_rs_bind_blob\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_bind_blob: {e}")))?;
                let bind_null = *lib
                    .get::<unsafe extern "C" fn(*mut c_void, c_int) -> c_int>(b"synclite_rs_bind_null\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_bind_null: {e}")))?;
                let bind_value = *lib
                    .get::<unsafe extern "C" fn(*mut c_void, c_int, *const c_void) -> c_int>(b"synclite_rs_bind_value\0")
                    .map_err(|e| Error::Internal(format!("load synclite_rs_bind_value: {e}")))?;

                return Ok(Self {
                    _lib: lib,
                    open,
                    close,
                    exec,
                    errmsg,
                    set_change_callback,
                    clear_change_callback,
                    value_type,
                    value_int,
                    value_int64,
                    value_double,
                    value_text,
                    value_blob,
                    prepare,
                    finalize_stmt,
                    reset,
                    clear_bindings,
                    step,
                    bind_int,
                    bind_int64,
                    bind_double,
                    bind_text,
                    bind_blob,
                    bind_null,
                    bind_value,
                });
            }
        }

        Err(Error::Internal(
            "failed to load libsynclitecdc (synclitecdc_x86_64/synclitecdc_x86)".to_string(),
        ))
    }

    fn replay(
        &self,
        replica_path: &Path,
        entries: &[SegmentEntry],
        command_log_segment_seq: i64,
        segment_path: &Path,
        layout: &ConsolidatorLayout,
        writer: Option<&Mutex<NativeCdcLogWriter>>,
    ) -> Result<Vec<CdcEvent>> {
        let replica_c = CString::new(replica_path.to_string_lossy().to_string())
            .map_err(|e| Error::Internal(format!("replica path contains NUL: {e}")))?;

        let mut db: *mut c_void = std::ptr::null_mut();
        // SAFETY: `replica_c` and output pointer are valid for call duration.
        let open_rc = unsafe { (self.open)(replica_c.as_ptr(), &mut db as *mut *mut c_void) };
        if open_rc != 0 || db.is_null() {
            return Err(Error::Internal(format!(
                "synclite_rs_open failed rc={open_rc}: {}",
                self.errmsg_text(db)
            )));
        }

        self.exec_sql(
            db,
                            "CREATE TABLE IF NOT EXISTS replay_checkpoint(\
                             commandlog_segment_sequence_number LONG NOT NULL,\
                             commit_id LONG NOT NULL,\
                             cdc_log_segment_sequence_number LONG NOT NULL\
                         )",
        )?;

                // Older replicas may already have a 2-column replay_checkpoint.
                // Add the CDC segment sequence column in place for restart correctness.
                self.exec_sql_allow_duplicate_column(
                        db,
                        "ALTER TABLE replay_checkpoint ADD COLUMN cdc_log_segment_sequence_number LONG NOT NULL DEFAULT 0",
                )?;

        let writer_ptr = writer
            .map(|w| w as *const Mutex<NativeCdcLogWriter> as *mut Mutex<NativeCdcLogWriter>)
            .unwrap_or(std::ptr::null_mut());
        let initial_table_columns = if writer_ptr.is_null() {
            HashMap::new()
        } else {
            match load_replica_table_columns(replica_path) {
                Ok(m) => {
                    if env::var("SYNCLITE_DEBUG_CDC_CALLBACK").ok().as_deref() == Some("1") {
                        let summary: Vec<String> = m.iter().map(|(k, v)| format!("{}={}", k, v.len())).collect();
                        let line = format!("[replay-init-cols] seg={command_log_segment_seq} replica={} tables=[{}]\n",
                            replica_path.display(), summary.join(","));
                        eprintln!("{}", line.trim_end());
                        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open("C:\\tmp\\rust_debug.log") {
                            use std::io::Write;
                            let _ = f.write_all(line.as_bytes());
                        }
                    }
                    m
                }
                Err(e) => {
                    if env::var("SYNCLITE_DEBUG_CDC_CALLBACK").ok().as_deref() == Some("1") {
                        let line = format!("[replay-init-cols] seg={command_log_segment_seq} replica={} ERR={}\n",
                            replica_path.display(), e);
                        eprintln!("{}", line.trim_end());
                        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open("C:\\tmp\\rust_debug.log") {
                            use std::io::Write;
                            let _ = f.write_all(line.as_bytes());
                        }
                    }
                    HashMap::new()
                }
            }
        };

        let ctx = Box::new(ReplayContext {
            api: NativeValueApi {
                value_type: self.value_type,
                value_int: self.value_int,
                value_int64: self.value_int64,
                value_double: self.value_double,
                value_text: self.value_text,
                value_blob: self.value_blob,
            },
            current_commit: Mutex::new(0),
            current_change: Mutex::new(0),
            events: Mutex::new(Vec::new()),
            callback_invocations: Mutex::new(0),
            callback_ops: Mutex::new(HashMap::new()),
            last_error: Mutex::new(None),
            writer_ptr,
            table_columns: Mutex::new(initial_table_columns),
            last_callback_sql: Mutex::new(None),
            next_change_number: Mutex::new(0),
        });
        let ctx_ptr = Box::into_raw(ctx);

        // SAFETY: callback and context pointer are valid through replay.
        let set_rc = unsafe { (self.set_change_callback)(db, Some(native_change_callback), ctx_ptr as *mut c_void) };
        if set_rc != 0 {
            // SAFETY: ctx_ptr came from Box::into_raw above.
            let _ = unsafe { Box::from_raw(ctx_ptr) };
            return Err(Error::Internal(format!(
                "synclite_rs_set_change_callback failed rc={set_rc}: {}",
                self.errmsg_text(db)
            )));
        }

        let replay_result = self.replay_entries(
            db,
            entries,
            ctx_ptr,
            command_log_segment_seq,
            segment_path,
            layout,
        );

        // SAFETY: clear callback before dropping ctx.
        let clear_rc = unsafe { (self.clear_change_callback)(db) };
        if clear_rc != 0 {
            tracing::warn!(rc = clear_rc, "synclite_rs_clear_change_callback returned non-zero");
        }

        // SAFETY: transfer ownership back and extract events.
        let ctx_box = unsafe { Box::from_raw(ctx_ptr) };
        let events = ctx_box.events.lock().map(|v| v.clone()).unwrap_or_default();
        let callback_invocations = ctx_box
            .callback_invocations
            .lock()
            .map(|v| *v)
            .unwrap_or(0);
        let callback_ops = ctx_box
            .callback_ops
            .lock()
            .map(|v| v.clone())
            .unwrap_or_default();

        // SAFETY: `db` came from `open` and may be closed exactly once.
        let close_rc = unsafe { (self.close)(db) };
        if close_rc != 0 {
            tracing::warn!(rc = close_rc, "synclite_rs_close returned non-zero");
        }

        replay_result?;
        if env::var("SYNCLITE_DEBUG_CDC_CALLBACK").ok().as_deref() == Some("1") {
            eprintln!(
                "[synclite-rust] callback_invocations={} callback_ops={:?} events={}",
                callback_invocations,
                callback_ops,
                events.len()
            );
            for ev in events.iter().take(10) {
                eprintln!(
                    "[synclite-rust] event change={} commit={} op={} table={} args={}",
                    ev.change_number,
                    ev.commit_id,
                    ev.op_type,
                    ev.table_name,
                    ev.args.len()
                );
            }
        }
        if let Ok(last_error) = ctx_box.last_error.lock() {
            if let Some(msg) = &*last_error {
                return Err(Error::Internal(format!("native CDC callback failed: {msg}")));
            }
        }
        Ok(events)
    }

    fn replay_entries(
        &self,
        db: *mut c_void,
        entries: &[SegmentEntry],
        ctx_ptr: *mut ReplayContext,
        command_log_segment_seq: i64,
        segment_path: &Path,
        layout: &ConsolidatorLayout,
    ) -> Result<()> {
        let mut in_txn = false;

        let apply_replay_checkpoint_update = |commit_id: i64| -> Result<()> {
            let update_sql = format!(
                "DELETE FROM replay_checkpoint; \
                 INSERT INTO replay_checkpoint(\
                     commandlog_segment_sequence_number,\
                     commit_id,\
                     cdc_log_segment_sequence_number\
                 ) VALUES ({command_seg}, {commit_id}, {cdc_seg})",
                command_seg = command_log_segment_seq,
                commit_id = commit_id,
                cdc_seg = command_log_segment_seq,
            );
            self.exec_sql(db, &update_sql)
        };

        for entry in entries {
            // SAFETY: ctx_ptr remains valid through replay lifecycle.
            if let Ok(mut commit_id) = unsafe { &*ctx_ptr }.current_commit.lock() {
                *commit_id = entry.commit_id;
            }
            if let Ok(mut change_number) = unsafe { &*ctx_ptr }.current_change.lock() {
                *change_number = entry.change_number;
            }

            let sql_norm = entry.sql.trim_start().to_ascii_uppercase();
            if sql_norm.starts_with("BEGIN") {
                if !in_txn {
                    self.exec_sql(db, "BEGIN TRANSACTION")?;
                    in_txn = true;
                }
                // Java parity: DeviceReplicator.beginTran emits a BEGINTRAN
                // cdclog row (sql="BEGIN", no table/args) inside the writer txn.
                // SAFETY: ctx_ptr valid for replay lifetime.
                let ctx_ref = unsafe { &*ctx_ptr };
                self.emit_txn_marker_via_writer(ctx_ref, "BEGINTRAN", "BEGIN", entry.commit_id)?;
                continue;
            }

            if sql_norm.starts_with("COMMIT") {
                if in_txn {
                    // Java parity: DML cdclog rows are written from the
                    // commit_hook callback during the dst-db COMMIT. The
                    // COMMITTRAN marker must follow those DML rows. The
                    // native commit_hook buffers preupdate entries and only
                    // dispatches the Rust callback inside exec("COMMIT"),
                    // so we exec the dst COMMIT first (firing DML writes
                    // into the open writer txn), then emit COMMITTRAN and
                    // flush the writer.
                    // SAFETY: ctx_ptr valid for replay lifetime.
                    let ctx_ref = unsafe { &*ctx_ptr };
                    apply_replay_checkpoint_update(entry.commit_id)?;
                    self.exec_sql(db, "COMMIT")?;
                    self.emit_txn_marker_via_writer(ctx_ref, "COMMITTRAN", "COMMIT", entry.commit_id)?;
                    self.flush_writer_via_ctx(ctx_ref)?;
                    in_txn = false;
                }
                continue;
            }
            if sql_norm.starts_with("ROLLBACK") {
                if in_txn {
                    // SAFETY: ctx_ptr valid for replay lifetime.
                    let ctx_ref = unsafe { &*ctx_ptr };
                    self.rollback_writer_via_ctx(ctx_ref)?;
                    self.exec_sql(db, "ROLLBACK")?;
                    in_txn = false;
                }
                continue;
            }
            if sql_norm.starts_with("CHECKPOINT") || sql_norm.starts_with("PRAGMA") {
                continue;
            }
            if sql_norm.starts_with("REPLAY_TXN") {
                if !in_txn {
                    self.exec_sql(db, "BEGIN TRANSACTION")?;
                    in_txn = true;
                }
                self.replay_staged_txn_sidecar(
                    db,
                    segment_path,
                    command_log_segment_seq,
                    entry.commit_id,
                    layout,
                    ctx_ptr,
                )?;
                continue;
            }

            // Java parity: DDL records are emitted by the replay loop BEFORE
            // the DDL is exec'd (mirrors `CDCLogger.logDDLRecord` ordering).
            // DML records are emitted from the change callback during commit.
            let op_type = map_cdclog_op_type(&entry.sql);
            let table_name = extract_table_name_for_operation(&entry.sql);
            // SAFETY: ctx_ptr valid for replay duration.
            let ctx_ref = unsafe { &*ctx_ptr };
            if !ctx_ref.writer_ptr.is_null()
                && !matches!(
                    op_type,
                    "INSERT" | "UPDATE" | "DELETE"
                        | "BEGINTRAN" | "COMMITTRAN" | "ROLLBACKTRAN" | "CHECKPOINTTRAN"
                        | "NOOP"
                )
            {
                self.emit_ddl_via_writer(ctx_ref, op_type, table_name.as_deref(), entry)?;
            }

            let rendered = render_sql_with_args(&entry.sql, &entry.args);
            if !in_txn {
                self.exec_sql(db, "BEGIN TRANSACTION")?;
                in_txn = true;
            }
            self.exec_sql(db, &rendered)?;
        }

        if in_txn {
            self.exec_sql(db, "ROLLBACK")?;
        }
        Ok(())
    }

    /// Java parity: write a transaction-boundary cdclog row (BEGINTRAN /
    /// COMMITTRAN). Mirrors `DeviceReplicator.logBeginRecord` and the
    /// COMMITTRAN portion of `DeviceReplicator.logCommitAndFlush`.
    fn emit_txn_marker_via_writer(
        &self,
        ctx: &ReplayContext,
        op_type: &str,
        sql_text: &str,
        commit_id: i64,
    ) -> Result<()> {
        if ctx.writer_ptr.is_null() {
            return Ok(());
        }
        // SAFETY: writer_ptr lifetime guaranteed by caller of replay().
        let writer_mtx = unsafe { &*ctx.writer_ptr };

        let change_number = match ctx.next_change_number.lock() {
            Ok(mut cn) => {
                let v = *cn;
                *cn = v + 1;
                v
            }
            Err(_) => return Err(Error::Internal("txn change_number lock poisoned".into())),
        };

        let mut writer = writer_mtx
            .lock()
            .map_err(|_| Error::Internal("writer lock poisoned".into()))?;
        writer.write_ddl_record(
            commit_id,
            Some("main"),
            None,
            None,
            op_type,
            Some(sql_text),
            change_number,
            &[],
        )?;
        // Reset DML dedup cache across txn boundaries.
        if let Ok(mut last) = ctx.last_callback_sql.lock() {
            *last = None;
        }
        Ok(())
    }

    /// Java parity: commit the writer's native cdclog txn (flushLogSegment).
    fn flush_writer_via_ctx(&self, ctx: &ReplayContext) -> Result<()> {
        if ctx.writer_ptr.is_null() {
            return Ok(());
        }
        // SAFETY: writer_ptr lifetime guaranteed by caller of replay().
        let writer_mtx = unsafe { &*ctx.writer_ptr };
        let mut writer = writer_mtx
            .lock()
            .map_err(|_| Error::Internal("writer lock poisoned".into()))?;
        writer.commit_tran()
    }

    /// Java parity: rollback the writer's native cdclog txn.
    fn rollback_writer_via_ctx(&self, ctx: &ReplayContext) -> Result<()> {
        if ctx.writer_ptr.is_null() {
            return Ok(());
        }
        // SAFETY: writer_ptr lifetime guaranteed by caller of replay().
        let writer_mtx = unsafe { &*ctx.writer_ptr };
        let mut writer = writer_mtx
            .lock()
            .map_err(|_| Error::Internal("writer lock poisoned".into()))?;
        writer.rollback_tran()
    }

    fn emit_ddl_via_writer(
        &self,
        ctx: &ReplayContext,
        op_type: &str,
        table_name: Option<&str>,
        entry: &SegmentEntry,
    ) -> Result<()> {
        if ctx.writer_ptr.is_null() {
            return Ok(());
        }
        // SAFETY: writer_ptr lifetime guaranteed by caller of replay().
        let writer_mtx = unsafe { &*ctx.writer_ptr };

        // Update in-memory table_columns map to match Java's logical schema
        // tracking before forwarding to the writer.
        let columns_for_schema: Vec<(String, String, i64)> = match op_type {
            "CREATETABLE" => {
                let cols = parse_create_table_columns(&entry.sql);
                if let Some(tbl) = table_name {
                    if let Ok(mut g) = ctx.table_columns.lock() {
                        g.insert(
                            tbl.to_ascii_lowercase(),
                            cols.iter().map(|(c, _, _)| c.clone()).collect(),
                        );
                    }
                }
                cols
            }
            "ADDCOLUMN" => {
                if let Some(tbl) = table_name {
                    if let Some(col) = parse_alter_add_column_name(&entry.sql) {
                        if let Ok(mut g) = ctx.table_columns.lock() {
                            g.entry(tbl.to_ascii_lowercase()).or_default().push(col);
                        }
                    }
                }
                Vec::new()
            }
            "DROPCOLUMN" => {
                if let Some(tbl) = table_name {
                    if let Some(col) = parse_alter_drop_column_name(&entry.sql) {
                        if let Ok(mut g) = ctx.table_columns.lock() {
                            if let Some(cols) = g.get_mut(&tbl.to_ascii_lowercase()) {
                                cols.retain(|c| !c.eq_ignore_ascii_case(&col));
                            }
                        }
                    }
                }
                Vec::new()
            }
            "RENAMECOLUMN" => {
                if let Some(tbl) = table_name {
                    if let Some((old_col, new_col)) = parse_alter_rename_column_names(&entry.sql) {
                        if let Ok(mut g) = ctx.table_columns.lock() {
                            if let Some(cols) = g.get_mut(&tbl.to_ascii_lowercase()) {
                                for c in cols.iter_mut() {
                                    if c.eq_ignore_ascii_case(&old_col) {
                                        *c = new_col.clone();
                                    }
                                }
                            }
                        }
                    }
                }
                Vec::new()
            }
            "RENAMETABLE" => {
                if let Some(old_tbl) = table_name {
                    if let Some(new_tbl) = parse_alter_rename_table_name(&entry.sql) {
                        if let Ok(mut g) = ctx.table_columns.lock() {
                            let old_key = old_tbl.to_ascii_lowercase();
                            if let Some(cols) = g.remove(&old_key) {
                                g.insert(new_tbl.to_ascii_lowercase(), cols);
                            }
                        }
                    }
                }
                Vec::new()
            }
            "DROPTABLE" => {
                if let Some(tbl) = table_name {
                    if let Ok(mut g) = ctx.table_columns.lock() {
                        g.remove(&tbl.to_ascii_lowercase());
                    }
                }
                Vec::new()
            }
            _ => Vec::new(),
        };

        let change_number = match ctx.next_change_number.lock() {
            Ok(mut cn) => {
                let v = *cn;
                *cn = v + 1;
                v
            }
            Err(_) => return Err(Error::Internal("ddl change_number lock poisoned".into())),
        };

        // Build schema-column descriptors borrowing into local owned strings.
        let schema_cols: Vec<CdcLogSchemaCol<'_>> = columns_for_schema
            .iter()
            .enumerate()
            .map(|(idx, (name, ty, pk_pos))| CdcLogSchemaCol {
                column_index: idx as i64,
                column_name: name.as_str(),
                column_type: ty.as_str(),
                not_null: false,
                primary_key_pos: *pk_pos,
                auto_increment: false,
                old_table_name: None,
                old_column_name: None,
            })
            .collect();

        let mut writer = writer_mtx
            .lock()
            .map_err(|_| Error::Internal("writer lock poisoned".into()))?;
        writer.write_ddl_record(
            entry.commit_id,
            Some("main"),
            None,
            table_name,
            op_type,
            Some(&entry.sql),
            change_number,
            &schema_cols,
        )?;
        // Java parity: dedup cache resets across DDL boundaries so the next
        // DML row always persists its synthesized SQL.
        if let Ok(mut last) = ctx.last_callback_sql.lock() {
            *last = None;
        }
        Ok(())
    }


    fn replay_staged_txn_sidecar(
        &self,
        db: *mut c_void,
        segment_path: &Path,
        command_log_segment_seq: i64,
        commit_id: i64,
        layout: &ConsolidatorLayout,
        ctx_ptr: *mut ReplayContext,
    ) -> Result<()> {
        let sidecar = replay_txn_sidecar_path(segment_path, command_log_segment_seq as u64, commit_id as u64);
        if !sidecar.exists() {
            return Err(Error::Config(format!(
                "consolidator: missing txn sidecar for REPLAY_TXN marker: {}",
                sidecar.display()
            )));
        }

        let conn = Connection::open(&sidecar).map_err(map_sql_err)?;
        let (_, entries) = read_segment_entries(&conn, layout)?;
        for sidecar_entry in entries {
            let sql_norm = sidecar_entry.sql.trim_start().to_ascii_uppercase();
            if sql_norm.starts_with("BEGIN")
                || sql_norm.starts_with("COMMIT")
                || sql_norm.starts_with("ROLLBACK")
                || sql_norm.starts_with("CHECKPOINT")
                || sql_norm.starts_with("PRAGMA")
            {
                continue;
            }

            // Java parity: sidecar entries carry their own intra-sidecar
            // change_number/commit_id (starting at 0), but logically they
            // are part of the outer REPLAY_TXN transaction. Re-stamp each
            // entry with the outer commit_id so that cdclog rows and the
            // commit_hook DML callback see the same commit as the wrapping
            // BEGIN/COMMIT triple in the main sqllog. Without this, DDL
            // would be persisted under commit_id=0 and the downstream
            // device-sync-processor would never advance past it.
            let entry = SegmentEntry {
                commit_id,
                ..sidecar_entry
            };

            // SAFETY: ctx_ptr remains valid through replay lifecycle.
            if let Ok(mut cur_commit) = unsafe { &*ctx_ptr }.current_commit.lock() {
                *cur_commit = commit_id;
            }
            if let Ok(mut cur_change) = unsafe { &*ctx_ptr }.current_change.lock() {
                *cur_change = entry.change_number;
            }

            // Java parity: sidecar DDL must update the writer cdclog and the
            // in-memory schema map exactly like the main replay loop does,
            // otherwise DML rows that follow in this txn have no schema and
            // the destination table never materializes.
            let op_type = map_cdclog_op_type(&entry.sql);
            let table_name = extract_table_name_for_operation(&entry.sql);
            // SAFETY: ctx_ptr valid for replay duration.
            let ctx_ref = unsafe { &*ctx_ptr };
            if !ctx_ref.writer_ptr.is_null()
                && !matches!(
                    op_type,
                    "INSERT" | "UPDATE" | "DELETE"
                        | "BEGINTRAN" | "COMMITTRAN" | "ROLLBACKTRAN" | "CHECKPOINTTRAN"
                        | "NOOP"
                )
            {
                self.emit_ddl_via_writer(ctx_ref, op_type, table_name.as_deref(), &entry)?;
            }

            let rendered = render_sql_with_args(&entry.sql, &entry.args);
            self.exec_sql(db, &rendered)?;
        }

        Ok(())
    }

    fn exec_sql(&self, db: *mut c_void, sql: &str) -> Result<()> {
        // Java parity (JDBCExecutor.exec): `tracer.debug("SQL : " + sql)` for
        // every statement executed against the replica.
        with_device_tracer(|t| tracer_debug!(t, "JDBCExecutor", "SQL : {}", sql));
        let c_sql = CString::new(sql).map_err(|e| Error::Internal(format!("sql contains NUL: {e}")))?;
        // SAFETY: db handle and SQL pointer are valid for call duration.
        let rc = unsafe { (self.exec)(db, c_sql.as_ptr()) };
        if rc == 0 {
            Ok(())
        } else {
            Err(Error::Internal(format!(
                "synclite_rs_exec failed rc={rc}: {}",
                self.errmsg_text(db)
            )))
        }
    }

    fn exec_sql_allow_duplicate_column(&self, db: *mut c_void, sql: &str) -> Result<()> {
        let c_sql = CString::new(sql).map_err(|e| Error::Internal(format!("sql contains NUL: {e}")))?;
        // SAFETY: db handle and SQL pointer are valid for call duration.
        let rc = unsafe { (self.exec)(db, c_sql.as_ptr()) };
        if rc == 0 {
            return Ok(());
        }

        let msg = self.errmsg_text(db).to_ascii_lowercase();
        if msg.contains("duplicate column") || msg.contains("already exists") {
            Ok(())
        } else {
            Err(Error::Internal(format!(
                "synclite_rs_exec failed rc={rc}: {msg}"
            )))
        }
    }

    fn errmsg_text(&self, db: *mut c_void) -> String {
        // SAFETY: errmsg returns either null or a null-terminated C string.
        let ptr = unsafe { (self.errmsg)(db) };
        if ptr.is_null() {
            return String::new();
        }
        // SAFETY: pointer is expected to remain valid long enough for conversion.
        unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned()
    }

    fn stmt_api(&self) -> NativeStmtApi {
        NativeStmtApi {
            open: self.open,
            close: self.close,
            exec: self.exec,
            errmsg: self.errmsg,
            prepare: self.prepare,
            finalize_stmt: self.finalize_stmt,
            reset: self.reset,
            clear_bindings: self.clear_bindings,
            step: self.step,
            bind_int: self.bind_int,
            bind_int64: self.bind_int64,
            bind_double: self.bind_double,
            bind_text: self.bind_text,
            bind_blob: self.bind_blob,
            bind_null: self.bind_null,
            bind_value: self.bind_value,
        }
    }
}

/// Java parity: subset of `synclite_rs_*` symbols required by the
/// `NativeCdcLogWriter` (mirrors `com.synclite.consolidator.nativedb.PreparedStatement` JNI).
#[derive(Clone, Copy)]
#[allow(dead_code)]
struct NativeStmtApi {
    open: unsafe extern "C" fn(*const c_char, *mut *mut c_void) -> c_int,
    close: unsafe extern "C" fn(*mut c_void) -> c_int,
    exec: unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int,
    errmsg: unsafe extern "C" fn(*mut c_void) -> *const c_char,
    prepare: unsafe extern "C" fn(*mut c_void, *const c_char, *mut *mut c_void) -> c_int,
    finalize_stmt: unsafe extern "C" fn(*mut c_void) -> c_int,
    reset: unsafe extern "C" fn(*mut c_void) -> c_int,
    clear_bindings: unsafe extern "C" fn(*mut c_void) -> c_int,
    step: unsafe extern "C" fn(*mut c_void) -> c_int,
    bind_int: unsafe extern "C" fn(*mut c_void, c_int, c_int) -> c_int,
    bind_int64: unsafe extern "C" fn(*mut c_void, c_int, i64) -> c_int,
    bind_double: unsafe extern "C" fn(*mut c_void, c_int, f64) -> c_int,
    bind_text: unsafe extern "C" fn(*mut c_void, c_int, *const c_char) -> c_int,
    bind_blob: unsafe extern "C" fn(*mut c_void, c_int, *const c_void, c_int) -> c_int,
    bind_null: unsafe extern "C" fn(*mut c_void, c_int) -> c_int,
    bind_value: unsafe extern "C" fn(*mut c_void, c_int, *const c_void) -> c_int,
}

/// SQLite return codes (subset).
const SQLITE_OK: c_int = 0;
const SQLITE_DONE: c_int = 101;

/// Java parity: per-segment CDC log writer that mirrors
/// `com.synclite.consolidator.log.CDCLogSegment.CDCLogSegmentWriter`.
///
/// Owns a dedicated native sqlite handle for the cdclog file and a long-lived
/// prepared INSERT statement so DML rows can be persisted by binding raw
/// `sqlite3_value*` pointers received from the preupdate callback (`bindNativeValue`
/// in Java) instead of materializing values in Rust.
struct NativeCdcLogWriter {
    api: NativeStmtApi,
    db: *mut c_void,
    insert_stmt: *mut c_void,
    insert_schema_stmt: *mut c_void,
    arg_capacity: usize,
    in_txn: bool,
    cdclog_path: PathBuf,
}

// SAFETY: writer owns raw native pointers but is only ever used from a single
// thread at a time (the replay thread; callback fires synchronously inside it).
// Synchronization is enforced externally by wrapping the writer in a Mutex.
unsafe impl Send for NativeCdcLogWriter {}

impl NativeCdcLogWriter {
    const INITIAL_ARG_CAPACITY: usize = 16;
    const METADATA_BIND_COUNT: usize = 8;

    fn new(api: NativeStmtApi, cdclog_path: &Path) -> Result<Self> {
        if cdclog_path.exists() {
            std::fs::remove_file(cdclog_path).map_err(|e| {
                Error::Internal(format!(
                    "consolidator: failed to remove stale cdclog {}: {e}",
                    cdclog_path.display()
                ))
            })?;
        }

        let path_c = CString::new(cdclog_path.to_string_lossy().to_string())
            .map_err(|e| Error::Internal(format!("cdclog path contains NUL: {e}")))?;
        let mut db: *mut c_void = std::ptr::null_mut();
        // SAFETY: open writes the new handle into `db`.
        let rc = unsafe { (api.open)(path_c.as_ptr(), &mut db as *mut *mut c_void) };
        if rc != SQLITE_OK || db.is_null() {
            return Err(Error::Internal(format!(
                "synclite_rs_open(cdclog={}) failed rc={rc}",
                cdclog_path.display()
            )));
        }

        let mut writer = Self {
            api,
            db,
            insert_stmt: std::ptr::null_mut(),
            insert_schema_stmt: std::ptr::null_mut(),
            arg_capacity: 0,
            in_txn: false,
            cdclog_path: cdclog_path.to_path_buf(),
        };

        // Create cdclog tables/indexes (mirrors CDCLogSegment.open()).
        writer.exec_sql(
            "CREATE TABLE IF NOT EXISTS cdclog(\
             commit_id LONG, database_name TEXT, schema_name TEXT, table_name TEXT, \
             op_type TEXT, sql TEXT, change_number INTEGER PRIMARY KEY, arg_cnt INTEGER\
             )",
        )?;
        writer.exec_sql(
            "CREATE TABLE IF NOT EXISTS cdclog_schemas(\
             change_number INTEGER, database_name TEXT, schema_name TEXT, table_name TEXT, \
             column_index LONG, column_name TEXT, column_type TEXT, column_not_null INTEGER, \
             column_default_value BLOB, column_primary_key INTEGER, column_auto_increment INTEGER, \
             old_table_name TEXT, old_column_name TEXT\
             )",
        )?;
        writer.exec_sql("CREATE TABLE IF NOT EXISTS metadata(key TEXT, value TEXT)")?;
        writer.exec_sql("CREATE INDEX IF NOT EXISTS commit_id_cdclog_index ON cdclog(commit_id)")?;
        writer.exec_sql(
            "CREATE INDEX IF NOT EXISTS change_number_cdclog_schemas_index \
             ON cdclog_schemas(change_number)",
        )?;

        // Initial arg columns (grown on demand, matching Java addNewInlinedArgCols).
        writer.add_arg_columns(0, Self::INITIAL_ARG_CAPACITY)?;
        writer.arg_capacity = Self::INITIAL_ARG_CAPACITY;
        writer.reprepare_insert_stmt()?;
        writer.prepare_schema_stmt()?;

        Ok(writer)
    }

    fn exec_sql(&self, sql: &str) -> Result<()> {
        let c = CString::new(sql).map_err(|e| Error::Internal(format!("cdclog sql NUL: {e}")))?;
        // SAFETY: db handle valid for writer lifetime.
        let rc = unsafe { (self.api.exec)(self.db, c.as_ptr()) };
        if rc != SQLITE_OK {
            return Err(Error::Internal(format!(
                "cdclog exec failed rc={rc} sql={sql}: {}",
                self.errmsg()
            )));
        }
        Ok(())
    }

    fn errmsg(&self) -> String {
        // SAFETY: errmsg returns null or NUL-terminated C string valid briefly.
        let p = unsafe { (self.api.errmsg)(self.db) };
        if p.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
        }
    }

    fn add_arg_columns(&self, start_inclusive: usize, end_exclusive: usize) -> Result<()> {
        for i in (start_inclusive + 1)..=end_exclusive {
            let sql = format!("ALTER TABLE cdclog ADD COLUMN arg{i} BLOB");
            // Tolerate duplicates if a partial earlier run added the column.
            let c = CString::new(sql.clone()).map_err(|e| Error::Internal(format!("ALTER NUL: {e}")))?;
            let rc = unsafe { (self.api.exec)(self.db, c.as_ptr()) };
            if rc != SQLITE_OK {
                let msg = self.errmsg().to_ascii_lowercase();
                if !(msg.contains("duplicate column") || msg.contains("already exists")) {
                    return Err(Error::Internal(format!(
                        "cdclog ALTER add column rc={rc} sql={sql}: {}",
                        self.errmsg()
                    )));
                }
            }
        }
        Ok(())
    }

    fn reprepare_insert_stmt(&mut self) -> Result<()> {
        if !self.insert_stmt.is_null() {
            // SAFETY: finalize on a previously prepared stmt.
            let _ = unsafe { (self.api.finalize_stmt)(self.insert_stmt) };
            self.insert_stmt = std::ptr::null_mut();
        }

        let mut cols = String::from(
            "commit_id, database_name, schema_name, table_name, op_type, sql, change_number, arg_cnt",
        );
        for i in 1..=self.arg_capacity {
            cols.push_str(", arg");
            cols.push_str(&i.to_string());
        }
        let total = Self::METADATA_BIND_COUNT + self.arg_capacity;
        let placeholders = (1..=total)
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("INSERT INTO cdclog({cols}) VALUES ({placeholders})");
        let sql_c = CString::new(sql.clone()).map_err(|e| Error::Internal(format!("prepare NUL: {e}")))?;

        let mut stmt: *mut c_void = std::ptr::null_mut();
        // SAFETY: prepare writes new statement pointer.
        let rc = unsafe { (self.api.prepare)(self.db, sql_c.as_ptr(), &mut stmt as *mut *mut c_void) };
        if rc != SQLITE_OK || stmt.is_null() {
            return Err(Error::Internal(format!(
                "cdclog prepare insert rc={rc}: {}",
                self.errmsg()
            )));
        }
        self.insert_stmt = stmt;
        Ok(())
    }

    fn prepare_schema_stmt(&mut self) -> Result<()> {
        if !self.insert_schema_stmt.is_null() {
            let _ = unsafe { (self.api.finalize_stmt)(self.insert_schema_stmt) };
            self.insert_schema_stmt = std::ptr::null_mut();
        }
        let sql = "INSERT INTO cdclog_schemas(\
                   change_number, database_name, schema_name, table_name, \
                   column_index, column_name, column_type, column_not_null, \
                   column_default_value, column_primary_key, column_auto_increment, \
                   old_table_name, old_column_name\
                   ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)";
        let c = CString::new(sql).map_err(|e| Error::Internal(format!("prepare NUL: {e}")))?;
        let mut stmt: *mut c_void = std::ptr::null_mut();
        // SAFETY: writes statement pointer.
        let rc = unsafe { (self.api.prepare)(self.db, c.as_ptr(), &mut stmt as *mut *mut c_void) };
        if rc != SQLITE_OK || stmt.is_null() {
            return Err(Error::Internal(format!(
                "cdclog prepare schemas insert rc={rc}: {}",
                self.errmsg()
            )));
        }
        self.insert_schema_stmt = stmt;
        Ok(())
    }

    fn ensure_arg_capacity(&mut self, needed: usize) -> Result<()> {
        if needed <= self.arg_capacity {
            return Ok(());
        }
        // Grow geometrically, matching Java behavior of expanding once instead
        // of one column at a time when a wide row arrives.
        let mut new_cap = self.arg_capacity.max(1);
        while new_cap < needed {
            new_cap *= 2;
        }
        self.add_arg_columns(self.arg_capacity, new_cap)?;
        self.arg_capacity = new_cap;
        self.reprepare_insert_stmt()
    }

    fn begin_tran(&mut self) -> Result<()> {
        if !self.in_txn {
            let r = self.exec_sql("BEGIN TRANSACTION");
            if std::env::var("SYNCLITE_DEBUG_CDC_CALLBACK").ok().as_deref() == Some("1") {
                eprintln!("[cdclog-writer] BEGIN ({}) -> {:?}", self.cdclog_path.display(), r.as_ref().map(|_| "ok"));
            }
            r?;
            self.in_txn = true;
        }
        Ok(())
    }

    fn commit_tran(&mut self) -> Result<()> {
        if self.in_txn {
            let r = self.exec_sql("COMMIT");
            if std::env::var("SYNCLITE_DEBUG_CDC_CALLBACK").ok().as_deref() == Some("1") {
                eprintln!("[cdclog-writer] COMMIT ({}) -> {:?}", self.cdclog_path.display(), r.as_ref().map(|_| "ok"));
            }
            r?;
            self.in_txn = false;
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn rollback_tran(&mut self) -> Result<()> {
        if self.in_txn {
            self.exec_sql("ROLLBACK")?;
            self.in_txn = false;
        }
        Ok(())
    }

    /// Bind the fixed metadata columns 1..8 of the cdclog INSERT statement.
    /// Mirrors the first block of `CDCLogSegment.writeCDCLog`.
    fn bind_metadata(
        &self,
        commit_id: i64,
        database: Option<&str>,
        schema: Option<&str>,
        table: Option<&str>,
        op: &str,
        sql_text: Option<&str>,
        change_number: i64,
        arg_cnt: i64,
    ) -> Result<()> {
        // SAFETY: bind functions are safe to call with valid stmt; CString lifetimes guarded by holders.
        let stmt = self.insert_stmt;
        let api = &self.api;

        let _ = unsafe { (api.reset)(stmt) };
        let _ = unsafe { (api.clear_bindings)(stmt) };

        unsafe { (api.bind_int64)(stmt, 1, commit_id) };
        bind_text_or_null(api, stmt, 2, database)?;
        bind_text_or_null(api, stmt, 3, schema)?;
        bind_text_or_null(api, stmt, 4, table)?;
        bind_text_or_null(api, stmt, 5, Some(op))?;
        bind_text_or_null(api, stmt, 6, sql_text)?;
        unsafe { (api.bind_int64)(stmt, 7, change_number) };
        unsafe { (api.bind_int64)(stmt, 8, arg_cnt) };

        Ok(())
    }

    fn step_insert(&self) -> Result<()> {
        let stmt = self.insert_stmt;
        // SAFETY: stmt is owned by writer.
        let rc = unsafe { (self.api.step)(stmt) };
        if std::env::var("SYNCLITE_DEBUG_CDC_CALLBACK").ok().as_deref() == Some("1") {
            eprintln!("[cdclog-writer] step_insert rc={rc} ({})", self.cdclog_path.display());
        }
        if rc != SQLITE_DONE && rc != SQLITE_OK {
            return Err(Error::Internal(format!(
                "cdclog step insert rc={rc}: {}",
                self.errmsg()
            )));
        }
        Ok(())
    }

    /// Java parity: write a DDL CDC log record (no values), then its column
    /// schemas, mirroring `CDCLogger.logDDLRecord` + `writeCDCLog`.
    fn write_ddl_record(
        &mut self,
        commit_id: i64,
        database: Option<&str>,
        schema: Option<&str>,
        table: Option<&str>,
        op: &str,
        sql_text: Option<&str>,
        change_number: i64,
        column_schemas: &[CdcLogSchemaCol<'_>],
    ) -> Result<()> {
        self.begin_tran()?;
        self.bind_metadata(commit_id, database, schema, table, op, sql_text, change_number, 0)?;
        // All arg slots NULL for DDL.
        for i in 1..=self.arg_capacity {
            // SAFETY: bind_null with valid index.
            let _ = unsafe { (self.api.bind_null)(self.insert_stmt, (Self::METADATA_BIND_COUNT + i) as c_int) };
        }
        self.step_insert()?;

        for col in column_schemas {
            self.write_schema_row(change_number, database, schema, table, col)?;
        }
        Ok(())
    }

    fn write_schema_row(
        &self,
        change_number: i64,
        database: Option<&str>,
        schema: Option<&str>,
        table: Option<&str>,
        col: &CdcLogSchemaCol<'_>,
    ) -> Result<()> {
        let stmt = self.insert_schema_stmt;
        let api = &self.api;
        let _ = unsafe { (api.reset)(stmt) };
        let _ = unsafe { (api.clear_bindings)(stmt) };

        unsafe { (api.bind_int64)(stmt, 1, change_number) };
        bind_text_or_null(api, stmt, 2, database)?;
        bind_text_or_null(api, stmt, 3, schema)?;
        bind_text_or_null(api, stmt, 4, table)?;
        unsafe { (api.bind_int64)(stmt, 5, col.column_index) };
        bind_text_or_null(api, stmt, 6, Some(col.column_name))?;
        bind_text_or_null(api, stmt, 7, Some(col.column_type))?;
        unsafe { (api.bind_int)(stmt, 8, if col.not_null { 1 } else { 0 }) };
        // column_default_value (BLOB) -> NULL.
        let _ = unsafe { (api.bind_null)(stmt, 9) };
        unsafe { (api.bind_int64)(stmt, 10, col.primary_key_pos) };
        unsafe { (api.bind_int)(stmt, 11, if col.auto_increment { 1 } else { 0 }) };
        bind_text_or_null(api, stmt, 12, col.old_table_name)?;
        bind_text_or_null(api, stmt, 13, col.old_column_name)?;

        let rc = unsafe { (api.step)(stmt) };
        if rc != SQLITE_DONE && rc != SQLITE_OK {
            return Err(Error::Internal(format!(
                "cdclog step schemas insert rc={rc}: {}",
                self.errmsg()
            )));
        }
        Ok(())
    }

    /// Java parity: write a DML CDC log record using raw `sqlite3_value*`
    /// pointers from the preupdate callback (`bindNativeValue`).
    ///
    /// `before_ptrs` and `after_ptrs` are arrays of `sqlite3_value*` valid for
    /// the duration of the change-callback invocation. The binding order
    /// matches `CDCLogSegment.writeCDCLog`:
    ///   INSERT -> after values
    ///   UPDATE -> before values then after values
    ///   DELETE -> before values
    fn write_dml_record(
        &mut self,
        commit_id: i64,
        database: Option<&str>,
        schema: Option<&str>,
        table: Option<&str>,
        op: &str,
        sql_text: Option<&str>,
        change_number: i64,
        before_ptrs: &[*const c_void],
        after_ptrs: &[*const c_void],
    ) -> Result<()> {
        let op_upper = op.to_ascii_uppercase();
        let arg_cnt = match op_upper.as_str() {
            "INSERT" => after_ptrs.len(),
            "DELETE" => before_ptrs.len(),
            "UPDATE" => before_ptrs.len() + after_ptrs.len(),
            _ => 0,
        };
        self.ensure_arg_capacity(arg_cnt)?;
        self.begin_tran()?;
        self.bind_metadata(
            commit_id,
            database,
            schema,
            table,
            &op_upper,
            sql_text,
            change_number,
            arg_cnt as i64,
        )?;

        // Bind values via pointer pass-through (Java's bindNativeValue path).
        let stmt = self.insert_stmt;
        let api = &self.api;
        let mut idx: c_int = Self::METADATA_BIND_COUNT as c_int + 1;
        let bind_ptr = |idx: c_int, v: *const c_void| -> Result<()> {
            // SAFETY: v is a sqlite3_value* duped by the native preupdate hook
            // and remains live until the change callback returns; binding it
            // here is the exact behavior of Java's bindNativeValue.
            let rc = if v.is_null() {
                unsafe { (api.bind_null)(stmt, idx) }
            } else {
                unsafe { (api.bind_value)(stmt, idx, v) }
            };
            if rc != SQLITE_OK {
                return Err(Error::Internal(format!(
                    "cdclog bind_value idx={idx} rc={rc}: {}",
                    {
                        // SAFETY: errmsg call.
                        let p = unsafe { (api.errmsg)(self.db) };
                        if p.is_null() {
                            String::new()
                        } else {
                            unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
                        }
                    }
                )));
            }
            Ok(())
        };

        match op_upper.as_str() {
            "INSERT" => {
                for &p in after_ptrs {
                    bind_ptr(idx, p)?;
                    idx += 1;
                }
            }
            "UPDATE" => {
                for &p in before_ptrs {
                    bind_ptr(idx, p)?;
                    idx += 1;
                }
                for &p in after_ptrs {
                    bind_ptr(idx, p)?;
                    idx += 1;
                }
            }
            "DELETE" => {
                for &p in before_ptrs {
                    bind_ptr(idx, p)?;
                    idx += 1;
                }
            }
            _ => {}
        }

        // NULL any remaining arg slots so partial rebinds from prior rows do
        // not leak into this row.
        while (idx as usize) <= Self::METADATA_BIND_COUNT + self.arg_capacity {
            let _ = unsafe { (api.bind_null)(stmt, idx) };
            idx += 1;
        }

        self.step_insert()
    }

    fn close(mut self) -> Result<()> {
        // Best-effort: commit any open txn before tearing down statements.
        let r = self.commit_tran();
        if std::env::var("SYNCLITE_DEBUG_CDC_CALLBACK").ok().as_deref() == Some("1") {
            eprintln!("[cdclog-writer] close commit -> {:?} ({})", r.as_ref().map(|_| "ok").map_err(|e| e.to_string()), self.cdclog_path.display());
        }
        let _ = r;
        if !self.insert_stmt.is_null() {
            // SAFETY: finalize prepared stmt.
            let _ = unsafe { (self.api.finalize_stmt)(self.insert_stmt) };
            self.insert_stmt = std::ptr::null_mut();
        }
        if !self.insert_schema_stmt.is_null() {
            let _ = unsafe { (self.api.finalize_stmt)(self.insert_schema_stmt) };
            self.insert_schema_stmt = std::ptr::null_mut();
        }
        if !self.db.is_null() {
            // SAFETY: close native db handle.
            let _ = unsafe { (self.api.close)(self.db) };
            self.db = std::ptr::null_mut();
        }
        let _ = &self.cdclog_path; // path kept for diagnostics.
        Ok(())
    }
}

impl Drop for NativeCdcLogWriter {
    fn drop(&mut self) {
        if !self.insert_stmt.is_null() {
            let _ = unsafe { (self.api.finalize_stmt)(self.insert_stmt) };
            self.insert_stmt = std::ptr::null_mut();
        }
        if !self.insert_schema_stmt.is_null() {
            let _ = unsafe { (self.api.finalize_stmt)(self.insert_schema_stmt) };
            self.insert_schema_stmt = std::ptr::null_mut();
        }
        if !self.db.is_null() {
            let _ = unsafe { (self.api.close)(self.db) };
            self.db = std::ptr::null_mut();
        }
    }
}

fn bind_text_or_null(
    api: &NativeStmtApi,
    stmt: *mut c_void,
    index: c_int,
    value: Option<&str>,
) -> Result<()> {
    match value {
        Some(s) => {
            let c = CString::new(s).map_err(|e| Error::Internal(format!("bind_text NUL: {e}")))?;
            // SAFETY: native bind copies the text (SQLITE_TRANSIENT in native impl).
            let rc = unsafe { (api.bind_text)(stmt, index, c.as_ptr()) };
            if rc != SQLITE_OK {
                return Err(Error::Internal(format!(
                    "cdclog bind_text idx={index} rc={rc}"
                )));
            }
            Ok(())
        }
        None => {
            let _ = unsafe { (api.bind_null)(stmt, index) };
            Ok(())
        }
    }
}

/// Java parity: column metadata row written into `cdclog_schemas` for each DDL
/// record. Mirrors fields used in `CDCLogSegment.writeCDCLog` schema block.
/// Borrowed view used by `NativeCdcLogWriter`; distinct from the owned
/// `CdcSchemaColumn` used by the destination apply pipeline.
struct CdcLogSchemaCol<'a> {
    column_index: i64,
    column_name: &'a str,
    column_type: &'a str,
    not_null: bool,
    primary_key_pos: i64,
    auto_increment: bool,
    old_table_name: Option<&'a str>,
    old_column_name: Option<&'a str>,
}

fn replay_txn_sidecar_path(segment_path: &Path, segment_seq: u64, commit_id: u64) -> PathBuf {
    let dir = segment_path.parent().unwrap_or_else(|| Path::new("."));
    dir.join(format!("{segment_seq}.sqllog.{commit_id}.txn"))
}

fn candidate_native_library_names() -> &'static [&'static str] {
    #[cfg(target_os = "windows")]
    {
        &["synclitecdc_x86_64.dll", "synclitecdc_x86.dll", "synclitecdc_x86_64", "synclitecdc_x86"]
    }
    #[cfg(target_os = "linux")]
    {
        &["libsynclitecdc_x86_64.so", "libsynclitecdc_x86.so", "synclitecdc_x86_64", "synclitecdc_x86"]
    }
    #[cfg(target_os = "macos")]
    {
        &["libsynclitecdc_x86_64.dylib", "libsynclitecdc_x86.dylib", "synclitecdc_x86_64", "synclitecdc_x86"]
    }
}

fn candidate_native_library_paths() -> Vec<PathBuf> {
    let names = candidate_native_library_names();
    let mut candidates = Vec::new();

    if let Ok(dir) = env::var("SYNCLITE_CDC_LIB_DIR") {
        let base = PathBuf::from(dir);
        for name in names {
            candidates.push(base.join(name));
        }
    }

    // Prefer Rust-workspace-local copied binaries when running from workspace roots.
    for relative in ["native", "synclite-logger-rust/native", "../native", "../synclite-logger-rust/native"] {
        let base = PathBuf::from(relative);
        for name in names {
            candidates.push(base.join(name));
        }
    }

    // Resolve from this crate location at compile-time.
    // CARGO_MANIFEST_DIR => .../synclite-logger-rust/crates/consolidator/runtime
    let manifest_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if let Some(workspace_root) = manifest_root.parent().and_then(|p| p.parent()).and_then(|p| p.parent()) {
        let base = workspace_root.join("native");
        for name in names {
            candidates.push(base.join(name));
        }
    }

    // Resolve relative to the test/binary executable path.
    if let Ok(exe) = env::current_exe() {
        if let Some(exe_dir) = exe.parent() {
            for relative in ["native", "../../native", "../../../native", "../../../../native"] {
                let base = exe_dir.join(relative);
                for name in names {
                    candidates.push(base.join(name));
                }
            }
        }
    }

    for name in names {
        candidates.push(PathBuf::from(name));
    }

    candidates
}

fn mirror_stage_artifact_to_work(layout: &ConsolidatorLayout, stage_path: &Path) -> Result<PathBuf> {
    let file_name = stage_path
        .file_name()
        .ok_or_else(|| Error::Config(format!("consolidator: invalid staged path {}", stage_path.display())))?;

    let stage_container = stage_path
        .parent()
        .and_then(|p| p.file_name())
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| format!("synclite-{}-{}", layout.device_name, layout.device_id));

    let work_device_dir = layout.work_dir.join(stage_container);
    std::fs::create_dir_all(&work_device_dir)?;

    let work_path = work_device_dir.join(file_name);
    // If the stage file has already been removed by the shipper cleaner
    // but a prior mirror produced the work copy, accept the work copy as
    // authoritative. This handles the pause/resume race where the shipper
    // wins the cleanup before the consolidator drains a paused segment.
    if !stage_path.exists() && work_path.exists() {
        return Ok(work_path);
    }
    // Idempotency: if the work copy already exists and has been marked
    // APPLIED, do NOT copy from stage. Re-copying would clobber the
    // APPLIED marker (which lives in the work copy's `metadata` table)
    // and cause the consolidator to replay the segment. This handles
    // the multi-notification path: mover notifies once, the shipper's
    // `on_shipped` callback notifies again after upload, and on restart
    // `catch_up_stage_dir` notifies any leftover stage files.
    if work_path.exists() && segment_metadata_status_is_applied(&work_path) {
        return Ok(work_path);
    }
    std::fs::copy(stage_path, &work_path)?;

    // Java parity: if a sqllog segment is mirrored, mirror all matching
    // sqllog transaction sidecars (`<seq>.sqllog.*.txn`) as well.
    if is_sqllog_path(stage_path) {
        mirror_sqllog_txn_sidecars(stage_path, &work_device_dir)?;
    }

    Ok(work_path)
}

fn mirror_sqllog_txn_sidecars(stage_sqllog_path: &Path, work_device_dir: &Path) -> Result<()> {
    let Some(seq) = parse_segment_seq(stage_sqllog_path) else {
        return Ok(());
    };
    let Some(stage_dir) = stage_sqllog_path.parent() else {
        return Ok(());
    };

    let prefix = format!("{seq}.sqllog.");
    let suffix = ".txn";

    if let Ok(entries) = std::fs::read_dir(stage_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.starts_with(&prefix) || !name.ends_with(suffix) {
                continue;
            }

            let dst = work_device_dir.join(name);
            std::fs::copy(&path, &dst)?;
        }
    }

    Ok(())
}

fn remove_sqllog_txn_sidecars_for_seq(dir: &Path, seq: u64) -> std::io::Result<()> {
    let prefix = format!("{seq}.sqllog.");
    let suffix = ".txn";

    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.starts_with(&prefix) || !name.ends_with(suffix) {
                continue;
            }
            remove_file_with_retry(&path)?;
        }
    }

    Ok(())
}

fn cleanup_processed_sqllog(
    layout: &ConsolidatorLayout,
    state_conn: &Connection,
    stage_path: &Path,
    work_path: &Path,
) -> Result<()> {
    if !is_sqllog_path(stage_path) {
        return Ok(());
    }

    let Some(applied_seq) = parse_segment_seq(work_path).or_else(|| parse_segment_seq(stage_path)) else {
        return Ok(());
    };

    if applied_seq == 0 {
        return Ok(());
    }

    // Java parity: DeviceLogCleaner deletes work artifacts (cdclog + cmdlog
    // + work-side sidecars) up to `appliedLogSegmentSeqNum - 1` for THIS
    // destination. Stage (upload) artifacts are deleted up to
    // `min(last_consolidated_cdc_log_segment_seq_num)` across ALL configured
    // destinations (Device.getLastConsolidatedLogSegmentSequenceNumber) so
    // that a slower destination can still resume from stage.
    let work_target_seq = (applied_seq as i64) - 1;
    cleanup_work_artifacts_up_to(layout, state_conn, stage_path, work_path, work_target_seq)?;

    if layout.cleanup_stage_files {
        let stage_target_seq = compute_stage_cleanup_target(layout)?;
        if stage_target_seq >= 0 {
            cleanup_stage_artifacts_up_to(layout, state_conn, stage_path, stage_target_seq)?;
        }
    }
    Ok(())
}

/// Java parity: `Device.getLastConsolidatedLogSegmentSequenceNumber` — the
/// MIN of `last_consolidated_cdc_log_segment_seq_num` across every
/// configured destination. Returns `min - 1` (the highest seq safe to delete
/// from the shared stage), or -1 when no destination has applied anything
/// yet.
fn compute_stage_cleanup_target(layout: &ConsolidatorLayout) -> Result<i64> {
    let mut min_seq: i64 = i64::MAX;
    for dst in &layout.all_dst_indexes {
        let v = consolidator_state::get_consolidator_property_long(
            layout,
            *dst,
            "last_consolidated_cdc_log_segment_seq_num",
        )?
        .unwrap_or(-1);
        if v < min_seq {
            min_seq = v;
        }
    }
    if min_seq == i64::MAX || min_seq <= 0 {
        return Ok(-1);
    }
    Ok(min_seq - 1)
}

fn cleanup_work_artifacts_up_to(
    layout: &ConsolidatorLayout,
    state_conn: &Connection,
    stage_path: &Path,
    work_path: &Path,
    target_seq: i64,
) -> Result<()> {
    // Java parity (DeviceLogCleaner.cleanedUpto): watermark lives in the
    // per-device metadata DB (sibling of state_db_path). Read is
    // best-effort — a miss falls back to -1 (harmless re-walk of
    // already-deleted segments).
    let mut next_contiguous = read_device_metadata_i64(layout, LAST_CLEANED_SEGMENT_KEY, -1);
    if target_seq <= next_contiguous {
        return Ok(());
    }
    // `state_conn` is retained in the signature for now: callers still
    // pass it for stage cleanup bookkeeping under a different key.
    let _ = state_conn;
    for seq in (next_contiguous + 1)..=target_seq {
        let seq_work_path = staged_segment_path_for_seq(work_path, seq as u64);
        let seq_cdclog_path = layout.device_work_dir.join(format!("{seq}.cdclog"));
        let seq_work_dir = seq_work_path.parent().map(Path::to_path_buf);

        let mut cleaned = true;
        // Java parity: DeviceLogCleaner deletes the cdclog segment first,
        // then command (sqllog) segment + sidecars. Missing file is OK
        // (segment may have had zero entries → no cdclog written).
        if seq_cdclog_path.exists() {
            if let Err(e) = remove_file_with_retry(&seq_cdclog_path) {
                tracing::warn!(
                    error = %e,
                    sequence = seq,
                    path = %seq_cdclog_path.display(),
                    "failed to clean cdclog, will retry in next cleanup cycle"
                );
                cleaned = false;
            }
        }
        if let Err(e) = remove_file_with_retry(&seq_work_path) {
            tracing::warn!(
                error = %e,
                sequence = seq,
                path = %seq_work_path.display(),
                "failed to clean work sqllog, will retry in next cleanup cycle"
            );
            cleaned = false;
        }
        if let Some(work_dir) = &seq_work_dir {
            if let Err(e) = remove_sqllog_txn_sidecars_for_seq(work_dir, seq as u64) {
                tracing::warn!(
                    error = %e,
                    sequence = seq,
                    path = %work_dir.display(),
                    "failed to clean work sqllog txn sidecars, will retry in next cleanup cycle"
                );
                cleaned = false;
            }
        }

        if cleaned && seq == (next_contiguous + 1) {
            next_contiguous = seq;
        }
        if cleaned {
            with_device_tracer(|t| {
                tracer_info!(
                    t,
                    "DeviceConsolidator",
                    "Cleaned processed segment : seqNum={} cdclogPath={} workPath={}",
                    seq,
                    seq_cdclog_path.display(),
                    seq_work_path.display()
                )
            });
        }
    }
    // Persist is best-effort too. Failure leaves the in-memory state
    // advanced (caller will skip the cleaned range for the rest of this
    // process); a restart simply re-walks already-deleted files.
    write_device_metadata_i64_best_effort(layout, LAST_CLEANED_SEGMENT_KEY, next_contiguous);
    // Silence the `stage_path` unused-variable lint for future caller code
    // paths that may pre-compute and pass a distinct stage location.
    let _ = stage_path;
    Ok(())
}

fn cleanup_stage_artifacts_up_to(
    layout: &ConsolidatorLayout,
    state_conn: &Connection,
    stage_path: &Path,
    target_seq: i64,
) -> Result<()> {
    let mut next_contiguous = get_state_i64(state_conn, LAST_STAGE_CLEANED_SEGMENT_KEY, -1)?;
    if target_seq <= next_contiguous {
        return Ok(());
    }
    for seq in (next_contiguous + 1)..=target_seq {
        let seq_stage_path = staged_segment_path_for_seq(stage_path, seq as u64);
        let seq_stage_dir = seq_stage_path.parent().map(Path::to_path_buf);

        let mut cleaned = true;
        let mut existed = false;
        if seq_stage_path.exists() {
            existed = true;
            if let Err(e) = remove_file_with_retry(&seq_stage_path) {
                tracing::warn!(
                    error = %e,
                    sequence = seq,
                    path = %seq_stage_path.display(),
                    "failed to clean stage sqllog, will retry in next cleanup cycle"
                );
                cleaned = false;
            }
        }
        if let Some(stage_dir) = &seq_stage_dir {
            if let Err(e) = remove_sqllog_txn_sidecars_for_seq(stage_dir, seq as u64) {
                tracing::warn!(
                    error = %e,
                    sequence = seq,
                    path = %stage_dir.display(),
                    "failed to clean stage sqllog txn sidecars, will retry in next cleanup cycle"
                );
                cleaned = false;
            }
        }

        if cleaned && seq == (next_contiguous + 1) {
            next_contiguous = seq;
        }
        if cleaned && existed {
            let _ = layout;
            with_device_tracer(|t| {
                tracer_info!(
                    t,
                    "DeviceConsolidator",
                    "Cleaned stage segment : seqNum={} stagePath={}",
                    seq,
                    seq_stage_path.display()
                )
            });
        }
    }
    put_state_i64(state_conn, LAST_STAGE_CLEANED_SEGMENT_KEY, next_contiguous)?;
    Ok(())
}

fn staged_segment_path_for_seq(reference_segment_path: &Path, seq: u64) -> PathBuf {
    let parent = reference_segment_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!("{seq}.sqllog"))
}

fn is_sqllog_path(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("sqllog"))
        .unwrap_or(false)
}

fn is_sqllog_txn_sidecar_path(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    name.ends_with(".txn") && name.contains(".sqllog.")
}

/// True only for files the consolidator's apply pipeline knows how to
/// process: `<seq>.sqllog` (txn devices) and `<seq>.cdclog` (post-
/// replicator CDC files). Everything else in a device's stage subdir
/// (`<db>.synclite.backup`, `<db>.synclite.metadata`, txn sidecars,
/// trace/lock files, …) must be skipped during catch-up — the
/// device-level snapshot/metadata flow through a different code path
/// (`Msg::Backup` / device metadata loader), not `StagePathReady`.
fn is_apply_candidate_segment(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
        return false;
    };
    ext.eq_ignore_ascii_case("sqllog") || ext.eq_ignore_ascii_case("cdclog")
}

fn last_txn_fate_decided(segment_path: &Path) -> Result<bool> {
    let conn = Connection::open(segment_path).map_err(map_sql_err)?;
    let latest_commit: Option<i64> = conn
        .query_row("SELECT MAX(commit_id) FROM commandlog", [], |row| row.get(0))
        .map_err(map_sql_err)?;
    let Some(commit_id) = latest_commit else {
        return Ok(true);
    };
    let fate_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM commandlog WHERE commit_id = ?1 AND (sql = 'COMMIT' OR sql = 'ROLLBACK')",
            [commit_id],
            |row| row.get(0),
        )
        .map_err(map_sql_err)?;
    if fate_count > 0 {
        return Ok(true);
    }

    let replay_txn_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM commandlog WHERE commit_id = ?1 AND sql = 'REPLAY_TXN'",
            [commit_id],
            |row| row.get(0),
        )
        .map_err(map_sql_err)?;
    if replay_txn_count == 0 {
        return Ok(false);
    }

    let Some(segment_seq) = parse_segment_seq(segment_path) else {
        return Ok(false);
    };
    Ok(replay_txn_sidecar_path(segment_path, segment_seq as u64, commit_id as u64).exists())
}

fn remove_file_with_retry(path: &Path) -> std::io::Result<()> {
    let mut last_err: Option<std::io::Error> = None;
    for _ in 0..20 {
        match std::fs::remove_file(path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e)
                if e.kind() == std::io::ErrorKind::PermissionDenied
                    || e.kind() == std::io::ErrorKind::WouldBlock =>
            {
                last_err = Some(e);
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap_or_else(|| std::io::Error::other("remove_file_with_retry: unknown failure")))
}

#[derive(Debug, Clone)]
struct SegmentEntry {
    change_number: i64,
    commit_id: i64,
    source: SegmentSource,
    table_name: Option<String>,
    op_type: Option<String>,
    ddl_columns: Vec<CdcSchemaColumn>,
    sql: String,
    args: Vec<SqlValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentSource {
    CommandLog,
    CdcLog,
}

#[derive(Debug, Clone)]
struct CdcSchemaColumn {
    column_index: i64,
    column_name: String,
    column_type: Option<String>,
    /// Java parity: `cdclog_schemas.column_primary_key`. 0 = not a PK
    /// column; >= 1 = PK position (1-based). Used by
    /// `dst-idempotent-data-ingestion-method` to rewrite INSERT into
    /// UPSERT / REPLACE / DELETE+INSERT, mirroring
    /// `TableMapper.getMappedInsertOper`.
    column_primary_key: i64,
    old_table_name: Option<String>,
    old_column_name: Option<String>,
}

fn read_segment_entries(seg_conn: &Connection, layout: &ConsolidatorLayout) -> Result<(i64, Vec<SegmentEntry>)> {
    if has_table(seg_conn, "commandlog")? {
        return read_commandlog_entries(seg_conn, layout);
    }
    if has_table(seg_conn, "cdclog")? {
        return read_cdclog_entries(seg_conn);
    }
    Err(Error::Config(
        "consolidator: segment has neither commandlog nor cdclog table".to_string(),
    ))
}

fn read_segment_entries_with_replica(
    seg_conn: &Connection,
    schema_replica: &mut Connection,
) -> Result<(i64, Vec<SegmentEntry>)> {
    if has_table(seg_conn, "commandlog")? {
        return read_commandlog_entries_with_replica(seg_conn, schema_replica);
    }
    if has_table(seg_conn, "cdclog")? {
        return read_cdclog_entries(seg_conn);
    }
    Err(Error::Config(
        "consolidator: segment has neither commandlog nor cdclog table".to_string(),
    ))
}

fn apply_resume_filter(
    entries: &mut Vec<SegmentEntry>,
    segment_path: &Path,
    layout: &ConsolidatorLayout,
) -> Result<()> {
    if layout.metadata_store != MetadataStore::Destination {
        return Ok(());
    }
    let Some(segment_seq) = parse_segment_seq(segment_path).map(|v| v as i64) else {
        return Ok(());
    };
    let Some(resume) = read_destination_resume_checkpoint(layout)? else {
        return Ok(());
    };
    if resume.cdc_log_segment_sequence_number != segment_seq {
        return Ok(());
    }
    if resume.cdc_change_number < 0 {
        return Ok(());
    }
    let resume_change = resume.cdc_change_number;
    entries.retain(|entry| entry.change_number > resume_change);
    Ok(())
}

fn read_segment_entries_with_resume_and_replica(
    seg_conn: &Connection,
    segment_path: &Path,
    layout: &ConsolidatorLayout,
    schema_replica: &mut Connection,
) -> Result<(i64, Vec<SegmentEntry>)> {
    let (first_commit_id, mut entries) = read_segment_entries_with_replica(seg_conn, schema_replica)?;
    apply_resume_filter(&mut entries, segment_path, layout)?;
    Ok((first_commit_id, entries))
}

fn read_segment_entries_with_resume(
    seg_conn: &Connection,
    segment_path: &Path,
    layout: &ConsolidatorLayout,
) -> Result<(i64, Vec<SegmentEntry>)> {
    let (first_commit_id, mut entries) = read_segment_entries(seg_conn, layout)?;
    apply_resume_filter(&mut entries, segment_path, layout)?;
    Ok((first_commit_id, entries))
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

fn read_commandlog_entries(seg_conn: &Connection, layout: &ConsolidatorLayout) -> Result<(i64, Vec<SegmentEntry>)> {
    let mut schema_replica = seed_commandlog_schema_replica(layout)?;
    read_commandlog_entries_with_replica(seg_conn, &mut schema_replica)
}

fn read_commandlog_entries_with_replica(
    seg_conn: &Connection,
    schema_replica: &mut Connection,
) -> Result<(i64, Vec<SegmentEntry>)> {
    let first_commit_id = seg_conn
        .query_row("SELECT MIN(commit_id) FROM commandlog", [], |row| {
            row.get::<_, Option<i64>>(0)
        })
        .map_err(map_sql_err)?
        .unwrap_or(0);

    let mut stmt = seg_conn
        .prepare("SELECT change_number, commit_id, sql, arg_cnt FROM commandlog ORDER BY change_number")
        .map_err(map_sql_err)?;
    let mut rows = stmt.query([]).map_err(map_sql_err)?;
    let mut last_sql: Option<String> = None;
    let mut entries = Vec::new();

    while let Some(row) = rows.next().map_err(map_sql_err)? {
        let change_number: i64 = row.get(0).map_err(map_sql_err)?;
        let commit_id: i64 = row.get(1).map_err(map_sql_err)?;
        let sql_opt: Option<String> = row.get(2).map_err(map_sql_err)?;
        let arg_cnt: i64 = row.get(3).map_err(map_sql_err)?;
        let sql = match sql_opt {
            Some(sql) => {
                last_sql = Some(sql.clone());
                sql
            }
            None => match &last_sql {
                Some(sql) => sql.clone(),
                None => continue,
            },
        };

        let args = if arg_cnt <= 0 {
            Vec::new()
        } else {
            let mut arg_stmt = seg_conn
                .prepare(&format!(
                    "SELECT {} FROM commandlog WHERE change_number=?1",
                    (1..=arg_cnt)
                        .map(|i| format!("arg{i}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .map_err(map_sql_err)?;
            arg_stmt
                .query_row([change_number], |arg_row| {
                    let mut vals = Vec::with_capacity(arg_cnt as usize);
                    for i in 0..arg_cnt as usize {
                        vals.push(arg_row.get::<_, SqlValue>(i)?);
                    }
                    Ok(vals)
                })
                .map_err(map_sql_err)?
        };

        // Java parity: DeviceEventStreamer keeps an in-memory schema replica.
        // Seed from persisted metadata, apply DDL into the replica, then
        // query PRAGMA table_info for column metadata.
        let op_type_str = map_cdclog_op_type(&sql);
        let op_type = if op_type_str == "NOOP" {
            None
        } else {
            Some(op_type_str.to_string())
        };
        let table_name = extract_table_name_for_operation(&sql);
        let ddl_columns = derive_commandlog_ddl_columns(
            schema_replica,
            op_type_str,
            table_name.as_deref(),
            &sql,
        )?;

        entries.push(SegmentEntry {
            change_number,
            commit_id,
            source: SegmentSource::CommandLog,
            table_name,
            op_type,
            ddl_columns,
            sql,
            args,
        });
    }

    Ok((first_commit_id, entries))
}

fn seed_commandlog_schema_replica(layout: &ConsolidatorLayout) -> Result<Connection> {
    let replica = Connection::open_in_memory().map_err(map_sql_err)?;
    let create_sql_rows = load_seed_create_sql_rows(layout)?;
    for (_table, create_sql) in create_sql_rows {
        if create_sql.trim().is_empty() {
            continue;
        }
        if let Err(e) = replica.execute_batch(&create_sql) {
            let msg = e.to_string().to_ascii_lowercase();
            if !msg.contains("already exists") && !msg.contains("more than one primary key") {
                return Err(map_sql_err(e));
            }
        }
    }
    Ok(replica)
}

fn load_seed_create_sql_rows(layout: &ConsolidatorLayout) -> Result<Vec<(String, String)>> {
    if layout.metadata_store == MetadataStore::Local {
        return load_local_seed_create_sql_rows(layout);
    }
    load_destination_seed_create_sql_rows(layout)
}

fn load_local_seed_create_sql_rows(layout: &ConsolidatorLayout) -> Result<Vec<(String, String)>> {
    let path = layout.consolidator_metadata_path(layout.dst_index);
    if !path.exists() {
        return Ok(Vec::new());
    }

    let conn = Connection::open(&path).map_err(map_sql_err)?;
    let mut stmt = conn
        .prepare(
            "SELECT table_name, value FROM table_metadata \
             WHERE key = 'create_sql' \
             ORDER BY database_name, table_name",
        )
        .map_err(map_sql_err)?;
    let rows = stmt
        .query_map([], |r| {
            let table: String = r.get(0)?;
            let sql: Option<String> = r.get(1)?;
            Ok((table, sql.unwrap_or_default()))
        })
        .map_err(map_sql_err)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(map_sql_err)?;
    Ok(rows)
}

fn load_destination_seed_create_sql_rows(layout: &ConsolidatorLayout) -> Result<Vec<(String, String)>> {
    let query = "SELECT table_name, prop_value FROM synclite_consolidator_table_metadata \
        WHERE synclite_device_id = ?1 AND synclite_device_name = ?2 AND prop_key = 'create_sql' \
        ORDER BY table_name";
    match layout.dst_type {
        DstType::Sqlite => {
            let conn = Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
            let mut stmt = conn.prepare(query).map_err(map_sql_err)?;
            let rows = stmt
                .query_map(params![layout.device_id, layout.device_name], |r| {
                    let table: String = r.get(0)?;
                    let sql: Option<String> = r.get(1)?;
                    Ok((table, sql.unwrap_or_default()))
                })
                .map_err(map_sql_err)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(map_sql_err)?;
            Ok(rows)
        }
        DstType::DuckDb => {
            let conn = DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
            let mut stmt = conn.prepare(query).map_err(map_duck_err)?;
            let mut rows = stmt
                .query(duck_params_from_iter([
                    DuckValue::Text(layout.device_id.clone()),
                    DuckValue::Text(layout.device_name.clone()),
                ].iter()))
                .map_err(map_duck_err)?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().map_err(map_duck_err)? {
                let table: String = row.get(0).map_err(map_duck_err)?;
                let sql: Option<String> = row.get(1).map_err(map_duck_err)?;
                out.push((table, sql.unwrap_or_default()));
            }
            Ok(out)
        }
        DstType::Postgres => {
            let mut client = PgClient::connect(&layout.dst_connection_string, NoTls).map_err(map_pg_err)?;
            let meta = pg_meta_table(layout, "synclite_consolidator_table_metadata");
            let sql = format!(
                "SELECT table_name, prop_value FROM {meta} \
                 WHERE synclite_device_id = $1 AND synclite_device_name = $2 AND prop_key = 'create_sql' \
                 ORDER BY table_name"
            );
            let rows = client
                .query(&sql, &[&layout.device_id, &layout.device_name])
                .map_err(map_pg_err)?;
            Ok(rows
                .into_iter()
                .map(|r| {
                    let table: String = r.get(0);
                    let sql: Option<String> = r.get(1);
                    (table, sql.unwrap_or_default())
                })
                .collect())
        }
    }
}

fn derive_commandlog_ddl_columns(
    replica: &mut Connection,
    op_type: &str,
    table_name: Option<&str>,
    sql: &str,
) -> Result<Vec<CdcSchemaColumn>> {
    let ddl_ops = matches!(
        op_type,
        "CREATETABLE" | "ADDCOLUMN" | "ALTERCOLUMN" | "DROPCOLUMN" | "RENAMECOLUMN" | "RENAMETABLE" | "DROPTABLE"
    );
    if !ddl_ops {
        return Ok(Vec::new());
    }

    // Java parity (`DeviceEventStreamer.executeDDLOnReplica`): the replica
    // is long-lived per device-dst, so re-applying the same segment (retry
    // path or restart catch-up) will replay DDL that is already present.
    // Tolerate the idempotency error classes Java tolerates instead of
    // failing the segment.
    if let Err(e) = replica.execute_batch(sql) {
        let msg = e.to_string().to_ascii_lowercase();
        let idempotent = msg.contains("already exists")
            || msg.contains("duplicate column name")
            || msg.contains("no such column")
            || msg.contains("no such table");
        if !idempotent {
            return Err(map_sql_err(e));
        }
    }

    let effective_table = if op_type == "RENAMETABLE" {
        parse_alter_rename_table_name(sql)
            .or_else(|| table_name.map(|t| t.to_string()))
    } else {
        table_name.map(|t| t.to_string())
    };

    let Some(tbl) = effective_table else {
        return Ok(Vec::new());
    };

    if op_type == "DROPTABLE" {
        return Ok(Vec::new());
    }

    let cols = fetch_schema_columns_from_replica(replica, &tbl)?;
    if op_type == "ADDCOLUMN" {
        if let Some(parsed) = parse_add_column_column(sql) {
            let name = parsed.column_name.to_ascii_lowercase();
            if let Some(found) = cols.iter().find(|c| c.column_name.eq_ignore_ascii_case(&name)).cloned() {
                return Ok(vec![found]);
            }
        }
    }
    Ok(cols)
}

fn fetch_schema_columns_from_replica(replica: &Connection, table_name: &str) -> Result<Vec<CdcSchemaColumn>> {
    let sqlite_table = table_name
        .trim()
        .trim_matches('"')
        .split('.')
        .last()
        .unwrap_or(table_name)
        .trim_matches('"')
        .to_string();
    if sqlite_table.is_empty() {
        return Ok(Vec::new());
    }

    let pragma = format!("PRAGMA table_info({})", quote_ident_raw(&sqlite_table));
    let mut stmt = replica.prepare(&pragma).map_err(map_sql_err)?;
    let mut rows = stmt.query([]).map_err(map_sql_err)?;

    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(map_sql_err)? {
        let cid: i64 = row.get(0).map_err(map_sql_err)?;
        let name: String = row.get(1).map_err(map_sql_err)?;
        let ty: Option<String> = row.get(2).map_err(map_sql_err)?;
        let pk: i64 = row.get(5).map_err(map_sql_err)?;
        out.push(CdcSchemaColumn {
            column_index: cid,
            column_name: name,
            column_type: ty,
            column_primary_key: pk,
            old_table_name: None,
            old_column_name: None,
        });
    }
    Ok(out)
}

/// Java parity: parse a `CREATE TABLE [IF NOT EXISTS] <name> (<col-defs>)`
/// statement into a list of `CdcSchemaColumn` entries.
///
/// Mirrors the metadata DeviceEventStreamer fetches from its in-memory
/// replica via `PRAGMA table_info(...)` for store devices.
///
/// Skips table-level constraints (PRIMARY KEY (...), FOREIGN KEY (...),
/// CHECK (...), UNIQUE (...), CONSTRAINT ...) so that only real columns
/// are returned.
fn parse_create_table_schema_columns(sql: &str) -> Option<Vec<CdcSchemaColumn>> {
    let open = sql.find('(')?;
    let close = sql.rfind(')')?;
    if close <= open {
        return None;
    }
    let body = &sql[open + 1..close];
    let parts = split_top_level_commas(body);
    let mut cols = Vec::new();
    let mut table_level_pks: Vec<String> = Vec::new();
    let mut idx: i64 = 0;
    for raw in parts {
        let part = raw.trim();
        if part.is_empty() {
            continue;
        }
        let upper = part.to_ascii_uppercase();
        if upper.starts_with("PRIMARY KEY") {
            // Capture column list inside (...).
            if let Some(po) = part.find('(') {
                if let Some(pc) = part.rfind(')') {
                    if pc > po {
                        for c in part[po + 1..pc].split(',') {
                            let n = strip_quotes(c.trim());
                            if !n.is_empty() {
                                table_level_pks.push(n);
                            }
                        }
                    }
                }
            }
            continue;
        }
        if upper.starts_with("FOREIGN KEY")
            || upper.starts_with("UNIQUE")
            || upper.starts_with("CHECK")
            || upper.starts_with("CONSTRAINT")
        {
            continue;
        }
        let (name, ty) = split_column_name_and_type(part);
        let name = strip_quotes(&name);
        if name.is_empty() {
            continue;
        }
        // Inline `PRIMARY KEY` after the type tags this column as a PK.
        let inline_pk = ty
            .as_deref()
            .map(|t| t.to_ascii_uppercase().contains("PRIMARY KEY"))
            .unwrap_or(false);
        cols.push(CdcSchemaColumn {
            column_index: idx,
            column_name: name,
            column_type: ty,
            column_primary_key: if inline_pk { idx + 1 } else { 0 },
            old_table_name: None,
            old_column_name: None,
        });
        idx += 1;
    }
    // Apply table-level PRIMARY KEY (a, b, ...) ordering to columns that
    // were not already tagged inline.
    if !table_level_pks.is_empty() {
        for (pk_pos, pk_name) in table_level_pks.iter().enumerate() {
            if let Some(col) = cols
                .iter_mut()
                .find(|c| c.column_name.eq_ignore_ascii_case(pk_name))
            {
                if col.column_primary_key == 0 {
                    col.column_primary_key = (pk_pos as i64) + 1;
                }
            }
        }
    }
    Some(cols)
}

/// Java parity: parse a single `ALTER TABLE <t> ADD COLUMN <col> <type>`
/// into a `CdcSchemaColumn`.
fn parse_add_column_column(sql: &str) -> Option<CdcSchemaColumn> {
    let upper = sql.to_ascii_uppercase();
    let idx = upper.find(" ADD COLUMN ")?;
    let rest = sql[idx + " ADD COLUMN ".len()..].trim();
    let rest = rest.trim_end_matches(';').trim();
    let (name, ty) = split_column_name_and_type(rest);
    let name = strip_quotes(&name);
    if name.is_empty() {
        return None;
    }
    let inline_pk = ty
        .as_deref()
        .map(|t| t.to_ascii_uppercase().contains("PRIMARY KEY"))
        .unwrap_or(false);
    Some(CdcSchemaColumn {
        column_index: 0,
        column_name: name,
        column_type: ty,
        column_primary_key: if inline_pk { 1 } else { 0 },
        old_table_name: None,
        old_column_name: None,
    })
}

/// Split a comma-separated list at top-level commas only (ignoring commas
/// inside parentheses, e.g. `decimal(10,2)`).
fn split_top_level_commas(body: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth: i32 = 0;
    let mut current = String::new();
    for ch in body.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                parts.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current);
    }
    parts
}

/// Split a column definition like `qty INTEGER NOT NULL DEFAULT 0` into
/// `("qty", Some("INTEGER"))`. Type may include a parenthesized argument
/// (e.g. `VARCHAR(64)` or `DECIMAL(10,2)`). Trailing constraints (NOT NULL,
/// DEFAULT ..., PRIMARY KEY, etc.) are dropped — they are not part of the
/// column type the DataTypeMapper consumes.
fn split_column_name_and_type(def: &str) -> (String, Option<String>) {
    let def = def.trim();
    // Name: first token (handles quoted identifiers minimally).
    let (name, rest) = match def.find(|c: char| c.is_whitespace()) {
        Some(idx) => (def[..idx].to_string(), def[idx..].trim_start().to_string()),
        None => return (def.to_string(), None),
    };
    if rest.is_empty() {
        return (name, None);
    }
    // Type token: first non-whitespace token, optionally followed by a
    // single parenthesized argument list (e.g. VARCHAR(64), DECIMAL(10,2)).
    let mut chars = rest.char_indices().peekable();
    let mut type_end = rest.len();
    let mut saw_open = false;
    let mut depth: i32 = 0;
    while let Some((i, ch)) = chars.next() {
        if ch.is_whitespace() && !saw_open && depth == 0 {
            type_end = i;
            break;
        }
        if ch == '(' {
            saw_open = true;
            depth += 1;
        } else if ch == ')' {
            depth -= 1;
            if depth == 0 {
                type_end = i + 1;
                break;
            }
        }
    }
    let ty = rest[..type_end].trim().to_string();
    let ty_opt = if ty.is_empty() { None } else { Some(ty) };
    (name, ty_opt)
}

fn strip_quotes(name: &str) -> String {
    let t = name.trim();
    let trimmed = t
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']');
    trimmed.to_string()
}

fn read_cdclog_entries(seg_conn: &Connection) -> Result<(i64, Vec<SegmentEntry>)> {
    let first_commit_id = seg_conn
        .query_row("SELECT MIN(commit_id) FROM cdclog", [], |row| {
            row.get::<_, Option<i64>>(0)
        })
        .map_err(map_sql_err)?
        .unwrap_or(0);

    let mut stmt = seg_conn
        .prepare("SELECT change_number, commit_id, sql, arg_cnt, table_name, op_type FROM cdclog ORDER BY change_number")
        .map_err(map_sql_err)?;
    let mut rows = stmt.query([]).map_err(map_sql_err)?;
    let mut entries = Vec::new();
    let mut last_sql: Option<String> = None;

    while let Some(row) = rows.next().map_err(map_sql_err)? {
        let change_number: i64 = row.get(0).map_err(map_sql_err)?;
        let commit_id: i64 = row.get(1).map_err(map_sql_err)?;
        let sql_opt: Option<String> = row.get(2).map_err(map_sql_err)?;
        let sql = if let Some(s) = sql_opt {
            if s.trim().is_empty() {
                last_sql.clone().unwrap_or_default()
            } else {
                last_sql = Some(s.clone());
                s
            }
        } else {
            last_sql.clone().unwrap_or_default()
        };
        let arg_cnt: i64 = row.get(3).map_err(map_sql_err)?;
        let table_name: Option<String> = row.get(4).map_err(map_sql_err)?;
        let op_type: Option<String> = row.get(5).map_err(map_sql_err)?;
        let ddl_columns = if op_type
            .as_deref()
            .map(|op| matches!(
                op.to_ascii_uppercase().as_str(),
                "CREATETABLE" | "ADDCOLUMN" | "DROPCOLUMN" | "RENAMECOLUMN" | "RENAMETABLE" | "ALTERCOLUMN" | "DROPTABLE"
            ))
            .unwrap_or(false)
        {
            read_cdclog_schema_columns(seg_conn, change_number)?
        } else {
            Vec::new()
        };

        let has_placeholders = sql.contains('?');
        let mut args = if arg_cnt <= 0 || !has_placeholders {
            Vec::new()
        } else {
            let mut arg_stmt = seg_conn
                .prepare(&format!(
                    "SELECT {} FROM cdclog WHERE change_number=?1",
                    (1..=arg_cnt)
                        .map(|i| format!("arg{i}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .map_err(map_sql_err)?;
            arg_stmt
                .query_row([change_number], |arg_row| {
                    let mut vals = Vec::with_capacity(arg_cnt as usize);
                    for i in 0..arg_cnt as usize {
                        vals.push(arg_row.get::<_, SqlValue>(i)?);
                    }
                    Ok(vals)
                })
                .map_err(map_sql_err)?
        };

        // Java parity: cdclog stores UPDATE args as `before…after`; apply expects `after…before`.
        if op_type
            .as_deref()
            .map(|op| op.eq_ignore_ascii_case("UPDATE"))
            .unwrap_or(false)
            && !args.is_empty()
            && args.len() % 2 == 0
        {
            let half = args.len() / 2;
            let mut reordered = Vec::with_capacity(args.len());
            reordered.extend_from_slice(&args[half..]);
            reordered.extend_from_slice(&args[..half]);
            args = reordered;
        }

        entries.push(SegmentEntry {
            change_number,
            commit_id,
            source: SegmentSource::CdcLog,
            table_name,
            op_type,
            ddl_columns,
            sql,
            args,
        });
    }

    Ok((first_commit_id, entries))
}

fn read_cdclog_schema_columns(seg_conn: &Connection, change_number: i64) -> Result<Vec<CdcSchemaColumn>> {
    let mut stmt = seg_conn
        .prepare(
            "SELECT column_index, column_name, column_type, column_primary_key, old_table_name, old_column_name \
             FROM cdclog_schemas WHERE change_number=?1 ORDER BY column_index",
        )
        .map_err(map_sql_err)?;
    let mut rows = stmt.query([change_number]).map_err(map_sql_err)?;
    let mut cols = Vec::new();
    while let Some(row) = rows.next().map_err(map_sql_err)? {
        let col = CdcSchemaColumn {
            column_index: row.get(0).map_err(map_sql_err)?,
            column_name: row.get::<_, Option<String>>(1).map_err(map_sql_err)?.unwrap_or_default(),
            column_type: row.get(2).map_err(map_sql_err)?,
            column_primary_key: row.get::<_, Option<i64>>(3).map_err(map_sql_err)?.unwrap_or(0),
            old_table_name: row.get(4).map_err(map_sql_err)?,
            old_column_name: row.get(5).map_err(map_sql_err)?,
        };
        cols.push(col);
    }
    Ok(cols)
}

fn apply_staged_segment_with_retry(layout: &ConsolidatorLayout, segment_path: &Path) -> Result<ApplyProgress> {
    with_device_tracer(|t| {
        tracer_info!(
            t,
            "DeviceConsolidator",
            "Started consolidating cdc log segment : {} on dst : {}",
            segment_path.display(),
            layout.dst_index
        )
    });
    let max_attempts = layout.dst_oper_retry_count.max(1);
    let mut last_err: Option<Error> = None;
    for attempt in 1..=max_attempts {
        match apply_staged_segment_once(layout, segment_path) {
            Ok(progress) => {
                with_device_tracer(|t| {
                    tracer_info!(
                        t,
                        "DeviceConsolidator",
                        "Finished consolidating cdc log segment : {} on dst : {} (appliedTxns={} lastCommitID={})",
                        segment_path.display(),
                        layout.dst_index,
                        progress.applied_txn_cnt,
                        progress.applied_commit
                    )
                });
                // Java parity: Monitor.PrometheusDumper counters
                // (Device.applyCDCLogSegment success path).
                let m = monitor::monitor();
                m.incr_total_cdc_log_segment_cnt(1);
                m.incr_total_dst_txn_cnt(progress.applied_txn_cnt as i64);
                m.incr_total_processed_oper_count(progress.applied_change);
                if let Ok(meta) = std::fs::metadata(segment_path) {
                    m.incr_total_processed_log_size(meta.len() as i64);
                }
                return Ok(progress);
            }
            Err(e) => {
                let retry = should_retry_apply_error(layout.dst_type, &e) && attempt < max_attempts;
                last_err = Some(e);
                if retry {
                    thread::sleep(Duration::from_millis(layout.dst_oper_retry_interval_ms));
                } else {
                    break;
                }
            }
        }
    }
    let err = last_err.unwrap_or_else(|| Error::Config("consolidator: unknown apply error".to_string()));
    if layout.dst_skip_failed_log_files {
        // Java parity: `dst-skip-failed-log-files=true` keeps the
        // consolidator advancing past a poison segment by logging the
        // failure and returning an empty progress so callers checkpoint
        // and clean up the file as if it succeeded.
        tracing::error!(
            segment = %segment_path.display(),
            dst_index = layout.dst_index,
            attempts = max_attempts,
            error = %err,
            "skipping failed log file (dst_skip_failed_log_files=true)"
        );
        with_device_tracer(|t| {
            tracer_info!(
                t,
                "DeviceConsolidator",
                "Skipping failed cdc log segment : {} on dst : {} after {} attempts. Last error: {}",
                segment_path.display(),
                layout.dst_index,
                max_attempts,
                err
            )
        });
        monitor::monitor().incr_total_cdc_log_segment_cnt(1);
        return Ok(ApplyProgress {
            applied_change: -1,
            applied_commit: -1,
            applied_txn_cnt: 0,
        });
    }
    Err(err)
}

fn apply_staged_segment_once(layout: &ConsolidatorLayout, segment_path: &Path) -> Result<ApplyProgress> {
    let seg_conn = Connection::open(segment_path).map_err(map_sql_err)?;
    // Java parity (`DeviceSyncProcessor.inMemoryReplicaConn`): the schema
    // replica is owned by the worker thread for the lifetime of the
    // consolidator (one worker == one device-dst). Acquire it for the
    // duration of this segment apply, then return it on drop so the next
    // segment reuses the accumulated CREATE TABLE / ALTER TABLE state.
    let mut guard = acquire_schema_replica(layout)?;
    let schema_replica = guard.as_mut();
    let (first_commit_id, entries) =
        read_segment_entries_with_resume_and_replica(&seg_conn, segment_path, layout, schema_replica)?;
    // DuckDB-family devices stage per-transaction records into a
    // `.sqllog.{commit}.txn` sidecar; the main segment only carries
    // BEGIN/COMMIT/REPLAY_TXN markers. The SQL-device path expands these
    // sidecars inside `replay_segment_with_native_cdc`, but the
    // store/streaming apply path reaches us directly — expand here so the
    // user's CREATE TABLE / INSERT / UPDATE rows actually get applied.
    let entries = if let Some(seq) = parse_segment_seq(segment_path) {
        expand_replay_txn_entries(entries, segment_path, seq as i64, schema_replica)?
    } else {
        entries
    };
    let progress = match layout.dst_type {
        DstType::Sqlite => apply_to_sqlite_destination(layout, first_commit_id, &entries)?,
        DstType::DuckDb => apply_to_duckdb_destination(layout, first_commit_id, &entries)?,
        DstType::Postgres => apply_to_postgres_destination(layout, first_commit_id, &entries)?,
    };

    if let Some(seq) = parse_segment_seq(segment_path) {
        if layout.metadata_store == MetadataStore::Destination {
            map_destination_checkpoint(layout, seq as i64, progress)?;
        }
        // Java parity: every successful segment apply updates
        // last_consolidated_cdc_log_segment_seq_num in the consolidator
        // metadata store (LOCAL file or destination synclite_consolidator_metadata).
        if let Err(e) = consolidator_state::record_consolidated_cdc_seq(layout, layout.dst_index, seq as i64) {
            tracing::warn!(error = %e, seq, "failed to record consolidated cdc seq in metadata");
        }
    }

    Ok(progress)
}

fn apply_to_sqlite_destination(
    layout: &ConsolidatorLayout,
    first_commit_id: i64,
    entries: &[SegmentEntry],
) -> Result<ApplyProgress> {
    tracing::info!(
        dst_alias = %layout.dst_alias,
        dst_index = layout.dst_index,
        predicate_opt = layout.dst_oper_predicate_optimization,
        unparsable_to_null = layout.dst_set_unparsable_values_to_null,
        entries = entries.len(),
        "applying batch to SQLite destination"
    );
    let dst_conn = Connection::open(&layout.dst_connection_string).map_err(map_sql_err)?;
    if layout.metadata_store == MetadataStore::Destination {
        ensure_sqlite_metadata_seeded(&dst_conn, layout, first_commit_id)?;
    }
    dst_conn.execute_batch("BEGIN IMMEDIATE;").map_err(map_sql_err)?;

    let mut progress = ApplyProgress {
        applied_change: -1,
        applied_commit: first_commit_id,
        applied_txn_cnt: 0,
    };

    let sql_generator = DestinationSqlGenerator::new(layout.dst_type);
    let mut mapped_cache: HashMap<MappedOperationKey, CachedMappedOperation> = HashMap::new();
    let mut active_batch_key: Option<MappedOperationKey> = None;
    let mut batch_count = 0usize;
    let mut pk_cache: PkCache = HashMap::new();
    let mut tables_touched: HashSet<String> = HashSet::new();

    for entry in entries {
        let mapped = match map_operation_using_mapper(layout, entry) {
            ApplyAction::Skip => continue,
            ApplyAction::Execute(mapped) => mapped,
        };

        let key = MappedOperationKey {
            mapper: mapped.mapper,
            kind: mapped.kind,
            table_name: mapped.table_name.clone(),
            sql: mapped.sql.clone(),
        };
        let batch_limit = batch_limit_for_kind(layout, mapped.kind);
        if active_batch_key.as_ref() != Some(&key) || batch_count >= batch_limit {
            active_batch_key = Some(key.clone());
            batch_count = 0;
        }

        let cached = mapped_cache.entry(key).or_insert_with(|| CachedMappedOperation {
            sql: mapped.sql.clone(),
            reusable_args: Vec::with_capacity(mapped.args.len().max(8)),
        });
        cached.reusable_args.clear();
        cached.reusable_args.extend_from_slice(&mapped.args);

        if let Some(tbl) = mapped.table_name.as_deref() {
            if !mapped.is_system_table && mapped.kind != DmlKind::Other {
                tables_touched.insert(tbl.to_ascii_uppercase());
            }
        }

        if mapped.kind == DmlKind::Other {
            // Java parity: DDL is a multi-statement script
            // (CREATE + ADD COLUMN per col + optional DELETE FROM);
            // execute each statement and swallow benign errors.
            if let Err(e) = apply_ddl_batch_sqlite(&dst_conn, &cached.sql) {
                let _ = dst_conn.execute_batch("ROLLBACK;");
                return Err(e);
            }
            // CREATETABLE / ADDCOLUMN may have changed PK metadata; drop cache entry.
            if let Some(tbl) = mapped.table_name.as_deref() {
                pk_cache.remove(&tbl.to_ascii_lowercase());
            }
        } else if mapped.kind == DmlKind::Insert
            && layout.dst_idempotent_data_ingestion
            && !mapped.is_system_table
        {
            let table = mapped.table_name.as_deref().unwrap_or("");
            let pks = if table.is_empty() {
                Vec::new()
            } else {
                lookup_pks_sqlite(&dst_conn, &mut pk_cache, table)
            };
            let stmts = rewrite_insert_idempotent(
                layout,
                DstType::Sqlite,
                layout.dst_idempotent_data_ingestion_method,
                &cached.sql,
                &cached.reusable_args,
                &pks,
            );
            let mut failed: Option<rusqlite::Error> = None;
            for (s, a) in &stmts {
                if let Err(e) = dst_conn.execute(s, params_from_iter(a.iter())) {
                    failed = Some(e);
                    break;
                }
            }
            if let Some(e) = failed {
                if is_sqlite_duplicate_err(&e) {
                    tracing::warn!(change_number = mapped.change_number, error = %e, "idempotent ingest ignored failed insert");
                } else if layout.dst_set_unparsable_values_to_null && is_sqlite_unparsable_err(&e) {
                    tracing::warn!(
                        change_number = mapped.change_number,
                        error = %e,
                        "dropping row with unparsable value (dst-set-unparsable-values-to-null)"
                    );
                } else {
                    let _ = dst_conn.execute_batch("ROLLBACK;");
                    return Err(map_sql_err(e));
                }
            }
        } else {
            let exec_result = match sql_generator.generate(cached) {
                GeneratedSql::Parameterized { sql, args } => dst_conn.execute(sql, params_from_iter(args.iter())),
                GeneratedSql::Text(_) => unreachable!("sqlite path must use parameterized SQL"),
            };

            if let Err(e) = exec_result {
                if layout.dst_idempotent_data_ingestion && mapped.kind == DmlKind::Insert && is_sqlite_duplicate_err(&e) {
                    tracing::warn!(change_number = mapped.change_number, error = %e, "idempotent ingest ignored failed insert");
                } else if layout.dst_set_unparsable_values_to_null
                    && matches!(mapped.kind, DmlKind::Insert | DmlKind::Update)
                    && is_sqlite_unparsable_err(&e)
                {
                    tracing::warn!(
                        change_number = mapped.change_number,
                        error = %e,
                        "dropping row with unparsable value (dst-set-unparsable-values-to-null)"
                    );
                } else {
                    let _ = dst_conn.execute_batch("ROLLBACK;");
                    return Err(map_sql_err(e));
                }
            }
        }

        progress.applied_change = mapped.change_number;
        progress.applied_commit = mapped.commit_id;
        progress.applied_txn_cnt += 1;
        batch_count += 1;
    }

    // Java parity: run dst_triggers SQL for every table touched by this batch,
    // inside the same transaction so triggers commit atomically with the data.
    execute_triggers_sqlite(&dst_conn, layout, &tables_touched)?;

    dst_conn.execute_batch("COMMIT;").map_err(map_sql_err)?;
    if layout.metadata_store == MetadataStore::Destination {
        sync_sqlite_destination_metadata(&dst_conn, layout)?;
    } else {
        sync_sqlite_local_metadata_create_sql(&dst_conn, layout)?;
    }
    Ok(progress)
}

fn execute_triggers_sqlite(
    conn: &Connection,
    layout: &ConsolidatorLayout,
    tables: &HashSet<String>,
) -> Result<()> {
    if layout.dst_triggers.is_empty() || tables.is_empty() {
        return Ok(());
    }
    for tbl in tables {
        if let Some(stmts) = layout.dst_triggers.get(tbl) {
            tracing::info!(table = %tbl, trigger_count = stmts.len(), "executing dst_triggers (sqlite)");
            for s in stmts {
                tracing::debug!(table = %tbl, sql = %s, "executing dst_trigger statement");
                if let Err(e) = conn.execute_batch(s) {
                    tracing::error!(table = %tbl, error = %e, sql = %s, "dst_trigger execution failed (sqlite)");
                    let _ = conn.execute_batch("ROLLBACK;");
                    return Err(map_sql_err(e));
                }
            }
        }
    }
    Ok(())
}

fn apply_to_duckdb_destination(
    layout: &ConsolidatorLayout,
    first_commit_id: i64,
    entries: &[SegmentEntry],
) -> Result<ApplyProgress> {
    tracing::info!(
        dst_alias = %layout.dst_alias,
        dst_index = layout.dst_index,
        predicate_opt = layout.dst_oper_predicate_optimization,
        unparsable_to_null = layout.dst_set_unparsable_values_to_null,
        entries = entries.len(),
        "applying batch to DuckDB destination"
    );
    let dst_conn = DuckConnection::open(&layout.dst_connection_string).map_err(map_duck_err)?;
    if layout.metadata_store == MetadataStore::Destination {
        ensure_duckdb_metadata_seeded(&dst_conn, layout, first_commit_id)?;
    }
    dst_conn.execute_batch("BEGIN;").map_err(map_duck_err)?;

    let mut progress = ApplyProgress {
        applied_change: -1,
        applied_commit: first_commit_id,
        applied_txn_cnt: 0,
    };

    let sql_generator = DestinationSqlGenerator::new(layout.dst_type);
    let mut mapped_cache: HashMap<MappedOperationKey, CachedMappedOperation> = HashMap::new();
    let mut active_batch_key: Option<MappedOperationKey> = None;
    let mut batch_count = 0usize;
    let mut pk_cache: PkCache = HashMap::new();
    let mut tables_touched: HashSet<String> = HashSet::new();

    for entry in entries {
        let mapped = match map_operation_using_mapper(layout, entry) {
            ApplyAction::Skip => continue,
            ApplyAction::Execute(mapped) => mapped,
        };

        let key = MappedOperationKey {
            mapper: mapped.mapper,
            kind: mapped.kind,
            table_name: mapped.table_name.clone(),
            sql: mapped.sql.clone(),
        };
        let batch_limit = batch_limit_for_kind(layout, mapped.kind);
        if active_batch_key.as_ref() != Some(&key) || batch_count >= batch_limit {
            active_batch_key = Some(key.clone());
            batch_count = 0;
        }

        let cached = mapped_cache.entry(key).or_insert_with(|| CachedMappedOperation {
            sql: mapped.sql.clone(),
            reusable_args: Vec::with_capacity(mapped.args.len().max(8)),
        });
        cached.reusable_args.clear();
        cached.reusable_args.extend_from_slice(&mapped.args);

        if let Some(tbl) = mapped.table_name.as_deref() {
            if !mapped.is_system_table && mapped.kind != DmlKind::Other {
                tables_touched.insert(tbl.to_ascii_uppercase());
            }
        }

        if mapped.kind == DmlKind::Other {
            // Java parity: DDL multi-statement script, swallow benign errors.
            if let Err(e) = apply_ddl_batch_duckdb(&dst_conn, &cached.sql) {
                let _ = dst_conn.execute_batch("ROLLBACK;");
                return Err(e);
            }
            if let Some(tbl) = mapped.table_name.as_deref() {
                pk_cache.remove(&tbl.to_ascii_lowercase());
            }
        } else if mapped.kind == DmlKind::Insert
            && layout.dst_idempotent_data_ingestion
            && !mapped.is_system_table
        {
            let table = mapped.table_name.as_deref().unwrap_or("");
            let pks = if table.is_empty() {
                Vec::new()
            } else {
                lookup_pks_duckdb(&dst_conn, &mut pk_cache, table)
            };
            let stmts = rewrite_insert_idempotent(
                layout,
                DstType::DuckDb,
                layout.dst_idempotent_data_ingestion_method,
                &cached.sql,
                &cached.reusable_args,
                &pks,
            );
            let mut failed: Option<Error> = None;
            for (s, a) in &stmts {
                let duck_vals: Vec<DuckValue> = a.iter().map(sqlvalue_to_duckvalue).collect();
                if let Err(e) = duck_exec_with_params(&dst_conn, s, &duck_vals) {
                    failed = Some(e);
                    break;
                }
            }
            if let Some(e) = failed {
                if is_duckdb_duplicate_insert_err(&e) {
                    tracing::warn!(change_number = mapped.change_number, error = %e, "idempotent ingest ignored failed insert");
                } else if layout.dst_set_unparsable_values_to_null && is_duckdb_unparsable_err(&e) {
                    tracing::warn!(
                        change_number = mapped.change_number,
                        error = %e,
                        "dropping row with unparsable value (dst-set-unparsable-values-to-null)"
                    );
                } else {
                    let _ = dst_conn.execute_batch("ROLLBACK;");
                    return Err(e);
                }
            }
        } else {
            let exec_result = match sql_generator.generate(cached) {
                GeneratedSql::Parameterized { sql, args } => {
                    let duck_vals: Vec<DuckValue> = args.iter().map(sqlvalue_to_duckvalue).collect();
                    duck_exec_with_params(&dst_conn, sql, &duck_vals)
                }
                GeneratedSql::Text(_) => unreachable!("duckdb path must use parameterized SQL"),
            };

            if let Err(e) = exec_result {
                if layout.dst_idempotent_data_ingestion && mapped.kind == DmlKind::Insert && is_duckdb_duplicate_insert_err(&e) {
                    tracing::warn!(change_number = mapped.change_number, error = %e, "idempotent ingest ignored failed insert");
                } else if layout.dst_set_unparsable_values_to_null
                    && matches!(mapped.kind, DmlKind::Insert | DmlKind::Update)
                    && is_duckdb_unparsable_err(&e)
                {
                    tracing::warn!(
                        change_number = mapped.change_number,
                        error = %e,
                        "dropping row with unparsable value (dst-set-unparsable-values-to-null)"
                    );
                } else {
                    let _ = dst_conn.execute_batch("ROLLBACK;");
                    return Err(e);
                }
            }
        }

        progress.applied_change = mapped.change_number;
        progress.applied_commit = mapped.commit_id;
        progress.applied_txn_cnt += 1;
        batch_count += 1;
    }

    // Java parity: run dst_triggers SQL for every table touched by this batch.
    if !layout.dst_triggers.is_empty() {
        for tbl in &tables_touched {
            if let Some(stmts) = layout.dst_triggers.get(tbl) {
                tracing::info!(table = %tbl, trigger_count = stmts.len(), "executing dst_triggers (duckdb)");
                for s in stmts {
                    tracing::debug!(table = %tbl, sql = %s, "executing dst_trigger statement");
                    if let Err(e) = dst_conn.execute_batch(s) {
                        tracing::error!(table = %tbl, error = %e, sql = %s, "dst_trigger execution failed (duckdb)");
                        let _ = dst_conn.execute_batch("ROLLBACK;");
                        return Err(map_duck_err(e));
                    }
                }
            }
        }
    }

    dst_conn.execute_batch("COMMIT;").map_err(map_duck_err)?;
    if layout.metadata_store == MetadataStore::Destination {
        sync_duckdb_destination_metadata(&dst_conn, layout)?;
    } else {
        sync_duckdb_local_metadata_create_sql(&dst_conn, layout)?;
    }
    Ok(progress)
}

fn sync_sqlite_destination_metadata(dest_conn: &Connection, layout: &ConsolidatorLayout) -> Result<()> {
    ensure_sqlite_metadata_init_status_column(dest_conn)?;
    dest_conn
        .execute(
            "UPDATE synclite_checkpoint
             SET initialization_status = 1
             WHERE synclite_device_id = ?1 AND synclite_device_name = ?2",
            params![layout.device_id, layout.device_name],
        )
        .map_err(map_sql_err)?;

    dest_conn
        .execute(
            "DELETE FROM synclite_consolidator_table_metadata WHERE synclite_device_id = ?1 AND synclite_device_name = ?2 AND prop_key = 'create_sql'",
            params![layout.device_id, layout.device_name],
        )
        .map_err(map_sql_err)?;

    let mut stmt = dest_conn
        .prepare(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'table'
               AND name NOT IN (
                   'synclite_checkpoint',
                   'synclite_consolidator_metadata',
                   'synclite_consolidator_table_metadata',
                   'sqlite_sequence'
               )
             ORDER BY name",
        )
        .map_err(map_sql_err)?;
    let mut rows = stmt.query([]).map_err(map_sql_err)?;
    while let Some(row) = rows.next().map_err(map_sql_err)? {
        let table_name: String = row.get(0).map_err(map_sql_err)?;
        let create_sql: Option<String> = row.get(1).map_err(map_sql_err)?;
        dest_conn
            .execute(
                "INSERT OR REPLACE INTO synclite_consolidator_table_metadata(
                     synclite_device_id, synclite_device_name, synclite_update_timestamp, database_name, table_name, prop_key, prop_value
                 ) VALUES(?1, ?2, ?3, 'main', ?4, 'create_sql', ?5)",
                params![
                    layout.device_id,
                    layout.device_name,
                    current_update_timestamp(),
                    table_name,
                    create_sql.unwrap_or_default(),
                ],
            )
            .map_err(map_sql_err)?;
    }

    Ok(())
}

fn sync_sqlite_local_metadata_create_sql(dest_conn: &Connection, layout: &ConsolidatorLayout) -> Result<()> {
    let meta_path = layout.consolidator_metadata_path(layout.dst_index);
    let meta_conn = Connection::open(&meta_path).map_err(map_sql_err)?;
    meta_conn
        .execute(
            "DELETE FROM table_metadata WHERE database_name = ?1 AND key = 'create_sql'",
            params![layout.database_name],
        )
        .map_err(map_sql_err)?;

    let mut stmt = dest_conn
        .prepare(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'table'
               AND name NOT IN (
                   'synclite_checkpoint',
                   'synclite_consolidator_metadata',
                   'synclite_consolidator_table_metadata',
                   'sqlite_sequence'
               )
             ORDER BY name",
        )
        .map_err(map_sql_err)?;
    let mut rows = stmt.query([]).map_err(map_sql_err)?;
    while let Some(row) = rows.next().map_err(map_sql_err)? {
        let table_name: String = row.get(0).map_err(map_sql_err)?;
        let create_sql: Option<String> = row.get(1).map_err(map_sql_err)?;
        consolidator_state::upsert_consolidator_table_metadata(
            layout,
            layout.dst_index,
            &layout.database_name,
            "",
            &table_name,
            "create_sql",
            &create_sql.unwrap_or_default(),
        )?;
    }
    Ok(())
}

fn ensure_sqlite_metadata_init_status_column(dest_conn: &Connection) -> Result<()> {
    let mut stmt = dest_conn
        .prepare("PRAGMA table_info(synclite_checkpoint)")
        .map_err(map_sql_err)?;
    let mut rows = stmt.query([]).map_err(map_sql_err)?;
    let mut has_column = false;
    while let Some(row) = rows.next().map_err(map_sql_err)? {
        let column_name: String = row.get(1).map_err(map_sql_err)?;
        if column_name == "initialization_status" {
            has_column = true;
            break;
        }
    }
    if !has_column {
        dest_conn
            .execute_batch("ALTER TABLE synclite_checkpoint ADD COLUMN initialization_status INTEGER NOT NULL DEFAULT 0")
            .map_err(map_sql_err)?;
    }
    Ok(())
}

/// DuckDB DESTINATION-mode bookkeeping refresh: marks the device row as
/// initialized and rebuilds per-table `create_sql` rows under
/// `synclite_consolidator_table_metadata` from the live destination catalog.
/// Mirrors `sync_sqlite_destination_metadata`.
///
/// DuckDB has no `sqlite_master` analog, so we synthesize a SQLite-flavored
/// CREATE TABLE per user table from `PRAGMA table_info(...)`. The downstream
/// consumer (Java `loadSchemasFromDestination`) only replays the CREATE on
/// an in-memory SQLite and reads back column shape via `PRAGMA table_info`,
/// so name + type + NOT NULL + PRIMARY KEY are sufficient (matches Java's
/// `buildCreateSqlFromColumns` shape).
fn sync_duckdb_destination_metadata(dest_conn: &DuckConnection, layout: &ConsolidatorLayout) -> Result<()> {
    duck_exec_with_params(
        dest_conn,
        "UPDATE synclite_checkpoint
         SET initialization_status = 1
         WHERE synclite_device_id = ? AND synclite_device_name = ?",
        &[
            DuckValue::Text(layout.device_id.clone()),
            DuckValue::Text(layout.device_name.clone()),
        ],
    )?;

    duck_exec_with_params(
        dest_conn,
        "DELETE FROM synclite_consolidator_table_metadata WHERE synclite_device_id = ? AND synclite_device_name = ? AND prop_key = 'create_sql'",
        &[
            DuckValue::Text(layout.device_id.clone()),
            DuckValue::Text(layout.device_name.clone()),
        ],
    )?;

    let mut tables: Vec<String> = Vec::new();
    {
        let mut stmt = dest_conn
            .prepare(
                "SELECT table_name FROM information_schema.tables
                 WHERE table_type = 'BASE TABLE'
                   AND table_name NOT IN (
                       'synclite_checkpoint',
                       'synclite_consolidator_metadata',
                       'synclite_consolidator_table_metadata'
                   )
                   AND table_schema NOT IN ('information_schema', 'pg_catalog')
                 ORDER BY table_name",
            )
            .map_err(map_duck_err)?;
        let mut rows = stmt.query([]).map_err(map_duck_err)?;
        while let Some(row) = rows.next().map_err(map_duck_err)? {
            let name: String = row.get(0).map_err(map_duck_err)?;
            tables.push(name);
        }
    }

    for tbl in tables {
        let create_sql = build_create_sql_from_duckdb_columns(dest_conn, &tbl)?;
        let ts = current_update_timestamp();
        duck_exec_with_params(
            dest_conn,
            "INSERT INTO synclite_consolidator_table_metadata(
                 synclite_device_id, synclite_device_name, synclite_update_timestamp, database_name, table_name, prop_key, prop_value
             ) VALUES(?, ?, ?, 'main', ?, 'create_sql', ?)",
            &[
                DuckValue::Text(layout.device_id.clone()),
                DuckValue::Text(layout.device_name.clone()),
                DuckValue::Text(ts),
                DuckValue::Text(tbl),
                DuckValue::Text(create_sql),
            ],
        )?;
    }
    Ok(())
}

fn sync_duckdb_local_metadata_create_sql(dest_conn: &DuckConnection, layout: &ConsolidatorLayout) -> Result<()> {
    let meta_path = layout.consolidator_metadata_path(layout.dst_index);
    let meta_conn = Connection::open(&meta_path).map_err(map_sql_err)?;
    meta_conn
        .execute(
            "DELETE FROM table_metadata WHERE database_name = ?1 AND key = 'create_sql'",
            params![layout.database_name],
        )
        .map_err(map_sql_err)?;

    let mut tables: Vec<String> = Vec::new();
    {
        let mut stmt = dest_conn
            .prepare(
                "SELECT table_name FROM information_schema.tables
                 WHERE table_type = 'BASE TABLE'
                   AND table_name NOT IN (
                       'synclite_checkpoint',
                       'synclite_consolidator_metadata',
                       'synclite_consolidator_table_metadata'
                   )
                   AND table_schema NOT IN ('information_schema', 'pg_catalog')
                 ORDER BY table_name",
            )
            .map_err(map_duck_err)?;
        let mut rows = stmt.query([]).map_err(map_duck_err)?;
        while let Some(row) = rows.next().map_err(map_duck_err)? {
            let name: String = row.get(0).map_err(map_duck_err)?;
            tables.push(name);
        }
    }

    for tbl in tables {
        let create_sql = build_create_sql_from_duckdb_columns(dest_conn, &tbl)?;
        consolidator_state::upsert_consolidator_table_metadata(
            layout,
            layout.dst_index,
            &layout.database_name,
            "",
            &tbl,
            "create_sql",
            &create_sql,
        )?;
    }
    Ok(())
}

fn build_create_sql_from_duckdb_columns(conn: &DuckConnection, table: &str) -> Result<String> {
    let pragma = format!("PRAGMA table_info('{}')", table.replace('\'', "''"));
    let mut stmt = conn.prepare(&pragma).map_err(map_duck_err)?;
    let mut rows = stmt.query([]).map_err(map_duck_err)?;
    let mut cols: Vec<(String, String, bool)> = Vec::new();
    let mut pks: Vec<(i64, String)> = Vec::new();
    while let Some(row) = rows.next().map_err(map_duck_err)? {
        let name: String = row.get(1).map_err(map_duck_err)?;
        let ty: String = row.get(2).map_err(map_duck_err)?;
        let notnull: i64 = row.get(3).map_err(map_duck_err)?;
        let pk: i64 = row.get(5).map_err(map_duck_err)?;
        cols.push((name.clone(), ty, notnull != 0));
        if pk > 0 {
            pks.push((pk, name));
        }
    }
    pks.sort_by_key(|(o, _)| *o);

    let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (", table);
    let mut first = true;
    for (name, ty, nn) in &cols {
        if !first {
            sql.push_str(", ");
        }
        first = false;
        sql.push_str(name);
        sql.push(' ');
        sql.push_str(ty);
        if *nn {
            sql.push_str(" NOT NULL");
        }
    }
    if !pks.is_empty() {
        sql.push_str(", PRIMARY KEY(");
        let pk_list: Vec<String> = pks.iter().map(|(_, n)| n.clone()).collect();
        sql.push_str(&pk_list.join(", "));
        sql.push(')');
    }
    sql.push(')');
    Ok(sql)
}

/// Postgres DESTINATION-mode bookkeeping refresh. Parallel to
/// `sync_sqlite_destination_metadata` / `sync_duckdb_destination_metadata`.
/// Synthesizes a SQLite-parseable CREATE TABLE from `information_schema.columns`
/// + `pg_index` PK info.
fn sync_postgres_destination_metadata(client: &mut PgClient, layout: &ConsolidatorLayout) -> Result<()> {
    let meta = pg_meta_table(layout, "synclite_checkpoint");
    let ctmeta = pg_meta_table(layout, "synclite_consolidator_table_metadata");

    let upd_sql = format!(
        "UPDATE {meta}
         SET initialization_status = 1
         WHERE synclite_device_id = $1 AND synclite_device_name = $2"
    );
    client
        .execute(upd_sql.as_str(), &[&layout.device_id, &layout.device_name])
        .map_err(|e| map_pg_err_with_sql(e, &upd_sql))?;

    let del_sql = format!(
        "DELETE FROM {ctmeta} WHERE synclite_device_id = $1 AND synclite_device_name = $2 AND prop_key = 'create_sql'"
    );
    client
        .execute(del_sql.as_str(), &[&layout.device_id, &layout.device_name])
        .map_err(|e| map_pg_err_with_sql(e, &del_sql))?;

    // Determine scanning schema: explicit dst_schema when scope is on,
    // otherwise current_schema().
    let schema_filter: String = if layout.dst_use_schema_scope_resolution {
        layout
            .dst_schema
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| pg_current_schema(client))
    } else {
        pg_current_schema(client)
    };

    let list_sql = "SELECT table_name FROM information_schema.tables \
                    WHERE table_schema = $1 \
                      AND table_type = 'BASE TABLE' \
                      AND table_name NOT IN ( \
                          'synclite_checkpoint', \
                          'synclite_consolidator_metadata', \
                          'synclite_consolidator_table_metadata' \
                      ) \
                    ORDER BY table_name";
    let rows = client
        .query(list_sql, &[&schema_filter])
        .map_err(|e| map_pg_err_with_sql(e, list_sql))?;
    let tables: Vec<String> = rows
        .iter()
        .filter_map(|r| r.try_get::<_, String>(0).ok())
        .collect();

    let ins_sql = format!(
        "INSERT INTO {ctmeta}(synclite_device_id, synclite_device_name, synclite_update_timestamp, database_name, table_name, prop_key, prop_value) \
         VALUES($1, $2, $3, 'main', $4, 'create_sql', $5)"
    );
    for tbl in tables {
        let create_sql = build_create_sql_from_postgres_columns(client, &schema_filter, &tbl)?;
        let ts = current_update_timestamp();
        client
            .execute(
                ins_sql.as_str(),
                &[&layout.device_id, &layout.device_name, &ts, &tbl, &create_sql],
            )
            .map_err(|e| map_pg_err_with_sql(e, &ins_sql))?;
    }
    Ok(())
}

fn sync_postgres_local_metadata_create_sql(client: &mut PgClient, layout: &ConsolidatorLayout) -> Result<()> {
    let meta_path = layout.consolidator_metadata_path(layout.dst_index);
    let meta_conn = Connection::open(&meta_path).map_err(map_sql_err)?;
    meta_conn
        .execute(
            "DELETE FROM table_metadata WHERE database_name = ?1 AND key = 'create_sql'",
            params![layout.database_name],
        )
        .map_err(map_sql_err)?;

    let schema_filter: String = if layout.dst_use_schema_scope_resolution {
        layout
            .dst_schema
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| pg_current_schema(client))
    } else {
        pg_current_schema(client)
    };

    let list_sql = "SELECT table_name FROM information_schema.tables \
                    WHERE table_schema = $1 \
                      AND table_type = 'BASE TABLE' \
                      AND table_name NOT IN ( \
                          'synclite_checkpoint', \
                          'synclite_consolidator_metadata', \
                          'synclite_consolidator_table_metadata' \
                      ) \
                    ORDER BY table_name";
    let rows = client
        .query(list_sql, &[&schema_filter])
        .map_err(|e| map_pg_err_with_sql(e, list_sql))?;
    let tables: Vec<String> = rows
        .iter()
        .filter_map(|r| r.try_get::<_, String>(0).ok())
        .collect();

    for tbl in tables {
        let create_sql = build_create_sql_from_postgres_columns(client, &schema_filter, &tbl)?;
        consolidator_state::upsert_consolidator_table_metadata(
            layout,
            layout.dst_index,
            &layout.database_name,
            "",
            &tbl,
            "create_sql",
            &create_sql,
        )?;
    }
    Ok(())
}

fn pg_current_schema(client: &mut PgClient) -> String {
    client
        .query_one("SELECT current_schema()", &[])
        .ok()
        .and_then(|r| r.try_get::<_, String>(0).ok())
        .unwrap_or_else(|| "public".to_string())
}

fn build_create_sql_from_postgres_columns(
    client: &mut PgClient,
    schema: &str,
    table: &str,
) -> Result<String> {
    let col_sql = "SELECT column_name, data_type, is_nullable \
                   FROM information_schema.columns \
                   WHERE table_schema = $1 AND table_name = $2 \
                   ORDER BY ordinal_position";
    let col_rows = client
        .query(col_sql, &[&schema, &table])
        .map_err(|e| map_pg_err_with_sql(e, col_sql))?;
    let cols: Vec<(String, String, bool)> = col_rows
        .iter()
        .filter_map(|r| {
            let name: String = r.try_get(0).ok()?;
            let ty: String = r.try_get(1).ok()?;
            let nullable: String = r.try_get(2).ok()?;
            Some((name, ty, nullable.eq_ignore_ascii_case("NO")))
        })
        .collect();

    let pk_sql = "SELECT a.attname \
                  FROM pg_index i \
                  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
                  JOIN pg_class c ON c.oid = i.indrelid \
                  JOIN pg_namespace n ON n.oid = c.relnamespace \
                  WHERE i.indisprimary AND c.relname = $1 AND n.nspname = $2 \
                  ORDER BY array_position(i.indkey, a.attnum)";
    let pk_rows = client
        .query(pk_sql, &[&table, &schema])
        .map_err(|e| map_pg_err_with_sql(e, pk_sql))?;
    let pks: Vec<String> = pk_rows
        .iter()
        .filter_map(|r| r.try_get::<_, String>(0).ok())
        .collect();

    let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (", table);
    let mut first = true;
    for (name, ty, nn) in &cols {
        if !first {
            sql.push_str(", ");
        }
        first = false;
        sql.push_str(name);
        sql.push(' ');
        sql.push_str(ty);
        if *nn {
            sql.push_str(" NOT NULL");
        }
    }
    if !pks.is_empty() {
        sql.push_str(", PRIMARY KEY(");
        sql.push_str(&pks.join(", "));
        sql.push(')');
    }
    sql.push(')');
    Ok(sql)
}

fn apply_to_postgres_destination(
    layout: &ConsolidatorLayout,
    first_commit_id: i64,
    entries: &[SegmentEntry],
) -> Result<ApplyProgress> {
    tracing::info!(
        dst_alias = %layout.dst_alias,
        dst_index = layout.dst_index,
        predicate_opt = layout.dst_oper_predicate_optimization,
        unparsable_to_null = layout.dst_set_unparsable_values_to_null,
        entries = entries.len(),
        "applying batch to Postgres destination"
    );
    let conn_str = build_postgres_connection_string(layout);
    let mut client = PgClient::connect(&conn_str, NoTls).map_err(map_pg_err)?;
    if layout.metadata_store == MetadataStore::Destination {
        ensure_postgres_metadata_seeded(&mut client, layout, first_commit_id)?;
    }
    client.batch_execute("BEGIN").map_err(map_pg_err)?;

    let mut progress = ApplyProgress {
        applied_change: -1,
        applied_commit: first_commit_id,
        applied_txn_cnt: 0,
    };

    let sql_generator = DestinationSqlGenerator::new(layout.dst_type);
    let mut mapped_cache: HashMap<MappedOperationKey, CachedMappedOperation> = HashMap::new();
    let mut active_batch_key: Option<MappedOperationKey> = None;
    let mut pk_cache: PkCache = HashMap::new();
    let mut batch_count = 0usize;
    let mut pending_sql: Vec<(String, bool, i64)> = Vec::new();
    let mut tables_touched: HashSet<String> = HashSet::new();

    for entry in entries {
        let mapped = match map_operation_using_mapper(layout, entry) {
            ApplyAction::Skip => continue,
            ApplyAction::Execute(mapped) => mapped,
        };

        let key = MappedOperationKey {
            mapper: mapped.mapper,
            kind: mapped.kind,
            table_name: mapped.table_name.clone(),
            sql: mapped.sql.clone(),
        };
        let batch_limit = batch_limit_for_kind(layout, mapped.kind);
        let should_rotate = active_batch_key.as_ref() != Some(&key) || batch_count >= batch_limit;
        if should_rotate {
            flush_postgres_batch(&mut client, layout, &mut pending_sql)?;
            active_batch_key = Some(key.clone());
            batch_count = 0;
        }

        let cached = mapped_cache.entry(key).or_insert_with(|| CachedMappedOperation {
            sql: mapped.sql.clone(),
            reusable_args: Vec::with_capacity(mapped.args.len().max(8)),
        });
        cached.reusable_args.clear();
        cached.reusable_args.extend_from_slice(&mapped.args);

        if let Some(tbl) = mapped.table_name.as_deref() {
            if !mapped.is_system_table && mapped.kind != DmlKind::Other {
                tables_touched.insert(tbl.to_ascii_uppercase());
            }
        }

        if mapped.kind == DmlKind::Other {
            // Java parity: drain queued DML first, then execute DDL as a
            // multi-statement script swallowing benign errors.
            flush_postgres_batch(&mut client, layout, &mut pending_sql)?;
            if let Err(e) = apply_ddl_batch_postgres(&mut client, &cached.sql) {
                let _ = client.batch_execute("ROLLBACK");
                return Err(e);
            }
            if let Some(tbl) = mapped.table_name.as_deref() {
                pk_cache.remove(&tbl.to_ascii_lowercase());
            }
        } else if mapped.kind == DmlKind::Insert
            && layout.dst_idempotent_data_ingestion
            && !mapped.is_system_table
        {
            let table = mapped.table_name.as_deref().unwrap_or("");
            let pks = if table.is_empty() {
                Vec::new()
            } else {
                lookup_pks_postgres(&mut client, &mut pk_cache, table)
            };
            let stmts = rewrite_insert_idempotent(
                layout,
                DstType::Postgres,
                layout.dst_idempotent_data_ingestion_method,
                &cached.sql,
                &cached.reusable_args,
                &pks,
            );
            for (s, a) in stmts {
                let rendered = render_sql_with_args(&s, &a);
                pending_sql.push((rendered, true, mapped.change_number));
            }
        } else {
            let rendered_sql = match sql_generator.generate(cached) {
                GeneratedSql::Text(sql) => sql,
                GeneratedSql::Parameterized { .. } => unreachable!("postgres path must use rendered SQL"),
            };
            pending_sql.push((rendered_sql, mapped.kind == DmlKind::Insert, mapped.change_number));
        }

        progress.applied_change = mapped.change_number;
        progress.applied_commit = mapped.commit_id;
        progress.applied_txn_cnt += 1;
        batch_count += 1;
    }

    flush_postgres_batch(&mut client, layout, &mut pending_sql)?;

    // Java parity: run dst_triggers SQL for every table touched by this batch.
    if !layout.dst_triggers.is_empty() {
        for tbl in &tables_touched {
            if let Some(stmts) = layout.dst_triggers.get(tbl) {
                tracing::info!(table = %tbl, trigger_count = stmts.len(), "executing dst_triggers (postgres)");
                for s in stmts {
                    tracing::debug!(table = %tbl, sql = %s, "executing dst_trigger statement");
                    if let Err(e) = client.batch_execute(s) {
                        tracing::error!(table = %tbl, error = %e, sql = %s, "dst_trigger execution failed (postgres)");
                        let _ = client.batch_execute("ROLLBACK");
                        return Err(map_pg_err(e));
                    }
                }
            }
        }
    }

    client.batch_execute("COMMIT").map_err(map_pg_err)?;
    if layout.metadata_store == MetadataStore::Destination {
        sync_postgres_destination_metadata(&mut client, layout)?;
    } else {
        sync_postgres_local_metadata_create_sql(&mut client, layout)?;
    }
    Ok(progress)
}

fn flush_postgres_batch(
    client: &mut PgClient,
    layout: &ConsolidatorLayout,
    pending_sql: &mut Vec<(String, bool, i64)>,
) -> Result<()> {
    if pending_sql.is_empty() {
        return Ok(());
    }

    let mut batch_sql = String::new();
    for (sql, _, _) in pending_sql.iter() {
        batch_sql.push_str(sql);
        if !sql.trim_end().ends_with(';') {
            batch_sql.push(';');
        }
    }

    // Wrap the consolidated batch in a SAVEPOINT so that on failure we can
    // roll back just the batch (leaving the outer apply BEGIN/COMMIT live)
    // and retry per-statement. Without this, the failed batch leaves the
    // txn in aborted state and every per-statement retry below returns
    // 25P02, masking the real error.
    client.batch_execute("SAVEPOINT synclite_flush_sp").map_err(map_pg_err)?;
    match client.batch_execute(&batch_sql) {
        Ok(()) => {
            client
                .batch_execute("RELEASE SAVEPOINT synclite_flush_sp")
                .map_err(map_pg_err)?;
            pending_sql.clear();
            return Ok(());
        }
        Err(_) => {
            client
                .batch_execute("ROLLBACK TO SAVEPOINT synclite_flush_sp")
                .map_err(map_pg_err)?;
            client
                .batch_execute("RELEASE SAVEPOINT synclite_flush_sp")
                .map_err(map_pg_err)?;
        }
    }

    for (sql, is_insert, change_number) in pending_sql.iter() {
        with_device_tracer(|t| {
            tracer_info!(t, "JDBCExecutor", "PG DML : {}", sql)
        });
        client
            .batch_execute("SAVEPOINT synclite_flush_stmt_sp")
            .map_err(map_pg_err)?;
        match client.batch_execute(sql) {
            Ok(()) => {
                client
                    .batch_execute("RELEASE SAVEPOINT synclite_flush_stmt_sp")
                    .map_err(map_pg_err)?;
            }
            Err(e) => {
                client
                    .batch_execute("ROLLBACK TO SAVEPOINT synclite_flush_stmt_sp")
                    .map_err(map_pg_err)?;
                client
                    .batch_execute("RELEASE SAVEPOINT synclite_flush_stmt_sp")
                    .map_err(map_pg_err)?;

                let is_unique_violation = e.code().map(|c| c.code() == "23505").unwrap_or(false);
                if layout.dst_idempotent_data_ingestion && *is_insert && is_unique_violation {
                    tracing::warn!(change_number = change_number, "idempotent ingest ignored duplicate key insert");
                    continue;
                }
                // Java parity: dst-set-unparsable-values-to-null — when a value
                // cannot be coerced to its column type the row is dropped (with
                // a warning) instead of failing the whole batch. SQLSTATE
                // class 22 covers data exceptions (invalid_text_representation,
                // numeric_value_out_of_range, datetime_field_overflow, etc.).
                if layout.dst_set_unparsable_values_to_null {
                    let is_data_exception = e
                        .code()
                        .map(|c| c.code().starts_with("22"))
                        .unwrap_or(false);
                    if is_data_exception {
                        tracing::warn!(
                            change_number = change_number,
                            sqlstate = e.code().map(|c| c.code()).unwrap_or("?"),
                            "dropping row with unparsable value (dst-set-unparsable-values-to-null)"
                        );
                        continue;
                    }
                }
                return Err(map_pg_err_with_sql(e, sql));
            }
        }
    }

    pending_sql.clear();
    Ok(())
}

fn ensure_sqlite_metadata_seeded(dest_conn: &Connection, layout: &ConsolidatorLayout, initial_commit_id: i64) -> Result<()> {
    dest_conn
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS synclite_checkpoint(\n\
                 synclite_device_id TEXT NOT NULL,\n\
                 synclite_device_name TEXT NOT NULL,\n\
                 synclite_update_timestamp TEXT,\n\
                 commit_id LONG NOT NULL,\n\
                 cdc_change_number LONG NOT NULL,\n\
                 cdc_log_segment_sequence_number LONG NOT NULL,\n\
                 initialization_status INTEGER NOT NULL DEFAULT 0,\n\
                 txn_count LONG NOT NULL,\n\
                 PRIMARY KEY(synclite_device_id, synclite_device_name, commit_id)\n\
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
    let has_row: i64 = dest_conn
        .query_row(
            "SELECT COUNT(*) FROM synclite_checkpoint WHERE synclite_device_id = ?1 AND synclite_device_name = ?2",
            params![layout.device_id, layout.device_name],
            |row| row.get(0),
        )
        .map_err(map_sql_err)?;
    if has_row == 0 {
        dest_conn
            .execute(
                "INSERT INTO synclite_checkpoint(\n\
                     synclite_device_id, synclite_device_name, synclite_update_timestamp,\n\
                     commit_id, cdc_change_number,\n\
                     cdc_log_segment_sequence_number, initialization_status, txn_count\n\
                 ) VALUES(?1, ?2, ?3, ?4, -1, 0, 0, 0)",
                params![layout.device_id, layout.device_name, current_update_timestamp(), initial_commit_id],
            )
            .map_err(map_sql_err)?;
    }
    // Seed synclite_metadata_version row if absent (Java parity with
    // ConsolidatorMetadataManager.seedDstMetadataVersionIfAbsent). Idempotent.
    let version_present: i64 = dest_conn
        .query_row(
            "SELECT COUNT(*) FROM synclite_consolidator_metadata \
             WHERE synclite_device_id = ?1 AND synclite_device_name = ?2 AND prop_key = ?3",
            params![layout.device_id, layout.device_name, SYNCLITE_METADATA_VERSION_KEY],
            |row| row.get(0),
        )
        .map_err(map_sql_err)?;
    if version_present == 0 {
        dest_conn
            .execute(
                "INSERT INTO synclite_consolidator_metadata(\
                     synclite_device_id, synclite_device_name, synclite_update_timestamp, prop_key, prop_value\
                 ) VALUES(?1, ?2, ?3, ?4, ?5)",
                params![
                    layout.device_id,
                    layout.device_name,
                    current_update_timestamp(),
                    SYNCLITE_METADATA_VERSION_KEY,
                    SYNCLITE_METADATA_VERSION.to_string(),
                ],
            )
            .map_err(map_sql_err)?;
    }
    Ok(())
}

fn ensure_duckdb_metadata_seeded(dest_conn: &DuckConnection, layout: &ConsolidatorLayout, initial_commit_id: i64) -> Result<()> {
    dest_conn
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS synclite_checkpoint(\n\
                 synclite_device_id VARCHAR NOT NULL,\n\
                 synclite_device_name VARCHAR NOT NULL,\n\
                 synclite_update_timestamp VARCHAR,\n\
                 commit_id BIGINT NOT NULL,\n\
                 cdc_change_number BIGINT NOT NULL,\n\
                 cdc_log_segment_sequence_number BIGINT NOT NULL,\n\
                 initialization_status INTEGER NOT NULL DEFAULT 0,\n\
                 txn_count BIGINT NOT NULL,\n\
                 PRIMARY KEY(synclite_device_id, synclite_device_name, commit_id)\n\
             );\n\
             CREATE TABLE IF NOT EXISTS synclite_consolidator_metadata(\n\
                 synclite_device_id VARCHAR NOT NULL,\n\
                 synclite_device_name VARCHAR NOT NULL,\n\
                 synclite_update_timestamp TEXT,\n\
                 prop_key VARCHAR NOT NULL,\n\
                 prop_value TEXT,\n\
                 PRIMARY KEY(synclite_device_id, synclite_device_name, prop_key)\n\
             );\n\
             CREATE TABLE IF NOT EXISTS synclite_consolidator_table_metadata(\n\
                 synclite_device_id VARCHAR NOT NULL,\n\
                 synclite_device_name VARCHAR NOT NULL,\n\
                 synclite_update_timestamp TEXT,\n\
                 database_name VARCHAR NOT NULL,\n\
                 table_name VARCHAR NOT NULL,\n\
                 prop_key VARCHAR NOT NULL,\n\
                 prop_value TEXT,\n\
                 PRIMARY KEY(synclite_device_id, synclite_device_name, database_name, table_name, prop_key)\n\
             );",
        )
        .map_err(map_duck_err)?;
    let has_row: i64 = dest_conn
        .query_row(
            "SELECT COUNT(*) FROM synclite_checkpoint WHERE synclite_device_id = ? AND synclite_device_name = ?",
            duck_params_from_iter([DuckValue::Text(layout.device_id.clone()), DuckValue::Text(layout.device_name.clone())].iter()),
            |row| row.get(0),
        )
        .map_err(map_duck_err)?;
    if has_row == 0 {
        let vals = [
            DuckValue::Text(layout.device_id.clone()),
            DuckValue::Text(layout.device_name.clone()),
            DuckValue::Text(current_update_timestamp()),
            DuckValue::BigInt(initial_commit_id),
        ];
        duck_exec_with_params(
            dest_conn,
            "INSERT INTO synclite_checkpoint(\n\
                 synclite_device_id, synclite_device_name, synclite_update_timestamp,\n\
                 commit_id, cdc_change_number,\n\
                 cdc_log_segment_sequence_number, initialization_status, txn_count\n\
             ) VALUES(?, ?, ?, ?, -1, 0, 0, 0)",
            &vals,
        )?;
    }
    // Seed synclite_metadata_version row if absent (Java parity).
    let version_present: i64 = {
        let mut stmt = dest_conn
            .prepare(
                "SELECT COUNT(*) FROM synclite_consolidator_metadata \
                 WHERE synclite_device_id = ? AND synclite_device_name = ? AND prop_key = ?",
            )
            .map_err(map_duck_err)?;
        let mut rows = stmt
            .query(duck_params_from_iter(
                [
                    DuckValue::Text(layout.device_id.clone()),
                    DuckValue::Text(layout.device_name.clone()),
                    DuckValue::Text(SYNCLITE_METADATA_VERSION_KEY.to_string()),
                ]
                .iter(),
            ))
            .map_err(map_duck_err)?;
        match rows.next().map_err(map_duck_err)? {
            Some(row) => row.get(0).map_err(map_duck_err)?,
            None => 0,
        }
    };
    if version_present == 0 {
        duck_exec_with_params(
            dest_conn,
            "INSERT INTO synclite_consolidator_metadata(\
                 synclite_device_id, synclite_device_name, synclite_update_timestamp, prop_key, prop_value\
             ) VALUES(?, ?, ?, ?, ?)",
            &[
                DuckValue::Text(layout.device_id.clone()),
                DuckValue::Text(layout.device_name.clone()),
                DuckValue::Text(current_update_timestamp()),
                DuckValue::Text(SYNCLITE_METADATA_VERSION_KEY.to_string()),
                DuckValue::Text(SYNCLITE_METADATA_VERSION.to_string()),
            ],
        )?;
    }
    Ok(())
}

fn ensure_postgres_metadata_seeded(client: &mut PgClient, layout: &ConsolidatorLayout, initial_commit_id: i64) -> Result<()> {
    let meta = pg_meta_table(layout, "synclite_checkpoint");
    let cmeta = pg_meta_table(layout, "synclite_consolidator_metadata");
    let ctmeta = pg_meta_table(layout, "synclite_consolidator_table_metadata");
    let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {meta}(\n\
                 synclite_device_id VARCHAR(36) NOT NULL,\n\
                 synclite_device_name VARCHAR(255) NOT NULL,\n\
                 synclite_update_timestamp VARCHAR(255),\n\
                 commit_id BIGINT NOT NULL,\n\
                 cdc_change_number BIGINT NOT NULL,\n\
                 cdc_log_segment_sequence_number BIGINT NOT NULL,\n\
                 initialization_status INTEGER NOT NULL DEFAULT 0,\n\
                 txn_count BIGINT NOT NULL,\n\
                 PRIMARY KEY(synclite_device_id, synclite_device_name, commit_id)\n\
             );\n\
             CREATE TABLE IF NOT EXISTS {cmeta}(\n\
                 synclite_device_id VARCHAR(64) NOT NULL,\n\
                 synclite_device_name VARCHAR(255) NOT NULL,\n\
                 synclite_update_timestamp TEXT,\n\
                 prop_key VARCHAR(255) NOT NULL,\n\
                 prop_value TEXT,\n\
                 PRIMARY KEY(synclite_device_id, synclite_device_name, prop_key)\n\
             );\n\
             CREATE TABLE IF NOT EXISTS {ctmeta}(\n\
                 synclite_device_id VARCHAR(64) NOT NULL,\n\
                 synclite_device_name VARCHAR(255) NOT NULL,\n\
                 synclite_update_timestamp TEXT,\n\
                 database_name VARCHAR(255) NOT NULL,\n\
                 table_name VARCHAR(255) NOT NULL,\n\
                 prop_key VARCHAR(255) NOT NULL,\n\
                 prop_value TEXT,\n\
                 PRIMARY KEY(synclite_device_id, synclite_device_name, database_name, table_name, prop_key)\n\
             );");
    with_device_tracer(|t| tracer_info!(t, "JDBCExecutor", "PG metadata DDL : {}", ddl));
    client.batch_execute(&ddl).map_err(|e| map_pg_err_with_sql(e, &ddl))?;
    // Idempotent column-type promotion: earlier builds may have created
    // these bookkeeping tables with int4 columns. Rust binds i64 (Java
    // BIGINT parity), so promote int4 -> int8 in place. ALTER COLUMN
    // TYPE BIGINT is a no-op when the column is already BIGINT.
    let promote_sql = format!(
        "ALTER TABLE {meta} ALTER COLUMN commit_id TYPE BIGINT USING commit_id::bigint;\n\
         ALTER TABLE {meta} ALTER COLUMN cdc_change_number TYPE BIGINT USING cdc_change_number::bigint;\n\
         ALTER TABLE {meta} ALTER COLUMN cdc_log_segment_sequence_number TYPE BIGINT USING cdc_log_segment_sequence_number::bigint;\n\
         ALTER TABLE {meta} ALTER COLUMN txn_count TYPE BIGINT USING txn_count::bigint;"
    );
    with_device_tracer(|t| tracer_info!(t, "JDBCExecutor", "PG metadata promote : {}", promote_sql));
    if let Err(e) = client.batch_execute(&promote_sql) {
        with_device_tracer(|t| tracer_info!(
            t,
            "JDBCExecutor",
            "PG metadata column promote failed (continuing): {}",
            e
        ));
    } else {
        with_device_tracer(|t| tracer_info!(t, "JDBCExecutor", "PG metadata promote OK on {}", meta));
    }
    let count_sql = format!(
        "SELECT COUNT(*) FROM {meta} WHERE synclite_device_id = $1 AND synclite_device_name = $2"
    );
    let count: i64 = client
        .query_one(&count_sql, &[&layout.device_id, &layout.device_name])
        .map_err(|e| map_pg_err_with_sql(e, &count_sql))?
        .get(0);
    if count == 0 {
        let insert_sql = format!(
            "INSERT INTO {meta}(\n\
                 synclite_device_id, synclite_device_name, synclite_update_timestamp,\n\
                 commit_id, cdc_change_number,\n\
                 cdc_log_segment_sequence_number, initialization_status, txn_count\n\
             ) VALUES($1, $2, $3, $4, -1, 0, 0, 0)"
        );
        client
            .execute(
                insert_sql.as_str(),
                &[&layout.device_id, &layout.device_name, &current_update_timestamp(), &initial_commit_id],
            )
            .map_err(|e| map_pg_err_with_sql(e, &insert_sql))?;
    }
    // Seed synclite_metadata_version row if absent (Java parity).
    let version_count_sql = format!(
        "SELECT COUNT(*) FROM {cmeta} WHERE synclite_device_id = $1 AND synclite_device_name = $2 AND prop_key = $3"
    );
    let version_key: String = SYNCLITE_METADATA_VERSION_KEY.to_string();
    let version_value: String = SYNCLITE_METADATA_VERSION.to_string();
    let version_count: i64 = client
        .query_one(
            version_count_sql.as_str(),
            &[&layout.device_id, &layout.device_name, &version_key],
        )
        .map_err(|e| map_pg_err_with_sql(e, &version_count_sql))?
        .get(0);
    if version_count == 0 {
        let version_insert_sql = format!(
            "INSERT INTO {cmeta}(synclite_device_id, synclite_device_name, synclite_update_timestamp, prop_key, prop_value) VALUES($1, $2, $3, $4, $5)"
        );
        client
            .execute(
                version_insert_sql.as_str(),
                &[
                    &layout.device_id,
                    &layout.device_name,
                    &current_update_timestamp(),
                    &version_key,
                    &version_value,
                ],
            )
            .map_err(|e| map_pg_err_with_sql(e, &version_insert_sql))?;
    }
    Ok(())
}

fn sqlvalue_to_duckvalue(v: &SqlValue) -> DuckValue {
    match v {
        SqlValue::Null => DuckValue::Null,
        SqlValue::Integer(i) => DuckValue::BigInt(*i),
        SqlValue::Real(f) => DuckValue::Double(*f),
        SqlValue::Text(t) => DuckValue::Text(t.clone()),
        SqlValue::Blob(b) => DuckValue::Blob(b.clone()),
    }
}

fn duck_exec_with_params(conn: &DuckConnection, sql: &str, vals: &[DuckValue]) -> Result<()> {
    let mut stmt = conn.prepare(sql).map_err(map_duck_err)?;
    let mut rows = stmt
        .query(duck_params_from_iter(vals.iter()))
        .map_err(map_duck_err)?;
    while rows.next().map_err(map_duck_err)?.is_some() {
        // Drain to fully execute statement.
    }
    Ok(())
}

fn is_duckdb_duplicate_insert_err(e: &Error) -> bool {
    let msg = e.to_string().to_ascii_lowercase();
    msg.contains("duplicate") || msg.contains("constraint") || msg.contains("unique")
}

/// Java parity: `dst-set-unparsable-values-to-null`. SQLite reports type
/// coercion failures as "datatype mismatch"; CHECK / NOT NULL failures on
/// values that could not be parsed surface as "constraint failed".
fn is_sqlite_unparsable_err(e: &rusqlite::Error) -> bool {
    let msg = e.to_string().to_ascii_lowercase();
    msg.contains("datatype mismatch")
        || msg.contains("data type mismatch")
        || msg.contains("invalid value")
}

/// Java parity: DuckDB raises `Conversion Error` / `Invalid Input Error`
/// for unparseable casts (e.g. `'abc'::INTEGER`). Distinct from duplicate
/// key, which `is_duckdb_duplicate_insert_err` already covers.
fn is_duckdb_unparsable_err(e: &Error) -> bool {
    let msg = e.to_string().to_ascii_lowercase();
    msg.contains("conversion error")
        || msg.contains("invalid input")
        || msg.contains("could not convert")
        || msg.contains("out of range")
}

/// Java parity: `TableMapper.getMappedInsertOper`. Rewrites a parameterized
/// `INSERT INTO t (cols) VALUES (?, ?, …)` into the configured idempotent
/// form when `dst_idempotent_data_ingestion=true` and the destination
/// table has primary-key columns known to the consolidator.
///
/// Returns a `Vec` because `DELETE_INSERT` produces two statements that
/// must execute as a pair. Each element shares the same args slice with
/// pre-computed slice ranges — for safety we materialize new `Vec<SqlValue>`
/// per statement.
fn warn_missing_pk_once(
    dst_type: DstType,
    method: DstIdempotentDataIngestionMethod,
    table: &str,
) {
    static MISSING_PK_WARNED: Mutex<Option<HashSet<String>>> = Mutex::new(None);
    let key = format!("{:?}::{}", dst_type, table.to_ascii_lowercase());
    let mut guard = match MISSING_PK_WARNED.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let set = guard.get_or_insert_with(HashSet::new);
    if set.insert(key) {
        tracing::warn!(
            ?dst_type,
            ?method,
            table = %table,
            "dst-idempotent-data-ingestion is enabled but destination table has no primary key; \
             falling back to plain INSERT. Re-ingesting the same source rows will produce duplicates. \
             Declare a primary key on the source table to enable the configured idempotent method."
        );
    }
}

fn rewrite_insert_idempotent(
    layout: &ConsolidatorLayout,
    dst_type: DstType,
    method: DstIdempotentDataIngestionMethod,
    sql: &str,
    args: &[SqlValue],
    pks: &[String],
) -> Vec<(String, Vec<SqlValue>)> {
    let fallback = || vec![(sql.to_string(), args.to_vec())];
    let parsed = parse_insert_sql(sql);
    if pks.is_empty() {
        // Java parity (`TableMapper.warnMissingPrimaryKeyOnce`): when the
        // user enabled `dst-idempotent-data-ingestion` but the destination
        // table has no primary key, plain INSERT will silently duplicate
        // rows on replay. Emit a one-shot WARN per (dst_type, table) so
        // operators notice without spamming every record.
        let table_for_warn = parsed
            .as_ref()
            .map(|(t, _)| t.as_str())
            .unwrap_or("<unknown>");
        warn_missing_pk_once(dst_type, method, table_for_warn);
        return fallback();
    }
    let Some((table, cols)) = parsed else {
        tracing::debug!(?dst_type, ?method, sql = %sql, "idempotent ingest skipped: INSERT lacks explicit column list");
        return fallback();
    };
    if cols.len() != args.len() {
        tracing::debug!(?dst_type, ?method, cols = cols.len(), args = args.len(), "idempotent ingest skipped: column/arg count mismatch");
        return fallback();
    }

    // Resolve PK column positions in the INSERT column list.
    let mut pk_positions: Vec<usize> = Vec::with_capacity(pks.len());
    for pk in pks {
        match cols.iter().position(|c| c.eq_ignore_ascii_case(pk)) {
            Some(i) => pk_positions.push(i),
            None => {
                tracing::debug!(?dst_type, ?method, table = %table, missing_pk = %pk, "idempotent ingest skipped: PK column not in INSERT");
                return fallback(); // PK column not in INSERT → cannot rewrite safely
            }
        }
    }
    let pk_args: Vec<SqlValue> = pk_positions.iter().map(|i| args[*i].clone()).collect();

    let qtable = qualify_table(layout, &table);
    let qcols: Vec<String> = cols.iter().map(|c| quote_col(layout, c)).collect();
    let qpks: Vec<String> = pks.iter().map(|c| quote_col(layout, c)).collect();

    // Helper: SET clause skipping PK columns.
    let build_set_clause_excl_pk = |alias: &str| -> String {
        qcols
            .iter()
            .enumerate()
            .filter(|(i, _)| !pk_positions.contains(i))
            .map(|(_, c)| format!("{c} = {alias}.{c}"))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let pk_list = qpks.join(", ");
    let placeholders = (0..cols.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
    let cols_csv = qcols.join(", ");
    let base_insert = format!("INSERT INTO {qtable} ({cols_csv}) VALUES ({placeholders})");

    let stmts = match (dst_type, method) {
        // NATIVE_UPSERT
        (DstType::Sqlite, DstIdempotentDataIngestionMethod::NativeUpsert)
        | (DstType::DuckDb, DstIdempotentDataIngestionMethod::NativeUpsert)
        | (DstType::Postgres, DstIdempotentDataIngestionMethod::NativeUpsert) => {
            let set = build_set_clause_excl_pk("EXCLUDED");
            let sql = if set.is_empty() {
                // All columns are PKs — UPSERT is a no-op; use DO NOTHING.
                format!("{base_insert} ON CONFLICT ({pk_list}) DO NOTHING")
            } else {
                format!("{base_insert} ON CONFLICT ({pk_list}) DO UPDATE SET {set}")
            };
            vec![(sql, args.to_vec())]
        }
        // NATIVE_REPLACE — SQLite has `INSERT OR REPLACE`. DuckDB and PG
        // do not; Java falls back to DELETE_INSERT in that case.
        (DstType::Sqlite, DstIdempotentDataIngestionMethod::NativeReplace) => {
            let sql = format!(
                "INSERT OR REPLACE INTO {qtable} ({cols_csv}) VALUES ({placeholders})"
            );
            vec![(sql, args.to_vec())]
        }
        (DstType::DuckDb, DstIdempotentDataIngestionMethod::NativeReplace)
        | (DstType::Postgres, DstIdempotentDataIngestionMethod::NativeReplace)
        | (_, DstIdempotentDataIngestionMethod::DeleteInsert) => {
            let where_clause = qpks
                .iter()
                .map(|c| format!("{c} = ?"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let del = format!("DELETE FROM {qtable} WHERE {where_clause}");
            vec![(del, pk_args), (base_insert, args.to_vec())]
        }
    };

    tracing::debug!(
        ?dst_type,
        ?method,
        table = %table,
        pk_cols = ?pks,
        stmt_count = stmts.len(),
        "rewrote INSERT for idempotent ingest"
    );
    stmts
}

/// Java parity: per-destination primary-key cache used to drive
/// `dst-idempotent-data-ingestion-method` SQL rewriting. Keyed by
/// destination table name (ASCII-lowercased); value is the ordered
/// list of PK column names.
type PkCache = HashMap<String, Vec<String>>;

fn lookup_pks_sqlite(conn: &Connection, cache: &mut PkCache, table: &str) -> Vec<String> {
    let key = table.to_ascii_lowercase();
    if let Some(v) = cache.get(&key) {
        return v.clone();
    }
    // PRAGMA table_info returns columns (cid, name, type, notnull, dflt_value, pk).
    let mut out: Vec<(i64, String)> = Vec::new();
    match conn.prepare(&format!("PRAGMA table_info({table})")) {
        Ok(mut stmt) => {
            if let Ok(mut rows) = stmt.query([]) {
                while let Ok(Some(row)) = rows.next() {
                    let name: String = row.get(1).unwrap_or_default();
                    let pk: i64 = row.get(5).unwrap_or(0);
                    if pk > 0 {
                        out.push((pk, name));
                    }
                }
            }
        }
        Err(e) => {
            tracing::error!(table = %table, error = %e, "sqlite PRAGMA table_info failed; idempotent ingest will not rewrite this table");
        }
    }
    out.sort_by_key(|(p, _)| *p);
    let pks: Vec<String> = out.into_iter().map(|(_, n)| n).collect();
    tracing::debug!(table = %table, pks = ?pks, "sqlite PK lookup");
    cache.insert(key, pks.clone());
    pks
}

fn lookup_pks_duckdb(conn: &DuckConnection, cache: &mut PkCache, table: &str) -> Vec<String> {
    let key = table.to_ascii_lowercase();
    if let Some(v) = cache.get(&key) {
        return v.clone();
    }
    // DuckDB supports PRAGMA table_info too; pk column is index 5.
    let mut out: Vec<(i64, String)> = Vec::new();
    let pragma = format!("PRAGMA table_info('{}')", table.replace('\'', "''"));
    match conn.prepare(&pragma) {
        Ok(mut stmt) => {
            if let Ok(mut rows) = stmt.query([]) {
                while let Ok(Some(row)) = rows.next() {
                    let name: String = row.get(1).unwrap_or_default();
                    let pk_flag: bool = row.get(5).unwrap_or(false);
                    if pk_flag {
                        let cid: i64 = row.get(0).unwrap_or(0);
                        out.push((cid, name));
                    }
                }
            }
        }
        Err(e) => {
            tracing::error!(table = %table, error = %e, "duckdb PRAGMA table_info failed; idempotent ingest will not rewrite this table");
        }
    }
    out.sort_by_key(|(p, _)| *p);
    let pks: Vec<String> = out.into_iter().map(|(_, n)| n).collect();
    tracing::debug!(table = %table, pks = ?pks, "duckdb PK lookup");
    cache.insert(key, pks.clone());
    pks
}

fn lookup_pks_postgres(client: &mut PgClient, cache: &mut PkCache, table: &str) -> Vec<String> {
    let key = table.to_ascii_lowercase();
    if let Some(v) = cache.get(&key) {
        return v.clone();
    }
    // Split optional schema. Use `current_schema()` when none provided.
    let (schema_filter, table_filter) = match table.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => ("public".to_string(), table.to_string()),
    };
    let sql = "SELECT a.attname \
               FROM pg_index i \
               JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
               JOIN pg_class c ON c.oid = i.indrelid \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
               WHERE i.indisprimary AND c.relname = $1 AND n.nspname = $2 \
               ORDER BY array_position(i.indkey, a.attnum)";
    // Wrap in a SAVEPOINT: this runs inside the outer apply BEGIN/COMMIT,
    // and any failure (e.g. catalog perms) would otherwise abort the whole
    // transaction (25P02) and mask the real cause for the rest of the
    // segment. Best-effort: if the SAVEPOINT itself can't be set, fall
    // through to the unguarded query (caller already tolerates errors).
    let sp_set = client.batch_execute("SAVEPOINT synclite_pk_sp").is_ok();
    let mut pks: Vec<String> = Vec::new();
    match client.query(sql, &[&table_filter, &schema_filter]) {
        Ok(rows) => {
            if sp_set {
                let _ = client.batch_execute("RELEASE SAVEPOINT synclite_pk_sp");
            }
            for row in rows {
                if let Ok(name) = row.try_get::<_, String>(0) {
                    pks.push(name);
                }
            }
        }
        Err(e) => {
            if sp_set {
                let _ = client.batch_execute("ROLLBACK TO SAVEPOINT synclite_pk_sp");
                let _ = client.batch_execute("RELEASE SAVEPOINT synclite_pk_sp");
            }
            let msg = e.to_string();
            tracing::error!(table = %table, error = %msg, "postgres PK lookup query failed; idempotent ingest will not rewrite this table");
            with_device_tracer(|t| {
                tracer_error!(
                    t,
                    "DeviceConsolidator",
                    "Postgres PK lookup failed for table {}: {}",
                    table,
                    msg
                )
            });
        }
    }
    tracing::debug!(table = %table, pks = ?pks, "postgres PK lookup");
    cache.insert(key, pks.clone());
    pks
}

/// Java parity: DDL apply must tolerate "already-applied" errors so the
/// CREATE TABLE + redundant ADD COLUMN sequence (and DELETE FROM truncate)
/// is idempotent across retries and across multiple devices targeting the
/// same destination table.
fn is_benign_ddl_err_msg(msg: &str) -> bool {
    let l = msg.to_ascii_lowercase();
    l.contains("duplicate column")
        || l.contains("already exists")
        || l.contains("no such column")
}

fn split_ddl_statements(sql: &str) -> impl Iterator<Item = &str> {
    sql.split(";\n").map(str::trim).filter(|s| !s.is_empty())
}

fn apply_ddl_batch_sqlite(conn: &Connection, sql: &str) -> Result<()> {
    for stmt in split_ddl_statements(sql) {
        if let Err(e) = conn.execute(stmt, []) {
            let msg = e.to_string();
            if is_benign_ddl_err_msg(&msg) {
                tracing::warn!(stmt = %stmt, error = %msg, "consolidation DDL: ignored benign error");
                continue;
            }
            return Err(map_sql_err(e));
        }
    }
    Ok(())
}

fn apply_ddl_batch_duckdb(conn: &DuckConnection, sql: &str) -> Result<()> {
    for stmt in split_ddl_statements(sql) {
        if let Err(e) = conn.execute_batch(stmt) {
            let msg = e.to_string();
            if is_benign_ddl_err_msg(&msg) {
                tracing::warn!(stmt = %stmt, error = %msg, "consolidation DDL: ignored benign error");
                continue;
            }
            return Err(map_duck_err(e));
        }
    }
    Ok(())
}

/// Java parity: merge `dst-user`, `dst-password`, and
/// `dst-connection-timeout-s` into the user-supplied
/// `dst_connection_string` when they are not already encoded there.
/// Accepts both `postgresql://` URIs and libpq-style key/value strings.
/// Passwords are never logged.
fn build_postgres_connection_string(layout: &ConsolidatorLayout) -> String {
    let base = layout.dst_connection_string.trim().to_string();
    let lower = base.to_ascii_lowercase();
    let mut out = base.clone();

    let append_param = |s: &mut String, key: &str, value: &str| {
        // Quote spaces / specials for libpq-style; URI form uses query string.
        let needs_quote = value.contains(' ') || value.contains('\'') || value.contains('\\');
        if is_uri_form(s) {
            let sep = if s.contains('?') { '&' } else { '?' };
            // postgres URI encoding: percent-encode minimally.
            let encoded = pg_uri_encode(value);
            s.push(sep);
            s.push_str(key);
            s.push('=');
            s.push_str(&encoded);
        } else {
            if !s.is_empty() {
                s.push(' ');
            }
            s.push_str(key);
            s.push('=');
            if needs_quote {
                let escaped = value.replace('\\', "\\\\").replace('\'', "\\'");
                s.push('\'');
                s.push_str(&escaped);
                s.push('\'');
            } else {
                s.push_str(value);
            }
        }
    };

    if !pg_param_present(&lower, "user") {
        if let Some(u) = layout.dst_user.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            append_param(&mut out, "user", u);
        }
    }
    if !pg_param_present(&lower, "password") {
        if let Some(p) = layout.dst_password.as_deref().filter(|s| !s.is_empty()) {
            append_param(&mut out, "password", p);
        }
    }
    if !pg_param_present(&lower, "connect_timeout") && layout.dst_connection_timeout_s > 0 {
        let secs = layout.dst_connection_timeout_s.to_string();
        append_param(&mut out, "connect_timeout", &secs);
    }

    tracing::debug!(
        user_set = layout.dst_user.is_some(),
        password_set = layout.dst_password.is_some(),
        connect_timeout_s = layout.dst_connection_timeout_s,
        "built postgres connection string"
    );
    out
}

fn is_uri_form(s: &str) -> bool {
    let l = s.to_ascii_lowercase();
    l.starts_with("postgres://") || l.starts_with("postgresql://")
}

fn pg_param_present(lower_conn: &str, key: &str) -> bool {
    // Match `key=` token boundaries in both URI and key/value forms.
    let needle = format!("{key}=");
    if !lower_conn.contains(&needle) {
        return false;
    }
    // Cheap boundary check: preceded by start, space, '?', or '&'.
    for (idx, _) in lower_conn.match_indices(&needle) {
        if idx == 0 {
            return true;
        }
        let prev = lower_conn.as_bytes()[idx - 1];
        if matches!(prev, b' ' | b'?' | b'&') {
            return true;
        }
    }
    false
}

fn pg_uri_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        let safe = matches!(b,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~'
        );
        if safe {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

fn apply_ddl_batch_postgres(client: &mut PgClient, sql: &str) -> Result<()> {
    // Postgres aborts the surrounding transaction on ANY error, so we can't
    // simply `continue` past a benign "already exists" / "duplicate column"
    // DDL the way SQLite/DuckDB allow — subsequent DML in the same txn
    // would then return 25P02 "current transaction is aborted". The sole
    // caller (`apply_to_postgres_destination`) always opens BEGIN before
    // we run, so wrap each DDL in a SAVEPOINT and roll back to it on a
    // benign error to keep the outer txn live.
    for stmt in split_ddl_statements(sql) {
        with_device_tracer(|t| tracer_info!(t, "JDBCExecutor", "PG DDL : {}", stmt));
        client.batch_execute("SAVEPOINT synclite_ddl_sp").map_err(map_pg_err)?;
        match client.batch_execute(stmt) {
            Ok(()) => {
                client
                    .batch_execute("RELEASE SAVEPOINT synclite_ddl_sp")
                    .map_err(map_pg_err)?;
            }
            Err(e) => {
                let sqlstate_ok = e
                    .code()
                    .map(|c| matches!(c.code(), "42701" | "42P07" | "42703"))
                    .unwrap_or(false);
                let msg = e.to_string();
                if sqlstate_ok || is_benign_ddl_err_msg(&msg) {
                    client
                        .batch_execute("ROLLBACK TO SAVEPOINT synclite_ddl_sp")
                        .map_err(map_pg_err)?;
                    client
                        .batch_execute("RELEASE SAVEPOINT synclite_ddl_sp")
                        .map_err(map_pg_err)?;
                    tracing::warn!(stmt = %stmt, error = %msg, "consolidation DDL: ignored benign error");
                    with_device_tracer(|t| {
                        tracer_info!(
                            t,
                            "JDBCExecutor",
                            "PG DDL benign-skip: {} :: {}",
                            stmt,
                            msg
                        )
                    });
                    continue;
                }
                let _ = client.batch_execute("ROLLBACK TO SAVEPOINT synclite_ddl_sp");
                let _ = client.batch_execute("RELEASE SAVEPOINT synclite_ddl_sp");
                with_device_tracer(|t| {
                    tracer_error!(
                        t,
                        "JDBCExecutor",
                        "PG DDL failed: {} :: {}",
                        stmt,
                        msg
                    )
                });
                return Err(map_pg_err(e));
            }
        }
    }
    Ok(())
}

fn render_sql_with_args(sql: &str, args: &[SqlValue]) -> String {
    let mut rendered = String::with_capacity(sql.len() + args.len() * 8);
    let mut arg_idx = 0usize;
    for ch in sql.chars() {
        if ch == '?' && arg_idx < args.len() {
            rendered.push_str(&sql_literal(&args[arg_idx]));
            arg_idx += 1;
        } else {
            rendered.push(ch);
        }
    }
    rendered
}

fn sql_literal(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Real(f) => {
            if f.is_finite() {
                f.to_string()
            } else {
                "NULL".to_string()
            }
        }
        SqlValue::Text(t) => format!("'{}'", t.replace('\\', "\\\\").replace('\'', "''")),
        SqlValue::Blob(b) => {
            let mut hex = String::with_capacity(b.len() * 2);
            for byte in b {
                use std::fmt::Write as _;
                let _ = write!(hex, "{:02x}", byte);
            }
            format!("decode('{}','hex')", hex)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn layout_for_batch_tests() -> ConsolidatorLayout {
        let mut layout = ConsolidatorLayout::new(
            Path::new("work"),
            Some(PathBuf::from("work")),
            "dev-id",
            "dev",
            "sqlite_store",
            "db",
            1,
            true,
            MetadataStore::Destination,
            DstType::Sqlite,
            DestinationSyncMode::Consolidation,
            "dst.db".to_string(),
            3,
            1000,
            true,
            11,
            7,
            5,
            true,
        );
        layout.work_dir = PathBuf::from("work");
        layout.device_data_root = PathBuf::from("work");
        layout.device_work_dir = PathBuf::from("work").join("synclite-dev-id");
        layout.state_db_path = PathBuf::from("state.db");
        layout.stats_db_path = PathBuf::from("stats.db");
        layout.consolidator_stats_db_path = PathBuf::from("consolidator-stats.db");
        layout.all_dst_indexes = vec![1];
        layout
    }

    #[test]
    fn classify_sql_kind_detects_dml_prefixes() {
        assert_eq!(classify_sql_kind("INSERT INTO t VALUES (?)"), DmlKind::Insert);
        assert_eq!(classify_sql_kind("  update t set a = ?"), DmlKind::Update);
        assert_eq!(classify_sql_kind("\nDELETE FROM t WHERE id = ?"), DmlKind::Delete);
        assert_eq!(classify_sql_kind("CREATE TABLE t(x INT)"), DmlKind::Other);
    }

    #[test]
    fn batch_limit_for_sql_uses_configured_thresholds() {
        let layout = layout_for_batch_tests();
        assert_eq!(batch_limit_for_sql(&layout, "INSERT INTO t VALUES (?)"), 11);
        assert_eq!(batch_limit_for_sql(&layout, "UPDATE t SET v=?"), 7);
        assert_eq!(batch_limit_for_sql(&layout, "DELETE FROM t WHERE id=?"), 5);
        assert_eq!(batch_limit_for_sql(&layout, "CREATE TABLE t(x INT)"), 1);
    }

    #[test]
    fn render_sql_with_args_quotes_text_and_nulls() {
        let sql = "INSERT INTO t(a,b,c) VALUES(?,?,?)";
        let args = vec![
            SqlValue::Text("O'Hara".to_string()),
            SqlValue::Integer(42),
            SqlValue::Null,
        ];
        let rendered = render_sql_with_args(sql, &args);
        assert_eq!(
            rendered,
            "INSERT INTO t(a,b,c) VALUES('O''Hara',42,NULL)"
        );
    }

    #[test]
    fn operation_mapper_skips_transaction_control() {
        let entry = SegmentEntry {
            change_number: 1,
            commit_id: 1,
            source: SegmentSource::CommandLog,
            table_name: None,
            op_type: None,
            ddl_columns: Vec::new(),
            sql: "BEGIN".to_string(),
            args: vec![],
        };
        assert!(matches!(
            map_operation_for_destination(DstType::Sqlite, &entry),
            ApplyAction::Skip
        ));
    }

    #[test]
    fn retry_classifier_does_not_retry_syntax_errors() {
        let err = Error::Config("consolidator: sqlite syntax error near SELECT".to_string());
        assert!(!should_retry_apply_error(DstType::Sqlite, &err));
    }

    #[test]
    fn cleanup_keeps_last_applied_and_tracks_last_cleaned_segment() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("synclite-cleanup-test-{nonce}"));
        let stage_dir = root.join("stage");
        let work_dir = root.join("work");
        let state_db = root.join("state.db");

        fs::create_dir_all(&stage_dir).unwrap();
        fs::create_dir_all(&work_dir).unwrap();
        initialize_state_db(&state_db).unwrap();
        let state_conn = Connection::open(&state_db).unwrap();

        for seq in 0..=2 {
            fs::write(stage_dir.join(format!("{seq}.sqllog")), b"x").unwrap();
            fs::write(work_dir.join(format!("{seq}.sqllog")), b"x").unwrap();
        }

        let mut layout = layout_for_batch_tests();
        layout.cleanup_stage_files = true;
        layout.metadata_store = MetadataStore::Local;
        layout.device_work_dir = root.clone();
        layout.all_dst_indexes = vec![layout.dst_index];
        // Java parity: production wires `record_consolidated_cdc_seq` after a
        // successful apply, before cleanup runs. Without it, stage cleanup is
        // gated to -1 (no destination has applied anything).
        consolidator_state::record_consolidated_cdc_seq(&layout, layout.dst_index, 2).unwrap();
        cleanup_processed_sqllog(
            &layout,
            &state_conn,
            &stage_dir.join("2.sqllog"),
            &work_dir.join("2.sqllog"),
        )
        .unwrap();

        assert!(!stage_dir.join("0.sqllog").exists());
        assert!(!stage_dir.join("1.sqllog").exists());
        assert!(stage_dir.join("2.sqllog").exists());
        assert!(!work_dir.join("0.sqllog").exists());
        assert!(!work_dir.join("1.sqllog").exists());
        assert!(work_dir.join("2.sqllog").exists());

        let cleaned = read_device_metadata_i64(&layout, LAST_CLEANED_SEGMENT_KEY, -1);
        assert_eq!(cleaned, 1);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn retain_committed_entries_excludes_rollback_and_unknown_commits() {
        let entries = vec![
            SegmentEntry {
                change_number: 1,
                commit_id: 10,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "BEGIN".to_string(),
                args: vec![],
            },
            SegmentEntry {
                change_number: 2,
                commit_id: 10,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "INSERT INTO t VALUES(?)".to_string(),
                args: vec![SqlValue::Integer(1)],
            },
            SegmentEntry {
                change_number: 3,
                commit_id: 10,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "COMMIT".to_string(),
                args: vec![],
            },
            SegmentEntry {
                change_number: 4,
                commit_id: 11,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "BEGIN".to_string(),
                args: vec![],
            },
            SegmentEntry {
                change_number: 5,
                commit_id: 11,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "INSERT INTO t VALUES(?)".to_string(),
                args: vec![SqlValue::Integer(2)],
            },
            SegmentEntry {
                change_number: 6,
                commit_id: 11,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "ROLLBACK".to_string(),
                args: vec![],
            },
            SegmentEntry {
                change_number: 7,
                commit_id: 12,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "BEGIN".to_string(),
                args: vec![],
            },
            SegmentEntry {
                change_number: 8,
                commit_id: 12,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "INSERT INTO t VALUES(?)".to_string(),
                args: vec![SqlValue::Integer(3)],
            },
        ];

        let kept = retain_committed_entries(entries);
        assert_eq!(kept.len(), 3);
        assert!(kept.iter().all(|e| e.commit_id == 10));
        assert!(kept.iter().any(|e| e.sql.eq_ignore_ascii_case("COMMIT")));
    }

    #[test]
    fn txn_fates_by_commit_id_marks_expected_fates() {
        let entries = vec![
            SegmentEntry {
                change_number: 1,
                commit_id: 1,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "INSERT INTO t VALUES(1)".to_string(),
                args: vec![],
            },
            SegmentEntry {
                change_number: 2,
                commit_id: 1,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "COMMIT".to_string(),
                args: vec![],
            },
            SegmentEntry {
                change_number: 3,
                commit_id: 2,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "INSERT INTO t VALUES(2)".to_string(),
                args: vec![],
            },
            SegmentEntry {
                change_number: 4,
                commit_id: 2,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "ROLLBACK".to_string(),
                args: vec![],
            },
            SegmentEntry {
                change_number: 5,
                commit_id: 3,
                source: SegmentSource::CommandLog,
                table_name: None,
                op_type: None,
                ddl_columns: Vec::new(),
                sql: "INSERT INTO t VALUES(3)".to_string(),
                args: vec![],
            },
        ];

        let fates = txn_fates_by_commit_id(&entries);
        assert_eq!(fates.get(&1), Some(&TxnFate::Commit));
        assert_eq!(fates.get(&2), Some(&TxnFate::Rollback));
        assert_eq!(fates.get(&3), Some(&TxnFate::Unknown));
    }
}



