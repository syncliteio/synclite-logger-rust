use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub mod datatype_mapper;
pub use datatype_mapper::{DataTypeMapper, DstDataTypeMapping};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DstType {
    Sqlite,
    DuckDb,
    Postgres,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationSyncMode {
    Consolidation,
    Replication,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataStore {
    Destination,
    Local,
}

/// Java parity: `com.synclite.consolidator.global.DstObjectInitMode`.
/// Controls how destination tables are initialized when a `CREATE TABLE`
/// CDC op arrives. See `ConsolidationTableMapper.mapOper(CreateTable)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DstObjectInitMode {
    /// Plain `CREATE TABLE` + per-column `ADD COLUMN`, then truncate.
    OverwriteObject,
    /// Plain `CREATE TABLE` + per-column `ADD COLUMN`, then truncate.
    TryCreateDeleteData,
    /// Plain `CREATE TABLE` + per-column `ADD COLUMN`. Default.
    TryCreateAppendData,
    /// Skip CREATE; just truncate existing table.
    DeleteData,
    /// Skip CREATE and TRUNCATE; pure append.
    AppendData,
}

impl DstObjectInitMode {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "OVERWRITE_OBJECT" => Some(Self::OverwriteObject),
            "TRY_CREATE_DELETE_DATA" => Some(Self::TryCreateDeleteData),
            "TRY_CREATE_APPEND_DATA" => Some(Self::TryCreateAppendData),
            "DELETE_DATA" => Some(Self::DeleteData),
            "APPEND_DATA" => Some(Self::AppendData),
            _ => None,
        }
    }
    /// Java parity: emit `CREATE TABLE` + per-column `ADD COLUMN`?
    pub fn emits_create(self) -> bool {
        matches!(
            self,
            Self::OverwriteObject | Self::TryCreateDeleteData | Self::TryCreateAppendData
        )
    }
    /// Java parity: append `TRUNCATE TABLE` (rendered as `DELETE FROM t`)?
    pub fn emits_truncate(self) -> bool {
        matches!(
            self,
            Self::OverwriteObject | Self::TryCreateDeleteData | Self::DeleteData
        )
    }
}

impl Default for DstObjectInitMode {
    fn default() -> Self {
        Self::TryCreateAppendData
    }
}

/// Java parity: `com.synclite.consolidator.global.DstIdempotentDataIngestionMethod`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DstIdempotentDataIngestionMethod {
    /// `ON CONFLICT (pk) DO UPDATE SET ...` style.
    NativeUpsert,
    /// `INSERT OR REPLACE` / `ON CONFLICT (pk) DO REPLACE`.
    NativeReplace,
    /// `DELETE FROM t WHERE pk=?; INSERT INTO t ...`.
    DeleteInsert,
}

impl DstIdempotentDataIngestionMethod {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "NATIVE_UPSERT" => Some(Self::NativeUpsert),
            "NATIVE_REPLACE" => Some(Self::NativeReplace),
            "DELETE_INSERT" => Some(Self::DeleteInsert),
            _ => None,
        }
    }
}

impl Default for DstIdempotentDataIngestionMethod {
    fn default() -> Self {
        Self::NativeUpsert
    }
}

/// Java parity: `com.synclite.consolidator.global.DstDeviceSchemaNamePolicy`.
/// Currently a single-valued enum kept for forward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DstDeviceSchemaNamePolicy {
    SyncLiteDeviceIdAndName,
}

impl DstDeviceSchemaNamePolicy {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "SYNCLITE_DEVICE_ID_AND_NAME" => Some(Self::SyncLiteDeviceIdAndName),
            _ => None,
        }
    }
}

impl Default for DstDeviceSchemaNamePolicy {
    fn default() -> Self {
        Self::SyncLiteDeviceIdAndName
    }
}

#[derive(Debug, Clone)]
pub struct ConsolidatorLayout {
    pub work_dir: PathBuf,
    pub device_data_root: PathBuf,
    pub device_work_dir: PathBuf,
    pub state_db_path: PathBuf,
    pub stats_db_path: PathBuf,
    pub consolidator_stats_db_path: PathBuf,
    pub device_id: String,
    pub device_name: String,
    pub device_type: String,
    pub database_name: String,
    pub dst_index: i32,
    pub destination_apply_enabled: bool,
    pub metadata_store: MetadataStore,
    pub dst_type: DstType,
    pub destination_sync_mode: DestinationSyncMode,
    pub dst_connection_string: String,
    pub dst_oper_retry_count: u32,
    pub dst_oper_retry_interval_ms: u64,
    pub dst_idempotent_data_ingestion: bool,
    pub dst_insert_batch_size: u32,
    pub dst_update_batch_size: u32,
    pub dst_delete_batch_size: u32,
    pub cleanup_stage_files: bool,
    /// Java parity: full set of configured destination indexes for this
    /// device. Used by stage cleanup to compute
    /// `min(last_consolidated_cdc_log_segment_seq_num)` across all
    /// destinations before deleting upload/stage artifacts (mirrors
    /// `Device.getLastConsolidatedLogSegmentSequenceNumber`). Includes the
    /// current `dst_index`.
    pub all_dst_indexes: Vec<i32>,
    /// Java parity: `device-trace-level` config key — one of
    /// `ERROR` / `INFO` / `DEBUG`. `None` means use the default level
    /// (INFO) chosen by the observability layer.
    pub trace_level: Option<String>,
    /// Java parity: `ConfLoader.isAllowedTable`. `None` means no filter rules
    /// are configured (default-allow, matching Java when
    /// `dstTableFilterMapperRules == null`). When `Some`, only listed table
    /// names (case-insensitive) are allowed; everything else is filtered out.
    pub allowed_tables: Option<std::collections::HashSet<String>>,
    /// Java parity: per-destination filter-mapper rules. Mirrors
    /// `dst-enable-filter-mapper-rules-N` + `dst-filter-mapper-rules-file-N`
    /// + `dst-allow-unspecified-tables-N` + `dst-allow-unspecified-columns-N`
    /// (see `ConfLoader.parseFilterMapperRulesFile`,
    /// `isAllowedTable`, `getMappedTableName`, `isAllowedColumn`,
    /// `getMappedColumnName`).
    pub filter_mapper: FilterMapperRules,
    /// Java parity: per-destination value-mapper rules. Mirrors
    /// `dst-enable-value-mapper-N` + `dst-value-mappings-file-N`
    /// (see `ConfLoader.parseValueMappingsFile`, `getMappedValue`).
    pub value_mapper: ValueMapperRules,
    /// Java parity: `dst-data-type-mapping-N` (one of `ALL_TEXT`,
    /// `BEST_EFFORT`, `CUSTOMIZED`, `EXACT`). Default `ALL_TEXT`.
    pub dst_data_type_mapping: DstDataTypeMapping,
    /// Java parity: `map-src-<type>-to-dst-<N>` user override map collected
    /// at config parse time. Keys are already normalized to lowercase, no
    /// whitespace, and the type token (e.g. `"varchar"`,
    /// `"varchar(length)"`, `"decimal(precision,scale)"`). Empty when the
    /// mode is not `CUSTOMIZED` or no overrides were supplied.
    pub user_type_overrides: HashMap<String, String>,
    /// Java parity: `dst-vector-extension-enabled-N`. Postgres-only flag
    /// used by the array best-effort path.
    pub dst_vector_extension_enabled: bool,
    /// Java parity: `dst-object-init-mode-N`. Default `TRY_CREATE_APPEND_DATA`.
    /// Drives `CREATE TABLE` + per-column `ADD COLUMN` + truncate emission
    /// in `ConsolidationTableMapper.mapOper(CreateTable)`.
    pub dst_object_init_mode: DstObjectInitMode,
    /// Java parity: `dst-idempotent-data-ingestion-method-N`.
    /// Only honored when `dst_idempotent_data_ingestion` is `true`.
    pub dst_idempotent_data_ingestion_method: DstIdempotentDataIngestionMethod,
    /// Java parity: `dst-connection-timeout-s-N`. Default 30s.
    pub dst_connection_timeout_s: u32,
    /// Java parity: `dst-skip-failed-log-files-N`. Default `false`.
    pub dst_skip_failed_log_files: bool,
    /// Java parity: `dst-set-unparsable-values-to-null-N`. Default `false`.
    pub dst_set_unparsable_values_to_null: bool,
    /// Java parity: `dst-quote-object-names-N`. Default `false`.
    pub dst_quote_object_names: bool,
    /// Java parity: `dst-quote-column-names-N`. Default `false`.
    pub dst_quote_column_names: bool,
    /// Java parity: `dst-use-catalog-scope-resolution-N`. Default `true`.
    pub dst_use_catalog_scope_resolution: bool,
    /// Java parity: `dst-use-schema-scope-resolution-N`. Default `true`.
    pub dst_use_schema_scope_resolution: bool,
    /// Java parity: `dst-database-N`. Optional target database.
    pub dst_database: Option<String>,
    /// Java parity: `dst-schema-N`. Optional target schema.
    pub dst_schema: Option<String>,
    /// Java parity: `dst-user-N`. Optional.
    pub dst_user: Option<String>,
    /// Java parity: `dst-password-N`. Optional. Never logged.
    pub dst_password: Option<String>,
    /// Java parity: `dst-alias-N`. Default `DB-N`.
    pub dst_alias: String,
    /// Java parity: `dst-type-name-N`. Default derived from dst_type.
    pub dst_type_name: Option<String>,
    /// Java parity: `dst-create-table-suffix-N`. Default empty.
    pub dst_create_table_suffix: String,
    /// Java parity: `dst-oper-predicate-optimization-N`. Default `true`.
    pub dst_oper_predicate_optimization: bool,
    /// Java parity: `dst-device-schema-name-policy-N`. Default
    /// `SYNCLITE_DEVICE_ID_AND_NAME`.
    pub dst_device_schema_name_policy: DstDeviceSchemaNamePolicy,
    /// Java parity: `dst-enable-triggers-N`. Default `false`.
    pub dst_enable_triggers: bool,
    /// Java parity: `dst-triggers-file-N`. Required when enable_triggers.
    pub dst_triggers_file: Option<PathBuf>,
    /// Java parity: trigger SQL parsed from `dst_triggers_file`. Keyed by
    /// table name (case-insensitive ASCII upper). Each entry is a list of
    /// SQL statements to execute after a batch touches that table.
    pub dst_triggers: HashMap<String, Vec<String>>,
    /// Optional pause-sentinel file. When the file exists the worker
    /// buffers incoming staged segments instead of applying them.
    /// On removal the buffered segments are drained in order. The
    /// upstream logger keeps shipping into the stage subdir while
    /// paused.
    pub pause_sentinel: Option<PathBuf>,
}

/// Run `f` against the destination, retrying up to
/// `layout.dst_oper_retry_count` attempts and sleeping
/// `layout.dst_oper_retry_interval_ms` between attempts. Every error
/// is retryable: SyncLite's destination metadata operations
/// (`CREATE TABLE IF NOT EXISTS`, idempotent UPSERTs,
/// `DROP TABLE IF EXISTS`) are idempotent by construction, so the only
/// useful policy is "loop until success or budget exhausted" — which
/// matches Java's `ConsolidatorMetadataManager` catching plain
/// `SQLException`. `f` must be self-contained (re-open the
/// destination connection on each attempt) so that transient
/// network/lock errors get a clean attempt each loop. The
/// per-segment apply path uses a separate `should_retry_apply_error`
/// filter because *data* errors (e.g., PK collisions under
/// non-idempotent ingestion) must not retry forever — do NOT swap
/// `retry_dst` in there.
pub fn retry_dst<T, E, F>(layout: &ConsolidatorLayout, op: &str, f: F) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Display,
{
    retry_op(
        layout.dst_oper_retry_count,
        layout.dst_oper_retry_interval_ms,
        layout.dst_index,
        op,
        f,
    )
}

/// Lower-level retry primitive used by `retry_dst` and by call sites
/// (e.g. device-side reinitialize) that don't have a full
/// `ConsolidatorLayout` available. Same idempotency contract as
/// `retry_dst`: every error is retried, `f` must re-open its
/// connection each attempt.
pub fn retry_op<T, E, F>(
    count: u32,
    interval_ms: u64,
    dst_index: i32,
    op: &str,
    mut f: F,
) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Display,
{
    let max = count.max(1);
    let mut last_err: Option<E> = None;
    for attempt in 1..=max {
        match f() {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt < max {
                    tracing::warn!(
                        op = %op,
                        dst_index = dst_index,
                        attempt,
                        max,
                        error = %e,
                        "destination op failed; retrying after backoff"
                    );
                    last_err = Some(e);
                    std::thread::sleep(std::time::Duration::from_millis(interval_ms));
                } else {
                    tracing::error!(
                        op = %op,
                        dst_index = dst_index,
                        attempts = max,
                        error = %e,
                        "destination op failed after all retries"
                    );
                    return Err(e);
                }
            }
        }
    }
    Err(last_err.expect("retry_op: loop drained without setting last_err"))
}

impl ConsolidatorLayout {
    pub fn new(
        device_home: &Path,
        work_dir_override: Option<PathBuf>,
        device_id: impl Into<String>,
        device_name: impl Into<String>,
        device_type: impl Into<String>,
        database_name: impl Into<String>,
        dst_index: i32,
        destination_apply_enabled: bool,
        metadata_store: MetadataStore,
        dst_type: DstType,
        destination_sync_mode: DestinationSyncMode,
        dst_connection_string: String,
        dst_oper_retry_count: u32,
        dst_oper_retry_interval_ms: u64,
        dst_idempotent_data_ingestion: bool,
        dst_insert_batch_size: u32,
        dst_update_batch_size: u32,
        dst_delete_batch_size: u32,
        cleanup_stage_files: bool,
    ) -> Self {
        let legacy_work_dir = device_home.join("synclite-syncer");
        let work_dir = if let Some(work_dir_override) = work_dir_override {
            work_dir_override
        } else if legacy_work_dir.exists() {
            legacy_work_dir
        } else {
            device_home.join("synclite-consolidator")
        };
        let device_name = device_name.into();
        let device_id = device_id.into();
        let device_work_dir = work_dir.join(format!("synclite-{}-{}", device_name, device_id));
        let state_db_path = match metadata_store {
            MetadataStore::Local => device_work_dir.join(format!("synclite_consolidator_metadata_{}.db", dst_index)),
            MetadataStore::Destination => std::env::temp_dir()
                .join("synclite-consolidator-metadata")
                .join(format!("synclite_consolidator_metadata_{}_{}.db", dst_index, device_id)),
        };
        let stats_db_path = device_work_dir.join("synclite_device_statistics.db");
        let consolidator_stats_db_path = work_dir.join("synclite_consolidator_statistics.db");
        Self {
            work_dir,
            device_data_root: device_home.to_path_buf(),
            device_work_dir,
            state_db_path,
            stats_db_path,
            consolidator_stats_db_path,
            device_id,
            device_name,
            device_type: device_type.into(),
            database_name: database_name.into(),
            dst_index,
            destination_apply_enabled,
            metadata_store,
            dst_type,
            destination_sync_mode,
            dst_connection_string,
            dst_oper_retry_count,
            dst_oper_retry_interval_ms,
            dst_idempotent_data_ingestion,
            dst_insert_batch_size,
            dst_update_batch_size,
            dst_delete_batch_size,
            cleanup_stage_files,
            all_dst_indexes: vec![dst_index],
            trace_level: None,
            allowed_tables: None,
            filter_mapper: FilterMapperRules::disabled(),
            value_mapper: ValueMapperRules::disabled(),
            dst_data_type_mapping: DstDataTypeMapping::default(),
            user_type_overrides: HashMap::new(),
            dst_vector_extension_enabled: false,
            dst_object_init_mode: DstObjectInitMode::default(),
            dst_idempotent_data_ingestion_method: DstIdempotentDataIngestionMethod::default(),
            dst_connection_timeout_s: 30,
            dst_skip_failed_log_files: false,
            dst_set_unparsable_values_to_null: false,
            dst_quote_object_names: false,
            dst_quote_column_names: false,
            dst_use_catalog_scope_resolution: true,
            dst_use_schema_scope_resolution: true,
            dst_database: None,
            dst_schema: None,
            dst_user: None,
            dst_password: None,
            dst_alias: format!("DB-{}", dst_index),
            dst_type_name: None,
            dst_create_table_suffix: String::new(),
            dst_oper_predicate_optimization: true,
            dst_device_schema_name_policy: DstDeviceSchemaNamePolicy::default(),
            dst_enable_triggers: false,
            dst_triggers_file: None,
            dst_triggers: HashMap::new(),
            pause_sentinel: None,
        }
    }

    /// Java parity: factory for the per-destination data-type mapper. Built
    /// from layout-level config so callers do not need to re-thread the
    /// individual fields.
    pub fn data_type_mapper(&self) -> DataTypeMapper {
        DataTypeMapper::new(
            self.dst_type,
            self.dst_data_type_mapping,
            self.user_type_overrides.clone(),
            self.dst_vector_extension_enabled,
        )
    }
}

impl ConsolidatorLayout {
    /// Java parity: `ConfLoader.isAllowedTable`. Returns true when no rules
    /// are configured (matches Java's default-allow); otherwise true only if
    /// the table name appears (case-insensitive) in the configured set.
    pub fn is_table_allowed(&self, table_name: &str) -> bool {
        match &self.allowed_tables {
            None => true,
            Some(set) => set.contains(&table_name.to_ascii_uppercase()),
        }
    }

    /// Path to the per-device identity metadata file. Mirrors Java's
    /// `<rootPath>/synclite_device_metadata.db` (see SyncLiteDeviceInfo).
    pub fn device_metadata_path(&self) -> PathBuf {
        self.device_data_root.join("synclite_device_metadata.db")
    }

    /// Shared transactional-device backup path. Mirrors Java's
    /// `SyncLiteLoggerInfo.getDataBackupPath` (`<dbName>.synclite.backup`).
    pub fn shared_backup_path(&self) -> PathBuf {
        self.device_data_root
            .join(format!("{}.synclite.backup", self.database_name))
    }

    /// Per-destination replica path used by store/streaming (non-transactional)
    /// devices. Mirrors Java's `SyncLiteDeviceInfo.getReplicaPath`
    /// (`<dbName>.<dstIndex>.synclite.backup`). Lives under the per-device
    /// work dir (Java's `rootPath`) so it stays adjacent to the rest of the
    /// consolidator's per-device state.
    pub fn replica_path(&self) -> PathBuf {
        self.device_work_dir.join(format!(
            "{}.{}.synclite.backup",
            self.database_name, self.dst_index
        ))
    }

    /// Whether the device follows Java's transactional-device semantics
    /// (single shared replica across destinations). Matches
    /// `SyncLiteLoggerInfo.isTransactionalDeviceType`.
    pub fn is_transactional_device(&self) -> bool {
        matches!(
            self.device_type.to_ascii_uppercase().as_str(),
            "SQLITE" | "DUCKDB"
        )
    }

    /// Path to the per-destination consolidator metadata file. Mirrors Java's
    /// `<rootPath>/synclite_consolidator_metadata_<dstIndex>.db` (LOCAL mode)
    /// and Java's ephemeral tempdir variant (DESTINATION mode).
    pub fn consolidator_metadata_path(&self, dst_index: i32) -> PathBuf {
        match self.metadata_store {
            MetadataStore::Local => self
                .device_work_dir
                .join(format!("synclite_consolidator_metadata_{}.db", dst_index)),
            MetadataStore::Destination => std::env::temp_dir()
                .join("synclite-consolidator-metadata")
                .join(format!(
                    "synclite_consolidator_metadata_{}_{}.db",
                    dst_index, self.device_id
                )),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ApplyProgress {
    pub applied_change: i64,
    pub applied_commit: i64,
    pub applied_txn_cnt: i64,
}

/// Java parity: per-destination filter-mapper rules.
///
/// Mirrors `ConfLoader.dstTableFilterMapperRules` /
/// `dstColumnFilterMapperRules` /
/// `dstTableToSrcTableMap` /
/// `dstAllowUnspecifiedTables` / `dstAllowUnspecifiedColumns`.
///
/// Java semantics (preserved here):
/// * When rules are NOT loaded (`enabled == false`), every table and column
///   is allowed and names pass through unchanged.
/// * Table-level rule values are `"true"` (allow, no rename), `"false"`
///   (block), or any other string (rename target).
/// * Column-level rule values follow the same convention.
/// * Unspecified tables/columns inherit `allow_unspecified_tables` /
///   `allow_unspecified_columns`.
/// * Keys are stored upper-case (parser uppercases them).
#[derive(Debug, Clone)]
pub struct FilterMapperRules {
    pub enabled: bool,
    pub allow_unspecified_tables: bool,
    pub allow_unspecified_columns: bool,
    pub table_rules: HashMap<String, String>,
    pub column_rules: HashMap<String, HashMap<String, String>>,
    pub dst_to_src_table_map: HashMap<String, String>,
}

impl FilterMapperRules {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            allow_unspecified_tables: false,
            allow_unspecified_columns: false,
            table_rules: HashMap::new(),
            column_rules: HashMap::new(),
            dst_to_src_table_map: HashMap::new(),
        }
    }

    /// Java parity: `ConfLoader.parseFilterMapperRulesFile`.
    /// Format: one `KEY=VALUE` per line. `KEY` is `TABLE` or `TABLE.COLUMN`
    /// (case-insensitive, stored upper-case). Blank lines and `#`-prefixed
    /// comments are skipped. Value `true` means allow, `false` means block,
    /// anything else is a rename target. After parsing, `synclite_metadata`
    /// is always force-allowed.
    pub fn parse_rules_file(
        path: &Path,
        allow_unspecified_tables: bool,
        allow_unspecified_columns: bool,
    ) -> Result<Self, String> {
        let f = File::open(path)
            .map_err(|e| format!("Failed to load configuration file : {} : {}", path.display(), e))?;
        let reader = BufReader::new(f);
        let mut table_rules: HashMap<String, String> = HashMap::new();
        let mut column_rules: HashMap<String, HashMap<String, String>> = HashMap::new();
        let mut dst_to_src_table_map: HashMap<String, String> = HashMap::new();

        for line_res in reader.lines() {
            let line = line_res
                .map_err(|e| format!("Failed to read configuration file : {} : {}", path.display(), e))?;
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let eq_pos = trimmed.find('=').ok_or_else(|| {
                format!("Invalid line in config file {} : {}", path.display(), trimmed)
            })?;
            let key = trimmed[..eq_pos].trim().to_ascii_uppercase();
            let val = trimmed[eq_pos + 1..].trim().to_string();

            let key_tokens: Vec<&str> = key.split('.').collect();
            match key_tokens.len() {
                1 => {
                    table_rules.insert(key.clone(), val.clone());
                    // Mirror Java: only build reverse map when value is a rename target
                    // (i.e., not the literal "true"/"false" markers).
                    if val != "true" && val != "false" {
                        dst_to_src_table_map.insert(val.to_ascii_uppercase(), key);
                    }
                }
                2 => {
                    let entry = column_rules
                        .entry(key_tokens[0].to_string())
                        .or_insert_with(HashMap::new);
                    entry.insert(key_tokens[1].to_string(), val);
                }
                _ => {
                    return Err(format!(
                        "Invalid table/column name specified in : {} : {} in rule : {}",
                        path.display(),
                        key,
                        trimmed
                    ));
                }
            }
        }

        // Java parity: synclite_metadata is always force-allowed.
        table_rules.insert("SYNCLITE_METADATA".to_string(), "true".to_string());

        Ok(Self {
            enabled: true,
            allow_unspecified_tables,
            allow_unspecified_columns,
            table_rules,
            column_rules,
            dst_to_src_table_map,
        })
    }

    /// Java parity: `ConfLoader.isAllowedTable`.
    pub fn is_allowed_table(&self, table_name: &str) -> bool {
        if !self.enabled {
            return true;
        }
        let upper = table_name.to_ascii_uppercase();
        match self.table_rules.get(&upper) {
            None => self.allow_unspecified_tables,
            Some(rule) if rule == "true" => true,
            Some(rule) if rule == "false" => false,
            Some(_rename) => true,
        }
    }

    /// Java parity: `ConfLoader.getMappedTableName`. `None` means blocked.
    pub fn mapped_table_name(&self, table_name: &str) -> Option<String> {
        if !self.enabled {
            return Some(table_name.to_string());
        }
        let upper = table_name.to_ascii_uppercase();
        match self.table_rules.get(&upper) {
            None => {
                if self.allow_unspecified_tables {
                    Some(table_name.to_string())
                } else {
                    None
                }
            }
            Some(rule) if rule == "true" => Some(table_name.to_string()),
            Some(rule) if rule == "false" => None,
            Some(rename) => Some(rename.clone()),
        }
    }

    /// Java parity: `ConfLoader.isAllowedColumn`.
    pub fn is_allowed_column(&self, table_name: &str, column_name: &str) -> bool {
        if !self.enabled {
            return true;
        }
        let table_upper = table_name.to_ascii_uppercase();
        let col_upper = column_name.to_ascii_uppercase();
        let cols = match self.column_rules.get(&table_upper) {
            None => return true, // Java: no column entries for table → allow
            Some(c) => c,
        };
        match cols.get(&col_upper) {
            None => self.allow_unspecified_columns,
            Some(rule) if rule == "true" => true,
            Some(rule) if rule == "false" => false,
            Some(_rename) => true,
        }
    }

    /// Java parity: `ConfLoader.getMappedColumnName`. `None` means blocked.
    pub fn mapped_column_name(&self, table_name: &str, column_name: &str) -> Option<String> {
        if !self.enabled {
            return Some(column_name.to_string());
        }
        let table_upper = table_name.to_ascii_uppercase();
        let col_upper = column_name.to_ascii_uppercase();
        let cols = match self.column_rules.get(&table_upper) {
            None => return Some(column_name.to_string()),
            Some(c) => c,
        };
        match cols.get(&col_upper) {
            None => {
                if self.allow_unspecified_columns {
                    Some(column_name.to_string())
                } else {
                    None
                }
            }
            Some(rule) if rule == "true" => Some(column_name.to_string()),
            Some(rule) if rule == "false" => None,
            Some(rename) => Some(rename.clone()),
        }
    }

    /// Java parity: `ConfLoader.getSrcTableFromDstTable`.
    pub fn src_table_from_dst_table(&self, dst_table: &str) -> String {
        let upper = dst_table.to_ascii_uppercase();
        self.dst_to_src_table_map
            .get(&upper)
            .cloned()
            .unwrap_or_else(|| dst_table.to_string())
    }

    /// Java parity: `ConfLoader.tableHasFilterMapperRules`.
    pub fn table_has_column_rules(&self, table_name: &str) -> bool {
        self.enabled
            && self
                .column_rules
                .get(&table_name.to_ascii_uppercase())
                .is_some()
    }
}

/// Java parity: per-destination value-mapper rules.
///
/// Mirrors `ConfLoader.dstValueMappings`:
/// `Map<TABLE_UPPER, Map<COLUMN_UPPER, Map<srcValueString, dstValueString>>>`.
/// `null` source values are stored under the literal key `"null"` (Java
/// converts via `String.valueOf(null) == "null"`).
#[derive(Debug, Clone)]
pub struct ValueMapperRules {
    pub enabled: bool,
    pub mappings: HashMap<String, HashMap<String, HashMap<String, String>>>,
}

impl ValueMapperRules {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            mappings: HashMap::new(),
        }
    }

    /// Java parity: `ConfLoader.parseValueMappingsFile`. Flat JSON format
    /// `{"TABLE.COLUMN": {"src":"dst"}, ...}`. Per Java, entries are skipped
    /// when the filter-mapper rejects the table or column.
    pub fn parse_mappings_file(path: &Path, filter: &FilterMapperRules) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            format!("Failed to parse value mappings JSON file: {} : {}", path.display(), e)
        })?;
        let root: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            format!("Failed to parse value mappings JSON file: {} : {}", path.display(), e)
        })?;
        let obj = root.as_object().ok_or_else(|| {
            format!(
                "Failed to parse value mappings JSON file: {} : top-level must be a JSON object",
                path.display()
            )
        })?;

        let mut mappings: HashMap<String, HashMap<String, HashMap<String, String>>> = HashMap::new();
        for (table_col_key, sub) in obj {
            let parts: Vec<&str> = table_col_key.splitn(2, '.').collect();
            if parts.len() != 2 {
                return Err(format!(
                    "Invalid key in value mappings file (expected \"table.column\" format): {}",
                    table_col_key
                ));
            }
            let src_table = parts[0].trim();
            let src_column = parts[1].trim();

            if !filter.is_allowed_table(src_table) {
                continue;
            }
            if !filter.is_allowed_column(src_table, src_column) {
                continue;
            }

            let sub_obj = sub.as_object().ok_or_else(|| {
                format!(
                    "Failed to parse value mappings JSON file: {} : value for {} must be an object",
                    path.display(),
                    table_col_key
                )
            })?;
            let mut value_mappings: HashMap<String, String> = HashMap::new();
            for (src_value, dst_value) in sub_obj {
                let dst_str = dst_value.as_str().ok_or_else(|| {
                    format!(
                        "Failed to parse value mappings JSON file: {} : mapping for {}.{} must be a string",
                        path.display(),
                        table_col_key,
                        src_value
                    )
                })?;
                value_mappings.insert(src_value.clone(), dst_str.to_string());
            }

            mappings
                .entry(src_table.to_ascii_uppercase())
                .or_insert_with(HashMap::new)
                .insert(src_column.to_ascii_uppercase(), value_mappings);
        }

        Ok(Self {
            enabled: true,
            mappings,
        })
    }

    /// Java parity: `ConfLoader.getMappedValue`. `value` is the stringified
    /// source value (`"null"` for SQL NULL, matching Java's
    /// `String.valueOf(null)`). Returns `Some(mapped)` only when a mapping
    /// exists; the caller keeps the original value otherwise.
    pub fn mapped_value(&self, table_name: &str, column_name: &str, value: &str) -> Option<String> {
        if !self.enabled {
            return None;
        }
        self.mappings
            .get(&table_name.to_ascii_uppercase())
            .and_then(|cols| cols.get(&column_name.to_ascii_uppercase()))
            .and_then(|vals| vals.get(value))
            .cloned()
    }

    /// Java parity: `ConfLoader.tableHasValueMappings`.
    pub fn table_has_value_mappings(&self, table_name: &str) -> bool {
        self.enabled
            && self
                .mappings
                .get(&table_name.to_ascii_uppercase())
                .is_some()
    }
}

/// Java parity: `ConfLoader.parseTriggersFile`. Reads a JSON object whose
/// keys are destination table names and whose values are arrays of SQL
/// statement strings. Returns a map keyed by ASCII-uppercased table name.
pub fn parse_triggers_file(path: &Path) -> Result<HashMap<String, Vec<String>>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        format!("Failed to parse triggers JSON file: {} : {}", path.display(), e)
    })?;
    let root: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
        format!("Failed to parse triggers JSON file: {} : {}", path.display(), e)
    })?;
    let obj = root.as_object().ok_or_else(|| {
        format!(
            "Failed to parse triggers JSON file: {} : top-level must be a JSON object",
            path.display()
        )
    })?;
    let mut out = HashMap::new();
    for (table, stmts) in obj.iter() {
        if table.trim().is_empty() {
            return Err(format!(
                "Invalid empty table name key in triggers file: {}",
                path.display()
            ));
        }
        let arr = stmts.as_array().ok_or_else(|| {
            format!(
                "Failed to parse triggers JSON file: {} : value for {} must be an array of SQL strings",
                path.display(),
                table
            )
        })?;
        let mut sqls = Vec::with_capacity(arr.len());
        for v in arr {
            let s = v.as_str().ok_or_else(|| {
                format!(
                    "Failed to parse triggers JSON file: {} : entries for {} must be SQL strings",
                    path.display(),
                    table
                )
            })?;
            sqls.push(s.to_string());
        }
        out.insert(table.to_ascii_uppercase(), sqls);
    }
    Ok(out)
}
