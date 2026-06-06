//! Java parity: `com.synclite.consolidator.schema.DataTypeMapper`
//! hierarchy.
//!
//! The Java side ships one mapper per destination type. The Rust port keeps
//! the three destination types we support (SQLite, DuckDB, Postgres) and
//! routes through a single `DataTypeMapper` enum so callers can stay
//! allocation-free.
//!
//! The mapping mode (`DstDataTypeMapping`) follows the Java enum exactly:
//! `ALL_TEXT`, `BEST_EFFORT` (default), `CUSTOMIZED`, `EXACT`. Resolution
//! order mirrors `DataTypeMapper.mapType()`:
//!
//! 1. If mode is `CUSTOMIZED`, look up
//!    `map-src-<type>-to-dst-<N>` /
//!    `map-src-<base>(length)-to-dst-<N>` /
//!    `map-src-<base>(precision,scale)-to-dst-<N>` in the user-supplied
//!    `user_overrides` map. Falls through to conservative mapping when the
//!    user did not specify an override for that type.
//! 2. If mode is `EXACT`, return the source type unchanged.
//! 3. If mode is `BEST_EFFORT`, normalize via the per-destination
//!    best-effort switch (Java `doMapTypeBestEffort`).
//! 4. Otherwise (`ALL_TEXT`), use the per-destination conservative mapping
//!    (text for everything except `blob` → `blob`/`bytea`).

use std::collections::HashMap;

use crate::DstType;

/// Java parity: `com.synclite.consolidator.global.DstDataTypeMapping`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DstDataTypeMapping {
    /// Java default: every column declared as TEXT (BYTEA for blobs).
    AllText,
    BestEffort,
    Customized,
    Exact,
}

impl DstDataTypeMapping {
    /// Java parity: `DstDataTypeMapping.valueOf(propValue)` exception text
    /// wording is preserved by the caller; this helper only does the parse.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "ALL_TEXT" => Some(Self::AllText),
            "BEST_EFFORT" => Some(Self::BestEffort),
            "CUSTOMIZED" => Some(Self::Customized),
            "EXACT" => Some(Self::Exact),
            _ => None,
        }
    }
}

impl Default for DstDataTypeMapping {
    fn default() -> Self {
        Self::BestEffort
    }
}

/// Per-destination type mapper. Stateless except for the destination kind,
/// the active mode, the user override map, and (for Postgres) the
/// vector-extension flag.
#[derive(Debug, Clone)]
pub struct DataTypeMapper {
    dst_type: DstType,
    mode: DstDataTypeMapping,
    /// Map of normalized override keys → user-supplied target SQL type. Keys
    /// look like `"varchar"`, `"varchar(length)"`, `"decimal(precision,scale)"`
    /// (i.e. already lowercased and with whitespace stripped, matching the
    /// Java property lookup).
    user_overrides: HashMap<String, String>,
    /// Java parity: `dst-vector-extension-enabled-N`. Postgres-only; toggles
    /// pgvector `VECTOR` mapping for float/vector array source types.
    vector_extension_enabled: bool,
}

impl DataTypeMapper {
    pub fn new(
        dst_type: DstType,
        mode: DstDataTypeMapping,
        user_overrides: HashMap<String, String>,
        vector_extension_enabled: bool,
    ) -> Self {
        Self {
            dst_type,
            mode,
            user_overrides,
            vector_extension_enabled,
        }
    }

    /// Java parity: `DataTypeMapper.mapType(srcType)` returning the
    /// destination native SQL type string.
    pub fn map_type(&self, src_type: &str) -> String {
        let src = src_type.trim();
        if src.is_empty() {
            return self.conservative(src);
        }
        match self.mode {
            DstDataTypeMapping::Customized => {
                if let Some(mapped) = self.user_mapped_type(src) {
                    return mapped;
                }
                // Java falls through to conservative mapping when no
                // user override is supplied.
                self.conservative(src)
            }
            DstDataTypeMapping::Exact => src.to_string(),
            DstDataTypeMapping::BestEffort => self.best_effort(src),
            DstDataTypeMapping::AllText => self.conservative(src),
        }
    }

    // ---- Java parity: userMappedType() ------------------------------

    fn user_mapped_type(&self, src: &str) -> Option<String> {
        // Java: typeToCheck = type.dbNativeDataType.replace("\\s+", "").toLowerCase().
        // The regex is a no-op in Java (it does not call replaceAll), so the
        // effective transform is "lowercase". Mirror that.
        let type_to_check = src.to_ascii_lowercase();
        if let Some(v) = self.user_overrides.get(&type_to_check) {
            return Some(v.clone());
        }
        let open = type_to_check.find('(')?;
        let base = type_to_check[..open].trim().to_string();

        // length variant
        let length_key = format!("{base}(length)");
        if let Some(v) = self.user_overrides.get(&length_key) {
            let lower = v.to_ascii_lowercase();
            if lower.contains("(length)") {
                let dst_open = lower.find('(').expect("checked");
                let dst_base = lower[..dst_open].trim();
                return Some(type_to_check.replacen(&base, dst_base, 1));
            }
            return Some(lower);
        }

        // precision,scale variant
        let prec_key = format!("{base}(precision,scale)");
        if let Some(v) = self.user_overrides.get(&prec_key) {
            let lower = v.to_ascii_lowercase();
            if lower.contains("(precision,scale)") {
                let dst_open = lower.find('(').expect("checked");
                let dst_base = lower[..dst_open].trim();
                return Some(type_to_check.replacen(&base, dst_base, 1));
            }
            return Some(lower);
        }

        None
    }

    // ---- Java parity: doMapTypeConservative() per destination -------

    fn conservative(&self, src: &str) -> String {
        let lower = src.trim().to_ascii_lowercase();
        let is_blob = lower == "blob";
        match self.dst_type {
            DstType::Sqlite | DstType::DuckDb => {
                if is_blob {
                    "blob".to_string()
                } else {
                    "text".to_string()
                }
            }
            DstType::Postgres => {
                if is_blob {
                    "BYTEA".to_string()
                } else {
                    "TEXT".to_string()
                }
            }
        }
    }

    // ---- Java parity: doMapTypeBestEffort() switch ------------------

    fn best_effort(&self, src: &str) -> String {
        // SQLite's BEST_EFFORT path is identity (Java
        // SQLiteDataTypeMapper.doMapTypeBestEffort returns the source type
        // verbatim).
        if self.dst_type == DstType::Sqlite {
            return src.to_string();
        }
        let normalized = normalize_type_for_switch(src);
        match normalized.as_str() {
            "smallserial" | "serial" | "bigserial" | "bit" | "integer" | "int" | "tinyint"
            | "smallint" | "mediumint" | "bigint" | "int2" | "int4" | "int8" | "long"
            | "byteint" | "unsigned" => self.best_effort_integer(),

            "text" | "varchar" | "varchar2" | "nvarchar" | "char" | "nchar" | "native"
            | "character" | "varying" | "nvarchar2" | "xmltype" | "xml" | "json" => {
                self.best_effort_text()
            }

            "clob" | "dbclob" => self.best_effort_clob(),

            "array" | "integer[" | "bigint[" | "text[" | "boolean[" | "float[" | "numeric["
            | "timestamp[" | "date[" | "time[" | "character[" | "json[" | "jsonb[" | "vector" => {
                self.best_effort_array(src)
            }

            "blob" | "bytea" | "binary" | "varbinary" | "image" | "object" | "geography"
            | "geometry" | "raw" | "sdo_geometry" | "sdo_topo_geometry" | "bfile" | "ref"
            | "ordicom" | "ordaudio" | "ordvideo" | "orddoc" | "table" | "associative"
            | "varray" | "graphic" | "vargraphic" => self.best_effort_blob(),

            "real" | "double" | "float" | "numeric" | "money" | "smallmoney" | "number"
            | "decimal" | "binary_float" | "binary_double" => self.best_effort_real(),

            "boolean" | "bool" => self.best_effort_boolean(),

            "date" => self.best_effort_date(),

            "datetime" | "datetime2" | "time" | "timestamp" => self.best_effort_datetime(),

            _ => self.best_effort_text(),
        }
    }

    fn best_effort_integer(&self) -> String {
        // All three destinations: bigint.
        "bigint".to_string()
    }

    fn best_effort_text(&self) -> String {
        "text".to_string()
    }

    fn best_effort_clob(&self) -> String {
        "text".to_string()
    }

    fn best_effort_blob(&self) -> String {
        match self.dst_type {
            DstType::Sqlite | DstType::DuckDb => "blob".to_string(),
            DstType::Postgres => "bytea".to_string(),
        }
    }

    fn best_effort_real(&self) -> String {
        match self.dst_type {
            DstType::Sqlite => "real".to_string(),
            DstType::DuckDb => "double".to_string(),
            DstType::Postgres => "double precision".to_string(),
        }
    }

    fn best_effort_boolean(&self) -> String {
        "boolean".to_string()
    }

    fn best_effort_date(&self) -> String {
        // Java SQLite/DuckDB/Postgres all return timestamp.
        "timestamp".to_string()
    }

    fn best_effort_datetime(&self) -> String {
        "timestamp".to_string()
    }

    fn best_effort_array(&self, src: &str) -> String {
        // Default (SQLite/DuckDB and Postgres without vector extension):
        // emit text. Postgres with vector-extension enabled returns VECTOR
        // for float[]/vector source types and the original native type
        // otherwise — matching PGDataTypeMapper.getBestEffortArrayDataType.
        match self.dst_type {
            DstType::Postgres => {
                if self.vector_extension_enabled {
                    let first = src
                        .trim()
                        .to_ascii_lowercase()
                        .split(|c: char| c.is_whitespace() || c == '(')
                        .next()
                        .unwrap_or("")
                        .to_string();
                    if first.starts_with("float") || first.starts_with("vector") {
                        return "VECTOR".to_string();
                    }
                    return src.to_string();
                }
                src.to_string()
            }
            _ => "text".to_string(),
        }
    }
}

/// Java parity: `type.toLowerCase().trim().split("[\\s(]+")[0]` followed by
/// the `[`-suffix slice for array discrimination.
fn normalize_type_for_switch(src: &str) -> String {
    let mut s: String = src
        .trim()
        .to_ascii_lowercase()
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_string();
    if let Some(idx) = s.find('[') {
        s.truncate(idx + 1);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapper(dst: DstType, mode: DstDataTypeMapping) -> DataTypeMapper {
        DataTypeMapper::new(dst, mode, HashMap::new(), false)
    }

    #[test]
    fn all_text_default_maps_everything_to_text_except_blob() {
        let m = mapper(DstType::Sqlite, DstDataTypeMapping::AllText);
        assert_eq!(m.map_type("INTEGER"), "text");
        assert_eq!(m.map_type("varchar(255)"), "text");
        assert_eq!(m.map_type("BLOB"), "blob");

        let pg = mapper(DstType::Postgres, DstDataTypeMapping::AllText);
        assert_eq!(pg.map_type("INTEGER"), "TEXT");
        assert_eq!(pg.map_type("BLOB"), "BYTEA");
    }

    #[test]
    fn exact_returns_source_verbatim() {
        let m = mapper(DstType::DuckDb, DstDataTypeMapping::Exact);
        assert_eq!(m.map_type("VARCHAR(100)"), "VARCHAR(100)");
    }

    #[test]
    fn best_effort_sqlite_is_identity() {
        let m = mapper(DstType::Sqlite, DstDataTypeMapping::BestEffort);
        assert_eq!(m.map_type("VARCHAR(64)"), "VARCHAR(64)");
    }

    #[test]
    fn best_effort_duckdb_maps_integer_to_bigint() {
        let m = mapper(DstType::DuckDb, DstDataTypeMapping::BestEffort);
        assert_eq!(m.map_type("integer"), "bigint");
        assert_eq!(m.map_type("float"), "double");
        assert_eq!(m.map_type("decimal(10,2)"), "double");
        assert_eq!(m.map_type("varchar(64)"), "text");
        assert_eq!(m.map_type("blob"), "blob");
    }

    #[test]
    fn best_effort_postgres_blob_is_bytea() {
        let m = mapper(DstType::Postgres, DstDataTypeMapping::BestEffort);
        assert_eq!(m.map_type("blob"), "bytea");
        assert_eq!(m.map_type("real"), "double precision");
    }

    #[test]
    fn customized_falls_back_to_conservative() {
        let mut overrides = HashMap::new();
        overrides.insert("varchar".to_string(), "VARCHAR(8000)".to_string());
        let m = DataTypeMapper::new(
            DstType::Postgres,
            DstDataTypeMapping::Customized,
            overrides,
            false,
        );
        assert_eq!(m.map_type("varchar"), "VARCHAR(8000)");
        // No override for "integer" → conservative path.
        assert_eq!(m.map_type("integer"), "TEXT");
    }

    #[test]
    fn customized_length_variant_substitutes_base_name() {
        let mut overrides = HashMap::new();
        overrides.insert(
            "varchar(length)".to_string(),
            "nvarchar(length)".to_string(),
        );
        let m = DataTypeMapper::new(
            DstType::Postgres,
            DstDataTypeMapping::Customized,
            overrides,
            false,
        );
        assert_eq!(m.map_type("varchar(255)"), "nvarchar(255)");
    }

    #[test]
    fn postgres_vector_extension_array() {
        let m = DataTypeMapper::new(
            DstType::Postgres,
            DstDataTypeMapping::BestEffort,
            HashMap::new(),
            true,
        );
        assert_eq!(m.map_type("float[]"), "VECTOR");
        assert_eq!(m.map_type("vector"), "VECTOR");
    }
}
