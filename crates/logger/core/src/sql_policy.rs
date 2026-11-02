//! Shared SQL-shape validation rules for SyncLite device modes.

use std::sync::OnceLock;

use regex::Regex;

use crate::{Error, Result};

/// SQL policy mode used by SyncLite device types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlPolicyMode {
    /// STORE devices allow CRUD (with restricted INSERT/UPDATE/DELETE shapes) and DDL.
    Store,
    /// STREAMING devices allow INSERT (restricted shape) and DDL.
    Streaming,
}

/// Parsed shape of one SQL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlShape {
    /// `INSERT INTO ... VALUES (...)` statement.
    Insert(InsertShape),
    /// `UPDATE ... SET ...` statement.
    Update,
    /// `DELETE FROM ...` statement.
    Delete,
    /// `CREATE|DROP|ALTER ...` statement.
    Ddl,
    /// `SELECT ...` statement.
    Select,
    /// Statement type outside the supported SQL policy.
    Other,
}

/// Parsed details of a restricted INSERT statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertShape {
    /// Target table name (without optional `<db>.` prefix).
    pub table_name: String,
    /// Number of explicitly listed columns, if a column list is present.
    pub column_count: Option<usize>,
    /// Number of values listed in the `VALUES(...)` clause.
    pub value_count: usize,
}

/// Coarse statement classification used when a backend does not expose a
/// safe prepared-statement read-only flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlStatementKind {
    /// A statement expected not to mutate database state.
    ReadOnly,
    /// An `INSERT` or `REPLACE` statement.
    Insert,
    /// An `UPDATE` statement.
    Update,
    /// A `DELETE` statement.
    Delete,
    /// A schema-changing statement.
    Ddl,
    /// A recognized non-query command.
    Command,
    /// An unknown statement, treated conservatively as mutating.
    Unknown,
}

/// Classify the top-level SQL operation while ignoring comments, quoted
/// values, and nested CTE bodies.
///
/// Unknown operations are considered mutating so callers never accidentally
/// bypass SyncLite logging.
pub fn classify_sql_statement(sql: &str) -> SqlStatementKind {
    let keywords = top_level_keywords(sql);
    let Some(first) = keywords.first().map(String::as_str) else {
        return SqlStatementKind::Unknown;
    };

    if first == "EXPLAIN" && !keywords.iter().any(|word| word == "ANALYZE") {
        return SqlStatementKind::ReadOnly;
    }

    let primary = if first == "WITH" {
        keywords.iter().skip(1).find_map(|word| {
            matches!(
                word.as_str(),
                "SELECT" | "VALUES" | "FROM" | "INSERT" | "REPLACE" | "UPDATE" | "DELETE" | "MERGE"
            )
            .then_some(word.as_str())
        })
    } else if first == "EXPLAIN" {
        keywords.iter().skip(1).find_map(|word| {
            matches!(
                word.as_str(),
                "SELECT" | "VALUES" | "FROM" | "INSERT" | "REPLACE" | "UPDATE" | "DELETE" | "MERGE"
            )
            .then_some(word.as_str())
        })
    } else {
        Some(first)
    };

    match primary {
        Some(
            "SELECT" | "VALUES" | "FROM" | "SHOW" | "DESCRIBE" | "DESC" | "SUMMARIZE" | "PRAGMA"
            | "PIVOT" | "UNPIVOT" | "CALL",
        ) => SqlStatementKind::ReadOnly,
        Some("INSERT" | "REPLACE") => SqlStatementKind::Insert,
        Some("UPDATE" | "MERGE") => SqlStatementKind::Update,
        Some("DELETE") => SqlStatementKind::Delete,
        Some("CREATE" | "DROP" | "ALTER") => SqlStatementKind::Ddl,
        Some(
            "ATTACH" | "DETACH" | "COPY" | "EXPORT" | "IMPORT" | "SET" | "RESET" | "TRUNCATE"
            | "VACUUM" | "ANALYZE",
        ) => SqlStatementKind::Command,
        _ => SqlStatementKind::Unknown,
    }
}

/// Return whether a keyword occurs at SQL nesting depth zero.
pub fn has_top_level_keyword(sql: &str, keyword: &str) -> bool {
    let keyword = keyword.to_ascii_uppercase();
    top_level_keywords(sql).iter().any(|word| word == &keyword)
}

fn top_level_keywords(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut words = Vec::new();
    let mut index = 0;
    let mut depth = 0_u32;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' | b'`' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == quote {
                        if index + 1 < bytes.len() && bytes[index + 1] == quote {
                            index += 2;
                            continue;
                        }
                        index += 1;
                        break;
                    }
                    index += 1;
                }
            }
            b'[' => {
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b']' {
                        if index + 1 < bytes.len() && bytes[index + 1] == b']' {
                            index += 2;
                            continue;
                        }
                        index += 1;
                        break;
                    }
                    index += 1;
                }
            }
            b'-' if index + 1 < bytes.len() && bytes[index + 1] == b'-' => {
                index += 2;
                while index < bytes.len() && !matches!(bytes[index], b'\r' | b'\n') {
                    index += 1;
                }
            }
            b'/' if index + 1 < bytes.len() && bytes[index + 1] == b'*' => {
                index += 2;
                let mut comment_depth = 1_u32;
                while index < bytes.len() && comment_depth > 0 {
                    if index + 1 < bytes.len() && bytes[index] == b'/' && bytes[index + 1] == b'*' {
                        comment_depth += 1;
                        index += 2;
                    } else if index + 1 < bytes.len()
                        && bytes[index] == b'*'
                        && bytes[index + 1] == b'/'
                    {
                        comment_depth -= 1;
                        index += 2;
                    } else {
                        index += 1;
                    }
                }
            }
            b'(' => {
                depth += 1;
                index += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                index += 1;
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || matches!(bytes[index], b'_' | b'$'))
                {
                    index += 1;
                }
                if depth == 0 {
                    words.push(sql[start..index].to_ascii_uppercase());
                }
            }
            _ => index += 1,
        }
    }
    words
}

/// Returns true when the top-level statement is `CREATE`, `DROP`, or `ALTER`.
pub fn is_ddl(sql: &str) -> bool {
    classify_sql_statement(sql) == SqlStatementKind::Ddl
}

/// Validate one SQL statement against STORE/STREAMING policy.
///
/// Returns the parsed SQL shape when valid for the requested mode.
pub fn validate_sql_policy(sql: &str, mode: SqlPolicyMode) -> Result<SqlShape> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(Error::Db(
            "Unsupported SQL: empty SQL statement".to_string(),
        ));
    }

    if let Some(insert) = parse_insert_shape(sql) {
        return Ok(SqlShape::Insert(insert));
    }

    if is_ddl(sql) {
        return Ok(SqlShape::Ddl);
    }

    if is_select(sql) {
        return Ok(SqlShape::Select);
    }

    if is_update_shape(sql) {
        return match mode {
            SqlPolicyMode::Store => Ok(SqlShape::Update),
            SqlPolicyMode::Streaming => Err(Error::Db(format!(
                "Unsupported SQL: SyncLite streaming device does not allow SQL: {sql}. Allowed SQLs are CREATE TABLE, DROP TABLE, ALTER TABLE, INSERT INTO, SELECT"
            ))),
        };
    }

    if is_delete_shape(sql) {
        return match mode {
            SqlPolicyMode::Store => Ok(SqlShape::Delete),
            SqlPolicyMode::Streaming => Err(Error::Db(format!(
                "Unsupported SQL: SyncLite streaming device does not allow SQL: {sql}. Allowed SQLs are CREATE TABLE, DROP TABLE, ALTER TABLE, INSERT INTO, SELECT"
            ))),
        };
    }

    match mode {
        SqlPolicyMode::Store => Err(Error::Db(format!(
            "Unsupported SQL: SyncLite store device does not allow SQL: {sql}. Allowed SQLs are CREATE TABLE, DROP TABLE, ALTER TABLE, INSERT INTO, UPDATE, DELETE, SELECT"
        ))),
        SqlPolicyMode::Streaming => Err(Error::Db(format!(
            "Unsupported SQL: SyncLite streaming device does not allow SQL: {sql}. Allowed SQLs are CREATE TABLE, DROP TABLE, ALTER TABLE, INSERT INTO, SELECT"
        ))),
    }
}

/// Parse INSERT shape if SQL matches the supported INSERT regex.
pub fn parse_insert_shape(sql: &str) -> Option<InsertShape> {
    let caps = insert_regex().captures(sql.trim())?;
    let table_name = caps.get(2)?.as_str().to_string();
    let column_count = caps
        .get(3)
        .map(|m| split_csv_count(m.as_str()))
        .filter(|count| *count > 0);
    let value_count = split_csv_count(caps.get(4)?.as_str());
    Some(InsertShape {
        table_name,
        column_count,
        value_count,
    })
}

fn is_select(sql: &str) -> bool {
    sql.trim_start().to_ascii_uppercase().starts_with("SELECT ")
}

fn split_csv_count(s: &str) -> usize {
    s.split(',').filter(|part| !part.trim().is_empty()).count()
}

fn insert_regex() -> &'static Regex {
    static INSERT: OnceLock<Regex> = OnceLock::new();
    INSERT.get_or_init(|| {
        Regex::new(r"(?is)^\s*INSERT\s+INTO\s+(?:([A-Za-z0-9_]+)\.)?([A-Za-z0-9_]+)\s*(?:\(([^)]+)\))?\s*VALUES\s*\(([^)]+)\)\s*$")
            .expect("valid insert regex")
    })
}

fn delete_regex() -> &'static Regex {
    static DELETE: OnceLock<Regex> = OnceLock::new();
    DELETE.get_or_init(|| {
        Regex::new(
            r"(?is)^\s*DELETE\s+FROM\s+(?:[A-Za-z0-9_]+\.)?[A-Za-z0-9_]+(?:\s+WHERE\s+(.+))?\s*$",
        )
        .expect("valid delete regex")
    })
}

fn is_update_shape(sql: &str) -> bool {
    let Some(caps) = update_regex().captures(sql.trim()) else {
        return false;
    };
    let Some(set_expr) = caps.get(1).map(|m| m.as_str()) else {
        return false;
    };
    !contains_forbidden_select_or_call(set_expr)
}

fn is_delete_shape(sql: &str) -> bool {
    let Some(caps) = delete_regex().captures(sql.trim()) else {
        return false;
    };
    let where_expr = caps.get(1).map(|m| m.as_str()).unwrap_or("");
    !contains_forbidden_select_or_call(where_expr)
}

fn contains_forbidden_select_or_call(expr: &str) -> bool {
    if expr.trim().is_empty() {
        return false;
    }
    if contains_select_regex().is_match(expr) {
        return true;
    }
    contains_word_call_regex().is_match(expr)
}

fn contains_select_regex() -> &'static Regex {
    static SELECT_WORD: OnceLock<Regex> = OnceLock::new();
    SELECT_WORD.get_or_init(|| Regex::new(r"(?i)\bSELECT\b").expect("valid SELECT detector regex"))
}

fn contains_word_call_regex() -> &'static Regex {
    static WORD_CALL: OnceLock<Regex> = OnceLock::new();
    WORD_CALL.get_or_init(|| {
        Regex::new(r"(?i)\b[A-Za-z_][A-Za-z0-9_]*\s*\(").expect("valid word-call detector regex")
    })
}

fn update_regex() -> &'static Regex {
    static UPDATE: OnceLock<Regex> = OnceLock::new();
    UPDATE.get_or_init(|| {
        Regex::new(r"(?is)^\s*UPDATE\s+(?:[A-Za-z0-9_]+\.)?[A-Za-z0-9_]+\s+SET\s+(.+)$")
            .expect("valid update regex")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_queries_ctes_and_returning() {
        assert_eq!(
            classify_sql_statement("-- comment\nSELECT * FROM t"),
            SqlStatementKind::ReadOnly
        );
        assert_eq!(
            classify_sql_statement("WITH source AS (SELECT 1) SELECT * FROM source"),
            SqlStatementKind::ReadOnly
        );
        assert_eq!(
            classify_sql_statement("WITH source AS (SELECT 1) INSERT INTO t SELECT * FROM source"),
            SqlStatementKind::Insert
        );
        assert_eq!(
            classify_sql_statement("UPDATE t SET value = 1"),
            SqlStatementKind::Update
        );
        assert_eq!(
            classify_sql_statement("CREATE TABLE t(id INTEGER)"),
            SqlStatementKind::Ddl
        );
        assert_eq!(
            classify_sql_statement("CALL pragma_version()"),
            SqlStatementKind::ReadOnly
        );
        assert!(has_top_level_keyword(
            "INSERT INTO t VALUES (1) RETURNING id",
            "returning"
        ));
        assert!(!has_top_level_keyword(
            "SELECT 'RETURNING', (SELECT returning FROM nested)",
            "returning"
        ));
    }

    #[test]
    fn store_accepts_simple_update_delete() {
        assert!(matches!(
            validate_sql_policy("UPDATE t SET v = ? WHERE id = ?", SqlPolicyMode::Store),
            Ok(SqlShape::Update)
        ));
        assert!(matches!(
            validate_sql_policy("DELETE FROM t WHERE id = ?", SqlPolicyMode::Store),
            Ok(SqlShape::Delete)
        ));
    }

    #[test]
    fn store_rejects_select_and_function_calls_in_update_delete() {
        let u = validate_sql_policy("UPDATE t SET v = (SELECT 'x')", SqlPolicyMode::Store)
            .expect_err("expected update with select to be rejected")
            .to_string();
        assert!(u.contains("Unsupported SQL"));

        let d = validate_sql_policy(
            "DELETE FROM t WHERE id IN (SELECT id FROM t)",
            SqlPolicyMode::Store,
        )
        .expect_err("expected delete with select to be rejected")
        .to_string();
        assert!(d.contains("Unsupported SQL"));

        let f = validate_sql_policy("UPDATE t SET v = lower(?)", SqlPolicyMode::Store)
            .expect_err("expected update with function call to be rejected")
            .to_string();
        assert!(f.contains("Unsupported SQL"));
    }
}
