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

/// Returns true when SQL starts with `CREATE`, `DROP`, or `ALTER`.
pub fn is_ddl(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    upper.starts_with("CREATE ") || upper.starts_with("DROP ") || upper.starts_with("ALTER ")
}

/// Validate one SQL statement against STORE/STREAMING policy.
///
/// Returns the parsed SQL shape when valid for the requested mode.
pub fn validate_sql_policy(sql: &str, mode: SqlPolicyMode) -> Result<SqlShape> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(Error::Db("Unsupported SQL: empty SQL statement".to_string()));
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
        Regex::new(r"(?is)^\s*DELETE\s+FROM\s+(?:[A-Za-z0-9_]+\.)?[A-Za-z0-9_]+(?:\s+WHERE\s+(.+))?\s*$")
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
        Regex::new(r"(?i)\b[A-Za-z_][A-Za-z0-9_]*\s*\(")
            .expect("valid word-call detector regex")
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
        let u = validate_sql_policy(
            "UPDATE t SET v = (SELECT 'x')",
            SqlPolicyMode::Store,
        )
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

        let f = validate_sql_policy(
            "UPDATE t SET v = lower(?)",
            SqlPolicyMode::Store,
        )
        .expect_err("expected update with function call to be rejected")
        .to_string();
        assert!(f.contains("Unsupported SQL"));
    }
}
