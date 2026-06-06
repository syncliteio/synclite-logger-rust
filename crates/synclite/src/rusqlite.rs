use std::path::Path;

use logger_core::{Backend, DeviceType, Error, Result};
use logger_db_traits::{Row, Value};

use crate::{default_config_for_backend, derive_device_name, SyncLiteOptions, Logger};
use crate::sql_split::split_sqls;

const DEFAULT_BATCH_CAPACITY: usize = 4096;

/// SyncLite-wrapped SQLite connection with a `rusqlite`-shaped surface.
pub struct Connection {
    runtime: Logger,
    user_auto_commit: bool,
}

/// SyncLite-wrapped prepared statement.
pub struct Statement<'a> {
    conn: &'a mut Connection,
    sql: String,
    batch_params: Vec<Vec<Value>>,
}

impl Connection {
    fn process_txn_message_if_needed(&mut self, sql: &str) -> Result<bool> {
        let lowered = sql.trim().to_ascii_lowercase();
        if lowered.starts_with("begin") {
            self.user_auto_commit = false;
            return Ok(true);
        }
        if lowered.starts_with("end") || lowered.starts_with("commit") {
            self.commit()?;
            self.user_auto_commit = true;
            return Ok(true);
        }
        if lowered.starts_with("rollback") {
            self.rollback()?;
            self.user_auto_commit = true;
            return Ok(true);
        }
        Ok(false)
    }

    /// Initialize a SyncLite-managed SQLite database using path-derived defaults.
    pub fn initialize<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let device_name = derive_device_name(db_path.as_ref());
        crate::initialize(
            DeviceType::Sqlite,
            &device_name,
            db_path.as_ref(),
            None,
            SyncLiteOptions::default(),
        )?;
        Self::open(db_path)
    }

    /// Open a SyncLite-managed SQLite database using path-derived defaults.
    pub fn open<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let cfg = default_config_for_backend(db_path.as_ref().to_path_buf(), Backend::Sqlite);
        Ok(Self {
            runtime: Logger::open_with(cfg)?,
            user_auto_commit: true,
        })
    }

    /// Initialize from an existing SyncLite config file.
    pub fn initialize_with_config<P: AsRef<Path>>(conf_path: P) -> Result<Self> {
        let cfg = logger_config::SyncLiteConfig::load_any(&conf_path)?;
        let db_path = cfg.db_path.clone().ok_or_else(|| {
            logger_core::Error::Config(
                "db-path is required in config for initialize_with_config".to_string(),
            )
        })?;
        let device_name = cfg
            .device_name
            .clone()
            .unwrap_or_else(|| derive_device_name(&db_path));
        crate::initialize(
            DeviceType::Sqlite,
            &device_name,
            db_path,
            None,
            SyncLiteOptions {
                config: Some(cfg),
                ..SyncLiteOptions::default()
            },
        )?;
        Self::open_with_config(conf_path)
    }

    /// Open from an existing SyncLite config file.
    pub fn open_with_config<P: AsRef<Path>>(conf_path: P) -> Result<Self> {
        Ok(Self {
            runtime: Logger::open(conf_path)?,
            user_auto_commit: true,
        })
    }

    /// Execute a mutating statement.
    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64> {
        let sqls = split_sqls(sql);
        if sqls.len() > 1 && !params.is_empty() {
            return Err(Error::Db(
                "multi-statement execute does not support bound params; pass params only for a single SQL statement".into(),
            ));
        }
        let mut affected = 0u64;
        for next_sql in sqls {
            if self.process_txn_message_if_needed(&next_sql)? {
                continue;
            }
            let next_params = if params.is_empty() { &[][..] } else { params };
            affected = self.runtime.execute(&next_sql, next_params)?;
            if self.user_auto_commit {
                self.runtime.commit()?;
            }
        }
        Ok(affected)
    }

    /// Execute a mutating statement without writing to commandlog.
    pub fn execute_unlogged(&mut self, sql: &str, params: &[Value]) -> Result<u64> {
        let sqls = split_sqls(sql);
        if sqls.len() > 1 && !params.is_empty() {
            return Err(Error::Db(
                "multi-statement execute_unlogged does not support bound params; pass params only for a single SQL statement".into(),
            ));
        }
        let mut affected = 0u64;
        for next_sql in sqls {
            if self.process_txn_message_if_needed(&next_sql)? {
                continue;
            }
            let next_params = if params.is_empty() { &[][..] } else { params };
            affected = self.runtime.execute_unlogged(&next_sql, next_params)?;
            if self.user_auto_commit {
                self.runtime.commit()?;
            }
        }
        Ok(affected)
    }

    /// Execute a query and collect all rows eagerly.
    pub fn query(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        self.runtime.query(sql, params)
    }

    /// Prepare one statement for repeated execution.
    pub fn prepare<'a>(&'a mut self, sql: &str) -> Result<Statement<'a>> {
        Ok(Statement {
            conn: self,
            sql: sql.to_string(),
            batch_params: Vec::with_capacity(DEFAULT_BATCH_CAPACITY),
        })
    }

    /// Mirror Java wrapper behavior: user-facing autocommit toggle.
    pub fn set_auto_commit(&mut self, auto_commit: bool) {
        self.user_auto_commit = auto_commit;
    }

    /// Return the user-facing autocommit mode.
    pub fn get_auto_commit(&self) -> bool {
        self.user_auto_commit
    }

    /// Commit the current logical transaction.
    pub fn commit(&mut self) -> Result<()> {
        self.runtime.commit()
    }

    /// Roll back the current logical transaction.
    pub fn rollback(&mut self) -> Result<()> {
        self.runtime.rollback()
    }

    /// Finalize the active log segment (roll to a fresh one) so the
    /// shipper sees in-flight work. Pair with [`synclite::await_sync`]
    /// to block until the rolled segment has been consumed.
    pub fn flush(&mut self) -> Result<()> {
        self.runtime.flush()
    }

    /// Close the connection.
    pub fn close(mut self) -> Result<()> {
        if !self.user_auto_commit {
            self.runtime.commit()?;
        }
        self.runtime.close()
    }

}

impl<'a> Statement<'a> {
    /// Execute the prepared statement once.
    pub fn execute(&mut self, params: &[Value]) -> Result<u64> {
        self.conn.execute(&self.sql, params)
    }

    /// Run a query through the prepared statement.
    pub fn query(&mut self, params: &[Value]) -> Result<Vec<Row>> {
        self.conn.query(&self.sql, params)
    }

    /// Queue one batch row.
    pub fn add_batch(&mut self, params: &[Value]) {
        self.batch_params.push(params.to_vec());
    }

    /// Clear queued batch rows.
    pub fn clear_batch(&mut self) {
        self.batch_params.clear();
    }

    /// Execute queued batch rows.
    pub fn execute_batch(&mut self) -> Result<Vec<u64>> {
        let batch = std::mem::take(&mut self.batch_params);
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        let out = self.conn.runtime.execute_prepared_batch(&self.sql, &batch)?;
        if self.conn.user_auto_commit {
            self.conn.runtime.commit()?;
        }
        Ok(out)
    }
}





