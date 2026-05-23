//! SyncLite runtime contract.
//!
//! This crate defines the runtime-facing API meant to be consumed by
//! language bindings. User-facing language wrappers should remain thin and
//! delegate all logging, shipping, recovery, and metadata behavior to this
//! runtime layer.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::path::Path;
use std::sync::mpsc;
use std::thread;

use synclite_config::SyncLiteConfig;
use synclite_core::{Backend, DeviceType, Error, Result};
use synclite_db_traits::Value;
use synclite::Logger;

/// Runtime contract between language adapters and SyncLite internals.
pub trait RuntimeContract {
    /// Runtime backend currently in use.
    fn backend(&self) -> Backend;
    /// Log one SQL operation.
    fn log(&mut self, sql: &str, params: &[Value]) -> Result<u64>;
    /// Runtime pre-hook before wrapper executes one user-DB mutating statement.
    fn pre_user_execute(&mut self, sql: &str, params: &[Value]) -> Result<()>;
    /// Runtime post-hook after wrapper executes one user-DB mutating statement.
    fn post_user_execute(&mut self, sql: &str) -> Result<()>;
    /// Runtime pre-hook before wrapper executes a user-DB prepared batch.
    fn pre_user_execute_batch(&mut self, sql: &str, batch_params: &[Vec<Value>]) -> Result<()>;
    /// Runtime post-hook after wrapper executes a user-DB prepared batch.
    fn post_user_execute_batch(&mut self, sql: &str) -> Result<()>;
    /// Commit current transaction scope.
    fn commit(&mut self) -> Result<()>;
    /// Flush buffered log records without commit/rollback fate.
    fn flush_log(&mut self) -> Result<()>;
    /// Roll back current transaction scope.
    fn rollback(&mut self) -> Result<()>;
    /// Close runtime and finalize state.
    fn close(self) -> Result<()>;
}

/// Default runtime implementation backed by `synclite::Logger`.
pub struct Runtime {
    mode_logger: ModeLogger,
}

enum ModeLogger {
    Txn(TxnLogger),
    Event(EventLogger),
    Async(AsyncLogger),
}

struct TxnLogger {
    logger: Logger,
}

struct EventLogger {
    logger: Logger,
}

struct AsyncLogger {
    backend: Backend,
    tx: mpsc::SyncSender<RuntimeCommand>,
    join_handle: Option<thread::JoinHandle<Result<()>>>,
}

enum RuntimeCommand {
    Log {
        sql: String,
        params: Vec<Value>,
        reply: mpsc::Sender<Result<u64>>,
    },
    PreUserExecute {
        sql: String,
        params: Vec<Value>,
        reply: mpsc::Sender<Result<()>>,
    },
    PostUserExecute {
        sql: String,
        reply: mpsc::Sender<Result<()>>,
    },
    PreUserExecuteBatch {
        sql: String,
        batch_params: Vec<Vec<Value>>,
        reply: mpsc::Sender<Result<()>>,
    },
    PostUserExecuteBatch {
        sql: String,
        reply: mpsc::Sender<Result<()>>,
    },
    Commit {
        reply: mpsc::Sender<Result<()>>,
    },
    FlushLog {
        reply: mpsc::Sender<Result<()>>,
    },
    Rollback {
        reply: mpsc::Sender<Result<()>>,
    },
    Close {
        reply: mpsc::Sender<Result<()>>,
    },
}

impl AsyncLogger {
    fn spawn(logger: Logger, backend: Backend, queue_size: usize) -> Result<Self> {
        let (tx, rx) = mpsc::sync_channel(queue_size);
        let join_handle = thread::Builder::new()
            .name("synclite-runtime-async".into())
            .spawn(move || worker_loop(logger, rx))
            .map_err(|e| Error::Internal(format!("failed to spawn runtime worker: {e}")))?;
        Ok(Self {
            backend,
            tx,
            join_handle: Some(join_handle),
        })
    }

    fn backend(&self) -> Backend {
        self.backend
    }

    fn request<T>(
        &self,
        build: impl FnOnce(mpsc::Sender<Result<T>>) -> RuntimeCommand,
    ) -> Result<T> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(build(reply_tx))
            .map_err(|e| Error::Internal(format!("async runtime send failed: {e}")))?;
        reply_rx
            .recv()
            .map_err(|e| Error::Internal(format!("async runtime reply failed: {e}")))?
    }

    fn log(&self, sql: &str, params: &[Value]) -> Result<u64> {
        self.request(|reply| RuntimeCommand::Log {
            sql: sql.to_string(),
            params: params.to_vec(),
            reply,
        })
    }

    fn pre_user_execute(&self, sql: &str, params: &[Value]) -> Result<()> {
        self.request(|reply| RuntimeCommand::PreUserExecute {
            sql: sql.to_string(),
            params: params.to_vec(),
            reply,
        })
    }

    fn post_user_execute(&self, sql: &str) -> Result<()> {
        self.request(|reply| RuntimeCommand::PostUserExecute {
            sql: sql.to_string(),
            reply,
        })
    }

    fn pre_user_execute_batch(&self, sql: &str, batch_params: &[Vec<Value>]) -> Result<()> {
        self.request(|reply| RuntimeCommand::PreUserExecuteBatch {
            sql: sql.to_string(),
            batch_params: batch_params.to_vec(),
            reply,
        })
    }

    fn post_user_execute_batch(&self, sql: &str) -> Result<()> {
        self.request(|reply| RuntimeCommand::PostUserExecuteBatch {
            sql: sql.to_string(),
            reply,
        })
    }

    fn commit(&self) -> Result<()> {
        self.request(|reply| RuntimeCommand::Commit { reply })
    }

    fn flush_log(&self) -> Result<()> {
        self.request(|reply| RuntimeCommand::FlushLog { reply })
    }

    fn rollback(&self) -> Result<()> {
        self.request(|reply| RuntimeCommand::Rollback { reply })
    }

    fn close(mut self) -> Result<()> {
        let result = self.request(|reply| RuntimeCommand::Close { reply });
        if let Some(join_handle) = self.join_handle.take() {
            match join_handle.join() {
                Ok(join_result) => join_result.and(result),
                Err(_) => Err(Error::Internal(
                    "async runtime worker panicked during close".into(),
                )),
            }
        } else {
            result
        }
    }
}

fn worker_loop(mut logger: Logger, rx: mpsc::Receiver<RuntimeCommand>) -> Result<()> {
    while let Ok(command) = rx.recv() {
        match command {
            RuntimeCommand::Log { sql, params, reply } => {
                let _ = reply.send(logger.log_sql(&sql, &params).map(|_| 0));
            }
            RuntimeCommand::PreUserExecute { sql, params, reply } => {
                let _ = reply.send(logger.pre_user_execute(&sql, &params));
            }
            RuntimeCommand::PostUserExecute { sql, reply } => {
                let _ = reply.send(logger.post_user_execute(&sql));
            }
            RuntimeCommand::PreUserExecuteBatch { sql, batch_params, reply } => {
                let _ = reply.send(logger.pre_user_execute_batch(&sql, &batch_params));
            }
            RuntimeCommand::PostUserExecuteBatch { sql, reply } => {
                let _ = reply.send(logger.post_user_execute_batch(&sql));
            }
            RuntimeCommand::Commit { reply } => {
                let _ = reply.send(logger.commit());
            }
            RuntimeCommand::FlushLog { reply } => {
                let _ = reply.send(logger.flush_log());
            }
            RuntimeCommand::Rollback { reply } => {
                let _ = reply.send(logger.rollback());
            }
            RuntimeCommand::Close { reply } => {
                let _ = reply.send(logger.close());
                break;
            }
        }
    }
    Ok(())
}

impl ModeLogger {
    fn backend(&self) -> Backend {
        match self {
            ModeLogger::Txn(m) => m.logger.backend(),
            ModeLogger::Event(m) => m.logger.backend(),
            ModeLogger::Async(m) => m.backend(),
        }
    }

    fn log(&mut self, sql: &str, params: &[Value]) -> Result<u64> {
        match self {
            ModeLogger::Txn(m) => {
                m.logger.log_sql(sql, params)?;
                Ok(0)
            }
            ModeLogger::Event(m) => {
                m.logger.log_sql(sql, params)?;
                Ok(0)
            }
            ModeLogger::Async(m) => m.log(sql, params),
        }
    }

    fn commit(&mut self) -> Result<()> {
        match self {
            ModeLogger::Txn(m) => m.logger.commit(),
            ModeLogger::Event(m) => m.logger.commit(),
            ModeLogger::Async(m) => m.commit(),
        }
    }

    fn flush_log(&mut self) -> Result<()> {
        match self {
            ModeLogger::Txn(m) => m.logger.flush_log(),
            ModeLogger::Event(m) => m.logger.flush_log(),
            ModeLogger::Async(m) => m.flush_log(),
        }
    }

    fn rollback(&mut self) -> Result<()> {
        match self {
            ModeLogger::Txn(m) => m.logger.rollback(),
            ModeLogger::Event(m) => m.logger.rollback(),
            ModeLogger::Async(m) => m.rollback(),
        }
    }

    fn close(self) -> Result<()> {
        match self {
            ModeLogger::Txn(m) => m.logger.close(),
            ModeLogger::Event(m) => m.logger.close(),
            ModeLogger::Async(m) => m.close(),
        }
    }

    fn pre_user_execute(&mut self, sql: &str, params: &[Value]) -> Result<()> {
        match self {
            ModeLogger::Txn(m) => m.logger.pre_user_execute(sql, params),
            ModeLogger::Event(m) => m.logger.pre_user_execute(sql, params),
            ModeLogger::Async(m) => m.pre_user_execute(sql, params),
        }
    }

    fn post_user_execute(&mut self, sql: &str) -> Result<()> {
        match self {
            ModeLogger::Txn(m) => m.logger.post_user_execute(sql),
            ModeLogger::Event(m) => m.logger.post_user_execute(sql),
            ModeLogger::Async(m) => m.post_user_execute(sql),
        }
    }

    fn pre_user_execute_batch(&mut self, sql: &str, batch_params: &[Vec<Value>]) -> Result<()> {
        match self {
            ModeLogger::Txn(m) => m.logger.pre_user_execute_batch(sql, batch_params),
            ModeLogger::Event(m) => m.logger.pre_user_execute_batch(sql, batch_params),
            ModeLogger::Async(m) => m.pre_user_execute_batch(sql, batch_params),
        }
    }

    fn post_user_execute_batch(&mut self, sql: &str) -> Result<()> {
        match self {
            ModeLogger::Txn(m) => m.logger.post_user_execute_batch(sql),
            ModeLogger::Event(m) => m.logger.post_user_execute_batch(sql),
            ModeLogger::Async(m) => m.post_user_execute_batch(sql),
        }
    }
}

impl Runtime {
    /// Initialize runtime from `synclite_logger.conf` path.
    pub fn initialize<P: AsRef<Path>>(conf_path: P) -> Result<Self> {
        Self::open(conf_path)
    }

    /// Open runtime from `synclite_logger.conf` path.
    pub fn open<P: AsRef<Path>>(conf_path: P) -> Result<Self> {
        let cfg = SyncLiteConfig::load(conf_path)?;
        Self::open_with(cfg)
    }

    /// Initialize runtime from parsed config.
    pub fn initialize_with(cfg: SyncLiteConfig) -> Result<Self> {
        Self::open_with(cfg)
    }

    /// Open runtime from parsed config.
    pub fn open_with(cfg: SyncLiteConfig) -> Result<Self> {
        let log_queue_size = cfg.log_queue_size.unwrap_or(i32::MAX as u64);
        let backend = cfg
            .backend
            .or_else(|| cfg.device_type.map(DeviceType::backend))
            .unwrap_or(Backend::Sqlite);
        let device_type = cfg
            .device_type
            .unwrap_or_else(|| DeviceType::default_for_backend(backend));
        let async_mode = should_use_async_logger(device_type, &cfg);
        let logger = Logger::open_with(cfg)?;
        let mode_logger = if async_mode {
            let queue_size = usize::try_from(log_queue_size)
                .map_err(|_| Error::Config("log-queue-size is too large for this platform".into()))?;
            ModeLogger::Async(AsyncLogger::spawn(logger, backend, queue_size)?)
        } else if device_type.is_transactional() {
            ModeLogger::Txn(TxnLogger { logger })
        } else {
            ModeLogger::Event(EventLogger { logger })
        };
        Ok(Self {
            mode_logger,
        })
    }
}

fn should_use_async_logger(device_type: DeviceType, cfg: &SyncLiteConfig) -> bool {
    if device_type.is_transactional() {
        !cfg.disable_async_logging_for_txn_device.unwrap_or(false)
    } else {
        cfg.enable_async_logging_for_appender_device.unwrap_or(false)
    }
}

impl RuntimeContract for Runtime {
    fn backend(&self) -> Backend {
        self.mode_logger.backend()
    }

    fn log(&mut self, sql: &str, params: &[Value]) -> Result<u64> {
        self.mode_logger.log(sql, params)
    }

    fn pre_user_execute(&mut self, sql: &str, params: &[Value]) -> Result<()> {
        self.mode_logger.pre_user_execute(sql, params)
    }

    fn post_user_execute(&mut self, sql: &str) -> Result<()> {
        self.mode_logger.post_user_execute(sql)
    }

    fn pre_user_execute_batch(&mut self, sql: &str, batch_params: &[Vec<Value>]) -> Result<()> {
        self.mode_logger.pre_user_execute_batch(sql, batch_params)
    }

    fn post_user_execute_batch(&mut self, sql: &str) -> Result<()> {
        self.mode_logger.post_user_execute_batch(sql)
    }

    fn commit(&mut self) -> Result<()> {
        self.mode_logger.commit()
    }

    fn flush_log(&mut self) -> Result<()> {
        self.mode_logger.flush_log()
    }

    fn rollback(&mut self) -> Result<()> {
        self.mode_logger.rollback()
    }

    fn close(self) -> Result<()> {
        self.mode_logger.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transactional_device_defaults_to_async() {
        let cfg = SyncLiteConfig::default();
        assert!(should_use_async_logger(DeviceType::Sqlite, &cfg));
    }

    #[test]
    fn transactional_device_honors_disable_async_flag() {
        let mut cfg = SyncLiteConfig::default();
        cfg.disable_async_logging_for_txn_device = Some(true);
        assert!(!should_use_async_logger(DeviceType::DuckDb, &cfg));
    }

    #[test]
    fn appender_device_defaults_to_sync() {
        let cfg = SyncLiteConfig::default();
        assert!(!should_use_async_logger(DeviceType::SqliteStore, &cfg));
    }

    #[test]
    fn appender_device_honors_enable_async_flag() {
        let mut cfg = SyncLiteConfig::default();
        cfg.enable_async_logging_for_appender_device = Some(true);
        assert!(should_use_async_logger(DeviceType::SqliteStore, &cfg));
    }
}
