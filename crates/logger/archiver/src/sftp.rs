//! SFTP archiver. Uploads finalized log segments to a remote directory
//! over SSH, using the pure-Rust `russh` + `russh-sftp` stack so embedders
//! don't need OpenSSL / libssh2 at build time.
//!
//! Like the S3 archiver this lives behind its own `sftp` feature flag and
//! owns a private Tokio runtime to bridge `russh`'s async API into the
//! synchronous [`Archiver`] trait.
//!
//! Uploads land in `<remote_dir>/<name>.tmp` and are renamed to
//! `<remote_dir>/<name>` so consumers never observe a partial file.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use russh::client::{self, Handle, Handler};
use russh::ChannelMsg;
use russh_keys::key::PublicKey;
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::OpenFlags;
use tokio::io::AsyncWriteExt;
use tokio::runtime::{Builder as RtBuilder, Runtime};
use tracing::debug;

use logger_core::{Error, Result};

use crate::Archiver;

/// Authentication mode for SFTP.
#[derive(Debug, Clone)]
pub enum SftpAuth {
    /// Username + password.
    Password {
        /// Cleartext password sent over the encrypted channel.
        password: String,
    },
    /// Username + private key on disk (optionally passphrase-protected).
    PrivateKeyFile {
        /// Path to an OpenSSH-format private key.
        path: PathBuf,
        /// Optional passphrase decrypting the key.
        passphrase: Option<String>,
    },
}

/// Configuration for the SFTP archiver.
#[derive(Debug, Clone)]
pub struct SftpConfig {
    /// Hostname or IP of the SFTP server.
    pub host: String,
    /// TCP port (default 22).
    pub port: u16,
    /// Username to authenticate as.
    pub username: String,
    /// Authentication method.
    pub auth: SftpAuth,
    /// Remote directory; segment files land directly inside it.
    pub remote_dir: String,
    /// When `true`, the client accepts ANY server host key. Convenient
    /// for first-run / lab setups but disables MITM protection. Set to
    /// `false` and rely on pre-known hosts in production.
    pub accept_any_host_key: bool,
    /// Connect timeout.
    pub connect_timeout: Duration,
}

impl SftpConfig {
    /// Convenience constructor with required fields.
    pub fn new<S1: Into<String>, S2: Into<String>>(
        host: S1,
        username: S2,
        auth: SftpAuth,
        remote_dir: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: 22,
            username: username.into(),
            auth,
            remote_dir: remote_dir.into(),
            accept_any_host_key: true,
            connect_timeout: Duration::from_secs(30),
        }
    }

    /// Override the port.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Override host-key acceptance behaviour.
    pub fn with_accept_any_host_key(mut self, on: bool) -> Self {
        self.accept_any_host_key = on;
        self
    }

    /// Override the connect timeout.
    pub fn with_connect_timeout(mut self, t: Duration) -> Self {
        self.connect_timeout = t;
        self
    }
}

/// SFTP-backed archiver. A fresh SSH connection is opened for every
/// `ship()` call to keep the implementation simple and resilient to
/// transient server-side disconnects between widely-spaced segments.
pub struct SftpArchiver {
    cfg: SftpConfig,
    rt: Arc<Runtime>,
}

impl std::fmt::Debug for SftpArchiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpArchiver")
            .field("host", &self.cfg.host)
            .field("port", &self.cfg.port)
            .field("username", &self.cfg.username)
            .field("remote_dir", &self.cfg.remote_dir)
            .field("accept_any_host_key", &self.cfg.accept_any_host_key)
            .finish()
    }
}

impl SftpArchiver {
    /// Build an archiver from a [`SftpConfig`]. Spawns a private Tokio
    /// runtime used for all uploads.
    pub fn new(cfg: SftpConfig) -> Result<Self> {
        let rt = Arc::new(
            RtBuilder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("synclite-sftp")
                .build()
                .map_err(|e| Error::Archiver(format!("tokio runtime: {e}")))?,
        );
        Ok(Self { cfg, rt })
    }

    /// Compute the remote object path for a segment.
    pub(crate) fn remote_path(&self, segment_path: &Path) -> Result<String> {
        let file_name = segment_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                Error::Archiver(format!(
                    "segment path has no file name: {}",
                    segment_path.display()
                ))
            })?;
        Ok(join_remote(&self.cfg.remote_dir, file_name))
    }
}

fn join_remote(dir: &str, file_name: &str) -> String {
    if dir.is_empty() || dir == "." {
        return file_name.to_string();
    }
    let trimmed = dir.trim_end_matches('/');
    format!("{trimmed}/{file_name}")
}

struct AcceptAllHandler;

#[async_trait]
impl Handler for AcceptAllHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &PublicKey,
    ) -> std::result::Result<bool, Self::Error> {
        Ok(true)
    }
}

async fn ship_async(cfg: &SftpConfig, src: &Path, remote_final: &str) -> Result<()> {
    if !cfg.accept_any_host_key {
        // Strict host-key checking would need a known-hosts file plumbed
        // through; we surface a clear error rather than silently accept.
        return Err(Error::Archiver(
            "sftp: strict host-key checking is not yet implemented; \
             enable accept_any_host_key for now"
                .into(),
        ));
    }

    let ssh_cfg = Arc::new(client::Config {
        inactivity_timeout: Some(Duration::from_secs(120)),
        ..Default::default()
    });

    let connect = client::connect(
        ssh_cfg,
        (cfg.host.as_str(), cfg.port),
        AcceptAllHandler,
    );
    let mut session: Handle<AcceptAllHandler> =
        tokio::time::timeout(cfg.connect_timeout, connect)
            .await
            .map_err(|_| Error::Archiver(format!("sftp connect to {}:{} timed out", cfg.host, cfg.port)))?
            .map_err(|e| Error::Archiver(format!("sftp connect: {e}")))?;

    // Authenticate.
    let authed = match &cfg.auth {
        SftpAuth::Password { password } => session
            .authenticate_password(&cfg.username, password)
            .await
            .map_err(|e| Error::Archiver(format!("sftp auth (password): {e}")))?,
        SftpAuth::PrivateKeyFile { path, passphrase } => {
            let key = russh_keys::load_secret_key(path, passphrase.as_deref())
                .map_err(|e| {
                    Error::Archiver(format!(
                        "sftp load key {}: {e}",
                        path.display()
                    ))
                })?;
            session
                .authenticate_publickey(&cfg.username, Arc::new(key))
                .await
                .map_err(|e| Error::Archiver(format!("sftp auth (publickey): {e}")))?
        }
    };
    if !authed {
        return Err(Error::Archiver(format!(
            "sftp authentication rejected for user {}",
            cfg.username
        )));
    }

    // Open the sftp subsystem on a channel.
    let channel = session
        .channel_open_session()
        .await
        .map_err(|e| Error::Archiver(format!("sftp channel open: {e}")))?;
    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(|e| Error::Archiver(format!("sftp subsystem request: {e}")))?;
    let sftp = SftpSession::new(channel.into_stream())
        .await
        .map_err(|e| Error::Archiver(format!("sftp session init: {e}")))?;

    let tmp_path = format!("{remote_final}.tmp");

    // Read source bytes (segments are small enough to fit in memory).
    let bytes = tokio::fs::read(src)
        .await
        .map_err(|e| Error::Archiver(format!("read {}: {e}", src.display())))?;

    // Best-effort: ignore "already removed" error if a previous attempt
    // left a stale .tmp file behind.
    let _ = sftp.remove_file(&tmp_path).await;

    let mut file = sftp
        .open_with_flags(
            &tmp_path,
            OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
        )
        .await
        .map_err(|e| Error::Archiver(format!("sftp open {tmp_path}: {e}")))?;
    file.write_all(&bytes)
        .await
        .map_err(|e| Error::Archiver(format!("sftp write {tmp_path}: {e}")))?;
    file.shutdown()
        .await
        .map_err(|e| Error::Archiver(format!("sftp close {tmp_path}: {e}")))?;
    drop(file);

    // Rename into final position. On some servers `rename` fails if the
    // target exists, so remove it first (best-effort).
    let _ = sftp.remove_file(remote_final).await;
    sftp.rename(&tmp_path, remote_final).await.map_err(|e| {
        Error::Archiver(format!(
            "sftp rename {tmp_path} -> {remote_final}: {e}"
        ))
    })?;

    // Close the channel cleanly. Errors here are best-effort.
    let _ = session.disconnect(russh::Disconnect::ByApplication, "", "").await;
    // Touch ChannelMsg to avoid unused-import lint when feature combinations
    // strip the underlying message types.
    let _ = std::mem::size_of::<ChannelMsg>();
    Ok(())
}

impl Archiver for SftpArchiver {
    fn name(&self) -> &str {
        "sftp"
    }

    fn ship(&self, segment_path: &Path) -> Result<()> {
        let remote_final = self.remote_path(segment_path)?;
        let cfg = self.cfg.clone();
        let src = segment_path.to_path_buf();
        debug!(
            target: "synclite::archiver::sftp",
            host = %cfg.host,
            port = cfg.port,
            remote = %remote_final,
            "uploading segment"
        );
        self.rt
            .block_on(async move { ship_async(&cfg, &src, &remote_final).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_path_join_variants() {
        assert_eq!(join_remote("", "seg-1.db"), "seg-1.db");
        assert_eq!(join_remote(".", "seg-1.db"), "seg-1.db");
        assert_eq!(join_remote("uploads", "seg-1.db"), "uploads/seg-1.db");
        assert_eq!(join_remote("uploads/", "seg-1.db"), "uploads/seg-1.db");
        assert_eq!(
            join_remote("/var/synclite/logs", "seg-42.db"),
            "/var/synclite/logs/seg-42.db"
        );
    }

    #[test]
    fn remote_path_uses_file_name_only() {
        let cfg = SftpConfig::new(
            "example.com",
            "alice",
            SftpAuth::Password {
                password: "secret".into(),
            },
            "/srv/logs",
        );
        let a = SftpArchiver::new(cfg).unwrap();
        let p = a
            .remote_path(Path::new("/tmp/stage/segments/commandlog-3.db"))
            .unwrap();
        assert_eq!(p, "/srv/logs/commandlog-3.db");
    }
}


