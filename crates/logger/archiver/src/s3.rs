//! S3-compatible archiver. Uploads finalized segment files to a bucket via
//! `aws-sdk-s3`. Works against AWS S3, MinIO, Cloudflare R2, or any other
//! S3-compatible endpoint.
//!
//! Because the [`Archiver`] trait is synchronous but the AWS SDK is async,
//! the archiver owns a private multi-thread Tokio runtime that it
//! `block_on`s for each upload.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use tokio::runtime::{Builder as RtBuilder, Runtime};
use tracing::debug;

use logger_core::{Error, Result};

use crate::Archiver;

/// Configuration for the S3 archiver.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Target bucket name.
    pub bucket: String,
    /// Optional key prefix; segment file names are appended to this.
    /// A trailing `/` is added if missing and the prefix is non-empty.
    pub key_prefix: String,
    /// Region (e.g. `"us-east-1"`). Required for AWS S3; MinIO accepts
    /// any value.
    pub region: Option<String>,
    /// Optional custom endpoint URL (use for MinIO/R2).
    pub endpoint_url: Option<String>,
    /// Whether to force path-style addressing (`https://host/bucket/key`).
    /// Required by most MinIO deployments.
    pub force_path_style: bool,
    /// Optional static credentials. When `None`, the default AWS
    /// credential provider chain is used.
    pub credentials: Option<StaticCredentials>,
}

/// Static access key / secret key pair (for testing or environments
/// without an IAM identity).
#[derive(Debug, Clone)]
pub struct StaticCredentials {
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
    /// Optional session token.
    pub session_token: Option<String>,
}

impl S3Config {
    /// Convenience constructor with a bucket name; everything else default.
    pub fn new<S: Into<String>>(bucket: S) -> Self {
        Self {
            bucket: bucket.into(),
            key_prefix: String::new(),
            region: None,
            endpoint_url: None,
            force_path_style: false,
            credentials: None,
        }
    }

    /// Set the key prefix (folder).
    pub fn with_key_prefix<S: Into<String>>(mut self, prefix: S) -> Self {
        self.key_prefix = prefix.into();
        self
    }

    /// Set the AWS region.
    pub fn with_region<S: Into<String>>(mut self, region: S) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set a custom endpoint URL (e.g. for MinIO).
    pub fn with_endpoint_url<S: Into<String>>(mut self, url: S) -> Self {
        self.endpoint_url = Some(url.into());
        self
    }

    /// Force path-style addressing.
    pub fn with_path_style(mut self, on: bool) -> Self {
        self.force_path_style = on;
        self
    }

    /// Provide static credentials.
    pub fn with_credentials(mut self, creds: StaticCredentials) -> Self {
        self.credentials = Some(creds);
        self
    }
}

/// S3-backed archiver. Cheap to clone via `Arc`.
pub struct S3Archiver {
    cfg: S3Config,
    client: Client,
    rt: Arc<Runtime>,
}

impl std::fmt::Debug for S3Archiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Archiver")
            .field("bucket", &self.cfg.bucket)
            .field("key_prefix", &self.cfg.key_prefix)
            .field("region", &self.cfg.region)
            .field("endpoint_url", &self.cfg.endpoint_url)
            .field("force_path_style", &self.cfg.force_path_style)
            .finish()
    }
}

impl S3Archiver {
    /// Build an archiver from a [`S3Config`]. Spawns a private Tokio
    /// runtime used for all uploads.
    pub fn new(cfg: S3Config) -> Result<Self> {
        let rt = Arc::new(
            RtBuilder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("synclite-s3")
                .build()
                .map_err(|e| Error::Archiver(format!("tokio runtime: {e}")))?,
        );
        let client = rt.block_on(build_client(&cfg));
        Ok(Self { cfg, client, rt })
    }

    /// Compute the object key for a given segment path.
    pub(crate) fn object_key(&self, segment_path: &Path) -> Result<String> {
        let file_name = segment_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                Error::Archiver(format!(
                    "segment path has no file name: {}",
                    segment_path.display()
                ))
            })?;
        Ok(join_key(&self.cfg.key_prefix, file_name))
    }

    /// Target bucket.
    pub fn bucket(&self) -> &str {
        &self.cfg.bucket
    }
}

async fn build_client(cfg: &S3Config) -> Client {
    let mut loader =
        aws_config::defaults(BehaviorVersion::latest()).region(region_for(cfg));
    if let Some(creds) = &cfg.credentials {
        let provider = Credentials::new(
            creds.access_key_id.clone(),
            creds.secret_access_key.clone(),
            creds.session_token.clone(),
            None,
            "synclite-static",
        );
        loader = loader.credentials_provider(provider);
    }
    let shared = loader.load().await;

    let mut s3_builder = S3ConfigBuilder::from(&shared);
    if let Some(ep) = &cfg.endpoint_url {
        s3_builder = s3_builder.endpoint_url(ep.clone());
    }
    s3_builder = s3_builder.force_path_style(cfg.force_path_style);
    Client::from_conf(s3_builder.build())
}

fn region_for(cfg: &S3Config) -> Region {
    match &cfg.region {
        Some(r) => Region::new(r.clone()),
        None => Region::new("us-east-1"),
    }
}

fn join_key(prefix: &str, file_name: &str) -> String {
    if prefix.is_empty() {
        return file_name.to_string();
    }
    let trimmed = prefix.trim_end_matches('/');
    format!("{trimmed}/{file_name}")
}

impl Archiver for S3Archiver {
    fn name(&self) -> &str {
        "s3"
    }

    fn ship(&self, segment_path: &Path) -> Result<()> {
        let key = self.object_key(segment_path)?;
        let bucket = self.cfg.bucket.clone();
        let path: PathBuf = segment_path.to_path_buf();
        let client = self.client.clone();

        debug!(target: "synclite::archiver::s3", %bucket, %key, "uploading segment");

        self.rt.block_on(async move {
            let body = ByteStream::from_path(&path)
                .await
                .map_err(|e| Error::Archiver(format!("open {}: {e}", path.display())))?;
            client
                .put_object()
                .bucket(&bucket)
                .key(&key)
                .body(body)
                .send()
                .await
                .map_err(|e| Error::Archiver(format!("s3 put_object {key}: {e}")))?;
            Ok::<(), Error>(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_joining_handles_prefix_variants() {
        assert_eq!(join_key("", "seg-1.db"), "seg-1.db");
        assert_eq!(join_key("logs", "seg-1.db"), "logs/seg-1.db");
        assert_eq!(join_key("logs/", "seg-1.db"), "logs/seg-1.db");
        assert_eq!(
            join_key("tenants/a/logs/", "seg-42.db"),
            "tenants/a/logs/seg-42.db"
        );
    }

    #[test]
    fn object_key_uses_file_name_only() {
        let cfg = S3Config::new("bkt").with_key_prefix("logs");
        let a = S3Archiver::new(cfg).unwrap();
        let key = a
            .object_key(Path::new("/var/tmp/stage/segments/commandlog-3.db"))
            .unwrap();
        assert_eq!(key, "logs/commandlog-3.db");
    }
}


