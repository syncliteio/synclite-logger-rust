//! Shared synchronous PostgreSQL connector; no OpenSSL or platform CA API.

use consolidator_core::ConsolidatorLayout;
use logger_core::{Error, Result};
use postgres::{Client as PgClient, Config};
use rustls::{ClientConfig, RootCertStore};
use std::path::{Path, PathBuf};
use tokio_postgres_rustls::MakeRustlsConnect;

/// Connect using the layout's credentials and socket connection timeout.
///
/// `sslmode=disable` uses plaintext; `prefer` negotiates TLS but accepts a
/// server refusal; `require` refuses plaintext. `verify-ca` / `verify-full`
/// are accepted and behave as `require`: the rustls verifier always validates
/// the certificate chain and hostname whenever TLS is used, against the bundled
/// Mozilla/WebPKI roots plus any CA bundle named by `sslrootcert` (e.g. the AWS
/// RDS CA). Both `postgresql://` URIs and key/value strings are supported.
///
/// The configured timeout is postgres's per-address socket connect timeout,
/// not an overall DNS/TLS/authentication deadline. Call from synchronous worker
/// code, not from inside a Tokio async runtime (same restriction as PgClient).
pub fn connect_postgres(layout: &ConsolidatorLayout) -> Result<PgClient> {
    let (_, ssl_root_cert) = normalize_tls_options(layout.dst_connection_string.trim());
    let connection_string = build_postgres_connection_string(layout);
    let config = parse_config(&connection_string)?;
    let tls = MakeRustlsConnect::new(tls_config(ssl_root_cert.as_deref())?);
    config.connect(tls).map_err(|error| {
        // Never log the DSN. Server-sent errors (SQLSTATE present) may echo
        // server-controlled text, so only SQLSTATE is reported for them. Client-side
        // failures (DNS, TCP, timeout, TLS/certificate verification) carry no
        // credentials and are the only way to diagnose "error connecting to server",
        // so their cause chain is included.
        let code = error.code().map(|code| code.code()).unwrap_or("none");
        let cause = if error.as_db_error().is_some() {
            String::new()
        } else {
            let mut parts = Vec::new();
            let mut source = std::error::Error::source(&error);
            while let Some(inner) = source {
                parts.push(inner.to_string());
                source = inner.source();
            }
            if parts.is_empty() { String::new() } else { format!("; cause: {}", parts.join(": ")) }
        };
        Error::Db(format!(
            "consolidator: PostgreSQL connection failed ({error}; SQLSTATE={code}{cause})"
        ))
    })
}

fn parse_config(connection_string: &str) -> Result<Config> {
    connection_string.parse().map_err(|_| {
        Error::Config(
            "consolidator: invalid PostgreSQL connection configuration; supported sslmode \
             values are disable, prefer, require, verify-ca, and verify-full"
                .to_string(),
        )
    })
}

pub fn postgres_destination_conflict(
    current: &ConsolidatorLayout,
    requested: &ConsolidatorLayout,
) -> Result<Option<&'static str>> {
    let current = parse_config(&build_postgres_connection_string(current))?;
    let requested = parse_config(&build_postgres_connection_string(requested))?;
    if current.get_hosts() != requested.get_hosts()
        || current.get_hostaddrs() != requested.get_hostaddrs()
    {
        return Ok(Some("dst_host"));
    }
    let ports = |config: &Config| {
        let count = config.get_hosts().len().max(config.get_hostaddrs().len()).max(1);
        match config.get_ports() {
            [] => vec![5432; count],
            [port] => vec![*port; count],
            ports => ports.to_vec(),
        }
    };
    if ports(&current) != ports(&requested) {
        return Ok(Some("dst_port"));
    }
    let current_database = current.get_dbname().or_else(|| current.get_user());
    let requested_database = requested.get_dbname().or_else(|| requested.get_user());
    if current_database != requested_database {
        return Ok(Some("dst_database"));
    }
    if current.get_options() != requested.get_options() {
        return Ok(Some("dst_schema_options"));
    }
    Ok(None)
}

pub fn read_postgres_checkpoint(
    connection_string: &str,
    checkpoint_table: &str,
    device_id: &str,
    device_name: &str,
    timeout: Option<std::time::Duration>,
) -> Option<i64> {
    if timeout.is_some_and(|value| value.is_zero()) {
        return None;
    }
    let (dsn, ssl_root_cert) = normalize_tls_options(connection_string.trim());
    let config = dsn.parse::<tokio_postgres::Config>().ok()?;
    let tls = MakeRustlsConnect::new(tls_config(ssl_root_cert.as_deref()).ok()?);
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().ok()?;
    let result = runtime.block_on(async {
        let read = async {
            let (client, connection) = config.connect(tls).await.ok()?;
            let connection_task = tokio::spawn(connection);
            let sql = format!(
                "SELECT MAX(commit_id) FROM {checkpoint_table} \
                 WHERE synclite_device_id = $1 AND synclite_device_name = $2"
            );
            let row = client.query_one(&sql, &[&device_id, &device_name]).await;
            connection_task.abort();
            row.ok()?.try_get::<_, Option<i64>>(0).ok().flatten()
        };
        match timeout {
            Some(budget) => tokio::time::timeout(budget, read).await.ok().flatten(),
            None => read.await,
        }
    });
    runtime.shutdown_background();
    result
}

/// `tokio-postgres` (the parser behind `postgres::Config`) only knows
/// `sslmode` = disable/prefer/require and rejects the libpq `sslrootcert` key.
/// Remove `sslrootcert` (returned separately) and rewrite `verify-ca` /
/// `verify-full` to `require` before parsing. The rustls verifier always
/// validates the certificate chain and hostname whenever TLS is used, so the
/// verify modes are honoured by the TLS layer rather than by `sslmode`.
fn normalize_tls_options(dsn: &str) -> (String, Option<PathBuf>) {
    if is_uri(dsn) { normalize_uri(dsn) } else { normalize_kv(dsn) }
}

fn is_verify_mode(value: &str) -> bool {
    value.eq_ignore_ascii_case("verify-ca") || value.eq_ignore_ascii_case("verify-full")
}

fn normalize_uri(dsn: &str) -> (String, Option<PathBuf>) {
    let Some((base, query)) = dsn.split_once('?') else {
        return (dsn.to_string(), None);
    };
    let mut root = None;
    let mut kept: Vec<String> = Vec::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        match pair.split_once('=') {
            Some((name, value)) if uri_key_matches(name, "sslrootcert") => {
                let decoded = percent_decode(value);
                if !decoded.is_empty() {
                    root = Some(PathBuf::from(decoded));
                }
            }
            Some((name, value))
                if uri_key_matches(name, "sslmode") && is_verify_mode(&percent_decode(value)) =>
            {
                kept.push(format!("{name}=require"));
            }
            _ => kept.push(pair.to_string()),
        }
    }
    let out = if kept.is_empty() { base.to_string() } else { format!("{base}?{}", kept.join("&")) };
    (out, root)
}

fn normalize_kv(dsn: &str) -> (String, Option<PathBuf>) {
    let bytes = dsn.as_bytes();
    let len = bytes.len();
    let mut pos = 0;
    let mut root = None;
    let mut kept: Vec<String> = Vec::new();
    while pos < len {
        while pos < len && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= len {
            break;
        }
        let start = pos;
        while pos < len && !bytes[pos].is_ascii_whitespace() && bytes[pos] != b'=' {
            pos += 1;
        }
        let name = &dsn[start..pos];
        while pos < len && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= len || bytes[pos] != b'=' {
            // Malformed token: keep it so postgres rejects the whole string.
            kept.push(dsn[start..pos.max(start + name.len())].to_string());
            continue;
        }
        pos += 1; // '='
        while pos < len && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        let mut value: Vec<u8> = Vec::new();
        if pos < len && bytes[pos] == b'\'' {
            pos += 1;
            while pos < len {
                match bytes[pos] {
                    b'\\' if pos + 1 < len => {
                        value.push(bytes[pos + 1]);
                        pos += 2;
                    }
                    b'\'' => {
                        pos += 1;
                        break;
                    }
                    byte => {
                        value.push(byte);
                        pos += 1;
                    }
                }
            }
        } else {
            while pos < len && !bytes[pos].is_ascii_whitespace() {
                if bytes[pos] == b'\\' && pos + 1 < len {
                    value.push(bytes[pos + 1]);
                    pos += 2;
                } else {
                    value.push(bytes[pos]);
                    pos += 1;
                }
            }
        }
        let end = pos;
        let value = String::from_utf8_lossy(&value).into_owned();
        if name == "sslrootcert" {
            if !value.is_empty() {
                root = Some(PathBuf::from(value));
            }
        } else if name == "sslmode" && is_verify_mode(&value) {
            kept.push("sslmode=require".to_string());
        } else {
            kept.push(dsn[start..end].to_string());
        }
    }
    (kept.join(" "), root)
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                decoded.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        decoded.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn tls_config(ssl_root_cert: Option<&Path>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(root_cert) = ssl_root_cert {
        let pem = std::fs::read(root_cert).map_err(|error| {
            Error::Config(format!(
                "consolidator: failed to read PostgreSQL sslrootcert {}: {error}",
                root_cert.display()
            ))
        })?;
        let mut cursor = std::io::Cursor::new(pem);
        let certs = rustls_pemfile::certs(&mut cursor)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                Error::Config(format!(
                    "consolidator: failed to parse PostgreSQL sslrootcert {}: {error}",
                    root_cert.display()
                ))
            })?;
        if certs.is_empty() {
            return Err(Error::Config(format!(
                "consolidator: PostgreSQL sslrootcert {} contains no PEM certificates",
                root_cert.display()
            )));
        }
        for cert in certs {
            roots.add(cert).map_err(|_| {
                Error::Config(format!(
                    "consolidator: invalid certificate in PostgreSQL sslrootcert {}",
                    root_cert.display()
                ))
            })?;
        }
    }
    // Choose explicitly: other workspace dependencies can also enable aws-lc.
    // Do not install or rely on a process-global crypto provider.
    let mut tls = ClientConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .map_err(|_| Error::Config("consolidator: invalid PostgreSQL TLS protocol configuration".into()))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![b"postgresql".to_vec()];
    Ok(tls)
}

/// Shared form of runtime's PostgreSQL credential/timeout builder.
/// Explicit connection-string options take precedence over layout defaults,
/// including credentials in URI userinfo and `connect_timeout=0`.
/// The returned string contains secrets and MUST NOT be logged.
pub fn build_postgres_connection_string(layout: &ConsolidatorLayout) -> String {
    // Strip TLS options tokio-postgres cannot parse (sslrootcert, verify-*).
    let (mut out, _) = normalize_tls_options(layout.dst_connection_string.trim());
    // Let postgres identify credentials, including URI userinfo and quoted KV
    // values. Invalid input is left unchanged for parse_config to reject safely.
    let Ok(config) = out.parse::<Config>() else {
        return out;
    };
    if config.get_user().is_none() {
        if let Some(user) = layout.dst_user.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            append_param(&mut out, "user", user);
        }
    }
    if config.get_password().is_none() {
        if let Some(password) = layout.dst_password.as_deref().filter(|s| !s.is_empty()) {
            append_param(&mut out, "password", password);
        }
    }
    // A parsed zero timeout is indistinguishable from an absent one. Inspect
    // parameter names, not values (passwords may contain "connect_timeout=").
    if layout.dst_connection_timeout_s > 0 && !has_param(&out, "connect_timeout") {
        append_param(&mut out, "connect_timeout", &layout.dst_connection_timeout_s.to_string());
    }
    out
}

fn is_uri(s: &str) -> bool {
    s.starts_with("postgres://") || s.starts_with("postgresql://")
}

fn append_param(out: &mut String, key: &str, value: &str) {
    if is_uri(out) {
        out.push(if out.contains('?') { '&' } else { '?' });
        out.push_str(key);
        out.push('=');
        for byte in value.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                out.push(byte as char);
            } else {
                use std::fmt::Write;
                write!(out, "%{byte:02X}").expect("writing to String cannot fail");
            }
        }
    } else {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(key);
        out.push_str("='");
        out.push_str(&value.replace('\\', "\\\\").replace('\'', "\\'"));
        out.push('\'');
    }
}

fn has_param(s: &str, key: &str) -> bool {
    if is_uri(s) {
        return s.split_once('?').is_some_and(|(_, query)| {
            query.split('&').any(|pair| {
                pair.split_once('=').is_some_and(|(name, _)| uri_key_matches(name, key))
            })
        });
    }
    // Input was already validated by postgres. Skip quoted/escaped values so
    // tokens inside passwords or options cannot masquerade as parameter names.
    let mut chars = s.chars().peekable();
    while chars.peek().is_some() {
        while chars.peek().is_some_and(|ch| ch.is_whitespace()) { chars.next(); }
        let mut name = String::new();
        while chars.peek().is_some_and(|ch| !ch.is_whitespace() && *ch != '=') {
            name.push(chars.next().unwrap());
        }
        if name == key { return true; }
        while chars.peek().is_some_and(|ch| ch.is_whitespace()) { chars.next(); }
        chars.next(); // '='
        while chars.peek().is_some_and(|ch| ch.is_whitespace()) { chars.next(); }
        let quoted = chars.peek() == Some(&'\'');
        if quoted { chars.next(); }
        while let Some(ch) = chars.next() {
            if ch == '\\' { chars.next(); }
            else if (quoted && ch == '\'') || (!quoted && ch.is_whitespace()) { break; }
        }
    }
    false
}

fn uri_key_matches(name: &str, key: &str) -> bool {
    let mut bytes = name.bytes();
    let mut decoded = Vec::new();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let Some(hi) = bytes.next().and_then(|b| (b as char).to_digit(16)) else { return false; };
            let Some(lo) = bytes.next().and_then(|b| (b as char).to_digit(16)) else { return false; };
            decoded.push((hi * 16 + lo) as u8);
        } else {
            decoded.push(byte);
        }
    }
    decoded == key.as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use postgres::config::SslMode;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::Duration;

    fn accept_startup(stream: &mut TcpStream) {
        let mut length = [0; 4];
        stream.read_exact(&mut length).unwrap();
        let length = u32::from_be_bytes(length) as usize;
        assert!((8..65536).contains(&length));
        let mut startup = vec![0; length - 4];
        stream.read_exact(&mut startup).unwrap();
        assert_eq!(&startup[..4], &[0, 3, 0, 0]); // Protocol 3.0.
        // AuthenticationOk, then ReadyForQuery (no real DB required).
        stream.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0, b'Z', 0, 0, 0, 5, b'I']).unwrap();
        // Keep the socket alive until Client::drop sends Terminate.
        let mut terminate = [0; 5];
        stream.read_exact(&mut terminate).unwrap();
        assert_eq!(terminate, [b'X', 0, 0, 0, 4]);
    }

    #[test]
    fn plaintext_modes_support_hostname_and_hostaddr_without_hostname() {
        for host in ["host=127.0.0.1", "hostaddr=127.0.0.1"] {
            for mode in ["disable", "prefer", "require"] {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let port = listener.local_addr().unwrap().port();
                // Establish the socket from the client below, then let the
                // bounded server thread handle PostgreSQL negotiation only.
                let server = std::thread::spawn(move || {
                    let (mut stream, _) = listener.accept().unwrap();
                    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
                    if mode != "disable" {
                        let mut request = [0; 8];
                        stream.read_exact(&mut request).unwrap();
                        assert_eq!(request, [0, 0, 0, 8, 4, 210, 22, 47]);
                        stream.write_all(b"N").unwrap(); // Server does not support TLS.
                    }
                    if mode != "require" { accept_startup(&mut stream); }
                });
                let root = tempfile::tempdir().unwrap();
                let mut layout = super::super::tests::test_layout(root.path());
                layout.dst_connection_string = format!("{host} port={port} user=test sslmode={mode} connect_timeout=2");
                let result = connect_postgres(&layout);
                assert_eq!(result.is_ok(), mode != "require", "{host} sslmode={mode}");
                drop(result);
                server.join().unwrap();
            }
        }
    }

    #[test]
    fn prefer_does_not_retry_plaintext_after_tls_handshake_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut request = [0; 8];
            stream.read_exact(&mut request).unwrap();
            assert_eq!(request, [0, 0, 0, 8, 4, 210, 22, 47]);
            stream.write_all(b"S").unwrap();
            let mut header = [0; 5];
            stream.read_exact(&mut header).unwrap();
            assert_eq!(header[0], 22); // TLS handshake, not plaintext startup.
            let mut hello = vec![0; u16::from_be_bytes([header[3], header[4]]) as usize];
            stream.read_exact(&mut hello).unwrap();
            stream.write_all(&[21, 3, 3, 0, 2, 2, 40]).unwrap(); // Fatal handshake_failure alert.
        });
        let root = tempfile::tempdir().unwrap();
        let mut layout = super::super::tests::test_layout(root.path());
        layout.dst_connection_string = format!("host=localhost hostaddr=127.0.0.1 port={port} user=test sslmode=prefer connect_timeout=2");
        assert!(connect_postgres(&layout).is_err());
        server.join().unwrap();
    }

    #[test]
    fn ssl_modes_are_explicit_and_verified_tls_is_configured() {
        for (mode, expected) in [
            ("disable", SslMode::Disable),
            ("prefer", SslMode::Prefer),
            ("require", SslMode::Require),
        ] {
            assert_eq!(parse_config(&format!("host=localhost sslmode={mode}")).unwrap().get_ssl_mode(), expected);
        }
        assert_eq!(parse_config("host=localhost").unwrap().get_ssl_mode(), SslMode::Prefer);
        assert!(parse_config("host=localhost sslmode=bogus").is_err());
        // tokio-postgres itself cannot parse these; normalization must remove them.
        assert!(parse_config("host=localhost sslmode=verify-full").is_err());
        assert!(parse_config("host=localhost sslrootcert=/x.pem").is_err());

        assert!(!webpki_roots::TLS_SERVER_ROOTS.is_empty());
        assert_eq!(tls_config(None).unwrap().alpn_protocols, vec![b"postgresql".to_vec()]);
    }

    #[test]
    fn verify_modes_and_sslrootcert_are_normalized_in_kv_and_uri_forms() {
        for mode in ["verify-full", "verify-ca"] {
            let (kv, root) = normalize_tls_options(&format!(
                "host=db.example.com port=5432 dbname=syncdb user=u password='p w' sslmode={mode} sslrootcert='/data/my files/ca.pem'"
            ));
            assert_eq!(root, Some(PathBuf::from("/data/my files/ca.pem")));
            let cfg = parse_config(&kv).unwrap();
            assert_eq!(cfg.get_ssl_mode(), SslMode::Require);
            assert_eq!(cfg.get_user(), Some("u"));
            assert_eq!(cfg.get_password(), Some(&b"p w"[..]));

            let (uri, root) = normalize_tls_options(&format!(
                "postgresql://u:p@db.example.com:5432/syncdb?sslmode={mode}&sslrootcert=/data/user/0/app/files/aws-rds-ca.pem&connect_timeout=5"
            ));
            assert_eq!(root, Some(PathBuf::from("/data/user/0/app/files/aws-rds-ca.pem")));
            let cfg = parse_config(&uri).unwrap();
            assert_eq!(cfg.get_ssl_mode(), SslMode::Require);
            assert_eq!(cfg.get_connect_timeout(), Some(&Duration::from_secs(5)));
        }

        let (uri, root) = normalize_tls_options("postgresql://u:p@h/db?sslrootcert=%2Ftmp%2Fca%20x.pem");
        assert_eq!(root, Some(PathBuf::from("/tmp/ca x.pem")));
        assert_eq!(uri, "postgresql://u:p@h/db");

        // Untouched when no TLS extensions are present.
        let (kv, root) = normalize_tls_options("host=localhost sslmode=require");
        assert_eq!((kv.as_str(), root), ("host=localhost sslmode=require", None));
    }

    #[test]
    fn custom_root_ca_is_loaded_and_bad_files_are_rejected() {
        let missing = Path::new("/definitely/not/here.pem");
        assert!(tls_config(Some(missing)).is_err());

        let empty = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(empty.path(), "not a certificate\n").unwrap();
        assert!(tls_config(Some(empty.path())).is_err());
    }

    #[test]
    fn layout_credentials_are_merged_after_tls_options_are_stripped() {
        let root = tempfile::tempdir().unwrap();
        let mut layout = super::super::tests::test_layout(root.path());
        layout.dst_connection_string =
            "host=db.example.com sslmode=verify-full sslrootcert=/tmp/ca.pem".to_string();
        layout.dst_user = Some("layoutuser".to_string());
        layout.dst_password = Some("layoutpw".to_string());
        let cfg = parse_config(&build_postgres_connection_string(&layout)).unwrap();
        assert_eq!(cfg.get_user(), Some("layoutuser"));
        assert_eq!(cfg.get_password(), Some(&b"layoutpw"[..]));
        assert_eq!(cfg.get_ssl_mode(), SslMode::Require);
    }

    #[test]
    fn client_side_connection_failure_reports_cause_without_credentials() {
        // Bind then drop so the port is closed and the connect is refused.
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let root = tempfile::tempdir().unwrap();
        let mut layout = super::super::tests::test_layout(root.path());
        layout.dst_connection_string =
            format!("host=127.0.0.1 port={port} sslmode=disable connect_timeout=2");
        layout.dst_user = Some("someuser".to_string());
        layout.dst_password = Some("s3cretpw".to_string());
        let message = connect_postgres(&layout).err().expect("must fail").to_string();
        assert!(message.contains("SQLSTATE=none"), "{message}");
        assert!(message.contains("cause:"), "cause missing: {message}");
        assert!(!message.contains("s3cretpw"), "{message}");
    }

    #[test]
    fn checkpoint_verify_full_negotiates_tls_and_obeys_deadline() {
        use std::io::Read;
        use std::time::{Duration, Instant};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        socket.set_nonblocking(false).unwrap();
                        socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
                        let mut request = [0; 8];
                        socket.read_exact(&mut request).unwrap();
                        let _ = socket.read(&mut [0]);
                        return Some(request);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if Instant::now() >= deadline {
                            return None;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("accept failed: {error}"),
                }
            }
        });
        let started = Instant::now();
        let result = read_postgres_checkpoint(
            &format!("host=127.0.0.1 port={port} user=test sslmode=verify-full"),
            "synclite_checkpoint",
            "device-id",
            "device-name",
            Some(Duration::from_millis(150)),
        );
        let elapsed = started.elapsed();
        let request = server.join().unwrap();
        assert_eq!(request, Some([0, 0, 0, 8, 4, 210, 22, 47]));
        assert!(result.is_none());
        assert!(elapsed >= Duration::from_millis(150), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(1), "{elapsed:?}");
    }

    #[test]
    fn checkpoint_zero_timeout_returns_without_connecting() {
        assert!(read_postgres_checkpoint(
            "host=127.0.0.1 user=test sslmode=verify-full",
            "synclite_checkpoint", "device-id", "device-name",
            Some(std::time::Duration::ZERO),
        ).is_none());
    }

    #[test]
    fn parameter_names_ignore_quoted_passwords_and_accept_whitespace() {
        assert!(!has_param("host=localhost password='secret connect_timeout=3'", "connect_timeout"));
        assert!(has_param("host=localhost\tconnect_timeout = '0'", "connect_timeout"));
        assert!(has_param("postgresql://localhost/db?connect_timeout=0", "connect_timeout"));
        assert!(has_param("postgresql://localhost/db?connect%5Ftimeout=0", "connect_timeout"));
        assert!(!has_param("postgresql://localhost/db?password=a%20connect_timeout%3D1", "connect_timeout"));
    }

    #[test]
    fn credential_encoding_round_trips_in_both_formats() {
        let secret = "s p'a\\ss&=é";
        for base in ["host=localhost", "postgresql://localhost/db"] {
            let mut out = base.to_string();
            append_param(&mut out, "user", "a user");
            append_param(&mut out, "password", secret);
            let config = parse_config(&out).unwrap();
            assert_eq!(config.get_user(), Some("a user"));
            assert_eq!(config.get_password(), Some(secret.as_bytes()));
        }
    }

    #[test]
    fn invalid_configuration_never_echoes_credentials() {
        let error = parse_config("host=localhost password=TOP_SECRET sslmode=TOP_SECRET").unwrap_err().to_string();
        assert!(!error.contains("TOP_SECRET"));
    }
}