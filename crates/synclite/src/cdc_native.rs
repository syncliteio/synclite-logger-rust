//! Ships the `synclitecdc` native helper with the `synclite` crate.
//!
//! The consolidator runtime dlopens `synclitecdc` to drive preupdate-hook
//! based CDC capture for SQL devices. The matching prebuilt for the host
//! `(target_os, target_arch)` is embedded into this crate at build time;
//! `ensure_extracted` writes it to a stable per-version temp directory on
//! first use and sets `SYNCLITE_CDC_LIB_DIR` so the loader picks it up.
//!
//! Hosts not covered by an embedded prebuilt (e.g. macOS) fall through:
//! the loader keeps searching the workspace `native/` directory, the
//! system loader path, and any user-supplied `SYNCLITE_CDC_LIB_DIR`.

use std::env;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::OnceLock;

const ENV_VAR: &str = "SYNCLITE_CDC_LIB_DIR";

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const EMBEDDED: Option<(&str, &[u8])> = Some((
    "libsynclitecdc_x86_64.so",
    include_bytes!("../native/libsynclitecdc_x86_64.so"),
));

#[cfg(all(target_os = "linux", target_arch = "x86"))]
const EMBEDDED: Option<(&str, &[u8])> = Some((
    "libsynclitecdc_x86.so",
    include_bytes!("../native/libsynclitecdc_x86.so"),
));

#[cfg(all(target_os = "windows", target_arch = "x86_64"))]
const EMBEDDED: Option<(&str, &[u8])> = Some((
    "synclitecdc_x86_64.dll",
    include_bytes!("../native/synclitecdc_x86_64.dll"),
));

#[cfg(all(target_os = "windows", target_arch = "x86"))]
const EMBEDDED: Option<(&str, &[u8])> = Some((
    "synclitecdc_x86.dll",
    include_bytes!("../native/synclitecdc_x86.dll"),
));

#[cfg(not(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "x86"),
    all(target_os = "windows", target_arch = "x86_64"),
    all(target_os = "windows", target_arch = "x86"),
)))]
const EMBEDDED: Option<(&str, &[u8])> = None;

static EXTRACTED_DIR: OnceLock<Option<PathBuf>> = OnceLock::new();

pub fn ensure_extracted() -> Option<PathBuf> {
    EXTRACTED_DIR
        .get_or_init(|| {
            // Honor an existing override.
            if env::var_os(ENV_VAR).is_some() {
                return None;
            }
            let (name, bytes) = EMBEDDED?;
            let dir = env::temp_dir().join(format!("synclite-cdc-{}", env!("SYNCLITE_VERSION")));
            if let Err(err) = fs::create_dir_all(&dir) {
                tracing::warn!(error = %err, dir = %dir.display(), "failed to create synclitecdc extract dir");
                return None;
            }
            let target = dir.join(name);
            let needs_write = match fs::metadata(&target) {
                Ok(meta) => meta.len() != bytes.len() as u64,
                Err(_) => true,
            };
            if needs_write {
                let tmp = dir.join(format!("{name}.tmp"));
                let write_ok = (|| -> std::io::Result<()> {
                    let mut f = fs::File::create(&tmp)?;
                    f.write_all(bytes)?;
                    f.sync_all()?;
                    fs::rename(&tmp, &target)?;
                    Ok(())
                })();
                if let Err(err) = write_ok {
                    tracing::warn!(error = %err, "failed to extract embedded synclitecdc");
                    return None;
                }
            }
            env::set_var(ENV_VAR, &dir);
            Some(dir)
        })
        .clone()
}
