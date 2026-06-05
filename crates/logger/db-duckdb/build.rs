fn main() {
    // NB: cfg!(target_os = "windows") in a build script evaluates against the
    // HOST that builds build.rs, not the target being compiled. Cross-compiling
    // from Windows to Linux would otherwise leak `-lrstrtmgr` into the link
    // line. Always go through CARGO_CFG_TARGET_OS, which Cargo sets to the
    // current target.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        // DuckDB's `AdditionalLockInfo` uses Windows Restart Manager APIs
        // (RmStartSession / RmEndSession / RmRegisterResources / RmGetList).
        // libduckdb-sys does not emit this link directive itself.
        println!("cargo:rustc-link-lib=dylib=rstrtmgr");
    }
}

