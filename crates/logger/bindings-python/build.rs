fn main() {
    let v = std::env::var("SYNCLITE_RUST_ARTIFACT_VERSION")
        .unwrap_or_else(|_| std::env::var("CARGO_PKG_VERSION").unwrap());
    println!("cargo:rustc-env=SYNCLITE_VERSION={v}");
    println!("cargo:rerun-if-env-changed=SYNCLITE_RUST_ARTIFACT_VERSION");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").ok();
    let target_env = std::env::var("CARGO_CFG_TARGET_ENV").ok();

    if target_os.as_deref() == Some("windows") && target_env.as_deref() == Some("msvc") {
        // Delay-load duckdb.dll so importing the Python extension for
        // SQLite-only usage does not fail when DuckDB runtime files are absent.
        println!("cargo:rustc-link-arg=/DELAYLOAD:duckdb.dll");
        println!("cargo:rustc-link-lib=delayimp");
    }
}
