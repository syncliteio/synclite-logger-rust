fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").ok();
    let target_env = std::env::var("CARGO_CFG_TARGET_ENV").ok();

    if target_os.as_deref() == Some("windows") && target_env.as_deref() == Some("msvc") {
        // Delay-load duckdb.dll so requiring the Node addon for SQLite-only
        // usage does not fail when DuckDB runtime files are absent.
        println!("cargo:rustc-link-arg=/DELAYLOAD:duckdb.dll");
        println!("cargo:rustc-link-lib=delayimp");
    }
}
