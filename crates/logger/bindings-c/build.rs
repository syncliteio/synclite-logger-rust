fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").ok();
    let target_env = std::env::var("CARGO_CFG_TARGET_ENV").ok();

    if target_os.as_deref() == Some("windows") && target_env.as_deref() == Some("msvc") {
        // Delay-load duckdb.dll so SQLite-only programs can start without
        // requiring DuckDB runtime files on the loader path.
        println!("cargo:rustc-link-arg=/DELAYLOAD:duckdb.dll");
        println!("cargo:rustc-link-lib=delayimp");
    }
}
