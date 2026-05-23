fn main() {
    #[cfg(target_os = "windows")]
    {
        // DuckDB's `AdditionalLockInfo` uses Windows Restart Manager APIs
        // (RmStartSession / RmEndSession / RmRegisterResources / RmGetList).
        // libduckdb-sys does not emit this link directive itself.
        println!("cargo:rustc-link-lib=dylib=rstrtmgr");
    }
}
