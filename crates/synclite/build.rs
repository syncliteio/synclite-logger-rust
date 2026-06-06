fn main() {
    let v = std::env::var("SYNCLITE_RUST_ARTIFACT_VERSION")
        .unwrap_or_else(|_| std::env::var("CARGO_PKG_VERSION").unwrap());
    println!("cargo:rustc-env=SYNCLITE_VERSION={v}");
    println!("cargo:rerun-if-env-changed=SYNCLITE_RUST_ARTIFACT_VERSION");
}
