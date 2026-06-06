# Rust Samples

All examples in this folder are self-contained files with no shared helper module.

## Run

From synclite-logger-rust:

- `cargo run -p synclite --example synclite_rusqlite`
- `cargo run -p synclite --example synclite_duckdb`
- `cargo run -p synclite --example synclite_rusqlite_store`
- `cargo run -p synclite --example synclite_duckdb_store`
- `cargo run -p synclite --example synclite_streaming`

These are reference samples for using SyncLite wrappers as drop-in replacements in existing Rust codebases that currently use `rusqlite` or `duckdb-rs` coding patterns.

Both samples show explicit initialize first via `initialize(device_type, device_name, db_path, destination, options)`, where `device_name` is an alphanumeric `&str`, `destination` is `Option<DestinationOptions>` and `options` is `SyncLiteOptions`, then open a wrapper connection and run normal SQL operations.

Store samples also do the same initialize flow, but use `SQLITE_STORE` / `DUCKDB_STORE` in a self-generated config file before opening wrapper connections.

## Included Samples

- `synclite_rusqlite.rs`: `synclite::rusqlite::Connection` replacement style.
- `synclite_duckdb.rs`: `synclite::duckdb::Connection` replacement style.
- `synclite_rusqlite_store.rs`: SQLite wrapper sample in STORE device mode.
- `synclite_duckdb_store.rs`: DuckDB wrapper sample in STORE device mode.
- `synclite_streaming.rs`: SQLite wrapper sample in STREAMING device mode.

