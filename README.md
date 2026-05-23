# SyncLite Logger for Rust

Part of the SyncLite platform. This workspace provides the Rust logger runtime that captures local SQL activity, writes compact segment files, and ships them to staging so downstream SyncLite services can ingest and consolidate data.

## Why Use It

SyncLite for Rust is designed for edge and embedded workloads where applications must keep writing locally and synchronize reliably in the background.

- Keep your app logic local-first with SQLite or DuckDB.
- Capture SQL mutations automatically into ordered segment files.
- Ship segments to a staging target for consolidation.
- Keep a familiar wrapper API for rusqlite-style and duckdb-style usage.

## Data Flow

```text
Rust App + SyncLite wrapper + Local DB
            |
            v
     Segment files (.sqllog)
            |
            v
      Stage directory / archiver
            |
            v
   SyncLite Consolidator / destination
```

## Device Types

- SQL devices (`SQLITE`, `DUCKDB`): full SQL behavior for general transactional workloads.
- Store devices (`SQLITE_STORE`, `DUCKDB_STORE`): constrained DML model for deterministic apply downstream.
- Streaming device (`STREAMING`): append-oriented ingestion; UPDATE and DELETE are rejected.

## Install And Build

From the Rust workspace root:

```powershell
cargo build --workspace
```

The public package is `synclite` and the primary top-level alias is `SyncLite`.

## Configuration

SyncLite reads a Java-properties style file, usually named `synclite_logger.conf`.

For the usual initialize-first flow, keep the config simple and point SyncLite at a local stage directory:

```properties
local-data-stage-directory=./synclite-stage
```

Add a destination only when you want background shipping enabled, for example:

```properties
destination-fs-target-dir=./target
```

If you use config-driven open helpers such as `Connection::open_with_config(...)` or `Connection::initialize_with_config(...)`, then the config file also needs the device-specific fields, typically:

- `device-name`
- `db-engine`
- `device-type`
- `db-path`

Other commonly used options include:

- `log-segment-page-size`
- `log-segment-shipping-frequency-ms`
- `max-inlined-log-args`
- `skip-restart-recovery`

## Quick Start (SQLite Wrapper)

```rust
use synclite::SyncLite;
use synclite::rusqlite::Connection;
use synclite_core::{DeviceType, Result};

fn main() -> Result<()> {
    let db_path = "sample.db";
    let conf_path = "synclite_logger.conf";

    SyncLite::initialize_with_config_path(DeviceType::Sqlite, db_path, conf_path)?;

    let mut conn = Connection::open_with_config(conf_path)?;
    conn.execute("CREATE TABLE IF NOT EXISTS t(id INTEGER, name TEXT)", &[])?;
    conn.execute("INSERT INTO t(id, name) VALUES(1, 'alice')", &[])?;
    conn.commit()?;
    conn.close()?;

    Ok(())
}
```

## Examples

Run from the workspace root:

```powershell
cargo run -p synclite --example synclite_rusqlite
cargo run -p synclite --example synclite_duckdb
cargo run -p synclite --example synclite_rusqlite_store
cargo run -p synclite --example synclite_duckdb_store
cargo run -p synclite --example synclite_streaming
```

Example source files are in `crates/synclite-logger/examples/`.

## Test

The primary test suite is the end-to-end integration suite for the public `synclite` package. It creates real devices, executes SQL through the wrappers, verifies log generation, and checks that segments and metadata are staged correctly.

```powershell
cargo test -p synclite --tests
```

## Workspace Layout

- `crates/synclite-core`: shared types, errors, SQL policy.
- `crates/synclite-config`: config parser for `synclite_logger.conf`.
- `crates/synclite-log`: segment writer and scan logic.
- `crates/synclite-db-sqlite`: SQLite device backend.
- `crates/synclite-db-duckdb`: DuckDB device backend.
- `crates/synclite-logger`: top-level public API and wrappers.
- `crates/synclite-runtime`: runtime logger selection and async wrapper.
- `crates/synclite-shipper`: shipper worker and cleaner.
- `crates/synclite-archiver`: pluggable archive destinations.
- `crates/synclite-bindings-c`: C FFI bindings.

## Naming

- Repository/workspace: `synclite-logger-rust`
- Published package: `synclite`
- Public alias: `SyncLite`
- Backward-compatible type: `Logger`

## Related Projects

- `synclite-consolidator`
- `synclite-logger-java`
- platform docs at the repository root
