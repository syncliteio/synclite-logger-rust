# SyncLite for Rust

> Part of the [SyncLite Platform](https://github.com/syncliteio/SyncLite) — Build Anything, Sync Anywhere.

**The entire SyncLite runtime packaged as a single embeddable Rust library.**

`synclite` brings the full SyncLite runtime — local SQL device, change capture,
segment shipping, consolidator, and destination apply — into one crate that
you add to `Cargo.toml`, point at a `synclite.conf`, and start writing rows.
No external services to deploy, no JVM, no sidecar processes. Local-first
behavior on day one, end-to-end synchronization to your destination of choice
on day two — same binary, same API.

## What's In The Box

A normal SyncLite deployment is multiple Java services (logger + shipper +
consolidator + destination apply + Prometheus). The Rust crate folds all of
them into one in-process runtime:

| Capability | Java component | Rust equivalent |
|---|---|---|
| Local SQL device (SQLite / DuckDB) | `synclite-logger` | `crates/logger/db-sqlite`, `crates/logger/db-duckdb` |
| Change capture into `.sqllog` segment files | `synclite-logger` | `crates/logger/log` |
| Async shipper to staging (FS / S3 / SFTP) | `synclite-logger` | `crates/logger/shipper`, `crates/logger/archiver` |
| Per-device consolidator + destination apply | `synclite-consolidator` | `crates/consolidator/runtime` |
| Filter / Value / Data-type mappers | `synclite-consolidator` | `crates/consolidator/core` + runtime |
| Prometheus statistics publisher | `PrometheusDumper` | `crates/observability`, `consolidator_runtime::monitor` |
| C ABI for embedding in other languages | `synclite-logger` JNI | `crates/logger/bindings-c` |

Open one `Connection` and you get:

- A working **local SQLite or DuckDB database** for your application reads/writes.
- **Atomic change capture** of every committed mutation into ordered segments.
- A **background shipper** that moves sealed segments to staging.
- An **embedded consolidator** that reads those segments and applies the
  consolidated stream to your destination — SQLite, DuckDB, or PostgreSQL.
- All of it driven from one `synclite.conf` file.

## Why Rust SyncLite

- **Single dependency.** Add `synclite` to `Cargo.toml`; you have the entire
  runtime. No services to install, no Docker network to wire up.
- **Local-first by default.** Your app writes to a local DB and keeps working
  even when the network is gone. Segments queue up and ship when staging is
  reachable again.
- **Drop-in API.** `rusqlite`- and `duckdb`-style wrappers — `Connection`,
  `execute`, `query`, `commit`, prepared-statement batches — so existing code
  ports with minimal churn.
- **Edge / embedded friendly.** Pure Rust, statically linkable, small binary,
  no runtime dependencies. Runs the same on a developer laptop, a CI box, or
  an IoT gateway.
- **All-inclusive data movement.** Captures and propagates DML *and* DDL,
  fully transactional, schema-aware and schema-adaptive — the destination
  schema evolves automatically alongside the source, no manual migrations.
- **Rich consolidator feature set.** Filter mapper, value mapper, data-type
  mapping, sync modes, metadata store placement, and Prometheus push are all
  available out of the box and configured through the same `synclite.conf`
  file.
- **C ABI included.** `crates/logger/bindings-c` exposes the same runtime
  through a C header for use from C/C++, Go, Python, etc.

## Architecture

```text
+---------------------------------------------------------------+
|  Your Rust application                                        |
|     |                                                         |
|     v                                                         |
|  synclite::Connection  (rusqlite-/duckdb-style API)           |
|     |                                                         |
|     +--> Local DB file (SQLite or DuckDB)  <-- your reads/writes
|     +--> Change capture --> .sqllog segments                  |
|                                  |                            |
|                                  v                            |
|                       Shipper (async)                         |
|                                  |                            |
|                                  v                            |
|                       Stage  (FS / S3 / SFTP)                 |
|                                  |                            |
|                                  v                            |
|         Embedded Consolidator  /  Standalone Consolidator     |
|                                  |                            |
|                                  v                            |
|              Destination apply: SQLite / DuckDB / PostgreSQL  |
+---------------------------------------------------------------+
```

Everything inside the box is one Rust process. `Connection::open_with_config`
spins up the logger, the shipper, the consolidator workers, and the
Prometheus publisher according to the config — and shuts them all down
cleanly on `close()`.

## Supported Device Types

- `SQLITE`, `DUCKDB` — **SQL devices**: full DML and DDL surface for
  general-purpose transactional workloads with arbitrary SQL.
- `SQLITE_STORE`, `DUCKDB_STORE` — **Store devices**: simplified key/value-
  style API over a SQL backend, with the same transactional guarantees;
  processed through the event-streamer path with full metadata extraction.
- `STREAMING` — **Streaming device**: append-oriented ingestion (also
  transactional); `UPDATE`/`DELETE` are rejected by design.

All three device classes are fully transactional, schema-aware, and
schema-adaptive — they capture and propagate `CREATE TABLE`,
`ALTER TABLE ADD/RENAME/DROP COLUMN`, `RENAME TABLE`, and other DDL
alongside data, so the destination schema evolves automatically as the
source schema changes. They all feed the same downstream destinations and
honor all three mappers (filter / value / data-type).

## Supported Destinations

- **SQLite** — file or in-memory.
- **DuckDB** — file or in-memory.
- **PostgreSQL** — via `dst-connection-string-N`.

## Install

Add to `Cargo.toml`:

```toml
[dependencies]
synclite = { path = "path/to/synclite-logger-rust/crates/synclite" }
```

Or build from the workspace root:

```powershell
cargo build --workspace
```

### Platform packaging (multi-arch cdylibs)

Building the umbrella SyncLite platform from the repo root with Maven
produces a complete, multi-arch native bundle under
`target/synclite-platform-<rev>/lib/native/`:

```
libsynclite_<rev>.dll                  # Windows host build
libsynclite_<rev>.lib                  # Windows import library
libsynclite_<rev>_linux_x86_64.so      # cross-compiled with cargo-zigbuild
libsynclite_<rev>_linux_aarch64.so     # cross-compiled with cargo-zigbuild
libsynclite_<rev>.dylib                # only when built on a macOS host
```

The Linux cdylibs are cross-compiled on every host by default (no
profile flag needed) using [`cargo-zigbuild`](https://github.com/rust-cross/cargo-zigbuild)
so a single `mvn -Drevision=<rev> package` ships a Windows + Linux
x86_64 + Linux aarch64 payload from one machine.

Required build-host prerequisites (in addition to Rust + Cargo):

- `cargo-zigbuild` Cargo subcommand
- [Zig](https://ziglang.org/download/) compiler on `PATH`
- Rust std libs for the Linux targets

One-time host setup:

```bash
cargo install cargo-zigbuild
rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu
# zig must be on PATH — download from https://ziglang.org/download/
```

> If `mvn package` fails with `error: no such command: zigbuild`, you are
> missing `cargo-zigbuild` — run `cargo install cargo-zigbuild` and retry.

macOS `.dylib` still requires running the build on a macOS host — the
Apple SDK is not redistributable. Run the same Maven build on macOS to
produce `libsynclite_<rev>.dylib`.

## Quick Start — Local SQLite Replicated To PostgreSQL

One `initialize` call, four lines of SQL. SyncLite captures every change
locally and the embedded consolidator streams it into PostgreSQL in the
background.

```rust
use synclite::rusqlite::Connection;
use synclite::{DestinationOptions, DeviceType, DstSyncMode, DstType, Result, SyncLiteOptions, Value};

fn main() -> Result<()> {
    const DB_PATH: &str = "sample.db";
    const DEVICE_NAME: &str = "sampledevice";

    synclite::initialize(
        DeviceType::SQLITE,
        DEVICE_NAME,
        DB_PATH,
        Some(DestinationOptions {
            dst_type: DstType::Postgres,
            dst_connection_string:
                "postgresql://postgres:postgres@localhost:5432/syncdb".into(),
            dst_database: Some("syncdb".into()),
            dst_schema: Some("syncschema".into()),
            dst_sync_mode: DstSyncMode::Consolidation,
        }),
        SyncLiteOptions::default(),
    )?;

    let mut conn = Connection::open(DB_PATH)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )?;
    conn.execute(
        "INSERT INTO users(id, name) VALUES(?, ?)",
        &[Value::Int(1), Value::Text("alice".into())],
    )?;
    conn.commit()?;

    // Force the active log segment to roll, then block until the in-process
    // shipper + consolidator have fully applied it to PostgreSQL.
    conn.flush()?;
    synclite::await_sync(DB_PATH, std::time::Duration::from_secs(30))?;
    conn.close()?;
    Ok(())
}
```

No config file required. SyncLite uses sensible defaults out of the box:

- Local stage directory: `<user-home>/synclite/job1/stageDir`
- Consolidator work directory: `<user-home>/synclite/job1/workDir`

Run it once and the `users` table — including the `CREATE TABLE` DDL —
shows up in your PostgreSQL database automatically. Change the table later
with `ALTER TABLE users ADD COLUMN ...` and the destination schema adapts
on its own.

### Local-Only Variant

If you just want local-first behavior without a destination, drop the
`DestinationOptions`:

```rust
synclite::initialize(
    DeviceType::SQLITE,
    DEVICE_NAME,
    DB_PATH,
    None,
    SyncLiteOptions::default(),
)?;
```

Your app gets a SQLite database plus a durable, ordered log of every
change, ready to be consolidated whenever you wire up a destination
later — no code change required, just pass `DestinationOptions`. The
same change stream can also be picked up by the centralized
[SyncLite Consolidator](https://github.com/syncliteio/SyncLite/tree/main/synclite-consolidator)
service, which fans out from many edge devices into a shared destination.

### Config-File Variant

For richer setups (multiple destinations, mappers, Prometheus, tuned
shipper, alternate stage transport), point SyncLite at a `synclite.conf`:

```rust
synclite::initialize(
    DeviceType::SQLITE,
    DEVICE_NAME,
    DB_PATH,
    None,
    SyncLiteOptions {
        config_path: Some("synclite.conf".into()),
        ..Default::default()
    },
)?;
```

```properties
# synclite.conf
destination-count=1
dst-type-1=POSTGRESQL
dst-connection-string-1=postgresql://user:password@localhost:5432/analytics
dst-sync-mode-1=CONSOLIDATION
dst-data-type-mapping-1=BEST_EFFORT
dst-enable-filter-mapper-rules-1=true
dst-filter-mapper-rules-file-1=./filter_rules.conf
```

## Feature Highlights

### Filter Mapper

Per-destination table allow/block/rename and column allow/block, driven by
a properties file — same syntax as Java:

```properties
dst-enable-filter-mapper-rules-1=true
dst-filter-mapper-rules-file-1=./filter_rules.conf
dst-allow-unspecified-tables-1=false
dst-allow-unspecified-columns-1=true
```

```properties
# filter_rules.conf
orders=true
audit_log=false
legacy_users=users_v2
orders.secret_token=false
```

### Value Mapper

Per-`(table, column)` value substitution at apply time:

```properties
dst-enable-value-mapper-1=true
dst-value-mappings-file-1=./value_mappings.json
```

### Data-Type Mapper

Four modes — `ALL_TEXT`, `BEST_EFFORT` (default), `CUSTOMIZED`, `EXACT` —
control how source SQLite/DuckDB column types are projected onto the
destination:

```properties
dst-data-type-mapping-1=BEST_EFFORT
```

### Sync Modes

- `CONSOLIDATION` — destination table carries `synclite_device_id`,
  `synclite_device_name`, `synclite_update_timestamp` bookkeeping; many
  source devices fan in to one consolidated table.
- `REPLICATION` — destination mirrors the source schema row-for-row.

```properties
dst-sync-mode-1=CONSOLIDATION
```

### Prometheus Integration

```properties
enable-prometheus-statistics-publisher=true
prometheus-push-gateway-url=http://localhost:9091
prometheus-statistics-publisher-interval-s=30
```

The runtime publishes the same metric set as the centralized
[SyncLite Consolidator](https://github.com/syncliteio/SyncLite/tree/main/synclite-consolidator),
so existing dashboards and alerts work unchanged.

### Multiple Destinations

Fan a single source out to many destinations — each with its own mappers,
sync mode, data-type mode:

```properties
destination-count=3
dst-type-1=SQLITE
dst-type-2=DUCKDB
dst-type-3=POSTGRESQL
```

### Crash Recovery & Restart Semantics

Segments are written, sealed, and shipped under crash-safe rules. On
restart, the recovery path reconciles in-flight transactions with the
last committed segment, mirroring Java's restart semantics.

### Reinitialize

Wipe a device's local state and (optionally) its destination tables and
bring it back up as the same logical device with a fresh segment
sequence. Useful for development, resetting a stuck device, or
recovering from a corrupt local stage.

```rust
// Preserve destination data; only local state is wiped.
synclite::reinitialize(DB_PATH, false)?;

// Clean destination too. In REPLICATION mode the user tables owned
// by this device are dropped; in CONSOLIDATION mode dropping is a
// no-op (the destination is shared across many devices and dropping
// would be catastrophic for siblings).
synclite::reinitialize(DB_PATH, true)?;
```

UUID, device-name, device-type, and destination wiring are preserved
across the call, and `dst-idempotent-data-ingestion-1=true` is flipped
on so the re-seed tolerates any rows the destination still holds.

**Trigger-file protocol.** Drop one of the following alongside the
database file and the next `synclite::initialize` call runs the
matching reinit, then removes the trigger:

```text
reinitialize.<device-name>                          # preserve dst
reinitialize_with_clean_destination.<device-name>   # clean dst
```

This is how out-of-process tooling (orchestrators, CI scripts, even a
shell on the same box) can force a reinit on the next bring-up without
linking against the crate.

### Pause / Resume Sync

Halt destination consolidation for a device without stopping the
logger. While paused, the in-process logger keeps appending segments
locally and the shipper keeps publishing them to the upload root —
only the consolidator's apply step is held back. On `resume_sync` the
queued segments drain in order.

```rust
synclite::pause_sync(DB_PATH)?;
assert!(synclite::is_sync_paused(DB_PATH)?);

// ...application keeps writing; segments accumulate but don't reach
//    the destination database...

synclite::resume_sync(DB_PATH)?;
synclite::await_sync(DB_PATH, std::time::Duration::from_secs(60))?;
```

Both calls are idempotent. The paused state is persisted in a sentinel
file under the device home, so it survives process restarts.

**Trigger-file protocol.** Like reinitialize, pause/resume can be
toggled from outside the process by dropping a marker next to the DB:

```text
pause_sync.<device-name>     # pauses on next synclite::initialize
resume_sync.<device-name>    # resumes on next synclite::initialize
```

### Sync Status, Latency, Statistics

Three read-only inspection APIs report what the consolidator is doing
for a device. They open SQLite files the consolidator has already
produced — no workers are started and no destination round-trips are
made.

```rust
let st = synclite::sync_status(DB_PATH)?;
// st.state is SyncState::NotInitialized | Paused | Running
// st.status / st.status_description / st.last_heartbeat_time_ms come
// from the consolidator's last heartbeat row.

let s = synclite::sync_statistics(DB_PATH)?;
// log_segments_applied, processed_oper_count, processed_txn_count,
// processed_log_size, last_consolidated_commit_id,
// last_heartbeat_time_ms.

let l = synclite::sync_latency(DB_PATH)?;
// l.source_commit_id  = MAX(commit_id) from device synclite_txn
// l.applied_commit_id = last commit_id applied at the destination
// l.latency_ms        = source - applied (wall-clock ms); -1 when the
//                       applied side is unknown (e.g. destination
//                       unreachable or consolidator not running yet).
```

Because every `commit_id` is a `System.currentTimeMillis()` value
emitted by the logger, `latency_ms` is the actual wall-clock sync lag.

## Configuration Keys

SyncLite reads a Java-properties file (typically `synclite.conf`).
Local-only essentials:

| Key | Purpose |
|---|---|
| `device-name` | Logical device identifier. |
| `device-type` | `SQLITE`, `DUCKDB`, `SQLITE_STORE`, `DUCKDB_STORE`, `STREAMING`. |
| `db-engine` | `SQLITE` or `DUCKDB`. |
| `db-path` | Path to the local DB file. |
| `local-data-stage-directory` | Where the logger writes `.sqllog` segments. |
| `device-data-root` | Work-dir root for the embedded consolidator. |
| `log-segment-page-size` | Segment rotation granularity. |
| `log-segment-shipping-frequency-ms` | Shipper poll interval. |
| `max-inlined-log-args` | Args inlined before promotion to blob storage. |
| `skip-restart-recovery` | Skip recovery on cold start (testing only). |

Per-destination (index `N` from `1` to `destination-count`):

| Key pattern | Purpose |
|---|---|
| `dst-type-N` | `SQLITE`, `DUCKDB`, `POSTGRESQL`. |
| `dst-connection-string-N` | JDBC-style connection string. |
| `dst-sync-mode-N` | `CONSOLIDATION` or `REPLICATION`. |
| `dst-data-type-mapping-N` | `ALL_TEXT` / `BEST_EFFORT` / `CUSTOMIZED` / `EXACT`. |
| `dst-enable-filter-mapper-rules-N` | Toggle filter mapper. |
| `dst-filter-mapper-rules-file-N` | Path to filter rules file. |
| `dst-allow-unspecified-tables-N` | Default-allow vs default-block tables. |
| `dst-allow-unspecified-columns-N` | Default-allow vs default-block columns. |
| `dst-enable-value-mapper-N` | Toggle value mapper. |
| `dst-value-mappings-file-N` | Path to value mappings file. |
| `metadata-store-N` / `dst-metadata-store-N` | Where to keep consolidator metadata. |

Prometheus:

| Key | Purpose |
|---|---|
| `enable-prometheus-statistics-publisher` | Turn on metric push. |
| `prometheus-push-gateway-url` | Push-gateway URL. |
| `prometheus-statistics-publisher-interval-s` | Publish interval (seconds). |

## Examples

```powershell
cargo run -p synclite --example synclite_rusqlite
cargo run -p synclite --example synclite_duckdb
cargo run -p synclite --example synclite_rusqlite_store
cargo run -p synclite --example synclite_duckdb_store
cargo run -p synclite --example synclite_streaming
cargo run -p synclite --example synclite_device_artifacts_demo
```

Source: [crates/synclite/examples/](crates/synclite/examples/).

End-to-end runnable samples (Rust, Python, C++) — including the marquee
**SQLite → PostgreSQL** demo — live under [samples/](samples/):

- [samples/rust/](samples/rust/) — Cargo package with one runnable example per
  device type plus `synclite_rusqlite_postgres` (`cargo run --example
  synclite_rusqlite_postgres`).
- [samples/python/](samples/python/) — uses the `synclite` PyO3 wheel; same
  sample set, same names.
- [samples/cpp/](samples/cpp/) — CMake project that links the `synclite-c`
  cdylib through the header-only `synclite.hpp` RAII wrapper.

The same trio is also published at the top of the platform repo under
[../synclite-code-samples/synclite-logger/](../synclite-code-samples/synclite-logger/).

## Language Bindings

The Rust crate is the source of truth; everything else is a thin layer over
the same runtime, so any sample written in one language ports trivially to
the others.

| Language | Crate / package | Header | Notes |
|---|---|---|---|
| **Rust** | `synclite` ([crates/synclite/](crates/synclite/)) | — | Native API. `rusqlite`- / `duckdb`-style `Connection` + `Statement`. |
| **Python** | `synclite` (PyO3 wheel built from [crates/logger/bindings-python/](crates/logger/bindings-python/)) | — | `import synclite as sl`. Matches the Rust API 1:1 — `sl.initialize`, `sl.Connection.open`, `sl.await_sync`. Build with `maturin develop` from [python/](python/). |
| **C / C++** | `synclite-c` cdylib + staticlib ([crates/logger/bindings-c/](crates/logger/bindings-c/)) | [include/synclite.h](include/synclite.h), [include/synclite.hpp](include/synclite.hpp) | C ABI for any FFI-capable language. The C++17 header (`synclite.hpp`) is a header-only RAII wrapper — `synclite::Connection`, `synclite::Statement`, `synclite::Value`. Build with `cargo build -p synclite-c [--release]`. |
| **Java** | `io.synclite` (JDBC) | — | Lives in the sibling [synclite-logger-java](../synclite-logger-java/) project; uses the same on-disk segment format and ships through the same staging/consolidator pipeline. |
| **Any (HTTP)** | [SyncLite DB](../synclite-db/) | — | Language-agnostic HTTP/JSON server fronting the same runtime. |

## Test

```powershell
cargo test -p synclite --tests
```

The `device_integration` suite exercises every device type against every
destination through the full local → segment → ship → consolidate → apply
pipeline, including the filter / value / data-type mappers.

## Workspace Layout

- `crates/synclite` — top-level public API, `Connection` wrappers, examples.
- `crates/logger/core` — shared types, errors, SQL policy.
- `crates/logger/config` — `synclite.conf` parser (Java key compatibility).
- `crates/logger/log` — segment writer and scan.
- `crates/logger/db-sqlite` — SQLite device backend.
- `crates/logger/db-duckdb` — DuckDB device backend.
- `crates/logger/runtime` — logger selection and async wrapper.
- `crates/logger/shipper` — shipper worker and cleaner.
- `crates/logger/archiver` — staging archivers: filesystem, S3, SFTP.
- `crates/logger/bindings-c` — C ABI for embedding from other languages.
- `crates/consolidator/core` — consolidator types, layout, mapper rules.
- `crates/consolidator/state` — state DB, checkpoints, bootstrap helpers.
- `crates/consolidator/runtime` — worker loop, event streamer, destination
  apply engine, Prometheus publisher.
- `crates/observability` — metrics registry shared across the runtime.

## Build Artifacts

A release build (`cargo build --release --workspace`) produces the following
under `target/release/`:

| Artifact | Source crate | Crate type | Consumer |
|----------|--------------|------------|----------|
| `libsynclite.rlib` | `synclite` | `rlib` | Rust apps via `synclite = "0.1"` in `Cargo.toml` |
| `synclite_c.dll` / `libsynclite_c.so` / `libsynclite_c.dylib` | `synclite-c` | `cdylib` | C / C++ / Go / Python (cffi) / any FFI host |
| `synclite_c.lib` / `libsynclite_c.a` | `synclite-c` | `staticlib` | Static linking into native binaries |
| Component `.rlib`s (`logger-core`, `logger-runtime`, `consolidator-runtime`, …) | individual crates | `rlib` | Internal; re-exported through `synclite` |

Most users only need the top-level `synclite` crate. The C ABI artifacts
from `synclite-c` exist for embedding into non-Rust hosts.

### The `synclitecdc` Native Helper

SQL devices rely on a small native shared library, `synclitecdc`, to
generate CDC records.

**The library ships inside the `synclite` crate.** Prebuilts for the
supported targets — Linux x86_64 / x86 and Windows x86_64 / x86 — are
embedded directly into the crate (`crates/synclite/native/`) and the
right one is selected at compile time for your host. On the first call
to `SyncLite::initialize`, the runtime extracts the binary to
`<temp>/synclite-cdc-<crate-version>/` and points the loader at it via
`SYNCLITE_CDC_LIB_DIR`. No extra files to bundle, no extra steps for
your end users — `cargo add synclite` is enough.

On hosts without an embedded prebuilt (e.g., macOS today),
the loader falls back to its standard search path: `SYNCLITE_CDC_LIB_DIR`,
the workspace `native/` directory, then the system loader path.

## Workspace Layout

- Repository / workspace: `synclite-logger-rust`
- Published package: `synclite`
- Public alias: `SyncLite`
- Backward-compatible type: `Logger`

## Related Projects

- [SyncLite Platform](https://github.com/syncliteio/SyncLite) — umbrella repository and platform documentation.
- [SyncLite Consolidator](https://github.com/syncliteio/SyncLite/tree/main/synclite-consolidator) — standalone Java consolidator service.
- [SyncLite Logger (Java)](https://github.com/syncliteio/SyncLite/tree/main/synclite-logger-java) — original Java/JDBC logger.
- [SyncLite DB](https://github.com/syncliteio/SyncLite/tree/main/synclite-db) — embeddable multi-engine SyncLite database server.
- [SyncLite DBReader](https://github.com/syncliteio/SyncLite/tree/main/synclite-dbreader) — source-database CDC reader feeding the same consolidator.
