# SyncLite C++ samples

These mirror the Rust samples in `../rust/` and the Python samples in
`../python/` — same shapes, same flows, same destinations — built on
the SyncLite C ABI (`synclite.h`) wrapped in a header-only C++17 RAII
layer (`synclite.hpp`).

## Layout

| file                          | mirrors                              |
|-------------------------------|--------------------------------------|
| `synclite_rusqlite.cpp`       | `synclite_rusqlite.rs`       |
| `synclite_rusqlite_store.cpp` | `synclite_rusqlite_store.rs` |
| `synclite_streaming.cpp`      | `synclite_streaming.rs`      |
| `synclite_duckdb.cpp`         | `synclite_duckdb.rs`         |
| `synclite_duckdb_store.cpp`   | `synclite_duckdb_store.rs`   |

The C ABI and the C++ headers live next to the Rust workspace:

```
synclite-logger-rust/
  include/
    synclite.h        # C ABI
    synclite.hpp      # header-only C++17 RAII wrapper
  crates/logger/bindings-c/   # crate producing the cdylib
```

## Device families

Every sample picks one of three **device families**. The connection and
SQL surface is identical across all three — the only code-level difference
is the `DeviceType` you pass to `synclite::initialize`:

- **SQL device** (`SQLITE`, `DUCKDB`) — a full, SQLite-syntax-compliant
  embedded SQL database. Run arbitrary `CREATE` / `ALTER` / `SELECT` /
  `INSERT` / `UPDATE` / `DELETE`. Reach for it when your app needs real
  SQL, JOINs, multi-statement transactions, or ad-hoc DDL.
  See `synclite_rusqlite.cpp` / `synclite_duckdb.cpp`.
- **Store device** (`SQLITE_STORE`, `DUCKDB_STORE`) — the same SQL-shaped
  API, tuned for bulk write-through. The runtime emits pre-formed row
  events that the Consolidator applies directly to the destination — no
  SQL-log parsing or CDC-deduction on the apply path — so it delivers the
  highest end-to-end consolidation throughput. It's usually the fastest
  *and* simplest starting point for a new app.
  See `synclite_rusqlite_store.cpp` / `synclite_duckdb_store.cpp`.
- **Streaming device** (`STREAMING`) — append-only ingestion for
  high-throughput event capture. Accepts `INSERT` + DDL and rejects
  `UPDATE` / `DELETE` by design. See `synclite_streaming.cpp`.

All three produce the same change log and flow through the same shipper +
consolidator, so you can mix device families inside one application.

## Two ways to consume the SyncLite runtime

You need two things at link/run time:

1. The C ABI header `synclite.h` and the C++ wrapper `synclite.hpp`.
2. The SyncLite cdylib + import library
   (`synclite_c.{dll,lib,so,dylib}`).

You can either **(A)** build them yourself from the Rust workspace, or
**(B)** drop in a prebuilt SDK release.

### Option A — build from source

Useful when you have the platform repo checked out (recommended for
contributors and pre-release tracking).

```pwsh
# 1. Build the cdylib
cd ..\..\..\synclite-logger-rust
cargo build -p synclite-c            # debug
# or: cargo build -p synclite-c --release

# 2. Build the samples
cd ..\synclite-code-samples\synclite-logger\cpp
cmake -S . -B build
cmake --build build --config Debug
```

This produces the SyncLite shared library plus the matching import library.
On Windows the packaged runtime is the single versioned DLL such as
`synclite_1_0_0.dll`, and CMake stages that file next to the sample
executable automatically. On Linux/macOS the corresponding files are
`libsynclite_1_0_0.so` / `libsynclite_1_0_0.dylib`.

Override paths / profile if needed:

```pwsh
cmake -S . -B build `
      -DSYNCLITE_RUST_ROOT=..\..\..\synclite-logger-rust `
      -DSYNCLITE_PROFILE=release
```

### Option B — consume a prebuilt SDK

Useful when you just want to embed SyncLite in a C/C++ project without
a Rust toolchain. Drop in a SyncLite C/C++ SDK with the following
layout:

```
synclite-c-sdk/
  include/
    synclite.h
    synclite.hpp
  lib/
    synclite_1_0_0.dll           # Windows
    synclite_1_0_0.lib           # Windows import library
    libsynclite_1_0_0.so         # Linux  (or libsynclite_1_0_0.dylib on macOS)
```

Then point CMake at it:

```pwsh
cmake -S . -B build `
      -DSYNCLITE_SDK_DIR=C:\path\to\synclite-c-sdk
cmake --build build --config Debug
```

`-DSYNCLITE_SDK_DIR=...` short-circuits the source-tree probing and
links directly against the prebuilt library. The same sample sources
compile against either option.

On Windows the cdylib is copied next to each `.exe` post-build so it
loads at run time. On Linux/macOS set `LD_LIBRARY_PATH` /
`DYLD_LIBRARY_PATH` to the directory containing the shared library
before running.

## Run

```pwsh
.\build\Debug\synclite_rusqlite.exe
.\build\Debug\synclite_rusqlite_store.exe
.\build\Debug\synclite_streaming.exe
.\build\Debug\synclite_duckdb.exe
.\build\Debug\synclite_duckdb_store.exe
```

Configure a destination before running the `synclite_rusqlite` and
`synclite_duckdb` binaries — see `synclite.conf` and the
`DestinationOptions` block at the top of each sample.

## API mapping

| Rust                                    | Python                          | C++                                          |
|-----------------------------------------|---------------------------------|----------------------------------------------|
| `synclite::initialize(...)`             | `synclite.initialize(...)`      | `synclite::initialize(...)`                  |
| `synclite::rusqlite::Connection::open`  | `synclite.Connection.open`      | `synclite::Connection::open`                 |
| `synclite::duckdb::Connection::open`    | `synclite.DuckDBConnection.open`| `synclite::DuckConnection::open`             |
| `conn.execute(sql, params)`             | `conn.execute(sql, params)`     | `conn.execute(sql, {params...})`             |
| `conn.prepare(sql)`                     | `conn.prepare(sql)`             | `conn.prepare(sql)`                          |
| `stmt.execute(params)`                  | `stmt.execute(params)`          | `stmt.execute({params...})`                  |
| `stmt.add_batch / execute_batch`        | `stmt.add_batch / execute_batch`| `stmt.add_batch({...}) / stmt.execute_batch()` |
| `conn.query(sql, params)`               | `conn.query(sql, params)`       | `conn.query(sql, {params...})` → `Rows`      |
| `conn.flush()` + `synclite::await_sync` | same                            | `conn.flush()` + `synclite::await_sync`      |

All C++ entry points throw `synclite::Error` on failure.
