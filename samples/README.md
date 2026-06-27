# SyncLite Logger — Language Samples

End-to-end runnable samples for the SyncLite logger runtime, in three
flavors that all sit on the same Rust core:

| Folder | Language | What it links against |
|---|---|---|
| [rust/](rust/) | Rust | `synclite` crate (path dep on this workspace) |
| [python/](python/) | Python | `synclite` PyO3 wheel (built from `crates/logger/bindings-python`) |
| [cpp/](cpp/) | C++17 | `synclite-c` cdylib + header-only `synclite.hpp` RAII wrapper |

Every folder exposes the same five samples under the same names, so you can
diff them side-by-side and confirm the API is identical across languages:

- `synclite_rusqlite` — SQLite device, basic CRUD.
- `synclite_rusqlite_store` — SQLite **Store** device (typed key/value over SQL).
- `synclite_streaming` — append-oriented streaming device.
- `synclite_duckdb` — DuckDB device, basic CRUD.
- `synclite_duckdb_store` — DuckDB Store device.

These fall into three **device families** — the connection + SQL surface is
identical across all three, only the `DeviceType` passed to `initialize`
changes:

- **SQL device** (`SQLITE`, `DUCKDB`) — a full, SQLite-syntax-compliant
  embedded SQL database for arbitrary `CREATE` / `ALTER` / `SELECT` /
  `INSERT` / `UPDATE` / `DELETE`. Use it when you need real SQL, JOINs,
  multi-statement transactions, or ad-hoc DDL.
- **Store device** (`SQLITE_STORE`, `DUCKDB_STORE`) — the same SQL-shaped
  API tuned for bulk write-through; the runtime emits pre-formed row events
  that the Consolidator applies directly to the destination, giving the
  highest end-to-end consolidation throughput and the simplest starting
  point for a new app.
- **Streaming device** (`STREAMING`) — append-only ingestion for
  high-throughput event capture; accepts `INSERT` + DDL and rejects
  `UPDATE` / `DELETE` by design.

The Rust folder also has the marquee **SQLite → PostgreSQL** demo
(`synclite_rusqlite_postgres`) plus device-artifact / reinitialize samples.

See each sub-folder's `README.md` for build + run instructions.

These same samples are mirrored at the top of the SyncLite platform repo
under `synclite-code-samples/synclite-logger/`.
