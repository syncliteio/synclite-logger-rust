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

The Rust folder also has the marquee **SQLite → PostgreSQL** demo
(`synclite_rusqlite_postgres`) plus device-artifact / reinitialize samples.

See each sub-folder's `README.md` for build + run instructions.

These same samples are mirrored at the top of the SyncLite platform repo
under `synclite-code-samples/synclite-logger/`.
