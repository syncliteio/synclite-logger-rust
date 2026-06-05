# synclite-logger (Python)

PyO3 bindings over the SyncLite Rust runtime. The Python API mirrors the
Rust wrapper crate 1:1 — `Connection`, `Statement`, `DuckDBConnection`,
`DuckDBStatement`, plus module-level `initialize` and `await_sync` — so
Python samples read like the Rust examples in
`synclite-logger-rust/crates/synclite/examples/`.

No DB-API adapter, no pre/post hooks, no separate user-DB connection.
Python holds a `Connection` directly bound to the Rust type.

## Build / install (editable, dev)

```pwsh
pip install maturin
maturin develop --release
```

This compiles `crates/logger/bindings-python` (cdylib `_native`) and
installs the `synclite` package into the active environment.

## Quickstart

```python
import synclite as sl

sl.initialize(
    device_type="SQLITE",
    device_name="sampledevice",
    db_path="myapp.db",
    destination=sl.DestinationOptions(
        dst_type="POSTGRES",
        dst_connection_string="postgresql://user:pw@localhost:5432/syncdb",
        dst_database="syncdb",
        dst_schema="public",
    ),
)

conn = sl.Connection.open("myapp.db")
conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)")

stmt = conn.prepare("INSERT INTO t(id, name) VALUES(?, ?)")
stmt.execute([1, "Alice"])
stmt.add_batch([2, "Bob"])
stmt.add_batch([3, "Carol"])
stmt.execute_batch()

for row in conn.query("SELECT id, name FROM t ORDER BY id"):
    print(row)

conn.commit()
conn.flush()
sl.await_sync("myapp.db", 30.0)
conn.close()
```

For DuckDB, swap `Connection` for `DuckDBConnection` and
`device_type="DUCKDB"`. For STORE / STREAMING devices, write a config
file with `device-type=SQLITE_STORE` or `STREAMING` and open with
`Connection.open_with_config(path)`.

See `synclite-code-samples/synclite-logger/python/` for runnable
samples — each one is a 1:1 mirror of the corresponding Rust example.

## Parameter / row conversion

| Python                  | Rust `Value`     |
| ----------------------- | ---------------- |
| `None`                  | `Value::Null`    |
| `bool`                  | `Value::Int(0/1)`|
| `int`                   | `Value::Int`     |
| `float`                 | `Value::Real`    |
| `str`                   | `Value::Text`    |
| `bytes`                 | `Value::Blob`    |

`query(...)` returns `list[tuple]` using the same mapping in reverse.

Pass parameters as a `list` or `tuple`; pass nothing (or `None`) for
parameterless statements.

## API surface

- `initialize(device_type, device_name, db_path, destination=None, config_path=None)`
- `await_sync(db_path, timeout_seconds)`
- `DestinationOptions(dst_type, dst_connection_string, dst_database=None, dst_schema=None, dst_sync_mode="CONSOLIDATION")`
- `Connection.open(path)` / `open_with_config(path)` /
  `initialize(path)` / `initialize_with_config(path)`
- `Connection.execute(sql, params=None) -> int`
- `Connection.query(sql, params=None) -> list[tuple]`
- `Connection.prepare(sql) -> Statement`
- `Connection.{commit,rollback,flush,close,set_auto_commit,get_auto_commit}`
- `Statement.execute(params=None) -> int`
- `Statement.query(params=None) -> list[tuple]`
- `Statement.{add_batch,clear_batch,execute_batch}`
- `DuckDBConnection` / `DuckDBStatement`: identical shape.

`device_type`, `dst_type`, `dst_sync_mode` are accepted as
case-insensitive strings:

- `device_type`: `"SQLITE"`, `"SQLITE_STORE"`, `"STREAMING"`, `"DUCKDB"`, `"DUCKDB_STORE"`
- `dst_type`: `"SQLITE"`, `"DUCKDB"`, `"POSTGRES"`
- `dst_sync_mode`: `"CONSOLIDATION"`, `"REPLICATION"`
