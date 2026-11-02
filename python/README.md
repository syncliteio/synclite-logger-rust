# synclite-logger (Python)

PyO3 bindings over the SyncLite Rust runtime. The package offers two layers:

- The basic API mirrors the Rust wrapper crate 1:1 — `Connection`,
  `Statement`, `DuckDBConnection`, `DuckDBStatement`, plus module-level
  `initialize` and `await_sync`.
- `synclite.dbapi2` is a PEP 249 / Python DB-API 2.0 facade with connections,
  cursors, transactions, result metadata, fetch methods, and standard DB-API
  exceptions.
- Optional SQLAlchemy 2.x dialects (`sqlite+synclite` and `duckdb+synclite`)
    reuse the existing SQLite and DuckDB SQL behavior but route driver calls
    through the SyncLite DB-API facade.

Both layers use the same native connection; there is no separate unlogged
user-database connection.

## Build / install (editable, dev)

```pwsh
pip install maturin
maturin develop --release
```

This compiles `crates/logger/bindings-python` (cdylib `_native`) and
installs the `synclite` package into the active environment.

## Distribution wheels (per-platform)

`synclite` ships as **platform-specific wheels** — one per OS/arch — because
the compiled `_native` extension links `libduckdb` natively; a single wheel
cannot cover every platform. `pip install synclite` auto-selects the wheel
matching the user's OS/arch. The full set is:

| Platform | Wheel tag | Built by |
| --- | --- | --- |
| Windows x64 | `win_amd64` | host build (`delvewheel` repair) |
| Linux x86_64 | `manylinux_2_28_x86_64` | Linux/WSL build (`patchelf`/auditwheel) |
| Linux aarch64 | `manylinux_2_28_aarch64` | Linux/WSL build (`patchelf`/auditwheel) |
| macOS x86_64 / arm64 | `macosx_*` | macOS runner (`delocate` repair) |

The repository build produces the **host** wheel automatically. The
**Linux `manylinux` wheels** require bundling `libduckdb.so` + RPATH-patching
via [`patchelf`](https://github.com/NixOS/patchelf) (POSIX-only, no Windows
binary), so on Windows the Maven build dispatches them into **WSL** — see
`GETTING_STARTED.md` → "Building Linux Python wheels". The complete
cross-platform set (including macOS) is produced by CI
(`.github/workflows/python-wheels.yml`).

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

## Python DB-API 2.0

Initialize destinations with the basic API as usual, then open the database
through `synclite.dbapi2`:

```python
import synclite as sl
from synclite import dbapi2

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

with dbapi2.connect("myapp.db") as conn:
        with conn.cursor() as cursor:
                cursor.execute("INSERT INTO t(name) VALUES(?)", ("Alice",))
                print(cursor.lastrowid)  # SQLite only

                cursor.execute("SELECT id, name FROM t ORDER BY id")
                print(cursor.description)
                for row in cursor:
                        print(row)

conn.close()
```

Use `dbapi2.connect("myapp.duckdb", backend="duckdb")` for DuckDB, or pass
`config_path=...` to call the corresponding native `open_with_config` API.
Passing `initialize=True` selects the native initialize-and-open variant.

DB-API behavior and backend notes:

- `apilevel = "2.0"`, `threadsafety = 1`, and `paramstyle = "qmark"`.
- DB-API connections run with native autocommit disabled. `commit()` and
    `rollback()` define transaction fate, and `close()` rolls back pending work.
    These transaction guarantees apply to transactional `SQLITE` and `DUCKDB`
    devices; STORE and STREAMING device modes retain their native SyncLite
    transaction semantics.
- `Connection.flush()` rolls committed SyncLite log work only. It raises
    `ProgrammingError` while the wrapper is tracking uncommitted mutations;
    call `commit()` or `rollback()` first.
- Query rows are buffered eagerly. `fetchone`, `fetchmany`, `fetchall`, cursor
    iteration, and multiple independent cursors are supported.
- `cursor.description` is available even when a query returns zero rows. Each
    entry is the required seven-item DB-API sequence; unavailable size,
    precision, scale, and nullability fields are `None`.
- SQLite reports affected-row counts and `lastrowid` for successful insert-like
    statements when SQLite produces a new row id.
- DuckDB does not provide a general SQLite-style generated row ID, so
    `cursor.lastrowid` is always `None`. Use `INSERT ... RETURNING` to retrieve
    generated values. DuckDB mutation row counts are `-1` when unavailable.
- The existing basic `execute()` and `query()` APIs are unchanged. Metadata is
    collected only by `execute_with_result()` and the DB-API facade.

## SQLAlchemy 2.x

Install the optional dialect dependencies with `pip install "synclite[sqlalchemy]"`
for SQLite or `pip install "synclite[sqlalchemy-duckdb]"` for DuckDB.
Initialize the SyncLite device and its destination *before* creating an
SQLAlchemy engine (see the [core](../../synclite-code-samples/python/core/synclite_rusqlite_postgres.py),
[DB-API](../../synclite-code-samples/python/dbapi/synclite_dbapi_postgres.py), and
[SQLAlchemy](../../synclite-code-samples/python/sqlalchemy/synclite_sqlalchemy_postgres.py)
samples):

```python
import synclite as sl
from sqlalchemy import create_engine, text

sl.initialize(
    device_type="SQLITE", device_name="sampledevice", db_path="myapp.db",
    destination=sl.DestinationOptions(
        dst_type="POSTGRES",
        dst_connection_string="postgresql://user:pw@localhost:5432/syncdb",
        dst_database="syncdb", dst_schema="public",
        dst_sync_mode="REPLICATION",
    ),
)
engine = create_engine("sqlite+synclite:///myapp.db")
with engine.begin() as connection:
    connection.execute(text("INSERT INTO t(name) VALUES(:name)"), {"name": "Alice"})
engine.dispose()
```

`sqlite+synclite` means SQLite SQL with SyncLite's logged driver, **not** a
direct connection to the PostgreSQL destination. For DuckDB, initialize a
`DUCKDB` device and use `duckdb+synclite:///myapp.duckdb`. The dialects use
`NullPool` because native connections are thread-affine; savepoints, two-phase
transactions, and autocommit isolation are unsupported. SQLAlchemy commits
locally; use SyncLite `flush()` and `await_sync()` separately when a short-lived
application needs to wait for destination delivery. These dialects require a
SyncLite wheel containing the DB-API and SQLAlchemy adapter; an older installed
wheel will not expose them.

For multiple destinations, use the same `initialize(...)` API with a non-empty
`destinations` sequence:

```python
sl.initialize(
    device_type="SQLITE",
    device_name="sampledevice",
    db_path="myapp.db",
    destinations=[destination_1, destination_2],
)
```

Sequence order defines stable one-based destination indexes. The complete
sequence is validated first, then destinations are initialized sequentially.

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

- `initialize(device_type, device_name, db_path, destination=None, config_path=None, *, destinations=None)`
- `await_sync(db_path, timeout_seconds)`
- `DestinationOptions(dst_type, dst_connection_string, dst_database=None, dst_schema=None, dst_sync_mode="CONSOLIDATION")`
- `Connection.open(path)` / `open_with_config(path)` /
  `initialize(path)` / `initialize_with_config(path)`
- `Connection.execute(sql, params=None) -> int`
- `Connection.execute_with_result(sql, params=None) -> ExecutionResult`
- `Connection.query(sql, params=None) -> list[tuple]`
- `Connection.prepare(sql) -> Statement`
- `Connection.{commit,rollback,flush,close,set_auto_commit,get_auto_commit}`
- `Statement.execute(params=None) -> int`
- `Statement.execute_with_result(params=None) -> ExecutionResult`
- `Statement.query(params=None) -> list[tuple]`
- `Statement.{add_batch,clear_batch,execute_batch}`
- `DuckDBConnection` / `DuckDBStatement`: identical shape.
- `dbapi2.connect(path, backend="sqlite", config_path=None, initialize=False)`
- `dbapi2.Connection.{cursor,execute,executemany,commit,rollback,close,flush}`
- `dbapi2.Cursor.{execute,executemany,fetchone,fetchmany,fetchall,close}`

`device_type`, `dst_type`, `dst_sync_mode` are accepted as
case-insensitive strings:

- `device_type`: `"SQLITE"`, `"SQLITE_STORE"`, `"STREAMING"`, `"DUCKDB"`, `"DUCKDB_STORE"`
- `dst_type`: `"SQLITE"`, `"DUCKDB"`, `"POSTGRES"`
- `dst_sync_mode`: `"CONSOLIDATION"`, `"REPLICATION"`
