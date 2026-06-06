"""Python bindings for the SyncLite Rust runtime (logger + embedded consolidator).

Mirrors the Rust user-facing API: `Connection` / `Statement` for the
SQLite-family devices, `DuckDBConnection` / `DuckDBStatement` for the
DuckDB-family devices, plus module-level `initialize` and `await_sync`.
"""

from synclite._native import (
    Connection,
    DestinationOptions,
    DuckDBConnection,
    DuckDBStatement,
    Statement,
    __version__,
    await_sync,
    initialize,
)

__all__ = [
    "Connection",
    "DestinationOptions",
    "DuckDBConnection",
    "DuckDBStatement",
    "Statement",
    "__version__",
    "await_sync",
    "initialize",
]
