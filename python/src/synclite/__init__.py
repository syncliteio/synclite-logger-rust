"""Python bindings for the SyncLite Rust runtime (logger + embedded consolidator).

Mirrors the Rust user-facing API: `Connection` / `Statement` for the
SQLite-family devices, `DuckDBConnection` / `DuckDBStatement` for the
DuckDB-family devices, plus module-level `initialize` and `await_sync`.
"""

from pathlib import Path

from ._native_loader import ensure_native_search_path

ensure_native_search_path(Path(__file__).resolve().parent)

try:
    from ._native import (
        Connection,
        DestinationOptions,
        DuckDBConnection,
        DuckDBStatement,
        Statement,
        __version__,
        await_sync,
        initialize,
    )
except ModuleNotFoundError as exc:
    if exc.name not in {"_native", "synclite._native"}:
        raise

    Connection = None
    DestinationOptions = None
    DuckDBConnection = None
    DuckDBStatement = None
    Statement = None
    __version__ = "0.0.0"

    def await_sync(*args, **kwargs):
        raise ModuleNotFoundError(
            "The SyncLite native extension is not available; reinstall the package after building it."
        )

    def initialize(*args, **kwargs):
        raise ModuleNotFoundError(
            "The SyncLite native extension is not available; reinstall the package after building it."
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
