"""Python bindings for the SyncLite Rust runtime.

Provides the direct Rust-shaped connection API plus the PEP 249-compatible
``synclite.dbapi2`` facade.
"""

from pathlib import Path

from ._native_loader import ensure_native_search_path

ensure_native_search_path(Path(__file__).resolve().parent)

try:
    from ._native import (
        ColumnInfo,
        Connection,
        DestinationOptions,
        DuckDBConnection,
        DuckDBStatement,
        ExecutionResult,
        Statement,
        __version__,
        await_sync,
        initialize,
    )
except ModuleNotFoundError as exc:
    if exc.name not in {"_native", "synclite._native"}:
        raise

    ColumnInfo = None
    Connection = None
    DestinationOptions = None
    DuckDBConnection = None
    DuckDBStatement = None
    ExecutionResult = None
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

from . import dbapi2

__all__ = [
    "ColumnInfo",
    "Connection",
    "DestinationOptions",
    "DuckDBConnection",
    "DuckDBStatement",
    "ExecutionResult",
    "Statement",
    "__version__",
    "await_sync",
    "dbapi2",
    "initialize",
]
