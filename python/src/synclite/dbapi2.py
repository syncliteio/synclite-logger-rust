"""PEP 249 (Python DB-API 2.0) facade for SyncLite connections.

The native extension owns SQL execution, mutation logging, result metadata,
and SQLite generated row IDs. This module owns cursor buffering and DB-API
transaction/error conventions.
"""

from __future__ import annotations

import datetime as _datetime
import os as _os
from typing import Any, Iterable, Iterator, List, Optional, Sequence, Tuple

from . import Connection as _SQLiteConnection
from . import DuckDBConnection as _DuckDBConnection

apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"


class Warning(Exception):
    """Exception raised for important warnings."""


class Error(Exception):
    """Base class for all DB-API errors."""


class InterfaceError(Error):
    """Error related to the database interface rather than the database."""


class DatabaseError(Error):
    """Base class for database-related errors."""


class DataError(DatabaseError):
    """Error caused by invalid or unsupported data."""


class OperationalError(DatabaseError):
    """Error related to database operation or availability."""


class IntegrityError(DatabaseError):
    """Error caused by a relational-integrity violation."""


class InternalError(DatabaseError):
    """Error caused by an internal database or driver condition."""


class ProgrammingError(DatabaseError):
    """Error caused by invalid SQL or API usage."""


class NotSupportedError(DatabaseError):
    """Error raised for an unsupported DB-API operation."""


class DBAPITypeObject:
    """PEP 249 type category used to compare description type codes."""

    def __init__(self, *values: str) -> None:
        self.values = frozenset(value.upper() for value in values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _TypeCode):
            return other.category is self
        if isinstance(other, str):
            return other.upper() in self.values
        return False

    def __repr__(self) -> str:
        return "DBAPITypeObject({})".format(
            ", ".join(repr(value) for value in sorted(self.values))
        )


class _TypeCode:
    def __init__(self, native_name: str, category: Optional[DBAPITypeObject]) -> None:
        self.native_name = native_name
        self.category = category

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DBAPITypeObject):
            return self.category is other
        if isinstance(other, _TypeCode):
            return self.native_name.upper() == other.native_name.upper()
        if isinstance(other, str):
            return self.native_name.upper() == other.upper()
        return False

    def __hash__(self) -> int:
        return hash(self.native_name.upper())

    def __repr__(self) -> str:
        return self.native_name


STRING = DBAPITypeObject("CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "TEXT", "STRING")
BINARY = DBAPITypeObject("BINARY", "VARBINARY", "BLOB", "BYTEA")
NUMBER = DBAPITypeObject(
    "BOOLEAN",
    "BOOL",
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "INT",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UINT",
    "UBIGINT",
    "UHUGEINT",
    "DECIMAL",
    "NUMERIC",
    "REAL",
    "FLOAT",
    "DOUBLE",
)
DATETIME = DBAPITypeObject("DATE", "TIME", "DATETIME", "TIMESTAMP", "INTERVAL")
ROWID = DBAPITypeObject("ROWID")

Date = _datetime.date
Time = _datetime.time
Timestamp = _datetime.datetime
Binary = bytes


def DateFromTicks(ticks: float) -> _datetime.date:
    """Build a date from seconds since the epoch, interpreted in local time."""

    return _datetime.date.fromtimestamp(ticks)


def TimeFromTicks(ticks: float) -> _datetime.time:
    """Build a time from seconds since the epoch, interpreted in local time."""

    return _datetime.datetime.fromtimestamp(ticks).time()


def TimestampFromTicks(ticks: float) -> _datetime.datetime:
    """Build a timestamp from seconds since the epoch, interpreted locally."""

    return _datetime.datetime.fromtimestamp(ticks)


def _type_code(type_name: Optional[str]) -> Optional[_TypeCode]:
    if not type_name:
        return None
    normalized = type_name.strip().upper()
    base = normalized.split("(", 1)[0].strip()
    category: Optional[DBAPITypeObject] = None
    for candidate in (STRING, BINARY, NUMBER, DATETIME, ROWID):
        if base in candidate.values or any(
            base.startswith(value + " ") for value in candidate.values
        ):
            category = candidate
            break
    return _TypeCode(type_name, category)


def _dbapi_error(exc: Exception) -> Error:
    if isinstance(exc, Error):
        return exc
    message = str(exc)
    lowered = message.lower()
    if "closed" in lowered or "native extension is not available" in lowered:
        error_type = InterfaceError
    elif any(
        marker in lowered
        for marker in (
            "constraint",
            "duplicate",
            "unique",
            "foreign key",
            "not null",
            "check constraint",
        )
    ):
        error_type = IntegrityError
    elif any(
        marker in lowered
        for marker in (
            "unsupported parameter type",
            "out of range",
            "conversion",
            "could not convert",
        )
    ):
        error_type = DataError
    elif any(
        marker in lowered
        for marker in (
            "syntax",
            "parser error",
            "binder error",
            "parameter",
            "binding",
            "exactly one sql statement",
            "unsupported sql",
        )
    ):
        error_type = ProgrammingError
    elif any(
        marker in lowered
        for marker in ("locked", "busy", "i/o", "io error", "read-only", "readonly")
    ):
        error_type = OperationalError
    elif any(marker in lowered for marker in ("internal", "panic", "poisoned")):
        error_type = InternalError
    else:
        error_type = DatabaseError
    return error_type(message)


def _parameters(values: Optional[Sequence[Any]]) -> Optional[Tuple[Any, ...]]:
    if values is None:
        return None
    if isinstance(values, (str, bytes, bytearray, memoryview, dict)):
        raise ProgrammingError("parameters must be a non-string sequence for qmark binding")
    try:
        return tuple(_adapt_parameter(value) for value in values)
    except TypeError as exc:
        raise ProgrammingError("parameters must be a sequence") from exc


def _adapt_parameter(value: Any) -> Any:
    if isinstance(value, _datetime.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (_datetime.date, _datetime.time)):
        return value.isoformat()
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return value


def _leading_sql_keyword(operation: str) -> str:
    index = 0
    length = len(operation)
    while index < length:
        while index < length and operation[index].isspace():
            index += 1
        if operation.startswith("--", index):
            newline = operation.find("\n", index + 2)
            index = length if newline < 0 else newline + 1
            continue
        if operation.startswith("/*", index):
            end = operation.find("*/", index + 2)
            if end < 0:
                return ""
            index = end + 2
            continue
        break
    start = index
    while index < length and (operation[index].isalnum() or operation[index] == "_"):
        index += 1
    return operation[start:index].upper()


class Connection:
    """PEP 249 connection wrapping one native SyncLite connection."""

    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, native: Any, backend: str) -> None:
        self._native = native
        self._backend = backend
        self._closed = False
        self._has_uncommitted_work = False
        self._call_native("set_auto_commit", False)

    def _check_open(self) -> None:
        if self._closed or self._native is None:
            raise InterfaceError("connection is closed")

    def _call_native(self, method: str, *args: Any) -> Any:
        self._check_open()
        try:
            return getattr(self._native, method)(*args)
        except Exception as exc:
            raise _dbapi_error(exc) from exc

    def cursor(self) -> "Cursor":
        self._check_open()
        return Cursor(self)

    def commit(self) -> None:
        self._call_native("commit")
        self._has_uncommitted_work = False

    def rollback(self) -> None:
        self._call_native("rollback")
        self._has_uncommitted_work = False

    def close(self) -> None:
        if self._closed:
            return
        native = self._native
        self._closed = True
        self._native = None
        first_error: Optional[Error] = None
        try:
            native.rollback()
        except Exception as exc:
            first_error = _dbapi_error(exc)
        try:
            # Native manual-mode close commits for backward compatibility.
            # Restore native autocommit first so DB-API close never commits.
            native.set_auto_commit(True)
            native.close()
        except Exception as exc:
            if first_error is None:
                first_error = _dbapi_error(exc)
        if first_error is not None:
            raise first_error

    def execute(
        self, operation: str, parameters: Optional[Sequence[Any]] = None
    ) -> "Cursor":
        return self.cursor().execute(operation, parameters)

    def executemany(
        self, operation: str, seq_of_parameters: Iterable[Sequence[Any]]
    ) -> "Cursor":
        return self.cursor().executemany(operation, seq_of_parameters)

    def flush(self) -> None:
        """Roll committed SyncLite log work so it can be shipped."""

        if self._has_uncommitted_work:
            raise ProgrammingError("commit or roll back before flushing SyncLite segments")
        self._call_native("flush")

    @property
    def autocommit(self) -> bool:
        self._check_open()
        return False

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._check_open()
        if value:
            raise NotSupportedError("DB-API autocommit mode is not supported")

    def __enter__(self) -> "Connection":
        self._check_open()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        return False

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class Cursor(Iterator[Tuple[Any, ...]]):
    """Eager PEP 249 cursor over a SyncLite native execution result."""

    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.arraysize = 1
        self.description: Optional[Tuple[Tuple[Any, ...], ...]] = None
        self.rowcount = -1
        self.lastrowid: Optional[int] = None
        self._rows: Optional[List[Tuple[Any, ...]]] = None
        self._row_index = 0
        self._closed = False

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError("cursor is closed")
        self.connection._check_open()

    def _clear_result(self) -> None:
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        self._rows = None
        self._row_index = 0

    def execute(
        self, operation: str, parameters: Optional[Sequence[Any]] = None
    ) -> "Cursor":
        self._check_open()
        if not isinstance(operation, str):
            raise ProgrammingError("operation must be a SQL string")
        if _leading_sql_keyword(operation) in {
            "BEGIN",
            "START",
            "COMMIT",
            "END",
            "ROLLBACK",
            "SAVEPOINT",
            "RELEASE",
        }:
            raise ProgrammingError(
                "use connection.commit() or connection.rollback() for transaction control"
            )
        self._clear_result()
        bound = _parameters(parameters)
        try:
            result = self.connection._call_native("execute_with_result", operation, bound)
        except Error:
            # A backend error can occur after a transaction was opened or a
            # statement partially ran. Require rollback before segment flush.
            self.connection._has_uncommitted_work = True
            raise
        native_rows = result.rows
        if native_rows is not None:
            self._rows = [tuple(row) for row in native_rows]
            self.description = tuple(
                (
                    column.name,
                    _type_code(column.type_name),
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                for column in result.columns
            )
        if result.row_count is not None:
            self.rowcount = int(result.row_count)
        self.lastrowid = result.last_insert_rowid
        if result.is_mutating:
            self.connection._has_uncommitted_work = True
        return self

    def executemany(
        self, operation: str, seq_of_parameters: Iterable[Sequence[Any]]
    ) -> "Cursor":
        self._check_open()
        if not isinstance(operation, str):
            raise ProgrammingError("operation must be a SQL string")
        if _leading_sql_keyword(operation) in {
            "BEGIN",
            "START",
            "COMMIT",
            "END",
            "ROLLBACK",
            "SAVEPOINT",
            "RELEASE",
        }:
            raise ProgrammingError(
                "use connection.commit() or connection.rollback() for transaction control"
            )
        self._clear_result()
        try:
            statement = self.connection._native.prepare(operation)
            count = 0
            for values in seq_of_parameters:
                statement.add_batch(_parameters(values))
                count += 1
            if count > 0:
                # Batch execution can fail after a prefix has run.
                self.connection._has_uncommitted_work = True
            counts = statement.execute_batch()
        except Exception as exc:
            raise _dbapi_error(exc) from exc
        if count == 0:
            self.rowcount = 0
        elif self.connection._backend == "duckdb":
            self.rowcount = -1
        else:
            self.rowcount = sum(int(value) for value in counts)
        return self

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        self._check_open()
        if self._rows is None:
            raise ProgrammingError("the previous operation did not produce a result set")
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        self._check_open()
        if self._rows is None:
            raise ProgrammingError("the previous operation did not produce a result set")
        if size is None:
            size = self.arraysize
        if not isinstance(size, int) or size < 0:
            raise ProgrammingError("fetchmany size must be a non-negative integer")
        start = self._row_index
        end = min(start + size, len(self._rows))
        self._row_index = end
        return self._rows[start:end]

    def fetchall(self) -> List[Tuple[Any, ...]]:
        self._check_open()
        if self._rows is None:
            raise ProgrammingError("the previous operation did not produce a result set")
        start = self._row_index
        self._row_index = len(self._rows)
        return self._rows[start:]

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._clear_result()

    def setinputsizes(self, sizes: Any) -> None:
        self._check_open()

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        self._check_open()

    def callproc(self, procname: str, parameters: Any = None) -> None:
        self._check_open()
        raise NotSupportedError("stored procedures are not supported")

    def nextset(self) -> None:
        self._check_open()
        self._clear_result()
        return None

    def __iter__(self) -> "Cursor":
        self._check_open()
        return self

    def __next__(self) -> Tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> "Cursor":
        self._check_open()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        self.close()
        return False


def connect(
    database: Any,
    *,
    backend: str = "sqlite",
    config_path: Optional[Any] = None,
    initialize: bool = False,
) -> Connection:
    """Open a SyncLite DB-API connection.

    ``backend`` accepts ``"sqlite"`` or ``"duckdb"``. Set ``config_path``
    to use the corresponding native ``open_with_config`` method. Metadata and
    generated-row-id collection are enabled only for this DB-API execution path.
    """

    if not isinstance(backend, str):
        raise InterfaceError("backend must be 'sqlite' or 'duckdb'")
    backend_name = backend.strip().lower()
    if backend_name == "sqlite":
        native_type = _SQLiteConnection
    elif backend_name in {"duckdb", "duck"}:
        native_type = _DuckDBConnection
        backend_name = "duckdb"
    else:
        raise InterfaceError("backend must be 'sqlite' or 'duckdb'")
    if native_type is None:
        raise InterfaceError("the SyncLite native extension is not available")

    try:
        if config_path is not None:
            path = _os.fsdecode(_os.fspath(config_path))
            method = "initialize_with_config" if initialize else "open_with_config"
        else:
            path = _os.fsdecode(_os.fspath(database))
            method = "initialize" if initialize else "open"
        native = getattr(native_type, method)(path)
        return Connection(native, backend_name)
    except Exception as exc:
        raise _dbapi_error(exc) from exc


__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Binary",
    "Connection",
    "Cursor",
    "connect",
]
