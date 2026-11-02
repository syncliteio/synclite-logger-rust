"""Driver hooks shared by the optional, file-backed SyncLite dialects."""

import os

from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.pool import NullPool

from .. import dbapi2


def _file_database(database):
    if database is None:
        raise dbapi2.NotSupportedError("SyncLite requires an explicit file-backed database")
    try:
        path = os.fsdecode(os.fspath(database))
    except TypeError as exc:
        raise dbapi2.InterfaceError("database must be a filesystem path") from exc
    if not path or path.lower() == ":memory:" or path.lower().startswith("file:"):
        raise dbapi2.NotSupportedError(
            "SyncLite SQLAlchemy requires a file path; in-memory databases and SQLite URIs are unsupported"
        )
    return os.path.abspath(path)


class SyncLiteDialectMixin:
    driver = "synclite"
    # SyncLite's native connection owns manual transactions. Isolation cannot
    # be changed via SQL without bypassing its transaction/logging state.
    synclite_isolation_level = "SERIALIZABLE"

    @classmethod
    def import_dbapi(cls):
        return dbapi2

    @classmethod
    def get_pool_class(cls, url):
        return NullPool

    def create_connect_args(self, url):
        if any(value is not None for value in (url.username, url.password, url.host, url.port)):
            raise dbapi2.InterfaceError("SyncLite URLs are local file URLs, not server URLs")
        unknown = set(url.query) - {"config_path", "initialize"}
        if unknown:
            raise dbapi2.InterfaceError("unsupported SyncLite URL options: " + ", ".join(sorted(unknown)))
        options = {"backend": self.name}
        if "config_path" in url.query:
            value = url.query["config_path"]
            if not isinstance(value, str) or not value:
                raise dbapi2.InterfaceError("config_path must be a single non-empty path")
            options["config_path"] = value
        if "initialize" in url.query:
            value = url.query["initialize"]
            if not isinstance(value, str) or value.lower() not in {"true", "false"}:
                raise dbapi2.InterfaceError("initialize must be true or false")
            options["initialize"] = value.lower() == "true"
        return [], dict(options, database=_file_database(url.database))

    def connect(self, *args, **kwargs):
        # Validate after SQLAlchemy merges connect_args, not only the URL.
        if args or kwargs.get("backend", self.name) != self.name:
            raise dbapi2.InterfaceError("the SyncLite dialect fixes the connection backend")
        kwargs["database"] = _file_database(kwargs.get("database"))
        kwargs["backend"] = self.name
        if not isinstance(kwargs.get("initialize", False), bool):
            raise dbapi2.InterfaceError("initialize must be a boolean")
        return self.loaded_dbapi.connect(**kwargs)

    def do_begin(self, dbapi_connection):
        # Native manual mode lazily starts its own transaction for mutations.
        # Never send BEGIN through SQL or call an unlogged driver connection.
        pass

    def do_commit(self, dbapi_connection):
        dbapi_connection.commit()

    def do_rollback(self, dbapi_connection):
        dbapi_connection.rollback()

    def get_isolation_level_values(self, dbapi_connection):
        return [self.synclite_isolation_level]

    def get_isolation_level(self, dbapi_connection):
        return self.synclite_isolation_level

    def get_default_isolation_level(self, dbapi_connection):
        return self.synclite_isolation_level

    def set_isolation_level(self, dbapi_connection, level):
        if level != self.synclite_isolation_level:
            raise dbapi2.NotSupportedError("SyncLite does not support changing isolation or AUTOCOMMIT")

    def detect_autocommit_setting(self, dbapi_connection):
        return False

    def do_savepoint(self, connection, name):
        raise dbapi2.NotSupportedError("SyncLite does not support savepoints or nested transactions")

    do_rollback_to_savepoint = do_savepoint
    do_release_savepoint = do_savepoint

    def do_begin_twophase(self, connection, xid):
        raise dbapi2.NotSupportedError("SyncLite does not support two-phase transactions")

    def do_prepare_twophase(self, connection, xid):
        raise dbapi2.NotSupportedError("SyncLite does not support two-phase transactions")

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise dbapi2.NotSupportedError("SyncLite does not support two-phase transactions")

    do_rollback_twophase = do_commit_twophase

    def do_recover_twophase(self, connection):
        raise dbapi2.NotSupportedError("SyncLite does not support two-phase transactions")

    def do_ping(self, dbapi_connection):
        # The psycopg2 ping changes autocommit; use the generic SELECT path.
        return DefaultDialect.do_ping(self, dbapi_connection)

    def is_disconnect(self, error, connection, cursor):
        return isinstance(error, dbapi2.InterfaceError) and "connection is closed" in str(error)