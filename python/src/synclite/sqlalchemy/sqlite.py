"""SQLite compilation/reflection with SyncLite's DB-API, not pysqlite hooks."""

from sqlalchemy.dialects.sqlite.base import SQLiteDialect

from ._common import SyncLiteDialectMixin


class SQLiteDialect_syncLite(SyncLiteDialectMixin, SQLiteDialect):
    supports_statement_cache = True
    supports_sane_rowcount_returning = False

    def __init__(self, **kwargs):
        # SQLiteDialect assumes dbapi.sqlite_version_info in its constructor.
        # SyncLite's SQLite is NOT the stdlib sqlite3 build. Discover it from
        # the actual connection instead of inventing a DB-API version constant.
        dbapi = kwargs.pop("dbapi", None)
        super().__init__(dbapi=None, **kwargs)
        self.dbapi = dbapi

    def _get_server_version_info(self, connection):
        version = connection.exec_driver_sql("SELECT sqlite_version()").scalar_one()
        return tuple(int(part) for part in version.split("."))

    def initialize(self, connection):
        super().initialize(connection)
        version = self.server_version_info
        self._broken_dotted_colnames = version < (3, 10, 0)
        self._broken_fk_pragma_quotes = version < (3, 6, 14)
        self.supports_default_values = version >= (3, 3, 8)
        self.supports_cast = version >= (3, 2, 3)
        self.supports_multivalues_insert = version >= (3, 7, 11)
        self.insert_returning = self.update_returning = self.delete_returning = version >= (3, 35, 0)
        if version < (3, 32, 0):
            self.insertmanyvalues_max_parameters = 999


dialect = SQLiteDialect_syncLite