"""duckdb-engine SQL/reflection with SyncLite's logged DB-API connection."""

from duckdb_engine import Dialect as DuckDBDialect
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgresql.base import PGExecutionContext
from sqlalchemy.engine.default import DefaultDialect

from .. import dbapi2
from ._common import SyncLiteDialectMixin


class DuckDBDialect_syncLite(SyncLiteDialectMixin, DuckDBDialect):
    # DuckDB's native isolation is snapshot isolation, not PostgreSQL's
    # configurable READ COMMITTED/SERIALIZABLE modes. This is SQLAlchemy's
    # conventional label for that fixed mode, not an SQL SET command.
    synclite_isolation_level = "REPEATABLE READ"
    default_paramstyle = "qmark"
    execution_ctx_cls = PGExecutionContext
    supports_statement_cache = False  # upstream duckdb-engine policy
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_sane_rowcount_returning = False
    supports_native_decimal = False
    supports_native_uuid = False
    colspecs = dict(DuckDBDialect.colspecs, **{})
    colspecs[sqltypes.Float] = sqltypes.Float

    def do_executemany(self, cursor, statement, parameters, context=None):
        return DefaultDialect.do_executemany(self, cursor, statement, parameters, context)

    def set_readonly(self, connection, value):
        raise dbapi2.NotSupportedError("postgresql_readonly is not a SyncLite DuckDB option")

    def set_deferrable(self, connection, value):
        raise dbapi2.NotSupportedError("postgresql_deferrable is not a SyncLite DuckDB option")

    def get_readonly(self, connection):
        return False

    def get_deferrable(self, connection):
        return False


dialect = DuckDBDialect_syncLite