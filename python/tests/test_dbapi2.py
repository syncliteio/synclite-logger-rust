import sys
import unittest
from datetime import date, datetime, time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from synclite import dbapi2


class _FakeStatement:
    def __init__(self, native, sql):
        self.native = native
        self.sql = sql
        self.batches = []

    def add_batch(self, params):
        self.batches.append(tuple(params or ()))

    def execute_batch(self):
        self.native.calls.append(("execute_batch", self.sql, list(self.batches)))
        return [1 for _ in self.batches]


class _FakeNative:
    last = None

    def __init__(self):
        self.calls = []
        self.next_result = SimpleNamespace(
            rows=None,
            columns=[],
            row_count=0,
            last_insert_rowid=None,
            is_mutating=True,
        )
        type(self).last = self

    @classmethod
    def open(cls, path):
        native = cls()
        native.calls.append(("open", path))
        return native

    @classmethod
    def initialize(cls, path):
        return cls.open(path)

    @classmethod
    def open_with_config(cls, path):
        return cls.open(path)

    @classmethod
    def initialize_with_config(cls, path):
        return cls.open(path)

    def set_auto_commit(self, value):
        self.calls.append(("set_auto_commit", value))

    def execute_with_result(self, sql, params):
        self.calls.append(("execute_with_result", sql, params))
        return self.next_result

    def prepare(self, sql):
        self.calls.append(("prepare", sql))
        return _FakeStatement(self, sql)

    def commit(self):
        self.calls.append(("commit",))

    def rollback(self):
        self.calls.append(("rollback",))

    def flush(self):
        self.calls.append(("flush",))

    def close(self):
        self.calls.append(("close",))


class DbApiTests(unittest.TestCase):
    def setUp(self):
        self.old_sqlite = dbapi2._SQLiteConnection
        self.old_duckdb = dbapi2._DuckDBConnection
        dbapi2._SQLiteConnection = _FakeNative
        dbapi2._DuckDBConnection = _FakeNative

    def tearDown(self):
        dbapi2._SQLiteConnection = self.old_sqlite
        dbapi2._DuckDBConnection = self.old_duckdb

    def test_module_metadata(self):
        self.assertEqual(dbapi2.apilevel, "2.0")
        self.assertEqual(dbapi2.threadsafety, 1)
        self.assertEqual(dbapi2.paramstyle, "qmark")

    def test_description_exists_for_empty_result(self):
        connection = dbapi2.connect("sample.db")
        native = _FakeNative.last
        native.next_result = SimpleNamespace(
            rows=[],
            columns=[SimpleNamespace(name="id", type_name="INTEGER")],
            row_count=None,
            last_insert_rowid=None,
            is_mutating=False,
        )

        cursor = connection.execute("SELECT id FROM t WHERE 1 = 0")

        self.assertEqual(cursor.fetchall(), [])
        self.assertEqual(cursor.description[0][0], "id")
        self.assertEqual(cursor.description[0][1], dbapi2.NUMBER)
        self.assertEqual(len(cursor.description[0]), 7)
        self.assertEqual(cursor.rowcount, -1)

    def test_fetching_and_cursor_iteration(self):
        connection = dbapi2.connect("sample.db")
        native = _FakeNative.last
        native.next_result = SimpleNamespace(
            rows=[[1, "one"], [2, "two"], [3, "three"]],
            columns=[
                SimpleNamespace(name="id", type_name="BIGINT"),
                SimpleNamespace(name="name", type_name="VARCHAR"),
            ],
            row_count=None,
            last_insert_rowid=None,
            is_mutating=False,
        )
        cursor = connection.cursor().execute("SELECT id, name FROM t")

        self.assertEqual(cursor.fetchone(), (1, "one"))
        cursor.arraysize = 1
        self.assertEqual(cursor.fetchmany(), [(2, "two")])
        self.assertEqual(list(cursor), [(3, "three")])
        self.assertIsNone(cursor.fetchone())

    def test_mutation_rowcount_and_lastrowid(self):
        connection = dbapi2.connect("sample.db")
        native = _FakeNative.last
        native.next_result = SimpleNamespace(
            rows=None,
            columns=[],
            row_count=1,
            last_insert_rowid=42,
            is_mutating=True,
        )

        cursor = connection.execute("INSERT INTO t(name) VALUES(?)", ["answer"])

        self.assertEqual(cursor.rowcount, 1)
        self.assertEqual(cursor.lastrowid, 42)
        self.assertIsNone(cursor.description)
        with self.assertRaises(dbapi2.ProgrammingError):
            cursor.fetchone()

    def test_executemany_uses_prepared_batch(self):
        connection = dbapi2.connect("sample.db")

        cursor = connection.executemany(
            "INSERT INTO t(id) VALUES(?)", ((1,), (2,), (3,))
        )

        self.assertEqual(cursor.rowcount, 3)
        self.assertIsNone(cursor.lastrowid)
        self.assertIn(
            (
                "execute_batch",
                "INSERT INTO t(id) VALUES(?)",
                [(1,), (2,), (3,)],
            ),
            _FakeNative.last.calls,
        )

    def test_close_rolls_back_before_native_close(self):
        connection = dbapi2.connect("sample.db")
        native = _FakeNative.last

        connection.close()
        connection.close()

        self.assertEqual(
            native.calls[-3:],
            [("rollback",), ("set_auto_commit", True), ("close",)],
        )
        with self.assertRaises(dbapi2.InterfaceError):
            connection.cursor()

    def test_duckdb_batch_rowcount_is_unknown(self):
        connection = dbapi2.connect("sample.duckdb", backend="duckdb")

        cursor = connection.executemany("INSERT INTO t VALUES(?)", [(1,), (2,)])

        self.assertEqual(cursor.rowcount, -1)

    def test_flush_requires_completed_transaction(self):
        connection = dbapi2.connect("sample.db")
        native = _FakeNative.last
        native.next_result = SimpleNamespace(
            rows=None,
            columns=[],
            row_count=1,
            last_insert_rowid=None,
            is_mutating=True,
        )
        connection.execute("UPDATE t SET value = ?", (1,))

        with self.assertRaises(dbapi2.ProgrammingError):
            connection.flush()

        connection.commit()
        connection.flush()
        self.assertEqual(native.calls[-2:], [("commit",), ("flush",)])

    def test_connection_context_commits_before_flush(self):
        connection = dbapi2.connect("sample.db")
        native = _FakeNative.last
        native.next_result = SimpleNamespace(
            rows=None,
            columns=[],
            row_count=1,
            last_insert_rowid=None,
            is_mutating=True,
        )

        with connection:
            connection.execute("UPDATE t SET value = ?", (1,))
        connection.flush()

        self.assertEqual(native.calls[-2:], [("commit",), ("flush",)])

    def test_rejects_sql_transaction_control(self):
        connection = dbapi2.connect("sample.db")

        with self.assertRaises(dbapi2.ProgrammingError):
            connection.execute("/* application */ COMMIT")

    def test_temporal_parameters_are_adapted_to_iso_text(self):
        connection = dbapi2.connect("sample.db")
        native = _FakeNative.last

        connection.execute(
            "INSERT INTO t VALUES (?, ?, ?)",
            (date(2025, 1, 2), time(3, 4, 5), datetime(2025, 1, 2, 3, 4, 5)),
        )

        self.assertIn(
            (
                "execute_with_result",
                "INSERT INTO t VALUES (?, ?, ?)",
                ("2025-01-02", "03:04:05", "2025-01-02 03:04:05"),
            ),
            native.calls,
        )

    def test_interval_type_is_datetime_not_number(self):
        type_code = dbapi2._type_code("INTERVAL")

        self.assertEqual(type_code, dbapi2.DATETIME)
        self.assertNotEqual(type_code, dbapi2.NUMBER)


if __name__ == "__main__":
    unittest.main()
