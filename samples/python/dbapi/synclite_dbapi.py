"""SyncLite Python DB-API 2.0 sample.

This uses the PEP 249 facade over the same SyncLite Rust runtime. The
application code uses connections, cursors, execute, executemany, fetchall,
commit, and rollback rather than SyncLite-specific Statement methods.
"""

import synclite as sl
from synclite import dbapi2

DB_PATH = "sample_dbapi.db"
DEVICE_NAME = "sample_dbapi"
POSTGRES_CONN = "postgresql://postgres:postgres@localhost:5432/syncdb"


def main() -> None:
    sl.initialize(
        device_type="SQLITE",
        device_name=DEVICE_NAME,
        db_path=DB_PATH,
        destination=sl.DestinationOptions(
            dst_type="POSTGRES",
            dst_connection_string=POSTGRES_CONN,
            dst_database="syncdb",
            dst_schema="syncschema",
            dst_sync_mode="CONSOLIDATION",
        ),
    )

    conn = dbapi2.connect(DB_PATH)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS users")
                cursor.execute(
                    "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"
                )
                cursor.executemany(
                    "INSERT INTO users(id, name, score) VALUES(?, ?, ?)",
                    [(1, "Alice", 100), (2, "Bob", 200)],
                )
                cursor.execute("UPDATE users SET score = ? WHERE id = ?", (250, 2))
                cursor.executemany(
                    "INSERT INTO users(id, name, score) VALUES(?, ?, ?)",
                    [(3, "Carol", 300), (4, "Dave", 400)],
                )
                cursor.execute("SELECT id, name, score FROM users ORDER BY id")
                print("[LOCAL]", cursor.fetchall())
                print("[METADATA]", cursor.description)
    finally:
        conn.flush()
        try:
            sl.await_sync(DB_PATH, 30.0)
            print("[SYNC] await_sync succeeded")
        except Exception as exc:
            print(f"[SYNC] await_sync failed: {exc}")
        conn.close()


if __name__ == "__main__":
    main()
