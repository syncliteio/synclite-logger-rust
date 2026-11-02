"""Core SyncLite Python API sample.

This is the direct Rust-shaped API: Connection, Statement, query, commit,
flush, and await_sync. It is the Python counterpart of the core Rust sample.
"""

import synclite as sl

DB_PATH = "sample_core.db"
DEVICE_NAME = "sample_core"
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

    conn = sl.Connection.open(DB_PATH)
    conn.execute("DROP TABLE IF EXISTS users")
    conn.execute(
        "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"
    )

    stmt = conn.prepare("INSERT INTO users(id, name, score) VALUES(?, ?, ?)")
    stmt.execute([1, "Alice", 100])
    stmt.execute([2, "Bob", 200])
    conn.execute("UPDATE users SET score = ? WHERE id = ?", [250, 2])
    conn.commit()

    stmt = conn.prepare("INSERT INTO users(id, name, score) VALUES(?, ?, ?)")
    stmt.add_batch([3, "Carol", 300])
    stmt.add_batch([4, "Dave", 400])
    stmt.execute_batch()
    conn.commit()

    print("[LOCAL]", conn.query("SELECT id, name, score FROM users ORDER BY id"))

    conn.flush()
    try:
        sl.await_sync(DB_PATH, 30.0)
        print("[SYNC] await_sync succeeded")
    except Exception as exc:
        print(f"[SYNC] await_sync failed: {exc}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
