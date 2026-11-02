"""SyncLite SQLAlchemy sample.

SQLAlchemy reuses its SQLite SQL compilation and ORM/Core APIs while the
registered ``sqlite+synclite`` dialect routes all work through SyncLite's
DB-API wrapper and mutation log.
"""

import synclite as sl
from synclite import dbapi2
from sqlalchemy import create_engine, text

DB_PATH = "sample_sqlalchemy.db"
DEVICE_NAME = "sample_sqlalchemy"
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

    engine = create_engine(f"sqlite+synclite:///{DB_PATH}")
    try:
        with engine.begin() as connection:
            connection.execute(text("DROP TABLE IF EXISTS users"))
            connection.execute(
                text(
                    "CREATE TABLE users(" 
                    "id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"
                )
            )
            connection.execute(
                text("INSERT INTO users(id, name, score) VALUES(:id, :name, :score)"),
                [{"id": 1, "name": "Alice", "score": 100},
                 {"id": 2, "name": "Bob", "score": 200}],
            )
            connection.execute(
                text("UPDATE users SET score = :score WHERE id = :id"),
                {"score": 250, "id": 2},
            )
            connection.execute(
                text("INSERT INTO users(id, name, score) VALUES(:id, :name, :score)"),
                [{"id": 3, "name": "Carol", "score": 300},
                 {"id": 4, "name": "Dave", "score": 400}],
            )
            rows = connection.execute(
                text("SELECT id, name, score FROM users ORDER BY id")
            ).all()
            print("[LOCAL]", rows)
    finally:
        engine.dispose()

    # SQLAlchemy commits locally. Roll the committed segment before waiting
    # for the destination in this short-lived demonstration.
    sync_conn = dbapi2.connect(DB_PATH)
    try:
        sync_conn.flush()
    finally:
        sync_conn.close()

    try:
        sl.await_sync(DB_PATH, 30.0)
        print("[SYNC] await_sync succeeded")
    except Exception as exc:
        print(f"[SYNC] await_sync failed: {exc}")


if __name__ == "__main__":
    main()
