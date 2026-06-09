use logger_core::Result;
use logger_db_traits::Value;
use synclite::rusqlite::Connection;
use logger_core::DeviceType;

fn main() -> Result<()> {
    const DB_PATH: &str = "sample_rusqlite_sqlite.db";

    const DEVICE_NAME: &str = "sampledevice";

    // Explicit initialize variant with device type + config path.

    // Destination-aware shape:
    // synclite::initialize(DeviceType::SQLITE, DEVICE_NAME, DB_PATH, None, synclite::SyncLiteOptions::default())?;

    // PostgreSQL destination example:
     synclite::initialize(
         DeviceType::SQLITE,
         DEVICE_NAME,
         DB_PATH,
         Some(synclite::DestinationOptions {
             dst_type: synclite::DstType::Postgres,
             dst_connection_string: "postgresql://postgres:postgres@localhost:5432/syncdb".into(),
             dst_database: Some("syncdb".into()),
             dst_schema: Some("syncschema".into()),
             dst_sync_mode: synclite::DstSyncMode::Consolidation,
         }),
         synclite::SyncLiteOptions::default(),
     )?;

    // SQLite destination example:
    // synclite::initialize(
    //     DeviceType::SQLITE,
    //     DEVICE_NAME,
    //     DB_PATH,
    //     Some(synclite::DestinationOptions {
    //         dst_type: synclite::DstType::Sqlite,
    //         dst_connection_string: "dst_sqlite.db".into(),
    //         dst_database: None,
    //         dst_schema: None,
    //         dst_sync_mode: synclite::DstSyncMode::Consolidation,
    //     }),
    //     synclite::SyncLiteOptions::default(),
    // )?;

    // DuckDB destination example:
    // synclite::initialize(
    //     DeviceType::SQLITE,
    //     DEVICE_NAME,
    //     DB_PATH,
    //     Some(synclite::DestinationOptions {
    //         dst_type: synclite::DstType::Duckdb,
    //         dst_connection_string: "dst_duckdb.duckdb".into(),
    //         dst_database: Some("dst_duckdb".into()),
    //         dst_schema: Some("main".into()),
    //         dst_sync_mode: synclite::DstSyncMode::Consolidation,
    //     }),
    //     synclite::SyncLiteOptions::default(),
    // )?;

    // Written in a rusqlite-like style so existing applications can swap
    // the connection type with minimal changes.
    let mut conn = Connection::open(DB_PATH)?;

    conn.execute("DROP TABLE IF EXISTS users", &[])?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
        &[],
    )?;

    {
        let mut stmt = conn.prepare("INSERT INTO users(id, name, score) VALUES(?, ?, ?)")?;
        stmt.execute(&[Value::Int(1), Value::Text("Alice".to_string()), Value::Int(100)])?;
        stmt.execute(&[Value::Int(2), Value::Text("Bob".to_string()), Value::Int(200)])?;
    }

    conn.execute(
        "UPDATE users SET score = ? WHERE name = ?",
        &[Value::Int(250), Value::Text("Bob".to_string())],
    )?;
    conn.commit()?;

    {
        let mut stmt = conn.prepare("INSERT INTO users(id, name, score) VALUES(?, ?, ?)")?;
        stmt.add_batch(&[Value::Int(3), Value::Text("Carol".to_string()), Value::Int(300)]);
        stmt.add_batch(&[Value::Int(4), Value::Text("Dave".to_string()), Value::Int(400)]);
        stmt.execute_batch()?;
    }
    conn.commit()?;

    let rows = conn.query("SELECT id, name, score FROM users ORDER BY id", &[])?;
    for row in rows {
        println!("{:?}", row);
    }

    // Force the active log segment to roll, then block until the
    // in-process shipper + consolidator have fully drained it to the
    // destination. Without this, a short-lived program exits before the
    // background pipeline can apply the changes.
    conn.flush()?;
    synclite::await_sync(DB_PATH, std::time::Duration::from_secs(30))?;

    conn.close()?;
    Ok(())
}





