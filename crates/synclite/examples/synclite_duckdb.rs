use logger_core::Result;
use logger_db_traits::Value;
use synclite::duckdb::Connection;
use logger_core::DeviceType;

fn main() -> Result<()> {
    const DB_PATH: &str = "sample_duckdb.duckdb";

    const DEVICE_NAME: &str = "sampledevice";

    synclite::initialize(
        DeviceType::DUCKDB,
        DEVICE_NAME,
        DB_PATH,
        None,
        synclite::SyncLiteOptions::default(),
    )?;

    // Destination-aware shape:
    // synclite::initialize(DeviceType::DUCKDB, DEVICE_NAME, DB_PATH, None, synclite::SyncLiteOptions::default())?;

    // PostgreSQL destination example:
    // synclite::initialize(
    //     DeviceType::DUCKDB,
    //     DEVICE_NAME,
    //     DB_PATH,
    //     Some(synclite::DestinationOptions {
    //         dst_type: synclite::DstType::Postgres,
    //         dst_connection_string: "postgresql://user:password@localhost:5432/synclite_demo".into(),
    //         dst_sync_mode: synclite::DstSyncMode::Consolidation,
    //     }),
    //     synclite::SyncLiteOptions::default(),
    // )?;

    // SQLite destination example:
    // synclite::initialize(
    //     DeviceType::DUCKDB,
    //     DEVICE_NAME,
    //     DB_PATH,
    //     Some(synclite::DestinationOptions {
    //         dst_type: synclite::DstType::Sqlite,
    //         dst_connection_string: "dst_sqlite.db".into(),
    //         dst_sync_mode: synclite::DstSyncMode::Consolidation,
    //     }),
    //     synclite::SyncLiteOptions::default(),
    // )?;

    // DuckDB destination example:
    // synclite::initialize(
    //     DeviceType::DUCKDB,
    //     DEVICE_NAME,
    //     DB_PATH,
    //     Some(synclite::DestinationOptions {
    //         dst_type: synclite::DstType::Duckdb,
    //         dst_connection_string: "dst_duckdb.duckdb".into(),
    //         dst_sync_mode: synclite::DstSyncMode::Consolidation,
    //     }),
    //     synclite::SyncLiteOptions::default(),
    // )?;

    // Mirrors a duckdb-rs style app with execute/query/prepare lifecycle.
    let mut conn = Connection::open(DB_PATH)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS events(id INTEGER, category TEXT, amount INTEGER)",
        &[],
    )?;

    {
        let mut stmt = conn.prepare("INSERT INTO events(id, category, amount) VALUES(?, ?, ?)")?;
        stmt.execute(&[Value::Int(1), Value::Text("retail".to_string()), Value::Int(40)])?;
        stmt.execute(&[Value::Int(2), Value::Text("retail".to_string()), Value::Int(60)])?;
    }

    conn.execute(
        "UPDATE events SET amount = ? WHERE id = ?",
        &[Value::Int(90), Value::Int(2)],
    )?;
    conn.commit()?;

    {
        let mut stmt = conn.prepare("INSERT INTO events(id, category, amount) VALUES(?, ?, ?)")?;
        stmt.add_batch(&[Value::Int(3), Value::Text("wholesale".to_string()), Value::Int(120)]);
        stmt.add_batch(&[Value::Int(4), Value::Text("wholesale".to_string()), Value::Int(180)]);
        stmt.execute_batch()?;
    }
    conn.commit()?;

    let rows = conn.query("SELECT id, category, amount FROM events ORDER BY id", &[])?;
    for row in rows {
        println!("{:?}", row);
    }

    conn.close()?;
    Ok(())
}





