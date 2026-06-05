use logger_core::{DeviceType, Result};
use logger_db_traits::Value;
use synclite::rusqlite::Connection;

fn main() -> Result<()> {
    const DB_PATH: &str = "sample_rusqlite_store_sqlite.db";

    const DEVICE_NAME: &str = "sampledevice";
    const CONF_PATH: &str = "sample_rusqlite_store.conf";

    // Self-contained store-device config for wrapper open_with_config.
    std::fs::write(
        CONF_PATH,
        format!(
            "device-name=sample-rusqlite-store\n\
db-engine=SQLITE\n\
device-type=SQLITE_STORE\n\
db-path={DB_PATH}\n\
local-data-stage-directory=synclite-stage\n"
        ),
    )?;

    // Explicit initialize variant with device type + config path.
    synclite::initialize(
        DeviceType::SqliteStore,
        DEVICE_NAME,
        DB_PATH,
        None,
        synclite::SyncLiteOptions {
            config_path: Some(CONF_PATH.into()),
            ..Default::default()
        },
    )?;

    // Destination-aware shape:
    // synclite::initialize(DeviceType::SqliteStore, DEVICE_NAME, DB_PATH, None, synclite::SyncLiteOptions::default())?;

    // PostgreSQL destination example:
    // synclite::initialize(
    //     DeviceType::SqliteStore,
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
    //     DeviceType::SqliteStore,
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
    //     DeviceType::SqliteStore,
    //     DEVICE_NAME,
    //     DB_PATH,
    //     Some(synclite::DestinationOptions {
    //         dst_type: synclite::DstType::Duckdb,
    //         dst_connection_string: "dst_duckdb.duckdb".into(),
    //         dst_sync_mode: synclite::DstSyncMode::Consolidation,
    //     }),
    //     synclite::SyncLiteOptions::default(),
    // )?;

    // Keep wrapper-style API surface while running in STORE device mode.
    let mut conn = Connection::open_with_config(CONF_PATH)?;

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

    conn.execute("DELETE FROM users WHERE id = ?", &[Value::Int(2)])?;

    let rows = conn.query("SELECT id, name, score FROM users ORDER BY id", &[])?;
    for row in rows {
        println!("{:?}", row);
    }

    conn.close()?;
    Ok(())
}





