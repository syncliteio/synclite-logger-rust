use logger_core::{DeviceType, Result};
use logger_db_traits::Value;
use synclite::duckdb::Connection;

fn main() -> Result<()> {
    const DB_PATH: &str = "sample_duckdb_store.duckdb";

    const DEVICE_NAME: &str = "sampledevice";
    const CONF_PATH: &str = "sample_duckdb_store.conf";

    // Self-contained store-device config for wrapper open_with_config.
    std::fs::write(
        CONF_PATH,
        format!(
            "device-name=sample-duckdb-store\n\
db-engine=DUCKDB\n\
device-type=DUCKDB_STORE\n\
db-path={DB_PATH}\n\
local-data-stage-directory=synclite-stage\n"
        ),
    )?;

    // Explicit initialize variant with device type + config path.
    synclite::initialize(
        DeviceType::DUCKDB_STORE,
        DEVICE_NAME,
        DB_PATH,
        None,
        synclite::SyncLiteOptions {
            config_path: Some(CONF_PATH.into()),
            ..Default::default()
        },
    )?;

    // Destination-aware shape:
    // synclite::initialize(DeviceType::DUCKDB_STORE, DEVICE_NAME, DB_PATH, None, synclite::SyncLiteOptions::default())?;

    // PostgreSQL destination example:
    // synclite::initialize(
    //     DeviceType::DUCKDB_STORE,
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
    //     DeviceType::DUCKDB_STORE,
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
    //     DeviceType::DUCKDB_STORE,
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

    conn.execute("DELETE FROM events WHERE id = ?", &[Value::Int(1)])?;

    let rows = conn.query("SELECT id, category, amount FROM events ORDER BY id", &[])?;
    for row in rows {
        println!("{:?}", row);
    }

    conn.close()?;
    Ok(())
}





