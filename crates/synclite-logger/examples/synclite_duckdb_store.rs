use synclite_core::{DeviceType, Result};
use synclite_db_traits::Value;
use synclite::SyncLite;
use synclite::duckdb::Connection;

fn main() -> Result<()> {
    const DB_PATH: &str = "sample_duckdb_store.duckdb";
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
    SyncLite::initialize_with_config_path(DeviceType::DuckDbStore, DB_PATH, CONF_PATH)?;
    // SyncLite::initialize(DeviceType::DuckDbStore, DB_PATH)?;
    // SyncLite::initialize_with_device_name(DeviceType::DuckDbStore, DB_PATH, "sample-duckdb-store")?;
    // SyncLite::initialize_with_config(DeviceType::DuckDbStore, DB_PATH, synclite_config::SyncLiteConfig::load(CONF_PATH)?)?;
    // SyncLite::initialize_with_config_path_and_device_name(DeviceType::DuckDbStore, DB_PATH, CONF_PATH, "sample-duckdb-store")?;
    // SyncLite::initialize_with_config_and_device_name(DeviceType::DuckDbStore, DB_PATH, synclite_config::SyncLiteConfig::load(CONF_PATH)?, "sample-duckdb-store")?;

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
