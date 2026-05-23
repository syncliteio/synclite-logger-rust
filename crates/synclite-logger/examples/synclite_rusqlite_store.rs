use synclite_core::{DeviceType, Result};
use synclite_db_traits::Value;
use synclite::SyncLite;
use synclite::rusqlite::Connection;

fn main() -> Result<()> {
    const DB_PATH: &str = "sample_rusqlite_store_sqlite.db";
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
    SyncLite::initialize_with_config_path(DeviceType::SqliteStore, DB_PATH, CONF_PATH)?;
    // SyncLite::initialize(DeviceType::SqliteStore, DB_PATH)?;
    // SyncLite::initialize_with_device_name(DeviceType::SqliteStore, DB_PATH, "sample-rusqlite-store")?;
    // SyncLite::initialize_with_config(DeviceType::SqliteStore, DB_PATH, synclite_config::SyncLiteConfig::load(CONF_PATH)?)?;
    // SyncLite::initialize_with_config_path_and_device_name(DeviceType::SqliteStore, DB_PATH, CONF_PATH, "sample-rusqlite-store")?;
    // SyncLite::initialize_with_config_and_device_name(DeviceType::SqliteStore, DB_PATH, synclite_config::SyncLiteConfig::load(CONF_PATH)?, "sample-rusqlite-store")?;

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
