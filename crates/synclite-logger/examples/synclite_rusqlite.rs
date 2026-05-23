use synclite_core::Result;
use synclite_db_traits::Value;
use synclite::SyncLite;
use synclite::rusqlite::Connection;
use synclite_core::DeviceType;

fn main() -> Result<()> {
    const DB_PATH: &str = "sample_rusqlite_sqlite.db";
    const CONF_PATH: &str = "synclite_logger.conf";

    // Explicit initialize variant with device type + config path.
    SyncLite::initialize_with_config_path(DeviceType::Sqlite, DB_PATH, CONF_PATH)?;
    // SyncLite::initialize(DeviceType::Sqlite, DB_PATH)?;
    // SyncLite::initialize_with_device_name(DeviceType::Sqlite, DB_PATH, "sample-rusqlite")?;
    // SyncLite::initialize_with_config(DeviceType::Sqlite, DB_PATH, synclite_config::SyncLiteConfig::load(CONF_PATH)?)?;
    // SyncLite::initialize_with_config_path_and_device_name(DeviceType::Sqlite, DB_PATH, CONF_PATH, "sample-rusqlite")?;
    // SyncLite::initialize_with_config_and_device_name(DeviceType::Sqlite, DB_PATH, synclite_config::SyncLiteConfig::load(CONF_PATH)?, "sample-rusqlite")?;

    // Written in a rusqlite-like style so existing applications can swap
    // the connection type with minimal changes.
    let mut conn = Connection::open(DB_PATH)?;

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

    conn.close()?;
    Ok(())
}
