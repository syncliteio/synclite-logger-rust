use synclite_core::{DeviceType, Result};
use synclite_db_traits::Value;
use synclite::SyncLite;
use synclite::rusqlite::Connection;

fn main() -> Result<()> {
    const DB_PATH: &str = "sample_streaming_sqlite.db";
    const CONF_PATH: &str = "sample_streaming.conf";

    // Self-contained streaming-device config for wrapper open_with_config.
    std::fs::write(
        CONF_PATH,
        format!(
            "device-name=sample-streaming\n\
db-engine=SQLITE\n\
device-type=STREAMING\n\
db-path={DB_PATH}\n\
local-data-stage-directory=synclite-stage\n"
        ),
    )?;

    // Explicit initialize variant with device type + config path.
    SyncLite::initialize_with_config_path(DeviceType::Streaming, DB_PATH, CONF_PATH)?;
    // SyncLite::initialize(DeviceType::Streaming, DB_PATH)?;
    // SyncLite::initialize_with_device_name(DeviceType::Streaming, DB_PATH, "sample-streaming")?;
    // SyncLite::initialize_with_config(DeviceType::Streaming, DB_PATH, synclite_config::SyncLiteConfig::load(CONF_PATH)?)?;
    // SyncLite::initialize_with_config_path_and_device_name(DeviceType::Streaming, DB_PATH, CONF_PATH, "sample-streaming")?;
    // SyncLite::initialize_with_config_and_device_name(DeviceType::Streaming, DB_PATH, synclite_config::SyncLiteConfig::load(CONF_PATH)?, "sample-streaming")?;

    // Streaming is SQLite-backed, so the wrapper surface is still rusqlite-like.
    let mut conn = Connection::open_with_config(CONF_PATH)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)",
        &[],
    )?;

    {
        let mut stmt = conn.prepare("INSERT INTO events(id, category, amount) VALUES(?, ?, ?)")?;
        stmt.execute(&[Value::Int(1), Value::Text("stream".to_string()), Value::Int(40)])?;
        stmt.execute(&[Value::Int(2), Value::Text("stream".to_string()), Value::Int(60)])?;
    }

    // STREAMING allows INSERTs and DDL, but rejects UPDATE / DELETE.
    let update_err = conn.execute(
        "UPDATE events SET amount = ? WHERE id = ?",
        &[Value::Int(90), Value::Int(2)],
    )
    .expect_err("streaming should reject UPDATE");
    println!("UPDATE rejected: {update_err}");

    let delete_err = conn.execute("DELETE FROM events WHERE id = ?", &[Value::Int(1)])
        .expect_err("streaming should reject DELETE");
    println!("DELETE rejected: {delete_err}");

    conn.commit()?;

    // DML is logged, not persisted into the SQLite file for streaming.
    let rows = conn.query("SELECT id, category, amount FROM events ORDER BY id", &[])?;
    println!("rows visible in backing db: {}", rows.len());

    conn.close()?;
    Ok(())
}
