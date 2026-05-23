use synclite_core::Result;
use synclite_db_traits::Value;
use synclite::SyncLite;
use synclite::duckdb::Connection;
use synclite_core::DeviceType;

fn main() -> Result<()> {
    const DB_PATH: &str = "sample_duckdb.duckdb";
    const CONF_PATH: &str = "synclite_logger.conf";

    SyncLite::initialize_with_config_path(DeviceType::DuckDb, DB_PATH, CONF_PATH)?;
    // SyncLite::initialize(DeviceType::DuckDb, DB_PATH)?;
    // SyncLite::initialize_with_device_name(DeviceType::DuckDb, DB_PATH, "sample-duckdb")?;
    // SyncLite::initialize_with_config(DeviceType::DuckDb, DB_PATH, synclite_config::SyncLiteConfig::load(CONF_PATH)?)?;
    // SyncLite::initialize_with_config_path_and_device_name(DeviceType::DuckDb, DB_PATH, CONF_PATH, "sample-duckdb")?;
    // SyncLite::initialize_with_config_and_device_name(DeviceType::DuckDb, DB_PATH, synclite_config::SyncLiteConfig::load(CONF_PATH)?, "sample-duckdb")?;

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
