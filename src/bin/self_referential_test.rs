use anyhow::Result;
use chrono::prelude::*;
use duckdb::{params, Connection};

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r"
        CREATE TABLE self_referential (
        id INTEGER PRIMARY KEY NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        cycle_id INTEGER REFERENCES self_referential(id))
      ",
    )?;
    Ok(())
}

fn fill_table(duck: &Connection) -> Result<()> {
    let max_id = 1_000_000;

    let mut appender = duck.appender("self_referential")?;
    for i in 0..max_id {
        let now = Utc::now();
        let reference_id = i;
        appender.append_row(params![i, now, reference_id])?;
    }
    appender.flush();
    println!("Flushed");
    Ok(())
}

fn main() -> Result<()> {
    ::std::env::set_var("RUST_LOG", "info");
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Please specify a path to a new duckdb file");
        std::process::exit(-1);
    }
    let duckdb_path = &args[1];
    if std::path::Path::new(duckdb_path).exists() {
        std::fs::remove_file(duckdb_path)?;
    }

    let duck = Connection::open(duckdb_path)?;
    create_tables(&duck)?;
    fill_table(&duck)?;
    Ok(())
}
