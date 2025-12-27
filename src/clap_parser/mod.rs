use clap::Parser;

#[derive(Parser, Debug)]
#[command(author = "Oleg Potapenko", version = "0.1.0", about = "Utility for import large tables from SQL Server to Postgres")]
pub struct Args {
    #[arg(long, short, default_value = "pg_pump.toml")]
    pub config_file: String,
    #[arg(long, default_value = "15")]
    pub timeout: i32,
    #[arg(long, short, default_value = "10")]
    pub threads: i32,
}