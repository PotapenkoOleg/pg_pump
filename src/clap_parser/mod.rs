use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    author = "Oleg Potapenko",
    version = "0.1.0",
    about = "Utility for import large tables from SQL Server to Postgres"
)]
pub struct Args {
    #[arg(long, short = 'F', default_value = "pg_pump.toml")]
    pub config_file: String,

    #[arg(long, short = 't', default_value = "10")]
    pub threads: u32,

    #[arg(long, short = 'o', default_value = "15")]
    pub timeout: u64,

    #[arg(long, short = 'S', default_value = "Sample")] // TODO: remove default value
    pub schema: String,

    #[arg(long, short = 'T', default_value = "TestData1")] // TODO: remove default value
    pub table: String,

    #[arg(long, short = 'C', default_value = "ID")] // TODO: remove default value
    pub column: String,
}
