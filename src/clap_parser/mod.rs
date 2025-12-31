use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, ValueEnum)]
pub enum CheckMetadata {
    Yes,
    No,
}

#[derive(Parser, Debug)]
#[command(
    author = "Oleg Potapenko",
    version = "0.1.0",
    about = "Utility for import large tables from SQL Server to Postgres"
)]
pub struct Args {
    #[arg(long, short = 'F', default_value = "pg_pump.toml")]
    pub config_file: String,

    #[arg(long, short = 'r', default_value = "1")]
    pub threads: u32,

    #[arg(long, short = 'o', default_value = "600")]
    pub timeout: u64,

    #[arg(long, short = 's', default_value = "Sample")] // TODO: remove default value
    pub source_schema: String,

    #[arg(long, short = 't', default_value = "AllTypes")] // TODO: remove default value
    pub source_table: String,

    #[arg(long, short = 'S', default_value = "Sample")] // TODO: remove default value
    pub target_schema: String,

    #[arg(long, short = 'T', default_value = "AllTypes")] // TODO: remove default value
    pub target_table: String,

    #[arg(long, short = 'C', default_value = "ID")] // TODO: remove default value
    pub column: String,

    #[arg(long, short = 'M', value_enum, default_value_t = CheckMetadata::Yes)]
    pub check_metadata: CheckMetadata,
}
