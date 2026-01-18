use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, ValueEnum)]
pub enum YesNoEnum{
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

    #[arg(long, short = 'r', default_value = "10")]
    pub threads: u32,

    #[arg(long, short = 'o', default_value = "600")]
    pub timeout: u64,

    #[arg(long, short = 's', default_value = "Sample")] // TODO: remove default value
    pub source_schema: String,

    #[arg(long, short = 't', default_value = "TestData1")] // TODO: remove default value
    pub source_table: String,

    #[arg(long, short = 'S', default_value = "Sample")] // TODO: remove default value
    pub target_schema: String,

    #[arg(long, short = 'T', default_value = "TestData1")] // TODO: remove default value
    pub target_table: String,

    #[arg(long, short = 'C', default_value = "ID")] // TODO: remove default value
    pub column: String,

    #[arg(long, short = 'M', value_enum, default_value_t = YesNoEnum::Yes)]
    pub check_metadata: YesNoEnum,

    #[arg(long, short = 'W', default_value = "5", help = "Wait period in seconds between tasks")]
    pub wait_period: u64, // TODO:

    #[arg(long, short = 'X', value_enum, default_value_t = YesNoEnum::Yes, help = "TRUNCATE target table if it's not empty")]
    pub truncate_target_table: YesNoEnum, // TODO:

}
