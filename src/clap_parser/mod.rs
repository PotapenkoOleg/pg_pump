use clap::{Parser, ValueEnum, value_parser};

#[derive(Debug, Clone, ValueEnum)]
pub enum YesNoEnum {
    Yes,
    No,
}

#[derive(Parser, Debug)]
#[command(
    author = "Oleg Potapenko",
    version = "1.0.0",
    about = "Utility for import large tables from SQL Server to Postgres"
)]
pub struct Args {
    #[arg(
        long,
        short = 'F',
        default_value = "pg_pump.toml",
        help = "Configuration file name"
    )]
    pub config_file: String,

    #[arg(long, short = 'r', default_value = "10", value_parser = value_parser!(u32).range(1..=100), help = "Number of threads from 1 to 100")]
    pub threads: u32,

    #[arg(long, short = 'o', default_value = "600", value_parser = value_parser!(u64).range(3..=1200), help = "Connection timeout in seconds from 3 to 1200")]
    pub timeout: u64,

    #[arg(
        long,
        short = 's',
        help = "Source schema name"
    )]
    pub source_schema: String,

    #[arg(
        long,
        short = 't',
        help = "Source table name"
    )]
    pub source_table: String,

    #[arg(
        long,
        short = 'S',
        help = "Target schema name"
    )]
    pub target_schema: String,

    #[arg(
        long,
        short = 'T',
        help = "Target table name"
    )]
    pub target_table: String,

    #[arg(
        long,
        short = 'C',
        help = "Increasing integer column for ordering source table"
    )]
    pub column: String,

    // #[arg(long, short = 'M', value_enum, default_value_t = YesNoEnum::Yes, help = "Compare metadata from source and target tables")]
    // pub check_metadata: YesNoEnum, // TODO:

    #[arg(long, short = 'W', default_value = "0", value_parser = value_parser!(u64).range(0..=120), help = "Wait period in seconds between tasks from 0 to 120")]
    pub wait_period: u64,

    #[arg(long, short = 'X', value_enum, default_value_t = YesNoEnum::Yes, help = "TRUNCATE target table if it's not empty")]
    pub truncate_target_table: YesNoEnum,

    #[arg(long, short = 'P', default_value = "10000", value_parser = value_parser!(i64).range(1_000..=1_000_000), help = "Minimum records per partition for parallel processing")]
    pub min_records_per_partition: i64,
}
