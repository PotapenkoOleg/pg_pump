mod clap_parser;
mod config_provider;
mod helpers;
mod version;
mod sql_server_provider;
mod postgres_consumer;

use crate::clap_parser::Args;
use crate::config_provider::ConfigProvider;
use crate::helpers::{print_banner, print_separator};
use anyhow::Result;
use clap::Parser;
use colored::Colorize;
use std::process;
use tokio::time::Instant;
use crate::postgres_consumer::postgres_consumer::PostgresConsumer;
use crate::sql_server_provider::sql_server_provider::SqlServerProvider;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    print_separator();
    print_banner();
    print_separator();
    println!("Loading Config File: <{}> ", &args.config_file);
    let config_provider = ConfigProvider::new(&args.config_file);
    let file_load_result = config_provider.read_config().await;
    if file_load_result.is_err() {
        eprintln!("{}", file_load_result.err().unwrap().to_string().red());
        process::exit(1);
    }
    let config = file_load_result.ok().unwrap();
    println!("{}", "DONE Loading Config File".green());
    print_separator();
    // println!("Running SQL Server Provider ...");
    // let sql_server_provider =
    //     SqlServerProvider::new(&config.get_source_database_as_ref());
    // let now = Instant::now();
    // sql_server_provider.sql_server_test().await?;
    // let elapsed = now.elapsed();
    // println!("Elapsed: {:.2?}", elapsed);
    print_separator();
    println!("Running Postgres Consumer ...");
    let postgres_consumer = PostgresConsumer::new(&config.get_target_database_as_ref());
    let now = Instant::now();
    //postgres_consumer.postgres_copy_test().await?;
    postgres_consumer.postgres_pool_test().await?;
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    print_separator();


    Ok(())
}
