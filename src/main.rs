mod clap_parser;
mod config_provider;
mod helpers;
mod postgres_consumer;
mod sql_server_provider;
mod version;

use crate::clap_parser::Args;
use crate::config_provider::ConfigProvider;
use crate::helpers::{print_banner, print_separator};
use crate::postgres_consumer::postgres_consumer::PostgresConsumer;
use crate::sql_server_provider::sql_server_provider::SqlServerProvider;
use anyhow::Result;
use clap::Parser;
use colored::Colorize;
use futures_util::TryStreamExt;
use futures_util::future::join_all;
use std::pin::pin;
use std::process;
use tiberius::{ColumnType, QueryItem};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_postgres::GenericClient;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;

const MIN_THREADS: u32 = 10;
const MAX_THREADS: u32 = 100;
const MIN_TIMEOUT: u64 = 3;
const MAX_TIMEOUT: u64 = 600;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    print_separator();
    print_banner();
    print_separator();
    // region Command Line Args
    let threads = adjust_number_of_threads(&args.threads);
    println!("Threads: <{}>", threads);
    let timeout = adjust_timeout(&args.timeout);
    println!("Timeout: <{}> seconds", timeout);
    let table_name = args.table.clone();
    println!("Table name: <{}>", &table_name);
    let schema_name = args.schema.clone();
    println!("Schema name: <{}>", &schema_name);
    let column_name = args.column.clone();
    println!("Column name: <{}>", &column_name);
    // endregion
    print_separator();
    // region Config File
    println!("Loading Config File: <{}> ", &args.config_file);
    let config_provider = ConfigProvider::new(&args.config_file);
    let file_load_result = config_provider.read_config().await;
    if file_load_result.is_err() {
        eprintln!("{}", file_load_result.err().unwrap().to_string().red());
        process::exit(1);
    }
    let config = file_load_result.ok().unwrap();
    println!("{}", "DONE Loading Config File".green());
    // endregion
    print_separator();
    // region SQL Server Connection Pool
    println!("Creating SQL Server Connection Pool ...");
    let sql_server_provider = SqlServerProvider::new(&config.get_source_database_as_ref());
    let sql_server_pool_result = sql_server_provider
        .create_connection_pool(threads.clone(), timeout.clone())
        .await;
    if sql_server_pool_result.is_err() {
        eprintln!(
            "{}",
            sql_server_pool_result.err().unwrap().to_string().red()
        );
        process::exit(1);
    }
    let sql_server_pool = sql_server_pool_result.ok().unwrap();
    println!("{}", "DONE Creating SQL Server Connection Pool".green());
    // endregion
    print_separator();
    // region Postgres Connection Pool
    println!("Creating SQL Server Connection Pool ...");
    let postgres_consumer = PostgresConsumer::new(&config.get_target_database_as_ref());
    let postgres_pool_result = postgres_consumer
        .create_connection_pool(threads.clone(), timeout.clone())
        .await;
    if postgres_pool_result.is_err() {
        eprintln!("{}", postgres_pool_result.err().unwrap().to_string().red());
        process::exit(1);
    }
    let postgres_pool = postgres_pool_result.ok().unwrap();
    println!("{}", "DONE Creating Postgres Connection Pool".green());
    // endregion
    print_separator();
    // region COPY data from SQL Server to Postgres
    println!("COPY data from SQL Server to Postgres ...");
    let now = Instant::now();
    let mut handles = Vec::new();
    for thread_id in 0..threads.clone() {
        let sql_server_pool = sql_server_pool.clone();
        let postgres_pool = postgres_pool.clone();
        let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            let mut sql_server_client = sql_server_pool.get().await.unwrap();
            let mut sql_server_stream = sql_server_client
                .query(
                    "SELECT ID, FileNumber, Code FROM [Sample].[TestData1] WITH (NOLOCK);", // TODO
                    &[],
                )
                .await
                .unwrap();
            let postgres_connection = postgres_pool.get().await?;
            let postgres_client = postgres_connection.client();
            let postgres_sink = postgres_client
                .copy_in("COPY \"Sample\".\"TestData1\" (\"ID\", \"FileNumber\", \"Code\") FROM STDIN BINARY")
                .await?;

            let mut postgres_writer = pin!(BinaryCopyInWriter::new(
                postgres_sink,
                &[Type::INT8, Type::INT4, Type::VARCHAR]
            ));
            let mut count = 0u64;
            while let Some(item) = sql_server_stream.try_next().await.unwrap() {
                match item {
                    QueryItem::Row(row) => {
                        let id: i64 = row.try_get::<i64, _>(0)?.expect("NULL id");
                        let file_number: i32 = row.try_get::<i32, _>(1)?.expect("NULL file_number");
                        let code: &str = row.try_get::<&str, _>(2)?.expect("NULL code");
                        //println!("i = {}, ID = {}, FN = {}, C = {}", i, id, file_number, code);
                        postgres_writer
                            .as_mut()
                            .write(&[&id, &file_number, &code])
                            .await?;
                    }
                    _ => {}
                }
                count += 1;
                if count % 10_000 == 0 {
                    println!("thread_id = {}, count = {}", thread_id, count);
                }
            }
            postgres_writer.finish().await?;

            Ok(())
        });
        handles.push(handle);
    }
    join_all(handles).await;
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    println!(
        "{}",
        "DONE COPY data from SQL Server to Postgres ...".green()
    );
    // endregion
    print_separator();

    Ok(())
}

fn adjust_number_of_threads(threads: &u32) -> u32 {
    if *threads < MIN_THREADS {
        return MIN_THREADS;
    }
    if *threads > MAX_THREADS {
        return MAX_THREADS;
    }
    *threads
}

fn adjust_timeout(timeout: &u64) -> u64 {
    if *timeout < MIN_TIMEOUT {
        return MIN_TIMEOUT;
    }
    if *timeout > MAX_TIMEOUT {
        return MAX_TIMEOUT;
    }
    *timeout
}
