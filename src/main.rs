mod clap_parser;
mod config_provider;
mod helpers;
mod postgres_consumer;
mod shared;
mod sql_server_provider;
mod version;

use crate::clap_parser::Args;
use crate::config_provider::ConfigProvider;
use crate::helpers::{print_banner, print_separator};
use crate::postgres_consumer::postgres_consumer::PostgresConsumer;
use crate::shared::pg_pump_column_type::PgPumpColumnType;
use crate::shared::postgres_column_types::get_postgres_column_type;
use crate::sql_server_provider::sql_server_provider::SqlServerProvider;
use anyhow::Result;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use bb8_tiberius::ConnectionManager;
use clap::Parser;
use colored::Colorize;
use futures_util::TryStreamExt;
use futures_util::future::join_all;
use std::pin::pin;
use std::process;
use tiberius::{ColumnType, QueryItem};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{GenericClient, NoTls};

const MIN_THREADS: u32 = 1;
const MAX_THREADS: u32 = 100;
const MIN_TIMEOUT: u64 = 3;
const MAX_TIMEOUT: u64 = 1200;

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
    let source_schema_name = args.source_schema.clone();
    println!("Source schema name: <{}>", &source_schema_name);
    let source_table_name = args.source_table.clone();
    println!("Source table name: <{}>", &source_table_name);
    let target_schema_name = args.target_schema.clone();
    println!("Target schema name: <{}>", &target_schema_name);
    let target_table_name = args.target_table.clone();
    println!("Target table name: <{}>", &target_table_name);
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
    // region SQL Server Metadata
    println!("Getting SQL Server metadata ...");
    let sql_server_provider = SqlServerProvider::new(&config.get_source_database_as_ref());
    let long_count = sql_server_provider
        .get_long_count(&source_schema_name, &source_table_name)
        .await;

    if long_count.is_err() {
        eprintln!("{}", long_count.err().unwrap().to_string().red());
        process::exit(1);
    }
    let long_count = long_count.ok().unwrap();
    if long_count == 0 {
        println!(
            "{}",
            format!(
                "ATTEMPT TO COPY EMPTY TABLE {}.{}. Exiting...",
                &source_schema_name, &source_table_name
            )
            .red()
        );
        process::exit(0);
    }
    let sql_server_metadata_result = sql_server_provider
        .get_table_metadata(&source_schema_name, &source_table_name)
        .await;
    if sql_server_metadata_result.is_err() {
        eprintln!(
            "{}",
            sql_server_metadata_result.err().unwrap().to_string().red()
        );
        process::exit(1);
    }
    let sql_server_metadata = sql_server_metadata_result.ok().unwrap();
    println!("{}", "DONE Getting SQL Server metadata".green());
    // endregion
    print_separator();
    // region Postgres Metadata
    println!("Getting Postgres metadata ...");
    let postgres_consumer = PostgresConsumer::new(&config.get_target_database_as_ref());
    // let postgres_metadata = postgres_consumer
    //     .get_table_metadata(&schema_name, &table_name)
    //     .await;
    println!("{}", "DONE Postgres metadata".green());
    // endregion
    print_separator();
    // region SQL Server Connection Pool
    println!("Creating SQL Server Connection Pool ...");
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
    println!("Creating Postgres Connection Pool ...");
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
    copy_data(
        threads,
        sql_server_pool,
        postgres_pool,
        sql_server_metadata,
        &source_schema_name,
        &source_table_name,
        &target_schema_name,
        &target_table_name,
        &column_name,
    )
    .await;
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

async fn copy_data(
    threads: u32,
    sql_server_pool: Pool<ConnectionManager>,
    postgres_pool: Pool<PostgresConnectionManager<NoTls>>,
    sql_server_metadata: Vec<(String, PgPumpColumnType)>,
    source_schema_name: &String,
    source_table_name: &String,
    target_schema_name: &String,
    target_table_name: &String,
    column_name: &String,
) {
    let now = Instant::now();
    let mut handles = Vec::new();
    let sql_server_columns = sql_server_metadata
        .iter()
        .map(|x| format!("[{}]", x.0.clone()))
        .collect::<Vec<String>>()
        .join(", ");
    let postgres_column_types = sql_server_metadata
        .iter()
        .map(|x| get_postgres_column_type(&x.1))
        .collect::<Vec<Type>>();
    let postgres_columns = sql_server_metadata
        .iter()
        .map(|x| format!("\"{}\"", x.0.clone()))
        .collect::<Vec<String>>()
        .join(", ");
    for thread_id in 0..threads.clone() {
        let sql_server_pool = sql_server_pool.clone();
        let postgres_column_types = postgres_column_types.clone();
        let postgres_pool = postgres_pool.clone();
        let source_schema_name = source_schema_name.clone();
        let source_table_name = source_table_name.clone();
        let target_schema_name = target_schema_name.clone();
        let target_table_name = target_table_name.clone();
        let sql_server_columns = sql_server_columns.clone();
        let postgres_columns = postgres_columns.clone();
        let column_name = column_name.clone();

        let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            let mut sql_server_client = sql_server_pool.get().await.unwrap();
            let sql_server_stream_query = format!(
                "SELECT {} FROM [{}].[{}] WITH (NOLOCK) WHERE {} BETWEEN @P1 AND @P2;",
                sql_server_columns, source_schema_name, source_table_name, column_name
            );
            let start = 0i64; // TODO:
            let end = 10i64;

            let mut sql_server_stream = sql_server_client
                .query(&sql_server_stream_query, &[&start, &end])
                .await
                .unwrap();

            let postgres_connection = postgres_pool.get().await?;
            let postgres_client = postgres_connection.client();

            let postgres_sink_query = format!(
                "COPY \"{}\".\"{}\" ({}) FROM STDIN BINARY;",
                target_schema_name, target_table_name, postgres_columns,
            );
            let postgres_sink = postgres_client.copy_in(&postgres_sink_query).await?;
            let mut postgres_writer = pin!(BinaryCopyInWriter::new(
                postgres_sink,
                &postgres_column_types[..]
            ));
            let mut count = 0u64;
            let mut data_row_boxed: Vec<Box<(dyn ToSql + Sync)>> =
                Vec::with_capacity(postgres_column_types.len());
            while let Some(item) = sql_server_stream.try_next().await.unwrap() {
                data_row_boxed.clear();
                match item {
                    QueryItem::Row(row) => {
                        for (index, column) in row.columns().iter().enumerate() {
                            match column.column_type() {
                                // ColumnType::Bitn => PgPumpColumnType::Boolean,
                                // ColumnType::Int2 => PgPumpColumnType::Byte,
                                ColumnType::Int4 => {
                                    let t_int: i32 =
                                        row.try_get::<i32, _>(index)?.expect("NULL  t_int");
                                    data_row_boxed.push(Box::new(t_int));
                                    continue;
                                }
                                ColumnType::Int8 => {
                                    let t_bigint: i64 =
                                        row.try_get::<i64, _>(index)?.expect("NULL t_bigint");
                                    data_row_boxed.push(Box::new(t_bigint));
                                    continue;
                                }
                                // ColumnType::Daten => PgPumpColumnType::Datetime,
                                // ColumnType::Timen => PgPumpColumnType::Datetime,
                                // ColumnType::Datetime2 => PgPumpColumnType::Datetime,
                                // ColumnType::Decimaln => PgPumpColumnType::Decimal,
                                // ColumnType::Float8 => PgPumpColumnType::Float,
                                // ColumnType::Guid => PgPumpColumnType::Uuid,
                                // ColumnType::NChar => PgPumpColumnType::Char,
                                // ColumnType::BigChar => PgPumpColumnType::Char,
                                // ColumnType::NVarchar => PgPumpColumnType::Varchar,
                                ColumnType::BigVarChar => {
                                    let t_bigvarchar: &str =
                                        row.try_get::<&str, _>(index)?.expect("NULL t_bigvarchar");
                                    data_row_boxed.push(Box::new(t_bigvarchar));
                                    continue;
                                }
                                _ => {}
                            }
                        }


                        postgres_writer
                            .as_mut()
                            //.write_raw()
                            .write(&data_row_boxed[..])
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
    let x = join_all(handles).await;

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
}
