mod clap_parser;
mod config_provider;
mod helpers;
mod postgres_consumer;
mod postgres_provider;
mod settings;
mod shared;
mod sql_server_provider;
mod version;

use crate::clap_parser::{Args, YesNoEnum};
use crate::config_provider::ConfigProvider;
use crate::helpers::{print_banner, print_separator};
use crate::postgres_consumer::postgres_consumer::PostgresConsumer;
use crate::postgres_provider::postgres_provider::PostgresProvider;
use crate::settings::settings::Settings;
use crate::shared::db_provider::DbProvider;
use crate::shared::pg_pump_column_type::PgPumpColumnType;
use crate::shared::postgres_column_types::PostgresColumnTypeProvider;
use crate::sql_server_provider::sql_server_provider::SqlServerProvider;
use anyhow::Result;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use bb8_tiberius::ConnectionManager;
use clap::Parser;
use colored::Colorize;
use futures_util::TryStreamExt;
use futures_util::future::join_all;
use rust_decimal::prelude::*;
use std::any::Any;
use std::pin::pin;
use std::process;
use std::time::Duration;
use tiberius::{ColumnType, QueryItem};
use time::{Date, PrimitiveDateTime, Time};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{GenericClient, NoTls};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    print_separator();
    print_banner();
    print_separator();

    // region Command Line Args
    let mut settings = Settings::from_args(&args);
    println!("{}", settings);
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
    println!("Getting Source DB configuration ...");
    let source_db_provider: &dyn DbProvider = if config
        .get_source_database_as_ref()
        .get_db_type_as_ref()
        .eq("sql_server")
    {
        &SqlServerProvider::new(&config.get_source_database_as_ref())
    } else if config
        .get_source_database_as_ref()
        .get_db_type_as_ref()
        .eq("postgres")
    {
        &PostgresProvider::new(&config.get_source_database_as_ref())
    } else {
        eprintln!(
            "Unsupported database type: {}",
            config
                .get_source_database_as_ref()
                .get_db_type_as_ref()
                .red()
        );
        process::exit(1);
    };
    println!("{}", "DONE Source DB configuration".green());
    //print_separator();
    let schema_vec = if settings.get_source_schema_name_as_ref().eq("*") {
        let result = source_db_provider.get_all_schemas().await?;
        result
    } else {
        vec![settings.get_source_schema_name_as_ref().to_string()]
    };
    for schema in schema_vec {
        print_separator();
        println!("Source Schema: <{}>", schema.yellow());
        let table_vec = if settings.get_source_table_name_as_ref().eq("*") {
            source_db_provider.get_all_tables_in_schema(&schema).await?
        } else {
            vec![settings.get_source_table_name_as_ref().to_string()]
        };
        for table in table_vec {
            print_separator();
            println!("Source Table: <{}>", table.yellow());
            print_separator();

            // region Table name setup
            let source_schema_name: String = schema.clone();
            let source_table_name: String = table.clone();

            let target_schema_name: String = if settings.get_target_schema_name_as_ref().eq("$") {
                schema.clone()
            } else {
                settings.get_target_schema_name_as_ref().to_string()
            };

            let target_table_name: String = if settings.get_target_table_name_as_ref().eq("$") {
                table.clone()
            } else {
                settings.get_target_table_name_as_ref().to_string()
            };
            // endregion

            // region Source DB Metadata
            println!("Getting Source DB metadata ...");
            let long_count: Result<i64> = source_db_provider
                .get_long_count(
                    &source_schema_name,
                    &source_table_name,
                    settings.get_column_name_as_ref(),
                    settings.get_min(),
                    settings.get_max(),
                )
                .await;
            if long_count.is_err() {
                eprintln!("{}", long_count.err().unwrap().to_string().red());
                process::exit(1);
            }
            let source_db_long_count = long_count.ok().unwrap();
            if source_db_long_count == 0 {
                println!(
                    "{}",
                    format!(
                        "ATTEMPT TO COPY EMPTY TABLE {}.{}. Skipping...",
                        &source_schema_name, &source_table_name
                    )
                    .red()
                );
                continue;
            }
            let mut source_db_long_count_msg =
                format!("Source table count: <{}>", source_db_long_count);
            if settings.get_max() != 0 {
                source_db_long_count_msg.push_str(
                    format!(
                        " between min = <{}> and max = <{}>",
                        settings.get_min(),
                        settings.get_max()
                    )
                    .as_str(),
                );
            }
            println!("{}", source_db_long_count_msg.yellow());
            let number_of_partitions = get_number_of_partitions(
                source_db_long_count,
                settings.get_min_records_per_partition(),
            );
            println!(
                "{}",
                format!("Partitions: <{}>", number_of_partitions).yellow()
            );
            if number_of_partitions < settings.get_threads() as i64 {
                settings.set_threads(number_of_partitions as u32);
                println!(
                    "{}",
                    format!("Threads: <{}>", settings.get_threads()).yellow()
                );
            }
            let source_db_metadata_result: Result<Vec<(String, PgPumpColumnType)>> =
                source_db_provider
                    .get_table_metadata(&source_schema_name, &source_table_name)
                    .await;
            if source_db_metadata_result.is_err() {
                eprintln!(
                    "{}",
                    source_db_metadata_result.err().unwrap().to_string().red()
                );
                process::exit(1);
            }
            let source_db_metadata = source_db_metadata_result.ok().unwrap();
            println!("{}", "DONE Getting Source DB metadata".green());
            // endregion

            print_separator();

            // region Target DB Setup
            println!("Target DB Setup ...");
            let postgres_consumer = PostgresConsumer::new(&config.get_target_database_as_ref());
            if settings.is_truncate_target_table() {
                let result = postgres_consumer
                    .truncate_table(&target_schema_name, &target_table_name)
                    .await;
                if result.is_err() {
                    eprintln!("{}", result.err().unwrap().to_string().red());
                    process::exit(1);
                }
                if result.is_ok() {
                    println!("{}", "TARGET TABLE TRUNCATED".yellow());
                }
            }
            println!("{}", "DONE Target DB Setup".green());
            // endregion

            print_separator();

            // region Compute Source DB Partitions
            println!("Compute Source DB partitions ...");
            let source_db_partitions_result: Result<Vec<(i64, i64, i64, i64)>> = source_db_provider
                .get_copy_partitions(
                    &source_schema_name,
                    &source_table_name,
                    settings.get_column_name_as_ref(),
                    number_of_partitions,
                    settings.get_min(),
                    settings.get_max(),
                )
                .await;
            if source_db_partitions_result.is_err() {
                eprintln!(
                    "{}",
                    source_db_partitions_result.err().unwrap().to_string().red()
                );
                process::exit(1);
            }
            let source_db_partitions = source_db_partitions_result.ok().unwrap();
            println!("{}", "DONE Compute Source DB partitions".green());
            // endregion

            print_separator();

            // region Source DB Connection Pool
            println!("Creating Source DB Connection Pool ...");
            // TODO: fix this
            // TODO: move out of copy loop
            let sql_server_provider = SqlServerProvider::new(&config.get_source_database_as_ref());
            let sql_server_pool_result = sql_server_provider
                .create_connection_pool(settings.get_threads(), settings.get_timeout())
                .await;
            if sql_server_pool_result.is_err() {
                eprintln!(
                    "{}",
                    sql_server_pool_result.err().unwrap().to_string().red()
                );
                process::exit(1);
            }
            let sql_server_pool = sql_server_pool_result.ok().unwrap();
            println!("{}", "DONE Creating Source DB Connection Pool".green());
            // endregion

            print_separator();

            // region Target DB Connection Pool
            println!("Creating Target DB Connection Pool ...");
            // TODO: move out of copy loop
            let postgres_pool_result = postgres_consumer
                .create_connection_pool(settings.get_threads(), settings.get_timeout())
                .await;
            if postgres_pool_result.is_err() {
                eprintln!("{}", postgres_pool_result.err().unwrap().to_string().red());
                process::exit(1);
            }
            let postgres_pool = postgres_pool_result.ok().unwrap();
            println!("{}", "DONE Creating Target DB Connection Pool".green());
            // endregion

            print_separator();

            // region COPY DATA
            println!("COPY DATA ...");
            copy_data(
                sql_server_pool,
                postgres_pool,
                source_db_metadata,
                &source_schema_name,
                &source_table_name,
                &target_schema_name,
                &target_table_name,
                &source_db_partitions,
                &settings
            )
            .await;
            println!(
                "{}",
                "DONE COPY DATA".green()
            );
            // endregion
        }
    }
    print_separator();

    Ok(())
}

fn get_number_of_partitions(long_count: i64, min_records_per_partition: i64) -> i64 {
    let result = long_count / min_records_per_partition;
    if long_count % min_records_per_partition == 0 {
        return result;
    }
    result + 1
}

async fn copy_data(
    sql_server_pool: Pool<ConnectionManager>,
    postgres_pool: Pool<PostgresConnectionManager<NoTls>>,
    sql_server_metadata: Vec<(String, PgPumpColumnType)>,
    source_schema_name: &String,
    source_table_name: &String,
    target_schema_name: &String,
    target_table_name: &String,
    sql_server_partitions: &Vec<(i64, i64, i64, i64)>,
    settings: &Settings,
) {
    let now = Instant::now();
    let mut handles = Vec::new();
    let sql_server_columns = sql_server_metadata
        .iter()
        .map(|x| format!("[{}]", x.0.clone()))
        .collect::<Vec<String>>()
        .join(", ");
    let postgres_column_type_provider = PostgresColumnTypeProvider::new();
    let postgres_column_types = sql_server_metadata
        .iter()
        .map(|x| postgres_column_type_provider.get_postgres_column_type(&x.1))
        .collect::<Vec<Type>>();
    let postgres_columns = sql_server_metadata
        .iter()
        .map(|x| format!("\"{}\"", x.0.clone()))
        .collect::<Vec<String>>()
        .join(", ");

    let (tx, rx) = flume::unbounded();

    for thread_id in 0..settings.get_threads() {
        let sql_server_pool = sql_server_pool.clone();
        let postgres_column_types = postgres_column_types.clone();
        let postgres_pool = postgres_pool.clone();
        let source_schema_name = source_schema_name.clone();
        let source_table_name = source_table_name.clone();
        let target_schema_name = target_schema_name.clone();
        let target_table_name = target_table_name.clone();
        let sql_server_columns = sql_server_columns.clone();
        let postgres_columns = postgres_columns.clone();
        let column_name = settings.get_column_name_as_ref().clone();
        let wait_period = settings.get_wait_period();
        let wait_nth_partition = settings.get_wait_nth_partition();
        let rx = rx.clone();

        let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            let mut sql_server_client = sql_server_pool.get().await.unwrap();
            let sql_server_stream_query = format!(
                "SELECT {} FROM [{}].[{}] WITH (NOLOCK) WHERE {} BETWEEN @P1 AND @P2;",
                sql_server_columns, source_schema_name, source_table_name, column_name
            );
            let mut partition_count = 0;

            while let Ok(partition) = rx.recv_async().await {
                partition_count += 1;

                let (partition_id, start, end, count) = partition;

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

                let mut data_row_boxed: Vec<Box<dyn ToSql + Send + Sync>> =
                    Vec::with_capacity(postgres_column_types.len());
                while let Some(item) = sql_server_stream.try_next().await.unwrap() {
                    data_row_boxed.clear();
                    match item {
                        QueryItem::Row(row) => {
                            for (index, column) in row.columns().iter().enumerate() {
                                match column.column_type() {
                                    ColumnType::Bitn => {
                                        let t_boolean: Option<bool> =
                                            row.try_get::<bool, _>(index)?;
                                        data_row_boxed.push(Box::new(t_boolean));
                                        continue;
                                    }
                                    ColumnType::Int2 => {
                                        let t_small_int: Option<i16> =
                                            row.try_get::<i16, _>(index)?;
                                        data_row_boxed.push(Box::new(t_small_int));
                                        continue;
                                    }
                                    ColumnType::Int4 => {
                                        let t_int_opt: Option<i32> =
                                            row.try_get::<i32, _>(index)?;
                                        data_row_boxed.push(Box::new(t_int_opt));
                                        continue;
                                    }
                                    ColumnType::Int8 => {
                                        let t_big_int: Option<i64> =
                                            row.try_get::<i64, _>(index)?;
                                        data_row_boxed.push(Box::new(t_big_int));
                                        continue;
                                    }
                                    ColumnType::Daten => {
                                        let t_date: Option<Date> = row.try_get::<Date, _>(index)?;
                                        data_row_boxed.push(Box::new(t_date));
                                        continue;
                                    }
                                    ColumnType::Timen => {
                                        let t_time: Option<Time> = row.try_get::<Time, _>(index)?;
                                        data_row_boxed.push(Box::new(t_time));
                                        continue;
                                    }
                                    ColumnType::Datetime2 => {
                                        let t_date_time_2: Option<PrimitiveDateTime> =
                                            row.try_get::<PrimitiveDateTime, _>(index)?;
                                        data_row_boxed.push(Box::new(t_date_time_2));
                                        continue;
                                    }
                                    ColumnType::Decimaln => {
                                        let t_decimal: Option<Decimal> =
                                            row.try_get::<Decimal, _>(index)?;
                                        data_row_boxed.push(Box::new(t_decimal));
                                        continue;
                                    }
                                    ColumnType::Float8 => {
                                        let t_float: Option<f64> = row.try_get::<f64, _>(index)?;
                                        data_row_boxed.push(Box::new(t_float));
                                        continue;
                                    }
                                    ColumnType::Guid => {
                                        let t_uuid: Option<Uuid> = row.try_get::<Uuid, _>(index)?;
                                        data_row_boxed.push(Box::new(t_uuid));
                                        continue;
                                    }
                                    ColumnType::NChar => {
                                        let t_n_char: Option<&str> =
                                            row.try_get::<&str, _>(index)?;
                                        match t_n_char {
                                            Some(value) => {
                                                data_row_boxed.push(Box::new(value.to_string()))
                                            }
                                            None => data_row_boxed.push(Box::new(None::<&str>)),
                                        }
                                        continue;
                                    }
                                    ColumnType::BigChar => {
                                        let t_big_char: Option<&str> =
                                            row.try_get::<&str, _>(index)?;
                                        match t_big_char {
                                            Some(value) => {
                                                data_row_boxed.push(Box::new(value.to_string()))
                                            }
                                            None => data_row_boxed.push(Box::new(None::<&str>)),
                                        }
                                        continue;
                                    }
                                    ColumnType::NVarchar => {
                                        let t_n_varchar: Option<&str> =
                                            row.try_get::<&str, _>(index)?;
                                        match t_n_varchar {
                                            Some(value) => {
                                                data_row_boxed.push(Box::new(value.to_string()))
                                            }
                                            None => data_row_boxed.push(Box::new(None::<&str>)),
                                        }
                                        continue;
                                    }
                                    ColumnType::BigVarChar => {
                                        let t_big_varchar: Option<&str> =
                                            row.try_get::<&str, _>(index)?;
                                        match t_big_varchar {
                                            Some(value) => {
                                                data_row_boxed.push(Box::new(value.to_string()))
                                            }
                                            None => data_row_boxed.push(Box::new(None::<&str>)),
                                        }
                                        continue;
                                    }
                                    _ => {
                                        panic!("Unknown column type");
                                    }
                                }
                            }

                            let row_to_write: Vec<&(dyn ToSql + Sync)> = data_row_boxed
                                .iter()
                                .map(|s| s.as_ref() as &(dyn ToSql + Sync))
                                .collect();

                            postgres_writer.as_mut().write(&row_to_write[..]).await?
                        }
                        _ => {}
                    }
                }
                postgres_writer.finish().await?;
                println!(
                    "thread_id = {}, partition_id = {}, count = {}",
                    thread_id, partition_id, count
                );
                if wait_period > 0 && partition_count == wait_nth_partition {
                    println!(
                        "thread_id = {}, partition_id = {}, partition_count = {}, sleeping for {} seconds",
                        thread_id, partition_id, partition_count, wait_period
                    );
                    partition_count = 0;
                    sleep(Duration::from_secs(wait_period)).await;
                }
            }

            Ok(())
        });
        handles.push(handle);
    }

    for partition in sql_server_partitions {
        tx.send_async(*partition).await.unwrap();
    }

    drop(tx);

    let thread_results = join_all(handles).await;
    for thread_result in thread_results {
        if thread_result.is_err() {
            eprintln!(
                "Error in thread: {}",
                thread_result.err().unwrap().to_string().red()
            );
        }
    }

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
}
