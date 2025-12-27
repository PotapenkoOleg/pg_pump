use crate::config_provider::TargetDatabase;
// ---
use anyhow::Result;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures_util::TryStreamExt;
use futures_util::future::join_all;
use std::pin::pin;
use tokio::task::JoinHandle;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Config, Error, GenericClient, NoTls};

pub struct PostgresConsumer {
    config: Config,
}

impl PostgresConsumer {
    pub fn new(target_database: &TargetDatabase) -> Self {
        let mut config = Config::new();
        config.host(target_database.get_host_as_ref());
        config.port(target_database.get_port_as_ref().clone());
        config.dbname(target_database.get_database_as_ref());
        config.user(target_database.get_user_as_ref());
        config.password(target_database.get_password_as_ref());
        config.keepalives(true);
        PostgresConsumer { config }
    }

    pub async fn create_connection_pool(
        &self,
        threads: u32,
        timeout: u64,
    ) -> Result<Pool<PostgresConnectionManager<NoTls>>> {
        let manager = PostgresConnectionManager::new(self.config.clone(), NoTls);
        let pool = Pool::builder()
            .max_size(threads)
            .connection_timeout(std::time::Duration::from_secs(timeout))
            .build(manager)
            .await?;
        Ok(pool)
    }

    pub async fn postgres_copy_test(&self) -> Result<()> {
        let (client, connection) = tokio_postgres::connect(
            "host=localhost user=postgres password=postgres dbname=developer",
            NoTls,
        )
        .await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let sink = client
            .copy_in("COPY \"Sample\".\"TestData1\" (\"ID\", \"FileNumber\", \"Code\") FROM STDIN BINARY")
            .await?;

        let mut writer = pin!(BinaryCopyInWriter::new(
            sink,
            &[Type::INT8, Type::INT4, Type::VARCHAR]
        ));

        for i in 0..1_000_000i64 {
            writer
                .as_mut()
                .write(&[&i, &0i32, &format!("the value for {i}")])
                .await?;
        }

        writer.finish().await?;
        println!("Successfully inserted 1,000,000 rows via binary copy.");

        Ok(())
    }

    pub async fn postgres_pool_test(&self) -> Result<()> {
        let manager = PostgresConnectionManager::new(self.config.clone(), NoTls);
        let pool = Pool::builder()
            .max_size(10)
            .connection_timeout(std::time::Duration::from_secs(10))
            .build(manager)
            .await?;

        let mut handles = Vec::new();
        for task_id in 1..=10 {
            let pool = pool.clone();
            let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
                let connection = pool.get().await?;
                let client = connection.client();

                let sink = client
                    .copy_in("COPY \"Sample\".\"TestData1\" (\"ID\", \"FileNumber\", \"Code\") FROM STDIN BINARY")
                    .await?;

                let mut writer = pin!(BinaryCopyInWriter::new(
                    sink,
                    &[Type::INT8, Type::INT4, Type::VARCHAR]
                ));

                for i in 0..100_000i64 {
                    writer
                        .as_mut()
                        .write(&[&i, &task_id, &format!("the value for {i}")])
                        .await?;
                }

                writer.finish().await?;
                Ok(())
            });
            handles.push(handle);
        }

        join_all(handles).await;

        Ok(())
    }
}
