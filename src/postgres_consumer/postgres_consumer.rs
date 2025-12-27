use crate::config_provider::TargetDatabase;
// ---
use anyhow::Result;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures_util::TryStreamExt;
use std::pin::pin;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Config, Error, GenericClient, NoTls};

pub struct PostgresConsumer {
    config: Config,
}

impl PostgresConsumer {
    pub fn new(target_database: &TargetDatabase) -> Self {
        let mut config = Config::new();
        config.host("localhost");
        config.port(5432);
        config.dbname("developer");
        config.user("postgres");
        config.password("postgres");
        config.keepalives(true);
        PostgresConsumer { config }
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

        let connection = pool.get().await?;
        let client = connection.client();

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

        // let rows = conn.query("SELECT 1 + 1", &[]).await?;
        // let value: i32 = rows[0].get(0);
        // println!("1 + 1 = {}", value);

        Ok(())
    }
}
