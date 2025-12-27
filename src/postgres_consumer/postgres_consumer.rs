use crate::config_provider::TargetDatabase;
// ---
use anyhow::Result;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures_util::TryStreamExt;
use std::pin::pin;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Error, NoTls};

pub struct PostgresConsumer {}

impl PostgresConsumer {
    pub fn new(target_database: &TargetDatabase) -> Self {
        PostgresConsumer {}
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
        // dotenvy::dotenv()?;
        // let database_url = std::env::var("DATABASE_URL")?;

        let database_url = "host=localhost user=postgres password=postgres dbname=developer";

        let manager = PostgresConnectionManager::new_from_stringlike(database_url, NoTls)?;

        let pool = Pool::builder().max_size(10).build(manager).await?;

        let conn = pool.get().await?;

        let rows = conn.query("SELECT 1 + 1", &[]).await?;
        let value: i32 = rows[0].get(0);
        println!("1 + 1 = {}", value);

        Ok(())
    }
}
