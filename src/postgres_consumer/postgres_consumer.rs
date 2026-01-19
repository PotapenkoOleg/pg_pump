use crate::config_provider::TargetDatabase;
use anyhow::Result;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::{Config, NoTls};

pub struct PostgresConsumer {
    config: Config,
    connection_string: String,
}

impl PostgresConsumer {
    pub fn new(target_database: &TargetDatabase) -> Self {
        let host = target_database.get_host_as_ref();
        let port = target_database.get_port_as_ref().clone();
        let dbname = target_database.get_database_as_ref();
        let user = target_database.get_user_as_ref();
        let password = target_database.get_password_as_ref();
        let mut config = Config::new();
        config.host(host);
        config.port(port);
        config.dbname(dbname);
        config.user(user);
        config.password(password);
        config.keepalives(true);
        let connection_string = format!(
            "host={} port={} dbname={} user={} password={}",
            host, port, dbname, user, password
        );
        PostgresConsumer {
            config,
            connection_string,
        }
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

    // region Postgres Metadata

    // pub async fn get_table_metadata(
    //     &self,
    //     schema_name: &str,
    //     table_name: &str,
    // ) -> Result<Vec<(String, PgPumpColumnType)>> {
    //     let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;
    //
    //     tokio::spawn(async move {
    //         if let Err(e) = connection.await {
    //             eprintln!("connection error: {}", e);
    //         }
    //     });
    //
    //     let query = format!(
    //         "SELECT * FROM \"{}\".\"{}\" LIMIT 1;",
    //         schema_name, table_name
    //     );
    //
    //     let rows = client.query(&query, &[]).await?;
    //
    //     let mut result = Vec::new();
    //     for row in rows {
    //         let column_name: String = row.get(0);
    //         let data_type: String = row.get(1);
    //
    //         //row.columns()
    //
    //         // let pg_type = match data_type.as_str() {
    //         //     "bigint" => PgPumpColumnType::BigInt,
    //         //     "integer" => PgPumpColumnType::Integer,
    //         //     "smallint" => PgPumpColumnType::SmallInt,
    //         //     "character varying" | "varchar" | "text" => PgPumpColumnType::Varchar,
    //         //     "boolean" => PgPumpColumnType::Boolean,
    //         //     "timestamp without time zone" => PgPumpColumnType::Timestamp,
    //         //     "timestamp with time zone" => PgPumpColumnType::TimestampTz,
    //         //     "date" => PgPumpColumnType::Date,
    //         //     "numeric" | "decimal" => PgPumpColumnType::Numeric,
    //         //     "real" => PgPumpColumnType::Real,
    //         //     "double precision" => PgPumpColumnType::DoublePrecision,
    //         //     _ => PgPumpColumnType::Varchar, // Default fallback
    //         // };
    //
    //         result.push((column_name, PgPumpColumnType::Unknown));
    //     }
    //
    //     Ok(result)
    // }

    // endregion

    // pub async fn get_long_count(&self, schema_name: &str, table_name: &str) -> Result<i64> {
    //     let query = format!(
    //         "SELECT COUNT(*) FROM \"{}\".\"{}\";",
    //         schema_name, table_name
    //     );
    //     let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;
    //     tokio::spawn(async move {
    //         if let Err(e) = connection.await {
    //             eprintln!("connection error: {}", e);
    //         }
    //     });
    //     let row = client.query_one(&query, &[]).await?;
    //     let count: i64 = row.get(0);
    //     Ok(count)
    // }

    pub async fn truncate_table(&self, schema_name: &str, target_table_name: &str) -> Result<()> {
        let query = format!(
            "TRUNCATE TABLE \"{}\".\"{}\";",
            schema_name, target_table_name
        );
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        client.batch_execute(&query).await?;
        Ok(())
    }
}
