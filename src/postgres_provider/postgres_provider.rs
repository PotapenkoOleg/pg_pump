use crate::config_provider::SourceDatabase;
use crate::shared::db_provider::DbProvider;
use crate::shared::pg_pump_column_type::PgPumpColumnType;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::{Config, NoTls};

pub struct PostgresProvider {
    config: Config,
    connection_string: String,
}

impl PostgresProvider {
    pub fn new(source_database: &SourceDatabase) -> Self {
        let host = source_database.get_host_as_ref();
        let port = source_database.get_port_as_ref().clone();
        let dbname = source_database.get_database_as_ref();
        let user = source_database.get_user_as_ref();
        let password = source_database.get_password_as_ref();
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
        PostgresProvider {
            config,
            connection_string,
        }
    }

    pub async fn create_connection_pool(
        &self,
        threads: u32,
        timeout: u64,
    ) -> anyhow::Result<Pool<PostgresConnectionManager<NoTls>>> {
        let manager = PostgresConnectionManager::new(self.config.clone(), NoTls);
        let pool = Pool::builder()
            .max_size(threads)
            .connection_timeout(std::time::Duration::from_secs(timeout))
            .build(manager)
            .await?;
        Ok(pool)
    }
}
impl DbProvider for PostgresProvider {
    async fn get_table_metadata(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> anyhow::Result<Vec<(String, PgPumpColumnType)>> {
        // TODO: rewrite using INFORMATION_SCHEMA

        let query = format!(
            "SELECT * FROM \"{}\".\"{}\" LIMIT 1;",
            schema_name, table_name
        );

        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let rows = client.query(&query, &[]).await?;

        let mut result = Vec::new();
        for row in rows {
            let column_name: String = row.get(0);
            let data_type: String = row.get(1);

            //row.columns()

            // let pg_type = match data_type.as_str() {
            //     "bigint" => PgPumpColumnType::BigInt,
            //     "integer" => PgPumpColumnType::Integer,
            //     "smallint" => PgPumpColumnType::SmallInt,
            //     "character varying" | "varchar" | "text" => PgPumpColumnType::Varchar,
            //     "boolean" => PgPumpColumnType::Boolean,
            //     "timestamp without time zone" => PgPumpColumnType::Timestamp,
            //     "timestamp with time zone" => PgPumpColumnType::TimestampTz,
            //     "date" => PgPumpColumnType::Date,
            //     "numeric" | "decimal" => PgPumpColumnType::Numeric,
            //     "real" => PgPumpColumnType::Real,
            //     "double precision" => PgPumpColumnType::DoublePrecision,
            //     _ => PgPumpColumnType::Varchar, // Default fallback
            // };

            result.push((column_name, PgPumpColumnType::Unknown));
        }

        Ok(result)
    }

    async fn get_long_count(
        &self,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
        min: u64,
        max: u64,
    ) -> anyhow::Result<i64> {
        let mut get_count_query = format!(
            "SELECT COUNT(*) FROM \"{}\".\"{}\"",
            schema_name, table_name
        );
        if max != 0 {
            get_count_query
                .push_str(format!(" WHERE {} BETWEEN {} AND {}", column_name, min, max).as_str());
        }
        get_count_query.push(';');
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let row = client.query_one(&get_count_query, &[]).await?;
        let count: i64 = row.get(0);
        Ok(count)
    }

    async fn get_copy_partitions(
        &self,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
        number_of_partitions: i64,
        min: u64,
        max: u64,
    ) -> anyhow::Result<Vec<(i64, i64, i64, i32)>> {
        let mut get_partitions_inner_query = format!(
            "SELECT {}, NTILE({}) OVER (ORDER BY {}) AS PartitionId FROM [{}].[{}] WITH (NOLOCK)",
            column_name, number_of_partitions, column_name, schema_name, table_name
        );
        if max != 0 {
            get_partitions_inner_query
                .push_str(format!(" WHERE {} BETWEEN {} AND {}", column_name, min, max).as_str());
        }
        let get_partitions_query = format!(
            "WITH CTE AS ({}) \
            SELECT PartitionId, MIN({}) AS [Min], MAX({}) AS [Max], COUNT({}) AS [Count] \
            FROM CTE GROUP BY PartitionId ORDER BY PartitionId;",
            get_partitions_inner_query, column_name, column_name, column_name
        );
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let rows = client.query(&get_partitions_query, &[]).await?;

        let mut result = Vec::new();
        for row in rows {
            let partition_id: i64 = row.get(0);
            let min_value: i64 = row.get(1);
            let max_value: i64 = row.get(2);
            let count: i32 = row.get(3);
            result.push((partition_id, min_value, max_value, count));
        }
        Ok(result)
    }
}
