use crate::config_provider::SourceDatabase;
use crate::shared::db_provider::DbProvider;
use crate::shared::pg_pump_column_type::PgPumpColumnType;
use crate::version::PRODUCT_NAME;
use anyhow::Result;
use bb8::{ManageConnection, Pool};
use bb8_tiberius::ConnectionManager;
use futures_util::TryStreamExt;
use tiberius::{AuthMethod, Client, ColumnType, Config, EncryptionLevel, QueryItem};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncReadCompatExt;

pub struct SqlServerProvider {
    config: Config,
}

impl SqlServerProvider {
    pub fn new(source_database: &SourceDatabase) -> Self {
        let mut config = Config::new();
        config.host(source_database.get_host_as_ref());
        config.port(source_database.get_port_as_ref().clone());
        config.database(source_database.get_database_as_ref());
        config.authentication(AuthMethod::sql_server(
            source_database.get_user_as_ref(),
            source_database.get_password_as_ref(),
        ));
        config.trust_cert();
        config.readonly(true);
        config.application_name(PRODUCT_NAME);
        config.encryption(EncryptionLevel::NotSupported); // TODO: remove on PROD
        SqlServerProvider { config }
    }
    pub async fn create_connection_pool(
        &self,
        threads: u32,
        timeout: u64,
    ) -> Result<Pool<ConnectionManager>> {
        let manager = ConnectionManager::new(self.config.clone());
        let pool = Pool::builder()
            .max_size(threads)
            .connection_timeout(std::time::Duration::from_secs(timeout))
            .build(manager)
            .await?;

        Ok(pool)
    }
}

impl DbProvider for SqlServerProvider {
    async fn get_table_metadata(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Vec<(String, PgPumpColumnType)>> {
        let tcp = TcpStream::connect(&self.config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        let mut client = Client::connect(self.config.clone(), tcp.compat()).await?;
        let get_metadata_query = format!(
            "SELECT TOP 1 * FROM [{}].[{}] WITH (NOLOCK);",
            schema_name, table_name
        );
        let first_row = client
            .query(&get_metadata_query, &[])
            .await?
            .into_row()
            .await?;
        let mut result = Vec::new();
        if let Some(row) = first_row {
            for column in row.columns() {
                let column_type = match column.column_type() {
                    ColumnType::Bitn => PgPumpColumnType::Boolean,
                    ColumnType::Int2 => PgPumpColumnType::Byte,
                    ColumnType::Int4 => PgPumpColumnType::Int,
                    ColumnType::Int8 => PgPumpColumnType::BigInt,
                    ColumnType::Daten => PgPumpColumnType::Date,
                    ColumnType::Timen => PgPumpColumnType::Time,
                    ColumnType::Datetime2 => PgPumpColumnType::Datetime,
                    ColumnType::Decimaln => PgPumpColumnType::Decimal,
                    ColumnType::Float8 => PgPumpColumnType::Float,
                    ColumnType::Guid => PgPumpColumnType::Uuid,
                    ColumnType::NChar => PgPumpColumnType::Char,
                    ColumnType::BigChar => PgPumpColumnType::Char,
                    ColumnType::NVarchar => PgPumpColumnType::Varchar,
                    ColumnType::BigVarChar => PgPumpColumnType::Varchar,
                    _ => PgPumpColumnType::Unknown,
                };
                result.push((column.name().to_string(), column_type));
            }
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
    ) -> Result<i64> {
        let tcp = TcpStream::connect(&self.config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        let mut client = Client::connect(self.config.clone(), tcp.compat()).await?;
        let mut get_count_query = format!(
            "SELECT COUNT_BIG(*) FROM [{}].[{}] WITH (NOLOCK)",
            schema_name, table_name
        );
        if max != 0 {
            get_count_query
                .push_str(format!(" WHERE {} BETWEEN {} AND {}", column_name, min, max).as_str());
        }
        get_count_query.push(';');
        let row = client
            .query(&get_count_query, &[])
            .await?
            .into_row()
            .await?;
        let mut count: i64 = 0;
        if let Some(row) = row {
            count = row.get(0).unwrap_or(0);
        }

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
    ) -> Result<Vec<(i64, i64, i64, i32)>> {
        let mut get_count_inner_query = format!(
            "SELECT {}, NTILE({}) OVER (ORDER BY {}) AS PartitionId FROM [{}].[{}] WITH (NOLOCK)",
            column_name, number_of_partitions, column_name, schema_name, table_name
        );
        if max != 0 {
            get_count_inner_query
                .push_str(format!(" WHERE {} BETWEEN {} AND {}", column_name, min, max).as_str());
        }
        let get_count_query = format!(
            "WITH CTE AS ({}) \
            SELECT PartitionId, MIN({}) AS [Min], MAX({}) AS [Max], COUNT({}) AS [Count] \
            FROM CTE GROUP BY PartitionId ORDER BY PartitionId;",
            get_count_inner_query, column_name, column_name, column_name
        );

        let tcp = TcpStream::connect(&self.config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        let mut client = Client::connect(self.config.clone(), tcp.compat()).await?;

        let mut result = Vec::new();
        let mut stream = client.query(&get_count_query, &[]).await?;

        while let Some(item) = stream.try_next().await? {
            match item {
                QueryItem::Row(row) => {
                    let partition_id: i64 = row.get(0).unwrap_or(0);
                    let min_value: i64 = row.get(1).unwrap_or(0);
                    let max_value: i64 = row.get(2).unwrap_or(0);
                    let count: i32 = row.get(3).unwrap_or(0);
                    result.push((partition_id, min_value, max_value, count));
                }
                _ => {}
            }
        }

        Ok(result)
    }
}
