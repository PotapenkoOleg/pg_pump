use crate::shared::pg_pump_column_type::PgPumpColumnType;

pub trait DbProvider {
    async fn get_table_metadata(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> anyhow::Result<Vec<(String, PgPumpColumnType)>>;

    async fn get_long_count(
        &self,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
        min: u64,
        max: u64,
    ) -> anyhow::Result<i64>;

    async fn get_copy_partitions(
        &self,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
        number_of_partitions: i64,
        min: u64,
        max: u64,
    ) -> anyhow::Result<Vec<(i64, i64, i64, i64)>>;
}
