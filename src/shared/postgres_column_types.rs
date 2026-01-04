use std::collections::HashMap;
use tokio_postgres::types::Type;
use crate::shared::pg_pump_column_type::PgPumpColumnType;

pub fn get_postgres_column_type(pg_pump_column_type: &PgPumpColumnType) -> Type {
    let types_hash_map = HashMap::from([
        (PgPumpColumnType::Boolean, Type::BOOL),
        (PgPumpColumnType::Byte, Type::INT2),
        (PgPumpColumnType::Int, Type::INT4),
        (PgPumpColumnType::BigInt, Type::INT8),
        (PgPumpColumnType::Date, Type::DATE),
        (PgPumpColumnType::Time, Type::TIME),
        (PgPumpColumnType::Datetime, Type::TIMESTAMP),
        (PgPumpColumnType::DatetimeTZ, Type::TIMESTAMPTZ),
        (PgPumpColumnType::Decimal, Type::NUMERIC),
        (PgPumpColumnType::Float, Type::FLOAT8),
        (PgPumpColumnType::Uuid, Type::UUID),
        (PgPumpColumnType::Char, Type::BPCHAR),
        (PgPumpColumnType::Varchar, Type::VARCHAR),
        (PgPumpColumnType::Unknown, Type::UNKNOWN),
    ]);
    types_hash_map.get(pg_pump_column_type).unwrap_or(&Type::UNKNOWN).clone()
}

