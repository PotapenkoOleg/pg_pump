use clap::Parser;

#[derive(Clone, Debug)]
pub enum PgPumpColumnType {
    Boolean,
    Byte,
    Int,
    BigInt,
    Datetime,
    Decimal,
    Float,
    Uuid,
    Char,
    Varchar,
    Unknown,
}
