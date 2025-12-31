#[derive(Clone, Debug, Hash, Eq, PartialEq)]
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
