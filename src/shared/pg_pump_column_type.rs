#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum PgPumpColumnType {
    Boolean,
    Byte,
    Int,
    BigInt,
    Date,
    Time,
    Datetime,
    DatetimeTZ,
    Decimal,
    Float,
    Uuid,
    Char,
    Varchar,
    Unknown,
}
