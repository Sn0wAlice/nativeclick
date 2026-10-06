//! Values of the types added for ClickHouse 25+/26: `Date32`, `Time`, `Time64`, and the rows of
//! `Variant` / `Dynamic` columns.

use chrono::{NaiveDate, TimeDelta};

use crate::{
    NativeclickError, Result, Value,
    convert::{FromSql, ToSql, unexpected_type},
    types::Type,
};

/// A row of a `Variant`, `Dynamic` or `Geometry` column: the value and its actual type.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DynamicValue {
    pub type_: Type,
    pub value: Value,
}

impl DynamicValue {
    pub fn new(type_: Type, value: Value) -> Self {
        Self { type_, value }
    }
}

impl ToSql for DynamicValue {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Dynamic(Box::new(self)))
    }
}

/// A `Variant`/`Dynamic`/`Geometry` row with its actual type (`LineString` stays `LineString`);
/// any other column gives its own type. NULL rows need an `Option<DynamicValue>`.
impl FromSql for DynamicValue {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        Self::from_sql_column(type_, value)
    }

    fn from_sql_column(type_: &Type, value: Value) -> Result<Self> {
        match value {
            Value::Dynamic(x) => Ok(*x),
            Value::Null => Err(NativeclickError::DeserializeError(format!(
                "NULL {type_} value: use Option<DynamicValue>"
            ))),
            value => Ok(DynamicValue::new(type_.clone(), value)),
        }
    }
}

/// Wrapper type for Clickhouse `Date32` type: days since 1970-01-01, signed.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Date32(pub i32);

fn epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
}

impl TryFrom<Date32> for NaiveDate {
    type Error = NativeclickError;

    fn try_from(date: Date32) -> Result<Self> {
        epoch()
            .checked_add_signed(TimeDelta::days(date.0 as i64))
            .ok_or_else(|| {
                NativeclickError::DeserializeError(format!("Date32 out of range: {}", date.0))
            })
    }
}

impl From<NaiveDate> for Date32 {
    fn from(date: NaiveDate) -> Self {
        Self(date.signed_duration_since(epoch()).num_days() as i32)
    }
}

impl ToSql for Date32 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Date32(self))
    }
}

impl FromSql for Date32 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        match value {
            Value::Date32(x) => Ok(x),
            Value::Date(x) => Ok(Date32(x.0 as i32)),
            _ => Err(unexpected_type(type_)),
        }
    }
}

/// Written as `Date32` into `Date32` columns, as `Date` otherwise.
impl ToSql for NaiveDate {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        match type_hint.map(Type::strip_null) {
            Some(Type::Date32) => Ok(Value::Date32(self.into())),
            _ => {
                let days = self.signed_duration_since(epoch()).num_days();
                let days = u16::try_from(days).map_err(|_| {
                    NativeclickError::SerializeError(format!(
                        "{self} is out of the range of Date (1970-01-01 to 2149-06-06), use a Date32 column"
                    ))
                })?;
                Ok(Value::Date(crate::Date(days)))
            }
        }
    }
}

impl FromSql for NaiveDate {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        match value {
            Value::Date(x) => Ok(x.into()),
            Value::Date32(x) => x.try_into(),
            _ => Err(unexpected_type(type_)),
        }
    }
}

/// `Time` / `Time64` as a signed duration.
impl ToSql for TimeDelta {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        let overflow =
            || NativeclickError::SerializeError(format!("{self} is out of range for Time"));
        match type_hint.map(Type::strip_null) {
            Some(Type::Time64(precision)) => {
                let precision = *precision;
                if precision > 9 {
                    return Err(overflow());
                }
                let nanos = self.num_nanoseconds().ok_or_else(overflow)?;
                Ok(Value::Time64(
                    precision,
                    nanos / 10i64.pow(9 - precision as u32),
                ))
            }
            _ => Ok(Value::Time(
                i32::try_from(self.num_seconds()).map_err(|_| overflow())?,
            )),
        }
    }
}

impl FromSql for TimeDelta {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        match value {
            Value::Time(x) => Ok(TimeDelta::seconds(x as i64)),
            Value::Time64(precision, x) if precision <= 9 => {
                Ok(TimeDelta::nanoseconds(x) * 10i32.pow(9 - precision as u32))
            }
            _ => Err(unexpected_type(type_)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date32_round_trips_before_1970_and_after_2149() {
        for (y, m, d) in [(1900, 1, 1), (1969, 12, 31), (1970, 1, 1), (2299, 12, 31)] {
            let date = NaiveDate::from_ymd_opt(y, m, d).unwrap();
            let value = date.to_sql(Some(&Type::Date32)).unwrap();
            assert_eq!(NaiveDate::from_sql(&Type::Date32, value).unwrap(), date);
        }
        assert_eq!(
            Date32::from(NaiveDate::from_ymd_opt(1969, 12, 31).unwrap()),
            Date32(-1)
        );
        // Date cannot hold it: explicit error instead of a wrapped value.
        assert!(
            NaiveDate::from_ymd_opt(1960, 1, 1)
                .unwrap()
                .to_sql(Some(&Type::Date))
                .is_err()
        );
    }

    #[test]
    fn time_and_time64_round_trip() {
        let delta = TimeDelta::seconds(-3600 * 30 - 5) + TimeDelta::milliseconds(-250);
        let value = delta.to_sql(Some(&Type::Time64(3))).unwrap();
        assert_eq!(value, Value::Time64(3, -108_005_250));
        assert_eq!(TimeDelta::from_sql(&Type::Time64(3), value).unwrap(), delta);

        let value = TimeDelta::seconds(90_000)
            .to_sql(Some(&Type::Time))
            .unwrap();
        assert_eq!(value, Value::Time(90_000));
        assert_eq!(
            TimeDelta::from_sql(&Type::Time, value).unwrap(),
            TimeDelta::seconds(90_000)
        );
    }
}
