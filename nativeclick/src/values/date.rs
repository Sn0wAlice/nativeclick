use std::num::TryFromIntError;

use chrono::{Duration, FixedOffset, NaiveDate, TimeZone, Utc};
use chrono_tz::{Tz, UTC};

use crate::{
    NativeclickError, Result, Value,
    convert::{FromSql, ToSql, unexpected_type},
    types::Type,
};

/// Wrapper type for Clickhouse `Date` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct Date(pub u16);

#[cfg(feature = "serde")]
impl serde::Serialize for Date {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: NaiveDate = (*self).into();
        date.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Date {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let date: NaiveDate = NaiveDate::deserialize(deserializer)?;
        Ok(date.into())
    }
}

impl ToSql for Date {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Date(self))
    }
}

impl FromSql for Date {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Date) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Date(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

#[allow(deprecated)]
impl From<Date> for chrono::Date<Utc> {
    fn from(date: Date) -> Self {
        Utc.from_utc_date(&NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            + Duration::days(date.0 as i64)
    }
}

#[allow(deprecated)]
impl From<chrono::Date<Utc>> for Date {
    fn from(other: chrono::Date<Utc>) -> Self {
        Self(
            other
                .signed_duration_since(
                    Utc.from_utc_date(&NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
                )
                .num_days() as u16,
        )
    }
}

impl From<Date> for chrono::NaiveDate {
    fn from(date: Date) -> Self {
        NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(date.0 as i64)
    }
}

impl From<chrono::NaiveDate> for Date {
    fn from(other: chrono::NaiveDate) -> Self {
        Self(
            other
                .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                .num_days() as u16,
        )
    }
}

/// Wrapper type for Clickhouse `DateTime` type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DateTime(pub Tz, pub u32);

#[cfg(feature = "serde")]
impl serde::Serialize for DateTime {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: chrono::DateTime<Tz> = (*self)
            .try_into()
            .map_err(|e: TryFromIntError| serde::ser::Error::custom(e.to_string()))?;
        date.to_rfc3339().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for DateTime {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw: String = String::deserialize(deserializer)?;
        let date: chrono::DateTime<FixedOffset> =
            chrono::DateTime::<FixedOffset>::parse_from_rfc3339(&raw)
                .map_err(|e: chrono::ParseError| serde::de::Error::custom(e.to_string()))?;

        date.try_into()
            .map_err(|e: TryFromIntError| serde::de::Error::custom(e.to_string()))
    }
}

impl ToSql for DateTime {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::DateTime(self))
    }
}

impl FromSql for DateTime {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime(_)) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl Default for DateTime {
    fn default() -> Self {
        Self(UTC, 0)
    }
}

impl TryFrom<DateTime> for chrono::DateTime<Tz> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime) -> Result<Self, TryFromIntError> {
        Ok(date.0.timestamp_opt(date.1.into(), 0).unwrap())
    }
}

impl TryFrom<DateTime> for chrono::DateTime<FixedOffset> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime) -> Result<Self, TryFromIntError> {
        Ok(date
            .0
            .timestamp_opt(date.1.into(), 0)
            .unwrap()
            .fixed_offset())
    }
}

impl TryFrom<DateTime> for chrono::DateTime<Utc> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime) -> Result<Self, TryFromIntError> {
        Ok(date
            .0
            .timestamp_opt(date.1.into(), 0)
            .unwrap()
            .with_timezone(&Utc))
    }
}

impl TryFrom<chrono::DateTime<Tz>> for DateTime {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<Tz>) -> Result<Self, TryFromIntError> {
        Ok(Self(other.timezone(), other.timestamp().try_into()?))
    }
}

impl TryFrom<chrono::DateTime<FixedOffset>> for DateTime {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<FixedOffset>) -> Result<Self, TryFromIntError> {
        chrono_tz::Tz::UTC
            .from_utc_datetime(&other.naive_utc())
            .try_into()
    }
}

impl TryFrom<chrono::DateTime<Utc>> for DateTime {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<chrono::Utc>) -> Result<Self, TryFromIntError> {
        Ok(Self(chrono_tz::UTC, other.timestamp().try_into()?))
    }
}

/// Wrapper type for Clickhouse `DateTime64` type: ticks of `10^-PRECISION` seconds since the
/// Unix epoch.
///
/// The field holds the raw bits of ClickHouse's signed Int64: dates before 1970 are negative
/// ticks stored as `u64` (two's complement). Use [`DateTime64::from_ticks`] and
/// [`DateTime64::ticks`] to work with the signed value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DateTime64<const PRECISION: usize>(pub Tz, pub u64);

impl<const PRECISION: usize> DateTime64<PRECISION> {
    /// Builds a value from signed ticks (negative before 1970).
    pub fn from_ticks(tz: Tz, ticks: i64) -> Self {
        Self(tz, ticks as u64)
    }

    /// Signed ticks since the Unix epoch (negative before 1970).
    pub fn ticks(&self) -> i64 {
        self.1 as i64
    }
}

/// Wrapper type for Clickhouse `DateTime64` type with dynamic precision.
/// Same raw-bits representation as [`DateTime64`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DynDateTime64(pub Tz, pub u64, pub usize);

impl DynDateTime64 {
    /// Builds a value from signed ticks (negative before 1970).
    pub fn from_ticks(tz: Tz, ticks: i64, precision: usize) -> Self {
        Self(tz, ticks as u64, precision)
    }

    /// Signed ticks since the Unix epoch (negative before 1970).
    pub fn ticks(&self) -> i64 {
        self.1 as i64
    }
}

/// The only error the `TryFrom` conversions below can report: a precision above 9 or a value
/// outside of what chrono or an `i64` of ticks can represent.
fn out_of_range() -> TryFromIntError {
    u8::try_from(u16::MAX).unwrap_err()
}

/// `DateTime64` ticks at `precision` to a chrono date in `tz`.
fn ticks_to_chrono(
    tz: Tz,
    ticks: i64,
    precision: usize,
) -> Result<chrono::DateTime<Tz>, TryFromIntError> {
    if precision > 9 {
        return Err(out_of_range());
    }
    let scale = 10i64.pow(precision as u32);
    // Euclidean division keeps the sub-second part positive for dates before 1970.
    let nanos = ticks.rem_euclid(scale) * 10i64.pow(9 - precision as u32);
    tz.timestamp_opt(ticks.div_euclid(scale), nanos as u32)
        .single()
        .ok_or_else(out_of_range)
}

/// A chrono date to `DateTime64` ticks at `precision`, truncating extra sub-second digits.
fn chrono_to_ticks<T: TimeZone>(
    date: &chrono::DateTime<T>,
    precision: usize,
) -> Result<i64, TryFromIntError> {
    if precision > 9 {
        return Err(out_of_range());
    }
    let sub = date.timestamp_subsec_nanos() as i64 / 10i64.pow(9 - precision as u32);
    date.timestamp()
        .checked_mul(10i64.pow(precision as u32))
        .and_then(|x| x.checked_add(sub))
        .ok_or_else(out_of_range)
}

impl<const PRECISION: usize> From<DateTime64<PRECISION>> for DynDateTime64 {
    fn from(value: DateTime64<PRECISION>) -> Self {
        Self(value.0, value.1, PRECISION)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for DynDateTime64 {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: chrono::DateTime<Tz> = (*self)
            .try_into()
            .map_err(|e: TryFromIntError| serde::ser::Error::custom(e.to_string()))?;
        date.to_rfc3339().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for DynDateTime64 {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw: String = String::deserialize(deserializer)?;
        let date: chrono::DateTime<Utc> = Utc.from_utc_datetime(
            &chrono::DateTime::<FixedOffset>::parse_from_rfc3339(&raw)
                .map_err(|e: chrono::ParseError| serde::de::Error::custom(e.to_string()))?
                .naive_utc(),
        );

        DynDateTime64::try_from_utc(date, 6)
            .map_err(|e: TryFromIntError| serde::de::Error::custom(e.to_string()))
    }
}

#[cfg(feature = "serde")]
impl<const PRECISION: usize> serde::Serialize for DateTime64<PRECISION> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: chrono::DateTime<Tz> = (*self)
            .try_into()
            .map_err(|e: TryFromIntError| serde::ser::Error::custom(e.to_string()))?;
        date.to_rfc3339().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, const PRECISION: usize> serde::Deserialize<'de> for DateTime64<PRECISION> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw: String = String::deserialize(deserializer)?;
        let date: chrono::DateTime<Utc> = Utc.from_utc_datetime(
            &chrono::DateTime::<FixedOffset>::parse_from_rfc3339(&raw)
                .map_err(|e: chrono::ParseError| serde::de::Error::custom(e.to_string()))?
                .naive_utc(),
        );

        date.try_into()
            .map_err(|e: TryFromIntError| serde::de::Error::custom(e.to_string()))
    }
}

impl<const PRECISION: usize> ToSql for DateTime64<PRECISION> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::DateTime64(self.into()))
    }
}

impl<const PRECISION: usize> FromSql for DateTime64<PRECISION> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime64(x, _) if *x == PRECISION) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime64(datetime) => Ok(Self(datetime.0, datetime.1)),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: usize> Default for DateTime64<PRECISION> {
    fn default() -> Self {
        Self(UTC, 0)
    }
}

impl ToSql for chrono::DateTime<Utc> {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        self.with_timezone(&chrono_tz::UTC).to_sql(type_hint)
    }
}

impl FromSql for chrono::DateTime<Utc> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        chrono::DateTime::<Tz>::from_sql(type_, value).map(|x| x.with_timezone(&Utc))
    }
}

impl<const PRECISION: usize> TryFrom<DateTime64<PRECISION>> for chrono::DateTime<Utc> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime64<PRECISION>) -> Result<Self, TryFromIntError> {
        Ok(ticks_to_chrono(date.0, date.ticks(), PRECISION)?.with_timezone(&Utc))
    }
}

impl TryFrom<DynDateTime64> for chrono::DateTime<Utc> {
    type Error = TryFromIntError;

    fn try_from(date: DynDateTime64) -> Result<Self, TryFromIntError> {
        Ok(ticks_to_chrono(date.0, date.ticks(), date.2)?.with_timezone(&Utc))
    }
}

impl ToSql for chrono::DateTime<Tz> {
    /// Sent as `DateTime64` at the precision of the column when known, microseconds otherwise.
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        let precision = match type_hint.map(Type::strip_null) {
            Some(Type::DateTime64(precision, _)) => *precision,
            _ => 6,
        };
        let ticks = chrono_to_ticks(&self, precision).map_err(|e| {
            NativeclickError::DeserializeError(format!("failed to convert DateTime64: {e:?}"))
        })?;
        Ok(Value::DateTime64(DynDateTime64::from_ticks(
            self.timezone(),
            ticks,
            precision,
        )))
    }
}

impl FromSql for chrono::DateTime<Tz> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime64(_, _) | Type::DateTime(_)) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime64(datetime) => datetime.try_into(),
            Value::DateTime(date) => date.try_into(),
            _ => return Err(unexpected_type(type_)),
        }
        .map_err(|e| {
            NativeclickError::DeserializeError(format!("failed to convert DateTime: {e:?}"))
        })
    }
}

impl<const PRECISION: usize> TryFrom<chrono::DateTime<Utc>> for DateTime64<PRECISION> {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<Utc>) -> Result<Self, TryFromIntError> {
        Ok(Self::from_ticks(
            chrono_tz::UTC,
            chrono_to_ticks(&other, PRECISION)?,
        ))
    }
}

impl DynDateTime64 {
    pub fn try_from_utc(
        other: chrono::DateTime<Utc>,
        precision: usize,
    ) -> Result<Self, TryFromIntError> {
        Ok(Self::from_ticks(
            chrono_tz::UTC,
            chrono_to_ticks(&other, precision)?,
            precision,
        ))
    }
}

impl<const PRECISION: usize> TryFrom<DateTime64<PRECISION>> for chrono::DateTime<Tz> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime64<PRECISION>) -> Result<Self, TryFromIntError> {
        ticks_to_chrono(date.0, date.ticks(), PRECISION)
    }
}

impl TryFrom<DynDateTime64> for chrono::DateTime<Tz> {
    type Error = TryFromIntError;

    fn try_from(date: DynDateTime64) -> Result<Self, TryFromIntError> {
        ticks_to_chrono(date.0, date.ticks(), date.2)
    }
}

impl<const PRECISION: usize> TryFrom<chrono::DateTime<Tz>> for DateTime64<PRECISION> {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<Tz>) -> Result<Self, TryFromIntError> {
        Ok(Self::from_ticks(
            other.timezone(),
            chrono_to_ticks(&other, PRECISION)?,
        ))
    }
}

impl DynDateTime64 {
    pub fn try_from_tz(
        other: chrono::DateTime<Tz>,
        precision: usize,
    ) -> Result<Self, TryFromIntError> {
        Ok(Self::from_ticks(
            other.timezone(),
            chrono_to_ticks(&other, precision)?,
            precision,
        ))
    }
}

impl<const PRECISION: usize> TryFrom<DateTime64<PRECISION>> for chrono::DateTime<FixedOffset> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime64<PRECISION>) -> Result<Self, TryFromIntError> {
        Ok(ticks_to_chrono(date.0, date.ticks(), PRECISION)?.fixed_offset())
    }
}

impl TryFrom<DynDateTime64> for chrono::DateTime<FixedOffset> {
    type Error = TryFromIntError;

    fn try_from(date: DynDateTime64) -> Result<Self, TryFromIntError> {
        Ok(ticks_to_chrono(date.0, date.ticks(), date.2)?.fixed_offset())
    }
}

#[cfg(test)]
mod chrono_tests {
    use super::*;
    use chrono::TimeZone;
    use chrono_tz::UTC;

    #[test]
    #[allow(deprecated)]
    fn test_date() {
        for i in 0..30000u16 {
            let date = Date(i);
            let chrono_date: chrono::Date<Utc> = date.into();
            let new_date = Date::from(chrono_date);
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_naivedate() {
        for i in 0..30000u16 {
            let date = Date(i);
            let chrono_date: NaiveDate = date.into();
            let new_date = Date::from(chrono_date);
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime() {
        for i in (0..30000u32).map(|x| x * 10000) {
            let date = DateTime(UTC, i);
            let chrono_date: chrono::DateTime<Tz> = date.try_into().unwrap();
            let new_date = DateTime::try_from(chrono_date).unwrap();
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime64() {
        for i in (-30000..30000i64).map(|x| x * 10000) {
            let date = DateTime64::<6>::from_ticks(UTC, i);
            let chrono_date: chrono::DateTime<Tz> = date.try_into().unwrap();
            let new_date = DateTime64::try_from(chrono_date).unwrap();
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime64_precision() {
        for i in (-30000..30000i64).map(|x| x * 10000) {
            let date = DateTime64::<6>::from_ticks(UTC, i);
            let date_value = date.to_sql(None).unwrap();
            assert_eq!(
                date_value,
                Value::DateTime64(DynDateTime64::from_ticks(UTC, i, 6))
            );
            let chrono_date: chrono::DateTime<Utc> =
                FromSql::from_sql(&Type::DateTime64(6, UTC), date_value).unwrap();
            let new_date = DateTime64::try_from(chrono_date).unwrap();
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime64_precision2() {
        for i in (0..300u64).map(|x| x * 1000000) {
            let chrono_time = Utc.timestamp_opt(i as i64, i as u32).unwrap();
            let date = chrono_time.to_sql(None).unwrap();
            let out_time: chrono::DateTime<Utc> =
                FromSql::from_sql(&Type::DateTime64(9, UTC), date.clone()).unwrap();
            assert_eq!(chrono_time, out_time);
            let date = match date {
                Value::DateTime64(mut datetime) => {
                    datetime.2 -= 3;
                    datetime.1 = (datetime.ticks() / 1000) as u64;
                    Value::DateTime64(datetime)
                }
                _ => unimplemented!(),
            };
            let out_time: chrono::DateTime<Utc> =
                FromSql::from_sql(&Type::DateTime64(9, UTC), date.clone()).unwrap();

            assert_eq!(chrono_time, out_time);
        }
    }

    #[test]
    #[allow(deprecated)]
    fn test_consistency_with_convert_for_str() {
        let test_date = "2022-04-22 00:00:00";

        let dt = chrono::NaiveDateTime::parse_from_str(test_date, "%Y-%m-%d %H:%M:%S").unwrap();

        let chrono_date =
            chrono::DateTime::<Tz>::from_utc(dt, chrono_tz::UTC.offset_from_utc_datetime(&dt));

        let date = DateTime(UTC, dt.timestamp() as u32);

        let new_chrono_date: chrono::DateTime<Tz> = date.try_into().unwrap();

        assert_eq!(new_chrono_date, chrono_date);
    }

    /// Dates before 1970 are negative ticks; the sub-second part must stay positive.
    #[test]
    fn test_datetime64_before_epoch() {
        let chrono_date = Utc.with_ymd_and_hms(1960, 1, 1, 0, 0, 0).unwrap()
            + chrono::Duration::milliseconds(250);
        let date = DateTime64::<3>::try_from(chrono_date).unwrap();
        assert_eq!(date.ticks(), -315_619_199_750);
        let back: chrono::DateTime<Utc> = date.try_into().unwrap();
        assert_eq!(back, chrono_date);

        // -1 tick is 1969-12-31 23:59:59.999, not a panic.
        let back: chrono::DateTime<Utc> = DateTime64::<3>::from_ticks(UTC, -1).try_into().unwrap();
        assert_eq!(
            back,
            Utc.with_ymd_and_hms(1969, 12, 31, 23, 59, 59).unwrap()
                + chrono::Duration::milliseconds(999)
        );
    }

    /// Every precision ClickHouse allows (0..=9) round-trips, and anything above is an error.
    #[test]
    fn test_datetime64_all_precisions() {
        let chrono_date = Utc.with_ymd_and_hms(1925, 6, 7, 8, 9, 10).unwrap()
            + chrono::Duration::nanoseconds(123_456_789);
        for precision in 0..=9 {
            let date = DynDateTime64::try_from_utc(chrono_date, precision).unwrap();
            let back: chrono::DateTime<Utc> = date.try_into().unwrap();
            let truncated =
                123_456_789 / 10i64.pow(9 - precision as u32) * 10i64.pow(9 - precision as u32);
            assert_eq!(
                back,
                Utc.with_ymd_and_hms(1925, 6, 7, 8, 9, 10).unwrap()
                    + chrono::Duration::nanoseconds(truncated)
            );
        }
        assert!(DynDateTime64::try_from_utc(chrono_date, 10).is_err());
        assert!(chrono::DateTime::<Utc>::try_from(DynDateTime64(UTC, 1, 12)).is_err());
        assert!(
            chrono::DateTime::<Utc>::try_from(DynDateTime64::from_ticks(UTC, i64::MAX, 0)).is_err()
        );
    }

    /// `ToSql` follows the precision of the target column instead of always using 6.
    #[test]
    fn test_chrono_to_sql_uses_column_precision() {
        let chrono_date = Utc.with_ymd_and_hms(1950, 1, 1, 0, 0, 1).unwrap();
        let value = chrono_date.to_sql(Some(&Type::DateTime64(3, UTC))).unwrap();
        assert_eq!(
            value,
            Value::DateTime64(DynDateTime64::from_ticks(UTC, -631_151_999_000, 3))
        );
        let value = chrono_date.to_sql(None).unwrap();
        assert_eq!(
            value,
            Value::DateTime64(DynDateTime64::from_ticks(UTC, -631_151_999_000_000, 6))
        );
    }
}
