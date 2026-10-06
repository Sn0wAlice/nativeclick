pub use chrono_tz::Tz;
use futures_util::FutureExt;
pub(crate) use kinds::KindTree;
use std::fmt::Debug;
use std::future::Future;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

mod deserialize;
mod dynamic;
mod json;
mod kinds;
mod low_cardinality;
mod parse;
mod serialize;
#[cfg(test)]
mod tests;
mod variant;

use crate::{
    Date, Date32, DateTime, DynDateTime64, Ipv4, Ipv6, NativeclickError, Result,
    default_bf16_value, i256,
    io::{ClickhouseRead, ClickhouseWrite},
    is_bfloat16_enabled,
    protocol::MAX_STRING_SIZE,
    u256,
    values::Value,
};

/// A raw Clickhouse type.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Type {
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,

    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,

    Float32,
    Float64,
    BFloat16,

    Decimal32(usize),
    Decimal64(usize),
    Decimal128(usize),
    Decimal256(usize),

    String,
    FixedString(usize),

    Uuid,

    Date,
    /// Days since 1970-01-01, signed (Int32).
    Date32,
    DateTime(Tz),
    DateTime64(usize, Tz),
    /// Seconds, signed (Int32), may exceed 24h.
    Time,
    /// Ticks of `10^-precision` seconds, signed (Int64).
    Time64(usize),
    /// An Int64 count of the given unit.
    Interval(IntervalKind),

    Bool,
    /// The type of `NULL` and `[]` literals: carries no data.
    Nothing,

    Ipv4,
    Ipv6,

    // Geo types, see
    // https://clickhouse.com/docs/en/sql-reference/data-types/geo
    // These are just aliases of primitive types.
    Point,
    Ring,
    Polygon,
    MultiPolygon,
    /// `Array(Point)`, like `Ring`.
    LineString,
    /// `Array(Point)`, like `Ring`.
    MultiPoint,
    /// `Array(LineString)`, like `Polygon`.
    MultiLineString,
    /// Variant of the geo types, with a fixed discriminator order.
    Geometry,

    Enum8(Vec<(String, i8)>),
    Enum16(Vec<(String, i16)>),

    LowCardinality(Box<Type>),

    Array(Box<Type>),

    Tuple(Vec<Type>),
    /// `Tuple(name Type, ...)`. Values are [`Value::Tuple`], like an unnamed tuple.
    NamedTuple(Vec<(String, Type)>),

    Nullable(Box<Type>),

    Map(Box<Type>, Box<Type>),

    /// One of several types per row (sorted by name, as the server prints them). Rows are read
    /// as [`Value::Dynamic`] carrying the row's type, or [`Value::Null`].
    Variant(Vec<Type>),
    /// Any type per row; `max_types` as declared (None for the default). Rows are read as
    /// [`Value::Dynamic`] or [`Value::Null`].
    Dynamic(Option<usize>),
    /// The JSON object type. Rows are JSON text ([`Value::String`]); the declared arguments
    /// (typed paths, limits) are kept verbatim.
    Json(String),
    /// `SimpleAggregateFunction(function, Type)`: stored and sent exactly as the inner type.
    SimpleAggregateFunction(String, Box<Type>),
}

/// Unit of an `Interval*` type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IntervalKind {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl IntervalKind {
    const ALL: [IntervalKind; 11] = [
        IntervalKind::Nanosecond,
        IntervalKind::Microsecond,
        IntervalKind::Millisecond,
        IntervalKind::Second,
        IntervalKind::Minute,
        IntervalKind::Hour,
        IntervalKind::Day,
        IntervalKind::Week,
        IntervalKind::Month,
        IntervalKind::Quarter,
        IntervalKind::Year,
    ];

    pub fn name(&self) -> &'static str {
        match self {
            IntervalKind::Nanosecond => "Nanosecond",
            IntervalKind::Microsecond => "Microsecond",
            IntervalKind::Millisecond => "Millisecond",
            IntervalKind::Second => "Second",
            IntervalKind::Minute => "Minute",
            IntervalKind::Hour => "Hour",
            IntervalKind::Day => "Day",
            IntervalKind::Week => "Week",
            IntervalKind::Month => "Month",
            IntervalKind::Quarter => "Quarter",
            IntervalKind::Year => "Year",
        }
    }
}

/// Variants of `Geometry`, in its fixed discriminator order (not sorted by name).
pub const GEOMETRY_VARIANTS: [Type; 7] = [
    Type::LineString,
    Type::MultiLineString,
    Type::MultiPolygon,
    Type::Point,
    Type::Polygon,
    Type::Ring,
    Type::MultiPoint,
];

static RING: Type = Type::Ring;
static POLYGON: Type = Type::Polygon;
static INT64: Type = Type::Int64;

impl Type {
    /// For types that only rename another one on the wire, the type whose layout they use.
    pub(crate) fn storage(&self) -> Option<&Type> {
        match self {
            Type::LineString | Type::MultiPoint => Some(&RING),
            Type::MultiLineString => Some(&POLYGON),
            Type::Interval(_) => Some(&INT64),
            Type::SimpleAggregateFunction(_, inner) => Some(inner),
            _ => None,
        }
    }

    /// Looks through `LowCardinality` and `SimpleAggregateFunction`, which keep the values.
    fn plain(&self) -> &Type {
        match self {
            Type::LowCardinality(x) | Type::SimpleAggregateFunction(_, x) => x.plain(),
            _ => self,
        }
    }

    pub fn unwrap_array(&self) -> &Type {
        self.unarray()
            .unwrap_or_else(|| panic!("unwrap_array on non-array type {self}"))
    }

    pub fn unarray(&self) -> Option<&Type> {
        match self.plain() {
            Type::Array(x) => Some(&**x),
            _ => None,
        }
    }

    pub fn unwrap_map(&self) -> (&Type, &Type) {
        self.unmap()
            .unwrap_or_else(|| panic!("unwrap_map on non-map type {self}"))
    }

    pub fn unmap(&self) -> Option<(&Type, &Type)> {
        match self.plain() {
            Type::Map(key, value) => Some((&**key, &**value)),
            _ => None,
        }
    }

    pub fn unwrap_tuple(&self) -> &[Type] {
        self.untuple()
            .unwrap_or_else(|| panic!("unwrap_tuple on non-tuple type {self}"))
    }

    /// Element types of an unnamed tuple.
    pub fn untuple(&self) -> Option<&[Type]> {
        match self.plain() {
            Type::Tuple(x) => Some(&x[..]),
            _ => None,
        }
    }

    /// Element types of a tuple, named or not.
    pub fn tuple_types(&self) -> Option<Vec<&Type>> {
        match self.plain() {
            Type::Tuple(x) => Some(x.iter().collect()),
            Type::NamedTuple(x) => Some(x.iter().map(|x| &x.1).collect()),
            _ => None,
        }
    }

    pub fn unnull(&self) -> Option<&Type> {
        match self.plain() {
            Type::Nullable(x) => Some(&**x),
            _ => None,
        }
    }

    /// Whether writing this type needs the column values in its prefix (`Dynamic` lists the
    /// types it contains there).
    pub(crate) fn contains_dynamic(&self) -> bool {
        match self {
            Type::Dynamic(_) => true,
            Type::Array(x) | Type::Nullable(x) | Type::LowCardinality(x) => x.contains_dynamic(),
            Type::SimpleAggregateFunction(_, x) => x.contains_dynamic(),
            Type::Map(k, v) => k.contains_dynamic() || v.contains_dynamic(),
            Type::Tuple(x) | Type::Variant(x) => x.iter().any(Type::contains_dynamic),
            Type::NamedTuple(x) => x.iter().any(|x| x.1.contains_dynamic()),
            _ => false,
        }
    }

    pub fn strip_null(&self) -> &Type {
        match self {
            Type::Nullable(x) => x,
            _ => self,
        }
    }

    pub fn is_nullable(&self) -> bool {
        matches!(self, Type::Nullable(_))
    }

    pub fn default_value(&self) -> Value {
        match self {
            Type::Int8 => Value::Int8(0),
            Type::Int16 => Value::Int16(0),
            Type::Int32 => Value::Int32(0),
            Type::Int64 => Value::Int64(0),
            Type::Int128 => Value::Int128(0),
            Type::Int256 => Value::Int256(i256::default()),
            Type::UInt8 => Value::UInt8(0),
            Type::UInt16 => Value::UInt16(0),
            Type::UInt32 => Value::UInt32(0),
            Type::UInt64 => Value::UInt64(0),
            Type::UInt128 => Value::UInt128(0),
            Type::UInt256 => Value::UInt256(u256::default()),
            Type::Float32 => Value::Float32(0.0),
            Type::Float64 => Value::Float64(0.0),
            Type::BFloat16 => default_bf16_value(),
            Type::Decimal32(s) => Value::Decimal32(*s, 0),
            Type::Decimal64(s) => Value::Decimal64(*s, 0),
            Type::Decimal128(s) => Value::Decimal128(*s, 0),
            Type::Decimal256(s) => Value::Decimal256(*s, i256::default()),
            Type::String => Value::String(vec![]),
            Type::FixedString(_) => Value::String(vec![]),
            Type::Uuid => Value::Uuid(Uuid::from_u128(0)),
            Type::Date => Value::Date(Date(0)),
            Type::Date32 => Value::Date32(Date32(0)),
            Type::Time => Value::Time(0),
            Type::Time64(precision) => Value::Time64(*precision, 0),
            Type::Bool => Value::Bool(false),
            Type::Nothing => Value::Null,
            Type::LineString | Type::MultiPoint | Type::MultiLineString | Type::Interval(_) => {
                self.storage().unwrap().default_value()
            }
            Type::SimpleAggregateFunction(_, inner) => inner.default_value(),
            Type::NamedTuple(types) => {
                Value::Tuple(types.iter().map(|x| x.1.default_value()).collect())
            }
            Type::Variant(_) | Type::Dynamic(_) | Type::Geometry => Value::Null,
            Type::Json(_) => Value::String(b"{}".to_vec()),
            Type::DateTime(tz) => Value::DateTime(DateTime(*tz, 0)),
            Type::DateTime64(precision, tz) => Value::DateTime64(DynDateTime64(*tz, 0, *precision)),
            Type::Ipv4 => Value::Ipv4(Ipv4::default()),
            Type::Ipv6 => Value::Ipv6(Ipv6::default()),
            Type::Point => Value::Point(Default::default()),
            Type::Ring => Value::Ring(Default::default()),
            Type::Polygon => Value::Polygon(Default::default()),
            Type::MultiPolygon => Value::MultiPolygon(Default::default()),
            Type::Enum8(_) => Value::Enum8(0),
            Type::Enum16(_) => Value::Enum16(0),
            Type::LowCardinality(x) => x.default_value(),
            Type::Array(_) => Value::Array(vec![]),
            Type::Tuple(types) => Value::Tuple(types.iter().map(|x| x.default_value()).collect()),
            Type::Nullable(_) => Value::Null,
            Type::Map(_, _) => Value::Map(vec![], vec![]),
        }
    }

    pub fn strip_low_cardinality(&self) -> &Type {
        match self {
            Type::LowCardinality(x) => x,
            _ => self,
        }
    }
}

/// Reads `rows` cumulative UInt64 end offsets (arrays, maps, size-stream strings) in one piece,
/// checking they never go back and stay within `MAX_STRING_SIZE` items.
pub(crate) async fn read_offsets<R: ClickhouseRead>(
    reader: &mut R,
    rows: usize,
) -> Result<Vec<u64>> {
    if rows > MAX_STRING_SIZE {
        return Err(NativeclickError::DeserializeError(format!(
            "too many rows: {rows}"
        )));
    }
    let mut bytes = vec![0u8; rows * 8];
    reader.read_exact(&mut bytes).await?;
    let offsets: Vec<u64> = bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|x| u64::from_le_bytes(*x))
        .collect();
    let mut last = 0;
    for offset in &offsets {
        if *offset < last || *offset > MAX_STRING_SIZE as u64 {
            return Err(NativeclickError::DeserializeError(format!(
                "malformed offsets: {offset} after {last}"
            )));
        }
        last = *offset;
    }
    Ok(offsets)
}

/// How a type is laid out on the wire: types sharing a layout share a codec.
enum Codec<'a> {
    /// One fixed-width value per row.
    Sized,
    String,
    Array,
    Tuple,
    Point,
    Ring,
    Polygon,
    MultiPolygon,
    Nullable,
    Map,
    LowCardinality,
    /// One placeholder byte per row.
    Nothing,
    Variant,
    Dynamic,
    Json,
    /// Same layout as another type.
    Alias(&'a Type),
}

impl Type {
    fn codec(&self) -> Codec<'_> {
        match self {
            Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::Int128
            | Type::Int256
            | Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::UInt128
            | Type::UInt256
            | Type::Float32
            | Type::Float64
            | Type::BFloat16
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_)
            | Type::Uuid
            | Type::Date
            | Type::Date32
            | Type::DateTime(_)
            | Type::DateTime64(_, _)
            | Type::Time
            | Type::Time64(_)
            | Type::Bool
            | Type::Ipv4
            | Type::Ipv6
            | Type::Enum8(_)
            | Type::Enum16(_) => Codec::Sized,
            Type::String | Type::FixedString(_) => Codec::String,
            Type::Array(_) => Codec::Array,
            Type::Tuple(_) | Type::NamedTuple(_) => Codec::Tuple,
            Type::Point => Codec::Point,
            Type::Ring => Codec::Ring,
            Type::Polygon => Codec::Polygon,
            Type::MultiPolygon => Codec::MultiPolygon,
            Type::Nullable(_) => Codec::Nullable,
            Type::Map(_, _) => Codec::Map,
            Type::LowCardinality(_) => Codec::LowCardinality,
            Type::Nothing => Codec::Nothing,
            Type::Variant(_) | Type::Geometry => Codec::Variant,
            Type::Dynamic(_) => Codec::Dynamic,
            Type::Json(_) => Codec::Json,
            Type::LineString
            | Type::MultiPoint
            | Type::MultiLineString
            | Type::Interval(_)
            | Type::SimpleAggregateFunction(_, _) => Codec::Alias(self.storage().unwrap()),
        }
    }

    pub(crate) fn deserialize_prefix<'a, R: ClickhouseRead>(
        &'a self,
        reader: &'a mut R,
        state: &'a mut DeserializerState,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        use deserialize::*;
        async move {
            match self.codec() {
                Codec::Sized | Codec::String | Codec::Nothing => Ok(()),
                Codec::Array => array::ArrayDeserializer::read_prefix(self, reader, state).await,
                Codec::Tuple => tuple::TupleDeserializer::read_prefix(self, reader, state).await,
                Codec::Point => geo::PointDeserializer::read_prefix(self, reader, state).await,
                Codec::Ring => geo::RingDeserializer::read_prefix(self, reader, state).await,
                Codec::Polygon => geo::PolygonDeserializer::read_prefix(self, reader, state).await,
                Codec::MultiPolygon => {
                    geo::MultiPolygonDeserializer::read_prefix(self, reader, state).await
                }
                Codec::Nullable => {
                    nullable::NullableDeserializer::read_prefix(self, reader, state).await
                }
                Codec::Map => map::MapDeserializer::read_prefix(self, reader, state).await,
                Codec::LowCardinality => {
                    low_cardinality::LowCardinalityDeserializer::read_prefix(self, reader, state)
                        .await
                }
                Codec::Variant => {
                    variant::VariantDeserializer::read_prefix(self, reader, state).await
                }
                Codec::Dynamic => {
                    dynamic::DynamicDeserializer::read_prefix(self, reader, state).await
                }
                Codec::Json => json::JsonDeserializer::read_prefix(self, reader, state).await,
                Codec::Alias(storage) => storage.deserialize_prefix(reader, state).await,
            }
        }
        .boxed()
    }

    pub(crate) fn deserialize_column<'a, R: ClickhouseRead>(
        &'a self,
        reader: &'a mut R,
        rows: usize,
        state: &'a mut DeserializerState,
    ) -> impl Future<Output = Result<Vec<Value>>> + Send + 'a {
        use deserialize::*;
        async move {
            if rows > MAX_STRING_SIZE {
                return Err(NativeclickError::DeserializeError(format!(
                    "deserialize response size too large. {rows} > {MAX_STRING_SIZE}"
                )));
            }
            match self.codec() {
                Codec::Sized => sized::SizedDeserializer::read(self, reader, rows, state).await,
                Codec::String => string::StringDeserializer::read(self, reader, rows, state).await,
                Codec::Array => array::ArrayDeserializer::read(self, reader, rows, state).await,
                Codec::Tuple => tuple::TupleDeserializer::read(self, reader, rows, state).await,
                Codec::Point => geo::PointDeserializer::read(self, reader, rows, state).await,
                Codec::Ring => geo::RingDeserializer::read(self, reader, rows, state).await,
                Codec::Polygon => geo::PolygonDeserializer::read(self, reader, rows, state).await,
                Codec::MultiPolygon => {
                    geo::MultiPolygonDeserializer::read(self, reader, rows, state).await
                }
                Codec::Nullable => {
                    nullable::NullableDeserializer::read(self, reader, rows, state).await
                }
                Codec::Map => map::MapDeserializer::read(self, reader, rows, state).await,
                Codec::LowCardinality => {
                    low_cardinality::LowCardinalityDeserializer::read(self, reader, rows, state)
                        .await
                }
                Codec::Nothing => {
                    let mut skip = vec![0u8; rows];
                    reader.read_exact(&mut skip).await?;
                    Ok(vec![Value::Null; rows])
                }
                Codec::Variant => {
                    variant::VariantDeserializer::read(self, reader, rows, state).await
                }
                Codec::Dynamic => {
                    dynamic::DynamicDeserializer::read(self, reader, rows, state).await
                }
                Codec::Json => json::JsonDeserializer::read(self, reader, rows, state).await,
                Codec::Alias(storage) => storage.deserialize_column(reader, rows, state).await,
            }
        }
        .boxed()
    }

    pub(crate) fn serialize_column<'a, W: ClickhouseWrite>(
        &'a self,
        values: Vec<Value>,
        writer: &'a mut W,
        state: &'a mut SerializerState,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        use serialize::*;
        async move {
            match self.codec() {
                Codec::Sized => sized::SizedSerializer::write(self, values, writer, state).await,
                Codec::String => string::StringSerializer::write(self, values, writer, state).await,
                Codec::Array => array::ArraySerializer::write(self, values, writer, state).await,
                Codec::Tuple => tuple::TupleSerializer::write(self, values, writer, state).await,
                Codec::Point => geo::PointSerializer::write(self, values, writer, state).await,
                Codec::Ring => geo::RingSerializer::write(self, values, writer, state).await,
                Codec::Polygon => geo::PolygonSerializer::write(self, values, writer, state).await,
                Codec::MultiPolygon => {
                    geo::MultiPolygonSerializer::write(self, values, writer, state).await
                }
                Codec::Nullable => {
                    nullable::NullableSerializer::write(self, values, writer, state).await
                }
                Codec::Map => map::MapSerializer::write(self, values, writer, state).await,
                Codec::LowCardinality => {
                    low_cardinality::LowCardinalitySerializer::write(self, values, writer, state)
                        .await
                }
                Codec::Nothing => Ok(writer.write_all(&vec![b'0'; values.len()]).await?),
                Codec::Variant => {
                    variant::VariantSerializer::write(self, values, writer, state).await
                }
                Codec::Dynamic => {
                    dynamic::DynamicSerializer::write(self, values, writer, state).await
                }
                Codec::Json => json::JsonSerializer::write(self, values, writer, state).await,
                Codec::Alias(storage) => storage.serialize_column(values, writer, state).await,
            }
        }
        .boxed()
    }

    /// Writes the prefix of a column. `values` are the column's values, needed only when
    /// [`Type::contains_dynamic`] (empty otherwise).
    pub(crate) fn serialize_prefix<'a, W: ClickhouseWrite>(
        &'a self,
        values: &'a [Value],
        writer: &'a mut W,
        state: &'a mut SerializerState,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        use serialize::*;
        async move {
            match self.codec() {
                Codec::Sized | Codec::String | Codec::Nothing => Ok(()),
                Codec::Array => {
                    array::ArraySerializer::write_prefix(self, values, writer, state).await
                }
                Codec::Tuple => {
                    tuple::TupleSerializer::write_prefix(self, values, writer, state).await
                }
                Codec::Point => {
                    geo::PointSerializer::write_prefix(self, values, writer, state).await
                }
                Codec::Ring => geo::RingSerializer::write_prefix(self, values, writer, state).await,
                Codec::Polygon => {
                    geo::PolygonSerializer::write_prefix(self, values, writer, state).await
                }
                Codec::MultiPolygon => {
                    geo::MultiPolygonSerializer::write_prefix(self, values, writer, state).await
                }
                Codec::Nullable => {
                    nullable::NullableSerializer::write_prefix(self, values, writer, state).await
                }
                Codec::Map => map::MapSerializer::write_prefix(self, values, writer, state).await,
                Codec::LowCardinality => {
                    low_cardinality::LowCardinalitySerializer::write_prefix(
                        self, values, writer, state,
                    )
                    .await
                }
                Codec::Variant => {
                    variant::VariantSerializer::write_prefix(self, values, writer, state).await
                }
                Codec::Dynamic => {
                    dynamic::DynamicSerializer::write_prefix(self, values, writer, state).await
                }
                Codec::Json => {
                    json::JsonSerializer::write_prefix(self, values, writer, state).await
                }
                Codec::Alias(storage) => storage.serialize_prefix(values, writer, state).await,
            }
        }
        .boxed()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        fn check_scale(name: &str, scale: usize, max: usize) -> Result<()> {
            if scale > max {
                return Err(NativeclickError::TypeParseError(format!(
                    "scale out of bounds for {name}({scale}), must be in range (0..={max})"
                )));
            }
            Ok(())
        }

        match self {
            // The argument stored in these variants is the scale (digits after the point),
            // which may be 0. Its upper bound is the max precision of the underlying integer.
            Type::Decimal32(scale) => check_scale("Decimal32", *scale, 9)?,
            Type::Decimal64(scale) => check_scale("Decimal64", *scale, 18)?,
            Type::Decimal128(scale) => check_scale("Decimal128", *scale, 38)?,
            Type::Decimal256(scale) => check_scale("Decimal256", *scale, 76)?,
            Type::DateTime64(precision, _) => check_scale("DateTime64", *precision, 9)?,
            Type::Time64(precision) => check_scale("Time64", *precision, 9)?,
            Type::LowCardinality(inner) => match inner.strip_null() {
                Type::String
                | Type::FixedString(_)
                | Type::Date
                | Type::Date32
                | Type::Bool
                | Type::DateTime(_)
                | Type::Ipv4
                | Type::Ipv6
                | Type::Int8
                | Type::Int16
                | Type::Int32
                | Type::Int64
                | Type::Int128
                | Type::Int256
                | Type::UInt8
                | Type::UInt16
                | Type::UInt32
                | Type::UInt64
                | Type::UInt128
                | Type::UInt256 => inner.validate()?,
                _ => {
                    return Err(NativeclickError::TypeParseError(format!(
                        "illegal type '{inner:?}' in LowCardinality, not allowed"
                    )));
                }
            },
            Type::Array(inner) => {
                inner.validate()?;
            }
            Type::Tuple(inner) | Type::Variant(inner) => {
                for inner in inner {
                    inner.validate()?;
                }
            }
            Type::NamedTuple(inner) => {
                for (_, inner) in inner {
                    inner.validate()?;
                }
            }
            Type::SimpleAggregateFunction(_, inner) => inner.validate()?,
            Type::Nullable(inner) => match &**inner {
                Type::Array(_)
                | Type::Map(_, _)
                | Type::LowCardinality(_)
                | Type::Nullable(_)
                | Type::Variant(_)
                | Type::Dynamic(_)
                | Type::Json(_) => {
                    return Err(NativeclickError::TypeParseError(format!(
                        "nullable cannot contain composite type '{inner:?}'"
                    )));
                }
                _ => inner.validate()?,
            },
            Type::Map(key, value) => {
                if !matches!(
                    &**key,
                    Type::String
                        | Type::FixedString(_)
                        | Type::Int8
                        | Type::Int16
                        | Type::Int32
                        | Type::Int64
                        | Type::Int128
                        | Type::Int256
                        | Type::UInt8
                        | Type::UInt16
                        | Type::UInt32
                        | Type::UInt64
                        | Type::UInt128
                        | Type::UInt256
                        | Type::LowCardinality(_)
                        | Type::Uuid
                        | Type::Date
                        | Type::Date32
                        | Type::Bool
                        | Type::DateTime(_)
                        | Type::DateTime64(_, _)
                        | Type::Enum8(_)
                        | Type::Enum16(_)
                        | Type::Ipv4
                        | Type::Ipv6
                ) {
                    return Err(NativeclickError::TypeParseError("key in map must be String, Integer, LowCardinality, FixedString, UUID, Date, DateTime, Date32, Enum".to_string()));
                }
                key.validate()?;
                value.validate()?;
            }
            _ => (),
        }
        Ok(())
    }

    /// Checks that `value` fits this type, the type itself being already validated.
    pub(crate) fn check_value(&self, value: &Value) -> Result<()> {
        if !self.inner_validate_value(value) {
            return Err(NativeclickError::TypeParseError(format!(
                "could not assign value '{value:?}' to type '{self:?}'"
            )));
        }
        Ok(())
    }

    fn inner_validate_value(&self, value: &Value) -> bool {
        match (self, value) {
            (Type::Bool, Value::Bool(_) | Value::UInt8(_))
            | (Type::UInt8, Value::Bool(_))
            | (Type::Date32, Value::Date32(_))
            | (Type::Time, Value::Time(_))
            | (Type::Nothing, Value::Null)
            | (Type::Json(_), Value::String(_))
            | (Type::Variant(_) | Type::Dynamic(_) | Type::Geometry, Value::Null) => true,
            (Type::Time64(p1), Value::Time64(p2, _)) => p1 == p2,
            (Type::Variant(types), value) => {
                let value = match value {
                    Value::Dynamic(dynamic) => &dynamic.value,
                    value => value,
                };
                types.iter().any(|x| x.inner_validate_value(value))
            }
            (Type::Geometry, value) => {
                let value = match value {
                    Value::Dynamic(dynamic) => &dynamic.value,
                    value => value,
                };
                GEOMETRY_VARIANTS.iter().any(|x| x.inner_validate_value(value))
            }
            (Type::Dynamic(_), Value::Dynamic(dynamic)) => {
                dynamic.type_.inner_validate_value(&dynamic.value)
            }
            (Type::Dynamic(_), _) => true,
            (Type::NamedTuple(types), Value::Tuple(values)) => {
                types.len() == values.len()
                    && types.iter().zip(values).all(|((_, t), v)| t.inner_validate_value(v))
            }
            (Type::LineString | Type::MultiPoint | Type::MultiLineString | Type::Interval(_), value)
            | (Type::SimpleAggregateFunction(_, _), value) => {
                self.storage().unwrap().inner_validate_value(value)
            }
            (Type::Int8, Value::Int8(_))
            //FIXME: this is for compatibility with bools in CH < 22
            | (Type::Int8, Value::UInt8(_))
            | (Type::Int16, Value::Int16(_))
            | (Type::Int32, Value::Int32(_))
            | (Type::Int64, Value::Int64(_))
            | (Type::Int128, Value::Int128(_))
            | (Type::Int256, Value::Int256(_))
            | (Type::UInt8, Value::UInt8(_))
            | (Type::UInt16, Value::UInt16(_))
            | (Type::UInt32, Value::UInt32(_))
            | (Type::UInt64, Value::UInt64(_))
            | (Type::UInt128, Value::UInt128(_))
            | (Type::UInt256, Value::UInt256(_))
            | (Type::Float32, Value::Float32(_))
            | (Type::Float64, Value::Float64(_)) => true,
            (Type::BFloat16, Value::BFloat16(_)) => is_bfloat16_enabled(),
            (Type::Decimal32(precision1), Value::Decimal32(precision2, _)) => {
                precision1 == precision2
            }
            (Type::Decimal64(precision1), Value::Decimal64(precision2, _)) => {
                precision1 == precision2
            }
            (Type::Decimal128(precision1), Value::Decimal128(precision2, _)) => {
                precision1 == precision2
            }
            (Type::Decimal256(precision1), Value::Decimal256(precision2, _)) => {
                precision1 == precision2
            }
            (Type::FixedString(_), Value::Array(items))
            | (Type::String, Value::Array(items)) if items.iter().all(|item| matches!(item, Value::UInt8(_) | Value::Int8(_)))
            => true,
            (Type::String, Value::String(_))
            | (Type::FixedString(_), Value::String(_))
            | (Type::Uuid, Value::Uuid(_))
            | (Type::Date, Value::Date(_)) => true,
            (Type::DateTime(tz1), Value::DateTime(date)) => tz1 == &date.0,
            (Type::DateTime64(precision1, tz1), Value::DateTime64(tz2)) => {
                tz1 == &tz2.0 && precision1 == &tz2.2
            }
            (Type::Ipv4, Value::Ipv4(_)) | (Type::Ipv6, Value::Ipv6(_)) => true,
            (Type::Point, Value::Point(_)) | (Type::Ring, Value::Ring(_)) | (Type::Polygon, Value::Polygon(_)) | (Type::MultiPolygon, Value::MultiPolygon(_)) => true,
            (Type::Enum8(entries), Value::Enum8(index)) => entries.iter().any(|x| x.1 == *index),
            (Type::Enum16(entries), Value::Enum16(index)) => entries.iter().any(|x| x.1 == *index),
            (Type::LowCardinality(x), value) => x.inner_validate_value(value),
            (Type::Array(inner_type), Value::Array(values)) => {
                values.iter().all(|x| inner_type.inner_validate_value(x))
            }
            (Type::Tuple(inner_types), Value::Tuple(values)) => inner_types.len() == values.len() && inner_types
                .iter()
                .zip(values.iter())
                .all(|(type_, value)| type_.inner_validate_value(value)),
            (Type::Nullable(inner), value) => {
                value == &Value::Null || inner.inner_validate_value(value)
            }
            (Type::Map(key, value), Value::Map(keys, values)) => {
                keys.iter().all(|x| key.inner_validate_value(x))
                    && values.iter().all(|x| value.inner_validate_value(x))
            }
            (_, _) => false,
        }
    }
}

pub struct DeserializerState {
    /// Strings use the size-stream layout (revision >= 54492), except where the server keeps
    /// varint strings (LowCardinality dictionaries).
    pub(crate) string_size_stream: bool,
    /// Structures of the `Dynamic` columns whose prefix was read, in prefix order.
    pub(crate) dynamic: std::collections::VecDeque<dynamic::DynamicStructure>,
}

impl DeserializerState {
    pub(crate) fn new(revision: u64) -> Self {
        Self {
            string_size_stream: revision
                >= crate::protocol::DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION,
            dynamic: Default::default(),
        }
    }
}

pub struct SerializerState {
    /// Strings use the size-stream layout (revision >= 54492), except in LowCardinality
    /// dictionaries.
    pub(crate) string_size_stream: bool,
}

impl SerializerState {
    pub(crate) fn new(revision: u64) -> Self {
        Self {
            string_size_stream: revision
                >= crate::protocol::DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION,
        }
    }
}

pub trait Deserializer {
    fn read_prefix<R: ClickhouseRead>(
        _type_: &Type,
        _reader: &mut R,
        _state: &mut DeserializerState,
    ) -> impl Future<Output = Result<()>> {
        async { Ok(()) }
    }

    fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> impl Future<Output = Result<Vec<Value>>>;
}

pub trait Serializer {
    fn write_prefix<W: ClickhouseWrite>(
        _type_: &Type,
        _values: &[Value],
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> impl Future<Output = Result<()>> {
        async { Ok(()) }
    }

    fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> impl Future<Output = Result<()>>;
}
