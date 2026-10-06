use std::net::{Ipv4Addr, Ipv6Addr};

use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::{
    Date, Date32, DateTime, DynDateTime64, NativeclickError, Result, deserialize_bf16_from_bits,
    i256, io::ClickhouseRead, u256, values::Value,
};

use super::{Deserializer, DeserializerState, Type};

pub struct SizedDeserializer;

/// Bytes per row of a fixed-size type.
pub(crate) fn width(type_: &Type) -> Option<usize> {
    Some(match type_ {
        Type::Int8 | Type::UInt8 | Type::Bool | Type::Enum8(_) => 1,
        Type::Int16 | Type::UInt16 | Type::Date | Type::Enum16(_) | Type::BFloat16 => 2,
        Type::Int32
        | Type::UInt32
        | Type::Float32
        | Type::Date32
        | Type::DateTime(_)
        | Type::Decimal32(_)
        | Type::Ipv4
        | Type::Time => 4,
        Type::Int64
        | Type::UInt64
        | Type::Float64
        | Type::Decimal64(_)
        | Type::DateTime64(_, _)
        | Type::Time64(_) => 8,
        Type::Int128 | Type::UInt128 | Type::Decimal128(_) | Type::Uuid | Type::Ipv6 => 16,
        Type::Int256 | Type::UInt256 | Type::Decimal256(_) => 32,
        _ => return None,
    })
}

/// Largest column read in one piece, so a hostile row count cannot request a huge allocation.
const MAX_COLUMN_BYTES: usize = 1 << 31;

fn little_endian_256(bytes: &[u8]) -> [u8; 32] {
    let mut buf: [u8; 32] = bytes.try_into().unwrap();
    buf.reverse();
    buf
}

/// One value from its `width(type_)` little-endian bytes.
fn decode(type_: &Type, b: &[u8]) -> Value {
    macro_rules! le {
        ($t:ty) => {
            <$t>::from_le_bytes(b.try_into().unwrap())
        };
    }
    match type_ {
        Type::Int8 => Value::Int8(b[0] as i8),
        Type::Int16 => Value::Int16(le!(i16)),
        Type::Int32 => Value::Int32(le!(i32)),
        Type::Int64 => Value::Int64(le!(i64)),
        Type::Int128 => Value::Int128(le!(i128)),
        Type::Int256 => Value::Int256(i256(little_endian_256(b))),
        Type::UInt8 => Value::UInt8(b[0]),
        Type::UInt16 => Value::UInt16(le!(u16)),
        Type::UInt32 => Value::UInt32(le!(u32)),
        Type::UInt64 => Value::UInt64(le!(u64)),
        Type::UInt128 => Value::UInt128(le!(u128)),
        Type::UInt256 => Value::UInt256(u256(little_endian_256(b))),
        Type::Float32 => Value::Float32(f32::from_bits(le!(u32))),
        Type::Float64 => Value::Float64(f64::from_bits(le!(u64))),
        Type::BFloat16 => deserialize_bf16_from_bits(le!(u16)),
        Type::Decimal32(s) => Value::Decimal32(*s, le!(i32)),
        Type::Decimal64(s) => Value::Decimal64(*s, le!(i64)),
        Type::Decimal128(s) => Value::Decimal128(*s, le!(i128)),
        Type::Decimal256(s) => Value::Decimal256(*s, i256(little_endian_256(b))),
        Type::Uuid => {
            let high = u64::from_le_bytes(b[..8].try_into().unwrap());
            let low = u64::from_le_bytes(b[8..].try_into().unwrap());
            Value::Uuid(Uuid::from_u128(((high as u128) << 64) | low as u128))
        }
        Type::Date => Value::Date(Date(le!(u16))),
        Type::Date32 => Value::Date32(Date32(le!(i32))),
        Type::DateTime(tz) => Value::DateTime(DateTime(*tz, le!(u32))),
        Type::DateTime64(precision, tz) => {
            Value::DateTime64(DynDateTime64(*tz, le!(u64), *precision))
        }
        Type::Time => Value::Time(le!(i32)),
        Type::Time64(precision) => Value::Time64(*precision, le!(i64)),
        Type::Ipv4 => Value::Ipv4(Ipv4Addr::from(le!(u32)).into()),
        Type::Ipv6 => Value::Ipv6(Ipv6Addr::from(<[u8; 16]>::try_from(b).unwrap()).into()),
        Type::Enum8(_) => Value::Enum8(b[0] as i8),
        Type::Enum16(_) => Value::Enum16(le!(i16)),
        Type::Bool => Value::Bool(b[0] != 0),
        _ => unreachable!("width() is Some only for the types above"),
    }
}

impl Deserializer for SizedDeserializer {
    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let width = width(type_).ok_or_else(|| {
            NativeclickError::DeserializeError(format!("not a fixed-size type: {type_}"))
        })?;
        let length = rows
            .checked_mul(width)
            .filter(|x| *x <= MAX_COLUMN_BYTES)
            .ok_or_else(|| {
                NativeclickError::DeserializeError(format!("column too large: {rows} rows"))
            })?;
        let mut bytes = vec![0u8; length];
        reader.read_exact(&mut bytes).await?;
        Ok(bytes
            .chunks_exact(width)
            .map(|b| decode(type_, b))
            .collect())
    }
}
