use tokio::io::AsyncWriteExt;

use crate::{Result, io::ClickhouseWrite, serialize_bf16_to_bits, values::Value};

use super::{Serializer, SerializerState, Type};

pub struct SizedSerializer;

fn swap_endian_256(mut input: [u8; 32]) -> [u8; 32] {
    input.reverse();
    input
}

/// Appends the little-endian bytes of one value.
fn encode(type_: &Type, value: &Value, out: &mut Vec<u8>) -> Result<()> {
    match value.justify_null_ref(type_).as_ref() {
        Value::Int8(x) => out.push(*x as u8),
        Value::Int16(x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::Int32(x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::Int64(x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::Int128(x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::Int256(x) => out.extend_from_slice(&swap_endian_256(x.0)),
        Value::UInt8(x) => out.push(*x),
        Value::UInt16(x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::UInt32(x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::UInt64(x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::UInt128(x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::UInt256(x) => out.extend_from_slice(&swap_endian_256(x.0)),
        Value::Float32(x) => out.extend_from_slice(&x.to_bits().to_le_bytes()),
        Value::Float64(x) => out.extend_from_slice(&x.to_bits().to_le_bytes()),
        Value::BFloat16(x) => out.extend_from_slice(&serialize_bf16_to_bits(x).to_le_bytes()),
        Value::Decimal32(_, x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::Decimal64(_, x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::Decimal128(_, x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::Decimal256(_, x) => out.extend_from_slice(&swap_endian_256(x.0)),
        Value::Uuid(x) => {
            let n = x.as_u128();
            out.extend_from_slice(&((n >> 64) as u64).to_le_bytes());
            out.extend_from_slice(&(n as u64).to_le_bytes());
        }
        Value::Date(x) => out.extend_from_slice(&x.0.to_le_bytes()),
        Value::DateTime(x) => out.extend_from_slice(&x.1.to_le_bytes()),
        Value::DateTime64(x) => out.extend_from_slice(&x.1.to_le_bytes()),
        Value::Ipv4(x) => out.extend_from_slice(&u32::from(x.0).to_le_bytes()),
        Value::Ipv6(x) => out.extend_from_slice(&x.octets()),
        Value::Enum8(x) => out.push(*x as u8),
        Value::Enum16(x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::Bool(x) => out.push(*x as u8),
        Value::Date32(x) => out.extend_from_slice(&x.0.to_le_bytes()),
        Value::Time(x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::Time64(_, x) => out.extend_from_slice(&x.to_le_bytes()),
        x => {
            return Err(crate::NativeclickError::SerializeError(format!(
                "cannot write {x:?} as {type_}"
            )));
        }
    }
    Ok(())
}

impl Serializer for SizedSerializer {
    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        let width = super::super::deserialize::sized::width(type_).unwrap_or(8);
        let mut out = Vec::with_capacity(values.len() * width);
        for value in &values {
            encode(type_, value, &mut out)?;
        }
        writer.write_all(&out).await?;
        Ok(())
    }
}
