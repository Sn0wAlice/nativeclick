use tokio::io::AsyncWriteExt;

use std::borrow::Cow;

use crate::{NativeclickError, Result, io::ClickhouseWrite, values::Value};

use super::{Serializer, SerializerState, Type};

pub struct StringSerializer;

async fn emit_bytes<W: ClickhouseWrite>(type_: &Type, bytes: &[u8], writer: &mut W) -> Result<()> {
    if let Type::FixedString(s) = type_ {
        if bytes.len() >= *s {
            writer.write_all(&bytes[..*s]).await?;
        } else {
            writer.write_all(bytes).await?;
            let padding = *s - bytes.len();
            for _ in 0..padding {
                writer.write_u8(0).await?;
            }
        }
    } else {
        writer.write_string(bytes).await?;
    }
    Ok(())
}

/// The bytes of a String value: a `String`, or an array of `UInt8`/`Int8` (`Vec<u8>` fields).
fn value_bytes(value: &Value) -> Result<Cow<'_, [u8]>> {
    match value {
        Value::String(bytes) => Ok(Cow::Borrowed(bytes)),
        Value::Null => Ok(Cow::Borrowed(&[])),
        Value::Array(items) => items
            .iter()
            .map(|x| match x {
                Value::UInt8(x) => Ok(*x),
                Value::Int8(x) => Ok(*x as u8),
                x => Err(NativeclickError::SerializeError(format!(
                    "expected bytes for a String, got {x:?}"
                ))),
            })
            .collect::<Result<Vec<u8>>>()
            .map(Cow::Owned),
        x => Err(NativeclickError::SerializeError(format!(
            "expected a String, got {x:?}"
        ))),
    }
}

impl Serializer for StringSerializer {
    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        if matches!(type_, Type::String) && state.string_size_stream {
            // Revision 54492+: cumulative end offsets, then all the bytes.
            let mut end = 0u64;
            for value in &values {
                end += value_bytes(value)?.len() as u64;
                writer.write_u64_le(end).await?;
            }
            for value in &values {
                writer.write_all(&value_bytes(value)?).await?;
            }
            return Ok(());
        }
        for value in &values {
            emit_bytes(type_, &value_bytes(value)?, writer).await?;
        }
        Ok(())
    }
}
