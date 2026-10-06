use tokio::io::AsyncReadExt;

use crate::{NativeclickError, Result, io::ClickhouseRead, values::Value};

use super::{Deserializer, DeserializerState, Type};

pub struct StringDeserializer;

impl Deserializer for StringDeserializer {
    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        match type_ {
            Type::String if state.string_size_stream => read_size_stream(reader, rows).await,
            Type::String => {
                let mut out = Vec::with_capacity(rows.min(1 << 16));
                for _ in 0..rows {
                    out.push(Value::String(reader.read_string().await?));
                }
                Ok(out)
            }
            Type::FixedString(n) => {
                let mut out = Vec::with_capacity(rows.min(1 << 16));
                for _ in 0..rows {
                    let mut buf = vec![0u8; *n];
                    reader.read_exact(&mut buf[..]).await?;
                    let first_null = buf.iter().position(|x| *x == 0).unwrap_or(buf.len());
                    buf.truncate(first_null);
                    out.push(Value::String(buf));
                }
                Ok(out)
            }
            _ => Err(NativeclickError::DeserializeError(format!(
                "not a string type: {type_}"
            ))),
        }
    }
}

/// Revision 54492+: `rows` cumulative end offsets (UInt64), then all the bytes.
async fn read_size_stream<R: ClickhouseRead>(reader: &mut R, rows: usize) -> Result<Vec<Value>> {
    let ends = super::super::read_offsets(reader, rows).await?;
    let mut bytes = vec![0u8; ends.last().copied().unwrap_or(0) as usize];
    reader.read_exact(&mut bytes).await?;
    let mut start = 0;
    Ok(ends
        .into_iter()
        .map(|end| {
            let value = Value::String(bytes[start..end as usize].to_vec());
            start = end as usize;
            value
        })
        .collect())
}
