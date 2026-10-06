use tokio::io::AsyncReadExt;

use crate::{
    NativeclickError, Result, io::ClickhouseRead, protocol::MAX_STRING_SIZE, values::Value,
};

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
    let mut ends = Vec::with_capacity(rows.min(1 << 16));
    let mut last = 0u64;
    for _ in 0..rows {
        let end = reader.read_u64_le().await?;
        if end < last || end > MAX_STRING_SIZE as u64 {
            return Err(NativeclickError::DeserializeError(format!(
                "malformed string offsets: {end} after {last}"
            )));
        }
        ends.push(end as usize);
        last = end;
    }
    let mut bytes = vec![0u8; last as usize];
    reader.read_exact(&mut bytes).await?;
    let mut out = Vec::with_capacity(rows);
    let mut start = 0;
    for end in ends {
        out.push(Value::String(bytes[start..end].to_vec()));
        start = end;
    }
    Ok(out)
}
