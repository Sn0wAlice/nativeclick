//! `JSON` columns as text: the client asks for `output_format_native_write_json_as_string`, which
//! sends the version tag 1 then one length-prefixed JSON document per row (never the size-stream
//! string layout). Inserts use the same form.

use tokio::io::AsyncWriteExt;

use super::{Deserializer, DeserializerState, Serializer, SerializerState, Type};
use crate::{
    NativeclickError, Result,
    io::{ClickhouseRead, ClickhouseWrite},
    values::Value,
};

const VERSION_STRING: u64 = 1;

pub struct JsonDeserializer;

impl Deserializer for JsonDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        _type_: &Type,
        reader: &mut R,
        _state: &mut DeserializerState,
    ) -> Result<()> {
        use tokio::io::AsyncReadExt;
        let version = reader.read_u64_le().await?;
        if version != VERSION_STRING {
            return Err(NativeclickError::DeserializeError(format!(
                "JSON serialization version {version} is not supported: keep the setting \
                 output_format_native_write_json_as_string = 1"
            )));
        }
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        _type_: &Type,
        reader: &mut R,
        rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let mut out = Vec::with_capacity(rows.min(1 << 16));
        for _ in 0..rows {
            out.push(Value::String(reader.read_string().await?));
        }
        Ok(out)
    }
}

pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        _type_: &Type,
        _values: &[Value],
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        writer.write_u64_le(VERSION_STRING).await?;
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        _type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        for value in values {
            match value {
                Value::String(text) => writer.write_string(&text).await?,
                Value::Null => writer.write_string(b"{}").await?,
                value => {
                    return Err(NativeclickError::SerializeError(format!(
                        "expected JSON text, got {value:?}"
                    )));
                }
            }
        }
        Ok(())
    }
}
