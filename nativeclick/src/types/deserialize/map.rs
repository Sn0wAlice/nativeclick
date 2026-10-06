use crate::{NativeclickError, Result, io::ClickhouseRead, values::Value};

use super::{Deserializer, DeserializerState, Type};

pub struct MapDeserializer;

fn key_value(type_: &Type) -> Result<(&Type, &Type)> {
    type_
        .unmap()
        .ok_or_else(|| NativeclickError::DeserializeError(format!("not a map type: {type_}")))
}

impl Deserializer for MapDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        // Array(Tuple(key, value)): the prefixes of key then value.
        let (key, value) = key_value(type_)?;
        key.deserialize_prefix(reader, state).await?;
        value.deserialize_prefix(reader, state).await?;
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let (key, value) = key_value(type_)?;
        let offsets = super::super::read_offsets(reader, rows).await?;
        let total = offsets.last().copied().unwrap_or(0) as usize;
        let keys = key.deserialize_column(reader, total, state).await?;
        let values = value.deserialize_column(reader, total, state).await?;
        if keys.len() != total || values.len() != total {
            return Err(NativeclickError::DeserializeError(format!(
                "map has {} keys and {} values, expected {total}",
                keys.len(),
                values.len()
            )));
        }
        let (mut keys, mut values) = (keys.into_iter(), values.into_iter());
        let mut start = 0;
        Ok(offsets
            .into_iter()
            .map(|end| {
                let count = end as usize - start;
                start = end as usize;
                Value::Map(
                    keys.by_ref().take(count).collect(),
                    values.by_ref().take(count).collect(),
                )
            })
            .collect())
    }
}
