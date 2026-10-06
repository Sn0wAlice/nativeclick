//! `Dynamic`, in the FLATTENED serialization: the list of types, then one index per row
//! (`types.len()` = NULL) and each type's column with only its own rows.
//!
//! The client asks for this layout with `output_format_native_use_flattened_dynamic_and_json_serialization`:
//! unlike V1/V2 it never puts values in the binary-encoded "shared variant".

use std::str::FromStr;

use tokio::io::AsyncWriteExt;

use super::{Deserializer, DeserializerState, Serializer, SerializerState, Type, variant};
use crate::{
    NativeclickError, Result,
    io::{ClickhouseRead, ClickhouseWrite},
    protocol::MAX_STRING_SIZE,
    values::Value,
};

const VERSION_FLATTENED: u64 = 3;

/// Types of a `Dynamic` column, read from its prefix.
pub(crate) struct DynamicStructure {
    types: Vec<Type>,
}

async fn read_index<R: ClickhouseRead>(reader: &mut R, width: usize) -> Result<u64> {
    use tokio::io::AsyncReadExt;
    Ok(match width {
        1 => reader.read_u8().await? as u64,
        2 => reader.read_u16_le().await? as u64,
        4 => reader.read_u32_le().await? as u64,
        _ => reader.read_u64_le().await?,
    })
}

/// Width of the index column for `types` types plus NULL.
fn index_width(types: usize) -> usize {
    match types + 1 {
        0..=0x100 => 1,
        0x101..=0x1_0000 => 2,
        0x1_0001..=0x1_0000_0000 => 4,
        _ => 8,
    }
}

pub struct DynamicDeserializer;

impl Deserializer for DynamicDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        _type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        use tokio::io::AsyncReadExt;
        let version = reader.read_u64_le().await?;
        if version != VERSION_FLATTENED {
            return Err(NativeclickError::DeserializeError(format!(
                "Dynamic serialization version {version} is not supported: keep the setting \
                 output_format_native_use_flattened_dynamic_and_json_serialization = 1"
            )));
        }
        let count = reader.read_var_uint().await?;
        if count as usize > MAX_STRING_SIZE {
            return Err(NativeclickError::DeserializeError(format!(
                "too many Dynamic types: {count}"
            )));
        }
        let mut types = Vec::with_capacity(count.min(1024) as usize);
        for _ in 0..count {
            types.push(Type::from_str(&reader.read_utf8_string().await?)?);
        }
        for type_ in &types {
            type_.deserialize_prefix(reader, state).await?;
        }
        state.dynamic.push_back(DynamicStructure { types });
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        _type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let DynamicStructure { types } = state.dynamic.pop_front().ok_or_else(|| {
            NativeclickError::DeserializeError("Dynamic column without prefix".to_string())
        })?;
        let width = index_width(types.len());
        let mut indexes = Vec::with_capacity(rows.min(1 << 16));
        for _ in 0..rows {
            indexes.push(read_index(reader, width).await?);
        }
        let columns = variant::read_columns(&types, &indexes, reader, state).await?;
        variant::assemble(&types, &indexes, types.len() as u64, columns)
    }
}

/// Types of the values in first-appearance order, and each value's index (`None` for NULL).
fn plan(values: &[Value]) -> (Vec<Type>, Vec<Option<usize>>) {
    let mut types: Vec<Type> = vec![];
    let indexes = values
        .iter()
        .map(|value| {
            let type_ = match value {
                Value::Null => return None,
                Value::Dynamic(dynamic) => dynamic.type_.clone(),
                value => value.guess_type(),
            };
            Some(match types.iter().position(|x| *x == type_) {
                Some(index) => index,
                None => {
                    types.push(type_);
                    types.len() - 1
                }
            })
        })
        .collect();
    (types, indexes)
}

fn unwrap_dynamic(value: &Value) -> Value {
    match value {
        Value::Dynamic(dynamic) => dynamic.value.clone(),
        value => value.clone(),
    }
}

/// Values of each type, in row order.
fn group(types: &[Type], indexes: &[Option<usize>], values: &[Value]) -> Vec<Vec<Value>> {
    let mut columns = vec![vec![]; types.len()];
    for (index, value) in indexes.iter().zip(values) {
        if let Some(index) = index {
            columns[*index].push(unwrap_dynamic(value));
        }
    }
    columns
}

pub struct DynamicSerializer;

impl Serializer for DynamicSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        _type_: &Type,
        values: &[Value],
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let (types, indexes) = plan(values);
        writer.write_u64_le(VERSION_FLATTENED).await?;
        writer.write_var_uint(types.len() as u64).await?;
        for type_ in &types {
            writer.write_string(type_.to_string()).await?;
        }
        for (type_, column) in types.iter().zip(group(&types, &indexes, values)) {
            type_.serialize_prefix(&column, writer, state).await?;
        }
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        _type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let (types, indexes) = plan(&values);
        let null = types.len() as u64;
        for index in &indexes {
            let index = index.map(|x| x as u64).unwrap_or(null);
            match index_width(types.len()) {
                1 => writer.write_u8(index as u8).await?,
                2 => writer.write_u16_le(index as u16).await?,
                4 => writer.write_u32_le(index as u32).await?,
                _ => writer.write_u64_le(index).await?,
            }
        }
        for (type_, column) in types.iter().zip(group(&types, &indexes, &values)) {
            if !column.is_empty() {
                type_.validate()?;
                type_.serialize_column(column, writer, state).await?;
            }
        }
        Ok(())
    }
}
