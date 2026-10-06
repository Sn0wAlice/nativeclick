//! `Variant(T1, T2, ...)` and `Geometry`: a discriminator per row (255 = NULL), then each
//! variant's column with only its own rows.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{Deserializer, DeserializerState, Serializer, SerializerState, Type};
use crate::{
    DynamicValue, NativeclickError, Result,
    io::{ClickhouseRead, ClickhouseWrite},
    values::Value,
};

pub(crate) const NULL_DISCRIMINATOR: u8 = 255;
/// Discriminators sent one per row (the only mode used by the Native format).
const MODE_BASIC: u64 = 0;

fn variants(type_: &Type) -> Result<&[Type]> {
    type_
        .variants()
        .ok_or_else(|| NativeclickError::DeserializeError(format!("not a variant type: {type_}")))
}

/// Rebuilds rows from per-row discriminators and one column per variant.
pub(crate) fn assemble(
    types: &[Type],
    discriminators: &[u64],
    null: u64,
    columns: Vec<Vec<Value>>,
) -> Result<Vec<Value>> {
    let mut columns = columns.into_iter().map(Vec::into_iter).collect::<Vec<_>>();
    discriminators
        .iter()
        .map(|d| {
            if *d == null {
                return Ok(Value::Null);
            }
            let value = columns
                .get_mut(*d as usize)
                .and_then(Iterator::next)
                .ok_or_else(|| {
                    NativeclickError::DeserializeError(format!("bad discriminator {d}"))
                })?;
            Ok(Value::Dynamic(Box::new(DynamicValue::new(
                types[*d as usize].clone(),
                value,
            ))))
        })
        .collect()
}

/// Reads the column of every variant that has rows.
pub(crate) async fn read_columns<R: ClickhouseRead>(
    types: &[Type],
    discriminators: &[u64],
    reader: &mut R,
    state: &mut DeserializerState,
) -> Result<Vec<Vec<Value>>> {
    let mut counts = vec![0usize; types.len()];
    for d in discriminators {
        if let Some(count) = counts.get_mut(*d as usize) {
            *count += 1;
        }
    }
    let mut columns = Vec::with_capacity(types.len());
    for (type_, count) in types.iter().zip(counts) {
        columns.push(if count == 0 {
            vec![]
        } else {
            type_.deserialize_column(reader, count, state).await?
        });
    }
    Ok(columns)
}

pub struct VariantDeserializer;

impl Deserializer for VariantDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        let mode = reader.read_u64_le().await?;
        if mode != MODE_BASIC {
            return Err(NativeclickError::DeserializeError(format!(
                "unsupported Variant discriminators mode {mode}"
            )));
        }
        for variant in variants(type_)? {
            variant.deserialize_prefix(reader, state).await?;
        }
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let types = variants(type_)?;
        let mut raw = vec![0u8; rows];
        reader.read_exact(&mut raw).await?;
        let discriminators = raw.into_iter().map(u64::from).collect::<Vec<_>>();
        let columns = read_columns(types, &discriminators, reader, state).await?;
        assemble(types, &discriminators, NULL_DISCRIMINATOR as u64, columns)
    }
}

/// Index of the variant a value is written as: the declared type of a [`Value::Dynamic`], else
/// the first variant that accepts the value.
fn discriminator(types: &[Type], value: &Value) -> Result<u8> {
    let position = match value {
        Value::Null => return Ok(NULL_DISCRIMINATOR),
        Value::Dynamic(dynamic) => types.iter().position(|t| *t == dynamic.type_).or_else(|| {
            types
                .iter()
                .position(|t| t.inner_validate_value(&dynamic.value))
        }),
        value => types.iter().position(|t| t.inner_validate_value(value)),
    };
    position.map(|x| x as u8).ok_or_else(|| {
        NativeclickError::SerializeError(format!(
            "value {value:?} matches none of the variants {types:?}"
        ))
    })
}

fn unwrap_dynamic(value: Value) -> Value {
    match value {
        Value::Dynamic(dynamic) => dynamic.value,
        value => value,
    }
}

pub struct VariantSerializer;

impl Serializer for VariantSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        type_: &Type,
        _values: &[Value],
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        writer.write_u64_le(MODE_BASIC).await?;
        for variant in variants(type_)? {
            variant.serialize_prefix(&[], writer, state).await?;
        }
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let types = variants(type_)?;
        let discriminators = values
            .iter()
            .map(|x| discriminator(types, x))
            .collect::<Result<Vec<_>>>()?;
        writer.write_all(&discriminators).await?;
        let mut columns = vec![vec![]; types.len()];
        for (d, value) in discriminators.into_iter().zip(values) {
            if d != NULL_DISCRIMINATOR {
                columns[d as usize].push(unwrap_dynamic(value));
            }
        }
        for (type_, column) in types.iter().zip(columns) {
            if !column.is_empty() {
                type_.serialize_column(column, writer, state).await?;
            }
        }
        Ok(())
    }
}
