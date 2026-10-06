use crate::{Result, io::ClickhouseWrite, values::Value};

use super::{Serializer, SerializerState, Type};

pub struct TupleSerializer;

impl Serializer for TupleSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        type_: &Type,
        values: &[Value],
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let inner_types = type_.tuple_types().unwrap_or_default();
        let dynamic = inner_types.iter().any(|x| x.contains_dynamic());
        for (i, item) in inner_types.into_iter().enumerate() {
            let column = if dynamic {
                values
                    .iter()
                    .map(|x| match x {
                        Value::Tuple(items) => items.get(i).cloned().unwrap_or(Value::Null),
                        _ => Value::Null,
                    })
                    .collect()
            } else {
                vec![]
            };
            item.serialize_prefix(&column, writer, state).await?;
        }
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let inner_types = type_.tuple_types().unwrap_or_default();

        let mut columns = vec![Vec::with_capacity(values.len()); inner_types.len()];

        for value in values {
            let Value::Tuple(tuple) = value else {
                return Err(crate::NativeclickError::SerializeError(format!(
                    "expected a tuple for {type_}, got {value:?}"
                )));
            };
            if tuple.len() != inner_types.len() {
                return Err(crate::NativeclickError::SerializeError(format!(
                    "expected {} tuple elements for {type_}, got {}",
                    inner_types.len(),
                    tuple.len()
                )));
            }
            for (i, value) in tuple.into_iter().enumerate() {
                columns[i].push(value);
            }
        }
        for (inner_type, column) in inner_types.iter().zip(columns) {
            inner_type.serialize_column(column, writer, state).await?;
        }
        Ok(())
    }
}
