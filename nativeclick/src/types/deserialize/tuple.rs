use crate::{Result, io::ClickhouseRead, values::Value};

use super::{Deserializer, DeserializerState, Type};

pub struct TupleDeserializer;

impl Deserializer for TupleDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        for item in type_.tuple_types().unwrap_or_default() {
            item.deserialize_prefix(reader, state).await?;
        }
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let mut columns = vec![];
        for type_ in type_.tuple_types().unwrap_or_default() {
            columns.push(type_.deserialize_column(reader, rows, state).await?);
        }
        Ok(zip_tuples(columns, rows))
    }
}

/// Rows of tuples from one column per element.
pub(crate) fn zip_tuples(columns: Vec<Vec<Value>>, rows: usize) -> Vec<Value> {
    let width = columns.len();
    let mut tuples = vec![Vec::with_capacity(width); rows];
    for column in columns {
        for (tuple, value) in tuples.iter_mut().zip(column) {
            tuple.push(value);
        }
    }
    tuples.into_iter().map(Value::Tuple).collect()
}
