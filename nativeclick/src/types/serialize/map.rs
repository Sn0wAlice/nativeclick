use tokio::io::AsyncWriteExt;

use crate::{Result, io::ClickhouseWrite, values::Value};

use super::{Serializer, SerializerState, Type};

pub struct MapSerializer;

impl Serializer for MapSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        type_: &Type,
        values: &[Value],
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        // Array(Tuple(key, value)): the prefixes of key then value.
        let (key, value) = type_.unwrap_map();
        let (mut keys, mut items) = (vec![], vec![]);
        if key.contains_dynamic() || value.contains_dynamic() {
            for map in values {
                if let Value::Map(k, v) = map {
                    keys.extend(k.iter().cloned());
                    items.extend(v.iter().cloned());
                }
            }
        }
        key.serialize_prefix(&keys, writer, state).await?;
        value.serialize_prefix(&items, writer, state).await?;
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let (key_type, value_type) = match type_ {
            Type::Map(key, value) => (key, value),
            _ => unimplemented!(),
        };

        let mut total_keys = vec![];
        let mut total_values = vec![];

        for value in values {
            let (keys, values) = match value {
                Value::Map(keys, values) => (keys, values),
                _ => unimplemented!(),
            };
            assert_eq!(keys.len(), values.len());
            writer
                .write_u64_le((total_keys.len() + keys.len()) as u64)
                .await?;
            total_keys.extend(keys);
            total_values.extend(values);
        }

        key_type.serialize_column(total_keys, writer, state).await?;
        value_type
            .serialize_column(total_values, writer, state)
            .await?;
        Ok(())
    }
}
