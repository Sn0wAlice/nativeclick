//! Custom serialization kinds (revision 54454+): a column may be sent SPARSE (only its
//! non-default values, revision 54465+) or REPLICATED (indexes into fewer distinct rows,
//! revision 54482+), announced by a kind tree after the column's type. Only tuples carry kinds for
//! their elements.

use futures_util::FutureExt;
use std::future::Future;
use tokio::io::AsyncReadExt;

use super::{DeserializerState, Type, deserialize::tuple::zip_tuples};
use crate::{NativeclickError, Result, io::ClickhouseRead, values::Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Default,
    Sparse,
    /// Server-to-server only (parallel block marshalling).
    Detached,
    Replicated,
}

/// Kinds of a column: its stack (innermost first, always starting with `Default`) and, for tuples,
/// the trees of its elements.
#[derive(Clone, Debug, Default)]
pub(crate) struct KindTree {
    stack: Vec<Kind>,
    elements: Vec<KindTree>,
}

const END_OF_GRANULE_FLAG: u64 = 1 << 62;

fn error(message: impl Into<String>) -> NativeclickError {
    NativeclickError::DeserializeError(message.into())
}

/// Element types that carry their own kind tree.
fn kind_children(type_: &Type) -> Vec<&Type> {
    static FLOAT64: Type = Type::Float64;
    match type_ {
        Type::Point => vec![&FLOAT64, &FLOAT64],
        Type::Tuple(_) | Type::NamedTuple(_) => type_.tuple_types().unwrap_or_default(),
        Type::SimpleAggregateFunction(_, inner) => kind_children(inner),
        _ => vec![],
    }
}

async fn read_stack<R: ClickhouseRead>(reader: &mut R) -> Result<Vec<Kind>> {
    use Kind::*;
    Ok(match reader.read_u8().await? {
        0 => vec![Default],
        1 => vec![Default, Sparse],
        2 => vec![Default, Detached],
        3 => vec![Default, Sparse, Detached],
        4 => vec![Default, Replicated],
        5 => {
            let count = reader.read_var_uint().await?;
            if count == 0 || count > 4 {
                return Err(error(format!("bad serialization kind count {count}")));
            }
            let mut stack = vec![];
            for _ in 0..count {
                stack.push(match reader.read_u8().await? {
                    0 => Default,
                    1 => Sparse,
                    2 => Detached,
                    3 => Replicated,
                    x => return Err(error(format!("unknown serialization kind {x}"))),
                });
            }
            if stack[0] != Default {
                return Err(error("serialization kinds must start with the default one"));
            }
            stack
        }
        x => return Err(error(format!("unknown serialization kind stack {x}"))),
    })
}

impl KindTree {
    pub(crate) fn read<'a, R: ClickhouseRead>(
        type_: &'a Type,
        reader: &'a mut R,
    ) -> impl Future<Output = Result<KindTree>> + Send + 'a {
        async move {
            let stack = read_stack(reader).await?;
            let mut elements = vec![];
            for child in kind_children(type_) {
                elements.push(KindTree::read(child, reader).await?);
            }
            Ok(KindTree { stack, elements })
        }
        .boxed()
    }

    fn outer(&self) -> Kind {
        self.stack.last().copied().unwrap_or(Kind::Default)
    }

    fn without_outer(&self) -> KindTree {
        let mut stack = self.stack.clone();
        stack.pop();
        KindTree {
            stack,
            elements: self.elements.clone(),
        }
    }
}

/// Rows of the non-default values of a sparse column: groups of default rows, each followed by
/// one value, then the trailing defaults flagged with `END_OF_GRANULE_FLAG`.
async fn read_sparse_offsets<R: ClickhouseRead>(reader: &mut R, rows: usize) -> Result<Vec<usize>> {
    let mut positions = vec![];
    let mut position = 0usize;
    loop {
        let group = reader.read_var_uint().await?;
        let end = group & END_OF_GRANULE_FLAG != 0;
        let defaults = usize::try_from(group & !END_OF_GRANULE_FLAG)
            .map_err(|_| error("sparse offsets out of range"))?;
        position = position
            .checked_add(defaults)
            .filter(|x| *x <= rows)
            .ok_or_else(|| error("sparse offsets exceed the block"))?;
        if end {
            break;
        }
        if position >= rows {
            return Err(error("sparse offsets exceed the block"));
        }
        positions.push(position);
        position += 1;
    }
    if position != rows {
        return Err(error(format!(
            "sparse offsets cover {position} rows, not {rows}"
        )));
    }
    Ok(positions)
}

async fn read_replicated_indexes<R: ClickhouseRead>(
    reader: &mut R,
    rows: usize,
) -> Result<Vec<u64>> {
    let count = reader.read_var_uint().await?;
    if count != rows as u64 {
        return Err(error(format!(
            "replicated column has {count} rows, not {rows}"
        )));
    }
    let width = reader.read_u8().await?;
    let mut indexes = Vec::with_capacity(rows.min(1 << 16));
    for _ in 0..rows {
        indexes.push(match width {
            1 => reader.read_u8().await? as u64,
            2 => reader.read_u16_le().await? as u64,
            4 => reader.read_u32_le().await? as u64,
            8 => reader.read_u64_le().await?,
            x => return Err(error(format!("bad replicated index width {x}"))),
        });
    }
    Ok(indexes)
}

impl Type {
    /// Reads a column sent with the custom serialization `kinds`.
    pub(crate) fn deserialize_column_kinds<'a, R: ClickhouseRead>(
        &'a self,
        reader: &'a mut R,
        rows: usize,
        state: &'a mut DeserializerState,
        kinds: &'a KindTree,
    ) -> impl Future<Output = Result<Vec<Value>>> + Send + 'a {
        async move {
            match kinds.outer() {
                Kind::Detached => Err(error("detached columns are only sent between servers")),
                Kind::Replicated => {
                    let indexes = read_replicated_indexes(reader, rows).await?;
                    let nested_rows = reader.read_var_uint().await?;
                    if nested_rows > crate::protocol::MAX_STRING_SIZE as u64 {
                        return Err(error(format!("too many replicated rows: {nested_rows}")));
                    }
                    let nested = self
                        .deserialize_column_kinds(
                            reader,
                            nested_rows as usize,
                            state,
                            &kinds.without_outer(),
                        )
                        .await?;
                    indexes
                        .into_iter()
                        .map(|i| {
                            nested
                                .get(i as usize)
                                .cloned()
                                .ok_or_else(|| error(format!("replicated index {i} out of range")))
                        })
                        .collect()
                }
                Kind::Sparse => {
                    let positions = read_sparse_offsets(reader, rows).await?;
                    // Sparse Nullable: only the non-null values, without their null map.
                    let (values_type, default) = match self {
                        Type::Nullable(inner) => (&**inner, Value::Null),
                        type_ => (type_, type_.default_value()),
                    };
                    let values = if positions.is_empty() {
                        vec![]
                    } else {
                        values_type
                            .deserialize_column_kinds(
                                reader,
                                positions.len(),
                                state,
                                &kinds.without_outer(),
                            )
                            .await?
                    };
                    let mut out = vec![default; rows];
                    for (position, value) in positions.into_iter().zip(values) {
                        out[position] = value;
                    }
                    Ok(out)
                }
                Kind::Default if !kinds.elements.is_empty() => {
                    let children = kind_children(self);
                    if children.len() != kinds.elements.len() {
                        return Err(error(format!("kinds do not match the elements of {self}")));
                    }
                    let mut columns = vec![];
                    for (child, child_kinds) in children.into_iter().zip(&kinds.elements) {
                        columns.push(
                            child
                                .deserialize_column_kinds(reader, rows, state, child_kinds)
                                .await?,
                        );
                    }
                    let tuples = zip_tuples(columns, rows);
                    Ok(match self {
                        Type::Point => tuples
                            .into_iter()
                            .map(|x| match x {
                                Value::Tuple(xy) => match xy[..] {
                                    [Value::Float64(x), Value::Float64(y)] => {
                                        Value::Point(crate::Point([x, y]))
                                    }
                                    _ => Value::Null,
                                },
                                x => x,
                            })
                            .collect(),
                        _ => tuples,
                    })
                }
                Kind::Default => self.deserialize_column(reader, rows, state).await,
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read(type_: &str, revision: u64, bytes: &[u8], rows: usize) -> Result<Vec<Value>> {
        let type_: Type = type_.parse().unwrap();
        let mut reader = bytes;
        let kinds = if reader.read_u8().await? != 0 {
            KindTree::read(&type_, &mut reader).await?
        } else {
            KindTree::default()
        };
        let mut state = DeserializerState::new(revision);
        type_.deserialize_prefix(&mut reader, &mut state).await?;
        let values = type_
            .deserialize_column_kinds(&mut reader, rows, &mut state, &kinds)
            .await?;
        assert!(reader.is_empty(), "{} bytes left", reader.len());
        Ok(values)
    }

    const END: [u8; 9] = [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x40];

    /// Worked example from the spec: UInt32 [0,0,5,0,7,0,0].
    #[tokio::test]
    async fn sparse_numbers() {
        let mut bytes = vec![0x01, 0x01, 0x02, 0x01, 0x82];
        bytes.extend_from_slice(&END[1..]);
        bytes.extend_from_slice(&[5, 0, 0, 0, 7, 0, 0, 0]);
        let values = read("UInt32", 54492, &bytes, 7).await.unwrap();
        let expected = [0, 0, 5, 0, 7, 0, 0].map(Value::UInt32);
        assert_eq!(values, expected);
    }

    /// Spec: Nullable(String) [NULL,'x',NULL,'',NULL] sparse, size-stream strings.
    #[tokio::test]
    async fn sparse_nullable_strings() {
        let mut bytes = vec![0x01, 0x01, 0x01, 0x01, 0x81];
        bytes.extend_from_slice(&END[1..]);
        bytes.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, b'x']);
        let values = read("Nullable(String)", 54492, &bytes, 5).await.unwrap();
        assert_eq!(
            values,
            [
                Value::Null,
                Value::string("x"),
                Value::Null,
                Value::string(""),
                Value::Null
            ]
        );
    }

    #[tokio::test]
    async fn sparse_all_default() {
        let mut bytes = vec![0x01, 0x01, 0x83];
        bytes.extend_from_slice(&END[1..]);
        let values = read("String", 54492, &bytes, 3).await.unwrap();
        assert_eq!(
            values,
            [Value::string(""), Value::string(""), Value::string("")]
        );
    }

    /// Spec: replicated String ['a','a','b','b'].
    #[tokio::test]
    async fn replicated_strings() {
        let bytes = [
            0x01, 0x04, 0x04, 0x01, 0, 0, 1, 1, 0x02, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0,
            0, b'a', b'b',
        ];
        let values = read("String", 54492, &bytes, 4).await.unwrap();
        assert_eq!(values, ["a", "a", "b", "b"].map(Value::string));
    }

    /// Spec: Tuple(a UInt8, b String) with b sparse.
    #[tokio::test]
    async fn sparse_tuple_element() {
        let mut bytes = vec![0x01, 0x00, 0x00, 0x01];
        bytes.extend_from_slice(&[1, 2]); // a = [1, 2]
        bytes.extend_from_slice(&[0x01]); // b: one default, then a value
        bytes.extend_from_slice(&END);
        bytes.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0, b'z']);
        let values = read("Tuple(a UInt8, b String)", 54492, &bytes, 2)
            .await
            .unwrap();
        assert_eq!(
            values,
            [
                Value::Tuple(vec![Value::UInt8(1), Value::string("")]),
                Value::Tuple(vec![Value::UInt8(2), Value::string("z")]),
            ]
        );
    }

    #[tokio::test]
    async fn malformed_kinds_are_errors_not_panics() {
        // Offsets past the end of the block.
        let mut bytes = vec![0x01, 0x01, 0x09];
        bytes.extend_from_slice(&END);
        assert!(read("UInt8", 54492, &bytes, 3).await.is_err());
        // Replicated index out of range.
        let bytes = [0x01, 0x04, 0x01, 0x01, 0x05, 0x01, 7];
        assert!(read("UInt8", 54492, &bytes, 1).await.is_err());
        // Unknown kind.
        assert!(read("UInt8", 54492, &[0x01, 0x09], 1).await.is_err());
        // Detached.
        assert!(read("UInt8", 54492, &[0x01, 0x02, 0], 1).await.is_err());
    }
}
