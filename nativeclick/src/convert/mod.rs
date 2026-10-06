use std::borrow::Cow;

use crate::{DynamicValue, NativeclickError, Result, Value, types::Type};

mod raw_row;
mod std_deserialize;
mod std_serialize;
pub use raw_row::*;
mod unit_value;
pub use unit_value::*;
mod vec_tuple;
pub use vec_tuple::*;
#[cfg(feature = "serde")]
mod json;
#[cfg(feature = "serde")]
pub use json::*;

/// A type that can be converted to a raw Clickhouse SQL value.
pub trait ToSql {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value>;
}

impl ToSql for Value {
    fn to_sql(self, _type_hint_: Option<&Type>) -> Result<Value> {
        Ok(self)
    }
}

pub fn unexpected_type(type_: &Type) -> NativeclickError {
    NativeclickError::DeserializeError(format!("unexpected type: {type_}"))
}

/// A type that can be converted from a raw Clickhouse SQL value.
pub trait FromSql: Sized {
    fn from_sql(type_: &Type, value: Value) -> Result<Self>;

    /// Converts a value as read from a column of `type_`. By default the wrappers listed in
    /// [`from_sql_resolved`] are looked through first; types that keep the raw value (like
    /// [`Value`]) override this.
    fn from_sql_column(type_: &Type, value: Value) -> Result<Self> {
        let (type_, value) = resolve(type_, value);
        Self::from_sql(&type_, value)
    }
}

/// The value exactly as read: a `Variant`/`Dynamic` row stays a [`Value::Dynamic`].
impl FromSql for Value {
    fn from_sql(_type_: &Type, value: Value) -> Result<Self> {
        Ok(value)
    }

    fn from_sql_column(_type_: &Type, value: Value) -> Result<Self> {
        Ok(value)
    }
}

/// Converts `value`, read from a column of type `type_`, into `T`.
///
/// Wrappers that do not change the value are looked through first: `LowCardinality`,
/// `SimpleAggregateFunction`, the actual type of a `Variant` / `Dynamic` / `Geometry` row,
/// `Interval*` (an `Int64`), `JSON` (its text), `LineString` / `MultiPoint` (a `Ring`),
/// `MultiLineString` (a `Polygon`) and named tuples. Use this instead of calling
/// [`FromSql::from_sql`] directly on a column type.
pub fn from_sql_resolved<T: FromSql>(type_: &Type, value: Value) -> Result<T> {
    T::from_sql_column(type_, value)
}

/// The type a value effectively has, see [`from_sql_resolved`].
pub(crate) fn resolve(type_: &Type, value: Value) -> (Cow<'_, Type>, Value) {
    match (type_, value) {
        (_, Value::Dynamic(dynamic)) => {
            let DynamicValue { type_, value } = *dynamic;
            let (resolved, value) = resolve(&type_, value);
            (Cow::Owned(resolved.into_owned()), value)
        }
        (Type::LowCardinality(inner) | Type::SimpleAggregateFunction(_, inner), value) => {
            resolve(inner, value)
        }
        (Type::Variant(_) | Type::Dynamic(_) | Type::Geometry, Value::Null) => (
            Cow::Owned(Type::Nullable(Box::new(Type::Nothing))),
            Value::Null,
        ),
        (Type::Interval(_), value) => (Cow::Owned(Type::Int64), value),
        (Type::Json(_), value) => (Cow::Owned(Type::String), value),
        (Type::LineString | Type::MultiPoint, value) => (Cow::Owned(Type::Ring), value),
        (Type::MultiLineString, value) => (Cow::Owned(Type::Polygon), value),
        (Type::NamedTuple(items), value) => (
            Cow::Owned(Type::Tuple(items.iter().map(|x| x.1.clone()).collect())),
            value,
        ),
        (type_, value) => (Cow::Borrowed(type_), value),
    }
}

/// A row that can be deserialized and serialized from a raw Clickhouse SQL value.
/// Generally this is not implemented manually, but using `nativeclick_derive::Row`.
/// I.e. `#[derive(nativeclick::Row)]`.
pub trait Row: Sized {
    /// If `Some`, `serialize_row` and `deserialize_row` MUST return this number of columns
    const COLUMN_COUNT: Option<usize>;

    /// If `Some`, `serialize_row` and `deserialize_row` MUST have these names
    fn column_names() -> Option<Vec<Cow<'static, str>>>;

    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self>;

    fn serialize_row(
        self,
        type_hints: &indexmap::IndexMap<String, Type>,
    ) -> Result<Vec<(Cow<'static, str>, Value)>>;
}
