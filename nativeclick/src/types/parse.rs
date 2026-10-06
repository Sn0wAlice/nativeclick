//! Parsing and printing of type names, as the server prints them (`IDataType::getName`).

use std::{fmt, str::FromStr};

use super::{GEOMETRY_VARIANTS, IntervalKind, Type};
use crate::{NativeclickError, Result, is_bfloat16_enabled};

fn error(message: impl Into<String>) -> NativeclickError {
    NativeclickError::TypeParseError(message.into())
}

/// Splits `input` at top-level commas, outside parentheses and quoted strings / identifiers
/// (`'...'`, `"..."`, `` `...` ``, with backslash escapes).
fn split_args(input: &str) -> Result<Vec<&str>> {
    let mut out = vec![];
    let mut depth = 0usize;
    let mut start = 0;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (i, c) in input.char_indices() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => quote = Some(c),
            '(' => depth += 1,
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| error(format!("mismatched parenthesis in '{input}'")))?
            }
            ',' if depth == 0 => {
                out.push(input[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    if quote.is_some() || depth != 0 {
        return Err(error(format!(
            "unterminated quote or parenthesis in '{input}'"
        )));
    }
    let last = input[start..].trim();
    if !last.is_empty() || !out.is_empty() {
        out.push(last);
    }
    Ok(out)
}

/// Reads a quoted string or identifier (`'...'` or `` `...` ``) at the start of `input`,
/// returning the unescaped content and the rest.
fn unquote(input: &str) -> Result<(String, &str)> {
    let mut chars = input.char_indices();
    let Some((_, q)) = chars.next() else {
        return Err(error("expected a quoted string, got nothing"));
    };
    let mut out = String::new();
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => {
                let (_, e) = chars
                    .next()
                    .ok_or_else(|| error(format!("dangling escape in '{input}'")))?;
                out.push(match e {
                    'b' => '\u{8}',
                    'f' => '\u{c}',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    '0' => '\0',
                    c => c,
                });
            }
            c if c == q => return Ok((out, &input[i + c.len_utf8()..])),
            c => out.push(c),
        }
    }
    Err(error(format!("unterminated quote in '{input}'")))
}

/// Writes `value` quoted with `q` and ClickHouse escapes (`writeAnyEscapedString`).
fn write_quoted(f: &mut fmt::Formatter<'_>, value: &str, q: char) -> fmt::Result {
    use fmt::Write;
    f.write_char(q)?;
    for c in value.chars() {
        match c {
            '\u{8}' => f.write_str("\\b")?,
            '\u{c}' => f.write_str("\\f")?,
            '\n' => f.write_str("\\n")?,
            '\r' => f.write_str("\\r")?,
            '\t' => f.write_str("\\t")?,
            '\0' => f.write_str("\\0")?,
            '\\' => f.write_str("\\\\")?,
            c if c == q => {
                f.write_char('\\')?;
                f.write_char(c)?;
            }
            c => f.write_char(c)?,
        }
    }
    f.write_char(q)
}

/// Tuple element names are printed bare when they are plain identifiers (`backQuoteIfNeed`).
fn write_name(f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
    const KEYWORDS: [&str; 13] = [
        "inf", "infinity", "nan", "true", "false", "distinct", "all", "some", "table", "select",
        "from", "top", "values",
    ];
    let mut chars = name.chars();
    let plain = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name != "null"
        && !KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(name));
    if plain {
        f.write_str(name)
    } else {
        write_quoted(f, name, '`')
    }
}

fn args_count(name: &str, args: &[&str], expected: usize) -> Result<()> {
    if args.len() != expected {
        return Err(error(format!(
            "bad arg count for {name}, expected {expected} and got {}",
            args.len()
        )));
    }
    Ok(())
}

fn parse_usize(name: &str, value: &str) -> Result<usize> {
    value
        .trim()
        .parse()
        .map_err(|_| error(format!("couldn't parse argument '{value}' of {name}")))
}

fn parse_timezone(name: &str, value: &str) -> Result<chrono_tz::Tz> {
    let (tz, rest) = unquote(value.trim())?;
    if !rest.trim().is_empty() {
        return Err(error(format!("malformed timezone for {name}: '{value}'")));
    }
    tz.parse()
        .map_err(|e| error(format!("failed to parse timezone for {name}: '{tz}': {e}")))
}

fn parse_enum<V: FromStr>(args: &[&str]) -> Result<Vec<(String, V)>> {
    args.iter()
        .map(|arg| {
            let (name, rest) = unquote(arg)?;
            let value = rest
                .trim()
                .strip_prefix('=')
                .ok_or_else(|| error(format!("enum variant missing '=': {arg}")))?
                .trim();
            let value = value
                .parse()
                .map_err(|_| error(format!("failed to parse enum variant value: {arg}")))?;
            Ok((name, value))
        })
        .collect()
}

/// `Tuple` elements: either all `Type` or all `name Type`.
fn parse_tuple(args: &[&str]) -> Result<Type> {
    let mut named = vec![];
    let mut unnamed = vec![];
    for arg in args {
        if arg.starts_with('`') {
            let (name, rest) = unquote(arg)?;
            named.push((name, rest.trim().parse()?));
            continue;
        }
        let end = arg
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
            .unwrap_or(arg.len());
        let rest = arg[end..].trim_start();
        if end > 0 && !rest.is_empty() && !rest.starts_with('(') {
            named.push((arg[..end].to_string(), rest.parse()?));
        } else {
            unnamed.push(arg.parse()?);
        }
    }
    match (named.is_empty(), unnamed.is_empty()) {
        (true, _) => Ok(Type::Tuple(unnamed)),
        (false, true) => Ok(Type::NamedTuple(named)),
        (false, false) => Err(error("tuple mixes named and unnamed elements")),
    }
}

impl FromStr for Type {
    type Err = NativeclickError;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let end = s
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(s.len());
        let (ident, rest) = s.split_at(end);
        if ident.is_empty() {
            return Err(error(format!("invalid empty identifier for type: '{s}'")));
        }
        let rest = rest.trim();
        if rest.is_empty() {
            return parse_simple(ident);
        }
        let inner = rest
            .strip_prefix('(')
            .and_then(|x| x.strip_suffix(')'))
            .ok_or_else(|| error(format!("malformed arguments to type '{s}'")))?;
        let args = split_args(inner)?;
        let one = |name: &str| -> Result<Type> {
            args_count(name, &args, 1)?;
            args[0].parse()
        };
        Ok(match ident {
            "Decimal" => {
                args_count(ident, &args, 2)?;
                let precision = parse_usize(ident, args[0])?;
                let scale = parse_usize(ident, args[1])?;
                match precision {
                    0..=9 => Type::Decimal32(scale),
                    10..=18 => Type::Decimal64(scale),
                    19..=38 => Type::Decimal128(scale),
                    39..=76 => Type::Decimal256(scale),
                    _ => return Err(error("bad decimal spec, cannot exceed 76 precision")),
                }
            }
            "Decimal32" | "Decimal64" | "Decimal128" | "Decimal256" => {
                args_count(ident, &args, 1)?;
                let scale = parse_usize(ident, args[0])?;
                match ident {
                    "Decimal32" => Type::Decimal32(scale),
                    "Decimal64" => Type::Decimal64(scale),
                    "Decimal128" => Type::Decimal128(scale),
                    _ => Type::Decimal256(scale),
                }
            }
            "FixedString" => {
                args_count(ident, &args, 1)?;
                Type::FixedString(parse_usize(ident, args[0])?)
            }
            "DateTime" => {
                args_count(ident, &args, 1)?;
                Type::DateTime(parse_timezone(ident, args[0])?)
            }
            "DateTime64" => match args.len() {
                1 => Type::DateTime64(parse_usize(ident, args[0])?, chrono_tz::UTC),
                2 => Type::DateTime64(
                    parse_usize(ident, args[0])?,
                    parse_timezone(ident, args[1])?,
                ),
                n => {
                    return Err(error(format!(
                        "bad arg count for DateTime64, expected 1 or 2 and got {n}"
                    )));
                }
            },
            "Time64" => {
                args_count(ident, &args, 1)?;
                Type::Time64(parse_usize(ident, args[0])?)
            }
            "Enum8" => Type::Enum8(parse_enum(&args)?),
            "Enum16" => Type::Enum16(parse_enum(&args)?),
            "LowCardinality" => Type::LowCardinality(Box::new(one(ident)?)),
            "Array" => Type::Array(Box::new(one(ident)?)),
            "Nullable" => Type::Nullable(Box::new(one(ident)?)),
            "Map" => {
                args_count(ident, &args, 2)?;
                Type::Map(Box::new(args[0].parse()?), Box::new(args[1].parse()?))
            }
            "Tuple" => parse_tuple(&args)?,
            "Variant" => Type::Variant(args.iter().map(|x| x.parse()).collect::<Result<_>>()?),
            "Dynamic" => {
                args_count(ident, &args, 1)?;
                let max_types = args[0]
                    .strip_prefix("max_types")
                    .and_then(|x| x.trim().strip_prefix('='))
                    .ok_or_else(|| error(format!("malformed Dynamic argument '{}'", args[0])))?;
                Type::Dynamic(Some(parse_usize(ident, max_types)?))
            }
            "JSON" => Type::Json(inner.trim().to_string()),
            "SimpleAggregateFunction" => {
                if args.len() < 2 {
                    return Err(error("SimpleAggregateFunction needs a function and a type"));
                }
                Type::SimpleAggregateFunction(
                    args[0].to_string(),
                    Box::new(args[1..].join(", ").parse()?),
                )
            }
            "AggregateFunction" => {
                return Err(error(format!(
                    "AggregateFunction columns cannot be read (their states have no generic \
                     format): select finalizeAggregation(column) instead. Type: '{s}'"
                )));
            }
            "Nested" => {
                return Err(error(
                    "Nested is sent as separate Array columns: use flatten_nested = 1",
                ));
            }
            _ => return Err(error(format!("invalid type with arguments: '{ident}'"))),
        })
    }
}

fn parse_simple(ident: &str) -> Result<Type> {
    if let Some(unit) = ident.strip_prefix("Interval") {
        return IntervalKind::ALL
            .into_iter()
            .find(|x| x.name() == unit)
            .map(Type::Interval)
            .ok_or_else(|| error(format!("invalid type name: '{ident}'")));
    }
    Ok(match ident {
        "Int8" => Type::Int8,
        "Int16" => Type::Int16,
        "Int32" => Type::Int32,
        "Int64" => Type::Int64,
        "Int128" => Type::Int128,
        "Int256" => Type::Int256,
        "UInt8" => Type::UInt8,
        "UInt16" => Type::UInt16,
        "UInt32" => Type::UInt32,
        "UInt64" => Type::UInt64,
        "UInt128" => Type::UInt128,
        "UInt256" => Type::UInt256,
        "Float32" => Type::Float32,
        "Float64" => Type::Float64,
        "BFloat16" => {
            if !is_bfloat16_enabled() {
                return Err(error(
                    "BFloat16 type is not supported. Enable the 'bfloat16' feature to use this type.",
                ));
            }
            Type::BFloat16
        }
        "Bool" => Type::Bool,
        "String" => Type::String,
        "UUID" => Type::Uuid,
        "Date" => Type::Date,
        "Date32" => Type::Date32,
        "DateTime" => Type::DateTime(chrono_tz::UTC),
        "Time" => Type::Time,
        "IPv4" => Type::Ipv4,
        "IPv6" => Type::Ipv6,
        "Nothing" => Type::Nothing,
        "Point" => Type::Point,
        "Ring" => Type::Ring,
        "LineString" => Type::LineString,
        "MultiPoint" => Type::MultiPoint,
        "MultiLineString" => Type::MultiLineString,
        "Polygon" => Type::Polygon,
        "MultiPolygon" => Type::MultiPolygon,
        "Geometry" => Type::Geometry,
        "Dynamic" => Type::Dynamic(None),
        "JSON" => Type::Json(String::new()),
        "Tuple" => Type::Tuple(vec![]),
        _ => return Err(error(format!("invalid type name: '{ident}'"))),
    })
}

fn write_list<T>(
    f: &mut fmt::Formatter<'_>,
    items: &[T],
    mut item: impl FnMut(&mut fmt::Formatter<'_>, &T) -> fmt::Result,
) -> fmt::Result {
    for (i, x) in items.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        item(f, x)?;
    }
    Ok(())
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Int8 => write!(f, "Int8"),
            Type::Int16 => write!(f, "Int16"),
            Type::Int32 => write!(f, "Int32"),
            Type::Int64 => write!(f, "Int64"),
            Type::Int128 => write!(f, "Int128"),
            Type::Int256 => write!(f, "Int256"),
            Type::UInt8 => write!(f, "UInt8"),
            Type::UInt16 => write!(f, "UInt16"),
            Type::UInt32 => write!(f, "UInt32"),
            Type::UInt64 => write!(f, "UInt64"),
            Type::UInt128 => write!(f, "UInt128"),
            Type::UInt256 => write!(f, "UInt256"),
            Type::Float32 => write!(f, "Float32"),
            Type::Float64 => write!(f, "Float64"),
            Type::BFloat16 => write!(f, "BFloat16"),
            Type::Decimal32(s) => write!(f, "Decimal32({s})"),
            Type::Decimal64(s) => write!(f, "Decimal64({s})"),
            Type::Decimal128(s) => write!(f, "Decimal128({s})"),
            Type::Decimal256(s) => write!(f, "Decimal256({s})"),
            Type::String => write!(f, "String"),
            Type::FixedString(s) => write!(f, "FixedString({s})"),
            Type::Uuid => write!(f, "UUID"),
            Type::Date => write!(f, "Date"),
            Type::Date32 => write!(f, "Date32"),
            Type::DateTime(tz) => write!(f, "DateTime('{tz}')"),
            Type::DateTime64(precision, tz) => write!(f, "DateTime64({precision}, '{tz}')"),
            Type::Time => write!(f, "Time"),
            Type::Time64(precision) => write!(f, "Time64({precision})"),
            Type::Interval(kind) => write!(f, "Interval{}", kind.name()),
            Type::Bool => write!(f, "Bool"),
            Type::Nothing => write!(f, "Nothing"),
            Type::Ipv4 => write!(f, "IPv4"),
            Type::Ipv6 => write!(f, "IPv6"),
            Type::Point => write!(f, "Point"),
            Type::Ring => write!(f, "Ring"),
            Type::LineString => write!(f, "LineString"),
            Type::MultiPoint => write!(f, "MultiPoint"),
            Type::MultiLineString => write!(f, "MultiLineString"),
            Type::Polygon => write!(f, "Polygon"),
            Type::MultiPolygon => write!(f, "MultiPolygon"),
            Type::Geometry => write!(f, "Geometry"),
            Type::Enum8(items) => {
                f.write_str("Enum8(")?;
                write_list(f, items, |f, (name, value)| {
                    write_quoted(f, name, '\'')?;
                    write!(f, " = {value}")
                })?;
                f.write_str(")")
            }
            Type::Enum16(items) => {
                f.write_str("Enum16(")?;
                write_list(f, items, |f, (name, value)| {
                    write_quoted(f, name, '\'')?;
                    write!(f, " = {value}")
                })?;
                f.write_str(")")
            }
            Type::LowCardinality(inner) => write!(f, "LowCardinality({inner})"),
            Type::Array(inner) => write!(f, "Array({inner})"),
            Type::Tuple(items) => {
                f.write_str("Tuple(")?;
                write_list(f, items, |f, x| write!(f, "{x}"))?;
                f.write_str(")")
            }
            Type::NamedTuple(items) => {
                f.write_str("Tuple(")?;
                write_list(f, items, |f, (name, type_)| {
                    write_name(f, name)?;
                    write!(f, " {type_}")
                })?;
                f.write_str(")")
            }
            Type::Nullable(inner) => write!(f, "Nullable({inner})"),
            Type::Map(key, value) => write!(f, "Map({key}, {value})"),
            Type::Variant(items) => {
                f.write_str("Variant(")?;
                write_list(f, items, |f, x| write!(f, "{x}"))?;
                f.write_str(")")
            }
            Type::Dynamic(None) => write!(f, "Dynamic"),
            Type::Dynamic(Some(max_types)) => write!(f, "Dynamic(max_types={max_types})"),
            Type::Json(args) if args.is_empty() => write!(f, "JSON"),
            Type::Json(args) => write!(f, "JSON({args})"),
            Type::SimpleAggregateFunction(function, inner) => {
                write!(f, "SimpleAggregateFunction({function}, {inner})")
            }
        }
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Type {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Type {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let name = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        name.parse().map_err(serde::de::Error::custom)
    }
}

impl Type {
    /// The variants of a `Variant` or `Geometry` column, in discriminator order.
    pub fn variants(&self) -> Option<&[Type]> {
        match self {
            Type::Variant(types) => Some(types),
            Type::Geometry => Some(&GEOMETRY_VARIANTS),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(name: &str) -> Type {
        let type_: Type = name.parse().unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(type_.to_string(), name, "printed back differently");
        type_
    }

    #[test]
    fn server_type_names_round_trip() {
        for name in [
            "Bool",
            "Date32",
            "Time",
            "Time64(3)",
            "IntervalSecond",
            "IntervalQuarter",
            "Nothing",
            "Nullable(Nothing)",
            "Array(Nothing)",
            "LineString",
            "MultiLineString",
            "MultiPoint",
            "Geometry",
            "Dynamic",
            "Dynamic(max_types=10)",
            "JSON",
            "JSON(max_dynamic_paths=10, a.b UInt32, SKIP c)",
            "Variant(Array(UInt8), String, UInt64)",
            "SimpleAggregateFunction(sum, UInt64)",
            "SimpleAggregateFunction(anyLast, Nullable(String))",
            "Tuple(a UInt8, b String)",
            "Tuple(`a b` UInt8, `select` String, `it\\`s` Array(Tuple(x Int8, y Nullable(String))))",
            "Tuple(UInt8, String)",
            "Map(String, Array(Tuple(a UInt8, b Enum8('x' = 1))))",
            "Enum8('a' = 1, 'b,c' = 2, 'd\\'e' = 3, '(f)' = 4)",
            "Enum16('a\\\\b' = -1, 'tab\\there' = 300)",
            "DateTime('Europe/Paris')",
            "DateTime64(3, 'UTC')",
            "LowCardinality(Nullable(String))",
        ] {
            round_trip(name);
        }
    }

    #[test]
    fn enum_names_are_unescaped() {
        let Type::Enum8(items) = round_trip("Enum8('a,b' = 1, 'it\\'s' = 2, 'x)y' = 3)") else {
            panic!()
        };
        assert_eq!(items[0], ("a,b".to_string(), 1));
        assert_eq!(items[1], ("it's".to_string(), 2));
        assert_eq!(items[2], ("x)y".to_string(), 3));
    }

    #[test]
    fn named_tuples_keep_names() {
        let Type::NamedTuple(items) = round_trip("Tuple(`a b` UInt8, c Array(String))") else {
            panic!()
        };
        assert_eq!(items[0], ("a b".to_string(), Type::UInt8));
        assert_eq!(
            items[1],
            ("c".to_string(), Type::Array(Box::new(Type::String)))
        );
    }

    #[test]
    fn simple_aggregate_function_with_parameters() {
        let type_: Type = "SimpleAggregateFunction(groupArrayArray(10), Array(String))"
            .parse()
            .unwrap();
        assert_eq!(
            type_,
            Type::SimpleAggregateFunction(
                "groupArrayArray(10)".to_string(),
                Box::new(Type::Array(Box::new(Type::String)))
            )
        );
    }

    #[test]
    fn unsupported_types_explain_themselves() {
        let error = "AggregateFunction(sum, UInt64)"
            .parse::<Type>()
            .unwrap_err();
        assert!(error.to_string().contains("finalizeAggregation"), "{error}");
        assert!("Enum8('a' = 1".parse::<Type>().is_err());
        assert!("Tuple(a UInt8, String)".parse::<Type>().is_err());
        assert!("Enum8(')(' = 1)".parse::<Type>().is_ok());
        assert!("Nope".parse::<Type>().is_err());
    }
}
