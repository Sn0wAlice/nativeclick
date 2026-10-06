use std::{borrow::Cow, string::FromUtf8Error};

use thiserror::Error;

use crate::Type;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum NativeclickError {
    #[error("no rows received when expecting at least one row")]
    MissingRow,
    #[error("can't fetch the same column twice from RawRow")]
    DoubleFetch,
    #[error("column index was out of bounds or not present")]
    OutOfBounds,
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("duplicate field {0} in struct")]
    DuplicateField(&'static str),
    #[error("protocol error: {0}")]
    ProtocolError(String),
    #[error("type parse error: {0}")]
    TypeParseError(String),
    #[error("deserialize error: {0}")]
    DeserializeError(String),
    #[error("serialize error: {0}")]
    SerializeError(String),
    #[error("deserialize error for column {0}: {1}")]
    DeserializeErrorWithColumn(&'static str, String),
    #[error("server exception: {code} {name}: {message}\n{stack_trace}")]
    ServerException {
        code: i32,
        name: String,
        message: String,
        stack_trace: String,
    },
    #[error("unexpected type: {0}")]
    UnexpectedType(Type),
    #[error("unexpected type for column {0}: {1}")]
    UnexpectedTypeWithColumn(Cow<'static, str>, Type),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("utf-8 conversion error: {0}")]
    Utf8(#[from] FromUtf8Error),
    #[error("timed out")]
    Timeout,
}

impl NativeclickError {
    /// Adds the column name to deserialization errors raised while reading a block.
    pub(crate) fn with_column_name_owned(self, name: &str) -> Self {
        match self {
            NativeclickError::DeserializeError(e) | NativeclickError::TypeParseError(e) => {
                NativeclickError::DeserializeError(format!("column {name}: {e}"))
            }
            x => x,
        }
    }

    pub fn with_column_name(self, name: &'static str) -> Self {
        match self {
            NativeclickError::DeserializeError(e) => {
                NativeclickError::DeserializeErrorWithColumn(name, e)
            }
            NativeclickError::UnexpectedType(e) => {
                NativeclickError::UnexpectedTypeWithColumn(Cow::Borrowed(name), e)
            }
            x => x,
        }
    }
}

impl Clone for NativeclickError {
    fn clone(&self) -> Self {
        match self {
            Self::MissingRow => Self::MissingRow,
            Self::DoubleFetch => Self::DoubleFetch,
            Self::OutOfBounds => Self::OutOfBounds,
            Self::MissingField(arg0) => Self::MissingField(arg0),
            Self::DuplicateField(arg0) => Self::DuplicateField(arg0),
            Self::ProtocolError(arg0) => Self::ProtocolError(arg0.clone()),
            Self::TypeParseError(arg0) => Self::TypeParseError(arg0.clone()),
            Self::DeserializeError(arg0) => Self::DeserializeError(arg0.clone()),
            Self::SerializeError(arg0) => Self::SerializeError(arg0.clone()),
            Self::DeserializeErrorWithColumn(arg0, arg1) => {
                Self::DeserializeErrorWithColumn(arg0, arg1.clone())
            }
            Self::ServerException {
                code,
                name,
                message,
                stack_trace,
            } => Self::ServerException {
                code: *code,
                name: name.clone(),
                message: message.clone(),
                stack_trace: stack_trace.clone(),
            },
            Self::UnexpectedType(arg0) => Self::UnexpectedType(arg0.clone()),
            Self::UnexpectedTypeWithColumn(arg0, arg1) => {
                Self::UnexpectedTypeWithColumn(arg0.clone(), arg1.clone())
            }
            Self::Io(arg0) => Self::Io(std::io::Error::new(arg0.kind(), format!("{arg0}"))),
            Self::Utf8(arg0) => Self::Utf8(arg0.clone()),
            Self::Timeout => Self::Timeout,
        }
    }
}

pub type Result<T, E = NativeclickError> = std::result::Result<T, E>;
