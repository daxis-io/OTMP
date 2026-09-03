use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("duplicate JSON key: {0}")]
    DuplicateJsonKey(String),
    #[error("floating-point JSON numbers are forbidden")]
    FloatingPointJson,
    #[error("JSON integer is outside the supported range")]
    IntegerOutOfRange,
    #[error("JSON is not in canonical form")]
    NonCanonicalJson,
    #[error("invalid JSON: {0}")]
    InvalidJson(String),
    #[error("invalid identifier: {0}")]
    InvalidId(String),
    #[error("invalid SHA-256 value: {0}")]
    InvalidHash(String),
    #[error("unsafe relative URI: {0}")]
    UnsafeRelativeUri(String),
    #[error("invalid feature set: {0}")]
    InvalidFeatureSet(String),
    #[error("invalid decimal integer string: {0}")]
    InvalidIntegerString(String),
    #[error("invalid logical schema: {0}")]
    InvalidSchema(String),
    #[error("invalid protocol object: {0}")]
    InvalidObject(String),
    #[error("invalid deterministic CBOR: {0}")]
    InvalidCbor(String),
    #[error("canonical encoding failed: {0}")]
    Encoding(String),
}

impl ProtocolError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::DuplicateJsonKey(_) => "OTMP_DUPLICATE_JSON_KEY",
            Self::FloatingPointJson => "OTMP_FLOATING_POINT_JSON",
            Self::IntegerOutOfRange | Self::InvalidIntegerString(_) => "OTMP_INVALID_INTEGER",
            Self::NonCanonicalJson => "OTMP_NONCANONICAL_JSON",
            Self::InvalidJson(_) => "OTMP_INVALID_JSON",
            Self::InvalidId(_) => "OTMP_INVALID_ID",
            Self::InvalidHash(_) => "OTMP_INVALID_HASH",
            Self::UnsafeRelativeUri(_) => "OTMP_UNSAFE_URI",
            Self::InvalidFeatureSet(_) => "OTMP_INVALID_FEATURE_SET",
            Self::InvalidSchema(_) => "OTMP_INVALID_SCHEMA",
            Self::InvalidObject(_) => "OTMP_INVALID_OBJECT",
            Self::InvalidCbor(_) => "OTMP_INVALID_CBOR",
            Self::Encoding(_) => "OTMP_ENCODING_ERROR",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorPayload {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default)]
    pub details: BTreeMap<String, String>,
}

impl From<&ProtocolError> for ErrorPayload {
    fn from(error: &ProtocolError) -> Self {
        Self {
            code: error.code().to_owned(),
            message: error.to_string(),
            retryable: false,
            details: BTreeMap::new(),
        }
    }
}
