use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use serde::de::{self, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256 as Sha256Hasher};

use crate::ProtocolError;

#[derive(Clone, Debug, PartialEq)]
pub enum CanonicalValue {
    Null,
    Bool(bool),
    Integer(i128),
    String(String),
    Array(Vec<Self>),
    Object(BTreeMap<String, Self>),
}

impl Serialize for CanonicalValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Integer(value) => serializer.serialize_i128(*value),
            Self::String(value) => serializer.serialize_str(value),
            Self::Array(value) => value.serialize(serializer),
            Self::Object(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for CanonicalValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::canonical_json::deserialize_value(deserializer)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id([u8; 16]);

impl Id {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn try_from_bytes(bytes: [u8; 16]) -> Result<Self, ProtocolError> {
        if bytes[6] >> 4 != 7 || bytes[8] >> 6 != 2 {
            return Err(ProtocolError::InvalidId(hex::encode(bytes)));
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl Display for Id {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        format_uuid(self.0, formatter)
    }
}

impl FromStr for Id {
    type Err = ProtocolError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let bytes = parse_uuid(input)?;
        Self::try_from_bytes(bytes).map_err(|_| ProtocolError::InvalidId(input.to_owned()))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UuidValue([u8; 16]);

impl UuidValue {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl Display for UuidValue {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        format_uuid(self.0, formatter)
    }
}

impl FromStr for UuidValue {
    type Err = ProtocolError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        parse_uuid(input).map(Self)
    }
}

impl Serialize for UuidValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for UuidValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(de::Error::custom)
    }
}

fn parse_uuid(input: &str) -> Result<[u8; 16], ProtocolError> {
    if input.len() != 36
        || input.as_bytes().iter().enumerate().any(|(index, byte)| {
            matches!(index, 8 | 13 | 18 | 23) && *byte != b'-'
                || !matches!(index, 8 | 13 | 18 | 23) && !matches!(byte, b'0'..=b'9' | b'a'..=b'f')
        })
    {
        return Err(ProtocolError::InvalidId(input.to_owned()));
    }
    let compact: String = input
        .chars()
        .filter(|character| *character != '-')
        .collect();
    hex::decode(compact)
        .map_err(|_| ProtocolError::InvalidId(input.to_owned()))?
        .try_into()
        .map_err(|_| ProtocolError::InvalidId(input.to_owned()))
}

fn format_uuid(bytes: [u8; 16], formatter: &mut Formatter<'_>) -> fmt::Result {
    let hex = hex::encode(bytes);
    write!(
        formatter,
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

impl Serialize for Id {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct IdVisitor;
        impl Visitor<'_> for IdVisitor {
            type Value = Id;
            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str("a canonical lower-case UUIDv7 string")
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Id::from_str(value).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(IdVisitor)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sha256([u8; 32]);

impl Sha256 {
    #[must_use]
    pub fn digest(bytes: impl AsRef<[u8]>) -> Self {
        let digest = Sha256Hasher::digest(bytes.as_ref());
        Self(digest.into())
    }

    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Display for Sha256 {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "sha256:{}", hex::encode(self.0))
    }
}

impl FromStr for Sha256 {
    type Err = ProtocolError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let Some(hex_part) = input.strip_prefix("sha256:") else {
            return Err(ProtocolError::InvalidHash(input.to_owned()));
        };
        if hex_part.len() != 64
            || !hex_part
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(ProtocolError::InvalidHash(input.to_owned()));
        }
        let bytes = hex::decode(hex_part)
            .map_err(|_| ProtocolError::InvalidHash(input.to_owned()))?
            .try_into()
            .map_err(|_| ProtocolError::InvalidHash(input.to_owned()))?;
        Ok(Self(bytes))
    }
}

impl Serialize for Sha256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Sha256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct JsonU64(pub u64);

impl Serialize for JsonU64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for JsonU64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_decimal(&value).map(Self).map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct JsonI64(pub i64);

impl Serialize for JsonI64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for JsonI64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value == "-0"
            || value.starts_with('+')
            || (value.starts_with('0') && value.len() > 1)
            || (value.starts_with("-0") && value.len() > 2)
        {
            return Err(de::Error::custom("noncanonical decimal integer"));
        }
        value.parse().map(Self).map_err(de::Error::custom)
    }
}

fn parse_decimal(value: &str) -> Result<u64, ProtocolError> {
    if value.is_empty()
        || value.starts_with('+')
        || (value.starts_with('0') && value.len() > 1)
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ProtocolError::InvalidIntegerString(value.to_owned()));
    }
    value
        .parse()
        .map_err(|_| ProtocolError::InvalidIntegerString(value.to_owned()))
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RelativeUri(String);

impl RelativeUri {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for RelativeUri {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for RelativeUri {
    type Err = ProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.starts_with('/')
            || value.ends_with('/')
            || value.contains('\\')
            || value.contains(':')
            || value
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
        {
            return Err(ProtocolError::UnsafeRelativeUri(value.to_owned()));
        }
        Ok(Self(value.to_owned()))
    }
}

impl Serialize for RelativeUri {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for RelativeUri {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct FeatureSet(Vec<String>);

impl FeatureSet {
    pub fn new(features: Vec<String>) -> Result<Self, ProtocolError> {
        let mut previous: Option<&str> = None;
        for feature in &features {
            if feature.is_empty()
                || !feature.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'-' | b'_')
                })
                || previous.is_some_and(|prior| prior >= feature.as_str())
            {
                return Err(ProtocolError::InvalidFeatureSet(feature.clone()));
            }
            previous = Some(feature);
        }
        Ok(Self(features))
    }

    #[must_use]
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }

    #[must_use]
    pub fn contains(&self, feature: &str) -> bool {
        self.0
            .binary_search_by(|item| item.as_str().cmp(feature))
            .is_ok()
    }

    pub fn require_supported(&self, supported: &BTreeSet<&str>) -> Result<(), ProtocolError> {
        if let Some(unknown) = self
            .0
            .iter()
            .find(|item| !supported.contains(item.as_str()))
        {
            return Err(ProtocolError::InvalidFeatureSet(format!(
                "unsupported required feature {unknown}"
            )));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for FeatureSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = Vec::<String>::deserialize(deserializer)?;
        Self::new(values).map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TypedScalar {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Decimal {
        precision: u32,
        scale: u32,
        unscaled: Vec<u8>,
    },
    Date(i32),
    TimeMicros(i64),
    TimestampMicros(i64),
    TimestamptzMicros(i64),
    String(String),
    Binary(Vec<u8>),
    Fixed(Vec<u8>),
    Uuid(UuidValue),
}

impl Serialize for TypedScalar {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let has_value = !matches!(self, Self::Null);
        let mut map = serializer.serialize_map(Some(if has_value { 2 } else { 1 }))?;
        let type_name = match self {
            Self::Null => "null",
            Self::Boolean(_) => "boolean",
            Self::Int32(_) => "int32",
            Self::Int64(_) => "int64",
            Self::Float32(_) => "float32",
            Self::Float64(_) => "float64",
            Self::Decimal { .. } => "decimal",
            Self::Date(_) => "date",
            Self::TimeMicros(_) => "time_micros",
            Self::TimestampMicros(_) => "timestamp_micros",
            Self::TimestamptzMicros(_) => "timestamptz_micros",
            Self::String(_) => "string",
            Self::Binary(_) => "binary",
            Self::Fixed(_) => "fixed",
            Self::Uuid(_) => "uuid",
        };
        map.serialize_entry("type", type_name)?;
        match self {
            Self::Null => {}
            Self::Boolean(value) => map.serialize_entry("value", value)?,
            Self::Int32(value) | Self::Date(value) => map.serialize_entry("value", value)?,
            Self::Int64(value)
            | Self::TimeMicros(value)
            | Self::TimestampMicros(value)
            | Self::TimestamptzMicros(value) => {
                map.serialize_entry("value", &value.to_string())?;
            }
            Self::Float32(value) => {
                let bits = if value.is_nan() {
                    0x7fc0_0000
                } else {
                    value.to_bits()
                };
                map.serialize_entry("value", &format!("0x{bits:08x}"))?;
            }
            Self::Float64(value) => {
                let bits = if value.is_nan() {
                    0x7ff8_0000_0000_0000
                } else {
                    value.to_bits()
                };
                map.serialize_entry("value", &format!("0x{bits:016x}"))?;
            }
            Self::Decimal {
                precision,
                scale,
                unscaled,
            } => {
                let value = BTreeMap::from([
                    ("precision", CanonicalValue::Integer(i128::from(*precision))),
                    ("scale", CanonicalValue::Integer(i128::from(*scale))),
                    (
                        "unscaled_hex",
                        CanonicalValue::String(hex::encode(unscaled)),
                    ),
                ]);
                map.serialize_entry("value", &value)?;
            }
            Self::String(value) => map.serialize_entry("value", value)?,
            Self::Binary(value) | Self::Fixed(value) => {
                map.serialize_entry("value", &hex::encode(value))?;
            }
            Self::Uuid(value) => map.serialize_entry("value", value)?,
        }
        map.end()
    }
}

impl TypedScalar {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Decimal {
                precision,
                scale,
                unscaled,
            } if *precision == 0 || scale > precision || !minimal_twos_complement(unscaled) => Err(
                ProtocolError::InvalidObject("invalid decimal scalar".into()),
            ),
            Self::TimeMicros(value) if !(0..86_400_000_000).contains(value) => Err(
                ProtocolError::InvalidObject("time_micros is outside one day".into()),
            ),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub fn partial_cmp_same_type(&self, other: &Self) -> Option<std::cmp::Ordering> {
        #[allow(clippy::match_same_arms)]
        match (self, other) {
            (Self::Boolean(left), Self::Boolean(right)) => left.partial_cmp(right),
            (Self::Int32(left), Self::Int32(right)) | (Self::Date(left), Self::Date(right)) => {
                left.partial_cmp(right)
            }
            (Self::Int64(left), Self::Int64(right))
            | (Self::TimeMicros(left), Self::TimeMicros(right))
            | (Self::TimestampMicros(left), Self::TimestampMicros(right))
            | (Self::TimestamptzMicros(left), Self::TimestamptzMicros(right)) => {
                left.partial_cmp(right)
            }
            (Self::Float32(left), Self::Float32(right)) => left.partial_cmp(right),
            (Self::Float64(left), Self::Float64(right)) => left.partial_cmp(right),
            (Self::String(left), Self::String(right)) => left.partial_cmp(right),
            (Self::Binary(left), Self::Binary(right)) | (Self::Fixed(left), Self::Fixed(right)) => {
                left.partial_cmp(right)
            }
            (Self::Uuid(left), Self::Uuid(right)) => left.partial_cmp(right),
            (
                Self::Decimal {
                    precision: left_precision,
                    scale: left_scale,
                    unscaled: left,
                },
                Self::Decimal {
                    precision: right_precision,
                    scale: right_scale,
                    unscaled: right,
                },
            ) if left_precision == right_precision && left_scale == right_scale => {
                signed_bytes_cmp(left, right)
            }
            _ => None,
        }
    }
}

impl<'de> Deserialize<'de> for TypedScalar {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let CanonicalValue::Object(mut map) = CanonicalValue::deserialize(deserializer)? else {
            return Err(de::Error::custom("typed scalar must be an object"));
        };
        let type_name = take_string(&mut map, "type").map_err(de::Error::custom)?;
        let value = map.remove("value");
        if !map.is_empty() {
            return Err(de::Error::custom("unknown typed scalar field"));
        }
        let scalar = match type_name.as_str() {
            "null" if value.is_none() => Self::Null,
            "boolean" => Self::Boolean(take_bool(value.as_ref()).map_err(de::Error::custom)?),
            "int32" => Self::Int32(take_i32(value.as_ref()).map_err(de::Error::custom)?),
            "int64" => Self::Int64(take_i64_string(value).map_err(de::Error::custom)?),
            "float32" => {
                let bits = take_hex_string(value, 8).map_err(de::Error::custom)?;
                Self::Float32(f32::from_bits(
                    u32::try_from(bits).map_err(de::Error::custom)?,
                ))
            }
            "float64" => {
                let bits = take_hex_string(value, 16).map_err(de::Error::custom)?;
                Self::Float64(f64::from_bits(bits))
            }
            "decimal" => {
                let Some(CanonicalValue::Object(mut value)) = value else {
                    return Err(de::Error::custom("decimal value must be an object"));
                };
                let precision =
                    take_u32_field(&mut value, "precision").map_err(de::Error::custom)?;
                let scale = take_u32_field(&mut value, "scale").map_err(de::Error::custom)?;
                let unscaled =
                    take_string(&mut value, "unscaled_hex").map_err(de::Error::custom)?;
                if !value.is_empty() {
                    return Err(de::Error::custom("unknown decimal field"));
                }
                Self::Decimal {
                    precision,
                    scale,
                    unscaled: hex::decode(unscaled).map_err(de::Error::custom)?,
                }
            }
            "date" => Self::Date(take_i32(value.as_ref()).map_err(de::Error::custom)?),
            "time_micros" => Self::TimeMicros(take_i64_string(value).map_err(de::Error::custom)?),
            "timestamp_micros" => {
                Self::TimestampMicros(take_i64_string(value).map_err(de::Error::custom)?)
            }
            "timestamptz_micros" => {
                Self::TimestamptzMicros(take_i64_string(value).map_err(de::Error::custom)?)
            }
            "string" => Self::String(take_value_string(value).map_err(de::Error::custom)?),
            "binary" => Self::Binary(
                hex::decode(take_value_string(value).map_err(de::Error::custom)?)
                    .map_err(de::Error::custom)?,
            ),
            "fixed" => Self::Fixed(
                hex::decode(take_value_string(value).map_err(de::Error::custom)?)
                    .map_err(de::Error::custom)?,
            ),
            "uuid" => Self::Uuid(
                UuidValue::from_str(&take_value_string(value).map_err(de::Error::custom)?)
                    .map_err(de::Error::custom)?,
            ),
            _ => return Err(de::Error::custom("invalid typed scalar shape")),
        };
        scalar.validate().map_err(de::Error::custom)?;
        Ok(scalar)
    }
}

fn take_string(map: &mut BTreeMap<String, CanonicalValue>, key: &str) -> Result<String, String> {
    take_value_string(map.remove(key))
}

fn take_value_string(value: Option<CanonicalValue>) -> Result<String, String> {
    match value {
        Some(CanonicalValue::String(value)) => Ok(value),
        _ => Err("expected string".into()),
    }
}

fn take_bool(value: Option<&CanonicalValue>) -> Result<bool, String> {
    match value {
        Some(CanonicalValue::Bool(value)) => Ok(*value),
        _ => Err("expected boolean".into()),
    }
}

fn take_i32(value: Option<&CanonicalValue>) -> Result<i32, String> {
    match value {
        Some(CanonicalValue::Integer(value)) => {
            i32::try_from(*value).map_err(|_| "int32 out of range".into())
        }
        _ => Err("expected integer".into()),
    }
}

fn take_i64_string(value: Option<CanonicalValue>) -> Result<i64, String> {
    match value {
        Some(CanonicalValue::String(value))
            if !(value == "-0"
                || value.starts_with('+')
                || value.starts_with('0') && value.len() > 1
                || value.starts_with("-0") && value.len() > 2) =>
        {
            value
                .parse::<i64>()
                .map_err(|_| "invalid int64 decimal string".into())
        }
        _ => Err("expected decimal string".into()),
    }
}

fn take_u32_field(map: &mut BTreeMap<String, CanonicalValue>, key: &str) -> Result<u32, String> {
    match map.remove(key) {
        Some(CanonicalValue::Integer(value)) => {
            u32::try_from(value).map_err(|_| format!("{key} out of range"))
        }
        _ => Err(format!("missing integer {key}")),
    }
}

fn take_hex_string(value: Option<CanonicalValue>, digits: usize) -> Result<u64, String> {
    let value = take_value_string(value)?;
    let Some(hex) = value.strip_prefix("0x") else {
        return Err("float bits require 0x prefix".into());
    };
    if hex.len() != digits
        || !hex
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err("float bits must be fixed-width lowercase hex".into());
    }
    u64::from_str_radix(hex, 16).map_err(|error| error.to_string())
}

fn minimal_twos_complement(bytes: &[u8]) -> bool {
    match bytes {
        [] => false,
        [first, second, ..] if *first == 0 && second & 0x80 == 0 => false,
        [first, second, ..] if *first == 0xff && second & 0x80 != 0 => false,
        _ => true,
    }
}

fn signed_bytes_cmp(left: &[u8], right: &[u8]) -> Option<std::cmp::Ordering> {
    let left_negative = left.first()? & 0x80 != 0;
    let right_negative = right.first()? & 0x80 != 0;
    Some(match (left_negative, right_negative) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => left.len().cmp(&right.len()).then_with(|| left.cmp(right)),
        (true, true) => right.len().cmp(&left.len()).then_with(|| left.cmp(right)),
    })
}
