use std::collections::BTreeMap;
use std::fmt::Formatter;

use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::{CanonicalValue, ProtocolError};

struct ValueSeed;

impl<'de> DeserializeSeed<'de> for ValueSeed {
    type Value = CanonicalValue;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_value(deserializer)
    }
}

pub(crate) fn deserialize_value<'de, D>(deserializer: D) -> Result<CanonicalValue, D::Error>
where
    D: Deserializer<'de>,
{
    struct CanonicalVisitor;

    impl<'de> Visitor<'de> for CanonicalVisitor {
        type Value = CanonicalValue;

        fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an OTMP canonical JSON value")
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(CanonicalValue::Null)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(CanonicalValue::Null)
        }

        fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
            Ok(CanonicalValue::Bool(value))
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
            Ok(CanonicalValue::Integer(i128::from(value)))
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
            Ok(CanonicalValue::Integer(i128::from(value)))
        }

        fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E> {
            Ok(CanonicalValue::Integer(value))
        }

        fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            i128::try_from(value)
                .map(CanonicalValue::Integer)
                .map_err(|_| E::custom("OTMP_INTEGER_OUT_OF_RANGE"))
        }

        fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom("OTMP_FLOATING_POINT_JSON"))
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
            Ok(CanonicalValue::String(value.to_owned()))
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
            Ok(CanonicalValue::String(value))
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut values = Vec::new();
            while let Some(value) = sequence.next_element_seed(ValueSeed)? {
                values.push(value);
            }
            Ok(CanonicalValue::Array(values))
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut values = BTreeMap::new();
            while let Some(key) = map.next_key::<String>()? {
                if values.contains_key(&key) {
                    return Err(de::Error::custom(format!("OTMP_DUPLICATE_KEY:{key}")));
                }
                let value = map.next_value_seed(ValueSeed)?;
                values.insert(key, value);
            }
            Ok(CanonicalValue::Object(values))
        }
    }

    deserializer.deserialize_any(CanonicalVisitor)
}

pub fn parse(bytes: &[u8]) -> Result<CanonicalValue, ProtocolError> {
    validate_number_tokens(bytes)?;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = ValueSeed
        .deserialize(&mut deserializer)
        .map_err(|error| map_json_error(&error))?;
    deserializer.end().map_err(|error| map_json_error(&error))?;
    Ok(value)
}

fn validate_number_tokens(bytes: &[u8]) -> Result<(), ProtocolError> {
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if byte == b'"' {
            in_string = true;
            index += 1;
            continue;
        }
        if byte == b'-' || byte.is_ascii_digit() {
            let start = index;
            index += 1;
            while index < bytes.len()
                && matches!(bytes[index], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
            {
                index += 1;
            }
            let token = std::str::from_utf8(&bytes[start..index])
                .map_err(|error| ProtocolError::InvalidJson(error.to_string()))?;
            if token.contains(['.', 'e', 'E']) {
                return Err(ProtocolError::FloatingPointJson);
            }
            let in_range = if token.starts_with('-') {
                token.parse::<i64>().is_ok()
            } else {
                token.parse::<u64>().is_ok()
            };
            if !in_range {
                return Err(ProtocolError::IntegerOutOfRange);
            }
            continue;
        }
        index += 1;
    }
    Ok(())
}

pub fn parse_canonical(bytes: &[u8]) -> Result<CanonicalValue, ProtocolError> {
    let value = parse(bytes)?;
    if encode(&value)? != bytes {
        return Err(ProtocolError::NonCanonicalJson);
    }
    Ok(value)
}

pub fn from_slice<T>(bytes: &[u8]) -> Result<T, ProtocolError>
where
    T: for<'de> Deserialize<'de>,
{
    parse(bytes)?;
    serde_json::from_slice(bytes).map_err(|error| map_json_error(&error))
}

pub fn from_slice_canonical<T>(bytes: &[u8]) -> Result<T, ProtocolError>
where
    T: for<'de> Deserialize<'de>,
{
    parse_canonical(bytes)?;
    serde_json::from_slice(bytes).map_err(|error| map_json_error(&error))
}

pub fn encode(value: &CanonicalValue) -> Result<Vec<u8>, ProtocolError> {
    let mut output = Vec::new();
    encode_into(value, &mut output)?;
    Ok(output)
}

pub fn to_value<T: Serialize>(value: &T) -> Result<CanonicalValue, ProtocolError> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| ProtocolError::Encoding(error.to_string()))?;
    parse(&bytes)
}

pub fn to_vec<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    encode(&to_value(value)?)
}

fn encode_into(value: &CanonicalValue, output: &mut Vec<u8>) -> Result<(), ProtocolError> {
    match value {
        CanonicalValue::Null => output.extend_from_slice(b"null"),
        CanonicalValue::Bool(true) => output.extend_from_slice(b"true"),
        CanonicalValue::Bool(false) => output.extend_from_slice(b"false"),
        CanonicalValue::Integer(integer) => {
            output.extend_from_slice(integer.to_string().as_bytes());
        }
        CanonicalValue::String(string) => output.extend_from_slice(
            serde_json::to_string(string)
                .map_err(|error| ProtocolError::Encoding(error.to_string()))?
                .as_bytes(),
        ),
        CanonicalValue::Array(values) => {
            output.push(b'[');
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                encode_into(item, output)?;
            }
            output.push(b']');
        }
        CanonicalValue::Object(values) => {
            output.push(b'{');
            for (index, (key, item)) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                output.extend_from_slice(
                    serde_json::to_string(key)
                        .map_err(|error| ProtocolError::Encoding(error.to_string()))?
                        .as_bytes(),
                );
                output.push(b':');
                encode_into(item, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn map_json_error(error: &serde_json::Error) -> ProtocolError {
    let message = error.to_string();
    if let Some(position) = message.find("OTMP_DUPLICATE_KEY:") {
        let key = message[position + "OTMP_DUPLICATE_KEY:".len()..]
            .split(" at line")
            .next()
            .unwrap_or_default();
        ProtocolError::DuplicateJsonKey(key.to_owned())
    } else if message.contains("OTMP_FLOATING_POINT_JSON") {
        ProtocolError::FloatingPointJson
    } else if message.contains("OTMP_INTEGER_OUT_OF_RANGE")
        || message.contains("number out of range")
    {
        ProtocolError::IntegerOutOfRange
    } else {
        ProtocolError::InvalidJson(message)
    }
}
