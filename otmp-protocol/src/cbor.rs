use std::collections::BTreeMap;

#[path = "page_map.rs"]
mod page_map;
pub use page_map::*;

use crate::{ProtocolError, TypedScalar, UuidValue};

#[must_use]
pub fn encode_typed_scalar(value: &TypedScalar) -> Vec<u8> {
    let mut output = Vec::new();
    array_len(2, &mut output);
    match value {
        TypedScalar::Null => {
            unsigned(0, &mut output);
            output.push(0xf6);
        }
        TypedScalar::Boolean(value) => {
            unsigned(1, &mut output);
            output.push(if *value { 0xf5 } else { 0xf4 });
        }
        TypedScalar::Int32(value) => {
            unsigned(2, &mut output);
            integer(i64::from(*value), &mut output);
        }
        TypedScalar::Int64(value) => {
            unsigned(3, &mut output);
            integer(*value, &mut output);
        }
        TypedScalar::Float32(value) => {
            unsigned(4, &mut output);
            bytes(&canonical_f32(*value).to_be_bytes(), &mut output);
        }
        TypedScalar::Float64(value) => {
            unsigned(5, &mut output);
            bytes(&canonical_f64(*value).to_be_bytes(), &mut output);
        }
        TypedScalar::Decimal {
            precision,
            scale,
            unscaled,
        } => {
            unsigned(6, &mut output);
            array_len(3, &mut output);
            unsigned(u64::from(*precision), &mut output);
            unsigned(u64::from(*scale), &mut output);
            bytes(unscaled, &mut output);
        }
        TypedScalar::Date(value) => {
            unsigned(7, &mut output);
            integer(i64::from(*value), &mut output);
        }
        TypedScalar::TimeMicros(value) => {
            unsigned(8, &mut output);
            integer(*value, &mut output);
        }
        TypedScalar::TimestampMicros(value) => {
            unsigned(9, &mut output);
            integer(*value, &mut output);
        }
        TypedScalar::TimestamptzMicros(value) => {
            unsigned(10, &mut output);
            integer(*value, &mut output);
        }
        TypedScalar::String(value) => {
            unsigned(11, &mut output);
            text(value, &mut output);
        }
        TypedScalar::Binary(value) => {
            unsigned(12, &mut output);
            bytes(value, &mut output);
        }
        TypedScalar::Fixed(value) => {
            unsigned(13, &mut output);
            bytes(value, &mut output);
        }
        TypedScalar::Uuid(value) => {
            unsigned(14, &mut output);
            bytes(value.as_bytes(), &mut output);
        }
    }
    output
}

#[must_use]
pub fn encode_partition_tuple(values: &BTreeMap<u32, TypedScalar>) -> Vec<u8> {
    let mut output = Vec::new();
    map_len(values.len(), &mut output);
    for (field_id, value) in values {
        unsigned(u64::from(*field_id), &mut output);
        output.extend_from_slice(&encode_typed_scalar(value));
    }
    output
}

pub fn decode_typed_scalar(bytes: &[u8]) -> Result<TypedScalar, ProtocolError> {
    let mut decoder = Decoder::new(bytes);
    let value = decoder.typed_scalar()?;
    decoder.finish()?;
    Ok(value)
}

pub fn decode_partition_tuple(bytes: &[u8]) -> Result<BTreeMap<u32, TypedScalar>, ProtocolError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.length(5)?;
    let mut values = BTreeMap::new();
    let mut previous = None;
    for _ in 0..count {
        let raw = decoder.unsigned()?;
        let field_id = u32::try_from(raw).map_err(|_| invalid("partition field ID overflow"))?;
        if field_id == 0 || previous.is_some_and(|prior| prior >= field_id) {
            return Err(invalid("partition field IDs must be positive and sorted"));
        }
        previous = Some(field_id);
        values.insert(field_id, decoder.typed_scalar()?);
    }
    decoder.finish()?;
    Ok(values)
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn typed_scalar(&mut self) -> Result<TypedScalar, ProtocolError> {
        if self.length(4)? != 2 {
            return Err(invalid("typed scalar must be a two-element array"));
        }
        let type_code = self.unsigned()?;
        let scalar = match type_code {
            0 => {
                if self.byte()? != 0xf6 {
                    return Err(invalid("null scalar payload must be CBOR null"));
                }
                Ok(TypedScalar::Null)
            }
            1 => match self.byte()? {
                0xf4 => Ok(TypedScalar::Boolean(false)),
                0xf5 => Ok(TypedScalar::Boolean(true)),
                _ => Err(invalid("boolean scalar payload is invalid")),
            },
            2 => i32::try_from(self.integer()?)
                .map(TypedScalar::Int32)
                .map_err(|_| invalid("int32 scalar is out of range")),
            3 => self.integer().map(TypedScalar::Int64),
            4 => {
                let bytes: [u8; 4] = self
                    .byte_string()?
                    .try_into()
                    .map_err(|_| invalid("float32 payload must have four bytes"))?;
                let bits = u32::from_be_bytes(bytes);
                let value = f32::from_bits(bits);
                if value.is_nan() && bits != 0x7fc0_0000 {
                    return Err(invalid("float32 NaN is not canonical"));
                }
                Ok(TypedScalar::Float32(value))
            }
            5 => {
                let bytes: [u8; 8] = self
                    .byte_string()?
                    .try_into()
                    .map_err(|_| invalid("float64 payload must have eight bytes"))?;
                let bits = u64::from_be_bytes(bytes);
                let value = f64::from_bits(bits);
                if value.is_nan() && bits != 0x7ff8_0000_0000_0000 {
                    return Err(invalid("float64 NaN is not canonical"));
                }
                Ok(TypedScalar::Float64(value))
            }
            6 => {
                if self.length(4)? != 3 {
                    return Err(invalid("decimal payload must be a three-element array"));
                }
                let precision = u32::try_from(self.unsigned()?)
                    .map_err(|_| invalid("decimal precision overflow"))?;
                let scale = u32::try_from(self.unsigned()?)
                    .map_err(|_| invalid("decimal scale overflow"))?;
                let unscaled = self.byte_string()?.to_vec();
                if precision == 0 || scale > precision || !minimal_twos_complement(&unscaled) {
                    return Err(invalid("invalid decimal payload"));
                }
                Ok(TypedScalar::Decimal {
                    precision,
                    scale,
                    unscaled,
                })
            }
            7 => i32::try_from(self.integer()?)
                .map(TypedScalar::Date)
                .map_err(|_| invalid("date scalar is out of range")),
            8 => self.integer().map(TypedScalar::TimeMicros),
            9 => self.integer().map(TypedScalar::TimestampMicros),
            10 => self.integer().map(TypedScalar::TimestamptzMicros),
            11 => self.text().map(ToOwned::to_owned).map(TypedScalar::String),
            12 => self
                .byte_string()
                .map(ToOwned::to_owned)
                .map(TypedScalar::Binary),
            13 => self
                .byte_string()
                .map(ToOwned::to_owned)
                .map(TypedScalar::Fixed),
            14 => {
                let bytes: [u8; 16] = self
                    .byte_string()?
                    .try_into()
                    .map_err(|_| invalid("UUID payload must have 16 bytes"))?;
                Ok(TypedScalar::Uuid(UuidValue::from_bytes(bytes)))
            }
            _ => Err(invalid("unknown typed scalar code")),
        }?;
        scalar.validate()?;
        Ok(scalar)
    }

    fn unsigned(&mut self) -> Result<u64, ProtocolError> {
        let (major, value) = self.head()?;
        if major != 0 {
            return Err(invalid("expected unsigned integer"));
        }
        Ok(value)
    }

    fn integer(&mut self) -> Result<i64, ProtocolError> {
        let (major, value) = self.head()?;
        match major {
            0 => i64::try_from(value).map_err(|_| invalid("positive integer overflow")),
            1 => i64::try_from(value)
                .map(|value| -1 - value)
                .map_err(|_| invalid("negative integer overflow")),
            _ => Err(invalid("expected signed integer")),
        }
    }

    fn length(&mut self, expected_major: u8) -> Result<usize, ProtocolError> {
        let (major, value) = self.head()?;
        if major != expected_major {
            return Err(invalid("unexpected CBOR major type"));
        }
        usize::try_from(value).map_err(|_| invalid("CBOR length overflow"))
    }

    fn byte_string(&mut self) -> Result<&'a [u8], ProtocolError> {
        let length = self.length(2)?;
        self.take(length)
    }

    fn text(&mut self) -> Result<&'a str, ProtocolError> {
        let length = self.length(3)?;
        std::str::from_utf8(self.take(length)?).map_err(|_| invalid("CBOR text is not UTF-8"))
    }

    fn head(&mut self) -> Result<(u8, u64), ProtocolError> {
        let head = self.byte()?;
        let major = head >> 5;
        let additional = head & 0x1f;
        let value = match additional {
            0..=23 => u64::from(additional),
            24 => {
                let value = u64::from(self.byte()?);
                if value < 24 {
                    return Err(invalid("non-shortest CBOR integer"));
                }
                value
            }
            25 => {
                let value = u64::from(u16::from_be_bytes(
                    self.take(2)?.try_into().expect("length checked"),
                ));
                if value <= 0xff {
                    return Err(invalid("non-shortest CBOR integer"));
                }
                value
            }
            26 => {
                let value = u64::from(u32::from_be_bytes(
                    self.take(4)?.try_into().expect("length checked"),
                ));
                if value <= 0xffff {
                    return Err(invalid("non-shortest CBOR integer"));
                }
                value
            }
            27 => {
                let value = u64::from_be_bytes(self.take(8)?.try_into().expect("length checked"));
                if value <= 0xffff_ffff {
                    return Err(invalid("non-shortest CBOR integer"));
                }
                value
            }
            _ => return Err(invalid("indefinite or reserved CBOR form")),
        };
        Ok((major, value))
    }

    fn byte(&mut self) -> Result<u8, ProtocolError> {
        let byte = self
            .bytes
            .get(self.offset)
            .copied()
            .ok_or_else(|| invalid("unexpected end of CBOR"))?;
        self.offset += 1;
        Ok(byte)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| invalid("CBOR offset overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid("unexpected end of CBOR"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn finish(&self) -> Result<(), ProtocolError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid("trailing CBOR bytes"))
        }
    }
}

fn minimal_twos_complement(bytes: &[u8]) -> bool {
    match bytes {
        [] => false,
        [first, second, ..] if *first == 0 && second & 0x80 == 0 => false,
        [first, second, ..] if *first == 0xff && second & 0x80 != 0 => false,
        _ => true,
    }
}

fn invalid(message: &str) -> ProtocolError {
    ProtocolError::InvalidCbor(message.into())
}

fn canonical_f32(value: f32) -> u32 {
    if value.is_nan() {
        0x7fc0_0000
    } else {
        value.to_bits()
    }
}

fn canonical_f64(value: f64) -> u64 {
    if value.is_nan() {
        0x7ff8_0000_0000_0000
    } else {
        value.to_bits()
    }
}

fn integer(value: i64, output: &mut Vec<u8>) {
    if value >= 0 {
        unsigned(value.unsigned_abs(), output);
    } else {
        major(1, value.unsigned_abs() - 1, output);
    }
}

fn unsigned(value: u64, output: &mut Vec<u8>) {
    major(0, value, output);
}

fn bytes(value: &[u8], output: &mut Vec<u8>) {
    major(2, value.len() as u64, output);
    output.extend_from_slice(value);
}

fn text(value: &str, output: &mut Vec<u8>) {
    major(3, value.len() as u64, output);
    output.extend_from_slice(value.as_bytes());
}

fn array_len(value: usize, output: &mut Vec<u8>) {
    major(4, value as u64, output);
}

fn map_len(value: usize, output: &mut Vec<u8>) {
    major(5, value as u64, output);
}

fn major(kind: u8, value: u64, output: &mut Vec<u8>) {
    let prefix = kind << 5;
    match value {
        0..=23 => output.push(prefix | u8::try_from(value).expect("value is at most 23")),
        24..=0xff => output.extend_from_slice(&[
            prefix | 0x18,
            u8::try_from(value).expect("value is at most u8::MAX"),
        ]),
        0x100..=0xffff => {
            output.push(prefix | 0x19);
            output.extend_from_slice(
                &u16::try_from(value)
                    .expect("value is at most u16::MAX")
                    .to_be_bytes(),
            );
        }
        0x1_0000..=0xffff_ffff => {
            output.push(prefix | 0x1a);
            output.extend_from_slice(
                &u32::try_from(value)
                    .expect("value is at most u32::MAX")
                    .to_be_bytes(),
            );
        }
        _ => {
            output.push(prefix | 0x1b);
            output.extend_from_slice(&value.to_be_bytes());
        }
    }
}
