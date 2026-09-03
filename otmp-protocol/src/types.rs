use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{ProtocolError, TypedScalar};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LogicalType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal { precision: u32, scale: u32 },
    Date,
    TimeMicros,
    TimestampMicros,
    TimestamptzMicros,
    String,
    Binary,
    Fixed { length: u32 },
    Uuid,
    Struct { fields: Vec<Field> },
    List { element: Box<Field> },
    Map { key: Box<Field>, value: Box<Field> },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    pub field_id: u32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: LogicalType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_default: Option<TypedScalar>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_default: Option<TypedScalar>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    pub schema_id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_schema_id: Option<u32>,
    pub fields: Vec<Field>,
    #[serde(default)]
    pub identifier_field_ids: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

impl Schema {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_id == 0 {
            return Err(invalid("schema_id must be positive"));
        }
        let mut ids = BTreeSet::new();
        let mut paths = BTreeMap::new();
        validate_siblings(&self.fields, 1, false, &mut ids, &mut paths)?;
        let mut seen_identifiers = BTreeSet::new();
        for field_id in &self.identifier_field_ids {
            if !seen_identifiers.insert(*field_id) {
                return Err(invalid("duplicate identifier field ID"));
            }
            let Some((field, under_optional)) = paths.get(field_id) else {
                return Err(invalid("identifier field does not exist"));
            };
            if !field.required || *under_optional || !field.field_type.is_identifier_primitive() {
                return Err(invalid(
                    "identifier fields must be required non-floating primitives outside optional containers",
                ));
            }
        }
        Ok(())
    }
}

impl LogicalType {
    fn is_identifier_primitive(&self) -> bool {
        matches!(
            self,
            Self::Boolean
                | Self::Int32
                | Self::Int64
                | Self::Decimal { .. }
                | Self::Date
                | Self::TimeMicros
                | Self::TimestampMicros
                | Self::TimestamptzMicros
                | Self::String
                | Self::Binary
                | Self::Fixed { .. }
                | Self::Uuid
        )
    }

    #[must_use]
    pub fn accepts(&self, value: &TypedScalar) -> bool {
        matches!(value, TypedScalar::Null)
            || matches!(
                (self, value),
                (Self::Boolean, TypedScalar::Boolean(_))
                    | (Self::Int32, TypedScalar::Int32(_))
                    | (Self::Int64, TypedScalar::Int64(_))
                    | (Self::Float32, TypedScalar::Float32(_))
                    | (Self::Float64, TypedScalar::Float64(_))
                    | (Self::Date, TypedScalar::Date(_))
                    | (Self::TimeMicros, TypedScalar::TimeMicros(_))
                    | (Self::TimestampMicros, TypedScalar::TimestampMicros(_))
                    | (Self::TimestamptzMicros, TypedScalar::TimestamptzMicros(_))
                    | (Self::String, TypedScalar::String(_))
                    | (Self::Binary, TypedScalar::Binary(_))
                    | (Self::Uuid, TypedScalar::Uuid(_))
            )
            || matches!((self, value), (Self::Decimal { precision, scale }, TypedScalar::Decimal { precision: p, scale: s, .. }) if precision == p && scale == s)
            || matches!((self, value), (Self::Fixed { length }, TypedScalar::Fixed(bytes)) if *length as usize == bytes.len())
    }

    #[must_use]
    pub const fn is_float(&self) -> bool {
        matches!(self, Self::Float32 | Self::Float64)
    }

    #[must_use]
    pub const fn is_primitive(&self) -> bool {
        !matches!(
            self,
            Self::Struct { .. } | Self::List { .. } | Self::Map { .. }
        )
    }
}

fn validate_siblings<'a>(
    fields: &'a [Field],
    depth: usize,
    under_optional: bool,
    ids: &mut BTreeSet<u32>,
    paths: &mut BTreeMap<u32, (&'a Field, bool)>,
) -> Result<(), ProtocolError> {
    if depth > 64 {
        return Err(invalid("maximum nesting depth is 64"));
    }
    let mut names = BTreeSet::new();
    for field in fields {
        if field.field_id == 0 || !ids.insert(field.field_id) {
            return Err(invalid("field IDs must be unique and positive"));
        }
        if field.name.is_empty() || !names.insert(field.name.as_str()) {
            return Err(invalid("sibling field names must be unique and nonempty"));
        }
        validate_type(&field.field_type)?;
        for value in [&field.initial_default, &field.write_default]
            .into_iter()
            .flatten()
        {
            value.validate()?;
            if !field.field_type.accepts(value)
                || (field.required && matches!(value, TypedScalar::Null))
            {
                return Err(invalid("field default is not type compatible"));
            }
        }
        paths.insert(field.field_id, (field, under_optional));
        let nested_optional = under_optional || !field.required;
        match &field.field_type {
            LogicalType::Struct { fields } => {
                validate_siblings(fields, depth + 1, nested_optional, ids, paths)?;
            }
            LogicalType::List { element } => {
                validate_siblings(
                    std::slice::from_ref(element.as_ref()),
                    depth + 1,
                    nested_optional,
                    ids,
                    paths,
                )?;
            }
            LogicalType::Map { key, value } => {
                if !key.required || !key.field_type.is_primitive() {
                    return Err(invalid("map keys must be required primitives"));
                }
                validate_siblings(
                    std::slice::from_ref(key.as_ref()),
                    depth + 1,
                    nested_optional,
                    ids,
                    paths,
                )?;
                validate_siblings(
                    std::slice::from_ref(value.as_ref()),
                    depth + 1,
                    nested_optional,
                    ids,
                    paths,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_type(field_type: &LogicalType) -> Result<(), ProtocolError> {
    match field_type {
        LogicalType::Decimal { precision, scale } if *precision == 0 || scale > precision => Err(
            invalid("decimal requires precision > 0 and scale <= precision"),
        ),
        LogicalType::Fixed { length: 0 } => Err(invalid("fixed length must be positive")),
        _ => Ok(()),
    }
}

fn invalid(message: &str) -> ProtocolError {
    ProtocolError::InvalidSchema(message.to_owned())
}
