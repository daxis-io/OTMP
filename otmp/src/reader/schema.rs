use std::collections::BTreeMap;
use std::sync::Arc;

use otmp_protocol::{Field, LogicalType, Schema, TypedScalar};

use crate::reader::ReadContext;
use crate::reader::cache::Reservation;
use crate::reader_engine::Engine;
use crate::{ObjectStore, RuntimeError};

const SCHEMA_QUERY_BUDGET: usize = 4 * 1024 * 1024;

struct Row {
    parent: Option<u32>,
    ordinal: u32,
    field: Field,
}

#[allow(
    clippy::too_many_lines,
    reason = "normalized and recursive schema validation share one bounded query transaction"
)]
pub(crate) async fn load_schema<S: ObjectStore>(
    context: &ReadContext<S>,
    engine: &Engine,
    schema_id: u32,
) -> Result<(Arc<Schema>, Reservation), RuntimeError> {
    if schema_id == 0 {
        return Err(RuntimeError::Corrupt("schema ID must be positive".into()));
    }
    // Keep the decoded schema and both bounded result sets charged while the
    // caller retains the returned reservation.
    let transient = context.reserve_bytes(SCHEMA_QUERY_BUDGET)?;
    let id = turso_core::Value::Numeric(turso_core::Numeric::Integer(i64::from(schema_id)));
    let header = engine
        .query(
            "SELECT parent_schema_id,doc FROM otmp_schemas WHERE schema_id=?1",
            vec![id.clone()],
            1,
            SCHEMA_QUERY_BUDGET,
        )
        .await?;
    let [header] = header.as_slice() else {
        return Err(RuntimeError::Corrupt("selected schema is missing".into()));
    };
    if header.len() != 2 {
        return Err(RuntimeError::Corrupt("invalid schema header row".into()));
    }
    let parent_schema_id = optional_u32(&header[0], "parent schema ID")?;
    let doc = optional_text(&header[1], "schema doc")?;
    let fields = engine
        .query(
            "SELECT field_id,parent_field_id,name,ordinal,required,type_json,doc,initial_default_json,write_default_json FROM otmp_fields WHERE schema_id=?1 ORDER BY field_id",
            vec![id.clone()],
            4096,
            SCHEMA_QUERY_BUDGET,
        )
        .await?;
    let identifiers = engine
        .query(
            "SELECT field_id,ordinal FROM otmp_identifier_fields WHERE schema_id=?1 ORDER BY ordinal",
            vec![id],
            4096,
            SCHEMA_QUERY_BUDGET,
        )
        .await?;
    let mut rows = BTreeMap::new();
    for values in fields {
        if values.len() != 9 {
            return Err(RuntimeError::Corrupt(
                "invalid normalized schema field row".into(),
            ));
        }
        let field_id = required_u32(&values[0], "field ID")?;
        let row = Row {
            parent: optional_u32(&values[1], "parent field ID")?,
            ordinal: required_u32(&values[3], "field ordinal")?,
            field: Field {
                field_id,
                name: required_text(&values[2], "field name")?,
                required: required_bool(&values[4])?,
                field_type: otmp_protocol::canonical_json::from_slice_canonical(
                    required_text(&values[5], "field type")?.as_bytes(),
                )?,
                doc: optional_text(&values[6], "field doc")?,
                initial_default: optional_scalar(&values[7], "initial default")?,
                write_default: optional_scalar(&values[8], "write default")?,
            },
        };
        if rows.insert(field_id, row).is_some() {
            return Err(RuntimeError::Corrupt(
                "duplicate normalized field ID".into(),
            ));
        }
    }
    let mut roots: Vec<_> = rows.values().filter(|row| row.parent.is_none()).collect();
    roots.sort_by_key(|row| row.ordinal);
    check_ordinals(roots.iter().map(|row| row.ordinal), "root field")?;
    for field in rows.values() {
        check_ordinals(
            rows.values()
                .filter(|row| row.parent == Some(field.field.field_id))
                .map(|row| row.ordinal),
            "child field",
        )?;
    }
    check_ordinals(
        identifiers
            .iter()
            .map(|row| match row.as_slice() {
                [_, ordinal] => required_u32(ordinal, "identifier ordinal"),
                _ => Err(RuntimeError::Corrupt("invalid identifier row".into())),
            })
            .collect::<Result<Vec<_>, _>>()?,
        "identifier",
    )?;
    let schema = Schema {
        schema_id,
        parent_schema_id,
        doc,
        fields: roots.iter().map(|row| row.field.clone()).collect(),
        identifier_field_ids: identifiers
            .iter()
            .map(|row| match row.as_slice() {
                [value, _] => required_u32(value, "identifier field ID"),
                _ => Err(RuntimeError::Corrupt("invalid identifier row".into())),
            })
            .collect::<Result<_, _>>()?,
    };
    for root in &schema.fields {
        validate_tree(&rows, root, None)?;
    }
    if rows.len() != count_tree(&schema.fields) {
        return Err(RuntimeError::Corrupt(
            "orphan normalized schema field".into(),
        ));
    }
    schema.validate()?;
    let retained_bytes = otmp_protocol::canonical_json::to_vec(&schema)?
        .len()
        .saturating_mul(4)
        .saturating_add(1024);
    drop(transient);
    let reservation = context.reserve_bytes(retained_bytes)?;
    Ok((Arc::new(schema), reservation))
}

fn validate_tree(
    rows: &BTreeMap<u32, Row>,
    field: &Field,
    expected_parent: Option<u32>,
) -> Result<(), RuntimeError> {
    let row = rows.get(&field.field_id).ok_or_else(|| {
        RuntimeError::Corrupt("nested field is absent from normalized rows".into())
    })?;
    if row.parent != expected_parent || row.field != *field {
        return Err(RuntimeError::Corrupt(
            "normalized field disagrees with recursive schema".into(),
        ));
    }
    let children: Vec<&Field> = match &field.field_type {
        LogicalType::Struct { fields } => fields.iter().collect(),
        LogicalType::List { element } => vec![element],
        LogicalType::Map { key, value } => vec![key, value],
        _ => vec![],
    };
    let mut normalized: Vec<&Row> = rows
        .values()
        .filter(|row| row.parent == Some(field.field_id))
        .collect();
    normalized.sort_by_key(|row| row.ordinal);
    if normalized.len() != children.len()
        || normalized
            .iter()
            .map(|r| r.field.field_id)
            .ne(children.iter().map(|f| f.field_id))
    {
        return Err(RuntimeError::Corrupt(
            "normalized child ordering differs from recursive schema".into(),
        ));
    }
    for child in children {
        validate_tree(rows, child, Some(field.field_id))?;
    }
    Ok(())
}
fn count_tree(fields: &[Field]) -> usize {
    fields
        .iter()
        .map(|f| {
            1 + match &f.field_type {
                LogicalType::Struct { fields } => count_tree(fields),
                LogicalType::List { element } => count_tree(std::slice::from_ref(element)),
                LogicalType::Map { key, value } => {
                    count_tree(std::slice::from_ref(key)) + count_tree(std::slice::from_ref(value))
                }
                _ => 0,
            }
        })
        .sum()
}

fn check_ordinals(values: impl IntoIterator<Item = u32>, name: &str) -> Result<(), RuntimeError> {
    for (expected, actual) in values.into_iter().enumerate() {
        if actual
            != u32::try_from(expected)
                .map_err(|_| RuntimeError::Corrupt(format!("too many {name} ordinals")))?
        {
            return Err(RuntimeError::Corrupt(format!(
                "noncontiguous {name} ordinals"
            )));
        }
    }
    Ok(())
}

fn required_bool(value: &turso_core::Value) -> Result<bool, RuntimeError> {
    match required_i64(value, "required")? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(RuntimeError::Corrupt("required must be zero or one".into())),
    }
}
fn required_i64(v: &turso_core::Value, n: &str) -> Result<i64, RuntimeError> {
    match v {
        turso_core::Value::Numeric(turso_core::Numeric::Integer(v)) => Ok(*v),
        _ => Err(RuntimeError::Corrupt(format!("invalid {n}"))),
    }
}
fn required_u32(v: &turso_core::Value, n: &str) -> Result<u32, RuntimeError> {
    u32::try_from(required_i64(v, n)?).map_err(|_| RuntimeError::Corrupt(format!("invalid {n}")))
}
fn optional_u32(v: &turso_core::Value, n: &str) -> Result<Option<u32>, RuntimeError> {
    if matches!(v, turso_core::Value::Null) {
        Ok(None)
    } else {
        required_u32(v, n).map(Some)
    }
}
fn required_text(v: &turso_core::Value, n: &str) -> Result<String, RuntimeError> {
    match v {
        turso_core::Value::Text(v) => Ok(v.to_string()),
        _ => Err(RuntimeError::Corrupt(format!("invalid {n}"))),
    }
}
fn optional_text(v: &turso_core::Value, n: &str) -> Result<Option<String>, RuntimeError> {
    if matches!(v, turso_core::Value::Null) {
        Ok(None)
    } else {
        required_text(v, n).map(Some)
    }
}
fn optional_scalar(v: &turso_core::Value, n: &str) -> Result<Option<TypedScalar>, RuntimeError> {
    optional_text(v, n)?
        .map(|v| {
            otmp_protocol::canonical_json::from_slice_canonical(v.as_bytes())
                .map_err(RuntimeError::from)
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn integer(value: i64) -> turso_core::Value {
        turso_core::Value::Numeric(turso_core::Numeric::Integer(value))
    }
    #[test]
    fn required_is_strictly_boolean() {
        assert!(required_bool(&integer(0)).is_ok());
        assert!(required_bool(&integer(1)).is_ok());
        assert!(required_bool(&integer(2)).is_err());
        assert!(required_bool(&turso_core::Value::Null).is_err());
    }
    #[test]
    fn ordinals_must_start_at_zero_without_gaps() {
        assert!(check_ordinals([0, 1, 2], "field").is_ok());
        assert!(check_ordinals([1], "field").is_err());
        assert!(check_ordinals([0, 2], "field").is_err());
        assert!(check_ordinals([0, 0], "field").is_err());
    }
}
