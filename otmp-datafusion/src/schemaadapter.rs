//! Bind physical Parquet fields through each file's immutable OTMP schema.
use datafusion::arrow::array::{Array, ArrayRef, ListArray, MapArray, StructArray, new_null_array};
use datafusion::arrow::datatypes::{DataType, FieldRef, Fields, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion::scalar::ScalarValue;
use otmp_protocol::{Field as OtmpField, LogicalType, Schema as OtmpSchema};
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

#[derive(Debug)]
pub(crate) struct OtmpAdapterFactory {
    query: Arc<OtmpSchema>,
    recorded: Arc<OtmpSchema>,
}
impl OtmpAdapterFactory {
    pub(crate) fn new(query: Arc<OtmpSchema>, recorded: Arc<OtmpSchema>) -> Self {
        Self { query, recorded }
    }
}

pub(crate) fn binding_charge(schema: &OtmpSchema) -> Result<usize> {
    fn field_count(items: &[OtmpField]) -> usize {
        items
            .iter()
            .map(|field| {
                let children = match &field.field_type {
                    LogicalType::Struct { fields: children } => field_count(children),
                    LogicalType::List { element } => field_count(std::slice::from_ref(element)),
                    LogicalType::Map { key, value } => field_count(std::slice::from_ref(key))
                        .saturating_add(field_count(std::slice::from_ref(value))),
                    _ => 0,
                };
                1_usize.saturating_add(children)
            })
            .sum()
    }
    let encoded = otmp_protocol::canonical_json::to_vec(schema)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    Ok(512_usize
        .saturating_add(encoded.len().saturating_mul(4))
        .saturating_add(field_count(&schema.fields).saturating_mul(512)))
}

#[derive(Debug)]
struct Adapter {
    columns: Vec<(String, Arc<dyn PhysicalExpr>)>,
}
impl PhysicalExprAdapter for Adapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(expr
            .transform_up(|expr| {
                let Some(column) = expr.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                let replacement = self
                    .columns
                    .iter()
                    .find(|(name, _)| name == column.name())
                    .ok_or_else(|| error(format!("unknown OTMP query field {}", column.name())))?
                    .1
                    .clone();
                Ok(Transformed::yes(replacement))
            })?
            .data)
    }
}
impl PhysicalExprAdapterFactory for OtmpAdapterFactory {
    fn create(
        &self,
        logical: SchemaRef,
        physical: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        validate_ids(physical.fields(), &mut BTreeSet::new())?;
        let mut columns = Vec::new();
        for query in &self.query.fields {
            let target = logical
                .field_with_name(&query.name)
                .map_err(|_| error("query schema and adapter disagree"))?;
            let recorded = self
                .recorded
                .fields
                .iter()
                .find(|f| f.field_id == query.field_id);
            let slot = bind(query, recorded, physical.fields(), Arc::new(target.clone()))?;
            let expr: Arc<dyn PhysicalExpr> = if let Some(index) = slot.index {
                let input: Arc<dyn PhysicalExpr> =
                    Arc::new(Column::new(physical.field(index).name(), index));
                if slot.mapping.identity(physical.field(index).data_type()) {
                    input
                } else {
                    Arc::new(MappingExpr {
                        input,
                        mapping: slot.mapping,
                    })
                }
            } else {
                match &slot.mapping.kind {
                    Kind::Missing(Some(value)) => Arc::new(Literal::new(value.clone())),
                    Kind::Missing(None) => {
                        Arc::new(Literal::new(ScalarValue::try_from(target.data_type())?))
                    }
                    _ => return Err(error("invalid missing-field mapping")),
                }
            };
            columns.push((query.name.clone(), expr));
        }
        Ok(Arc::new(Adapter { columns }))
    }
}

#[derive(Debug)]
pub(crate) struct ValidatedAdapterFactory {
    bindings: Vec<(SchemaRef, Arc<dyn PhysicalExprAdapter>)>,
}

impl ValidatedAdapterFactory {
    pub(crate) fn new(
        bindings: impl IntoIterator<Item = (SchemaRef, Arc<dyn PhysicalExprAdapter>)>,
    ) -> Self {
        let mut unique = Vec::new();
        for binding in bindings {
            if !unique
                .iter()
                .any(|(physical, _): &(SchemaRef, _)| physical == &binding.0)
            {
                unique.push(binding);
            }
        }
        Self { bindings: unique }
    }
}

impl PhysicalExprAdapterFactory for ValidatedAdapterFactory {
    fn create(
        &self,
        _logical: SchemaRef,
        physical: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        self.bindings
            .iter()
            .find(|(validated, _)| validated.as_ref() == physical.as_ref())
            .map(|(_, binding)| binding.clone())
            .ok_or_else(|| error("Parquet schema differs from validated immutable state"))
    }
}

fn physical_id(field: &datafusion::arrow::datatypes::Field) -> Result<Option<u32>> {
    field
        .metadata()
        .get("PARQUET:field_id")
        .map(|v| {
            let id = v
                .parse::<u32>()
                .map_err(|_| error("invalid Parquet field ID"))?;
            if id == 0 {
                return Err(error("Parquet field ID must be positive"));
            }
            Ok(id)
        })
        .transpose()
}
fn validate_ids(fields: &Fields, seen: &mut BTreeSet<u32>) -> Result<()> {
    for field in fields {
        if let Some(id) = physical_id(field)?
            && !seen.insert(id)
        {
            return Err(error("ambiguous duplicate Parquet field ID"));
        }
        match field.data_type() {
            DataType::Struct(children) => validate_ids(children, seen)?,
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _)
            | DataType::Map(child, _) => validate_ids(&vec![child.clone()].into(), seen)?,
            _ => (),
        }
    }
    Ok(())
}
fn position(recorded: &OtmpField, physical: &Fields) -> Result<Option<usize>> {
    let mut by_id = None;
    let mut by_name = None;
    for (index, field) in physical.iter().enumerate() {
        let id = physical_id(field)?;
        if id == Some(recorded.field_id) && by_id.replace(index).is_some() {
            return Err(error("ambiguous Parquet field ID"));
        }
        if field.name() == &recorded.name && id.is_none() && by_name.replace(index).is_some() {
            return Err(error("ambiguous recorded field name"));
        }
    }
    if by_id.is_some() && by_name.is_some() {
        return Err(error(
            "field is ambiguous between Parquet ID and recorded name",
        ));
    }
    Ok(by_id.or(by_name))
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Slot {
    index: Option<usize>,
    mapping: Mapping,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Mapping {
    target: FieldRef,
    kind: Kind,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Kind {
    Missing(Option<ScalarValue>),
    Primitive,
    Struct(Vec<Slot>),
    List(Box<Mapping>),
    Map(Vec<Slot>),
}

fn bind(
    query: &OtmpField,
    recorded: Option<&OtmpField>,
    physical: &Fields,
    target: FieldRef,
) -> Result<Slot> {
    let index = recorded
        .map(|f| position(f, physical))
        .transpose()?
        .flatten();
    let Some(index) = index else {
        if query.required || recorded.is_some_and(|f| f.required) {
            return Err(error(format!("missing required OTMP field {}", query.name)));
        }
        let value = query
            .initial_default
            .as_ref()
            .map(|v| {
                crate::pruning::scalar(v, target.data_type())
                    .ok_or_else(|| error(format!("unsupported initial default for {}", query.name)))
            })
            .transpose()?;
        return Ok(Slot {
            index: None,
            mapping: Mapping {
                target,
                kind: Kind::Missing(value),
            },
        });
    };
    let recorded = recorded.unwrap();
    let source = physical[index].data_type();
    let kind = match (
        &query.field_type,
        &recorded.field_type,
        source,
        target.data_type(),
    ) {
        (
            LogicalType::Struct { fields: query },
            LogicalType::Struct { fields: recorded },
            DataType::Struct(source),
            DataType::Struct(target),
        ) => Kind::Struct(
            query
                .iter()
                .zip(target)
                .map(|(q, t)| {
                    bind(
                        q,
                        recorded.iter().find(|f| f.field_id == q.field_id),
                        source,
                        t.clone(),
                    )
                })
                .collect::<Result<_>>()?,
        ),
        (
            LogicalType::List { element: q },
            LogicalType::List { element: r },
            DataType::List(source),
            DataType::List(target),
        ) => {
            let slot = bind_collection(q, r, source, target.clone(), 0)?;
            Kind::List(Box::new(slot.mapping))
        }
        (
            LogicalType::Map { key: qk, value: qv },
            LogicalType::Map { key: rk, value: rv },
            DataType::Map(source, _),
            DataType::Map(target, _),
        ) => {
            let (DataType::Struct(source), DataType::Struct(target)) =
                (source.data_type(), target.data_type())
            else {
                return Err(error("invalid physical map entries"));
            };
            if source.len() != 2 || target.len() != 2 {
                return Err(error("invalid map field count"));
            }
            Kind::Map(vec![
                bind_collection(qk, rk, &source[0], target[0].clone(), 0)?,
                bind_collection(qv, rv, &source[1], target[1].clone(), 1)?,
            ])
        }
        (q, r, source, target) if q == r && compatible(source, target) => Kind::Primitive,
        _ => {
            return Err(error(format!(
                "unsupported Parquet conversion for OTMP field {}: {source:?} -> {:?}",
                query.name,
                target.data_type()
            )));
        }
    };
    Ok(Slot {
        index: Some(index),
        mapping: Mapping { target, kind },
    })
}

/// List elements and map keys/values have intrinsic positions in Arrow and
/// Parquet. Their wrapper names can be erased or canonicalized by the reader.
/// Once the containing field has been bound through the recorded schema, bind
/// these unique roles and still require every physical ID to agree. Ordinary
/// struct children continue to use recorded names or physical IDs.
fn bind_collection(
    query: &OtmpField,
    recorded: &OtmpField,
    physical: &FieldRef,
    target: FieldRef,
    index: usize,
) -> Result<Slot> {
    if query.field_id != recorded.field_id
        || physical_id(physical)?.is_some_and(|id| id != recorded.field_id)
    {
        return Err(error("collection field ID disagrees with recorded schema"));
    }
    let named = Arc::new(physical.as_ref().clone().with_name(&recorded.name));
    let mut slot = bind(query, Some(recorded), &vec![named].into(), target)?;
    slot.index = Some(index);
    Ok(slot)
}
fn compatible(source: &DataType, target: &DataType) -> bool {
    if source == target {
        return true;
    }
    match (source, target) {
        (DataType::Int8 | DataType::Int16, DataType::Int32 | DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (DataType::Float32, DataType::Float64)
        | (DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Utf8)
        | (DataType::Binary | DataType::BinaryView | DataType::LargeBinary, DataType::Binary) => {
            true
        }
        (DataType::Dictionary(_, value), target) => compatible(value, target),
        (
            DataType::Decimal128(p, s),
            DataType::Decimal128(tp, ts) | DataType::Decimal256(tp, ts),
        )
        | (DataType::Decimal256(p, s), DataType::Decimal256(tp, ts)) => p <= tp && s == ts,
        _ => false,
    }
}
impl Mapping {
    fn identity(&self, source: &DataType) -> bool {
        matches!(self.kind, Kind::Primitive) && self.target.data_type() == source
    }
    fn apply(&self, input: Option<&ArrayRef>, len: usize) -> Result<ArrayRef> {
        match &self.kind {
            Kind::Missing(None) => Ok(new_null_array(self.target.data_type(), len)),
            Kind::Missing(Some(value)) => value.to_array_of_size(len),
            Kind::Primitive => {
                let input = input.ok_or_else(|| error("missing mapped primitive"))?;
                if input.data_type() == self.target.data_type() {
                    Ok(input.clone())
                } else {
                    Ok(datafusion::arrow::compute::cast(
                        input,
                        self.target.data_type(),
                    )?)
                }
            }
            Kind::Struct(slots) => {
                let source = input
                    .and_then(|v| v.as_any().downcast_ref::<StructArray>())
                    .ok_or_else(|| error("expected physical struct"))?;
                let DataType::Struct(target) = self.target.data_type() else {
                    return Err(error("expected query struct"));
                };
                let values = slots
                    .iter()
                    .map(|slot| {
                        slot.mapping
                            .apply(slot.index.map(|i| source.column(i)), len)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(StructArray::try_new(
                    target.clone(),
                    values,
                    source.nulls().cloned(),
                )?))
            }
            Kind::List(mapping) => {
                let source = input
                    .and_then(|v| v.as_any().downcast_ref::<ListArray>())
                    .ok_or_else(|| error("expected physical list"))?;
                let DataType::List(target) = self.target.data_type() else {
                    return Err(error("expected query list"));
                };
                let values = mapping.apply(Some(source.values()), source.values().len())?;
                Ok(Arc::new(ListArray::try_new(
                    target.clone(),
                    source.offsets().clone(),
                    values,
                    source.nulls().cloned(),
                )?))
            }
            Kind::Map(slots) => {
                let source = input
                    .and_then(|v| v.as_any().downcast_ref::<MapArray>())
                    .ok_or_else(|| error("expected physical map"))?;
                let DataType::Map(target, sorted) = self.target.data_type() else {
                    return Err(error("expected query map"));
                };
                let DataType::Struct(fields) = target.data_type() else {
                    return Err(error("expected query map entries"));
                };
                let values = slots
                    .iter()
                    .map(|slot| {
                        slot.mapping.apply(
                            slot.index.map(|i| source.entries().column(i)),
                            source.entries().len(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let entries = StructArray::try_new(fields.clone(), values, None)?;
                Ok(Arc::new(MapArray::try_new(
                    target.clone(),
                    source.offsets().clone(),
                    entries,
                    source.nulls().cloned(),
                    *sorted,
                )?))
            }
        }
    }
}

#[derive(Debug, Clone, Eq)]
struct MappingExpr {
    input: Arc<dyn PhysicalExpr>,
    mapping: Mapping,
}
impl PartialEq for MappingExpr {
    fn eq(&self, other: &Self) -> bool {
        self.input.eq(&other.input) && self.mapping == other.mapping
    }
}
impl Hash for MappingExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.mapping.hash(state);
    }
}
impl std::fmt::Display for MappingExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "otmp_field({})", self.input)
    }
}
impl PhysicalExpr for MappingExpr {
    fn return_field(&self, _: &Schema) -> Result<FieldRef> {
        Ok(self.mapping.target.clone())
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.input.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(ColumnarValue::Array(
            self.mapping.apply(Some(&array), batch.num_rows())?,
        ))
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [input]: [Arc<dyn PhysicalExpr>; 1] = children
            .try_into()
            .map_err(|_| error("OTMP mapped expression requires one child"))?;
        Ok(Arc::new(Self {
            input,
            mapping: self.mapping.clone(),
        }))
    }
    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "otmp_field({})", self.input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::Field;
    use otmp_protocol::TypedScalar;
    fn field(id: u32, name: &str, kind: LogicalType, required: bool) -> OtmpField {
        OtmpField {
            field_id: id,
            name: name.into(),
            required,
            field_type: kind,
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }
    fn schema(fields: Vec<OtmpField>) -> Arc<OtmpSchema> {
        Arc::new(OtmpSchema {
            schema_id: 1,
            parent_schema_id: None,
            fields,
            identifier_field_ids: vec![],
            doc: None,
        })
    }
    fn evaluate(
        query: &Arc<OtmpSchema>,
        recorded: Arc<OtmpSchema>,
        batch: &RecordBatch,
    ) -> Result<ArrayRef> {
        let logical = crate::schema_to_arrow(query)?;
        let adapter =
            OtmpAdapterFactory::new(query.clone(), recorded).create(logical, batch.schema())?;
        adapter
            .rewrite(Arc::new(Column::new(&query.fields[0].name, 0)))?
            .evaluate(batch)?
            .into_array(batch.num_rows())
    }
    #[test]
    fn field_ids_and_recorded_names_bind_before_query_names() {
        let query = schema(vec![field(1, "new", LogicalType::Int64, true)]);
        let recorded = schema(vec![field(1, "old", LogicalType::Int64, true)]);
        for physical in [
            Field::new("old", DataType::Int64, false),
            Field::new("physical", DataType::Int64, false)
                .with_metadata([("PARQUET:field_id".into(), "1".into())].into()),
        ] {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![physical])),
                vec![Arc::new(Int64Array::from(vec![1, 2]))],
            )
            .unwrap();
            assert_eq!(
                evaluate(&query, recorded.clone(), &batch).unwrap().as_ref(),
                &Int64Array::from(vec![1, 2])
            );
        }
        let wrong = Arc::new(Schema::new(vec![Field::new("new", DataType::Int64, false)]));
        assert!(
            OtmpAdapterFactory::new(query.clone(), recorded)
                .create(crate::schema_to_arrow(&query).unwrap(), wrong)
                .is_err()
        );
    }
    #[test]
    fn duplicate_ids_missing_required_and_unsupported_casts_fail() {
        let query = schema(vec![field(1, "value", LogicalType::Int64, true)]);
        let adapter = OtmpAdapterFactory::new(query.clone(), query.clone());
        let logical = crate::schema_to_arrow(&query).unwrap();
        let duplicate = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true)
                .with_metadata([("PARQUET:field_id".into(), "1".into())].into()),
            Field::new("b", DataType::Int64, true)
                .with_metadata([("PARQUET:field_id".into(), "1".into())].into()),
        ]));
        assert!(adapter.create(logical.clone(), duplicate).is_err());
        assert!(
            adapter
                .create(logical.clone(), Arc::new(Schema::empty()))
                .is_err()
        );
        assert!(
            adapter
                .create(
                    logical,
                    Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
                )
                .is_err()
        );
    }
    #[test]
    fn nested_additions_apply_defaults_and_preserve_parent_nulls() {
        let old_child = field(2, "old", LogicalType::Int64, false);
        let recorded = schema(vec![field(
            1,
            "record",
            LogicalType::Struct {
                fields: vec![old_child.clone()],
            },
            false,
        )]);
        let mut new_child = old_child;
        new_child.name = "renamed".into();
        let mut added = field(3, "added", LogicalType::String, false);
        added.initial_default = Some(TypedScalar::String("ready".into()));
        let query = schema(vec![field(
            1,
            "record",
            LogicalType::Struct {
                fields: vec![new_child, added],
            },
            false,
        )]);
        let old_arrow = crate::schema_to_arrow(&recorded).unwrap();
        let DataType::Struct(children) = old_arrow.field(0).data_type() else {
            unreachable!()
        };
        let values = StructArray::new(
            children.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(5), None]))],
            Some(vec![true, false].into()),
        );
        let batch = RecordBatch::try_new(old_arrow, vec![Arc::new(values)]).unwrap();
        let result = evaluate(&query, recorded, &batch).unwrap();
        let result = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(result.is_valid(0));
        assert!(result.is_null(1));
        assert_eq!(
            result.column(1).as_ref(),
            &StringArray::from(vec!["ready", "ready"])
        );
        assert_eq!(result.fields()[0].name(), "renamed");
    }
}
