//! Conservative OTMP metric pruning boundary.
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Column, pruning::PruningStatistics};
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::PruningPredicateBuilder;
use datafusion::scalar::ScalarValue;

pub(crate) fn lower_ranges(
    filters: &[datafusion::logical_expr::Expr],
    schema: &otmp_protocol::Schema,
) -> Vec<otmp::FileMetricRange> {
    fn visit(
        expression: &datafusion::logical_expr::Expr,
        schema: &otmp_protocol::Schema,
        ranges: &mut Vec<otmp::FileMetricRange>,
    ) {
        use datafusion::logical_expr::{Expr, Operator};
        let Expr::BinaryExpr(binary) = expression else {
            return;
        };
        if binary.op == Operator::And {
            visit(&binary.left, schema, ranges);
            visit(&binary.right, schema, ranges);
            return;
        }
        if !matches!(
            binary.op,
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
        ) {
            return;
        }
        let (column, literal, operator) = match (&*binary.left, &*binary.right) {
            (Expr::Column(column), Expr::Literal(value, _)) => (column, value, binary.op),
            (Expr::Literal(value, _), Expr::Column(column)) => (
                column,
                value,
                match binary.op {
                    Operator::Lt => Operator::Gt,
                    Operator::LtEq => Operator::GtEq,
                    Operator::Gt => Operator::Lt,
                    Operator::GtEq => Operator::LtEq,
                    _ => unreachable!(),
                },
            ),
            _ => return,
        };
        let Some(field) = schema.fields.iter().find(|field| field.name == column.name) else {
            return;
        };
        let range = match (&field.field_type, literal) {
            (otmp_protocol::LogicalType::Int32, ScalarValue::Int32(Some(value))) => {
                let (lower, upper) = comparison_bounds(operator, *value);
                otmp::FileMetricRange::Int32 {
                    field_id: field.field_id,
                    lower,
                    upper,
                }
            }
            (otmp_protocol::LogicalType::Int64, ScalarValue::Int64(Some(value))) => {
                let (lower, upper) = comparison_bounds(operator, *value);
                otmp::FileMetricRange::Int64 {
                    field_id: field.field_id,
                    lower,
                    upper,
                }
            }
            (otmp_protocol::LogicalType::Date, ScalarValue::Date32(Some(value))) => {
                let (lower, upper) = comparison_bounds(operator, *value);
                otmp::FileMetricRange::Date {
                    field_id: field.field_id,
                    lower,
                    upper,
                }
            }
            _ => return,
        };
        ranges.push(range);
    }

    let mut ranges = Vec::new();
    for filter in filters {
        visit(filter, schema, &mut ranges);
    }
    ranges
}

fn comparison_bounds<T>(
    operator: datafusion::logical_expr::Operator,
    value: T,
) -> (Bound<T>, Bound<T>) {
    use datafusion::logical_expr::Operator;
    match operator {
        Operator::Lt => (Bound::Unbounded, Bound::Excluded(value)),
        Operator::LtEq => (Bound::Unbounded, Bound::Included(value)),
        Operator::Gt => (Bound::Excluded(value), Bound::Unbounded),
        Operator::GtEq => (Bound::Included(value), Bound::Unbounded),
        _ => unreachable!(),
    }
}

/// Returns `true` for every file when statistics cannot safely prove pruning.
/// The provider retains its residual predicate, so this is always Inexact.
pub(crate) fn prune(
    predicate: Arc<dyn PhysicalExpr>,
    query_otmp_schema: &otmp_protocol::Schema,
    arrow_schema: SchemaRef,
    files: &[otmp::ReaderFile],
    file_schemas: &BTreeMap<u32, Arc<otmp_protocol::Schema>>,
) -> Result<Vec<bool>> {
    // Invalid SQL references were already rejected by logical planning, and
    // authoritative metadata was validated before this optimization boundary.
    let stats = Stats {
        schema: query_otmp_schema,
        arrow: arrow_schema.clone(),
        files,
        file_schemas,
    };
    match PruningPredicateBuilder::new()
        .with_file_schema(arrow_schema)
        .try_build(predicate)
    {
        Ok(predicate) => predicate
            .prune(&stats)
            .or_else(|_| Ok(vec![true; files.len()])),
        Err(_) => Ok(vec![true; files.len()]),
    }
}

struct Stats<'a> {
    schema: &'a otmp_protocol::Schema,
    arrow: SchemaRef,
    files: &'a [otmp::ReaderFile],
    file_schemas: &'a BTreeMap<u32, Arc<otmp_protocol::Schema>>,
}

enum FieldAvailability {
    Present,
    Absent(ScalarValue),
}
impl Stats<'_> {
    fn field(&self, column: &Column) -> Option<(&otmp_protocol::Field, &DataType)> {
        let index = self.arrow.index_of(&column.name).ok()?;
        let field = self.schema.fields.iter().find(|f| f.name == column.name)?;
        Some((field, self.arrow.field(index).data_type()))
    }

    /// Returns a scalar only when the immutable schema for this file proves
    /// that the query field did not exist. A missing metric is never evidence
    /// of a missing column.
    fn absent_field_value(
        &self,
        file: &otmp::ReaderFile,
        query_field: &otmp_protocol::Field,
        ty: &DataType,
    ) -> Option<FieldAvailability> {
        let file_schema = self.file_schemas.get(&file.schema_id)?;
        if file_schema
            .fields
            .iter()
            .any(|field| field.field_id == query_field.field_id)
        {
            return Some(FieldAvailability::Present);
        }
        Some(FieldAvailability::Absent(
            match &query_field.initial_default {
                Some(default) => scalar(default, ty)?,
                None => ScalarValue::try_from(ty).ok()?,
            },
        ))
    }

    fn metric<'b>(
        &self,
        file: &'b otmp::ReaderFile,
        query_field: &otmp_protocol::Field,
    ) -> Option<&'b otmp::FileMetric> {
        // If the recorded schema is available, validate the stable-ID mapping
        // before considering metrics. An incompatible evolution is not an
        // optimization opportunity.
        if let Some(schema) = self.file_schemas.get(&file.schema_id) {
            let field = schema
                .fields
                .iter()
                .find(|field| field.field_id == query_field.field_id)?;
            if field.field_type != query_field.field_type {
                return None;
            }
        }
        file.metrics
            .iter()
            .find(|metric| metric.field_id == query_field.field_id)
    }

    fn bounds(&self, column: &Column, lower: bool) -> Option<ArrayRef> {
        let (field, ty) = self.field(column)?;
        let values = self.files.iter().map(|file| {
            let known = self
                .absent_field_value(file, field, ty)
                .and_then(|availability| match availability {
                    FieldAvailability::Present => None,
                    FieldAvailability::Absent(value) => Some(value),
                })
                .or_else(|| {
                    self.metric(file, field)
                .and_then(|m| {
                    if matches!(m.nan_count, Some(0))
                        || !matches!(ty, DataType::Float32 | DataType::Float64)
                    {
                        let low = m.lower_bound.as_ref()?;
                        let high = m.upper_bound.as_ref()?;
                        let low_scalar = scalar(low, ty)?;
                        let high_scalar = scalar(high, ty)?;
                        if low_scalar
                            .partial_cmp(&high_scalar)
                            .is_some_and(std::cmp::Ordering::is_gt)
                        {
                            return None;
                        }
                        let value = if lower { low } else { high };
                        if matches!(value, otmp_protocol::TypedScalar::Float32(v) if v.is_nan())
                            || matches!(value, otmp_protocol::TypedScalar::Float64(v) if v.is_nan())
                        {
                            None
                        } else {
                            scalar(value, ty)
                        }
                    } else {
                        None
                    }
                })
                });
            known.unwrap_or_else(|| {
                ScalarValue::try_from(ty).expect("Arrow types admit a null scalar")
            })
        });
        ScalarValue::iter_to_array(values).ok()
    }

    fn null_counts_for(&self, column: &Column) -> Option<ArrayRef> {
        let (field, _) = self.field(column)?;
        let values = self.files.iter().map(|file| {
            match self.absent_field_value(file, field, self.field(column)?.1) {
                // An absent field with a non-null default has exactly zero
                // nulls; absent null columns need a trustworthy row count,
                // which this reader deliberately does not advertise.
                Some(FieldAvailability::Absent(value)) if !value.is_null() => Some(0_u64),
                Some(FieldAvailability::Absent(_)) => None,
                Some(FieldAvailability::Present) => {
                    self.metric(file, field).and_then(|m| m.null_count)
                }
                None => self.metric(file, field).and_then(|m| m.null_count),
            }
        });
        ScalarValue::iter_to_array(values.map(ScalarValue::UInt64)).ok()
    }
}
impl PruningStatistics for Stats<'_> {
    fn min_values(&self, c: &Column) -> Option<ArrayRef> {
        self.bounds(c, true)
    }
    fn max_values(&self, c: &Column) -> Option<ArrayRef> {
        self.bounds(c, false)
    }
    fn num_containers(&self) -> usize {
        self.files.len()
    }
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.null_counts_for(column)
    }
    fn row_counts(&self) -> Option<ArrayRef> {
        None
    }
    fn contained(&self, _: &Column, _: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

pub(crate) fn scalar(
    value: &otmp_protocol::TypedScalar,
    data_type: &DataType,
) -> Option<ScalarValue> {
    use otmp_protocol::TypedScalar as V;
    match (value, data_type) {
        (V::Null, _) => ScalarValue::try_from(data_type).ok(),
        (V::Boolean(v), DataType::Boolean) => Some(ScalarValue::Boolean(Some(*v))),
        (V::Int32(v), DataType::Int32) => Some(ScalarValue::Int32(Some(*v))),
        (V::Int64(v), DataType::Int64) => Some(ScalarValue::Int64(Some(*v))),
        (V::Float32(v), DataType::Float32) => Some(ScalarValue::Float32(Some(*v))),
        (V::Float64(v), DataType::Float64) => Some(ScalarValue::Float64(Some(*v))),
        (V::Date(v), DataType::Date32) => Some(ScalarValue::Date32(Some(*v))),
        (
            V::TimeMicros(v),
            DataType::Time64(datafusion::arrow::datatypes::TimeUnit::Microsecond),
        ) => Some(ScalarValue::Time64Microsecond(Some(*v))),
        (
            V::TimestampMicros(v),
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
        ) => Some(ScalarValue::TimestampMicrosecond(Some(*v), None)),
        (
            V::TimestamptzMicros(v),
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, tz),
        ) => Some(ScalarValue::TimestampMicrosecond(Some(*v), tz.clone())),
        (V::String(v), DataType::Utf8) => Some(ScalarValue::Utf8(Some(v.clone()))),
        (V::String(v), DataType::Utf8View) => Some(ScalarValue::Utf8View(Some(v.clone()))),
        (V::Binary(v), DataType::Binary) => Some(ScalarValue::Binary(Some(v.clone()))),
        (V::Binary(v), DataType::BinaryView) => Some(ScalarValue::BinaryView(Some(v.clone()))),
        (V::Fixed(v), DataType::FixedSizeBinary(length))
            if usize::try_from(*length)
                .ok()
                .is_some_and(|width| v.len() == width) =>
        {
            Some(ScalarValue::FixedSizeBinary(*length, Some(v.clone())))
        }
        (V::Uuid(v), DataType::FixedSizeBinary(16)) => Some(ScalarValue::FixedSizeBinary(
            16,
            Some(v.as_bytes().to_vec()),
        )),
        (
            V::Decimal {
                precision,
                scale,
                unscaled,
            },
            DataType::Decimal128(p, s),
        ) if *precision == u32::from(*p)
            && *scale == u32::try_from(*s).ok()?
            && unscaled.len() <= 16 =>
        {
            let mut bytes = [if unscaled.first().is_some_and(|v| v & 0x80 != 0) {
                0xff
            } else {
                0
            }; 16];
            bytes[16 - unscaled.len()..].copy_from_slice(unscaled);
            Some(ScalarValue::Decimal128(
                Some(i128::from_be_bytes(bytes)),
                *p,
                *s,
            ))
        }
        (
            V::Decimal {
                precision,
                scale,
                unscaled,
            },
            DataType::Decimal256(p, s),
        ) if *precision <= 76
            && *precision == u32::from(*p)
            && *scale == u32::try_from(*s).ok()?
            && unscaled.len() <= 32 =>
        {
            let mut bytes = [if unscaled.first().is_some_and(|v| v & 0x80 != 0) {
                0xff
            } else {
                0
            }; 32];
            bytes[32 - unscaled.len()..].copy_from_slice(unscaled);
            Some(ScalarValue::Decimal256(
                Some(datafusion::arrow::datatypes::i256::from_be_bytes(bytes)),
                *p,
                *s,
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Float64Array, Int64Array};
    use datafusion::arrow::datatypes::{Field as ArrowField, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, IsNullExpr, Literal};
    use std::collections::BTreeMap;
    use std::ops::Bound;

    #[test]
    fn lowers_only_exact_top_level_inequalities_and_flattens_and() {
        use datafusion::prelude::{col, lit};
        let schema = otmp_protocol::Schema {
            schema_id: 1,
            parent_schema_id: None,
            fields: vec![
                otmp_protocol::Field {
                    field_id: 1,
                    name: "i32".into(),
                    required: false,
                    field_type: otmp_protocol::LogicalType::Int32,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                otmp_protocol::Field {
                    field_id: 2,
                    name: "i64".into(),
                    required: false,
                    field_type: otmp_protocol::LogicalType::Int64,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                otmp_protocol::Field {
                    field_id: 3,
                    name: "day".into(),
                    required: false,
                    field_type: otmp_protocol::LogicalType::Date,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            ],
            identifier_field_ids: vec![],
            doc: None,
        };
        let filters = vec![
            col("i32").lt(lit(7_i32)).and(lit(9_i64).lt_eq(col("i64"))),
            col("day").gt_eq(lit(datafusion::scalar::ScalarValue::Date32(Some(-2)))),
            col("i64").eq(lit(10_i64)),
            col("i64").lt(lit(10_i32)),
            col("i64").lt(lit(20_i64)).or(col("i64").gt(lit(30_i64))),
            col("i64").not_eq(lit(11_i64)),
        ];
        assert_eq!(
            lower_ranges(&filters, &schema),
            vec![
                otmp::FileMetricRange::Int32 {
                    field_id: 1,
                    lower: Bound::Unbounded,
                    upper: Bound::Excluded(7),
                },
                otmp::FileMetricRange::Int64 {
                    field_id: 2,
                    lower: Bound::Included(9),
                    upper: Bound::Unbounded,
                },
                otmp::FileMetricRange::Date {
                    field_id: 3,
                    lower: Bound::Included(-2),
                    upper: Bound::Unbounded,
                },
            ]
        );
    }

    #[test]
    fn lowers_every_ordered_type_operator_and_operand_order() {
        use datafusion::logical_expr::{Expr, Operator};
        use datafusion::prelude::{col, lit};
        let schema = otmp_protocol::Schema {
            schema_id: 1,
            parent_schema_id: None,
            fields: vec![
                otmp_protocol::Field {
                    field_id: 1,
                    name: "i32".into(),
                    required: false,
                    field_type: otmp_protocol::LogicalType::Int32,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                otmp_protocol::Field {
                    field_id: 2,
                    name: "i64".into(),
                    required: false,
                    field_type: otmp_protocol::LogicalType::Int64,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                otmp_protocol::Field {
                    field_id: 3,
                    name: "day".into(),
                    required: false,
                    field_type: otmp_protocol::LogicalType::Date,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            ],
            identifier_field_ids: vec![],
            doc: None,
        };
        let compare = |left: Expr, operator, right: Expr| match operator {
            Operator::Lt => left.lt(right),
            Operator::LtEq => left.lt_eq(right),
            Operator::Gt => left.gt(right),
            Operator::GtEq => left.gt_eq(right),
            _ => unreachable!(),
        };
        let reverse = |operator| match operator {
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            _ => unreachable!(),
        };
        for (name, literal, field_id) in [
            ("i32", lit(7_i32), 1),
            ("i64", lit(7_i64), 2),
            ("day", lit(ScalarValue::Date32(Some(7))), 3),
        ] {
            for operator in [Operator::Lt, Operator::LtEq, Operator::Gt, Operator::GtEq] {
                for reversed in [false, true] {
                    let expression = if reversed {
                        compare(literal.clone(), operator, col(name))
                    } else {
                        compare(col(name), operator, literal.clone())
                    };
                    let normalized = if reversed {
                        reverse(operator)
                    } else {
                        operator
                    };
                    let expected = comparison_bounds(normalized, 7_i64);
                    let actual = lower_ranges(&[expression], &schema);
                    let bounds = match &actual[0] {
                        otmp::FileMetricRange::Int32 {
                            field_id: id,
                            lower,
                            upper,
                        }
                        | otmp::FileMetricRange::Date {
                            field_id: id,
                            lower,
                            upper,
                        } => {
                            assert_eq!(*id, field_id);
                            ((*lower).map(i64::from), (*upper).map(i64::from))
                        }
                        otmp::FileMetricRange::Int64 {
                            field_id: id,
                            lower,
                            upper,
                        } => {
                            assert_eq!(*id, field_id);
                            (*lower, *upper)
                        }
                    };
                    assert_eq!(bounds, expected, "{name} {operator:?} reversed={reversed}");
                }
            }
        }
    }
    #[test]
    fn scalar_preserves_exact_view_fixed_uuid_and_decimal_types() {
        assert!(matches!(
            scalar(
                &otmp_protocol::TypedScalar::String("x".into()),
                &DataType::Utf8View
            ),
            Some(ScalarValue::Utf8View(_))
        ));
        assert!(matches!(
            scalar(
                &otmp_protocol::TypedScalar::Fixed(vec![1, 2]),
                &DataType::FixedSizeBinary(2)
            ),
            Some(ScalarValue::FixedSizeBinary(2, _))
        ));
        let decimal = otmp_protocol::TypedScalar::Decimal {
            precision: 3,
            scale: 1,
            unscaled: vec![0, 123],
        };
        assert!(matches!(
            scalar(&decimal, &DataType::Decimal128(3, 1)),
            Some(ScalarValue::Decimal128(Some(123), 3, 1))
        ));
    }

    #[test]
    fn greater_than_prunes_only_proven_non_matches() {
        let schema = otmp_protocol::Schema {
            schema_id: 1,
            parent_schema_id: None,
            fields: vec![otmp_protocol::Field {
                field_id: 1,
                name: "value".into(),
                required: false,
                field_type: otmp_protocol::LogicalType::Int64,
                doc: None,
                initial_default: None,
                write_default: None,
            }],
            identifier_field_ids: vec![],
            doc: None,
        };
        let file = |max: Option<i64>| otmp::ReaderFile {
            file: otmp::LiveFile {
                file_id: otmp_protocol::Id::from_bytes([1; 16]),
                uri: "data/a.parquet".parse().unwrap(),
                file_format: "parquet".into(),
                file_size_bytes: 1,
                record_count: 1,
                content_sha256: None,
                sequence_number: 1,
            },
            schema_id: 1,
            metrics: max
                .map(|max| {
                    vec![otmp::FileMetric {
                        field_id: 1,
                        column_size_bytes: None,
                        value_count: None,
                        null_count: None,
                        nan_count: Some(0),
                        distinct_count: None,
                        lower_bound: Some(otmp_protocol::TypedScalar::Int64(0)),
                        upper_bound: Some(otmp_protocol::TypedScalar::Int64(max)),
                        metadata: BTreeMap::new(),
                    }]
                })
                .unwrap_or_default(),
        };
        let files = vec![file(Some(10)), file(Some(200)), file(None)];
        let arrow = Arc::new(Schema::new(vec![ArrowField::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("value", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(100)))),
        ));
        assert_eq!(
            prune(expr, &schema, arrow, &files, &BTreeMap::new()).unwrap(),
            vec![false, true, true]
        );
    }

    fn int_schema() -> otmp_protocol::Schema {
        otmp_protocol::Schema {
            schema_id: 1,
            parent_schema_id: None,
            fields: vec![otmp_protocol::Field {
                field_id: 1,
                name: "value".into(),
                required: false,
                field_type: otmp_protocol::LogicalType::Int64,
                doc: None,
                initial_default: None,
                write_default: None,
            }],
            identifier_field_ids: vec![],
            doc: None,
        }
    }

    fn reader_file(metric: Option<otmp::FileMetric>) -> otmp::ReaderFile {
        otmp::ReaderFile {
            file: otmp::LiveFile {
                file_id: otmp_protocol::Id::from_bytes([7; 16]),
                uri: "data/test.parquet".parse().unwrap(),
                file_format: "parquet".into(),
                file_size_bytes: 1,
                record_count: 10,
                content_sha256: None,
                sequence_number: 1,
            },
            schema_id: 1,
            metrics: metric.into_iter().collect(),
        }
    }

    fn metric(
        lower_bound: otmp_protocol::TypedScalar,
        upper_bound: otmp_protocol::TypedScalar,
        nan_count: Option<u64>,
    ) -> otmp::FileMetric {
        otmp::FileMetric {
            field_id: 1,
            column_size_bytes: None,
            value_count: None,
            null_count: None,
            nan_count,
            distinct_count: None,
            lower_bound: Some(lower_bound),
            upper_bound: Some(upper_bound),
            metadata: BTreeMap::new(),
        }
    }

    fn predicate(operator: Operator, scalar: ScalarValue) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("value", 0)),
            operator,
            Arc::new(Literal::new(scalar)),
        ))
    }

    fn has_match(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> bool {
        let value = expr
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        value
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .flatten()
            .any(|value| value)
    }

    #[test]
    fn randomized_metrics_never_prune_a_matching_int64_file() {
        let schema = int_schema();
        let arrow = Arc::new(Schema::new(vec![ArrowField::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let mut state = 0x4d59_5df4_d0f3_3173_u64;
        for case_number in 0..256_u64 {
            let values = (0..10)
                .map(|_| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1);
                    if state.is_multiple_of(5) {
                        None
                    } else {
                        Some((i64::try_from(state >> 16).unwrap() % 401) - 200)
                    }
                })
                .collect::<Vec<_>>();
            let non_null = values.iter().flatten().copied().collect::<Vec<_>>();
            let lower = *non_null.iter().min().unwrap_or(&0);
            let upper = *non_null.iter().max().unwrap_or(&0);
            let metric = match case_number % 4 {
                // Missing metrics and malformed reversed bounds must fail open.
                0 => None,
                1 => Some(metric(
                    otmp_protocol::TypedScalar::Int64(upper),
                    otmp_protocol::TypedScalar::Int64(lower),
                    Some(0),
                )),
                _ => Some(metric(
                    otmp_protocol::TypedScalar::Int64(lower),
                    otmp_protocol::TypedScalar::Int64(upper),
                    Some(0),
                )),
            };
            let threshold = (i64::try_from(state >> 32).unwrap() % 401) - 200;
            let operator = if case_number & 1 == 0 {
                Operator::Gt
            } else {
                Operator::Lt
            };
            let scalar = ScalarValue::Int64(Some(threshold));
            let expr = predicate(operator, scalar);
            let batch = RecordBatch::try_new(
                arrow.clone(),
                vec![Arc::new(Int64Array::from(values)) as ArrayRef],
            )
            .unwrap();
            let keep = prune(
                expr.clone(),
                &schema,
                arrow.clone(),
                &[reader_file(metric)],
                &BTreeMap::new(),
            )
            .unwrap()[0];
            assert!(
                !has_match(&expr, &batch) || keep,
                "case {case_number}: pruned a matching file"
            );
        }
    }

    #[test]
    fn float_nan_and_infinite_metrics_fail_open_without_a_proven_bound() {
        let schema = otmp_protocol::Schema {
            fields: vec![otmp_protocol::Field {
                field_type: otmp_protocol::LogicalType::Float64,
                ..int_schema().fields.remove(0)
            }],
            ..int_schema()
        };
        let arrow = Arc::new(Schema::new(vec![ArrowField::new(
            "value",
            DataType::Float64,
            true,
        )]));
        let expr = predicate(Operator::Gt, ScalarValue::Float64(Some(0.0)));
        for (values, nan_count) in [
            (vec![Some(f64::NAN), Some(1.0), None], None),
            (vec![Some(f64::NAN), Some(f64::INFINITY)], Some(1)),
            (vec![Some(f64::NEG_INFINITY), Some(f64::INFINITY)], None),
        ] {
            let batch = RecordBatch::try_new(
                arrow.clone(),
                vec![Arc::new(Float64Array::from(values.clone())) as ArrayRef],
            )
            .unwrap();
            let file = reader_file(Some(metric(
                otmp_protocol::TypedScalar::Float64(f64::NEG_INFINITY),
                otmp_protocol::TypedScalar::Float64(f64::INFINITY),
                nan_count,
            )));
            let keep = prune(
                expr.clone(),
                &schema,
                arrow.clone(),
                &[file],
                &BTreeMap::new(),
            )
            .unwrap()[0];
            assert!(!has_match(&expr, &batch) || keep);
        }
    }

    #[test]
    fn old_file_uses_only_its_declared_initial_default() {
        let old_schema = Arc::new(otmp_protocol::Schema {
            schema_id: 1,
            parent_schema_id: None,
            fields: vec![],
            identifier_field_ids: vec![],
            doc: None,
        });
        let mut query = int_schema();
        query.schema_id = 2;
        query.fields[0].initial_default = Some(otmp_protocol::TypedScalar::Int64(42));
        let arrow = Arc::new(Schema::new(vec![ArrowField::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let schemas = BTreeMap::from([(1, old_schema)]);
        let file = reader_file(None);
        assert_eq!(
            prune(
                predicate(Operator::Gt, ScalarValue::Int64(Some(100))),
                &query,
                arrow,
                &[file],
                &schemas,
            )
            .unwrap(),
            vec![false],
        );
    }

    #[test]
    fn absent_null_has_unknown_null_count_and_unsupported_predicate_retains() {
        let old_schema = Arc::new(otmp_protocol::Schema {
            schema_id: 1,
            parent_schema_id: None,
            fields: vec![],
            identifier_field_ids: vec![],
            doc: None,
        });
        let query = int_schema();
        let arrow = Arc::new(Schema::new(vec![ArrowField::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let schemas = BTreeMap::from([(1, old_schema)]);
        let file = reader_file(None);
        let stats = Stats {
            schema: &query,
            arrow: arrow.clone(),
            files: std::slice::from_ref(&file),
            file_schemas: &schemas,
        };
        let nulls = stats
            .null_counts_for(&datafusion::common::Column::new_unqualified("value"))
            .unwrap();
        assert_eq!(nulls.data_type(), &DataType::UInt64);
        assert!(nulls.is_null(0));
        assert_eq!(
            prune(
                Arc::new(IsNullExpr::new(Arc::new(Column::new("value", 0)))),
                &query,
                arrow,
                &[file],
                &schemas,
            )
            .unwrap(),
            vec![true],
        );
    }

    #[test]
    fn decimal_and_temporal_defaults_require_the_exact_arrow_type() {
        let decimal = otmp_protocol::TypedScalar::Decimal {
            precision: 3,
            scale: 1,
            unscaled: vec![0, 123],
        };
        assert!(matches!(
            scalar(&decimal, &DataType::Decimal128(3, 1)),
            Some(ScalarValue::Decimal128(Some(123), 3, 1))
        ));
        assert!(scalar(&decimal, &DataType::Decimal128(4, 1)).is_none());
        assert!(matches!(
            scalar(
                &otmp_protocol::TypedScalar::TimestampMicros(1_234),
                &DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
            ),
            Some(ScalarValue::TimestampMicrosecond(Some(1_234), None))
        ));
        assert!(
            scalar(
                &otmp_protocol::TypedScalar::TimestampMicros(1_234),
                &DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
            )
            .is_none()
        );
    }
}
