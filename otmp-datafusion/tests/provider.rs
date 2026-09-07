use otmp_datafusion::{ProviderOptions, schema_to_arrow};
use otmp_protocol::{Field, LogicalType, Schema};
use std::collections::BTreeMap;
use std::sync::Arc;

#[test]
fn provider_uses_a_bounded_default_planning_budget() {
    assert_eq!(
        ProviderOptions::default().planning_budget_bytes,
        64 * 1024 * 1024
    );
}

#[test]
fn schema_mapping_preserves_nested_types_and_optional_nullability() {
    let schema = Schema {
        schema_id: 1,
        parent_schema_id: None,
        identifier_field_ids: vec![],
        doc: None,
        fields: vec![Field {
            field_id: 1,
            name: "payload".into(),
            required: false,
            doc: None,
            initial_default: None,
            write_default: None,
            field_type: LogicalType::Struct {
                fields: vec![Field {
                    field_id: 2,
                    name: "items".into(),
                    required: false,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                    field_type: LogicalType::List {
                        element: Box::new(Field {
                            field_id: 3,
                            name: "element".into(),
                            required: false,
                            doc: None,
                            initial_default: None,
                            write_default: None,
                            field_type: LogicalType::String,
                        }),
                    },
                }],
            },
        }],
    };
    let mapped = schema_to_arrow(&schema).unwrap();
    assert!(mapped.field(0).is_nullable());
    assert!(matches!(
        mapped.field(0).data_type(),
        datafusion::arrow::datatypes::DataType::Struct(_)
    ));
}

fn table_schema() -> Schema {
    Schema {
        schema_id: 1,
        parent_schema_id: None,
        identifier_field_ids: vec![1],
        doc: None,
        fields: vec![Field {
            field_id: 1,
            name: "id".into(),
            required: true,
            doc: None,
            initial_default: None,
            write_default: None,
            field_type: LogicalType::Int64,
        }],
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one real-file scenario verifies the complete planning and execution lifecycle"
)]
async fn scans_real_parquet_through_sql_with_filter_limit_and_aggregate() {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use otmp::{
        AppendFile, AppendRequest, FileFormat, InitializeRequest, LocalObjectStore,
        SourceFingerprint, Table,
    };
    use otmp_protocol::Sha256;

    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.parquet");
    let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let file = std::fs::File::create(&source).unwrap();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = std::fs::read(&source).unwrap();

    let table = Table::new(LocalObjectStore::new(directory.path()).unwrap());
    table
        .initialize(InitializeRequest::new(table_schema()))
        .await
        .unwrap();
    table
        .append_files(&AppendRequest::new(
            "parquet",
            vec![AppendFile {
                source_path: source,
                fingerprint: SourceFingerprint {
                    sha256: Sha256::digest(&bytes),
                    length: bytes.len() as u64,
                },
                format: FileFormat::Parquet,
                record_count: 3,
                schema_id: 1,
                partition_spec_id: 0,
                sort_order_id: 0,
                partition_values: BTreeMap::new(),
                metrics: vec![],
                metadata: BTreeMap::new(),
            }],
        ))
        .await
        .unwrap();

    let provider = otmp_datafusion::OtmpTableProvider::open(
        &table,
        otmp::MetadataSelection::Current,
        otmp::SnapshotSelection::Ref("main".into()),
        otmp::ReaderOptions::default(),
        ProviderOptions::default(),
    )
    .await
    .unwrap();
    let context = datafusion::prelude::SessionContext::new();
    context.register_table("t", Arc::new(provider)).unwrap();
    let result = context
        .sql("SELECT sum(id) AS total FROM t WHERE id >= 2 LIMIT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let total = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total.value(0), 5);

    // Each scan receives an independent immutable bridge identity. A self join
    // therefore cannot replace the left side's store with the right side's.
    let joined = context
        .sql("SELECT count(*) AS pairs FROM t a JOIN t b ON a.id = b.id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let pairs = joined[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(pairs.value(0), 3);

    let low_budget = otmp_datafusion::OtmpTableProvider::open(
        &table,
        otmp::MetadataSelection::Current,
        otmp::SnapshotSelection::Ref("main".into()),
        otmp::ReaderOptions::default(),
        ProviderOptions {
            planning_budget_bytes: 1,
            ..ProviderOptions::default()
        },
    )
    .await
    .unwrap();
    let constrained = datafusion::prelude::SessionContext::new();
    constrained
        .register_table("t", Arc::new(low_budget))
        .unwrap();
    let error = constrained
        .sql("SELECT id FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("descriptor budget exhausted"));
}
