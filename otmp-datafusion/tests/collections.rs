use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ListArray, StructArray};
use datafusion::arrow::array::{ListBuilder, MapBuilder, StringBuilder};
use datafusion::arrow::datatypes::{Field as ArrowField, Int64Type, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::SessionContext;
use otmp::{
    AppendFile, AppendRequest, FileFormat, InitializeRequest, LocalObjectStore, MetadataSelection,
    ReaderOptions, SnapshotSelection, SourceFingerprint, Table,
};
use otmp_datafusion::{OtmpTableProvider, ProviderOptions, schema_to_arrow};
use otmp_protocol::{Field, LogicalType, Schema, Sha256};

fn field(id: u32, name: &str, required: bool, field_type: LogicalType) -> Field {
    Field {
        field_id: id,
        name: name.into(),
        required,
        doc: None,
        initial_default: None,
        write_default: None,
        field_type,
    }
}

fn collection_schema(element_name: &str) -> Schema {
    Schema {
        schema_id: 1,
        parent_schema_id: None,
        identifier_field_ids: vec![],
        doc: None,
        fields: vec![
            field(
                1,
                "numbers",
                false,
                LogicalType::List {
                    element: Box::new(field(2, element_name, false, LogicalType::Int64)),
                },
            ),
            field(
                3,
                "labels",
                false,
                LogicalType::Map {
                    key: Box::new(field(4, "key", true, LogicalType::String)),
                    value: Box::new(field(5, "value", false, LogicalType::String)),
                },
            ),
            field(
                6,
                "profile",
                false,
                LogicalType::Struct {
                    fields: vec![field(
                        7,
                        "nested_numbers",
                        false,
                        LogicalType::List {
                            element: Box::new(field(8, element_name, false, LogicalType::String)),
                        },
                    )],
                },
            ),
        ],
    }
}

fn write_collections(path: &std::path::Path, wrong_list_child_id: bool) {
    let numbers = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![Some(3)]),
    ]);
    let numbers = if wrong_list_child_id {
        let (_, offsets, values, nulls) = numbers.into_parts();
        let child = Arc::new(
            ArrowField::new("item", values.data_type().clone(), true).with_metadata(
                std::collections::HashMap::from([("PARQUET:field_id".to_owned(), "99".to_owned())]),
            ),
        );
        ListArray::new(child, offsets, values, nulls)
    } else {
        numbers
    };
    let mut labels = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    labels.keys().append_value("a");
    labels.values().append_value("one");
    labels.append(true).unwrap();
    labels.append(true).unwrap();
    labels.keys().append_value("b");
    labels.values().append_value("two");
    labels.append(true).unwrap();
    let labels = labels.finish();
    let mut nested = ListBuilder::new(StringBuilder::new());
    nested.values().append_value("x");
    nested.values().append_value("y");
    nested.append(true);
    nested.append(false);
    nested.values().append_value("z");
    nested.append(true);
    let nested = nested.finish();
    let profile = StructArray::new(
        vec![Arc::new(ArrowField::new(
            "nested_numbers",
            nested.data_type().clone(),
            true,
        ))]
        .into(),
        vec![Arc::new(nested)],
        None,
    );
    // This deliberately has no PARQUET:field_id metadata. The provider must bind
    // names through the file's recorded schema rather than the latest query schema.
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("numbers", numbers.data_type().clone(), true),
        ArrowField::new("labels", labels.data_type().clone(), true),
        ArrowField::new("profile", profile.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(numbers), Arc::new(labels), Arc::new(profile)],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

async fn provider(
    table: &Table<LocalObjectStore>,
    file_pruning: bool,
) -> OtmpTableProvider<LocalObjectStore> {
    OtmpTableProvider::open(
        table,
        MetadataSelection::Current,
        SnapshotSelection::Ref("main".into()),
        ReaderOptions::default(),
        ProviderOptions {
            file_pruning,
            ..ProviderOptions::default()
        },
    )
    .await
    .unwrap()
}

async fn query(provider: OtmpTableProvider<LocalObjectStore>) -> Vec<RecordBatch> {
    let context = SessionContext::new();
    context.register_table("t", Arc::new(provider)).unwrap();
    context
        .sql("SELECT numbers, labels, profile.nested_numbers FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
}

async fn append(table: &Table<LocalObjectStore>, source: std::path::PathBuf) {
    let bytes = std::fs::read(&source).unwrap();
    table
        .append_files(&AppendRequest::new(
            "collections",
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
}

#[tokio::test]
async fn id_free_parquet_collections_round_trip_through_recorded_names() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("collections.parquet");
    write_collections(&source, false);
    let table = Table::new(LocalObjectStore::new(directory.path()).unwrap());
    table
        .initialize(InitializeRequest::new(collection_schema("element")))
        .await
        .unwrap();
    append(&table, source).await;

    let optimized = query(provider(&table, true).await).await;
    let unpruned = query(provider(&table, false).await).await;
    assert_eq!(format!("{optimized:?}"), format!("{unpruned:?}"));
    let batch = &optimized[0];
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        array_value_to_string(batch.column(0).as_ref(), 0).unwrap(),
        "[1, 2]"
    );
    assert_eq!(
        array_value_to_string(batch.column(0).as_ref(), 1).unwrap(),
        ""
    );
    assert_eq!(
        array_value_to_string(batch.column(1).as_ref(), 0).unwrap(),
        "{a: one}"
    );
    assert_eq!(
        array_value_to_string(batch.column(1).as_ref(), 1).unwrap(),
        "{}"
    );
    assert_eq!(
        array_value_to_string(batch.column(2).as_ref(), 2).unwrap(),
        "[z]"
    );
}

#[tokio::test]
async fn parquet_canonical_list_element_name_round_trips_through_its_recorded_role() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("canonical-element.parquet");
    write_collections(&source, false);
    let table = Table::new(LocalObjectStore::new(directory.path()).unwrap());
    table
        .initialize(InitializeRequest::new(collection_schema(
            "recorded_element",
        )))
        .await
        .unwrap();
    append(&table, source).await;
    let batches = query(provider(&table, true).await).await;
    assert_eq!(batches[0].num_rows(), 3);
    assert_eq!(
        array_value_to_string(batches[0].column(0).as_ref(), 0).unwrap(),
        "[1, 2]"
    );
}

#[tokio::test]
async fn collection_child_physical_id_must_match_the_recorded_schema() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("wrong-child-id.parquet");
    write_collections(&source, true);
    let table = Table::new(LocalObjectStore::new(directory.path()).unwrap());
    table
        .initialize(InitializeRequest::new(collection_schema("element")))
        .await
        .unwrap();
    append(&table, source).await;
    let context = SessionContext::new();
    context
        .register_table("t", Arc::new(provider(&table, true).await))
        .unwrap();
    let error = context
        .sql("SELECT numbers FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("collection field ID disagrees with recorded schema"),
        "unexpected error: {error}"
    );
}

#[test]
fn excessive_decimal_precision_is_rejected_before_provider_construction() {
    let schema = Schema {
        schema_id: 1,
        parent_schema_id: None,
        identifier_field_ids: vec![],
        doc: None,
        fields: vec![field(
            1,
            "too_wide",
            false,
            LogicalType::Decimal {
                precision: 77,
                scale: 0,
            },
        )],
    };
    let error = schema_to_arrow(&schema).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("decimal precision exceeds Arrow Decimal256")
    );
}
