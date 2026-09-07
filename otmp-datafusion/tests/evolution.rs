use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Fields, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::SessionContext;
use otmp::{
    AppendFile, AppendRequest, CommitMetadata, FileFormat, InitializeRequest, LocalObjectStore,
    MetadataSelection, OperationRequest, ReaderOptions, Requirement, SnapshotSelection,
    SourceFingerprint, Table, TransactionRequest,
};
use otmp_datafusion::{OtmpTableProvider, ProviderOptions};
use otmp_protocol::{Field, LogicalType, Schema, Sha256, TypedScalar};

fn schema_one() -> Schema {
    Schema {
        schema_id: 1,
        parent_schema_id: None,
        identifier_field_ids: vec![1],
        doc: None,
        fields: vec![
            Field {
                field_id: 1,
                name: "id".into(),
                required: true,
                doc: None,
                initial_default: None,
                write_default: None,
                field_type: LogicalType::Int64,
            },
            Field {
                field_id: 2,
                name: "payload".into(),
                required: false,
                doc: None,
                initial_default: None,
                write_default: None,
                field_type: LogicalType::Struct {
                    fields: vec![Field {
                        field_id: 3,
                        name: "old_child".into(),
                        required: false,
                        doc: None,
                        initial_default: None,
                        write_default: None,
                        field_type: LogicalType::String,
                    }],
                },
            },
        ],
    }
}

fn schema_two() -> Schema {
    let mut schema = schema_one();
    schema.schema_id = 2;
    schema.parent_schema_id = Some(1);
    if let LogicalType::Struct { fields } = &mut schema.fields[1].field_type {
        fields.push(Field {
            field_id: 5,
            name: "new_child".into(),
            required: false,
            doc: None,
            initial_default: None,
            write_default: None,
            field_type: LogicalType::String,
        });
    }
    schema.fields.push(Field {
        field_id: 4,
        name: "state".into(),
        required: false,
        doc: None,
        initial_default: Some(TypedScalar::String("ready".into())),
        write_default: None,
        field_type: LogicalType::String,
    });
    schema
}

type Row<'a> = (i64, Option<&'a str>, Option<&'a str>, Option<&'a str>);

fn write_file(path: &std::path::Path, rows: &[Row<'_>], new: bool) {
    let child = Fields::from(vec![
        Arc::new(ArrowField::new("old_child", DataType::Utf8, true)),
        Arc::new(ArrowField::new("new_child", DataType::Utf8, true)),
    ]);
    let fields = if new {
        vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("payload", DataType::Struct(child), true),
            ArrowField::new("state", DataType::Utf8, true),
        ]
    } else {
        vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new(
                "payload",
                DataType::Struct(Fields::from(vec![Arc::new(ArrowField::new(
                    "old_child",
                    DataType::Utf8,
                    true,
                ))])),
                true,
            ),
        ]
    };
    let schema = Arc::new(ArrowSchema::new(fields));
    let ids: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
    ));
    let old: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.1).collect::<Vec<_>>(),
    ));
    let payload: ArrayRef = if new {
        Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(ArrowField::new("old_child", DataType::Utf8, true)),
                Arc::new(ArrowField::new("new_child", DataType::Utf8, true)),
            ]),
            vec![
                old,
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
            None,
        ))
    } else {
        Arc::new(StructArray::new(
            Fields::from(vec![Arc::new(ArrowField::new(
                "old_child",
                DataType::Utf8,
                true,
            ))]),
            vec![old],
            None,
        ))
    };
    let mut columns = vec![ids, payload];
    if new {
        columns.push(Arc::new(StringArray::from(
            rows.iter().map(|r| r.3).collect::<Vec<_>>(),
        )));
    }
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

async fn append(
    table: &Table<LocalObjectStore>,
    path: std::path::PathBuf,
    schema_id: u32,
    key: &str,
) {
    let bytes = std::fs::read(&path).unwrap();
    table
        .append_files(&AppendRequest::new(
            key,
            vec![AppendFile {
                source_path: path,
                fingerprint: SourceFingerprint {
                    sha256: Sha256::digest(&bytes),
                    length: bytes.len() as u64,
                },
                format: FileFormat::Parquet,
                record_count: 2,
                schema_id,
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

async fn query(
    provider: OtmpTableProvider<LocalObjectStore>,
    sql: &str,
) -> Vec<datafusion::arrow::record_batch::RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

async fn query_error(provider: OtmpTableProvider<LocalObjectStore>, sql: &str) -> String {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    match ctx.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(dataframe) => dataframe.collect().await.unwrap_err().to_string(),
    }
}

async fn planning_error(provider: OtmpTableProvider<LocalObjectStore>, sql: &str) -> String {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    ctx.sql(sql).await.unwrap_err().to_string()
}

async fn open_provider(
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

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one schema evolution scenario checks retained pins and optimized parity"
)]
async fn schema_evolution_keeps_old_provider_pinned_and_matches_unpruned_scans() {
    let dir = tempfile::tempdir().unwrap();
    let old = dir.path().join("old.parquet");
    let new = dir.path().join("new.parquet");
    write_file(
        &old,
        &[(1, Some("one"), None, None), (2, None, None, None)],
        false,
    );
    let table = Table::new(LocalObjectStore::new(dir.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema_one()))
        .await
        .unwrap();
    append(&table, old, 1, "old").await;
    let retained = OtmpTableProvider::open(
        &table,
        MetadataSelection::Current,
        SnapshotSelection::Ref("main".into()),
        ReaderOptions::default(),
        ProviderOptions::default(),
    )
    .await
    .unwrap();
    let next = schema_two();
    table
        .transact(&TransactionRequest {
            idempotency_key: "schema2".into(),
            requirements: vec![
                Requirement::CurrentSchemaIs { schema_id: 1 },
                Requirement::SchemaIdAbsent { schema_id: 2 },
                Requirement::FieldIdsAbsent {
                    field_ids: vec![4, 5],
                },
            ],
            operations: vec![
                OperationRequest::AddSchema {
                    operation_id: "add".into(),
                    schema: next,
                },
                OperationRequest::SetCurrentSchema {
                    operation_id: "set".into(),
                    schema_id: 2,
                },
            ],
            commit_metadata: CommitMetadata::default(),
        })
        .await
        .unwrap();
    write_file(
        &new,
        &[
            (3, Some("three"), Some("nested"), Some("live")),
            (4, Some("four"), None, Some("live")),
        ],
        true,
    );
    append(&table, new, 2, "new").await;
    let optimized = open_provider(&table, true).await;
    let unpruned = open_provider(&table, false).await;
    let sql = "SELECT id, state, payload.old_child, payload.new_child FROM t WHERE id >= 1 ORDER BY id LIMIT 10";
    let a = query(optimized, sql).await;
    let b = query(unpruned, sql).await;
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
    let batch = concat_batches(&a[0].schema(), &a).unwrap();
    let state = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let new_child = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        state.iter().collect::<Vec<_>>(),
        vec![Some("ready"), Some("ready"), Some("live"), Some("live")]
    );
    assert_eq!(
        new_child.iter().collect::<Vec<_>>(),
        vec![None, None, Some("nested"), None]
    );

    let aggregate =
        "SELECT count(*) AS rows, sum(id) AS id_sum, count(state) AS states FROM t WHERE id >= 1";
    assert_eq!(
        format!(
            "{:?}",
            query(open_provider(&table, true).await, aggregate).await
        ),
        format!(
            "{:?}",
            query(open_provider(&table, false).await, aggregate).await
        ),
    );
    let self_join =
        "SELECT a.id, a.state, b.payload.new_child FROM t a JOIN t b ON a.id = b.id ORDER BY a.id";
    assert_eq!(
        format!(
            "{:?}",
            query(open_provider(&table, true).await, self_join).await
        ),
        format!(
            "{:?}",
            query(open_provider(&table, false).await, self_join).await
        ),
    );
    let unknown = planning_error(
        open_provider(&table, true).await,
        "SELECT does_not_exist FROM t",
    )
    .await;
    assert!(
        unknown.contains("does_not_exist"),
        "unexpected error: {unknown}"
    );
    let old_rows = query(retained, "SELECT id, payload.old_child FROM t ORDER BY id").await;
    assert_eq!(old_rows[0].num_rows(), 2);
}

fn write_missing_id_file(path: &std::path::Path) {
    let payload = StructArray::new(
        Fields::from(vec![Arc::new(ArrowField::new(
            "old_child",
            DataType::Utf8,
            true,
        ))]),
        vec![Arc::new(StringArray::from(vec![Some("orphan")]))],
        None,
    );
    write_batch(
        path,
        vec![ArrowField::new(
            "payload",
            DataType::Struct(payload.fields().clone()),
            true,
        )],
        vec![Arc::new(payload)],
    );
}

fn write_duplicate_id_file(path: &std::path::Path) {
    let duplicate_id =
        std::collections::HashMap::from([("PARQUET:field_id".to_owned(), "1".to_owned())]);
    write_batch(
        path,
        vec![
            ArrowField::new("first", DataType::Int64, false).with_metadata(duplicate_id.clone()),
            ArrowField::new("second", DataType::Int64, false).with_metadata(duplicate_id),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
        ],
    );
}

fn write_batch(path: &std::path::Path, fields: Vec<ArrowField>, columns: Vec<ArrayRef>) {
    let schema = Arc::new(ArrowSchema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test]
async fn rejects_invalid_physical_schemas_even_when_sql_projects_them_away() {
    for (name, writer, expected) in [
        (
            "missing-id",
            write_missing_id_file as fn(&std::path::Path),
            "missing required OTMP field id",
        ),
        (
            "duplicate-id",
            write_duplicate_id_file as fn(&std::path::Path),
            "ambiguous duplicate Parquet field ID",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(format!("{name}.parquet"));
        writer(&file);
        let table = Table::new(LocalObjectStore::new(dir.path()).unwrap());
        table
            .initialize(InitializeRequest::new(schema_one()))
            .await
            .unwrap();
        append(&table, file, 1, name).await;
        let error = query_error(
            open_provider(&table, true).await,
            "SELECT payload.old_child FROM t",
        )
        .await;
        assert!(
            error.contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}
