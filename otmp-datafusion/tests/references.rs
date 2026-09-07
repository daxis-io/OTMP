use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::SessionContext;
use otmp::{
    AppendFile, AppendRequest, CommitMetadata, FileFormat, InitializeRequest, LocalObjectStore,
    MetadataSelection, OperationRequest, ReaderOptions, RefType, Requirement, SnapshotSelection,
    SourceFingerprint, Table, TransactionRequest,
};
use otmp_datafusion::{OtmpTableProvider, ProviderOptions};
use otmp_protocol::{Field, LogicalType, Schema, Sha256};

fn schema() -> Schema {
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

fn write_parquet(path: &std::path::Path, ids: &[i64]) {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

async fn append(
    table: &Table<LocalObjectStore>,
    path: std::path::PathBuf,
    ids: &[i64],
    target: &str,
) {
    let bytes = std::fs::read(&path).unwrap();
    let key = path.file_stem().unwrap().to_string_lossy().into_owned();
    let mut request = AppendRequest::new(
        key,
        vec![AppendFile {
            source_path: path,
            fingerprint: SourceFingerprint {
                sha256: Sha256::digest(&bytes),
                length: bytes.len() as u64,
            },
            format: FileFormat::Parquet,
            record_count: ids.len() as u64,
            schema_id: 1,
            partition_spec_id: 0,
            sort_order_id: 0,
            partition_values: BTreeMap::new(),
            metrics: vec![],
            metadata: BTreeMap::new(),
        }],
    );
    request.target_ref = target.into();
    table.append_files(&request).await.unwrap();
}

async fn provider(
    table: &Table<LocalObjectStore>,
    metadata: MetadataSelection,
    snapshot: SnapshotSelection,
    file_pruning: bool,
) -> OtmpTableProvider<LocalObjectStore> {
    OtmpTableProvider::open(
        table,
        metadata,
        snapshot,
        ReaderOptions::default(),
        ProviderOptions {
            file_pruning,
            ..ProviderOptions::default()
        },
    )
    .await
    .unwrap()
}

async fn count_and_sum(provider: OtmpTableProvider<LocalObjectStore>) -> (i64, Option<i64>) {
    let context = SessionContext::new();
    context.register_table("t", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT count(*) AS rows, sum(id) AS total FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let total = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (rows.value(0), total.is_valid(0).then(|| total.value(0)))
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one real-file scenario verifies the complete planning and execution lifecycle"
)]
async fn real_parquet_references_and_history_keep_sql_selection_pins() {
    let directory = tempfile::tempdir().unwrap();
    let initial = directory.path().join("initial.parquet");
    let main = directory.path().join("main.parquet");
    let dev = directory.path().join("dev.parquet");
    write_parquet(&initial, &[1, 2]);
    write_parquet(&main, &[10]);
    write_parquet(&dev, &[20, 21]);

    let table = Table::new(LocalObjectStore::new(directory.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    assert_eq!(
        count_and_sum(
            provider(
                &table,
                MetadataSelection::Current,
                SnapshotSelection::Ref("main".into()),
                true,
            )
            .await,
        )
        .await,
        (0, None),
        "the genesis snapshot is a SQL-visible empty table"
    );

    append(&table, initial, &[1, 2], "main").await;
    let retained = provider(
        &table,
        MetadataSelection::Current,
        SnapshotSelection::Ref("main".into()),
        true,
    )
    .await;
    let initial_snapshot = retained.reader().snapshot().unwrap().snapshot_id;
    table
        .transact(&TransactionRequest {
            idempotency_key: "create-refs".into(),
            requirements: vec![
                Requirement::RefAbsent { name: "dev".into() },
                Requirement::RefAbsent {
                    name: "frozen".into(),
                },
                Requirement::SnapshotExists {
                    snapshot_id: initial_snapshot,
                },
            ],
            operations: vec![
                OperationRequest::CreateRef {
                    operation_id: "dev".into(),
                    name: "dev".into(),
                    ref_type: RefType::Branch,
                    snapshot_id: Some(initial_snapshot),
                },
                OperationRequest::CreateRef {
                    operation_id: "tag".into(),
                    name: "frozen".into(),
                    ref_type: RefType::Tag,
                    snapshot_id: Some(initial_snapshot),
                },
            ],
            commit_metadata: CommitMetadata::default(),
        })
        .await
        .unwrap();

    let (old_result, ()) =
        tokio::join!(count_and_sum(retained), append(&table, main, &[10], "main"));
    assert_eq!(
        old_result,
        (2, Some(3)),
        "concurrent publication cannot move an existing provider pin"
    );
    append(&table, dev, &[20, 21], "dev").await;

    let selections = vec![
        (
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
            (3, Some(13)),
        ),
        (
            MetadataSelection::Current,
            SnapshotSelection::Ref("dev".into()),
            (4, Some(44)),
        ),
        (
            MetadataSelection::Current,
            SnapshotSelection::Ref("frozen".into()),
            (2, Some(3)),
        ),
        (
            MetadataSelection::Current,
            SnapshotSelection::SnapshotId(initial_snapshot),
            (2, Some(3)),
        ),
        (
            MetadataSelection::Current,
            SnapshotSelection::SequenceNumber(1),
            (2, Some(3)),
        ),
        (
            MetadataSelection::TableVersion(1),
            SnapshotSelection::Ref("main".into()),
            (2, Some(3)),
        ),
    ];
    for (metadata, snapshot, expected) in selections {
        let optimized = provider(&table, metadata, snapshot.clone(), true).await;
        if metadata == MetadataSelection::TableVersion(1) {
            assert_eq!(optimized.reader().coordinates().table_version, 1);
            assert_eq!(optimized.reader().anchor().table_version, 4);
        }
        assert_eq!(
            count_and_sum(optimized).await,
            expected,
            "optimized {snapshot:?}"
        );
        assert_eq!(
            count_and_sum(provider(&table, metadata, snapshot.clone(), false).await).await,
            expected,
            "unpruned {snapshot:?}"
        );
    }
}
