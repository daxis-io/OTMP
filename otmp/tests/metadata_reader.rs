use otmp::{
    CommitMetadata, FileMetricRange, InMemoryObjectStore, InitializeRequest, MetadataSelection,
    OperationRequest, ReaderOptions, Requirement, RuntimeError, SnapshotSelection, Table,
    TransactionRequest,
};
use otmp_protocol::{Field, LogicalType, Schema};
use std::collections::BTreeMap;
use std::ops::Bound;

fn schema() -> Schema {
    serde_json::from_slice(include_bytes!("../../conformance/sources/schema.json")).unwrap()
}

#[tokio::test]
async fn registration_and_schema_refresh_preserve_the_old_pin() {
    let store = InMemoryObjectStore::default();
    let table = Table::new(store.clone());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let reads = store.read_count();
    let old = table
        .open_metadata_reader(
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
            ReaderOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(old.schema(), &schema());
    assert!(old.snapshot().is_none());
    assert!(old.files(None, &[], 256).await.unwrap().files.is_empty());
    assert_eq!(
        store.read_count(),
        reads,
        "registration and scans use bounded reads"
    );
    let mut next = schema();
    next.schema_id = 2;
    next.parent_schema_id = Some(1);
    next.fields.push(Field {
        field_id: 100,
        name: "added".into(),
        required: false,
        field_type: LogicalType::String,
        doc: None,
        initial_default: None,
        write_default: None,
    });
    table
        .transact(&TransactionRequest {
            idempotency_key: "schema".into(),
            requirements: vec![
                Requirement::CurrentSchemaIs { schema_id: 1 },
                Requirement::SchemaIdAbsent { schema_id: 2 },
                Requirement::FieldIdsAbsent {
                    field_ids: vec![100],
                },
            ],
            operations: vec![
                OperationRequest::AddSchema {
                    operation_id: "add".into(),
                    schema: next.clone(),
                },
                OperationRequest::SetCurrentSchema {
                    operation_id: "select".into(),
                    schema_id: 2,
                },
            ],
            commit_metadata: CommitMetadata::default(),
        })
        .await
        .unwrap();
    let current = table
        .open_metadata_reader(
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
            ReaderOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(current.schema(), &next);
    assert_eq!(old.schema(), &schema());
    assert_eq!(old.coordinates().table_version, 0);
    assert_eq!(current.coordinates().table_version, 1);
    assert_eq!(current.file_schema(1).await.unwrap().as_ref(), &schema());
    let historical = table
        .open_metadata_reader(
            MetadataSelection::TableVersion(0),
            SnapshotSelection::Ref("main".into()),
            ReaderOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(historical.anchor().table_version, 1);
    assert_eq!(historical.coordinates().table_version, 0);
    assert_eq!(historical.schema(), &schema());
    assert!(old.files(None, &[], 0).await.is_err());
    assert!(current.statistics().peak_cache_bytes <= ReaderOptions::default().cache_budget_bytes);
}

#[tokio::test]
async fn tiny_resource_budget_fails_explicitly() {
    let store = InMemoryObjectStore::default();
    let table = Table::new(store);
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let result = table
        .open_metadata_reader(
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
            ReaderOptions {
                cache_budget_bytes: 4096,
                checkpoint_window_bytes: 4096,
                ..ReaderOptions::default()
            },
        )
        .await;
    assert!(matches!(result, Err(RuntimeError::ResourceExhausted(_))));
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One fixture covers range binding, contradiction, and conservative fallback.
async fn metric_ranges_filter_before_descriptors_and_metric_reads() {
    let table = Table::new(InMemoryObjectStore::default());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"metric-range-fixture").unwrap();
    let files = [(0, 9), (100, 109)]
        .into_iter()
        .map(|(lower, upper)| otmp::AppendFile {
            source_path: source.path().into(),
            fingerprint: otmp::SourceFingerprint {
                sha256: otmp_protocol::Sha256::digest(b"metric-range-fixture"),
                length: 20,
            },
            format: otmp::FileFormat::Parquet,
            record_count: 10,
            schema_id: 1,
            partition_spec_id: 0,
            sort_order_id: 0,
            partition_values: BTreeMap::new(),
            metrics: vec![otmp::FileMetric {
                field_id: 1,
                column_size_bytes: None,
                value_count: Some(10),
                null_count: Some(0),
                nan_count: None,
                distinct_count: None,
                lower_bound: Some(otmp_protocol::TypedScalar::Int64(lower)),
                upper_bound: Some(otmp_protocol::TypedScalar::Int64(upper)),
                metadata: BTreeMap::new(),
            }],
            metadata: BTreeMap::from([(
                "lower".into(),
                otmp_protocol::CanonicalValue::Integer(i128::from(lower)),
            )]),
        })
        .collect();
    table
        .append_files(&otmp::AppendRequest::new("ranges", files))
        .await
        .unwrap();
    let reader = table
        .open_metadata_reader(
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
            ReaderOptions::default(),
        )
        .await
        .unwrap();
    let batch = reader
        .files_matching(
            None,
            &[1],
            &[FileMetricRange::Int64 {
                field_id: 1,
                lower: Bound::Excluded(50),
                upper: Bound::Unbounded,
            }],
            256,
        )
        .await
        .unwrap();
    assert_eq!(batch.files.len(), 1);
    assert_eq!(
        batch.files[0].metrics[0].lower_bound,
        Some(otmp_protocol::TypedScalar::Int64(100))
    );

    let mismatched = reader
        .files_matching(
            None,
            &[],
            &[FileMetricRange::Int32 {
                field_id: 1,
                lower: Bound::Included(50),
                upper: Bound::Unbounded,
            }],
            256,
        )
        .await
        .unwrap();
    assert_eq!(mismatched.files.len(), 2, "type mismatch must retain files");

    let impossible = reader
        .files_matching(
            None,
            &[],
            &[
                FileMetricRange::Int64 {
                    field_id: 1,
                    lower: Bound::Included(100),
                    upper: Bound::Unbounded,
                },
                FileMetricRange::Int64 {
                    field_id: 1,
                    lower: Bound::Unbounded,
                    upper: Bound::Excluded(100),
                },
            ],
            256,
        )
        .await
        .unwrap();
    assert!(impossible.files.is_empty());
    assert!(impossible.next_cursor.is_none());

    let cursor = batch.next_cursor.unwrap();
    let Err(changed_range) = reader.files(Some(cursor), &[], 256).await else {
        panic!("cursor accepted a changed range set");
    };
    assert!(changed_range.to_string().contains("range set changed"));
}

async fn append_many(table: &Table<InMemoryObjectStore>, count: usize, key: &str, branch: &str) {
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"metadata-membership-fixture").unwrap();
    let file = otmp::AppendFile {
        source_path: source.path().into(),
        fingerprint: otmp::SourceFingerprint {
            sha256: otmp_protocol::Sha256::digest(b"metadata-membership-fixture"),
            length: 27,
        },
        format: otmp::FileFormat::Parquet,
        record_count: 3,
        schema_id: 1,
        partition_spec_id: 0,
        sort_order_id: 0,
        partition_values: BTreeMap::default(),
        metrics: vec![],
        metadata: BTreeMap::default(),
    };
    let files = (0..count)
        .map(|i| {
            let mut f = file.clone();
            f.metadata.insert(
                "ordinal".into(),
                otmp_protocol::CanonicalValue::Integer(i as i128),
            );
            f
        })
        .collect();
    let mut request = otmp::AppendRequest::new(key, files);
    request.target_ref = branch.into();
    table.append_files(&request).await.unwrap();
}

async fn list(
    reader: &otmp::MetadataReader<InMemoryObjectStore>,
    limit: usize,
) -> (Vec<otmp_protocol::Id>, bool) {
    let mut ids = Vec::new();
    let mut cursor = None;
    let mut empty_continuation = false;
    for _ in 0..1000 {
        let batch = reader.files(cursor, &[], limit).await.unwrap();
        empty_continuation |= batch.files.is_empty() && batch.next_cursor.is_some();
        ids.extend(batch.files.iter().map(|f| f.file.file_id));
        cursor = batch.next_cursor;
        if cursor.is_none() {
            ids.sort();
            return (ids, empty_continuation);
        }
    }
    panic!("file cursor failed to terminate");
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one oracle comparison covers the shared branch and history lifecycle"
)]
async fn file_batches_match_sqlite_for_branches_tags_and_history() {
    let table = Table::new(InMemoryObjectStore::default());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    append_many(&table, 260, "first", "main").await;
    let old = table
        .open_metadata_reader(
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
            ReaderOptions::default(),
        )
        .await
        .unwrap();
    let first = old.snapshot().unwrap().snapshot_id;
    table
        .transact(&TransactionRequest {
            idempotency_key: "refs".into(),
            requirements: vec![
                Requirement::RefAbsent { name: "dev".into() },
                Requirement::RefAbsent { name: "tag".into() },
                Requirement::SnapshotExists { snapshot_id: first },
            ],
            operations: vec![
                OperationRequest::CreateRef {
                    operation_id: "branch".into(),
                    name: "dev".into(),
                    ref_type: otmp::RefType::Branch,
                    snapshot_id: Some(first),
                },
                OperationRequest::CreateRef {
                    operation_id: "tag".into(),
                    name: "tag".into(),
                    ref_type: otmp::RefType::Tag,
                    snapshot_id: Some(first),
                },
            ],
            commit_metadata: CommitMetadata::default(),
        })
        .await
        .unwrap();
    append_many(&table, 1, "main-second", "main").await;
    append_many(&table, 2, "dev-second", "dev").await;
    assert_eq!(list(&old, 256).await.0.len(), 260);
    let pin = table
        .pin_metadata(MetadataSelection::Current)
        .await
        .unwrap();
    for selection in [
        SnapshotSelection::Ref("main".into()),
        SnapshotSelection::Ref("dev".into()),
        SnapshotSelection::Ref("tag".into()),
        SnapshotSelection::SnapshotId(first),
        SnapshotSelection::SequenceNumber(1),
    ] {
        let reader = table
            .open_metadata_reader(
                MetadataSelection::Current,
                selection.clone(),
                ReaderOptions::default(),
            )
            .await
            .unwrap();
        let mut expected: Vec<_> = pin
            .resolve_snapshot(selection)
            .unwrap()
            .files()
            .unwrap()
            .into_iter()
            .map(|f| f.file_id)
            .collect();
        expected.sort();
        assert_eq!(list(&reader, 256).await.0, expected);
        let first_batch = reader.files(None, &[], 1).await.unwrap();
        if let Some(cursor) = first_batch.next_cursor {
            assert!(
                old.files(Some(cursor), &[], 1).await.is_err(),
                "cursor must retain its originating pin"
            );
        }
    }
    let dev = table
        .open_metadata_reader(
            MetadataSelection::Current,
            SnapshotSelection::Ref("dev".into()),
            ReaderOptions::default(),
        )
        .await
        .unwrap();
    let historical = table
        .open_metadata_reader(
            MetadataSelection::Current,
            SnapshotSelection::SnapshotId(dev.snapshot().unwrap().snapshot_id),
            ReaderOptions::default(),
        )
        .await
        .unwrap();
    let (ids, empty) = list(&historical, 1).await;
    assert_eq!(ids.len(), 262);
    assert!(
        empty,
        "an empty raw batch must advance to snapshot ancestry"
    );
    let metadata_history = table
        .open_metadata_reader(
            MetadataSelection::TableVersion(1),
            SnapshotSelection::Ref("main".into()),
            ReaderOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(list(&metadata_history, 256).await.0.len(), 260);
    assert_eq!(metadata_history.coordinates().table_version, 1);
    assert_eq!(metadata_history.anchor().table_version, 4);
}
