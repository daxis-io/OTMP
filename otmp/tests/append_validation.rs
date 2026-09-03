use std::collections::BTreeMap;

use otmp::{
    AppendFile, AppendRequest, FileFormat, FileMetric, InitializeRequest, LocalObjectStore,
    SourceFingerprint, Table,
};
use otmp_protocol::{CanonicalValue, Field, LogicalType, Schema, Sha256, TypedScalar};

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        parent_schema_id: None,
        fields: vec![Field {
            field_id: 1,
            name: "id".into(),
            required: true,
            field_type: LogicalType::Int64,
            doc: None,
            initial_default: None,
            write_default: None,
        }],
        identifier_field_ids: vec![1],
        doc: None,
    }
}

fn file(path: std::path::PathBuf) -> AppendFile {
    AppendFile {
        source_path: path,
        fingerprint: SourceFingerprint {
            sha256: Sha256::digest(b"source is intentionally absent"),
            length: 30,
        },
        format: FileFormat::Parquet,
        record_count: 1,
        schema_id: 1,
        partition_spec_id: 0,
        sort_order_id: 0,
        partition_values: BTreeMap::new(),
        metrics: Vec::new(),
        metadata: BTreeMap::new(),
    }
}

fn metric() -> FileMetric {
    FileMetric {
        field_id: 1,
        column_size_bytes: Some(8),
        value_count: Some(1),
        null_count: Some(0),
        nan_count: None,
        distinct_count: Some(1),
        lower_bound: Some(TypedScalar::Int64(1)),
        upper_bound: Some(TypedScalar::Int64(2)),
        metadata: BTreeMap::new(),
    }
}

async fn initialized() -> (tempfile::TempDir, Table<LocalObjectStore>) {
    let directory = tempfile::tempdir().unwrap();
    let table = Table::new(LocalObjectStore::new(directory.path().join("table")).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    (directory, table)
}

async fn rejects_before_source_io(mut request: AppendRequest) {
    let (_directory, table) = initialized().await;
    request.idempotency_key = format!("invalid-{}", request.idempotency_key);
    let error = table.append_files(&request).await.unwrap_err();
    assert_eq!(error.code(), "OTMP_INVALID_APPEND", "{error}");
    assert_eq!(table.pin().await.unwrap().status().table_version, 0);
}

#[tokio::test]
async fn rejects_reserved_summary_duplicate_files_and_wrong_defaults_before_source_io() {
    let missing = std::path::PathBuf::from("source-does-not-exist.parquet");

    let mut request = AppendRequest::new("reserved", vec![file(missing.clone())]);
    request.summary.insert(
        "added-data-files".into(),
        CanonicalValue::String("9".into()),
    );
    rejects_before_source_io(request).await;

    let duplicate = file(missing.clone());
    rejects_before_source_io(AppendRequest::new(
        "duplicate",
        vec![duplicate.clone(), duplicate],
    ))
    .await;

    let mut wrong_schema = file(missing);
    wrong_schema.schema_id = 2;
    rejects_before_source_io(AppendRequest::new("defaults", vec![wrong_schema])).await;
}

#[tokio::test]
async fn rejects_invalid_metric_assertions_before_source_io() {
    let missing = std::path::PathBuf::from("source-does-not-exist.parquet");
    let invalid_metrics = [
        {
            let mut value = metric();
            value.field_id = 99;
            vec![value]
        },
        vec![metric(), metric()],
        {
            let mut value = metric();
            value.null_count = Some(2);
            vec![value]
        },
        {
            let mut value = metric();
            value.nan_count = Some(0);
            vec![value]
        },
        {
            let mut value = metric();
            value.lower_bound = Some(TypedScalar::String("wrong type".into()));
            vec![value]
        },
        {
            let mut value = metric();
            value.lower_bound = Some(TypedScalar::Int64(3));
            value.upper_bound = Some(TypedScalar::Int64(2));
            vec![value]
        },
        {
            let mut value = metric();
            value.value_count = Some(i64::MAX as u64 + 1);
            vec![value]
        },
    ];

    for (index, metrics) in invalid_metrics.into_iter().enumerate() {
        let mut entry = file(missing.clone());
        entry.metrics = metrics;
        rejects_before_source_io(AppendRequest::new(format!("metric-{index}"), vec![entry])).await;
    }
}

#[tokio::test]
async fn source_larger_than_declared_length_stops_and_cleans_staging() {
    let (directory, table) = initialized().await;
    let source = directory.path().join("too-long.parquet");
    tokio::fs::write(&source, b"abcdef").await.unwrap();
    let mut entry = file(source);
    entry.fingerprint = SourceFingerprint {
        sha256: Sha256::digest(b"abc"),
        length: 3,
    };

    let error = table
        .append_files(&AppendRequest::new("too-long", vec![entry]))
        .await
        .unwrap_err();

    assert_eq!(error.code(), "OTMP_FINGERPRINT_MISMATCH");
    assert_eq!(table.pin().await.unwrap().status().table_version, 0);
    let mut entries = tokio::fs::read_dir(directory.path().join("table/data"))
        .await
        .unwrap();
    assert!(entries.next_entry().await.unwrap().is_none());
}
