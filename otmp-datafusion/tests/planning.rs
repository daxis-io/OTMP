use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::SessionContext;
use otmp::{
    AppendFile, AppendRequest, FileFormat, FileMetric, InitializeRequest, LocalObjectStore,
    MetadataSelection, ReaderOptions, SnapshotSelection, SourceFingerprint, Table,
};
use otmp_datafusion::{OtmpTableProvider, ProviderOptions};
use otmp_protocol::{CanonicalValue, Field, LogicalType, Schema, Sha256, TypedScalar};

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
            field_type: LogicalType::Int64,
            doc: None,
            initial_default: None,
            write_default: None,
        }],
    }
}

fn write_parquet(path: &std::path::Path, values: Vec<i64>) -> Vec<u8> {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    std::fs::read(path).unwrap()
}

fn append_file(
    path: &std::path::Path,
    bytes: &[u8],
    min: i64,
    max: i64,
    ordinal: usize,
) -> AppendFile {
    AppendFile {
        source_path: path.into(),
        fingerprint: SourceFingerprint {
            sha256: Sha256::digest(bytes),
            length: bytes.len() as u64,
        },
        format: FileFormat::Parquet,
        // This deliberately lies: scan statistics must not replace Parquet
        // execution with a metadata-only result.
        record_count: 999,
        schema_id: 1,
        partition_spec_id: 0,
        sort_order_id: 0,
        partition_values: BTreeMap::new(),
        metrics: vec![FileMetric {
            field_id: 1,
            column_size_bytes: None,
            value_count: None,
            null_count: Some(0),
            nan_count: None,
            distinct_count: None,
            lower_bound: Some(TypedScalar::Int64(min)),
            upper_bound: Some(TypedScalar::Int64(max)),
            metadata: BTreeMap::new(),
        }],
        metadata: BTreeMap::from([("ordinal".into(), CanonicalValue::Integer(ordinal as i128))]),
    }
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

async fn count(provider: OtmpTableProvider<LocalObjectStore>) -> i64 {
    let context = SessionContext::new();
    context.register_table("t", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT count(*) AS rows FROM t WHERE id >= 100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn metric_pruning_keeps_projected_away_filter_correct_across_a_full_raw_batch() {
    let directory = tempfile::tempdir().unwrap();
    let low_path = directory.path().join("low.parquet");
    let high_path = directory.path().join("high.parquet");
    let low_bytes = write_parquet(&low_path, vec![1, 2, 3]);
    let high_bytes = write_parquet(&high_path, vec![100, 101]);
    let table = Table::new(LocalObjectStore::new(directory.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();

    // One append creates a 256-file raw metadata page. All those files are
    // pruned, so the reader must use the unfiltered continuation to reach the
    // high file in the next snapshot.
    let low_files = (0..256)
        .map(|ordinal| append_file(&low_path, &low_bytes, 1, 3, ordinal))
        .collect();
    table
        .append_files(&AppendRequest::new("low-batch", low_files))
        .await
        .unwrap();
    table
        .append_files(&AppendRequest::new(
            "high-file",
            vec![append_file(&high_path, &high_bytes, 100, 101, 256)],
        ))
        .await
        .unwrap();

    let optimized = provider(&table, true).await;
    assert_eq!(count(optimized).await, 2);

    // A separately pinned, unpruned provider has identical execution results.
    assert_eq!(count(provider(&table, false).await).await, 2);

    let optimized = Arc::new(provider(&table, true).await);
    let context = SessionContext::new();
    context.register_table("t", optimized.clone()).unwrap();
    let dataframe = context
        .sql("SELECT count(*) AS rows FROM t WHERE id >= 100")
        .await
        .unwrap();
    let batches = dataframe.collect().await.unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    assert_eq!(optimized.metrics().files_considered, 257);
    assert_eq!(optimized.metrics().files_pruned, 256);
    let repeated = context
        .sql("SELECT count(*) AS rows FROM t WHERE id >= 100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        repeated[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
}

#[tokio::test]
async fn descriptor_charges_obey_the_datafusion_pool_and_release_with_the_plan() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("one.parquet");
    let bytes = write_parquet(&source, vec![1, 2, 3]);
    let table = Table::new(LocalObjectStore::new(directory.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    table
        .append_files(&AppendRequest::new(
            "one",
            vec![append_file(&source, &bytes, 1, 3, 0)],
        ))
        .await
        .unwrap();

    let pool = Arc::new(GreedyMemoryPool::new(16 * 1024));
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .build()
            .unwrap(),
    );
    let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    context
        .register_table("t", Arc::new(provider(&table, false).await))
        .unwrap();
    let plan = context
        .sql("SELECT id FROM t")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    assert!(
        pool.reserved() > 0,
        "the scan plan must retain its descriptor charge"
    );
    drop(plan);
    assert_eq!(
        pool.reserved(),
        0,
        "dropping the plan releases descriptor charges"
    );

    let exhausted_pool = Arc::new(GreedyMemoryPool::new(1));
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(exhausted_pool.clone())
            .build()
            .unwrap(),
    );
    let exhausted = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    exhausted
        .register_table("t", Arc::new(provider(&table, false).await))
        .unwrap();
    let error = exhausted
        .sql("SELECT id FROM t")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Resources exhausted"), "{error}");
    assert_eq!(exhausted_pool.reserved(), 0);
}
