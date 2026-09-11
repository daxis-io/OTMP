//! Separate cold registration, planning and native Parquet execution evidence.
//! `--prepare TABLE` appends a deterministic sixteen-file Parquet workload to an
//! existing empty OTMP table with the conformance `id: int64` schema. Run this on
//! a copy of the large package made by `generate_reader_fixture`.
use datafusion::arrow::{array::Int64Array, record_batch::RecordBatch};
use datafusion::execution::{memory_pool::GreedyMemoryPool, runtime_env::RuntimeEnvBuilder};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{SessionConfig, SessionContext};
use otmp::{
    AppendFile, AppendRequest, FileFormat, FileMetric, LocalObjectStore, MetadataSelection,
    ReaderOptions, SnapshotSelection, SourceFingerprint, Table,
};
use otmp_datafusion::{OtmpTableProvider, ProviderOptions, schema_to_arrow};
use otmp_protocol::{Generation, Head, Sha256, TypedScalar, canonical_json};
use serde_json::json;
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Instant};

const SQL: &str = "SELECT count(*) AS n, sum(id) AS total FROM t WHERE id >= 14000";

async fn prepare(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let table = Table::new(LocalObjectStore::new(root)?);
    let reader = table
        .open_metadata_reader(
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
            ReaderOptions::default(),
        )
        .await?;
    if reader.snapshot().is_some() {
        return Err("qualification preparation requires an empty snapshot".into());
    }
    let schema = schema_to_arrow(reader.schema())?;
    let temporary = tempfile::tempdir()?;
    let mut files = Vec::new();
    for ordinal in 0_i64..16 {
        let path = temporary.path().join(format!("{ordinal}.parquet"));
        let array = Arc::new(Int64Array::from_iter_values(
            ordinal * 1000..(ordinal + 1) * 1000,
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
        let mut writer = ArrowWriter::try_new(std::fs::File::create(&path)?, schema.clone(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        let bytes = std::fs::read(&path)?;
        files.push(AppendFile {
            source_path: path,
            fingerprint: SourceFingerprint {
                sha256: Sha256::digest(&bytes),
                length: bytes.len() as u64,
            },
            format: FileFormat::Parquet,
            record_count: 1000,
            schema_id: 1,
            partition_spec_id: 0,
            sort_order_id: 0,
            partition_values: BTreeMap::new(),
            metadata: BTreeMap::new(),
            metrics: vec![FileMetric {
                field_id: 1,
                column_size_bytes: None,
                value_count: Some(1000),
                null_count: Some(0),
                nan_count: None,
                distinct_count: None,
                lower_bound: Some(TypedScalar::Int64(ordinal * 1000)),
                upper_bound: Some(TypedScalar::Int64((ordinal + 1) * 1000 - 1)),
                metadata: BTreeMap::new(),
            }],
        });
    }
    table
        .append_files(&AppendRequest::new("reader-qualification-parquet", files))
        .await?;
    table.verify().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|v| v == "--prepare") {
        return prepare(Path::new(
            args.get(1).ok_or("--prepare requires a table directory")?,
        ))
        .await;
    }
    let root = Path::new(
        args.first()
            .ok_or("usage: qualification [--prepare] <table-directory>")?,
    );
    let head: Head =
        canonical_json::from_slice_canonical(&std::fs::read(root.join("_otmp/HEAD"))?)?;
    let generation: Generation = canonical_json::from_slice_canonical(&std::fs::read(
        root.join(head.metadata_generation.uri.as_str()),
    )?)?;
    let image_bytes =
        generation.metadata_image.page_count.0 * u64::from(generation.metadata_image.page_size);
    if image_bytes < 128 * 1024 * 1024 {
        return Err("qualification requires at least 128 MiB of metadata".into());
    }
    let table = Table::new(LocalObjectStore::new(root)?);
    let started = Instant::now();
    let provider = Arc::new(
        OtmpTableProvider::open(
            &table,
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
            ReaderOptions::default(),
            ProviderOptions::default(),
        )
        .await?,
    );
    let registration_ms = started.elapsed().as_millis();
    let registered = provider.reader().statistics();
    if registered.bytes * 10 >= image_bytes {
        return Err("cold registration exceeded ten percent of metadata image".into());
    }
    let pool = Arc::new(GreedyMemoryPool::new(128 * 1024 * 1024));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()?;
    let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    context.register_table("t", provider.clone())?;
    let started = Instant::now();
    let plan = context.sql(SQL).await?.create_physical_plan().await?;
    let planning_ms = started.elapsed().as_millis();
    let planned = provider.reader().statistics();
    let planned_files = provider.metrics();
    let planning_reserved = datafusion::execution::memory_pool::MemoryPool::reserved(pool.as_ref());
    let started = Instant::now();
    let batches = datafusion::physical_plan::collect(plan.clone(), context.task_ctx()).await?;
    let execution_ms = started.elapsed().as_millis();
    let executed = provider.metrics();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("count type")?
        .value(0);
    let total = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("sum type")?
        .value(0);
    if (n, total) != (2000, 29_999_000) {
        return Err(format!("unexpected result: {n}, {total}").into());
    }
    if planned.peak_cache_bytes > provider.reader().statistics().peak_cache_bytes
        || planned.peak_cache_bytes > ReaderOptions::default().cache_budget_bytes
        || executed.peak_footer_cache_bytes > ProviderOptions::default().footer_cache_bytes
        || planning_reserved > ProviderOptions::default().planning_budget_bytes
    {
        return Err("configured reader budget exceeded".into());
    }
    println!(
        "{}",
        json!({ "metadata_image_bytes":image_bytes, "selected_table_version":provider.reader().coordinates().table_version,
            "registration":{"bytes":registered.bytes,"requests":registered.requests,"pages":registered.pages,"cache_hits":registered.cache_hits,"peak_cache_bytes":registered.peak_cache_bytes,"latency_ms":registration_ms},
            "planning":{"metadata_bytes":planned.bytes-registered.bytes,"metadata_requests":planned.requests-registered.requests,"metadata_pages":planned.pages-registered.pages,"metadata_cache_hits":planned.cache_hits-registered.cache_hits,"peak_cache_bytes":planned.peak_cache_bytes,"files_considered":planned_files.files_considered,"files_pruned":planned_files.files_pruned,"catalog_pruning_scans":planned_files.catalog_pruning_scans,"parquet_footer_bytes":planned_files.parquet_bytes,"parquet_requests":planned_files.parquet_requests,"validated_file_cache_hits":planned_files.validated_file_cache_hits,"reserved_bytes":planning_reserved,"latency_ms":planning_ms},
            "parquet_execution":{"bytes":executed.parquet_bytes-planned_files.parquet_bytes,"requests":executed.parquet_requests-planned_files.parquet_requests,"files_opened":executed.files_opened-planned_files.files_opened,"footer_cache_hits":executed.footer_cache_hits-planned_files.footer_cache_hits,"peak_footer_cache_bytes":executed.peak_footer_cache_bytes,"latency_ms":execution_ms,"result_count":n,"result_sum":total}
        })
    );
    Ok(())
}
