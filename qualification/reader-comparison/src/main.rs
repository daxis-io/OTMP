use datafusion::arrow::array::{Array, Int64Array};
use datafusion::execution::{
    memory_pool::{GreedyMemoryPool, MemoryPool},
    runtime_env::RuntimeEnvBuilder,
};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use iceberg::expr::Reference;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Datum, NestedField, PrimitiveType, Schema,
    Struct, Type,
};
use iceberg::table::StaticTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_datafusion::table::IcebergStaticTableProvider;
use otmp::{
    LocalObjectStore, MetadataSelection, ReaderOptions, SnapshotSelection, Table as OtmpTable,
};
use otmp_datafusion::{OtmpTableProvider, ProviderOptions, ProviderStatistics};
use otmp_reader_comparison::{Expected, expected_values};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use url::Url;

type AnyError = Box<dyn Error + Send + Sync>;

const ICEBERG_REV: &str = "28ede505ebc3a274d4840624e5c85eae480a77ae";
const DATAFUSION_VERSION: &str = "55.0.0";
const ARROW_PARQUET_VERSION: &str = "59.3.0";
const DF_POOL_BYTES: usize = 256 * 1024 * 1024;
const ENGINE_PAGE_CACHE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize)]
struct SourceQualification {
    files: usize,
    rows_per_file: usize,
    head_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PreparedFile {
    relative_path: String,
    sha256: String,
    size: u64,
    rows: u64,
    min_id: i64,
    max_id: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SourceIdentity {
    root: String,
    head_sha256: String,
    qualification_sha256: String,
    data_sha256: String,
    files: usize,
    rows_per_file: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DerivedIdentity {
    root: String,
    data_sha256: String,
    metadata_sha256: String,
    metadata_location: String,
    metadata_files: usize,
    data_mode: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ComparisonManifest {
    format_version: u32,
    source: SourceIdentity,
    derived: DerivedIdentity,
    files: Vec<PreparedFile>,
    dependencies: Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Format {
    Otmp,
    Iceberg,
}

impl Format {
    fn parse(value: &str) -> Result<Self, AnyError> {
        match value {
            "otmp" => Ok(Self::Otmp),
            "iceberg" => Ok(Self::Iceberg),
            _ => Err(invalid(format!("unsupported format {value:?}"))),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Otmp => "otmp",
            Self::Iceberg => "iceberg",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RunConfig {
    survivors: Option<usize>,
    passes: usize,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            survivors: Some(2),
            passes: 2,
        }
    }
}

fn invalid(message: impl Into<String>) -> AnyError {
    Box::new(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}

fn canonical(path: &Path) -> Result<PathBuf, AnyError> {
    Ok(path.canonicalize()?)
}

fn file_uri(path: &Path) -> Result<String, AnyError> {
    Url::from_file_path(canonical(path)?)
        .map(|url| url.to_string())
        .map_err(|()| invalid(format!("cannot convert {} to a file URI", path.display())))
}

fn sha256_file(path: &Path) -> Result<String, AnyError> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn digest_entries(entries: &[PreparedFile]) -> String {
    let mut digest = Sha256::new();
    for entry in entries {
        digest.update(entry.relative_path.as_bytes());
        digest.update([0]);
        digest.update(entry.size.to_le_bytes());
        digest.update(entry.sha256.as_bytes());
        digest.update([0]);
    }
    format!("sha256:{:x}", digest.finalize())
}

fn digest_tree(root: &Path) -> Result<(String, usize), AnyError> {
    fn visit(root: &Path, path: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                visit(root, &entry.path(), files)?;
            } else {
                files.push(entry.path().strip_prefix(root).unwrap().to_owned());
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    visit(root, root, &mut files)?;
    files.sort();
    let mut digest = Sha256::new();
    for relative in &files {
        let full = root.join(relative);
        digest.update(relative.to_string_lossy().as_bytes());
        digest.update([0]);
        digest.update(fs::metadata(&full)?.len().to_le_bytes());
        digest.update(sha256_file(&full)?.as_bytes());
        digest.update([0]);
    }
    Ok((format!("sha256:{:x}", digest.finalize()), files.len()))
}

fn inspect_parquet(path: &Path) -> Result<(u64, i64, i64), AnyError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let mut reader = builder.with_batch_size(8 * 1024).build()?;
    let mut rows = 0_u64;
    let mut minimum = None;
    let mut maximum = None;
    for batch in &mut reader {
        let batch = batch?;
        if batch.num_columns() != 1 || batch.schema().field(0).name() != "id" {
            return Err(invalid(format!(
                "{} does not have the expected single id column",
                path.display()
            )));
        }
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid(format!("{} id is not int64", path.display())))?;
        if ids.null_count() != 0 {
            return Err(invalid(format!("{} contains null ids", path.display())));
        }
        for value in ids.values() {
            minimum = Some(minimum.map_or(*value, |current: i64| current.min(*value)));
            maximum = Some(maximum.map_or(*value, |current: i64| current.max(*value)));
        }
        rows = rows
            .checked_add(u64::try_from(ids.len())?)
            .ok_or_else(|| invalid("row count overflow"))?;
    }
    Ok((
        rows,
        minimum.ok_or_else(|| invalid(format!("{} is empty", path.display())))?,
        maximum.unwrap(),
    ))
}

fn source_qualification(root: &Path) -> Result<(SourceQualification, String), AnyError> {
    let path = root.join("qualification.json");
    let bytes = fs::read(&path)?;
    let qualification: SourceQualification = serde_json::from_slice(&bytes)?;
    let qualification_sha256 = format!("sha256:{:x}", Sha256::digest(&bytes));
    let actual_head = sha256_file(&root.join("_otmp/HEAD"))?;
    if qualification.head_sha256 != actual_head {
        return Err(invalid(format!(
            "source HEAD mismatch: qualification has {}, actual is {actual_head}",
            qualification.head_sha256
        )));
    }
    Ok((qualification, qualification_sha256))
}

async fn create_local_catalog(warehouse: &Path) -> Result<iceberg::MemoryCatalog, AnyError> {
    Ok(MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "reader-comparison",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse.to_string_lossy().into_owned(),
            )]),
        )
        .await?)
}

async fn prepare(source_root: &Path, derived_root: &Path) -> Result<Value, AnyError> {
    if derived_root.exists() {
        return Err(invalid(format!(
            "derived root already exists: {}",
            derived_root.display()
        )));
    }
    let source_root = canonical(source_root)?;
    let (qualification, qualification_sha256) = source_qualification(&source_root)?;
    let mut source_files = fs::read_dir(source_root.join("data"))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    source_files.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension == "parquet")
    });
    source_files.sort();
    if source_files.len() != qualification.files {
        return Err(invalid(format!(
            "expected {} source Parquet files, found {}",
            qualification.files,
            source_files.len()
        )));
    }

    fs::create_dir_all(derived_root.join("data"))?;
    let derived_root = canonical(derived_root)?;
    let mut files = Vec::with_capacity(source_files.len());
    let mut hardlinks = 0_usize;
    for source in source_files {
        let name = source
            .file_name()
            .ok_or_else(|| invalid("source file has no name"))?;
        let target = derived_root.join("data").join(name);
        let mode = match fs::hard_link(&source, &target) {
            Ok(()) => "hardlink",
            Err(_) => {
                fs::copy(&source, &target)?;
                "copy"
            }
        };
        hardlinks += usize::from(mode == "hardlink");
        let source_sha256 = sha256_file(&source)?;
        if sha256_file(&target)? != source_sha256 {
            return Err(invalid(format!(
                "derived data differs from source: {}",
                target.display()
            )));
        }
        let (rows, min_id, max_id) = inspect_parquet(&target)?;
        if rows != u64::try_from(qualification.rows_per_file)? {
            return Err(invalid(format!(
                "{} has {rows} rows, expected {}",
                source.display(),
                qualification.rows_per_file
            )));
        }
        files.push(PreparedFile {
            relative_path: format!("data/{}", name.to_string_lossy()),
            sha256: source_sha256,
            size: fs::metadata(&source)?.len(),
            rows,
            min_id,
            max_id,
        });
    }
    files.sort_by_key(|file| file.min_id);
    for (ordinal, file) in files.iter().enumerate() {
        let expected_min = i64::try_from(
            ordinal
                .checked_mul(qualification.rows_per_file)
                .ok_or_else(|| invalid("fixture ordinal overflow"))?,
        )?;
        let expected_max = expected_min + i64::try_from(qualification.rows_per_file)? - 1;
        if (file.min_id, file.max_id) != (expected_min, expected_max) {
            return Err(invalid(format!(
                "{} has id range {}..={}, expected {expected_min}..={expected_max}",
                file.relative_path, file.min_id, file.max_id
            )));
        }
    }
    let data_sha256 = digest_entries(&files);

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .with_identifier_field_ids([1])
        .build()?;
    let warehouse = derived_root.to_string_lossy().into_owned();
    let catalog = create_local_catalog(&derived_root).await?;
    let namespace = NamespaceIdent::new("comparison".to_string());
    catalog.create_namespace(&namespace, HashMap::new()).await?;
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("fixture".to_string())
                .location(warehouse)
                .schema(schema)
                .properties(HashMap::new())
                .build(),
        )
        .await?;
    let mut data_files = Vec::with_capacity(files.len());
    for file in &files {
        data_files.push(
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(file_uri(&derived_root.join(&file.relative_path))?)
                .file_format(DataFileFormat::Parquet)
                .partition(Struct::empty())
                .record_count(file.rows)
                .file_size_in_bytes(file.size)
                .value_counts(HashMap::from([(1, file.rows)]))
                .null_value_counts(HashMap::from([(1, 0)]))
                .nan_value_counts(HashMap::new())
                .lower_bounds(HashMap::from([(1, Datum::long(file.min_id))]))
                .upper_bounds(HashMap::from([(1, Datum::long(file.max_id))]))
                .key_metadata(None)
                .partition_spec_id(0)
                .sort_order_id(0)
                .build()?,
        );
    }
    let transaction = Transaction::new(&table);
    let transaction = transaction
        .fast_append()
        .add_data_files(data_files)
        .apply(transaction)?;
    let table = transaction.commit(&catalog).await?;
    let metadata_location = table.metadata_location_result()?.to_string();
    let metadata_dir = derived_root.join("metadata");
    let (metadata_sha256, metadata_files) = digest_tree(&metadata_dir)?;
    let data_mode = if hardlinks == files.len() {
        "hardlink"
    } else if hardlinks == 0 {
        "copy"
    } else {
        "mixed"
    };
    let manifest = ComparisonManifest {
        format_version: 1,
        source: SourceIdentity {
            root: source_root.to_string_lossy().into_owned(),
            head_sha256: qualification.head_sha256,
            qualification_sha256,
            data_sha256: data_sha256.clone(),
            files: qualification.files,
            rows_per_file: qualification.rows_per_file,
        },
        derived: DerivedIdentity {
            root: derived_root.to_string_lossy().into_owned(),
            data_sha256: data_sha256.clone(),
            metadata_sha256,
            metadata_location,
            metadata_files,
            data_mode: data_mode.to_string(),
        },
        files,
        dependencies: dependency_identity(),
    };
    fs::write(
        derived_root.join("qualification.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(json!({"outcome":"success","manifest":manifest}))
}

fn dependency_identity() -> Value {
    json!({
        "datafusion": DATAFUSION_VERSION,
        "arrow_parquet": ARROW_PARQUET_VERSION,
        "iceberg_git": "https://github.com/apache/iceberg-rust.git",
        "iceberg_rev": ICEBERG_REV,
        "otmp_source": "repository-relative ../../",
    })
}

fn load_manifest(derived_root: &Path) -> Result<ComparisonManifest, AnyError> {
    Ok(serde_json::from_slice(&fs::read(
        derived_root.join("qualification.json"),
    )?)?)
}

fn verify_entries(root: &Path, entries: &[PreparedFile]) -> Result<String, AnyError> {
    for entry in entries {
        let path = root.join(&entry.relative_path);
        if fs::metadata(&path)?.len() != entry.size || sha256_file(&path)? != entry.sha256 {
            return Err(invalid(format!(
                "content identity mismatch: {}",
                path.display()
            )));
        }
    }
    Ok(digest_entries(entries))
}

fn validate_fixture_anchors(
    source_root: &Path,
    derived_root: &Path,
) -> Result<ComparisonManifest, AnyError> {
    let manifest = load_manifest(derived_root)?;
    if manifest.format_version != 1 || manifest.dependencies != dependency_identity() {
        return Err(invalid(
            "comparison manifest version or dependency pin mismatch",
        ));
    }
    if canonical(source_root)?.to_string_lossy() != manifest.source.root
        || canonical(derived_root)?.to_string_lossy() != manifest.derived.root
    {
        return Err(invalid("fixture root differs from prepared provenance"));
    }
    let (qualification, qualification_sha256) = source_qualification(source_root)?;
    if qualification.head_sha256 != manifest.source.head_sha256
        || qualification_sha256 != manifest.source.qualification_sha256
        || qualification.files != manifest.source.files
        || qualification.rows_per_file != manifest.source.rows_per_file
    {
        return Err(invalid("source qualification identity changed"));
    }
    Ok(manifest)
}

fn verify_fixture(source_root: &Path, derived_root: &Path) -> Result<ComparisonManifest, AnyError> {
    let manifest = validate_fixture_anchors(source_root, derived_root)?;
    if verify_entries(source_root, &manifest.files)? != manifest.source.data_sha256 {
        return Err(invalid("source data aggregate changed"));
    }
    if verify_entries(derived_root, &manifest.files)? != manifest.derived.data_sha256 {
        return Err(invalid("derived data aggregate changed"));
    }
    let (metadata_sha256, metadata_files) = digest_tree(&derived_root.join("metadata"))?;
    if metadata_sha256 != manifest.derived.metadata_sha256
        || metadata_files != manifest.derived.metadata_files
    {
        return Err(invalid("derived Iceberg metadata changed"));
    }
    Ok(manifest)
}

struct OpenedProvider {
    provider: Arc<dyn datafusion::datasource::TableProvider>,
    otmp: Option<Arc<OtmpTableProvider<LocalObjectStore>>>,
    lifetime: Value,
}

async fn open_provider(
    format: Format,
    source_root: &Path,
    manifest: &ComparisonManifest,
) -> Result<OpenedProvider, AnyError> {
    match format {
        Format::Otmp => {
            let table = OtmpTable::new(LocalObjectStore::new(source_root)?);
            let provider = Arc::new(
                OtmpTableProvider::open(
                    &table,
                    MetadataSelection::Current,
                    SnapshotSelection::Ref("main".to_string()),
                    ReaderOptions {
                        engine_page_cache_bytes: ENGINE_PAGE_CACHE_BYTES,
                        ..ReaderOptions::default()
                    },
                    ProviderOptions::default(),
                )
                .await?,
            );
            Ok(OpenedProvider {
                provider: provider.clone(),
                otmp: Some(provider),
                lifetime: json!({
                    "snapshot": "current/main pinned by provider",
                    "file_enumeration": "during DataFusion physical planning"
                }),
            })
        }
        Format::Iceberg => {
            let table = open_iceberg_table(manifest).await?;
            let snapshot_id = table
                .metadata()
                .current_snapshot()
                .ok_or_else(|| invalid("Iceberg fixture has no current snapshot"))?
                .snapshot_id();
            let provider = Arc::new(
                IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id).await?,
            );
            Ok(OpenedProvider {
                provider,
                otmp: None,
                lifetime: json!({
                    "snapshot_id": snapshot_id,
                    "snapshot": "explicitly pinned static provider",
                    "file_enumeration": "deferred by IcebergTableScan into execution"
                }),
            })
        }
    }
}

async fn open_iceberg_table(
    manifest: &ComparisonManifest,
) -> Result<iceberg::table::Table, AnyError> {
    let identifier = TableIdent::from_strs(["comparison", "fixture"])?;
    Ok(StaticTable::from_metadata_file(
        &manifest.derived.metadata_location,
        identifier,
        iceberg::io::FileIO::new_with_fs(),
    )
    .await?
    .into_table())
}

async fn plan_files_probe(
    source_root: &Path,
    derived_root: &Path,
    config: RunConfig,
) -> Result<Value, AnyError> {
    if !(1..=20).contains(&config.passes) {
        return Err(invalid("passes must be between 1 and 20"));
    }
    let manifest = validate_fixture_anchors(source_root, derived_root)?;
    let expected = expected_values(
        manifest.source.files,
        manifest.source.rows_per_file,
        config.survivors,
    )
    .map_err(invalid)?;
    let started = Instant::now();
    let table = open_iceberg_table(&manifest).await?;
    let initialization_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let expected_tasks = config.survivors.unwrap_or(manifest.source.files);
    let mut phases = Vec::with_capacity(config.passes);
    for pass in 0..config.passes {
        let started = Instant::now();
        let tasks = table
            .scan()
            .with_filter(Reference::new("id").greater_than_or_equal_to(Datum::long(expected.first)))
            .build()?
            .plan_files()
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
        if tasks.len() != expected_tasks {
            return Err(invalid(format!(
                "Iceberg planned {} tasks, expected {expected_tasks}",
                tasks.len()
            )));
        }
        phases.push(json!({
            "name":"metadata_file_selection",
            "pass":pass,
            "elapsed_ms":elapsed_ms,
            "details":{"file_tasks":tasks.len(),"data_files_opened":0},
        }));
    }
    Ok(json!({
        "outcome":"success",
        "format":"iceberg",
        "fixture":{
            "files":manifest.source.files,
            "rows_per_file":manifest.source.rows_per_file,
            "source_head_sha256":manifest.source.head_sha256,
            "source_data_sha256":manifest.source.data_sha256,
            "derived_metadata_sha256":manifest.derived.metadata_sha256,
        },
        "dependencies":manifest.dependencies,
        "config":{"survivors":config.survivors,"passes":config.passes},
        "initialization_ms":initialization_ms,
        "phases":phases,
        "measurement_contract":{
            "scope":"Iceberg TableScan::plan_files only",
            "process_isolation":"run separately from provider query samples to avoid warming their table cache",
            "data_files_opened":false,
            "os_cache":"uncontrolled",
        },
    }))
}

fn metric_delta(after: ProviderStatistics, before: ProviderStatistics) -> Value {
    json!({
        "files_considered": after.files_considered - before.files_considered,
        "files_pruned": after.files_pruned - before.files_pruned,
        "files_opened": after.files_opened - before.files_opened,
        "planning_micros": after.planning_micros - before.planning_micros,
    })
}

fn phase(name: &str, pass: usize, started: Instant, details: Value, reserved: usize) -> Value {
    json!({
        "name": name,
        "pass": pass,
        "elapsed_ms": started.elapsed().as_secs_f64() * 1_000.0,
        "details": details,
        "pool_reserved_bytes": reserved,
    })
}

fn validate_result(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    expected: Expected,
) -> Result<(), AnyError> {
    if batch.num_rows() != 1 || batch.num_columns() != 2 {
        return Err(invalid(format!(
            "unexpected result shape: {}x{}",
            batch.num_rows(),
            batch.num_columns()
        )));
    }
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid("count result is not int64"))?;
    let sum = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid("sum result is not int64"))?;
    if count.value(0) != expected.count || sum.value(0) != expected.sum {
        return Err(invalid(format!(
            "result mismatch: got ({}, {}), expected ({}, {})",
            count.value(0),
            sum.value(0),
            expected.count,
            expected.sum
        )));
    }
    Ok(())
}

async fn run(
    format: Format,
    source_root: &Path,
    derived_root: &Path,
    config: RunConfig,
) -> Result<Value, AnyError> {
    if !(1..=20).contains(&config.passes) {
        return Err(invalid("passes must be between 1 and 20"));
    }
    let process_started = Instant::now();
    let started = Instant::now();
    let manifest = validate_fixture_anchors(source_root, derived_root)?;
    let mut phases = vec![phase(
        "fixture_validation",
        0,
        started,
        json!({"timed_comparison": false,"scope":"identity anchors only; run `verify` outside measurement subprocesses for full content verification"}),
        0,
    )];
    let expected = expected_values(
        manifest.source.files,
        manifest.source.rows_per_file,
        config.survivors,
    )
    .map_err(invalid)?;
    let sql = format!(
        "SELECT count(*) AS n, coalesce(sum(id), 0) AS total FROM t WHERE id >= {}",
        expected.first
    );

    let started = Instant::now();
    let opened = open_provider(format, source_root, &manifest).await?;
    phases.push(phase(
        "initialization",
        0,
        started,
        opened.lifetime.clone(),
        0,
    ));
    let started = Instant::now();
    let pool = Arc::new(GreedyMemoryPool::new(DF_POOL_BYTES));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()?;
    let context =
        SessionContext::new_with_config_rt(SessionConfig::new().with_target_partitions(4), runtime);
    context.register_table("t", opened.provider.clone())?;
    phases.push(phase(
        "setup",
        0,
        started,
        json!({"target_partitions":4,"df_pool_bytes":DF_POOL_BYTES}),
        pool.reserved(),
    ));

    for pass in 0..config.passes {
        let before = opened
            .otmp
            .as_ref()
            .map(|provider| provider.metrics())
            .unwrap_or_default();
        let started = Instant::now();
        let plan = context.sql(&sql).await?.create_physical_plan().await?;
        let planning_ms = started.elapsed().as_secs_f64() * 1_000.0;
        let provider_metrics = opened
            .otmp
            .as_ref()
            .map(|provider| metric_delta(provider.metrics(), before));
        phases.push(json!({
            "name":"planning",
            "pass":pass,
            "elapsed_ms":planning_ms,
            "details":{
                "boundary":"SessionContext::sql plus DataFrame::create_physical_plan",
                "provider":provider_metrics,
                "files_ready": if format == Format::Otmp { "explicit native Parquet scan" } else { "no; IcebergTableScan defers manifest and file-task enumeration" },
            },
            "pool_reserved_bytes":pool.reserved(),
        }));

        let started = Instant::now();
        let mut stream = execute_stream(plan.clone(), context.task_ctx())?;
        let first_batch = stream
            .next()
            .await
            .ok_or_else(|| invalid("query returned no result batch"))??;
        let startup_ms = started.elapsed().as_secs_f64() * 1_000.0;
        validate_result(&first_batch, expected)?;
        phases.push(json!({
            "name":"execution_to_first_batch",
            "pass":pass,
            "elapsed_ms":startup_ms,
            "details":{
                "includes": if format == Format::Iceberg { "deferred manifest/file-task enumeration plus data scan and aggregate" } else { "data scan and aggregate after explicit file planning" },
                "combined_ready_and_first_result_ms": planning_ms + startup_ms,
            },
            "pool_reserved_bytes":pool.reserved(),
        }));
        let started = Instant::now();
        let mut additional_batches = 0_usize;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            additional_batches += 1;
            if batch.num_rows() != 0 {
                return Err(invalid(
                    "aggregate query returned more than one non-empty batch",
                ));
            }
        }
        phases.push(phase(
            "execution_rest",
            pass,
            started,
            json!({"additional_batches":additional_batches}),
            pool.reserved(),
        ));
        let started = Instant::now();
        drop(plan);
        phases.push(phase(
            "plan_release",
            pass,
            started,
            Value::Null,
            pool.reserved(),
        ));
    }
    let started = Instant::now();
    drop(context);
    drop(opened);
    tokio::task::yield_now().await;
    phases.push(phase(
        "teardown",
        0,
        started,
        json!({"pool_released":pool.reserved() == 0}),
        pool.reserved(),
    ));
    Ok(json!({
        "outcome":"success",
        "format":format.name(),
        "fixture":{
            "files":manifest.source.files,
            "rows_per_file":manifest.source.rows_per_file,
            "source_head_sha256":manifest.source.head_sha256,
            "source_qualification_sha256":manifest.source.qualification_sha256,
            "source_data_sha256":manifest.source.data_sha256,
            "derived_metadata_sha256":manifest.derived.metadata_sha256,
        },
        "dependencies":manifest.dependencies,
        "config":{
            "survivors":config.survivors,
            "passes":config.passes,
            "target_partitions":4,
            "df_pool_bytes":DF_POOL_BYTES,
            "engine_page_cache_bytes":ENGINE_PAGE_CACHE_BYTES,
        },
        "measurement_contract":{
            "primary":"planning plus execution_to_first_batch",
            "reason":"Iceberg defers manifest and file-task enumeration into execution",
            "os_cache":"uncontrolled",
            "preparation_in_timed_phases":false,
        },
        "sql":sql,
        "phases":phases,
        "result":{"count":expected.count,"sum":expected.sum},
        "process_elapsed_ms":process_started.elapsed().as_secs_f64()*1_000.0,
    }))
}

fn parse_run_options(args: &[String]) -> Result<RunConfig, AnyError> {
    let mut config = RunConfig::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--survivors" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| invalid("--survivors needs a value"))?;
                config.survivors = if value == "all" {
                    None
                } else {
                    Some(value.parse()?)
                };
            }
            "--passes" => {
                index += 1;
                config.passes = args
                    .get(index)
                    .ok_or_else(|| invalid("--passes needs a value"))?
                    .parse()?;
            }
            option => return Err(invalid(format!("unknown option {option:?}"))),
        }
        index += 1;
    }
    Ok(config)
}

fn usage() -> &'static str {
    "usage:\n  otmp-reader-comparison prepare SOURCE_ROOT DERIVED_ROOT\n  otmp-reader-comparison verify SOURCE_ROOT DERIVED_ROOT\n  otmp-reader-comparison plan-files SOURCE_ROOT DERIVED_ROOT [--survivors 2|all] [--passes 2]\n  otmp-reader-comparison run otmp|iceberg SOURCE_ROOT DERIVED_ROOT [--survivors 2|all] [--passes 2]"
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let result = match args.as_slice() {
        [command, source, derived] if command == "prepare" => {
            prepare(Path::new(source), Path::new(derived)).await?
        }
        [command, source, derived] if command == "verify" => {
            let manifest = verify_fixture(Path::new(source), Path::new(derived))?;
            json!({"outcome":"success","verified":true,"manifest":manifest})
        }
        [command, source, derived, rest @ ..] if command == "plan-files" => {
            plan_files_probe(
                Path::new(source),
                Path::new(derived),
                parse_run_options(rest)?,
            )
            .await?
        }
        [command, format, source, derived, rest @ ..] if command == "run" => {
            run(
                Format::parse(format)?,
                Path::new(source),
                Path::new(derived),
                parse_run_options(rest)?,
            )
            .await?
        }
        _ => return Err(invalid(usage())),
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn local_catalog_persists_table_metadata() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "otmp-reader-comparison-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();

        let catalog = create_local_catalog(&root).await.unwrap();
        let namespace = NamespaceIdent::new("test".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("fixture".to_string())
                    .location(root.to_string_lossy().into_owned())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();

        let location = table
            .metadata_location_result()
            .unwrap()
            .strip_prefix("file://")
            .unwrap_or(table.metadata_location_result().unwrap());
        assert!(Path::new(location).is_file(), "missing {location}");
        fs::remove_dir_all(root).unwrap();
    }
}
