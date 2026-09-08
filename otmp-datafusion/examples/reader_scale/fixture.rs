use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PrepareConfig {
    pub files: usize,
    pub rows_per_file: usize,
    pub batch_size: usize,
    pub property_bytes: usize,
    pub small_tail: bool,
    pub footer_padding_bytes: Vec<usize>,
}
impl Default for PrepareConfig {
    fn default() -> Self {
        Self {
            files: 16,
            rows_per_file: 128,
            batch_size: 128,
            property_bytes: 0,
            small_tail: true,
            footer_padding_bytes: vec![0],
        }
    }
}
pub fn expected(
    files: usize,
    rows: usize,
    survivors: Option<usize>,
) -> Result<(i64, i64, i64), Error> {
    let keep = survivors.unwrap_or(files);
    if keep > files {
        return Err("survivors exceeds file count".into());
    }
    let first = (files - keep)
        .checked_mul(rows)
        .ok_or("row count overflow")?;
    let count = keep.checked_mul(rows).ok_or("row count overflow")?;
    let first = i64::try_from(first)?;
    let count = i64::try_from(count)?;
    let sum = i128::from(count)
        .checked_mul(2 * i128::from(first) + i128::from(count) - 1)
        .ok_or("result sum overflow")?
        / 2;
    Ok((first, count, i64::try_from(sum)?))
}
pub async fn prepare(root: &Path, config: PrepareConfig) -> Result<Value, Error> {
    if root.exists() {
        return Err("refusing existing fixture target".into());
    }
    if config.files > 131_072
        || !(1..=1_048_576).contains(&config.rows_per_file)
        || config.batch_size == 0
        || config.batch_size > 131_072
        || config.property_bytes > 16 * 1024 * 1024
        || config.footer_padding_bytes.is_empty()
        || config.footer_padding_bytes.len() > 32
        || config.footer_padding_bytes.iter().any(|n| *n > 1024 * 1024)
    {
        return Err("fixture geometry exceeds qualification limits".into());
    }
    expected(config.files, config.rows_per_file, None)?;
    let table = Table::new(LocalObjectStore::new(root)?);
    let schema: Schema =
        serde_json::from_slice(include_bytes!("../../../conformance/sources/schema.json"))?;
    table
        .initialize(InitializeRequest::new(schema.clone()))
        .await?;
    let arrow = otmp_datafusion::schema_to_arrow(&schema)?;
    let temporary = tempfile::tempdir()?;
    let mut data_bytes = 0_u64;
    for start in (0..config.files).step_by(config.batch_size) {
        let end = (start + config.batch_size).min(config.files);
        let mut descriptors = Vec::new();
        for ordinal in start..end {
            let first = i64::try_from(ordinal * config.rows_per_file)?;
            let last = first + i64::try_from(config.rows_per_file)? - 1;
            let bytes = parquet_bytes(
                arrow.clone(),
                first..=last,
                config.footer_padding_bytes[ordinal % config.footer_padding_bytes.len()],
            )?;
            let path = temporary.path().join(format!("{ordinal}.parquet"));
            std::fs::write(&path, &bytes)?;
            data_bytes += bytes.len() as u64;
            descriptors.push(AppendFile {
                source_path: path,
                fingerprint: SourceFingerprint {
                    sha256: Sha256::digest(&bytes),
                    length: bytes.len() as u64,
                },
                format: FileFormat::Parquet,
                record_count: config.rows_per_file as u64,
                schema_id: 1,
                partition_spec_id: 0,
                sort_order_id: 0,
                partition_values: BTreeMap::new(),
                metadata: BTreeMap::new(),
                metrics: vec![FileMetric {
                    field_id: 1,
                    column_size_bytes: None,
                    value_count: Some(config.rows_per_file as u64),
                    null_count: Some(0),
                    nan_count: None,
                    distinct_count: None,
                    lower_bound: Some(TypedScalar::Int64(first)),
                    upper_bound: Some(TypedScalar::Int64(last)),
                    metadata: BTreeMap::new(),
                }],
            });
        }
        table
            .append_files(&AppendRequest::new(
                format!("qualification-files-{start}"),
                descriptors,
            ))
            .await?;
        eprintln!("published {end}/{} files", config.files);
    }
    if config.property_bytes > 0 {
        set_property(
            &table,
            "qualification-large",
            "qualification.large",
            "x".repeat(config.property_bytes),
        )
        .await?;
    }
    if config.small_tail {
        set_property(
            &table,
            "qualification-tail",
            "qualification.tail",
            "ready".into(),
        )
        .await?;
    }
    table.verify().await?;
    describe(root, &config, data_bytes)
}
fn parquet_bytes(
    schema: datafusion::arrow::datatypes::SchemaRef,
    rows: std::ops::RangeInclusive<i64>,
    padding: usize,
) -> Result<Vec<u8>, Error> {
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(rows))],
    )?;
    let properties = (padding > 0).then(|| {
        datafusion::parquet::file::properties::WriterProperties::builder()
            .set_key_value_metadata(Some(vec![
                datafusion::parquet::file::metadata::KeyValue::new(
                    "qualification.padding".into(),
                    "x".repeat(padding),
                ),
            ]))
            .build()
    });
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, properties)?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn heterogeneous_footers_preserve_rows_and_have_distinct_lengths() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("table");
        let config: PrepareConfig = serde_json::from_value(serde_json::json!({
            "files": 4, "rows_per_file": 3, "footer_padding_bytes": [0, 4096]
        }))
        .unwrap();
        prepare(&root, config).await.unwrap();
        let mut lengths = std::collections::BTreeSet::new();
        for entry in std::fs::read_dir(root.join("data")).unwrap() {
            let bytes = std::fs::read(entry.unwrap().path()).unwrap();
            lengths.insert(u32::from_le_bytes(
                bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap(),
            ));
        }
        assert_eq!(lengths.len(), 2);
        verify(&root).await.unwrap();
    }
    #[test]
    fn expected_results_cover_selectivity_empty_selection_and_overflow() {
        assert_eq!(expected(4, 3, Some(2)).unwrap(), (6, 6, 51));
        assert_eq!(expected(4, 3, Some(0)).unwrap(), (12, 0, 0));
        assert_eq!(expected(4, 3, None).unwrap(), (0, 12, 66));
        assert!(expected(4, 3, Some(5)).is_err());
        assert!(expected(usize::MAX, usize::MAX, None).is_err());
        assert!(expected(usize::MAX - 1, 1, Some(usize::MAX / 2)).is_err());
    }
    #[tokio::test]
    async fn writes_verified_real_parquet_and_refuses_existing_target() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("table");
        let config = PrepareConfig {
            files: 4,
            rows_per_file: 3,
            batch_size: 2,
            ..PrepareConfig::default()
        };
        let manifest = prepare(&root, config.clone()).await.unwrap();
        assert_eq!(manifest["files"], 4);
        assert_eq!(manifest["table_version"], 3);
        assert!(manifest["data_bytes"].as_u64().unwrap() > 0);
        assert!(manifest["commit_bytes"].as_u64().unwrap() > 0);
        assert!(prepare(&root, config).await.is_err());
        verify(&root).await.unwrap();
        let file = std::fs::read_dir(root.join("data"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::remove_file(file).unwrap();
        assert!(verify(&root).await.is_err());
    }
}

use datafusion::arrow::{array::Int64Array, record_batch::RecordBatch};
use datafusion::parquet::arrow::ArrowWriter;
use otmp::{
    AppendFile, AppendRequest, CommitMetadata, FileFormat, FileMetric, InitializeRequest,
    LocalObjectStore, OperationRequest, SourceFingerprint, Table, TransactionRequest,
};
use otmp_protocol::{
    CanonicalValue, Generation, Head, Schema, Sha256, TypedScalar, canonical_json,
};
use std::{collections::BTreeMap, sync::Arc};
async fn set_property(
    table: &Table<LocalObjectStore>,
    id: &str,
    key: &str,
    value: String,
) -> Result<(), Error> {
    table
        .transact(&TransactionRequest {
            idempotency_key: id.into(),
            requirements: vec![otmp::Requirement::PropertyIs {
                key: key.into(),
                value: CanonicalValue::Null,
            }],
            operations: vec![OperationRequest::SetProperties {
                operation_id: id.into(),
                updates: BTreeMap::from([(key.into(), CanonicalValue::String(value))]),
                removals: vec![],
            }],
            commit_metadata: CommitMetadata::default(),
        })
        .await?;
    Ok(())
}
fn describe(root: &Path, config: &PrepareConfig, data_bytes: u64) -> Result<Value, Error> {
    let head_bytes = std::fs::read(root.join("_otmp/HEAD"))?;
    let head: Head = canonical_json::from_slice_canonical(&head_bytes)?;
    let generation: Generation = canonical_json::from_slice_canonical(&std::fs::read(
        root.join(head.metadata_generation.uri.as_str()),
    )?)?;
    let result = serde_json::json!({
        "files":config.files,"rows_per_file":config.rows_per_file,"batch_size":config.batch_size,
        "property_bytes":config.property_bytes,"small_tail":config.small_tail,
        "footer_padding_bytes":config.footer_padding_bytes,
        "head_sha256":Sha256::digest(&head_bytes).to_string(),"table_version":head.table_version.0,
        "generation_sha256":head.metadata_generation.sha256.to_string(),
        "metadata_image_bytes":generation.metadata_image.page_count.0*u64::from(generation.metadata_image.page_size),
        "checkpoint_bytes":generation.metadata_image.checkpoint.length.0,
        "page_map_height":generation.metadata_image.page_map.as_ref().map(|map|map.height),
        "commit_bytes":std::fs::metadata(root.join(head.semantic_commit.uri.as_str()))?.len(),"data_bytes":data_bytes,"verified":true,
    });
    std::fs::write(
        root.join("qualification.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    Ok(result)
}
pub fn load(root: &Path) -> Result<Value, Error> {
    let value: Value = serde_json::from_slice(&std::fs::read(root.join("qualification.json"))?)?;
    let hash = Sha256::digest(&std::fs::read(root.join("_otmp/HEAD"))?).to_string();
    if value["head_sha256"].as_str() != Some(&hash) {
        return Err("fixture HEAD changed after preparation".into());
    }
    Ok(value)
}
pub async fn tail(root: &Path) -> Result<Value, Error> {
    let before = load(root)?;
    if before["small_tail"] == true {
        return Err("fixture already has a small tail commit".into());
    }
    let mut config: PrepareConfig = serde_json::from_value(serde_json::json!({
        "files":before["files"],"rows_per_file":before["rows_per_file"],"batch_size":before["batch_size"],"property_bytes":before["property_bytes"],"small_tail":false,
    }))?;
    if let Some(padding) = before.get("footer_padding_bytes") {
        config.footer_padding_bytes = serde_json::from_value(padding.clone())?;
    }
    let table = Table::new(LocalObjectStore::new(root)?);
    set_property(
        &table,
        "qualification-tail",
        "qualification.tail",
        "ready".into(),
    )
    .await?;
    table.verify().await?;
    config.small_tail = true;
    describe(
        root,
        &config,
        before["data_bytes"]
            .as_u64()
            .ok_or("invalid fixture data size")?,
    )
}

pub async fn verify(root: &Path) -> Result<Value, Error> {
    let before = load(root)?;
    Table::new(LocalObjectStore::new(root)?).verify().await?;
    if load(root)? != before {
        return Err("fixture changed during verification".into());
    }
    Ok(before)
}
