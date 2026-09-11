//! One exact-size append against an owned reader-scale fixture.

use datafusion::arrow::{array::Int64Array, record_batch::RecordBatch};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::{metadata::KeyValue, properties::WriterProperties};
use otmp::{
    AppendFile, AppendRequest, FileFormat, FileMetric, LocalObjectStore, SourceFingerprint, Table,
};
use otmp_protocol::{Schema, Sha256, TypedScalar};
use serde_json::json;
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Instant};

const APPEND_BYTES: usize = 1_705;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init()?;
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    let [root, expected_files] = arguments.as_slice() else {
        return Err("usage: writer_scale ROOT EXPECTED_FILES".into());
    };
    let root = Path::new(root);
    let expected_files = expected_files
        .to_str()
        .ok_or("EXPECTED_FILES is not UTF-8")?
        .parse::<usize>()?;
    let fixture: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("qualification.json"))?)?;
    if fixture["files"].as_u64() != Some(expected_files as u64) {
        return Err("fixture file count differs from EXPECTED_FILES".into());
    }
    let table = Table::new(LocalObjectStore::new(root)?);
    let before = table.pin().await?;
    if before.files("main")?.len() != expected_files {
        return Err("live file count differs from EXPECTED_FILES".into());
    }
    let temporary = tempfile::tempdir()?;
    let request = append_request(temporary.path(), expected_files)?;
    let source_sha256 = request.files[0].fingerprint.sha256;
    let started = Instant::now();
    let result = table.append_files(&request).await?;
    let elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    table.verify().await?;
    let after = table.pin().await?;
    if after.files("main")?.len() != expected_files + 1 {
        return Err("append did not add exactly one live file".into());
    }
    println!(
        "{}",
        serde_json::to_string(&json!({
            "outcome":"success",
            "files_before":expected_files,
            "files_after":expected_files + 1,
            "append_bytes":APPEND_BYTES,
            "source_sha256":source_sha256.to_string(),
            "table_version":result.table_version,
            "elapsed_ns":elapsed_ns,
            "verified":true,
        }))?
    );
    Ok(())
}

fn append_request(directory: &Path, files: usize) -> Result<AppendRequest, Error> {
    let schema: Schema =
        serde_json::from_slice(include_bytes!("../../conformance/sources/schema.json"))?;
    let arrow = otmp_datafusion::schema_to_arrow(&schema)?;
    let batch = RecordBatch::try_new(
        arrow.clone(),
        vec![Arc::new(Int64Array::from_iter_values([0]))],
    )?;
    let bytes = (0..=APPEND_BYTES)
        .find_map(|padding| {
            let properties = WriterProperties::builder()
                .set_key_value_metadata(Some(vec![KeyValue::new(
                    "qualification.padding".into(),
                    "x".repeat(padding),
                )]))
                .build();
            let mut writer =
                ArrowWriter::try_new(Vec::new(), arrow.clone(), Some(properties)).ok()?;
            writer.write(&batch).ok()?;
            let bytes = writer.into_inner().ok()?;
            (bytes.len() == APPEND_BYTES).then_some(bytes)
        })
        .ok_or("unable to construct the frozen 1,705-byte Parquet append")?;
    let source = directory.join("append.parquet");
    std::fs::write(&source, &bytes)?;
    Ok(AppendRequest::new(
        format!("native-metadata-pruning-{files}"),
        vec![AppendFile {
            source_path: source,
            fingerprint: SourceFingerprint {
                sha256: Sha256::digest(&bytes),
                length: bytes.len() as u64,
            },
            format: FileFormat::Parquet,
            record_count: 1,
            schema_id: 1,
            partition_spec_id: 0,
            sort_order_id: 0,
            partition_values: BTreeMap::new(),
            metrics: vec![FileMetric {
                field_id: 1,
                column_size_bytes: None,
                value_count: Some(1),
                null_count: Some(0),
                nan_count: None,
                distinct_count: Some(1),
                lower_bound: Some(TypedScalar::Int64(0)),
                upper_bound: Some(TypedScalar::Int64(0)),
                metadata: BTreeMap::new(),
            }],
            metadata: BTreeMap::new(),
        }],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_append_is_exactly_1705_bytes() {
        let temporary = tempfile::tempdir().unwrap();
        let request = append_request(temporary.path(), 256).unwrap();
        assert_eq!(
            std::fs::metadata(&request.files[0].source_path)
                .unwrap()
                .len(),
            1_705
        );
        assert_eq!(request.files[0].fingerprint.length, 1_705);
    }
}
