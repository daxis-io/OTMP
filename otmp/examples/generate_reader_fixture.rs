//! Generates a coherent, writer-produced large metadata package for local reader qualification.

use std::collections::BTreeMap;
use std::path::PathBuf;

use otmp::{
    CommitMetadata, InitializeRequest, LocalObjectStore, OperationRequest, Requirement, Table,
    TransactionRequest,
};
use otmp_protocol::{CanonicalValue, Generation, Head, Schema, canonical_json};

const PROPERTY_COUNT: usize = 32_768;
const VALUE_BYTES: usize = 4096;

fn schema() -> Schema {
    serde_json::from_slice(include_bytes!("../../conformance/sources/schema.json")).unwrap()
}

fn request(id: &str, updates: BTreeMap<String, CanonicalValue>) -> TransactionRequest {
    TransactionRequest {
        idempotency_key: id.into(),
        requirements: updates
            .keys()
            .map(|key| Requirement::PropertyIs {
                key: key.clone(),
                value: CanonicalValue::Null,
            })
            .collect(),
        operations: vec![OperationRequest::SetProperties {
            operation_id: id.into(),
            updates,
            removals: vec![],
        }],
        commit_metadata: CommitMetadata::default(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target: PathBuf = std::env::args()
        .nth(1)
        .ok_or("usage: generate_reader_fixture <empty-directory>")?
        .into();
    if target.exists() {
        return Err(format!("refusing existing target: {}", target.display()).into());
    }
    let table = Table::new(LocalObjectStore::new(&target)?);
    table.initialize(InitializeRequest::new(schema())).await?;
    let value = CanonicalValue::String("x".repeat(VALUE_BYTES));
    let updates = (0..PROPERTY_COUNT)
        .map(|number| (format!("perf.property.{number:05}"), value.clone()))
        .collect();
    table
        .transact(&request("large-properties", updates))
        .await?;
    let mut small = BTreeMap::new();
    small.insert(
        "perf.current".into(),
        CanonicalValue::String("small-update-v1".into()),
    );
    table.transact(&request("small-update", small)).await?;
    table.verify().await?;
    let head: Head =
        canonical_json::from_slice_canonical(&std::fs::read(target.join("_otmp/HEAD"))?)?;
    let generation: Generation = canonical_json::from_slice_canonical(&std::fs::read(
        target.join(head.metadata_generation.uri.as_str()),
    )?)?;
    let checkpoint = target.join(generation.metadata_image.checkpoint.uri.as_str());
    let bytes = checkpoint.metadata()?.len();
    if bytes < 128 * 1024 * 1024 {
        return Err(format!("checkpoint below 128 MiB: {bytes}").into());
    }
    let commit = target.join(head.semantic_commit.uri.as_str());
    println!(
        "{{\"checkpoint_bytes\":{bytes},\"latest_commit_bytes\":{},\"checkpoint_index\":{},\"target\":\"{}\"}}",
        commit.metadata()?.len(),
        generation.metadata_image.checkpoint_page_index.is_some(),
        target.display()
    );
    Ok(())
}
