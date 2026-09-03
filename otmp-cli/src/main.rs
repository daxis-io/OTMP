use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use otmp::{
    AppendFile, AppendRequest, FileFormat, FileMetric, InitializeRequest, LocalObjectStore,
    RuntimeError, SourceFingerprint, Table,
};
use otmp_protocol::{CanonicalValue, Schema, Sha256, TypedScalar, canonical_json};
use serde::Deserialize;
use serde_json::json;

#[derive(Parser)]
#[command(
    name = "otmp",
    version,
    about = "OTMP 0.0.2-alpha Gate 1 local/full-image POC"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Init {
        table: PathBuf,
        #[arg(long)]
        schema: PathBuf,
    },
    InspectFile {
        path: PathBuf,
    },
    Append {
        table: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
    },
    Status {
        table: PathBuf,
    },
    Files {
        table: PathBuf,
        #[arg(long, default_value = "main")]
        reference: String,
    },
    History {
        table: PathBuf,
    },
    Verify {
        table: PathBuf,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendManifest {
    idempotency_key: String,
    #[serde(default = "main_ref")]
    target_ref: String,
    files: Vec<ManifestFile>,
    #[serde(default)]
    summary: BTreeMap<String, CanonicalValue>,
    #[serde(default)]
    application_metadata: BTreeMap<String, CanonicalValue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestFile {
    source: PathBuf,
    sha256: Sha256,
    length: u64,
    #[serde(default = "parquet")]
    format: FileFormat,
    record_count: u64,
    schema_id: u32,
    #[serde(default)]
    partition_spec_id: u32,
    #[serde(default)]
    sort_order_id: u32,
    #[serde(default)]
    partition_values: BTreeMap<u32, TypedScalar>,
    #[serde(default)]
    metrics: Vec<FileMetric>,
    #[serde(default)]
    metadata: BTreeMap<String, CanonicalValue>,
}

fn main_ref() -> String {
    "main".into()
}

const fn parquet() -> FileFormat {
    FileFormat::Parquet
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init()
        .ok();
    match run(Cli::parse()).await {
        Ok(value) => match serde_json::to_string(&value) {
            Ok(json) => println!("{json}"),
            Err(error) => {
                eprintln!(
                    "{{\"code\":\"OTMP_ENCODING_ERROR\",\"message\":{error:?},\"retryable\":false,\"details\":{{}}}}"
                );
                std::process::exit(1);
            }
        },
        Err(error) => {
            let payload = error.payload();
            eprintln!(
                "{}",
                serde_json::to_string(&payload).unwrap_or_else(|_| {
                    "{\"code\":\"OTMP_ENCODING_ERROR\",\"message\":\"failed to encode error\",\"retryable\":false,\"details\":{}}".into()
                })
            );
            std::process::exit(1);
        }
    }
}

async fn run(cli: Cli) -> Result<serde_json::Value, RuntimeError> {
    match cli.command {
        Command::Init { table, schema } => {
            let bytes = tokio::fs::read(schema).await?;
            canonical_json::parse(&bytes)?;
            let schema: Schema = serde_json::from_slice(&bytes)
                .map_err(|error| RuntimeError::InvalidAppend(error.to_string()))?;
            let table = Table::new(LocalObjectStore::new(table)?);
            Ok(
                serde_json::to_value(table.initialize(InitializeRequest::new(schema)).await?)
                    .map_err(|error| RuntimeError::InvalidAppend(error.to_string()))?,
            )
        }
        Command::InspectFile { path } => {
            let bytes = tokio::fs::read(path).await?;
            Ok(json!({
                "sha256": Sha256::digest(&bytes),
                "length": bytes.len() as u64,
            }))
        }
        Command::Append { table, manifest } => {
            let bytes = tokio::fs::read(manifest).await?;
            canonical_json::parse(&bytes)?;
            let manifest: AppendManifest = serde_json::from_slice(&bytes)
                .map_err(|error| RuntimeError::InvalidAppend(error.to_string()))?;
            let request = AppendRequest {
                idempotency_key: manifest.idempotency_key,
                target_ref: manifest.target_ref,
                files: manifest
                    .files
                    .into_iter()
                    .map(|file| AppendFile {
                        source_path: file.source,
                        fingerprint: SourceFingerprint {
                            sha256: file.sha256,
                            length: file.length,
                        },
                        format: file.format,
                        record_count: file.record_count,
                        schema_id: file.schema_id,
                        partition_spec_id: file.partition_spec_id,
                        sort_order_id: file.sort_order_id,
                        partition_values: file.partition_values,
                        metrics: file.metrics,
                        metadata: file.metadata,
                    })
                    .collect(),
                summary: manifest.summary,
                application_metadata: manifest.application_metadata,
            };
            let table = Table::new(LocalObjectStore::new(table)?);
            Ok(serde_json::to_value(table.append_files(&request).await?)
                .map_err(|error| RuntimeError::InvalidAppend(error.to_string()))?)
        }
        Command::Status { table } => {
            let table = Table::new(LocalObjectStore::new(table)?);
            Ok(serde_json::to_value(table.pin().await?.status())
                .map_err(|error| RuntimeError::InvalidAppend(error.to_string()))?)
        }
        Command::Files { table, reference } => {
            let table = Table::new(LocalObjectStore::new(table)?);
            Ok(serde_json::to_value(table.pin().await?.files(&reference)?)
                .map_err(|error| RuntimeError::InvalidAppend(error.to_string()))?)
        }
        Command::History { table } => {
            let table = Table::new(LocalObjectStore::new(table)?);
            Ok(serde_json::to_value(table.pin().await?.history()?)
                .map_err(|error| RuntimeError::InvalidAppend(error.to_string()))?)
        }
        Command::Verify { table } => {
            let table = Table::new(LocalObjectStore::new(table)?);
            table.verify().await?;
            Ok(json!({"verified": true}))
        }
    }
}
