use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use otmp_protocol::{
    CanonicalValue, Generation, Head, Schema, SemanticCommit, Sha256, canonical_json,
};
use serde::{Deserialize, Serialize};

use super::{ProbeReport, QualificationStore};
use crate::{
    CommitMetadata, InitializeRequest, LocalObjectStore, OperationRequest, Requirement,
    RuntimeError, StorageError, Table, TransactionRequest,
};

const MAX_PROPERTY_BYTES: usize = 16 * 1024 * 1024;
const FIXTURE_MANIFEST: &str = "write-latency-fixture.json";
const REQUIRED_PHASES: &[&str] = &[
    "parent_pin",
    "idempotency",
    "candidate_build",
    "immutable_publication",
    "head_cas",
    "generation_resolution",
    "logical_image_materialization",
    "parent_validation",
    "operation_preparation",
    "turso_open",
    "turso_sql",
    "turso_checkpoint_freeze",
    "candidate_buffer_creation",
    "validation_file_write",
    "exhaustive_validation",
    "page_pack_page_map_construction",
    "commit_projection_validation",
];

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("configuration is invalid: {0}")]
    Config(String),
    #[error("fixture is invalid: {0}")]
    Fixture(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Protocol(#[from] otmp_protocol::ProtocolError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Probe(#[from] super::ProbeError),
}

impl WorkerError {
    #[must_use]
    pub fn output(&self) -> FailureOutput {
        let (code, retryable) = match self {
            Self::Runtime(error) => (error.code().to_owned(), error.retryable()),
            Self::Storage(error) => (error.code().to_owned(), error.retryable()),
            Self::Config(_) => ("OTMP_QUALIFICATION_CONFIG".into(), false),
            Self::Fixture(_) => ("OTMP_QUALIFICATION_FIXTURE".into(), false),
            Self::Probe(_) => ("OTMP_QUALIFICATION_PROBE".into(), false),
            Self::Io(_) => ("OTMP_IO_ERROR".into(), false),
            Self::Json(_) | Self::Protocol(_) => ("OTMP_QUALIFICATION_FORMAT".into(), false),
        };
        FailureOutput {
            ok: false,
            error: Failure {
                code,
                message: self.to_string(),
                retryable,
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub struct FailureOutput {
    ok: bool,
    error: Failure,
}

#[derive(Debug, Serialize)]
pub struct Failure {
    code: String,
    message: String,
    retryable: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareConfig {
    property_bytes: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunConfig {
    mode: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureIdentity {
    pub property_bytes: usize,
    pub table_id: String,
    pub table_version: u64,
    pub generation_id: String,
    pub head_sha256: String,
    pub head_bytes: u64,
    pub logical_image_bytes: u64,
    pub checkpoint_bytes: u64,
    pub commit_bytes: u64,
    pub retained_history_verified: bool,
}

pub type PrepareOutput = FixtureIdentity;

#[derive(Debug, Serialize)]
pub struct VerifyOutput {
    pub ok: bool,
    pub table_version: u64,
    pub retained_history_verified: bool,
}

#[derive(Debug, Serialize)]
pub struct RunOutput {
    pub ok: bool,
    pub mode: String,
    pub fixture: FixtureIdentity,
    pub before_table_version: u64,
    pub after_table_version: u64,
    pub measured_property: String,
    pub source_head_sha256: String,
    pub result_head_sha256: String,
    pub retained_history_verified: bool,
    pub incremental_publication: bool,
    pub complete_candidate_bytes: u64,
    pub published_image_artifact_bytes: u64,
    pub acknowledged_latency_ns: u64,
    pub unattributed_latency_ns: u64,
    pub probe: ProbeReport,
}

pub async fn prepare(root: &Path, config: &Path) -> Result<PrepareOutput, WorkerError> {
    if root.exists() {
        return Err(WorkerError::Fixture(format!(
            "destination already exists: {}",
            root.display()
        )));
    }
    let config: PrepareConfig = read_config(config)?;
    if config.property_bytes > MAX_PROPERTY_BYTES {
        return Err(WorkerError::Config(format!(
            "property_bytes exceeds {MAX_PROPERTY_BYTES}"
        )));
    }
    let store = LocalObjectStore::new(root)?;
    let table = Table::new(store);
    table.initialize(InitializeRequest::new(schema()?)).await?;
    table
        .transact(&property_request(
            "qualification.prepare.payload",
            "qualification.payload",
            "qualification.prepare.payload",
            "x".repeat(config.property_bytes),
        ))
        .await?;
    table
        .transact(&property_request(
            "qualification.prepare.tail",
            "qualification.tail",
            "qualification.prepare.tail",
            "fixed".into(),
        ))
        .await?;
    table.verify_history().await?;
    let identity = inspect(root, config.property_bytes, true)?;
    fs::write(
        root.join(FIXTURE_MANIFEST),
        serde_json::to_vec_pretty(&identity)?,
    )?;
    Ok(identity)
}

pub async fn run(root: &Path, config: &Path) -> Result<RunOutput, WorkerError> {
    let config: RunConfig = read_config(config)?;
    if config.mode != "fresh" && config.mode != "pre_pinned" {
        return Err(WorkerError::Config(
            "mode must be exactly fresh or pre_pinned".into(),
        ));
    }
    let expected: FixtureIdentity =
        serde_json::from_slice(&fs::read(root.join(FIXTURE_MANIFEST))?)?;
    let actual = inspect(root, expected.property_bytes, true)?;
    if actual != expected {
        return Err(WorkerError::Fixture(
            "fixture identity differs from its prepared manifest".into(),
        ));
    }
    let source_head_sha256 = actual.head_sha256.clone();
    let table = Table::new(QualificationStore::new(LocalObjectStore::new(root)?));
    let pinned = if config.mode == "pre_pinned" {
        Some(table.qualification_write_pin().await?)
    } else {
        None
    };
    let request = property_request(
        "qualification.measurement",
        "qualification.measured",
        "qualification.measurement",
        "measured".into(),
    );
    let session = super::start()?;
    let transaction = match pinned {
        Some(pinned) => table.transact_pre_pinned(&request, pinned).await,
        None => table.transact(&request).await,
    };
    let probe = session.finish()?;
    let result = transaction?;
    validate_probe(&probe)?;
    if result.table_version != actual.table_version + 1 {
        return Err(WorkerError::Fixture(
            "sample did not advance exactly one table version".into(),
        ));
    }
    let current = table.pin().await?;
    let measured = current
        .qualification_property("qualification.measured")?
        .ok_or_else(|| WorkerError::Fixture("measured property is absent".into()))?;
    let CanonicalValue::String(measured_property) = measured else {
        return Err(WorkerError::Fixture(
            "measured property has the wrong type".into(),
        ));
    };
    if measured_property != "measured" {
        return Err(WorkerError::Fixture(
            "measured property has the wrong value".into(),
        ));
    }
    table.verify_history().await?;
    let head = fs::read(root.join("_otmp/HEAD"))?;
    let complete_candidate_bytes = counter(&probe, "candidate_logical_bytes");
    let published_image_artifact_bytes = counter(&probe, "published_image_artifact_bytes");
    let top_level_ns = probe
        .phases
        .iter()
        .filter(|phase| phase.depth == 0)
        .map(|phase| phase.duration_ns)
        .sum::<u64>();
    let unattributed_latency_ns = probe.acknowledged_latency_ns.saturating_sub(top_level_ns);
    let maximum_unattributed = 1_000_000.max(probe.acknowledged_latency_ns / 20);
    if unattributed_latency_ns > maximum_unattributed {
        return Err(WorkerError::Probe(super::ProbeError(format!(
            "unattributed latency {unattributed_latency_ns} ns exceeds {maximum_unattributed} ns"
        ))));
    }
    if published_image_artifact_bytes >= complete_candidate_bytes {
        return Err(WorkerError::Probe(super::ProbeError(
            "small mutation did not remain incremental".into(),
        )));
    }
    Ok(RunOutput {
        ok: true,
        mode: config.mode,
        fixture: actual.clone(),
        before_table_version: actual.table_version,
        after_table_version: result.table_version,
        measured_property,
        source_head_sha256,
        result_head_sha256: Sha256::digest(&head).to_string(),
        retained_history_verified: true,
        incremental_publication: true,
        complete_candidate_bytes,
        published_image_artifact_bytes,
        acknowledged_latency_ns: probe.acknowledged_latency_ns,
        unattributed_latency_ns,
        probe,
    })
}

pub async fn verify(root: &Path) -> Result<VerifyOutput, WorkerError> {
    let table = Table::new(LocalObjectStore::new(root)?);
    let status = table.pin().await?.status();
    table.verify_history().await?;
    Ok(VerifyOutput {
        ok: true,
        table_version: status.table_version,
        retained_history_verified: true,
    })
}

fn validate_probe(probe: &ProbeReport) -> Result<(), WorkerError> {
    let names = probe
        .phases
        .iter()
        .map(|phase| phase.name.as_str())
        .collect::<Vec<_>>();
    for required in REQUIRED_PHASES {
        if names.iter().filter(|name| *name == required).count() != 1 {
            return Err(WorkerError::Probe(super::ProbeError(format!(
                "required phase {required} was not recorded exactly once"
            ))));
        }
    }
    let top_level = probe
        .phases
        .iter()
        .filter(|phase| phase.depth == 0)
        .map(|phase| phase.name.as_str())
        .collect::<BTreeSet<_>>();
    if top_level
        != BTreeSet::from([
            "parent_pin",
            "idempotency",
            "candidate_build",
            "immutable_publication",
            "head_cas",
        ])
    {
        return Err(WorkerError::Probe(super::ProbeError(
            "top-level phases are incomplete or overlap".into(),
        )));
    }
    Ok(())
}

fn counter(probe: &ProbeReport, name: &str) -> u64 {
    probe.counters.get(name).copied().unwrap_or_default()
}

fn inspect(
    root: &Path,
    property_bytes: usize,
    retained_history_verified: bool,
) -> Result<FixtureIdentity, WorkerError> {
    let head_bytes = fs::read(root.join("_otmp/HEAD"))?;
    let head: Head = canonical_json::from_slice_canonical(&head_bytes)?;
    let generation_bytes = fs::read(root.join(head.metadata_generation.uri.as_str()))?;
    if Sha256::digest(&generation_bytes) != head.metadata_generation.sha256
        || head
            .metadata_generation
            .length
            .is_none_or(|length| length.0 != generation_bytes.len() as u64)
    {
        return Err(WorkerError::Fixture(
            "generation identity differs from HEAD".into(),
        ));
    }
    let generation: Generation = canonical_json::from_slice_canonical(&generation_bytes)?;
    let commit_bytes = fs::read(root.join(head.semantic_commit.uri.as_str()))?;
    if Sha256::digest(&commit_bytes) != head.semantic_commit.sha256
        || head
            .semantic_commit
            .length
            .is_none_or(|length| length.0 != commit_bytes.len() as u64)
    {
        return Err(WorkerError::Fixture(
            "commit identity differs from HEAD".into(),
        ));
    }
    let commit: SemanticCommit = canonical_json::from_slice_canonical(&commit_bytes)?;
    let parent_reference = commit
        .parent_commit
        .ok_or_else(|| WorkerError::Fixture("fixture has no payload commit".into()))?;
    let payload_commit_bytes = fs::read(root.join(parent_reference.uri.as_str()))?;
    if Sha256::digest(&payload_commit_bytes) != parent_reference.sha256
        || parent_reference
            .length
            .is_none_or(|length| length.0 != payload_commit_bytes.len() as u64)
    {
        return Err(WorkerError::Fixture(
            "payload commit identity differs from the retained reference".into(),
        ));
    }
    let payload_commit: SemanticCommit =
        canonical_json::from_slice_canonical(&payload_commit_bytes)?;
    let actual_property_bytes = payload_commit
        .operations
        .iter()
        .find_map(|operation| {
            let CanonicalValue::Object(operation) = operation else {
                return None;
            };
            let Some(CanonicalValue::Object(updates)) = operation.get("updates") else {
                return None;
            };
            let Some(CanonicalValue::String(value)) = updates.get("qualification.payload") else {
                return None;
            };
            Some(value.len())
        })
        .ok_or_else(|| WorkerError::Fixture("fixture payload property is absent".into()))?;
    if actual_property_bytes != property_bytes {
        return Err(WorkerError::Fixture(
            "fixture payload size differs from its prepared manifest".into(),
        ));
    }
    Ok(FixtureIdentity {
        property_bytes,
        table_id: head.table_id.to_string(),
        table_version: head.table_version.0,
        generation_id: generation.generation_id.to_string(),
        head_sha256: Sha256::digest(&head_bytes).to_string(),
        head_bytes: head_bytes.len() as u64,
        logical_image_bytes: generation
            .metadata_image
            .page_count
            .0
            .checked_mul(u64::from(generation.metadata_image.page_size))
            .ok_or_else(|| WorkerError::Fixture("logical image size overflow".into()))?,
        checkpoint_bytes: generation.metadata_image.checkpoint.length.0,
        commit_bytes: commit_bytes.len() as u64,
        retained_history_verified,
    })
}

fn property_request(
    key: &str,
    property: &str,
    operation: &str,
    value: String,
) -> TransactionRequest {
    TransactionRequest {
        idempotency_key: key.into(),
        requirements: vec![Requirement::PropertyIs {
            key: property.into(),
            value: CanonicalValue::Null,
        }],
        operations: vec![OperationRequest::SetProperties {
            operation_id: operation.into(),
            updates: BTreeMap::from([(property.into(), CanonicalValue::String(value))]),
            removals: Vec::new(),
        }],
        commit_metadata: CommitMetadata::default(),
    }
}

fn schema() -> Result<Schema, WorkerError> {
    Ok(serde_json::from_slice(include_bytes!(
        "../../../conformance/sources/schema.json"
    ))?)
}

fn read_config<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, WorkerError> {
    serde_json::from_slice(&fs::read(path)?).map_err(|error| WorkerError::Config(error.to_string()))
}
