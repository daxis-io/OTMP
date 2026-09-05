pub(crate) mod history;
pub(crate) mod transactions;
pub use history::*;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
pub use transactions::{
    OperationRequest, OperationResult, RefType, Requirement, TransactionRequest, TransactionResult,
};

use otmp_protocol::{
    CHECKPOINT_MEDIA_TYPE, COMMIT_MEDIA_TYPE, CORE_FEATURE, CanonicalValue, FeatureSet,
    GENERATION_MEDIA_TYPE, Generation, Head, Id, IntentRecord, JsonI64, JsonU64, LogicalType,
    MetadataImage, ObjectReference, PARQUET_FEATURE, ProtocolError, RelativeUri,
    SQLITE_COW_FEATURE, Schema, SemanticCommit, Sha256, TypedScalar, canonical_json,
    encode_partition_tuple, encode_typed_scalar, genesis_state_hash, image_root_hash, intent_hash,
    next_state_hash, object_hash, partition_hash,
};
use serde::{Deserialize, Deserializer, Serialize, de};
use uuid::Uuid;

use crate::RuntimeError;
use crate::image::{
    self, AppendImage, ExpectedImage, GenesisImage, ImageFile, ImageMetric, MaterializedImage,
};
use crate::storage::{
    ConditionalWriteOutcome, ObjectStore, ObjectVersion, StorageError, StoredObject,
};

const HEAD_KEY: &str = "_otmp/HEAD";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Parquet,
}

impl FileFormat {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceFingerprint {
    pub sha256: Sha256,
    pub length: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMetric {
    pub field_id: u32,
    #[serde(default)]
    pub column_size_bytes: Option<u64>,
    #[serde(default)]
    pub value_count: Option<u64>,
    #[serde(default)]
    pub null_count: Option<u64>,
    #[serde(default)]
    pub nan_count: Option<u64>,
    #[serde(default)]
    pub distinct_count: Option<u64>,
    #[serde(default)]
    pub lower_bound: Option<TypedScalar>,
    #[serde(default)]
    pub upper_bound: Option<TypedScalar>,
    #[serde(default)]
    pub metadata: BTreeMap<String, CanonicalValue>,
}

#[derive(Clone, Debug)]
pub struct AppendFile {
    pub source_path: PathBuf,
    pub fingerprint: SourceFingerprint,
    pub format: FileFormat,
    pub record_count: u64,
    pub schema_id: u32,
    pub partition_spec_id: u32,
    pub sort_order_id: u32,
    pub partition_values: BTreeMap<u32, TypedScalar>,
    pub metrics: Vec<FileMetric>,
    pub metadata: BTreeMap<String, CanonicalValue>,
}

#[derive(Clone, Debug)]
pub struct AppendRequest {
    pub idempotency_key: String,
    pub target_ref: String,
    pub files: Vec<AppendFile>,
    pub summary: BTreeMap<String, CanonicalValue>,
    pub commit_metadata: CommitMetadata,
    pub snapshot_metadata: SnapshotMetadata,
}

impl AppendRequest {
    #[must_use]
    pub fn new(idempotency_key: impl Into<String>, files: Vec<AppendFile>) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            target_ref: "main".into(),
            files,
            summary: BTreeMap::new(),
            commit_metadata: CommitMetadata::default(),
            snapshot_metadata: SnapshotMetadata::default(),
        }
    }
}

/// Stable, caller-controlled metadata describing a semantic transaction.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(transparent)]
pub struct CommitMetadata(BTreeMap<String, CanonicalValue>);

impl CommitMetadata {
    #[must_use]
    pub fn as_object(&self) -> &BTreeMap<String, CanonicalValue> {
        &self.0
    }
}

impl From<BTreeMap<String, CanonicalValue>> for CommitMetadata {
    fn from(value: BTreeMap<String, CanonicalValue>) -> Self {
        Self(value)
    }
}

impl<'de> Deserialize<'de> for CommitMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_metadata(deserializer).map(Self)
    }
}

/// Stable, caller-controlled metadata describing an immutable data snapshot.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SnapshotMetadata(BTreeMap<String, CanonicalValue>);

impl SnapshotMetadata {
    #[must_use]
    pub fn as_object(&self) -> &BTreeMap<String, CanonicalValue> {
        &self.0
    }
}

impl From<BTreeMap<String, CanonicalValue>> for SnapshotMetadata {
    fn from(value: BTreeMap<String, CanonicalValue>) -> Self {
        Self(value)
    }
}

impl<'de> Deserialize<'de> for SnapshotMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_metadata(deserializer).map(Self)
    }
}

fn deserialize_metadata<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, CanonicalValue>, D::Error>
where
    D: Deserializer<'de>,
{
    match CanonicalValue::deserialize(deserializer)? {
        CanonicalValue::Object(metadata) => Ok(metadata),
        _ => Err(de::Error::custom("OTMP metadata must be a JSON object")),
    }
}

#[derive(Clone, Debug)]
pub struct InitializeRequest {
    pub schema: Schema,
    pub metadata: BTreeMap<String, CanonicalValue>,
}

impl InitializeRequest {
    #[must_use]
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionRetryPolicy {
    pub maximum_rebases: u32,
    pub maximum_indeterminate_reconciliations: u32,
}

impl Default for TransactionRetryPolicy {
    fn default() -> Self {
        Self {
            maximum_rebases: 8,
            maximum_indeterminate_reconciliations: 3,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedFile {
    pub file_id: Id,
    pub uri: RelativeUri,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendResult {
    #[serde(with = "u64_string")]
    pub table_version: u64,
    pub commit_id: Id,
    pub snapshot_id: Id,
    #[serde(with = "u64_string")]
    pub sequence_number: u64,
    #[serde(rename = "ref")]
    pub target_ref: String,
    pub files: Vec<CommittedFile>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Status {
    pub table_id: Id,
    pub table_version: u64,
    pub root_revision: u64,
    pub semantic_state_sha256: Sha256,
    pub current_snapshot_id: Option<Id>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LiveFile {
    pub file_id: Id,
    pub uri: RelativeUri,
    pub file_format: String,
    pub file_size_bytes: u64,
    pub record_count: u64,
    pub content_sha256: Option<Sha256>,
    pub sequence_number: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HistoryEntry {
    pub table_version: u64,
    pub commit_id: Id,
    pub created_at_ms: i64,
    pub semantic_state_sha256: Sha256,
    pub commit_object_uri: RelativeUri,
}

#[derive(Clone, Debug)]
pub struct VerifiedStagedFile {
    table_id: Id,
    ordinal: usize,
    file_id: Id,
    uri: RelativeUri,
    version: ObjectVersion,
    sha256: Sha256,
    length: u64,
    format: FileFormat,
    object_identity: Option<String>,
}

impl VerifiedStagedFile {
    #[must_use]
    pub const fn file_id(&self) -> Id {
        self.file_id
    }

    #[must_use]
    pub const fn uri(&self) -> &RelativeUri {
        &self.uri
    }
}

pub struct PinnedTable {
    raw_head: Vec<u8>,
    head_version: ObjectVersion,
    head: Head,
    commit: SemanticCommit,
    generation: Generation,
    current_main: Option<Id>,
    checkpoint_bytes: Vec<u8>,
    image: MaterializedImage,
}

impl PinnedTable {
    #[must_use]
    pub fn status(&self) -> Status {
        Status {
            table_id: self.head.table_id,
            table_version: self.head.table_version.0,
            root_revision: self.head.root_revision.0,
            semantic_state_sha256: self.head.semantic_state_sha256,
            current_snapshot_id: self.current_main,
        }
    }

    pub fn files(&self, reference: &str) -> Result<Vec<LiveFile>, RuntimeError> {
        let connection = image::open_readonly(&self.image.path)?;
        let exists: i64 = connection.query_row(
            "SELECT count(*) FROM otmp_refs WHERE ref_name=?1 AND ref_type='branch'",
            [reference],
            |row| row.get(0),
        )?;
        if exists != 1 {
            return Err(RuntimeError::RefNotFound(reference.to_owned()));
        }
        let mut statement = connection.prepare(
            "SELECT file_id, uri, file_format, file_size_bytes, record_count, content_sha256, file_sequence_number FROM otmp_live_files WHERE ref_name=?1 ORDER BY file_sequence_number, file_id",
        )?;
        let rows = statement.query_map([reference], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Option<Vec<u8>>>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })?;
        rows.map(|row| {
            let row = row?;
            Ok(LiveFile {
                file_id: id_from_blob(row.0)?,
                uri: row.1.parse()?,
                file_format: row.2,
                file_size_bytes: nonnegative(row.3, "file size")?,
                record_count: nonnegative(row.4, "record count")?,
                content_sha256: row.5.map(hash_from_blob).transpose()?,
                sequence_number: nonnegative(row.6, "sequence number")?,
            })
        })
        .collect()
    }

    pub fn history(&self) -> Result<Vec<HistoryEntry>, RuntimeError> {
        let connection = image::open_readonly(&self.image.path)?;
        let mut statement = connection.prepare(
            "SELECT table_version, commit_id, created_at_ms, semantic_state_sha256, commit_object_uri FROM otmp_commits ORDER BY table_version",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        rows.map(|row| {
            let row = row?;
            Ok(HistoryEntry {
                table_version: nonnegative(row.0, "table version")?,
                commit_id: id_from_blob(row.1)?,
                created_at_ms: row.2,
                semantic_state_sha256: hash_from_blob(row.3)?,
                commit_object_uri: row.4.parse()?,
            })
        })
        .collect()
    }
}

#[derive(Clone)]
pub struct Table<S> {
    store: S,
    retry_policy: TransactionRetryPolicy,
}

impl<S: ObjectStore> Table<S> {
    #[must_use]
    pub fn new(store: S) -> Self {
        Self {
            store,
            retry_policy: TransactionRetryPolicy::default(),
        }
    }

    #[must_use]
    pub fn with_retry_policy(mut self, retry_policy: TransactionRetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    #[allow(clippy::too_many_lines)]
    pub async fn initialize(&self, request: InitializeRequest) -> Result<Status, RuntimeError> {
        request.schema.validate()?;
        if request.schema.schema_id != 1 || request.schema.parent_schema_id.is_some() {
            return Err(RuntimeError::InvalidInitialize(
                "local/full-image profile genesis requires schema_id 1 with no parent schema"
                    .into(),
            ));
        }
        let table_id = new_id();
        let commit_id = new_id();
        let created_at_ms = now_ms()?;
        let features = runtime_features()?;
        let metadata = CanonicalValue::Object(request.metadata.clone());
        let result = object([("ref", string("main")), ("table_version", string("0"))]);
        let initialize_operation = object([
            ("operation_id", string("initialize")),
            ("type", string("initialize_table")),
            ("table_id", string(&table_id.to_string())),
            ("schema", canonical_json::to_value(&request.schema)?),
            ("partition_spec_id", string("0")),
            ("sort_order_id", string("0")),
            ("target_ref", string("main")),
        ]);
        let genesis_intent = intent_hash(&canonical_json::to_vec(&initialize_operation)?);
        let mut commit = SemanticCommit {
            kind: "otmp.semantic-commit".into(),
            format_version: 1,
            table_id,
            table_version: JsonU64(0),
            parent_table_version: None,
            commit_id,
            parent_commit: None,
            created_at_ms: JsonI64(created_at_ms),
            intents: vec![IntentRecord {
                key: "otmp.genesis".into(),
                intent_sha256: genesis_intent,
                operation_ids: vec!["initialize".into()],
                result: result.clone(),
            }],
            requirements: Vec::new(),
            operations: vec![initialize_operation],
            required_reader_features_after_commit: features.clone(),
            required_writer_features_after_commit: features.clone(),
            previous_semantic_state_sha256: None,
            semantic_state_sha256: Sha256::from_bytes([0; 32]),
            metadata,
        };
        commit.semantic_state_sha256 = genesis_state_hash(&commit_body(&commit)?);
        let commit_bytes = canonical_json::to_vec(&commit)?;
        let commit_hash = object_hash(&commit_bytes);
        let commit_uri: RelativeUri = format!("_otmp/commits/0/{commit_id}.json").parse()?;
        let result_json = canonical_text(&result)?;
        let operation_json = canonical_text(&commit.operations)?;
        let metadata_json = canonical_text(&commit.metadata)?;
        let features_json = canonical_text(&features)?;
        let checkpoint = image::create_genesis(&GenesisImage {
            table_id,
            schema: &request.schema,
            created_at_ms,
            semantic_state: commit.semantic_state_sha256,
            commit_id,
            commit_hash,
            commit_uri: commit_uri.as_str(),
            operation_json: &operation_json,
            result_json: &result_json,
            intent_hash: genesis_intent,
            metadata_json: &metadata_json,
            reader_features_json: &features_json,
            writer_features_json: &features_json,
        })?;
        let checkpoint_hash = object_hash(&checkpoint.bytes);
        let checkpoint_id = new_id();
        let checkpoint_uri: RelativeUri =
            format!("_otmp/checkpoints/0/{checkpoint_id}.sqlite3").parse()?;
        let generation_id = new_id();
        let commit_reference = object_reference(
            commit_uri.clone(),
            commit_hash,
            commit_bytes.len() as u64,
            COMMIT_MEDIA_TYPE,
        );
        let generation = Generation {
            kind: "otmp.metadata-generation".into(),
            format_version: 1,
            table_id,
            table_version: JsonU64(0),
            generation_id,
            created_at_ms: JsonI64(created_at_ms),
            semantic_state_sha256: commit.semantic_state_sha256,
            semantic_commit: commit_reference.clone(),
            physical_parent: None,
            metadata_image: MetadataImage {
                codec: SQLITE_COW_FEATURE.into(),
                page_size: image::PAGE_SIZE,
                page_count: JsonU64(checkpoint.page_count),
                checkpoint: otmp_protocol::Checkpoint {
                    table_version: JsonU64(0),
                    uri: checkpoint_uri.clone(),
                    sha256: checkpoint_hash,
                    length: JsonU64(checkpoint.bytes.len() as u64),
                },
                page_map: None,
                image_root_sha256: image_root_hash(
                    table_id,
                    0,
                    image::PAGE_SIZE,
                    checkpoint.page_count,
                    checkpoint_hash,
                    None,
                ),
            },
            scan_projection: None,
            metadata: BTreeMap::new(),
        };
        let generation_bytes = canonical_json::to_vec(&generation)?;
        let generation_hash = object_hash(&generation_bytes);
        let generation_uri: RelativeUri =
            format!("_otmp/generations/0/{generation_id}.json").parse()?;
        let head = Head {
            protocol: "otmp".into(),
            protocol_version: "0.0.2-alpha".into(),
            table_id,
            table_version: JsonU64(0),
            root_revision: JsonU64(0),
            semantic_state_sha256: commit.semantic_state_sha256,
            semantic_commit: commit_reference,
            metadata_generation: object_reference(
                generation_uri.clone(),
                generation_hash,
                generation_bytes.len() as u64,
                GENERATION_MEDIA_TYPE,
            ),
            required_reader_features: features.clone(),
            required_writer_features: features,
        };
        let head_bytes = canonical_json::to_vec(&head)?;

        put_immutable(&self.store, &commit_uri, &commit_bytes).await?;
        put_immutable(&self.store, &checkpoint_uri, &checkpoint.bytes).await?;
        put_immutable(&self.store, &generation_uri, &generation_bytes).await?;
        let mut reconciliations = 0;
        loop {
            match self.store.create_head(&head_bytes).await {
                ConditionalWriteOutcome::Applied { .. } => break,
                ConditionalWriteOutcome::Conflict { .. } => {
                    let current = self.store.read(&head_key()?).await?;
                    if current.bytes == head_bytes {
                        break;
                    }
                    return Err(RuntimeError::AlreadyExists);
                }
                ConditionalWriteOutcome::Indeterminate { .. } => {
                    reconciliations += 1;
                    match self.store.read(&head_key()?).await {
                        Ok(current) if current.bytes == head_bytes => break,
                        Ok(_) => return Err(RuntimeError::AlreadyExists),
                        Err(StorageError::NotFound(_))
                            if reconciliations
                                <= self.retry_policy.maximum_indeterminate_reconciliations => {}
                        Err(error)
                            if reconciliations
                                <= self.retry_policy.maximum_indeterminate_reconciliations =>
                        {
                            tracing::warn!(%error, "genesis reconciliation read failed");
                        }
                        Err(_) => return Err(RuntimeError::PublicationIndeterminate),
                    }
                }
            }
        }
        Ok(Status {
            table_id,
            table_version: 0,
            root_revision: 0,
            semantic_state_sha256: commit.semantic_state_sha256,
            current_snapshot_id: None,
        })
    }

    pub async fn pin(&self) -> Result<PinnedTable, RuntimeError> {
        let raw_head = self.store.read(&head_key()?).await?;
        let head: Head = canonical_json::from_slice_canonical(&raw_head.bytes)?;
        self.load_pin(raw_head, head).await
    }

    async fn load_pin(
        &self,
        raw_head: StoredObject,
        head: Head,
    ) -> Result<PinnedTable, RuntimeError> {
        let supported = BTreeSet::from([
            CORE_FEATURE,
            PARQUET_FEATURE,
            SQLITE_COW_FEATURE,
            "otmp.refs.v1",
        ]);
        head.validate(&supported)?;
        head.required_writer_features
            .require_supported(&supported)?;

        let commit_object = verified_read(&self.store, &head.semantic_commit).await?;
        let commit: SemanticCommit = canonical_json::from_slice_canonical(&commit_object.bytes)?;
        commit.validate_runtime_profile()?;
        if commit.table_id != head.table_id
            || commit.table_version != head.table_version
            || commit.semantic_state_sha256 != head.semantic_state_sha256
            || commit.required_reader_features_after_commit != head.required_reader_features
            || commit.required_writer_features_after_commit != head.required_writer_features
        {
            return Err(RuntimeError::Corrupt(
                "semantic commit does not match HEAD".into(),
            ));
        }
        let recomputed_state = if let Some(previous) = commit.previous_semantic_state_sha256 {
            next_state_hash(previous, &commit_body(&commit)?)
        } else {
            genesis_state_hash(&commit_body(&commit)?)
        };
        if recomputed_state != commit.semantic_state_sha256 {
            return Err(RuntimeError::Corrupt("semantic state hash mismatch".into()));
        }

        let generation_object = verified_read(&self.store, &head.metadata_generation).await?;
        let generation: Generation =
            canonical_json::from_slice_canonical(&generation_object.bytes)?;
        generation.validate_runtime_profile()?;
        if generation.table_id != head.table_id
            || generation.table_version != head.table_version
            || generation.semantic_state_sha256 != head.semantic_state_sha256
            || generation.semantic_commit != head.semantic_commit
        {
            return Err(RuntimeError::Corrupt(
                "generation does not match HEAD".into(),
            ));
        }
        let checkpoint_ref = ObjectReference {
            uri: generation.metadata_image.checkpoint.uri.clone(),
            sha256: generation.metadata_image.checkpoint.sha256,
            length: Some(generation.metadata_image.checkpoint.length),
            media_type: Some(CHECKPOINT_MEDIA_TYPE.into()),
        };
        let checkpoint = verified_read(&self.store, &checkpoint_ref).await?;
        if checkpoint.bytes.len() % image::PAGE_SIZE as usize != 0
            || (checkpoint.bytes.len() / image::PAGE_SIZE as usize) as u64
                != generation.metadata_image.page_count.0
            || image_root_hash(
                head.table_id,
                head.table_version.0,
                image::PAGE_SIZE,
                generation.metadata_image.page_count.0,
                generation.metadata_image.checkpoint.sha256,
                None,
            ) != generation.metadata_image.image_root_sha256
        {
            return Err(RuntimeError::Corrupt("metadata image root mismatch".into()));
        }
        let image = image::materialize(&checkpoint.bytes)?;
        let reader_features_json = canonical_text(&head.required_reader_features)?;
        let writer_features_json = canonical_text(&head.required_writer_features)?;
        image::validate(
            &image.path,
            &ExpectedImage {
                table_id: head.table_id,
                table_version: head.table_version.0,
                semantic_state: head.semantic_state_sha256,
                commit_id: commit.commit_id,
                commit_hash: head.semantic_commit.sha256,
                commit_uri: head.semantic_commit.uri.as_str(),
                reader_features_json: &reader_features_json,
                writer_features_json: &writer_features_json,
                previous_semantic_state: commit.previous_semantic_state_sha256,
            },
        )?;
        image::validate_commit_projection(&image.path, &commit)?;
        let current_main = query_optional_id(
            &image.path,
            "SELECT snapshot_id FROM otmp_refs WHERE ref_name='main'",
        )?;
        Ok(PinnedTable {
            current_main,
            raw_head: raw_head.bytes,
            head_version: raw_head.version,
            head,
            commit,
            generation,
            checkpoint_bytes: checkpoint.bytes,
            image,
        })
    }

    pub async fn stage_file(
        &self,
        table_id: Id,
        ordinal: usize,
        file: &AppendFile,
    ) -> Result<VerifiedStagedFile, RuntimeError> {
        let file_id = new_id();
        let uri: RelativeUri = format!("data/{file_id}.parquet").parse()?;
        let mut source = tokio::fs::File::open(&file.source_path).await?;
        let created = self
            .store
            .create_from_reader(&uri, &mut source, Some(file.fingerprint.length))
            .await
            .map_err(|error| match error {
                StorageError::MaximumLengthExceeded => RuntimeError::FingerprintMismatch,
                other => other.into(),
            })?;
        if created.length != file.fingerprint.length || created.sha256 != file.fingerprint.sha256 {
            let _ = self.store.delete_if_version(&uri, &created.version).await;
            return Err(RuntimeError::FingerprintMismatch);
        }
        let readback = self
            .store
            .confirm_readable(&uri, file.fingerprint.sha256, file.fingerprint.length)
            .await;
        let readback_version = match readback {
            Ok(version) => version,
            Err(error) => {
                let _ = self.store.delete_if_version(&uri, &created.version).await;
                return Err(match error {
                    StorageError::VerificationFailed(_) => RuntimeError::FingerprintMismatch,
                    other => other.into(),
                });
            }
        };
        if readback_version != created.version {
            let _ = self.store.delete_if_version(&uri, &created.version).await;
            return Err(RuntimeError::FingerprintMismatch);
        }
        failpoint("after_staging_flush");
        Ok(VerifiedStagedFile {
            table_id,
            ordinal,
            file_id,
            uri,
            version: created.version,
            sha256: created.sha256,
            length: created.length,
            format: file.format,
            object_identity: None,
        })
    }

    pub async fn append_files(
        &self,
        request: &AppendRequest,
    ) -> Result<AppendResult, RuntimeError> {
        let logical = logical_intent(request)?;
        let logical_hash = intent_hash(&logical);
        let first_pin = self.pin().await?;
        if let Some(result) = check_idempotency(&first_pin, &request.idempotency_key, logical_hash)?
        {
            return Ok(result);
        }
        validate_request(request, &first_pin)?;
        let mut staged = Vec::with_capacity(request.files.len());
        for (ordinal, file) in request.files.iter().enumerate() {
            match self
                .stage_file(first_pin.head.table_id, ordinal, file)
                .await
            {
                Ok(value) => staged.push(value),
                Err(error) => {
                    cleanup(&self.store, &staged).await;
                    return Err(error);
                }
            }
        }
        let second_pin = match self.pin().await {
            Ok(pin) => pin,
            Err(error) => {
                cleanup(&self.store, &staged).await;
                return Err(error);
            }
        };
        if let Some(result) =
            check_idempotency(&second_pin, &request.idempotency_key, logical_hash)?
        {
            cleanup(&self.store, &staged).await;
            return Ok(result);
        }
        let base_tip = transactions::ref_row(
            &image::open_readonly(&first_pin.image.path)?,
            &request.target_ref,
        )?;
        if let Err(error) = history::validate_append_rebase(
            &second_pin,
            &request.target_ref,
            base_tip,
            first_pin.head.table_version.0,
        ) {
            cleanup(&self.store, &staged).await;
            return Err(error);
        }
        if let Err(error) = validate_request(request, &second_pin) {
            cleanup(&self.store, &staged).await;
            return Err(error);
        }
        self.commit_staged_from_base(request, &staged, &first_pin)
            .await
    }

    pub async fn commit_staged_files(
        &self,
        request: &AppendRequest,
        staged: &[VerifiedStagedFile],
    ) -> Result<AppendResult, RuntimeError> {
        let pinned = self.pin().await?;
        self.commit_staged_from_base(request, staged, &pinned).await
    }

    async fn commit_staged_from_base(
        &self,
        request: &AppendRequest,
        staged: &[VerifiedStagedFile],
        pinned: &PinnedTable,
    ) -> Result<AppendResult, RuntimeError> {
        let logical_hash = intent_hash(&logical_intent(request)?);
        validate_staged(request, staged, pinned.head.table_id)?;
        let base_tip = transactions::ref_row(
            &image::open_readonly(&pinned.image.path)?,
            &request.target_ref,
        )?;
        let base_version = pinned.head.table_version.0;
        let (result, _) = self
            .publish_transaction(&request.idempotency_key, logical_hash, staged, |parent| {
                if parent.head.table_id != pinned.head.table_id {
                    return Err(RuntimeError::SemanticConflict(
                        "table identity changed".into(),
                    ));
                }
                history::validate_append_rebase(
                    parent,
                    &request.target_ref,
                    base_tip,
                    base_version,
                )?;
                validate_request(request, parent)?;
                build_candidate(request, staged, logical_hash, parent)
            })
            .await?;
        Ok(result)
    }

    #[allow(clippy::too_many_lines)] // Keep conditional outcomes and reconciliation in one state machine.
    async fn publish_transaction<R: serde::de::DeserializeOwned>(
        &self,
        key: &str,
        logical_hash: Sha256,
        staged: &[VerifiedStagedFile],
        build: impl Fn(&PinnedTable) -> Result<Candidate<R>, RuntimeError>,
    ) -> Result<(R, Sha256), RuntimeError> {
        let mut parent = self.pin().await?;
        let table_id = parent.head.table_id;

        let mut rebases = 0;
        loop {
            if parent.head.table_id != table_id {
                return Err(RuntimeError::SemanticConflict(
                    "table identity changed during publication".into(),
                ));
            }
            if let Some(result) = replay::<R>(&parent, key, logical_hash)? {
                return Ok(result);
            }
            let candidate = build(&parent)?;
            for staged_file in staged {
                let version = self
                    .store
                    .confirm_readable(&staged_file.uri, staged_file.sha256, staged_file.length)
                    .await
                    .map_err(|error| match error {
                        StorageError::VerificationFailed(_) => RuntimeError::FingerprintMismatch,
                        other => other.into(),
                    })?;
                if version != staged_file.version {
                    return Err(RuntimeError::FingerprintMismatch);
                }
            }
            put_immutable(&self.store, &candidate.commit_uri, &candidate.commit_bytes).await?;
            put_immutable(
                &self.store,
                &candidate.checkpoint_uri,
                &candidate.checkpoint_bytes,
            )
            .await?;
            put_immutable(
                &self.store,
                &candidate.generation_uri,
                &candidate.generation_bytes,
            )
            .await?;
            failpoint("after_immutable_uploads");

            let mut indeterminate = 0;
            loop {
                match self
                    .store
                    .replace_head(&parent.head_version, &candidate.head_bytes)
                    .await
                {
                    ConditionalWriteOutcome::Applied { .. } => {
                        return Ok((candidate.result, candidate.semantic_state));
                    }
                    ConditionalWriteOutcome::Conflict { .. } => break,
                    ConditionalWriteOutcome::Indeterminate { source } => {
                        indeterminate += 1;
                        match self.pin().await {
                            Ok(current) => {
                                if current.head.table_id != table_id {
                                    return Err(RuntimeError::SemanticConflict(
                                        "table identity changed during reconciliation".into(),
                                    ));
                                }
                                if let Some(result) = replay::<R>(&current, key, logical_hash)? {
                                    return Ok(result);
                                }
                                if current.head_version == parent.head_version
                                    && current.raw_head == parent.raw_head
                                {
                                    if indeterminate
                                        <= self.retry_policy.maximum_indeterminate_reconciliations
                                    {
                                        continue;
                                    }
                                    return Err(RuntimeError::PublicationIndeterminate);
                                }
                                break;
                            }
                            Err(error)
                                if indeterminate
                                    <= self.retry_policy.maximum_indeterminate_reconciliations =>
                            {
                                tracing::warn!(%source, %error, "publication reconciliation failed");
                            }
                            Err(_) => return Err(RuntimeError::PublicationIndeterminate),
                        }
                    }
                }
            }
            let winner = self.pin().await?;
            if winner.head.table_id != table_id {
                return Err(RuntimeError::SemanticConflict(
                    "table identity changed after conflict".into(),
                ));
            }
            if let Some(result) = replay::<R>(&winner, key, logical_hash)? {
                return Ok(result);
            }
            rebases += 1;
            if rebases > self.retry_policy.maximum_rebases {
                return Err(RuntimeError::RebaseExhausted);
            }
            parent = winner;
        }
    }
}

struct Candidate<R = AppendResult> {
    semantic_state: Sha256,
    commit_uri: RelativeUri,
    commit_bytes: Vec<u8>,
    checkpoint_uri: RelativeUri,
    checkpoint_bytes: Vec<u8>,
    generation_uri: RelativeUri,
    generation_bytes: Vec<u8>,
    head_bytes: Vec<u8>,
    result: R,
}

#[allow(clippy::too_many_lines)]
fn build_candidate(
    request: &AppendRequest,
    staged: &[VerifiedStagedFile],
    logical_hash: Sha256,
    parent: &PinnedTable,
) -> Result<Candidate, RuntimeError> {
    let table_version = parent
        .head
        .table_version
        .0
        .checked_add(1)
        .ok_or_else(|| RuntimeError::InvalidAppend("table version exhausted".into()))?;
    let (_, _, last_sequence) = image::current_schema_and_snapshot(&parent.image.path)?;
    let (_, parent_snapshot) = transactions::ref_row(
        &image::open_readonly(&parent.image.path)?,
        &request.target_ref,
    )?
    .ok_or_else(|| RuntimeError::RefNotFound(request.target_ref.clone()))?;
    let sequence_number = last_sequence
        .checked_add(1)
        .ok_or_else(|| RuntimeError::InvalidAppend("sequence number exhausted".into()))?;
    let commit_id = new_id();
    let snapshot_id = new_id();
    let created_at_ms = now_ms()?;
    let result = AppendResult {
        table_version,
        commit_id,
        snapshot_id,
        sequence_number,
        target_ref: request.target_ref.clone(),
        files: staged
            .iter()
            .map(|file| CommittedFile {
                file_id: file.file_id,
                uri: file.uri.clone(),
            })
            .collect(),
    };
    let derived_summary = derived_summary(request)?;
    let operation_files = staged
        .iter()
        .zip(&request.files)
        .map(|(staged, logical)| {
            Ok(object([
                ("file_id", string(&staged.file_id.to_string())),
                ("uri", string(staged.uri.as_str())),
                ("object_identity", CanonicalValue::Null),
                ("file_format", string(logical.format.as_str())),
                (
                    "file_size_bytes",
                    string(&logical.fingerprint.length.to_string()),
                ),
                ("record_count", string(&logical.record_count.to_string())),
                ("schema_id", string(&logical.schema_id.to_string())),
                (
                    "partition_spec_id",
                    string(&logical.partition_spec_id.to_string()),
                ),
                ("sort_order_id", string(&logical.sort_order_id.to_string())),
                (
                    "content_sha256",
                    string(&logical.fingerprint.sha256.to_string()),
                ),
                (
                    "partition_values",
                    canonical_json::to_value(&logical.partition_values)?,
                ),
                (
                    "metrics",
                    canonical_json::to_value(&logical_metrics(&logical.metrics))?,
                ),
                ("metadata", CanonicalValue::Object(logical.metadata.clone())),
            ]))
        })
        .collect::<Result<Vec<_>, RuntimeError>>()?;
    let snapshot = object([
        ("snapshot_id", string(&snapshot_id.to_string())),
        (
            "parent_snapshot_id",
            parent_snapshot.map_or(CanonicalValue::Null, |id| string(&id.to_string())),
        ),
        ("sequence_number", string(&sequence_number.to_string())),
        ("schema_id", string(&request.files[0].schema_id.to_string())),
        (
            "partition_spec_id",
            string(&request.files[0].partition_spec_id.to_string()),
        ),
        (
            "sort_order_id",
            string(&request.files[0].sort_order_id.to_string()),
        ),
        ("operation", string("append")),
        ("summary", CanonicalValue::Object(derived_summary.clone())),
        (
            "metadata",
            CanonicalValue::Object(request.snapshot_metadata.0.clone()),
        ),
    ]);
    let operation = object([
        ("operation_id", string("append-main")),
        ("type", string("commit_snapshot")),
        ("target_ref", string(&request.target_ref)),
        ("snapshot", snapshot),
        ("added_files", CanonicalValue::Array(operation_files)),
        ("removed_file_ids", CanonicalValue::Array(Vec::new())),
        ("scan_projection", CanonicalValue::Null),
        ("rebase_mode", string("append-safe")),
    ]);
    let result_value = canonical_json::to_value(&result)?;
    let mut commit = SemanticCommit {
        kind: "otmp.semantic-commit".into(),
        format_version: 1,
        table_id: parent.head.table_id,
        table_version: JsonU64(table_version),
        parent_table_version: Some(parent.head.table_version),
        commit_id,
        parent_commit: Some(parent.head.semantic_commit.clone()),
        created_at_ms: JsonI64(created_at_ms),
        intents: vec![IntentRecord {
            key: request.idempotency_key.clone(),
            intent_sha256: logical_hash,
            operation_ids: vec!["append-main".into()],
            result: result_value.clone(),
        }],
        requirements: vec![
            object([
                ("type", string("current_schema_is")),
                ("schema_id", string(&request.files[0].schema_id.to_string())),
            ]),
            object([
                ("type", string("default_partition_spec_is")),
                ("partition_spec_id", string("0")),
            ]),
            object([
                ("type", string("default_sort_order_is")),
                ("sort_order_id", string("0")),
            ]),
        ],
        operations: vec![operation],
        required_reader_features_after_commit: parent.head.required_reader_features.clone(),
        required_writer_features_after_commit: parent.head.required_writer_features.clone(),
        previous_semantic_state_sha256: Some(parent.head.semantic_state_sha256),
        semantic_state_sha256: Sha256::from_bytes([0; 32]),
        metadata: CanonicalValue::Object(request.commit_metadata.0.clone()),
    };
    commit.semantic_state_sha256 =
        next_state_hash(parent.head.semantic_state_sha256, &commit_body(&commit)?);
    let commit_bytes = canonical_json::to_vec(&commit)?;
    let commit_hash = object_hash(&commit_bytes);
    let commit_uri: RelativeUri =
        format!("_otmp/commits/{table_version}/{commit_id}.json").parse()?;
    let result_json = canonical_text(&result_value)?;
    let operation_json = canonical_text(&commit.operations)?;
    let commit_metadata_json = canonical_text(&commit.metadata)?;
    let snapshot_metadata_json = canonical_text(&request.snapshot_metadata)?;
    let image_files = request
        .files
        .iter()
        .zip(staged)
        .map(|(logical, staged)| {
            let partition_cbor = encode_partition_tuple(&logical.partition_values);
            Ok(ImageFile {
                file_id: staged.file_id,
                uri: staged.uri.to_string(),
                format: logical.format,
                file_size_bytes: staged.length,
                record_count: logical.record_count,
                schema_id: logical.schema_id,
                partition_spec_id: logical.partition_spec_id,
                sort_order_id: logical.sort_order_id,
                partition_hash: partition_hash(logical.partition_spec_id, &partition_cbor),
                partition_values_cbor: partition_cbor,
                content_sha256: staged.sha256,
                metrics: logical
                    .metrics
                    .iter()
                    .map(|metric| {
                        Ok(ImageMetric {
                            field_id: metric.field_id,
                            column_size_bytes: metric.column_size_bytes,
                            value_count: metric.value_count,
                            null_count: metric.null_count,
                            nan_count: metric.nan_count,
                            distinct_count: metric.distinct_count,
                            lower_bound_cbor: metric.lower_bound.as_ref().map(encode_typed_scalar),
                            upper_bound_cbor: metric.upper_bound.as_ref().map(encode_typed_scalar),
                            metadata_json: canonical_text(&metric.metadata)?,
                        })
                    })
                    .collect::<Result<Vec<_>, RuntimeError>>()?,
                metadata_json: canonical_text(&logical.metadata)?,
            })
        })
        .collect::<Result<Vec<_>, RuntimeError>>()?;
    let checkpoint = image::apply_append(
        &parent.checkpoint_bytes,
        &AppendImage {
            table_version,
            created_at_ms,
            semantic_state: commit.semantic_state_sha256,
            commit_id,
            commit_hash,
            commit_uri: commit_uri.as_str(),
            operation_json: &operation_json,
            result_json: &result_json,
            commit_metadata_json: &commit_metadata_json,
            idempotency_key: &request.idempotency_key,
            intent_hash: logical_hash,
            snapshot_id,
            parent_snapshot_id: parent_snapshot,
            target_ref: &request.target_ref,
            sequence_number,
            summary: &derived_summary,
            snapshot_metadata_json: &snapshot_metadata_json,
            files: &image_files,
        },
    )?;
    finish_candidate(
        parent,
        &commit,
        commit_uri,
        commit_bytes,
        checkpoint,
        result,
    )
}

#[allow(clippy::too_many_lines)]
fn finish_candidate<R>(
    parent: &PinnedTable,
    commit: &SemanticCommit,
    commit_uri: RelativeUri,
    commit_bytes: Vec<u8>,
    checkpoint: image::CheckpointImage,
    result: R,
) -> Result<Candidate<R>, RuntimeError> {
    commit.validate_runtime_profile()?;
    let table_version = commit.table_version.0;
    let commit_id = commit.commit_id;
    let created_at_ms = commit.created_at_ms.0;
    let commit_hash = object_hash(&commit_bytes);
    let checkpoint_hash = object_hash(&checkpoint.bytes);
    let checkpoint_id = new_id();
    let checkpoint_uri: RelativeUri =
        format!("_otmp/checkpoints/{table_version}/{checkpoint_id}.sqlite3").parse()?;
    image::validate(
        &checkpoint.path,
        &ExpectedImage {
            table_id: parent.head.table_id,
            table_version,
            semantic_state: commit.semantic_state_sha256,
            commit_id,
            commit_hash,
            commit_uri: commit_uri.as_str(),
            reader_features_json: &canonical_text(&parent.head.required_reader_features)?,
            writer_features_json: &canonical_text(&parent.head.required_writer_features)?,
            previous_semantic_state: commit.previous_semantic_state_sha256,
        },
    )?;
    let generation_id = new_id();
    let generation_uri: RelativeUri =
        format!("_otmp/generations/{table_version}/{generation_id}.json").parse()?;
    let generation = Generation {
        kind: "otmp.metadata-generation".into(),
        format_version: 1,
        table_id: parent.head.table_id,
        table_version: JsonU64(table_version),
        generation_id,
        created_at_ms: JsonI64(created_at_ms),
        semantic_state_sha256: commit.semantic_state_sha256,
        semantic_commit: object_reference(
            commit_uri.clone(),
            commit_hash,
            commit_bytes.len() as u64,
            COMMIT_MEDIA_TYPE,
        ),
        physical_parent: Some(parent.head.metadata_generation.clone()),
        metadata_image: MetadataImage {
            codec: SQLITE_COW_FEATURE.into(),
            page_size: image::PAGE_SIZE,
            page_count: JsonU64(checkpoint.page_count),
            checkpoint: otmp_protocol::Checkpoint {
                table_version: JsonU64(table_version),
                uri: checkpoint_uri.clone(),
                sha256: checkpoint_hash,
                length: JsonU64(checkpoint.bytes.len() as u64),
            },
            page_map: None,
            image_root_sha256: image_root_hash(
                parent.head.table_id,
                table_version,
                image::PAGE_SIZE,
                checkpoint.page_count,
                checkpoint_hash,
                None,
            ),
        },
        scan_projection: None,
        metadata: BTreeMap::new(),
    };
    let generation_bytes = canonical_json::to_vec(&generation)?;
    let generation_hash = object_hash(&generation_bytes);
    let head = Head {
        protocol: "otmp".into(),
        protocol_version: "0.0.2-alpha".into(),
        table_id: parent.head.table_id,
        table_version: JsonU64(table_version),
        root_revision: JsonU64(
            parent
                .head
                .root_revision
                .0
                .checked_add(1)
                .ok_or_else(|| RuntimeError::InvalidAppend("root revision exhausted".into()))?,
        ),
        semantic_state_sha256: commit.semantic_state_sha256,
        semantic_commit: generation.semantic_commit.clone(),
        metadata_generation: object_reference(
            generation_uri.clone(),
            generation_hash,
            generation_bytes.len() as u64,
            GENERATION_MEDIA_TYPE,
        ),
        required_reader_features: parent.head.required_reader_features.clone(),
        required_writer_features: parent.head.required_writer_features.clone(),
    };
    image::validate_commit_projection(&checkpoint.path, commit)?;
    let committed_state = image::open_readonly(&checkpoint.path)?.query_row(
        "SELECT semantic_state_sha256 FROM otmp_commits WHERE commit_id=?1",
        [commit.commit_id.as_bytes().as_slice()],
        |r| r.get(0),
    )?;
    Ok(Candidate {
        semantic_state: hash_from_blob(committed_state)?,
        commit_uri,
        commit_bytes,
        checkpoint_uri,
        checkpoint_bytes: checkpoint.bytes,
        generation_uri,
        generation_bytes,
        head_bytes: canonical_json::to_vec(&head)?,
        result,
    })
}

#[derive(Serialize)]
struct LogicalIntent<'a> {
    operation: &'static str,
    target_ref: &'a str,
    files: Vec<LogicalFile<'a>>,
    caller_summary: &'a BTreeMap<String, CanonicalValue>,
    commit_metadata: &'a CommitMetadata,
    snapshot_metadata: &'a SnapshotMetadata,
}

#[derive(Serialize)]
struct LogicalFile<'a> {
    expected_sha256: Sha256,
    expected_length: String,
    format: FileFormat,
    record_count: String,
    schema_id: String,
    partition_spec_id: String,
    sort_order_id: String,
    partition_values: &'a BTreeMap<u32, TypedScalar>,
    metrics: Vec<LogicalMetric<'a>>,
    metadata: &'a BTreeMap<String, CanonicalValue>,
}

#[derive(Serialize)]
struct LogicalMetric<'a> {
    field_id: String,
    column_size_bytes: Option<String>,
    value_count: Option<String>,
    null_count: Option<String>,
    nan_count: Option<String>,
    distinct_count: Option<String>,
    lower_bound: &'a Option<TypedScalar>,
    upper_bound: &'a Option<TypedScalar>,
    metadata: &'a BTreeMap<String, CanonicalValue>,
}

fn logical_metrics(metrics: &[FileMetric]) -> Vec<LogicalMetric<'_>> {
    metrics
        .iter()
        .map(|metric| LogicalMetric {
            field_id: metric.field_id.to_string(),
            column_size_bytes: metric.column_size_bytes.map(|value| value.to_string()),
            value_count: metric.value_count.map(|value| value.to_string()),
            null_count: metric.null_count.map(|value| value.to_string()),
            nan_count: metric.nan_count.map(|value| value.to_string()),
            distinct_count: metric.distinct_count.map(|value| value.to_string()),
            lower_bound: &metric.lower_bound,
            upper_bound: &metric.upper_bound,
            metadata: &metric.metadata,
        })
        .collect()
}

fn logical_intent(request: &AppendRequest) -> Result<Vec<u8>, RuntimeError> {
    let logical = LogicalIntent {
        operation: "append",
        target_ref: &request.target_ref,
        files: request
            .files
            .iter()
            .map(|file| LogicalFile {
                expected_sha256: file.fingerprint.sha256,
                expected_length: file.fingerprint.length.to_string(),
                format: file.format,
                record_count: file.record_count.to_string(),
                schema_id: file.schema_id.to_string(),
                partition_spec_id: file.partition_spec_id.to_string(),
                sort_order_id: file.sort_order_id.to_string(),
                partition_values: &file.partition_values,
                metrics: logical_metrics(&file.metrics),
                metadata: &file.metadata,
            })
            .collect(),
        caller_summary: &request.summary,
        commit_metadata: &request.commit_metadata,
        snapshot_metadata: &request.snapshot_metadata,
    };
    Ok(canonical_json::to_vec(&logical)?)
}

fn validate_request(request: &AppendRequest, pinned: &PinnedTable) -> Result<(), RuntimeError> {
    if request.idempotency_key.is_empty() || request.idempotency_key == "otmp.genesis" {
        return Err(RuntimeError::InvalidAppend(
            "invalid idempotency key".into(),
        ));
    }
    if request.files.is_empty() {
        return Err(RuntimeError::InvalidAppend(
            "local/full-image profile requires one non-empty append batch to main".into(),
        ));
    }
    let connection = image::open_readonly(&pinned.image.path)?;
    if !matches!(
        transactions::ref_row(&connection, &request.target_ref)?,
        Some((RefType::Branch, _))
    ) {
        return Err(RuntimeError::InvalidAppend(
            "target must exist and be a branch".into(),
        ));
    }
    let (current_schema, _, _) = image::current_schema_and_snapshot(&pinned.image.path)?;
    let field_types = image::field_types(&pinned.image.path, current_schema)?;
    let reserved = ["added-data-files", "added-records", "added-files-size"];
    if request
        .summary
        .keys()
        .any(|key| reserved.contains(&key.as_str()))
    {
        return Err(RuntimeError::InvalidAppend(
            "caller summary uses a runtime-reserved key".into(),
        ));
    }
    let mut logical_entries = BTreeSet::new();
    for file in &request.files {
        if file.schema_id != current_schema
            || file.partition_spec_id != 0
            || file.sort_order_id != 0
            || !file.partition_values.is_empty()
            || file.fingerprint.length > i64::MAX as u64
            || file.record_count > i64::MAX as u64
        {
            return Err(RuntimeError::InvalidAppend(
                "file assertions do not match local/full-image profile table defaults".into(),
            ));
        }
        let entry = canonical_json::to_vec(&LogicalFile {
            expected_sha256: file.fingerprint.sha256,
            expected_length: file.fingerprint.length.to_string(),
            format: file.format,
            record_count: file.record_count.to_string(),
            schema_id: file.schema_id.to_string(),
            partition_spec_id: file.partition_spec_id.to_string(),
            sort_order_id: file.sort_order_id.to_string(),
            partition_values: &file.partition_values,
            metrics: logical_metrics(&file.metrics),
            metadata: &file.metadata,
        })?;
        if !logical_entries.insert(entry) {
            return Err(RuntimeError::InvalidAppend(
                "duplicate logical file entry".into(),
            ));
        }
        validate_metrics(&file.metrics, &field_types)?;
    }
    derived_summary(request)?;
    Ok(())
}

fn validate_metrics(
    metrics: &[FileMetric],
    field_types: &BTreeMap<u32, LogicalType>,
) -> Result<(), RuntimeError> {
    let mut metric_ids = BTreeSet::new();
    for metric in metrics {
        if !metric_ids.insert(metric.field_id) {
            return Err(RuntimeError::InvalidAppend(
                "duplicate metric field ID".into(),
            ));
        }
        let Some(field_type) = field_types.get(&metric.field_id) else {
            return Err(RuntimeError::InvalidAppend(
                "metric field ID does not exist".into(),
            ));
        };
        if !field_type.is_primitive() {
            return Err(RuntimeError::InvalidAppend(
                "metrics require primitive fields".into(),
            ));
        }
        if metric
            .null_count
            .zip(metric.value_count)
            .is_some_and(|(nulls, values)| nulls > values)
            || metric.nan_count.is_some() && !field_type.is_float()
        {
            return Err(RuntimeError::InvalidAppend("invalid metric counts".into()));
        }
        if [
            metric.column_size_bytes,
            metric.value_count,
            metric.null_count,
            metric.nan_count,
            metric.distinct_count,
        ]
        .into_iter()
        .flatten()
        .any(|value| value > i64::MAX as u64)
        {
            return Err(RuntimeError::InvalidAppend(
                "metric count exceeds SQLite INTEGER".into(),
            ));
        }
        for bound in [&metric.lower_bound, &metric.upper_bound]
            .into_iter()
            .flatten()
        {
            bound.validate()?;
            if !field_type.accepts(bound)
                || matches!(bound, TypedScalar::Null)
                || scalar_is_nan(bound)
            {
                return Err(RuntimeError::InvalidAppend("invalid metric bound".into()));
            }
        }
        if let (Some(lower), Some(upper)) = (&metric.lower_bound, &metric.upper_bound)
            && lower
                .partial_cmp_same_type(upper)
                .is_some_and(std::cmp::Ordering::is_gt)
        {
            return Err(RuntimeError::InvalidAppend(
                "metric bounds are reversed".into(),
            ));
        }
    }
    Ok(())
}

fn validate_staged(
    request: &AppendRequest,
    staged: &[VerifiedStagedFile],
    table_id: Id,
) -> Result<(), RuntimeError> {
    if request.files.len() != staged.len() {
        return Err(RuntimeError::StagingMismatch("entry count differs".into()));
    }
    let mut ids = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for (ordinal, (logical, verified)) in request.files.iter().zip(staged).enumerate() {
        if verified.table_id != table_id
            || verified.ordinal != ordinal
            || verified.sha256 != logical.fingerprint.sha256
            || verified.length != logical.fingerprint.length
            || verified.format != logical.format
            || verified.object_identity.is_some()
            || !ids.insert(verified.file_id)
            || !uris.insert(verified.uri.clone())
        {
            return Err(RuntimeError::StagingMismatch(format!(
                "entry {ordinal} does not match"
            )));
        }
    }
    Ok(())
}

fn derived_summary(
    request: &AppendRequest,
) -> Result<BTreeMap<String, CanonicalValue>, RuntimeError> {
    let mut summary = request.summary.clone();
    let record_count = request.files.iter().try_fold(0_u64, |total, file| {
        total
            .checked_add(file.record_count)
            .ok_or_else(|| RuntimeError::InvalidAppend("record count overflow".into()))
    })?;
    let file_size = request.files.iter().try_fold(0_u64, |total, file| {
        total
            .checked_add(file.fingerprint.length)
            .ok_or_else(|| RuntimeError::InvalidAppend("file size overflow".into()))
    })?;
    if record_count > i64::MAX as u64 || file_size > i64::MAX as u64 {
        return Err(RuntimeError::InvalidAppend(
            "summary exceeds SQLite INTEGER".into(),
        ));
    }
    summary.insert(
        "added-data-files".into(),
        string(&request.files.len().to_string()),
    );
    summary.insert("added-records".into(), string(&record_count.to_string()));
    summary.insert("added-files-size".into(), string(&file_size.to_string()));
    Ok(summary)
}

fn check_idempotency(
    pinned: &PinnedTable,
    key: &str,
    intent: Sha256,
) -> Result<Option<AppendResult>, RuntimeError> {
    let Some((stored_hash, result)) = image::idempotency(&pinned.image.path, key)? else {
        return Ok(None);
    };
    if stored_hash != intent {
        return Err(RuntimeError::IdempotencyConflict);
    }
    let result = canonical_json::from_slice_canonical(result.as_bytes())?;
    Ok(Some(result))
}

async fn cleanup<S: ObjectStore>(store: &S, staged: &[VerifiedStagedFile]) {
    for file in staged {
        if let Err(error) = store.delete_if_version(&file.uri, &file.version).await {
            tracing::warn!(%error, uri=%file.uri, "best-effort staging cleanup failed");
        }
    }
}

async fn put_immutable<S: ObjectStore>(
    store: &S,
    key: &RelativeUri,
    bytes: &[u8],
) -> Result<(), RuntimeError> {
    match store.create_bytes(key, bytes).await {
        Ok(created) => {
            store
                .confirm_readable(key, created.sha256, created.length)
                .await?;
            Ok(())
        }
        Err(StorageError::ImmutableConflict(_)) => {
            store
                .confirm_readable(key, Sha256::digest(bytes), bytes.len() as u64)
                .await?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

async fn verified_read<S: ObjectStore>(
    store: &S,
    reference: &ObjectReference,
) -> Result<StoredObject, RuntimeError> {
    let object = store.read(&reference.uri).await?;
    if Sha256::digest(&object.bytes) != reference.sha256
        || reference
            .length
            .is_some_and(|length| length.0 != object.bytes.len() as u64)
    {
        return Err(RuntimeError::Corrupt(format!(
            "object hash or length mismatch: {}",
            reference.uri
        )));
    }
    Ok(object)
}

fn commit_body(commit: &SemanticCommit) -> Result<Vec<u8>, RuntimeError> {
    let mut value = canonical_json::to_value(commit)?;
    let CanonicalValue::Object(ref mut fields) = value else {
        return Err(RuntimeError::Corrupt(
            "commit did not encode as an object".into(),
        ));
    };
    fields.remove("semantic_state_sha256");
    Ok(canonical_json::encode(&value)?)
}

fn runtime_features() -> Result<FeatureSet, ProtocolError> {
    FeatureSet::new(vec![
        CORE_FEATURE.into(),
        PARQUET_FEATURE.into(),
        SQLITE_COW_FEATURE.into(),
        "otmp.refs.v1".into(),
    ])
}

fn object_reference(
    uri: RelativeUri,
    sha256: Sha256,
    length: u64,
    media_type: &str,
) -> ObjectReference {
    ObjectReference {
        uri,
        sha256,
        length: Some(JsonU64(length)),
        media_type: Some(media_type.into()),
    }
}

fn object<const N: usize>(entries: [(&str, CanonicalValue); N]) -> CanonicalValue {
    CanonicalValue::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    )
}

fn string(value: &str) -> CanonicalValue {
    CanonicalValue::String(value.to_owned())
}

fn canonical_text<T: Serialize>(value: &T) -> Result<String, RuntimeError> {
    String::from_utf8(canonical_json::to_vec(value)?)
        .map_err(|error| RuntimeError::Corrupt(error.to_string()))
}

fn new_id() -> Id {
    Id::from_bytes(*Uuid::now_v7().as_bytes())
}

fn now_ms() -> Result<i64, RuntimeError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| RuntimeError::InvalidAppend(error.to_string()))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| RuntimeError::InvalidAppend("timestamp overflow".into()))
}

fn head_key() -> Result<RelativeUri, ProtocolError> {
    HEAD_KEY.parse()
}

fn query_optional_id(path: &std::path::Path, query: &str) -> Result<Option<Id>, RuntimeError> {
    let connection = image::open_readonly(path)?;
    let blob: Option<Vec<u8>> = connection.query_row(query, [], |row| row.get(0))?;
    blob.map(id_from_blob).transpose()
}

fn id_from_blob(blob: Vec<u8>) -> Result<Id, RuntimeError> {
    let bytes: [u8; 16] = blob
        .try_into()
        .map_err(|_| RuntimeError::Corrupt("invalid ID blob".into()))?;
    Id::try_from_bytes(bytes).map_err(Into::into)
}

fn hash_from_blob(blob: Vec<u8>) -> Result<Sha256, RuntimeError> {
    let bytes: [u8; 32] = blob
        .try_into()
        .map_err(|_| RuntimeError::Corrupt("invalid SHA-256 blob".into()))?;
    Ok(Sha256::from_bytes(bytes))
}

fn nonnegative(value: i64, name: &str) -> Result<u64, RuntimeError> {
    u64::try_from(value).map_err(|_| RuntimeError::Corrupt(format!("negative {name}")))
}

fn scalar_is_nan(value: &TypedScalar) -> bool {
    matches!(value, TypedScalar::Float32(number) if number.is_nan())
        || matches!(value, TypedScalar::Float64(number) if number.is_nan())
}

fn failpoint(name: &str) {
    if std::env::var("OTMP_FAILPOINT").as_deref() == Ok(name) {
        std::process::exit(86);
    }
}

mod u64_string {
    use serde::{Deserialize, Deserializer, Serializer};

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.is_empty()
            || value.starts_with('+')
            || (value.starts_with('0') && value.len() > 1)
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(serde::de::Error::custom("noncanonical u64 decimal string"));
        }
        value.parse().map_err(serde::de::Error::custom)
    }
}

fn replay<R: serde::de::DeserializeOwned>(
    pinned: &PinnedTable,
    key: &str,
    intent: Sha256,
) -> Result<Option<(R, Sha256)>, RuntimeError> {
    let Some((stored_hash, result)) = image::idempotency(&pinned.image.path, key)? else {
        return Ok(None);
    };
    if stored_hash != intent {
        return Err(RuntimeError::IdempotencyConflict);
    }
    let connection = image::open_readonly(&pinned.image.path)?;
    let hash = connection.query_row("SELECT c.semantic_state_sha256 FROM otmp_commits c JOIN otmp_idempotency i ON i.commit_id=c.commit_id AND i.table_version=c.table_version WHERE i.idempotency_key=?1", [key], |r| r.get(0))?;
    Ok(Some((
        canonical_json::from_slice_canonical(result.as_bytes())?,
        hash_from_blob(hash)?,
    )))
}
