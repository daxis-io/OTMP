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
    COMMIT_MEDIA_TYPE, CORE_FEATURE, CanonicalValue, FeatureSet, GENERATION_MEDIA_TYPE, Generation,
    Head, Id, IntentRecord, JsonI64, JsonU64, LogicalType, MetadataImage, ObjectReference,
    PARQUET_FEATURE, ProtocolError, RelativeUri, SQLITE_COW_FEATURE, Schema, SemanticCommit,
    Sha256, TypedScalar, canonical_json, encode_partition_tuple, encode_typed_scalar,
    genesis_state_hash, image_root_hash, intent_hash, next_state_hash, object_hash, partition_hash,
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
    _checkpoint: std::sync::Arc<StoredObject>,
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

    #[doc(hidden)]
    #[cfg(feature = "write-latency-qualification")]
    pub fn qualification_property(
        &self,
        key: &str,
    ) -> Result<Option<CanonicalValue>, RuntimeError> {
        use rusqlite::OptionalExtension;
        let connection = image::open_readonly(&self.image.path)?;
        let value = connection
            .query_row(
                "SELECT value_json FROM otmp_properties WHERE property_key=?1",
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value
            .map(|value| canonical_json::parse_canonical(value.as_bytes()).map_err(Into::into))
            .transpose()
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
    // Present only on the private reader used by an anchored verification run.
    metadata_cache: Option<std::sync::Arc<tokio::sync::Mutex<history::VerifiedMetadataCache>>>,
    reader_contexts: std::sync::Arc<std::sync::Mutex<Vec<crate::reader::WeakReadContext<S>>>>,
}

impl<S: ObjectStore> Table<S> {
    #[must_use]
    pub fn new(store: S) -> Self {
        Self {
            store,
            retry_policy: TransactionRetryPolicy::default(),
            metadata_cache: None,
            reader_contexts: std::sync::Arc::default(),
        }
    }

    /// Opens one immutable metadata and snapshot view using authenticated ranges.
    /// Cloned table handles share caches for matching reader options.
    pub async fn open_metadata_reader(
        &self,
        metadata: MetadataSelection,
        snapshot: SnapshotSelection,
        options: crate::ReaderOptions,
    ) -> Result<crate::MetadataReader<S>, RuntimeError> {
        let context = {
            let mut contexts = self
                .reader_contexts
                .lock()
                .map_err(|_| RuntimeError::Corrupt("reader context lock poisoned".into()))?;
            contexts.retain(|entry| entry.upgrade().is_some());
            if let Some(context) = contexts
                .iter()
                .filter_map(crate::reader::WeakReadContext::upgrade)
                .find(|context| context.options() == &options)
            {
                context
            } else {
                let context = crate::reader::ReadContext::new(self.store.clone(), options)?;
                contexts.push(context.downgrade());
                context
            }
        };
        crate::reader::MetadataReader::open(context, metadata, snapshot).await
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
        let checkpoint = image::turso_genesis(&GenesisImage {
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
        let checkpoint_reference = otmp_protocol::Checkpoint {
            table_version: JsonU64(0),
            uri: checkpoint_uri.clone(),
            sha256: checkpoint_hash,
            length: JsonU64(checkpoint.bytes.len() as u64),
        };
        let (checkpoint_page_index, checkpoint_index_artifacts) = crate::checkpoint_index::build(
            &checkpoint_reference,
            image::PAGE_SIZE,
            &checkpoint.bytes,
        )?;
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
                checkpoint: checkpoint_reference,
                page_map: None,
                checkpoint_page_index: Some(checkpoint_page_index),
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
        for artifact in &checkpoint_index_artifacts {
            put_immutable(&self.store, &artifact.uri, &artifact.bytes).await?;
        }
        put_immutable(&self.store, &generation_uri, &generation_bytes).await?;
        let mut reconciliations = 0;
        loop {
            match self.store.create_head(&head_bytes).await {
                ConditionalWriteOutcome::Applied { .. } => break,
                ConditionalWriteOutcome::Conflict { .. } => {
                    let current = self.store.read(&head_key()?).await.map_err(|error| {
                        if reconciliations > 0 {
                            RuntimeError::PublicationIndeterminate
                        } else {
                            error.into()
                        }
                    })?;
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
        Self::validate_head_features(&head)?;
        let commit_object = self.read_metadata(&head.semantic_commit).await?;
        let commit = canonical_json::from_slice_canonical(&commit_object.bytes)?;
        let generation_object = self.read_metadata(&head.metadata_generation).await?;
        let generation = canonical_json::from_slice_canonical(&generation_object.bytes)?;
        self.load_pin_objects(raw_head, head, commit, generation)
            .await
    }

    fn validate_head_features(head: &Head) -> Result<(), RuntimeError> {
        let supported = BTreeSet::from([
            CORE_FEATURE,
            PARQUET_FEATURE,
            SQLITE_COW_FEATURE,
            "otmp.refs.v1",
        ]);
        head.validate(&supported)?;
        head.required_writer_features
            .require_supported(&supported)?;
        Ok(())
    }

    // Both entry points supply content-verified, parsed objects. Keep all cross-object
    // and relational validation here so historical reads cannot bypass it.
    async fn load_pin_objects(
        &self,
        raw_head: StoredObject,
        head: Head,
        commit: SemanticCommit,
        generation: Generation,
    ) -> Result<PinnedTable, RuntimeError> {
        Self::validate_head_features(&head)?;
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
        let physical = self.resolve_generation(&generation).await?;
        let image = image::materialize(&physical.bytes)?;
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
            _checkpoint: physical.checkpoint,
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
        Box::pin(self.append_files_inner(request)).await
    }

    async fn append_files_inner(
        &self,
        request: &AppendRequest,
    ) -> Result<AppendResult, RuntimeError> {
        let logical = logical_intent(request)?;
        let logical_hash = intent_hash(&logical);
        let first_pin = self.write_pin(&request.target_ref).await?;
        if let Some((result, _)) =
            replay_write(&first_pin, &request.idempotency_key, logical_hash).await?
        {
            return Ok(result);
        }
        validate_request_for_write(request, &first_pin).await?;
        let base_tip = first_pin.reader.ref_row(&request.target_ref).await?;
        let base_version = first_pin.reader.head.table_version.0;
        let table_id = first_pin.reader.head.table_id;
        drop(first_pin);
        let mut staged = Vec::with_capacity(request.files.len());
        for (ordinal, file) in request.files.iter().enumerate() {
            match self.stage_file(table_id, ordinal, file).await {
                Ok(value) => staged.push(value),
                Err(error) => {
                    cleanup(&self.store, &staged).await;
                    return Err(error);
                }
            }
        }
        let mut publication_may_have_applied = false;
        let result = Box::pin(self.publish_append_transaction(
            request,
            &staged,
            logical_hash,
            table_id,
            base_tip,
            base_version,
            &mut publication_may_have_applied,
        ))
        .await;
        if result.is_err() && !publication_may_have_applied {
            cleanup(&self.store, &staged).await;
        }
        result
    }

    pub async fn commit_staged_files(
        &self,
        request: &AppendRequest,
        staged: &[VerifiedStagedFile],
    ) -> Result<AppendResult, RuntimeError> {
        Box::pin(self.commit_staged_files_inner(request, staged)).await
    }

    async fn commit_staged_files_inner(
        &self,
        request: &AppendRequest,
        staged: &[VerifiedStagedFile],
    ) -> Result<AppendResult, RuntimeError> {
        let logical_hash = intent_hash(&logical_intent(request)?);
        let base = self.write_pin(&request.target_ref).await?;
        let table_id = base.reader.head.table_id;
        validate_staged(request, staged, table_id)?;
        validate_request_for_write(request, &base).await?;
        let base_tip = base.reader.ref_row(&request.target_ref).await?;
        let mut publication_may_have_applied = false;
        Box::pin(self.publish_append_transaction(
            request,
            staged,
            logical_hash,
            table_id,
            base_tip,
            base.reader.head.table_version.0,
            &mut publication_may_have_applied,
        ))
        .await
    }

    async fn write_pin(&self, target_ref: &str) -> Result<WritePin<S>, RuntimeError> {
        #[cfg(feature = "write-latency-qualification")]
        let _validation = crate::write_latency_qualification::phase("parent_validation");
        let options = crate::ReaderOptions {
            checkpoint_window_bytes: crate::image::PAGE_SIZE as usize,
            maximum_record_bytes: 4 * 1024 * 1024,
            ..crate::ReaderOptions::default()
        };
        let context = crate::reader::ReadContext::new(self.store.clone(), options)?;
        let mut reader = {
            #[cfg(feature = "write-latency-qualification")]
            let _resolution = crate::write_latency_qualification::phase("generation_resolution");
            crate::MetadataReader::open(
                context,
                MetadataSelection::Current,
                SnapshotSelection::Ref(target_ref.to_owned()),
            )
            .await?
        };
        #[cfg(feature = "write-latency-qualification")]
        crate::write_latency_qualification::skipped_phase("logical_image_materialization", 2);
        let head = self.store.read(&head_key()?).await?;
        if head.bytes != reader.raw_head {
            return Err(RuntimeError::SemanticConflict(
                "HEAD changed while obtaining the write pin".into(),
            ));
        }
        reader.head_version = head.version;
        let (page_tree, page_map_reads) = self.load_page_tree(&reader.generation).await?;
        Ok(WritePin {
            reader,
            page_tree,
            page_map_reads,
        })
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn publish_append_transaction(
        &self,
        request: &AppendRequest,
        staged: &[VerifiedStagedFile],
        logical_hash: Sha256,
        table_id: Id,
        base_tip: Option<(RefType, Option<Id>)>,
        base_version: u64,
        publication_may_have_applied: &mut bool,
    ) -> Result<AppendResult, RuntimeError> {
        let mut parent = self.write_pin(&request.target_ref).await?;
        let mut rebases = 0;
        loop {
            if parent.reader.head.table_id != table_id {
                return Err(RuntimeError::SemanticConflict(
                    "table identity changed during publication".into(),
                ));
            }
            if let Some((result, _)) =
                replay_write(&parent, &request.idempotency_key, logical_hash).await?
            {
                if !staged_match_result(staged, &result) {
                    cleanup(&self.store, staged).await;
                }
                return Ok(result);
            }
            validate_append_rebase_write(&parent, &request.target_ref, base_tip, base_version)
                .await?;
            validate_request_for_write(request, &parent).await?;
            let request_for_build = request.clone();
            let staged_for_build = staged.to_vec();
            let (returned_parent, candidate) = tokio::task::spawn_blocking(move || {
                let candidate =
                    build_candidate(&request_for_build, &staged_for_build, logical_hash, &parent);
                (parent, candidate)
            })
            .await
            .map_err(|error| RuntimeError::Turso(format!("candidate worker failed: {error}")))?;
            parent = returned_parent;
            let candidate = candidate?;
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
            for artifact in &candidate.image_artifacts {
                failpoint("before_immutable_artifact_write");
                put_immutable(&self.store, &artifact.uri, &artifact.bytes).await?;
            }
            failpoint("before_generation_write");
            put_immutable(
                &self.store,
                &candidate.generation_uri,
                &candidate.generation_bytes,
            )
            .await?;
            failpoint("after_immutable_uploads");

            let mut indeterminate = 0;
            let mut reconciled_winner = None;
            loop {
                failpoint("before_head_cas");
                match self
                    .store
                    .replace_head(&parent.reader.head_version, &candidate.head_bytes)
                    .await
                {
                    ConditionalWriteOutcome::Applied { .. } => return Ok(candidate.result),
                    ConditionalWriteOutcome::Conflict { .. } => break,
                    ConditionalWriteOutcome::Indeterminate { source } => {
                        *publication_may_have_applied = true;
                        indeterminate += 1;
                        match self.write_pin(&request.target_ref).await {
                            Ok(current) => {
                                if current.reader.head.table_id != table_id {
                                    return Err(RuntimeError::SemanticConflict(
                                        "table identity changed during reconciliation".into(),
                                    ));
                                }
                                if let Some((result, _)) =
                                    replay_write(&current, &request.idempotency_key, logical_hash)
                                        .await?
                                {
                                    if !staged_match_result(staged, &result) {
                                        cleanup(&self.store, staged).await;
                                    }
                                    return Ok(result);
                                }
                                if current.reader.head_version == parent.reader.head_version
                                    && current.reader.raw_head == parent.reader.raw_head
                                {
                                    if indeterminate
                                        <= self.retry_policy.maximum_indeterminate_reconciliations
                                    {
                                        continue;
                                    }
                                    return Err(RuntimeError::PublicationIndeterminate);
                                }
                                reconciled_winner = Some(current);
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
            let winner = match reconciled_winner {
                Some(winner) => winner,
                None => self.write_pin(&request.target_ref).await.map_err(|error| {
                    if indeterminate > 0 {
                        RuntimeError::PublicationIndeterminate
                    } else {
                        error
                    }
                })?,
            };
            if winner.reader.head.table_id != table_id {
                return Err(RuntimeError::SemanticConflict(
                    "table identity changed after conflict".into(),
                ));
            }
            if let Some((result, _)) =
                replay_write(&winner, &request.idempotency_key, logical_hash).await?
            {
                if !staged_match_result(staged, &result) {
                    cleanup(&self.store, staged).await;
                }
                return Ok(result);
            }
            rebases += 1;
            if rebases > self.retry_policy.maximum_rebases {
                return Err(RuntimeError::RebaseExhausted);
            }
            parent = winner;
        }
    }

    #[allow(clippy::too_many_lines)] // Keep conditional outcomes and reconciliation in one state machine.
    async fn publish_transaction(
        &self,
        request: TransactionRequest,
        logical_hash: Sha256,
    ) -> Result<(transactions::DurableResult, Sha256), RuntimeError> {
        #[cfg(feature = "write-latency-qualification")]
        let parent_pin_phase = crate::write_latency_qualification::phase("parent_pin");
        let parent = self.write_pin("main").await?;
        #[cfg(feature = "write-latency-qualification")]
        drop(parent_pin_phase);
        self.publish_transaction_from_parent(request, logical_hash, parent)
            .await
    }

    #[allow(clippy::too_many_lines)] // Keep conditional outcomes and reconciliation in one state machine.
    async fn publish_transaction_from_parent(
        &self,
        request: TransactionRequest,
        logical_hash: Sha256,
        mut parent: WritePin<S>,
    ) -> Result<(transactions::DurableResult, Sha256), RuntimeError> {
        let key = request.idempotency_key.clone();
        #[cfg(feature = "write-latency-qualification")]
        crate::write_latency_qualification::add_bytes(
            "parent_logical_bytes",
            parent.reader.image.length(),
        );
        let table_id = parent.reader.head.table_id;

        let mut rebases = 0;
        loop {
            if parent.reader.head.table_id != table_id {
                return Err(RuntimeError::SemanticConflict(
                    "table identity changed during publication".into(),
                ));
            }
            {
                #[cfg(feature = "write-latency-qualification")]
                let _phase = crate::write_latency_qualification::phase("idempotency");
                if let Some(result) = replay_write(&parent, &key, logical_hash).await? {
                    return Ok(result);
                }
            }
            let request_for_build = request.clone();
            let (returned_parent, candidate) = {
                #[cfg(feature = "write-latency-qualification")]
                let _phase = crate::write_latency_qualification::phase("candidate_build");
                tokio::task::spawn_blocking(move || {
                    let candidate = transactions::build_transaction_candidate(
                        &parent,
                        &request_for_build,
                        logical_hash,
                    );
                    (parent, candidate)
                })
                .await
                .map_err(|error| RuntimeError::Turso(format!("candidate worker failed: {error}")))?
            };
            parent = returned_parent;
            let candidate = candidate?;
            {
                #[cfg(feature = "write-latency-qualification")]
                let _phase = crate::write_latency_qualification::phase("immutable_publication");
                put_immutable(&self.store, &candidate.commit_uri, &candidate.commit_bytes).await?;
                for artifact in &candidate.image_artifacts {
                    failpoint("before_immutable_artifact_write");
                    put_immutable(&self.store, &artifact.uri, &artifact.bytes).await?;
                }
                failpoint("before_generation_write");
                put_immutable(
                    &self.store,
                    &candidate.generation_uri,
                    &candidate.generation_bytes,
                )
                .await?;
                failpoint("after_immutable_uploads");
            }

            let mut indeterminate = 0;
            loop {
                failpoint("before_head_cas");
                let outcome = {
                    #[cfg(feature = "write-latency-qualification")]
                    let _phase = crate::write_latency_qualification::phase("head_cas");
                    self.store
                        .replace_head(&parent.reader.head_version, &candidate.head_bytes)
                        .await
                };
                match outcome {
                    ConditionalWriteOutcome::Applied { .. } => {
                        return Ok((candidate.result, candidate.semantic_state));
                    }
                    ConditionalWriteOutcome::Conflict { .. } => break,
                    ConditionalWriteOutcome::Indeterminate { source } => {
                        indeterminate += 1;
                        match self.write_pin("main").await {
                            Ok(current) => {
                                if current.reader.head.table_id != table_id {
                                    return Err(RuntimeError::SemanticConflict(
                                        "table identity changed during reconciliation".into(),
                                    ));
                                }
                                if let Some(result) =
                                    replay_write(&current, &key, logical_hash).await?
                                {
                                    return Ok(result);
                                }
                                if current.reader.head_version == parent.reader.head_version
                                    && current.reader.raw_head == parent.reader.raw_head
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
            // A retry conflict does not resolve an earlier response loss. If the
            // winner cannot be pinned, this attempt may still have committed.
            let winner = self.write_pin("main").await.map_err(|error| {
                if indeterminate > 0 {
                    RuntimeError::PublicationIndeterminate
                } else {
                    error
                }
            })?;
            if winner.reader.head.table_id != table_id {
                return Err(RuntimeError::SemanticConflict(
                    "table identity changed after conflict".into(),
                ));
            }
            if let Some(result) = replay_write(&winner, &key, logical_hash).await? {
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
    image_artifacts: Vec<crate::physical::Artifact>,
    generation_uri: RelativeUri,
    generation_bytes: Vec<u8>,
    head_bytes: Vec<u8>,
    result: R,
}

pub(crate) struct WritePin<S> {
    reader: crate::MetadataReader<S>,
    page_tree: Option<std::sync::Arc<crate::physical::Tree>>,
    page_map_reads: crate::physical::PageMapReadStatistics,
}

fn trace_writer_reads(
    before: crate::reader::WriterReadStatistics,
    after: crate::reader::WriterReadStatistics,
    pinned_page_map: crate::physical::PageMapReadStatistics,
) {
    let lazy_page_map_bytes = after.page_map_bytes.saturating_sub(before.page_map_bytes);
    let lazy_page_map_requests = after
        .page_map_requests
        .saturating_sub(before.page_map_requests);
    tracing::info!(
        target: "otmp.writer",
        parent_sqlite_bytes = after.total.bytes.saturating_sub(before.total.bytes).saturating_sub(lazy_page_map_bytes),
        parent_sqlite_requests = after.total.requests.saturating_sub(before.total.requests).saturating_sub(lazy_page_map_requests),
        parent_sqlite_pages = after.total.pages.saturating_sub(before.total.pages),
        parent_sqlite_cache_hits = after.total.cache_hits.saturating_sub(before.total.cache_hits),
        page_map_read_bytes = lazy_page_map_bytes.saturating_add(pinned_page_map.bytes),
        page_map_read_requests = lazy_page_map_requests.saturating_add(pinned_page_map.requests),
        "writer parent-read summary"
    );
}

#[allow(clippy::too_many_lines)]
fn build_candidate<S: ObjectStore>(
    request: &AppendRequest,
    staged: &[VerifiedStagedFile],
    logical_hash: Sha256,
    parent: &WritePin<S>,
) -> Result<Candidate, RuntimeError> {
    let table_version = parent
        .reader
        .head
        .table_version
        .0
        .checked_add(1)
        .ok_or_else(|| RuntimeError::InvalidAppend("table version exhausted".into()))?;
    let parent_snapshot = parent
        .reader
        .snapshot()
        .map(|snapshot| snapshot.snapshot_id);
    let sequence_number = parent
        .reader
        .last_sequence
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
        table_id: parent.reader.head.table_id,
        table_version: JsonU64(table_version),
        parent_table_version: Some(parent.reader.head.table_version),
        commit_id,
        parent_commit: Some(parent.reader.head.semantic_commit.clone()),
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
        required_reader_features_after_commit: parent.reader.head.required_reader_features.clone(),
        required_writer_features_after_commit: parent.reader.head.required_writer_features.clone(),
        previous_semantic_state_sha256: Some(parent.reader.head.semantic_state_sha256),
        semantic_state_sha256: Sha256::from_bytes([0; 32]),
        metadata: CanonicalValue::Object(request.commit_metadata.0.clone()),
    };
    commit.semantic_state_sha256 = next_state_hash(
        parent.reader.head.semantic_state_sha256,
        &commit_body(&commit)?,
    );
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
    let reads_before = parent.reader.writer_statistics();
    let checkpoint = image::turso_append_pages(
        parent.reader.image.clone(),
        tokio::runtime::Handle::try_current()
            .map_err(|error| RuntimeError::Turso(format!("Tokio runtime is required: {error}")))?,
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
        &commit,
    )?;
    let candidate = finish_candidate(
        parent,
        &commit,
        commit_uri,
        commit_bytes,
        checkpoint,
        result,
    );
    trace_writer_reads(
        reads_before,
        parent.reader.writer_statistics(),
        parent.page_map_reads,
    );
    candidate
}

#[allow(clippy::too_many_lines)]
fn finish_candidate<S: ObjectStore, R>(
    parent: &WritePin<S>,
    commit: &SemanticCommit,
    commit_uri: RelativeUri,
    commit_bytes: Vec<u8>,
    mut checkpoint: image::CheckpointImage,
    result: R,
) -> Result<Candidate<R>, RuntimeError> {
    commit.validate_runtime_profile()?;
    let table_version = commit.table_version.0;
    let commit_id = commit.commit_id;
    let created_at_ms = commit.created_at_ms.0;
    let commit_hash = object_hash(&commit_bytes);
    let parent_head = &parent.reader.head;
    let candidate_length = checkpoint
        .page_count
        .checked_mul(u64::from(image::PAGE_SIZE))
        .ok_or_else(|| RuntimeError::Corrupt("candidate image length overflow".into()))?;
    #[cfg(feature = "write-latency-qualification")]
    {
        crate::write_latency_qualification::add_bytes("candidate_logical_bytes", candidate_length);
        crate::write_latency_qualification::add_bytes(
            "changed_pages",
            checkpoint.changed_pages.len() as u64,
        );
    }
    if checkpoint.frozen.is_none() {
        #[cfg(feature = "write-latency-qualification")]
        let _phase = crate::write_latency_qualification::phase("exhaustive_validation");
        image::validate(
            &checkpoint.path,
            &ExpectedImage {
                table_id: parent_head.table_id,
                table_version,
                semantic_state: commit.semantic_state_sha256,
                commit_id,
                commit_hash,
                commit_uri: commit_uri.as_str(),
                reader_features_json: &canonical_text(&parent_head.required_reader_features)?,
                writer_features_json: &canonical_text(&parent_head.required_writer_features)?,
                previous_semantic_state: commit.previous_semantic_state_sha256,
            },
        )?;
        image::validate_commit_projection(&checkpoint.path, commit)?;
    }
    let incremental = crate::physical::persist(
        parent.page_tree.as_ref(),
        &checkpoint.changed_pages,
        checkpoint.page_count,
    )?;
    let lazy_candidate = checkpoint.frozen.is_some();
    let changed_pages = checkpoint.changed_pages.len();
    let checkpoint_fallback =
        incremental.root.is_none() || incremental.reachable_bytes >= candidate_length;
    if checkpoint_fallback && checkpoint.bytes.is_empty() {
        checkpoint.bytes = {
            #[cfg(feature = "write-latency-qualification")]
            let _phase = crate::write_latency_qualification::phase("candidate_buffer_creation");
            checkpoint
                .frozen
                .take()
                .expect("lazy candidate retains its frozen overlay")
                .materialize()?
        };
        {
            #[cfg(feature = "write-latency-qualification")]
            let _phase = crate::write_latency_qualification::phase("validation_file_write");
            std::fs::write(&checkpoint.path, &checkpoint.bytes)?;
            #[cfg(feature = "write-latency-qualification")]
            crate::write_latency_qualification::add_bytes(
                "temporary_file_bytes",
                checkpoint.bytes.len() as u64,
            );
        }
        {
            #[cfg(feature = "write-latency-qualification")]
            let _phase = crate::write_latency_qualification::phase("exhaustive_validation");
            image::validate(
                &checkpoint.path,
                &ExpectedImage {
                    table_id: parent_head.table_id,
                    table_version,
                    semantic_state: commit.semantic_state_sha256,
                    commit_id,
                    commit_hash,
                    commit_uri: commit_uri.as_str(),
                    reader_features_json: &canonical_text(&parent_head.required_reader_features)?,
                    writer_features_json: &canonical_text(&parent_head.required_writer_features)?,
                    previous_semantic_state: commit.previous_semantic_state_sha256,
                },
            )?;
            image::validate_commit_projection(&checkpoint.path, commit)?;
        }
    } else if lazy_candidate {
        #[cfg(feature = "write-latency-qualification")]
        {
            crate::write_latency_qualification::skipped_phase("candidate_buffer_creation", 1);
            crate::write_latency_qualification::skipped_phase("validation_file_write", 1);
            crate::write_latency_qualification::skipped_phase("exhaustive_validation", 1);
        }
    }
    let (base_checkpoint, page_map, checkpoint_page_index, image_artifacts) = if checkpoint_fallback
    {
        let hash = object_hash(&checkpoint.bytes);
        let uri: RelativeUri =
            format!("_otmp/checkpoints/{table_version}/{}.sqlite3", new_id()).parse()?;
        let base_checkpoint = otmp_protocol::Checkpoint {
            table_version: JsonU64(table_version),
            uri: uri.clone(),
            sha256: hash,
            length: JsonU64(checkpoint.bytes.len() as u64),
        };
        let (checkpoint_page_index, mut artifacts) =
            crate::checkpoint_index::build(&base_checkpoint, image::PAGE_SIZE, &checkpoint.bytes)?;
        artifacts.push(crate::physical::Artifact {
            uri,
            bytes: std::mem::take(&mut checkpoint.bytes),
        });
        (
            base_checkpoint,
            None,
            Some(checkpoint_page_index),
            artifacts,
        )
    } else {
        (
            parent.reader.generation.metadata_image.checkpoint.clone(),
            incremental.root,
            parent
                .reader
                .generation
                .metadata_image
                .checkpoint_page_index
                .clone(),
            incremental.artifacts,
        )
    };
    let page_map_bytes = image_artifacts
        .iter()
        .filter(|artifact| artifact.uri.as_str().starts_with("_otmp/page-maps/"))
        .map(|artifact| artifact.bytes.len())
        .sum::<usize>();
    let uploaded_artifact_bytes = image_artifacts
        .iter()
        .map(|artifact| artifact.bytes.len())
        .sum::<usize>();
    #[cfg(feature = "write-latency-qualification")]
    crate::write_latency_qualification::add_bytes(
        "published_image_artifact_bytes",
        uploaded_artifact_bytes as u64,
    );
    tracing::info!(
        target: "otmp.writer",
        changed_pages,
        page_map_bytes,
        uploaded_artifact_bytes,
        full_materializations = usize::from(lazy_candidate && checkpoint_fallback),
        temporary_full_image_bytes = if lazy_candidate && checkpoint_fallback {
            candidate_length
        } else {
            0
        },
        checkpoint_fallback,
        "writer candidate summary"
    );
    let image_root = image_root_hash(
        parent_head.table_id,
        table_version,
        image::PAGE_SIZE,
        checkpoint.page_count,
        base_checkpoint.sha256,
        page_map.as_ref().map(|root| root.sha256),
    );
    let generation_id = new_id();
    let generation_uri: RelativeUri =
        format!("_otmp/generations/{table_version}/{generation_id}.json").parse()?;
    let generation = Generation {
        kind: "otmp.metadata-generation".into(),
        format_version: 1,
        table_id: parent_head.table_id,
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
        physical_parent: Some(parent_head.metadata_generation.clone()),
        metadata_image: MetadataImage {
            codec: SQLITE_COW_FEATURE.into(),
            page_size: image::PAGE_SIZE,
            page_count: JsonU64(checkpoint.page_count),
            checkpoint: base_checkpoint,
            page_map,
            checkpoint_page_index,
            image_root_sha256: image_root,
        },
        scan_projection: None,
        metadata: BTreeMap::new(),
    };
    let generation_bytes = canonical_json::to_vec(&generation)?;
    let generation_hash = object_hash(&generation_bytes);
    let head = Head {
        protocol: "otmp".into(),
        protocol_version: "0.0.2-alpha".into(),
        table_id: parent_head.table_id,
        table_version: JsonU64(table_version),
        root_revision: JsonU64(
            parent_head
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
        required_reader_features: parent_head.required_reader_features.clone(),
        required_writer_features: parent_head.required_writer_features.clone(),
    };
    Ok(Candidate {
        semantic_state: commit.semantic_state_sha256,
        commit_uri,
        commit_bytes,
        image_artifacts,
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

async fn validate_request_for_write<S: ObjectStore>(
    request: &AppendRequest,
    pinned: &WritePin<S>,
) -> Result<(), RuntimeError> {
    if !matches!(
        pinned.reader.ref_row(&request.target_ref).await?,
        Some((RefType::Branch, _))
    ) {
        return Err(RuntimeError::InvalidAppend(
            "target must exist and be a branch".into(),
        ));
    }
    let mut field_types = BTreeMap::new();
    collect_field_types(&pinned.reader.schema().fields, &mut field_types);
    validate_request_against(request, pinned.reader.schema().schema_id, &field_types)
}

fn collect_field_types(fields: &[otmp_protocol::Field], output: &mut BTreeMap<u32, LogicalType>) {
    for field in fields {
        output.insert(field.field_id, field.field_type.clone());
        match &field.field_type {
            LogicalType::Struct { fields } => collect_field_types(fields, output),
            LogicalType::List { element } => {
                collect_field_types(std::slice::from_ref(element), output);
            }
            LogicalType::Map { key, value } => {
                collect_field_types(std::slice::from_ref(key), output);
                collect_field_types(std::slice::from_ref(value), output);
            }
            _ => {}
        }
    }
}

fn validate_request_against(
    request: &AppendRequest,
    current_schema: u32,
    field_types: &BTreeMap<u32, LogicalType>,
) -> Result<(), RuntimeError> {
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
        validate_metrics(&file.metrics, field_types)?;
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

async fn replay_write<R: serde::de::DeserializeOwned, S: ObjectStore>(
    pinned: &WritePin<S>,
    key: &str,
    intent: Sha256,
) -> Result<Option<(R, Sha256)>, RuntimeError> {
    let Some((stored_hash, result, state)) = pinned.reader.idempotency(key).await? else {
        return Ok(None);
    };
    if stored_hash != intent {
        return Err(RuntimeError::IdempotencyConflict);
    }
    Ok(Some((
        canonical_json::from_slice_canonical(result.as_bytes())?,
        state,
    )))
}

async fn validate_append_rebase_write<S: ObjectStore>(
    pinned: &WritePin<S>,
    name: &str,
    original: Option<(RefType, Option<Id>)>,
    mut version: u64,
) -> Result<(), RuntimeError> {
    let Some((RefType::Branch, old)) = original else {
        return Err(RuntimeError::SemanticConflict(
            "append target was not a branch".into(),
        ));
    };
    let Some((RefType::Branch, current)) = pinned.reader.ref_row(name).await? else {
        return Err(RuntimeError::SemanticConflict(
            "append target removed".into(),
        ));
    };
    if let Some(old) = old
        && current != Some(old)
        && !pinned.reader.snapshot_descends_from(current, old).await?
    {
        return Err(RuntimeError::SemanticConflict(
            "target is not an append descendant".into(),
        ));
    }
    let target_version = pinned.reader.coordinates().table_version;
    while version < target_version {
        let rows = pinned.reader.commit_operations_page_after(version).await?;
        if rows.is_empty() {
            return Err(RuntimeError::Corrupt(
                "commit operation history is incomplete".into(),
            ));
        }
        for (row_version, row) in rows {
            if row_version != version + 1 {
                return Err(RuntimeError::Corrupt(
                    "commit operation history is not contiguous".into(),
                ));
            }
            version = row_version;
            let operations: Vec<CanonicalValue> =
                canonical_json::from_slice_canonical(row.as_bytes())?;
            for operation in operations {
                if let CanonicalValue::Object(fields) = operation {
                    if fields.get("ref") == Some(&string(name))
                        && fields.get("type") != Some(&string("commit_snapshot"))
                    {
                        return Err(RuntimeError::SemanticConflict(
                            "target ref changed during append".into(),
                        ));
                    }
                    if fields.get("type") == Some(&string("set_current_schema")) {
                        return Err(RuntimeError::SemanticConflict(
                            "current schema changed during append".into(),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

async fn cleanup<S: ObjectStore>(store: &S, staged: &[VerifiedStagedFile]) {
    for file in staged {
        if let Err(error) = store.delete_if_version(&file.uri, &file.version).await {
            tracing::warn!(%error, uri=%file.uri, "best-effort staging cleanup failed");
        }
    }
}

fn staged_match_result(staged: &[VerifiedStagedFile], result: &AppendResult) -> bool {
    staged.len() == result.files.len()
        && staged.iter().zip(&result.files).all(|(staged, committed)| {
            staged.file_id == committed.file_id && staged.uri == committed.uri
        })
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
    #[cfg(test)]
    history::VERIFIED_HASHES.with(|counts| {
        *counts
            .borrow_mut()
            .entry(reference.uri.to_string())
            .or_default() += 1;
    });
    #[cfg(test)]
    if reference.media_type.is_none() {
        history::DATA_HASHES.with(|count| count.set(count.get() + 1));
    }
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

pub(crate) fn failpoint(name: &str) {
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
