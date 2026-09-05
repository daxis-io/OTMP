use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use otmp::{
    AppendFile, AppendRequest, AppendResult, CommitMetadata, ConditionalWriteOutcome, FileFormat,
    InMemoryObjectStore, InitializeRequest, InjectedConditional, ObjectStore, ObjectVersion,
    SnapshotMetadata, SourceFingerprint, StorageError, Table, TransactionRetryPolicy,
};
use otmp_protocol::{
    CanonicalValue, Field, Generation, Head, LogicalType, RelativeUri, Schema, SemanticCommit,
    Sha256, canonical_json,
};
use tokio::io::AsyncRead;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        parent_schema_id: None,
        fields: vec![Field {
            field_id: 1,
            name: "id".into(),
            required: true,
            field_type: LogicalType::Int64,
            doc: None,
            initial_default: None,
            write_default: None,
        }],
        identifier_field_ids: vec![1],
        doc: None,
    }
}

fn request(path: std::path::PathBuf, bytes: &[u8], key: &str) -> AppendRequest {
    AppendRequest::new(
        key,
        vec![AppendFile {
            source_path: path,
            fingerprint: SourceFingerprint {
                sha256: Sha256::digest(bytes),
                length: bytes.len() as u64,
            },
            format: FileFormat::Parquet,
            record_count: 1,
            schema_id: 1,
            partition_spec_id: 0,
            sort_order_id: 0,
            partition_values: BTreeMap::new(),
            metrics: Vec::new(),
            metadata: BTreeMap::new(),
        }],
    )
}

fn metadata(namespace: &str, value: &str) -> BTreeMap<String, CanonicalValue> {
    BTreeMap::from([(namespace.into(), CanonicalValue::String(value.into()))])
}

async fn setup() -> (
    tempfile::TempDir,
    InMemoryObjectStore,
    Table<InMemoryObjectStore>,
) {
    let directory = tempfile::tempdir().unwrap();
    let store = InMemoryObjectStore::default();
    let table = Table::new(store.clone());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    (directory, store, table)
}

#[tokio::test]
async fn applied_but_response_lost_is_reconciled_by_idempotency() {
    let (directory, store, table) = setup().await;
    let bytes = b"a";
    let path = directory.path().join("a.parquet");
    tokio::fs::write(&path, bytes).await.unwrap();
    store.inject_conditional(InjectedConditional::IndeterminateAfter);

    let result = table
        .append_files(&request(path, bytes, "lost"))
        .await
        .unwrap();

    assert_eq!(result.table_version, 1);
    assert_eq!(table.pin().await.unwrap().history().unwrap().len(), 2);
}

#[tokio::test]
async fn definite_conflict_rebases_without_changing_staged_identity() {
    let (directory, store, table) = setup().await;
    let bytes = b"a";
    let path = directory.path().join("a.parquet");
    tokio::fs::write(&path, bytes).await.unwrap();
    let request = request(path, bytes, "rebase");
    let table_id = table.pin().await.unwrap().status().table_id;
    let staged = table
        .stage_file(table_id, 0, &request.files[0])
        .await
        .unwrap();
    let file_id = staged.file_id();
    let uri = staged.uri().clone();
    store.inject_conditional(InjectedConditional::Conflict);

    let result = table
        .commit_staged_files(&request, &[staged])
        .await
        .unwrap();

    assert_eq!(result.files[0].file_id, file_id);
    assert_eq!(result.files[0].uri, uri);
    assert_eq!(result.table_version, 1);
}

#[tokio::test]
async fn append_safe_rebase_preserves_commit_and_snapshot_metadata() {
    let (directory, store, table) = setup().await;
    let bytes = b"metadata";
    let path = directory.path().join("metadata.parquet");
    tokio::fs::write(&path, bytes).await.unwrap();
    let mut request = request(path, bytes, "metadata-rebase");
    let commit_metadata = BTreeMap::from([(
        "com.example.catalog".into(),
        CanonicalValue::String("transaction-1".into()),
    )]);
    let snapshot_metadata = BTreeMap::from([(
        "com.example.pipeline".into(),
        CanonicalValue::String("watermark-1".into()),
    )]);
    request.commit_metadata = CommitMetadata::from(commit_metadata.clone());
    request.snapshot_metadata = SnapshotMetadata::from(snapshot_metadata.clone());
    store.inject_conditional(InjectedConditional::Conflict);

    table.append_files(&request).await.unwrap();

    let head: Head = canonical_json::from_slice_canonical(
        &store
            .read(&"_otmp/HEAD".parse().unwrap())
            .await
            .unwrap()
            .bytes,
    )
    .unwrap();
    let commit: SemanticCommit = canonical_json::from_slice_canonical(
        &store.read(&head.semantic_commit.uri).await.unwrap().bytes,
    )
    .unwrap();
    assert_eq!(commit.metadata, CanonicalValue::Object(commit_metadata));
    let CanonicalValue::Object(operation) = &commit.operations[0] else {
        panic!("commit_snapshot operation must be an object");
    };
    let Some(CanonicalValue::Object(snapshot)) = operation.get("snapshot") else {
        panic!("commit_snapshot snapshot must be an object");
    };
    assert_eq!(
        snapshot.get("metadata"),
        Some(&CanonicalValue::Object(snapshot_metadata))
    );
}

#[tokio::test]
async fn retry_exhaustion_retains_caller_owned_verified_staging() {
    let (directory, store, table) = setup().await;
    let bytes = b"a";
    let path = directory.path().join("a.parquet");
    tokio::fs::write(&path, bytes).await.unwrap();
    let request = request(path, bytes, "exhaust");
    let table_id = table.pin().await.unwrap().status().table_id;
    let staged = table
        .stage_file(table_id, 0, &request.files[0])
        .await
        .unwrap();
    let uri = staged.uri().clone();
    store.inject_conditional(InjectedConditional::Conflict);
    let table = table.with_retry_policy(TransactionRetryPolicy {
        maximum_rebases: 0,
        maximum_indeterminate_reconciliations: 3,
    });

    let error = table
        .commit_staged_files(&request, &[staged])
        .await
        .unwrap_err();

    assert_eq!(error.code(), "OTMP_REBASE_EXHAUSTED");
    assert_eq!(store.read(&uri).await.unwrap().bytes, bytes);
}

#[tokio::test]
async fn indeterminate_reconciliation_limit_returns_retryable_error() {
    let (directory, store, table) = setup().await;
    let bytes = b"a";
    let path = directory.path().join("a.parquet");
    tokio::fs::write(&path, bytes).await.unwrap();
    for _ in 0..3 {
        store.inject_conditional(InjectedConditional::IndeterminateBefore);
    }
    let table = table.with_retry_policy(TransactionRetryPolicy {
        maximum_rebases: 8,
        maximum_indeterminate_reconciliations: 2,
    });

    let error = table
        .append_files(&request(path, bytes, "indeterminate"))
        .await
        .unwrap_err();

    assert_eq!(error.code(), "OTMP_PUBLICATION_INDETERMINATE");
    assert!(error.retryable());
    assert_eq!(table.pin().await.unwrap().status().table_version, 0);
}

#[tokio::test]
async fn staged_mutation_is_detected_before_head_publication() {
    let (directory, store, table) = setup().await;
    let bytes = b"a";
    let path = directory.path().join("a.parquet");
    tokio::fs::write(&path, bytes).await.unwrap();
    let request = request(path, bytes, "mutated");
    let table_id = table.pin().await.unwrap().status().table_id;
    let staged = table
        .stage_file(table_id, 0, &request.files[0])
        .await
        .unwrap();
    store.replace_object_for_test(staged.uri(), b"b".to_vec());

    let error = table
        .commit_staged_files(&request, &[staged])
        .await
        .unwrap_err();

    assert!(matches!(
        error.code(),
        "OTMP_STORAGE_ERROR" | "OTMP_FINGERPRINT_MISMATCH"
    ));
    assert_eq!(table.pin().await.unwrap().status().table_version, 0);
}

#[tokio::test]
async fn normal_reads_do_not_list_or_replay_commits() {
    let (_directory, store, table) = setup().await;
    let pinned = table.pin().await.unwrap();
    let _ = pinned.status();
    pinned.files("main").unwrap();
    pinned.history().unwrap();
    assert_eq!(store.listing_count(), 0);
}

#[derive(Clone)]
struct FailingArtifactStore {
    inner: InMemoryObjectStore,
    enabled: Arc<AtomicBool>,
    artifact_calls: Arc<AtomicUsize>,
    fail_on: usize,
}

impl FailingArtifactStore {
    fn new(fail_on: usize) -> Self {
        Self {
            inner: InMemoryObjectStore::default(),
            enabled: Arc::new(AtomicBool::new(false)),
            artifact_calls: Arc::new(AtomicUsize::new(0)),
            fail_on,
        }
    }
}

#[async_trait]
impl ObjectStore for FailingArtifactStore {
    async fn read(&self, key: &RelativeUri) -> Result<otmp::storage::StoredObject, StorageError> {
        self.inner.read(key).await
    }

    async fn create_from_reader(
        &self,
        key: &RelativeUri,
        reader: &mut (dyn AsyncRead + Send + Unpin),
        maximum_length: Option<u64>,
    ) -> Result<otmp::storage::CreatedObject, StorageError> {
        if self.enabled.load(Ordering::SeqCst) && key.as_str().starts_with("_otmp/") {
            let call = self.artifact_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.fail_on {
                return Err(StorageError::Injected(format!(
                    "immutable artifact upload {call}"
                )));
            }
        }
        self.inner
            .create_from_reader(key, reader, maximum_length)
            .await
    }

    async fn create_head(&self, bytes: &[u8]) -> ConditionalWriteOutcome {
        self.inner.create_head(bytes).await
    }

    async fn replace_head(
        &self,
        expected: &ObjectVersion,
        bytes: &[u8],
    ) -> ConditionalWriteOutcome {
        self.inner.replace_head(expected, bytes).await
    }

    async fn delete_if_version(
        &self,
        key: &RelativeUri,
        version: &ObjectVersion,
    ) -> Result<bool, StorageError> {
        self.inner.delete_if_version(key, version).await
    }
}

#[tokio::test]
async fn partial_immutable_artifact_uploads_remain_invisible() {
    for fail_on in 1..=3 {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.parquet");
        tokio::fs::write(&path, b"a").await.unwrap();
        let store = FailingArtifactStore::new(fail_on);
        let table = Table::new(store.clone());
        table
            .initialize(InitializeRequest::new(schema()))
            .await
            .unwrap();
        store.enabled.store(true, Ordering::SeqCst);

        let error = table
            .append_files(&request(path, b"a", &format!("partial-{fail_on}")))
            .await
            .unwrap_err();

        assert_eq!(error.code(), "OTMP_STORAGE_ERROR");
        assert_eq!(table.pin().await.unwrap().status().table_version, 0);
        table.verify().await.unwrap();
    }
}

#[derive(Clone)]
struct TwoWriterStore {
    inner: InMemoryObjectStore,
    barrier: Arc<tokio::sync::Barrier>,
    enabled: Arc<AtomicBool>,
    replace_calls: Arc<AtomicUsize>,
}

impl TwoWriterStore {
    fn new() -> Self {
        Self {
            inner: InMemoryObjectStore::default(),
            barrier: Arc::new(tokio::sync::Barrier::new(2)),
            enabled: Arc::new(AtomicBool::new(false)),
            replace_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl ObjectStore for TwoWriterStore {
    async fn read(&self, key: &RelativeUri) -> Result<otmp::storage::StoredObject, StorageError> {
        self.inner.read(key).await
    }

    async fn create_from_reader(
        &self,
        key: &RelativeUri,
        reader: &mut (dyn AsyncRead + Send + Unpin),
        maximum_length: Option<u64>,
    ) -> Result<otmp::storage::CreatedObject, StorageError> {
        self.inner
            .create_from_reader(key, reader, maximum_length)
            .await
    }

    async fn create_head(&self, bytes: &[u8]) -> ConditionalWriteOutcome {
        self.inner.create_head(bytes).await
    }

    async fn replace_head(
        &self,
        expected: &ObjectVersion,
        bytes: &[u8],
    ) -> ConditionalWriteOutcome {
        let call = self.replace_calls.fetch_add(1, Ordering::SeqCst);
        if self.enabled.load(Ordering::SeqCst) && call < 2 {
            self.barrier.wait().await;
        }
        self.inner.replace_head(expected, bytes).await
    }

    async fn delete_if_version(
        &self,
        key: &RelativeUri,
        version: &ObjectVersion,
    ) -> Result<bool, StorageError> {
        self.inner.delete_if_version(key, version).await
    }
}

async fn assert_rebased_metadata(
    store: &TwoWriterStore,
    checkpoint_path: &std::path::Path,
    winner: &AppendResult,
    rebased: &AppendResult,
    expected_commit_metadata: BTreeMap<String, CanonicalValue>,
    expected_snapshot_metadata: BTreeMap<String, CanonicalValue>,
) {
    assert_eq!(rebased.table_version, 2);
    let head: Head = canonical_json::from_slice_canonical(
        &store
            .read(&"_otmp/HEAD".parse().unwrap())
            .await
            .unwrap()
            .bytes,
    )
    .unwrap();
    let commit: SemanticCommit = canonical_json::from_slice_canonical(
        &store.read(&head.semantic_commit.uri).await.unwrap().bytes,
    )
    .unwrap();
    assert_eq!(
        commit.metadata,
        CanonicalValue::Object(expected_commit_metadata)
    );
    let CanonicalValue::Object(operation) = &commit.operations[0] else {
        panic!("commit_snapshot operation must be an object");
    };
    let Some(CanonicalValue::Object(snapshot)) = operation.get("snapshot") else {
        panic!("commit_snapshot snapshot must be an object");
    };
    assert_eq!(
        snapshot.get("parent_snapshot_id"),
        Some(&CanonicalValue::String(winner.snapshot_id.to_string()))
    );
    assert_eq!(
        snapshot.get("metadata"),
        Some(&CanonicalValue::Object(expected_snapshot_metadata))
    );
    let generation: Generation = canonical_json::from_slice_canonical(
        &store
            .read(&head.metadata_generation.uri)
            .await
            .unwrap()
            .bytes,
    )
    .unwrap();
    let checkpoint = store
        .read(&generation.metadata_image.checkpoint.uri)
        .await
        .unwrap();
    tokio::fs::write(checkpoint_path, checkpoint.bytes)
        .await
        .unwrap();
    let connection = rusqlite::Connection::open(checkpoint_path).unwrap();
    let (stored_version, stored_snapshot_metadata): (i64, String) = connection
        .query_row(
            "SELECT committed_table_version, metadata_json FROM otmp_snapshots WHERE snapshot_id=?1",
            [rebased.snapshot_id.as_bytes().as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(stored_version, 2);
    assert_eq!(
        stored_snapshot_metadata,
        String::from_utf8(canonical_json::to_vec(snapshot.get("metadata").unwrap()).unwrap())
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_appends_from_one_parent_rebase_and_both_become_live() {
    let directory = tempfile::tempdir().unwrap();
    let store = TwoWriterStore::new();
    let table = Table::new(store.clone());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let a = directory.path().join("a.parquet");
    let b = directory.path().join("b.parquet");
    tokio::fs::write(&a, b"a").await.unwrap();
    tokio::fs::write(&b, b"b").await.unwrap();
    let commit_metadata_a = metadata("com.example.catalog", "transaction-a");
    let snapshot_metadata_a = metadata("com.example.pipeline", "watermark-a");
    let commit_metadata_b = metadata("com.example.catalog", "transaction-b");
    let snapshot_metadata_b = metadata("com.example.pipeline", "watermark-b");
    let mut request_a = request(a, b"a", "a");
    request_a.commit_metadata = CommitMetadata::from(commit_metadata_a.clone());
    request_a.snapshot_metadata = SnapshotMetadata::from(snapshot_metadata_a.clone());
    let mut request_b = request(b, b"b", "b");
    request_b.commit_metadata = CommitMetadata::from(commit_metadata_b.clone());
    request_b.snapshot_metadata = SnapshotMetadata::from(snapshot_metadata_b.clone());
    let table_id = table.pin().await.unwrap().status().table_id;
    let staged_a = table
        .stage_file(table_id, 0, &request_a.files[0])
        .await
        .unwrap();
    let staged_b = table
        .stage_file(table_id, 0, &request_b.files[0])
        .await
        .unwrap();
    let staged_identities = [
        (staged_a.file_id(), staged_a.uri().clone()),
        (staged_b.file_id(), staged_b.uri().clone()),
    ];
    let staged_a = [staged_a];
    let staged_b = [staged_b];
    store.enabled.store(true, Ordering::SeqCst);

    let (left, right) = tokio::join!(
        table.commit_staged_files(&request_a, &staged_a),
        table.commit_staged_files(&request_b, &staged_b),
    );
    let left = left.unwrap();
    let right = right.unwrap();

    assert_ne!(left.snapshot_id, right.snapshot_id);
    assert_eq!(
        (left.files[0].file_id, left.files[0].uri.clone()),
        staged_identities[0]
    );
    assert_eq!(
        (right.files[0].file_id, right.files[0].uri.clone()),
        staged_identities[1]
    );
    assert_eq!(table.pin().await.unwrap().status().table_version, 2);
    assert_eq!(table.pin().await.unwrap().files("main").unwrap().len(), 2);

    let (winner, rebased, expected_commit_metadata, expected_snapshot_metadata) =
        if left.table_version == 1 {
            (&left, &right, commit_metadata_b, snapshot_metadata_b)
        } else {
            (&right, &left, commit_metadata_a, snapshot_metadata_a)
        };
    let checkpoint_path = directory.path().join("rebased.sqlite3");
    assert_rebased_metadata(
        &store,
        &checkpoint_path,
        winner,
        rebased,
        expected_commit_metadata,
        expected_snapshot_metadata,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_identical_idempotent_attempts_return_one_stable_result() {
    let directory = tempfile::tempdir().unwrap();
    let store = TwoWriterStore::new();
    let table = Table::new(store.clone());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let a = directory.path().join("a.parquet");
    let b = directory.path().join("b.parquet");
    tokio::fs::write(&a, b"same").await.unwrap();
    tokio::fs::write(&b, b"same").await.unwrap();
    store.enabled.store(true, Ordering::SeqCst);
    let request_a = request(a, b"same", "same-key");
    let request_b = request(b, b"same", "same-key");

    let (left, right) = tokio::join!(
        table.append_files(&request_a),
        table.append_files(&request_b)
    );
    let left = left.unwrap();
    let right = right.unwrap();

    assert_eq!(left, right);
    let pinned = table.pin().await.unwrap();
    assert_eq!(pinned.status().table_version, 1);
    assert_eq!(pinned.files("main").unwrap().len(), 1);
    assert_eq!(pinned.history().unwrap().len(), 2);
}

#[derive(Clone)]
struct PauseAfterDataCreateStore {
    inner: InMemoryObjectStore,
    pause: Arc<AtomicBool>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    paused_keys: Arc<std::sync::Mutex<Vec<RelativeUri>>>,
}

impl PauseAfterDataCreateStore {
    fn new() -> Self {
        Self {
            inner: InMemoryObjectStore::default(),
            pause: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            paused_keys: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl ObjectStore for PauseAfterDataCreateStore {
    async fn read(&self, key: &RelativeUri) -> Result<otmp::storage::StoredObject, StorageError> {
        self.inner.read(key).await
    }

    async fn create_from_reader(
        &self,
        key: &RelativeUri,
        reader: &mut (dyn AsyncRead + Send + Unpin),
        maximum_length: Option<u64>,
    ) -> Result<otmp::storage::CreatedObject, StorageError> {
        let created = self
            .inner
            .create_from_reader(key, reader, maximum_length)
            .await?;
        if self.pause.load(Ordering::SeqCst) && key.as_str().starts_with("data/") {
            self.paused_keys
                .lock()
                .expect("paused key lock poisoned")
                .push(key.clone());
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(created)
    }

    async fn create_head(&self, bytes: &[u8]) -> ConditionalWriteOutcome {
        self.inner.create_head(bytes).await
    }

    async fn replace_head(
        &self,
        expected: &ObjectVersion,
        bytes: &[u8],
    ) -> ConditionalWriteOutcome {
        self.inner.replace_head(expected, bytes).await
    }

    async fn delete_if_version(
        &self,
        key: &RelativeUri,
        version: &ObjectVersion,
    ) -> Result<bool, StorageError> {
        self.inner.delete_if_version(key, version).await
    }
}

#[tokio::test]
async fn second_idempotency_check_cleans_high_level_duplicate_staging() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("same.parquet");
    tokio::fs::write(&source, b"same").await.unwrap();
    let store = PauseAfterDataCreateStore::new();
    let table = Table::new(store.clone());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let request = request(source, b"same", "second-check");
    let table_id = table.pin().await.unwrap().status().table_id;
    let winning_staged = table
        .stage_file(table_id, 0, &request.files[0])
        .await
        .unwrap();
    store.pause.store(true, Ordering::SeqCst);

    let high_level = table.append_files(&request);
    let winner = async {
        store.entered.notified().await;
        let result = table.commit_staged_files(&request, &[winning_staged]).await;
        store.release.notify_one();
        result
    };
    let (duplicate_result, winning_result) = tokio::join!(high_level, winner);

    assert_eq!(duplicate_result.unwrap(), winning_result.unwrap());
    let paused_key = store.paused_keys.lock().expect("paused key lock poisoned")[0].clone();
    assert!(matches!(
        store.inner.read(&paused_key).await,
        Err(StorageError::NotFound(_))
    ));
}

fn property_transaction(key: &str, property: &str) -> otmp::TransactionRequest {
    otmp::TransactionRequest {
        idempotency_key: key.into(),
        requirements: vec![otmp::Requirement::PropertyIs {
            key: property.into(),
            value: CanonicalValue::Null,
        }],
        operations: vec![otmp::OperationRequest::SetProperties {
            operation_id: "set".into(),
            updates: [(property.into(), CanonicalValue::Bool(true))].into(),
            removals: vec![],
        }],
        commit_metadata: CommitMetadata::default(),
    }
}
#[tokio::test]
async fn unrelated_properties_rebase_but_stale_touched_property_conflicts() {
    for same_property in [false, true] {
        let store = TwoWriterStore::new();
        let table = Table::new(store.clone());
        table
            .initialize(InitializeRequest::new(schema()))
            .await
            .unwrap();
        store.enabled.store(true, Ordering::SeqCst);
        let a = property_transaction("a", "owner");
        let b = property_transaction(
            "b",
            if same_property {
                "owner"
            } else {
                "description"
            },
        );
        let (a, b) = tokio::join!(table.transact(&a), table.transact(&b));
        if same_property {
            assert_eq!(u8::from(a.is_ok()) + u8::from(b.is_ok()), 1);
            let error = a.err().or_else(|| b.err()).unwrap();
            assert_eq!(error.code(), "OTMP_SEMANTIC_CONFLICT");
        } else {
            a.unwrap();
            b.unwrap();
            assert_eq!(table.pin().await.unwrap().status().table_version, 2);
        }
        table.verify().await.unwrap();
    }
}
#[tokio::test]
async fn metadata_concurrent_idempotency_returns_the_committed_hash() {
    let store = TwoWriterStore::new();
    let table = Table::new(store.clone());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    store.enabled.store(true, Ordering::SeqCst);
    let request = property_transaction("same", "owner");
    let (a, b) = tokio::join!(table.transact(&request), table.transact(&request));
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(table.pin().await.unwrap().status().table_version, 1);
}
#[tokio::test]
async fn append_rebases_across_unrelated_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data");
    tokio::fs::write(&path, b"a").await.unwrap();
    let store = TwoWriterStore::new();
    let table = Table::new(store.clone());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    store.enabled.store(true, Ordering::SeqCst);
    let append = request(path, b"a", "append");
    let property = property_transaction("property", "owner");
    let (a, b) = tokio::join!(table.append_files(&append), table.transact(&property));
    a.unwrap();
    b.unwrap();
    assert_eq!(table.pin().await.unwrap().status().table_version, 2);
    assert_eq!(table.pin().await.unwrap().files("main").unwrap().len(), 1);
    table.verify().await.unwrap();
}
