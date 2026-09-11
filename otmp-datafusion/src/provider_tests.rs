//! Faults enter after authenticated metadata retrieval, before provider pruning and
//! reconciliation. Storage, pagination, preflight and native plan assembly stay real.
use super::*;
use datafusion::arrow::{array::Int64Array, record_batch::RecordBatch};
use datafusion::catalog::TableProvider;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{SessionContext, col, lit};
use otmp::{
    AppendFile, AppendRequest, FileFormat, FileMetric, InitializeRequest, LocalObjectStore,
    MetadataSelection, ReaderOptions, SnapshotSelection, SourceFingerprint, Table,
};
use otmp_protocol::{Sha256, TypedScalar};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

type BatchEdit = Arc<dyn Fn(&mut [otmp::ReaderFile]) + Send + Sync>;
#[derive(Default)]
pub(super) struct Hooks {
    pub batch: Option<BatchEdit>,
    pub validation: Gate,
    pub after_pin: Gate,
    pub binding: Gate,
}
type Gate = Mutex<Option<[Arc<tokio::sync::Notify>; 2]>>;
pub(super) async fn pause(hook: &Gate) {
    let gate = hook.lock().unwrap().take();
    if let Some(gate) = gate {
        gate[0].notify_one();
        gate[1].notified().await;
    }
}

async fn fixture(files: usize) -> (tempfile::TempDir, Table<LocalObjectStore>) {
    let dir = tempfile::tempdir().unwrap();
    let schema: otmp_protocol::Schema =
        serde_json::from_slice(include_bytes!("../../conformance/sources/schema.json")).unwrap();
    let arrow = crate::schema_to_arrow(&schema).unwrap();
    let batch = RecordBatch::try_new(
        arrow.clone(),
        vec![Arc::new(Int64Array::from(vec![100, 101]))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), arrow, None).unwrap();
    writer.write(&batch).unwrap();
    let bytes = writer.into_inner().unwrap();
    let source = dir.path().join("source.parquet");
    std::fs::write(&source, &bytes).unwrap();
    let table = Table::new(LocalObjectStore::new(dir.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema))
        .await
        .unwrap();
    // Multiple snapshots exercise historical continuation as well as branch seek.
    for start in (0..files).step_by(128) {
        let descriptors = (start..files.min(start + 128))
            .map(|ordinal| AppendFile {
                source_path: source.clone(),
                fingerprint: SourceFingerprint {
                    sha256: Sha256::digest(&bytes),
                    length: bytes.len() as u64,
                },
                format: FileFormat::Parquet,
                record_count: 2,
                schema_id: 1,
                partition_spec_id: 0,
                sort_order_id: 0,
                partition_values: BTreeMap::new(),
                metadata: BTreeMap::from([(
                    "ordinal".into(),
                    otmp_protocol::CanonicalValue::Integer(ordinal as i128),
                )]),
                metrics: vec![FileMetric {
                    field_id: 1,
                    lower_bound: Some(TypedScalar::Int64(100)),
                    upper_bound: Some(TypedScalar::Int64(101)),
                    null_count: Some(0),
                    ..metric()
                }],
            })
            .collect();
        table
            .append_files(&AppendRequest::new(format!("batch-{start}"), descriptors))
            .await
            .unwrap();
    }
    (dir, table)
}
fn metric() -> FileMetric {
    FileMetric {
        field_id: 1,
        column_size_bytes: None,
        value_count: None,
        null_count: None,
        nan_count: None,
        distinct_count: None,
        lower_bound: None,
        upper_bound: None,
        metadata: BTreeMap::new(),
    }
}
async fn open(
    table: &Table<LocalObjectStore>,
    history: bool,
    concurrency: usize,
) -> OtmpTableProvider<LocalObjectStore> {
    OtmpTableProvider::open(
        table,
        MetadataSelection::Current,
        if history {
            SnapshotSelection::SequenceNumber(3)
        } else {
            SnapshotSelection::Ref("main".into())
        },
        ReaderOptions::default(),
        ProviderOptions {
            preflight_concurrency: concurrency,
            ..ProviderOptions::default()
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn replanning_with_one_provider_reuses_the_validated_file() {
    let (_dir, table) = fixture(1).await;
    let provider = open(&table, false, 8).await;
    let context = SessionContext::new();
    let first = provider
        .scan(&context.state(), None, &[], None)
        .await
        .unwrap();
    let cold = provider.metrics();
    assert_eq!(cold.parquet_requests, 3);
    drop(first);

    let second = provider
        .scan(&context.state(), None, &[], None)
        .await
        .unwrap();
    let warm = provider.metrics();
    assert_eq!(warm.parquet_requests, cold.parquet_requests);
    assert_eq!(warm.parquet_bytes, cold.parquet_bytes);
    assert_eq!(warm.validated_file_cache_hits, 1);
    drop(second);
    provider.footer_cache.assert_idle();
}

#[tokio::test]
async fn repeated_and_conflicting_identities_reconcile_before_any_plan_is_returned() {
    let (_dir, table) = fixture(257).await;
    for history in [false, true] {
        for concurrency in [1, 8] {
            for later in [false, true] {
                for pruned in [false, true] {
                    for conflict in ["none", "hash", "length"] {
                        let mut provider = open(&table, history, concurrency).await;
                        let seen = Arc::new(AtomicUsize::new(0));
                        let original = Arc::new(Mutex::new(None::<otmp::LiveFile>));
                        let target = if later { 256 } else { 1 };
                        let seen_hook = seen.clone();
                        provider.hooks.batch = Some(Arc::new(move |files| {
                            let mut original = original.lock().unwrap();
                            for file in files {
                                let ordinal = seen_hook.fetch_add(1, Ordering::Relaxed);
                                let first = original.get_or_insert_with(|| file.file.clone());
                                // All descriptors select the same immutable object, making
                                // the one-stat assertion independent of pagination order.
                                file.file.uri = first.uri.clone();
                                if ordinal == target {
                                    match conflict {
                                        "hash" => {
                                            file.file.content_sha256 =
                                                Some(Sha256::digest(b"conflict"));
                                        }
                                        "length" => file.file.file_size_bytes += 1,
                                        _ => {}
                                    }
                                }
                                if pruned && ordinal != 0 {
                                    file.metrics[0].lower_bound = Some(TypedScalar::Int64(0));
                                    file.metrics[0].upper_bound = Some(TypedScalar::Int64(1));
                                }
                            }
                        }));
                        let context = SessionContext::new();
                        let result = provider
                            .scan(
                                &context.state(),
                                Some(&vec![0]),
                                &[col("id").gt_eq(lit(100_i64))],
                                None,
                            )
                            .await;
                        if conflict == "none" {
                            let plan = result.unwrap();
                            assert_eq!(
                                provider.io.requests.load(Ordering::Relaxed),
                                3,
                                "one stat and two footer ranges for the repeated URI"
                            );
                            let batches =
                                datafusion::physical_plan::collect(plan, context.task_ctx())
                                    .await
                                    .unwrap();
                            assert_eq!(
                                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                                if pruned { 2 } else { 514 }
                            );
                        } else {
                            let error = result.unwrap_err();
                            assert!(
                                error
                                    .to_string()
                                    .contains("conflicting immutable data descriptors"),
                                "history={history} later={later} pruned={pruned} {conflict}: {error}"
                            );
                        }
                        assert!(seen.load(Ordering::Relaxed) > target);
                        assert_eq!(provider.preflight.available_permits(), concurrency);
                        assert_eq!(provider.active_scans.load(Ordering::Relaxed), 0);
                        provider.footer_cache.assert_idle();
                        assert_eq!(context.runtime_env().memory_pool.reserved(), 0);
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn cancelling_at_schema_validation_releases_scan_ownership() {
    let (_dir, table) = fixture(1).await;
    let mut provider = open(&table, false, 8).await;
    let gate = [
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    ];
    provider.hooks.validation = Mutex::new(Some(gate.clone()));
    let provider = Arc::new(provider);
    let context = SessionContext::new();
    let scanning = provider.clone();
    let state = context.state();
    let task = tokio::spawn(async move { scanning.scan(&state, None, &[], None).await });
    gate[0].notified().await;
    assert_eq!(provider.preflight.available_permits(), 7);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(provider.preflight.available_permits(), 8);
    assert_eq!(provider.active_scans.load(Ordering::Relaxed), 0);
    provider.footer_cache.assert_idle();
    assert_eq!(context.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn independent_scans_pin_their_own_version_and_stale_ranges_preserve_the_cause() {
    use otmp::ObjectStore;
    let (dir, table) = fixture(1).await;
    let mut old = open(&table, false, 8).await;
    let uri = old.reader.files(None, &[], 1).await.unwrap().files[0]
        .file
        .uri
        .clone();
    let before = old.reader.store().stat(&uri).await.unwrap();
    let gate = [
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    ];
    old.hooks.after_pin = Mutex::new(Some(gate.clone()));
    let old = Arc::new(old);
    let context = SessionContext::new();
    let scanning = old.clone();
    let state = context.state();
    let task = tokio::spawn(async move { scanning.scan(&state, None, &[], None).await });
    gate[0].notified().await;
    // Replace the inode with identical bytes: content/length remain compatible,
    // while the immutable transport version must never be borrowed from a peer.
    let path = dir.path().join(uri.as_str());
    let replacement = dir.path().join("replacement.parquet");
    std::fs::copy(&path, &replacement).unwrap();
    std::fs::rename(&replacement, &path).unwrap();
    assert_ne!(
        old.reader.store().stat(&uri).await.unwrap().version,
        before.version
    );
    let new = open(&table, false, 8).await;
    let plan = new.scan(&context.state(), None, &[], None).await.unwrap();
    assert_eq!(new.io.requests.load(Ordering::Relaxed), 3);
    assert_eq!(
        datafusion::physical_plan::collect(plan, context.task_ctx())
            .await
            .unwrap()[0]
            .num_rows(),
        2
    );
    gate[1].notify_one();
    let error = task.await.unwrap().unwrap_err();
    let mut cause: &(dyn std::error::Error + 'static) = &error;
    loop {
        if let Some(otmp::StorageError::VerificationFailed(message)) =
            cause.downcast_ref::<otmp::StorageError>()
        {
            assert!(message.contains("version changed"));
            break;
        }
        cause = cause
            .source()
            .unwrap_or_else(|| panic!("original storage cause lost: {error:?}"));
    }
    assert_eq!(
        old.io.requests.load(Ordering::Relaxed),
        2,
        "the stale scan acquired its own pin then attempted its pinned trailer"
    );
    old.footer_cache.assert_idle();
    assert_eq!(old.footer_cache.statistics().0, 0);
    assert_eq!(old.preflight.available_permits(), 8);
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Keep both shared failure and retry assertions in one controlled fill.
async fn concurrent_malformed_files_fail_without_a_plan_or_stranded_load() {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    for corruption in ["trailer", "length", "container", "required-field"] {
        let (dir, table) = fixture(1).await;
        let mut provider = open(&table, false, 8).await;
        let file = provider.reader.files(None, &[], 1).await.unwrap().files[0]
            .file
            .clone();
        let path = dir.path().join(file.uri.as_str());
        let mut bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        let expected = match corruption {
            "trailer" => {
                bytes[n - 1] = b'X';
                "invalid Parquet footer trailer"
            }
            "length" => {
                bytes[n - 8..n - 4].copy_from_slice(&u32::MAX.to_le_bytes());
                "footer length exceeds"
            }
            "container" => {
                let footer_len =
                    u32::from_le_bytes(bytes[n - 8..n - 4].try_into().unwrap()) as usize;
                // Compact-Thrift list field with an impossible declared element count.
                bytes[n - 8 - footer_len..n - 8 - footer_len + 6]
                    .copy_from_slice(&[0x19, 0xf5, 0xff, 0xff, 0xff, 0x7f]);
                "Parquet"
            }
            _ => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "other",
                    DataType::Int64,
                    false,
                )]));
                let batch =
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
                        .unwrap();
                let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
                writer.write(&batch).unwrap();
                bytes = writer.into_inner().unwrap();
                "required"
            }
        };
        std::fs::write(&path, &bytes).unwrap();
        let length = bytes.len() as u64;
        provider.hooks.batch = Some(Arc::new(move |files| {
            for file in files {
                file.file.file_size_bytes = length;
                file.file.content_sha256 = Some(Sha256::digest(&bytes));
            }
        }));
        let gate = [
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        ];
        provider.hooks.after_pin = Mutex::new(Some(gate.clone()));
        let provider = Arc::new(provider);
        let context = SessionContext::new();
        let state = context.state();
        // Empty projection must still validate the required id field.
        let projection = vec![];
        let first = {
            let provider = provider.clone();
            let state = state.clone();
            let projection = projection.clone();
            tokio::spawn(async move { provider.scan(&state, Some(&projection), &[], None).await })
        };
        let second = {
            let provider = provider.clone();
            let state = state.clone();
            let projection = projection.clone();
            tokio::spawn(async move { provider.scan(&state, Some(&projection), &[], None).await })
        };
        gate[0].notified().await;
        while provider.preflight.available_permits() != 6 {
            tokio::task::yield_now().await;
        }
        gate[1].notify_one();
        let (a, b) = tokio::join!(first, second);
        for result in [a.unwrap(), b.unwrap()] {
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "{corruption}: {error}"
            );
        }
        let fill_requests = if matches!(corruption, "trailer" | "length") {
            2
        } else {
            3
        };
        assert_eq!(
            provider.io.requests.load(Ordering::Relaxed),
            fill_requests,
            "concurrent first scans must share one stat/footer fill"
        );
        provider.footer_cache.assert_idle();
        assert_eq!(provider.preflight.available_permits(), 8);
        assert_eq!(context.runtime_env().memory_pool.reserved(), 0);
        // Repeat after failure: no stale registry entry may strand or hide it.
        assert!(
            provider
                .scan(&state, Some(&projection), &[], None)
                .await
                .is_err()
        );
        assert_eq!(
            provider.io.requests.load(Ordering::Relaxed),
            if corruption == "required-field" {
                fill_requests + 1
            } else {
                fill_requests * 2
            },
            "{corruption}: failed fills must be removed before retry"
        );
        provider.footer_cache.assert_idle();
    }
}

#[tokio::test]
async fn eviction_forces_full_revalidation_under_one_entry_budget() {
    use otmp::ObjectStore;
    let (dir, table) = fixture(2).await;
    let mut provider = open(&table, false, 8).await;
    let files = provider.reader.files(None, &[], 256).await.unwrap();
    let mut limit = 0;
    for file in &files.files {
        let metadata = provider.reader.store().stat(&file.file.uri).await.unwrap();
        let bytes = std::fs::read(dir.path().join(file.file.uri.as_str())).unwrap();
        let footer_len =
            u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap())
                as usize;
        limit = limit.max(
            footer_len * 128
                + 64 * 1024
                + 512
                + file.file.uri.as_str().len() * 4
                + metadata.version.as_opaque().len() * 2
                + 512
                + file.file.uri.as_str().len() * 4
                + 256,
        );
    }
    provider.footer_cache = crate::footer::FooterCache::new(limit).unwrap();
    let rounds = AtomicUsize::new(0);
    provider.hooks.batch = Some(Arc::new(move |files| {
        if files.is_empty() {
            return;
        }
        let selected = usize::from(rounds.fetch_add(1, Ordering::Relaxed) >= 1);
        for (index, file) in files.iter_mut().enumerate() {
            if index != selected {
                file.metrics[0].lower_bound = Some(TypedScalar::Int64(0));
                file.metrics[0].upper_bound = Some(TypedScalar::Int64(1));
            }
        }
    }));
    let context = SessionContext::new();
    let filters = [col("id").gt_eq(lit(100_i64))];
    drop(
        provider
            .scan(&context.state(), None, &filters, None)
            .await
            .unwrap(),
    );
    assert_eq!(provider.io.requests.load(Ordering::Relaxed), 3);
    drop(
        provider
            .scan(&context.state(), None, &filters, None)
            .await
            .unwrap(),
    );
    assert_eq!(provider.io.requests.load(Ordering::Relaxed), 6);
    provider.footer_cache.assert_idle();
    assert!(provider.footer_cache.statistics().1 <= limit);
    assert_eq!(provider.preflight.available_permits(), 8);
    assert_eq!(context.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn repeated_uri_with_another_schema_keeps_a_preflight_lease_during_final_binding() {
    let (_dir, table) = fixture(2).await;
    let initial = open(&table, false, 8).await;
    let mut second = initial.reader.schema().clone();
    second.schema_id = 2;
    second.parent_schema_id = Some(1);
    table
        .transact(&otmp::TransactionRequest {
            idempotency_key: "second-recorded-schema".into(),
            requirements: vec![
                otmp::Requirement::CurrentSchemaIs { schema_id: 1 },
                otmp::Requirement::SchemaIdAbsent { schema_id: 2 },
                otmp::Requirement::FieldIdsAbsent { field_ids: vec![] },
            ],
            operations: vec![otmp::OperationRequest::AddSchema {
                operation_id: "add".into(),
                schema: second,
            }],
            commit_metadata: otmp::CommitMetadata::default(),
        })
        .await
        .unwrap();
    let mut provider = open(&table, false, 8).await;
    provider.hooks.batch = Some(Arc::new(|files| {
        if files.len() == 2 {
            files[1].file.uri = files[0].file.uri.clone();
            files[1].schema_id = 2;
        }
    }));
    let gate = [
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    ];
    provider.hooks.binding = Mutex::new(Some(gate.clone()));
    let provider = Arc::new(provider);
    let context = SessionContext::new();
    let scanning = provider.clone();
    let state = context.state();
    let task = tokio::spawn(async move { scanning.scan(&state, None, &[], None).await });
    gate[0].notified().await;
    assert_eq!(
        provider.io.requests.load(Ordering::Relaxed),
        3,
        "the second schema must reuse the original pin/footer"
    );
    assert!(provider.footer_cache.statistics().0 > 0);
    gate[1].notify_one();
    let plan = task.await.unwrap().unwrap();
    assert_eq!(
        datafusion::physical_plan::collect(plan, context.task_ctx())
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );
    provider.footer_cache.assert_idle();
}
