use std::collections::BTreeMap;
use std::path::Path;

use otmp::{
    AppendFile, AppendRequest, FileFormat, InitializeRequest, LocalObjectStore, SourceFingerprint,
    Table,
};
use otmp_protocol::{Field, LogicalType, Schema, Sha256};

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
        doc: Some("test schema".into()),
    }
}

async fn initialized(root: &Path) -> Table<LocalObjectStore> {
    let table = Table::new(LocalObjectStore::new(root).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    table
}

#[tokio::test]
async fn genesis_requires_initial_schema_one_without_a_parent() {
    for invalid_schema in [
        Schema {
            schema_id: 2,
            ..schema()
        },
        Schema {
            parent_schema_id: Some(1),
            ..schema()
        },
    ] {
        let directory = tempfile::tempdir().unwrap();
        let table = Table::new(LocalObjectStore::new(directory.path()).unwrap());
        let error = table
            .initialize(InitializeRequest::new(invalid_schema))
            .await
            .unwrap_err();
        assert_eq!(error.code(), "OTMP_INVALID_INITIALIZE");
        assert!(!directory.path().join("_otmp/HEAD").exists());
    }
}

fn append_request(path: &Path, bytes: &[u8], key: &str) -> AppendRequest {
    AppendRequest::new(
        key,
        vec![AppendFile {
            source_path: path.to_path_buf(),
            fingerprint: SourceFingerprint {
                sha256: Sha256::digest(bytes),
                length: bytes.len() as u64,
            },
            format: FileFormat::Parquet,
            record_count: 3,
            schema_id: 1,
            partition_spec_id: 0,
            sort_order_id: 0,
            partition_values: BTreeMap::new(),
            metrics: Vec::new(),
            metadata: BTreeMap::new(),
        }],
    )
}

#[tokio::test]
async fn genesis_is_self_contained_and_pinned() {
    let directory = tempfile::tempdir().unwrap();
    let table = initialized(directory.path()).await;

    let pinned = table.pin().await.unwrap();
    assert_eq!(pinned.status().table_version, 0);
    assert_eq!(pinned.status().root_revision, 0);
    assert_eq!(pinned.status().current_snapshot_id, None);
    assert!(pinned.files("main").unwrap().is_empty());
    assert_eq!(pinned.history().unwrap().len(), 1);
    table.verify().await.unwrap();

    assert!(directory.path().join("_otmp/HEAD").is_file());
    assert!(directory.path().join("_otmp/commits/0").is_dir());
    assert!(directory.path().join("_otmp/checkpoints/0").is_dir());
    assert!(directory.path().join("_otmp/generations/0").is_dir());
}

#[tokio::test]
async fn append_stages_exact_bytes_and_retry_returns_stable_result() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.parquet");
    let bytes = b"PAR1not-semantically-validatedPAR1";
    tokio::fs::write(&source, bytes).await.unwrap();
    let table = initialized(&directory.path().join("table")).await;
    let request = append_request(&source, bytes, "batch-1");

    let first = table.append_files(&request).await.unwrap();
    tokio::fs::remove_file(&source).await.unwrap();
    let second = table.append_files(&request).await.unwrap();

    assert_eq!(first, second);
    assert_eq!(first.table_version, 1);
    assert_eq!(first.files.len(), 1);
    let stored = tokio::fs::read(
        directory
            .path()
            .join("table")
            .join(first.files[0].uri.as_str()),
    )
    .await
    .unwrap();
    assert_eq!(stored, bytes);

    let pinned = table.pin().await.unwrap();
    assert_eq!(pinned.files("main").unwrap().len(), 1);
    assert_eq!(pinned.history().unwrap().len(), 2);
    table.verify().await.unwrap();
}

#[tokio::test]
async fn idempotency_conflict_happens_before_source_io() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.parquet");
    let bytes = b"data";
    tokio::fs::write(&source, bytes).await.unwrap();
    let table = initialized(&directory.path().join("table")).await;
    table
        .append_files(&append_request(&source, bytes, "same-key"))
        .await
        .unwrap();
    tokio::fs::remove_file(&source).await.unwrap();

    let changed = append_request(&source, b"different", "same-key");
    let error = table.append_files(&changed).await.unwrap_err();
    assert_eq!(error.code(), "OTMP_IDEMPOTENCY_CONFLICT");
}

#[tokio::test]
async fn bad_fingerprint_does_not_advance_head_or_leave_staging() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.parquet");
    tokio::fs::write(&source, b"actual").await.unwrap();
    let table = initialized(&directory.path().join("table")).await;
    let request = append_request(&source, b"expected", "bad");

    let error = table.append_files(&request).await.unwrap_err();
    assert_eq!(error.code(), "OTMP_FINGERPRINT_MISMATCH");
    assert_eq!(table.pin().await.unwrap().status().table_version, 0);
    let data_dir = directory.path().join("table/data");
    let mut entries = tokio::fs::read_dir(data_dir).await.unwrap();
    assert!(entries.next_entry().await.unwrap().is_none());
}

#[tokio::test]
async fn an_old_pin_does_not_reread_head() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.parquet");
    let bytes = b"data";
    tokio::fs::write(&source, bytes).await.unwrap();
    let table = initialized(&directory.path().join("table")).await;
    let old = table.pin().await.unwrap();

    table
        .append_files(&append_request(&source, bytes, "pin"))
        .await
        .unwrap();

    assert_eq!(old.status().table_version, 0);
    assert!(old.files("main").unwrap().is_empty());
    assert_eq!(table.pin().await.unwrap().status().table_version, 1);
}

#[tokio::test]
async fn copied_table_root_opens_without_a_catalog_or_sources() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.parquet");
    let bytes = b"data";
    tokio::fs::write(&source, bytes).await.unwrap();
    let table_root = directory.path().join("table");
    let table = initialized(&table_root).await;
    table
        .append_files(&append_request(&source, bytes, "copy"))
        .await
        .unwrap();

    let copy_root = directory.path().join("copy");
    copy_dir(&table_root, &copy_root).await;
    tokio::fs::remove_file(source).await.unwrap();
    let copy = Table::new(LocalObjectStore::new(copy_root).unwrap());
    copy.verify().await.unwrap();
    assert_eq!(copy.pin().await.unwrap().files("main").unwrap().len(), 1);
}

async fn copy_dir(source: &Path, destination: &Path) {
    tokio::fs::create_dir_all(destination).await.unwrap();
    let mut stack = vec![(source.to_path_buf(), destination.to_path_buf())];
    while let Some((from, to)) = stack.pop() {
        let mut entries = tokio::fs::read_dir(from).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let target = to.join(entry.file_name());
            if entry.file_type().await.unwrap().is_dir() {
                tokio::fs::create_dir_all(&target).await.unwrap();
                stack.push((entry.path(), target));
            } else {
                tokio::fs::copy(entry.path(), target).await.unwrap();
            }
        }
    }
}
