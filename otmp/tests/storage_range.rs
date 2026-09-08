use std::ops::Range;

use otmp::storage::{
    InMemoryObjectStore, LocalObjectStore, MAXIMUM_RANGE_LENGTH, ObjectStore, StorageError,
};
use otmp_protocol::RelativeUri;

async fn assert_exact_bounded_range<S: ObjectStore>(store: S) {
    let key: RelativeUri = "metadata/image.sqlite".parse().unwrap();
    store.create_bytes(&key, b"0123456789").await.unwrap();
    let metadata = store.stat(&key).await.unwrap();
    assert_eq!(metadata.length, 10);

    let result = store.read_range(&key, 2..6, &metadata).await.unwrap();
    assert_eq!(result.bytes, b"2345");
    assert_eq!(result.range, 2..6);
    result.validate(&metadata, &(2..6)).unwrap();

    for range in [
        0..0,
        Range { start: 7, end: 6 },
        0..11,
        0..MAXIMUM_RANGE_LENGTH + 1,
    ] {
        assert!(matches!(
            store.read_range(&key, range, &metadata).await,
            Err(StorageError::VerificationFailed(_))
        ));
    }
}

#[tokio::test]
async fn in_memory_range_reads_are_exact_bounded_and_version_pinned() {
    assert_exact_bounded_range(InMemoryObjectStore::default()).await;
}

#[tokio::test]
async fn local_range_reads_are_exact_bounded_and_version_pinned() {
    let directory = tempfile::tempdir().unwrap();
    assert_exact_bounded_range(LocalObjectStore::new(directory.path()).unwrap()).await;
}

#[cfg(unix)]
#[tokio::test]
async fn local_revision_detects_rewrites_even_when_mtime_is_restored() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(directory.path()).unwrap();
    let key: RelativeUri = "object".parse().unwrap();
    store.create_bytes(&key, b"original").await.unwrap();
    let expected = store.stat(&key).await.unwrap();
    let path = directory.path().join("object");
    let modified = path.metadata().unwrap().modified().unwrap();
    std::fs::write(&path, b"modified").unwrap();
    std::fs::File::open(&path)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(modified))
        .unwrap();
    assert!(matches!(
        store.read_range(&key, 0..4, &expected).await,
        Err(StorageError::VerificationFailed(_))
    ));
}

#[test]
fn stored_ranges_reject_wrong_metadata_and_wrong_body_length() {
    use otmp::storage::{ObjectMetadata, ObjectVersion, StoredRange};

    let expected = ObjectMetadata {
        length: 4,
        version: ObjectVersion::from_opaque("one"),
    };
    let range: Range<u64> = 1..3;
    let incorrect = StoredRange {
        bytes: b"x".to_vec(),
        range: range.clone(),
        metadata: expected.clone(),
    };
    assert!(matches!(
        incorrect.validate(&expected, &range),
        Err(StorageError::VerificationFailed(_))
    ));
}
