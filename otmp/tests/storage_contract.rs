use otmp::{InMemoryObjectStore, LocalObjectStore, ObjectStore, ObjectVersion, StorageError};
use otmp_protocol::{RelativeUri, Sha256};

#[tokio::test]
async fn concurrent_create_only_writes_never_replace_the_winner() {
    let store = InMemoryObjectStore::default();
    let key: RelativeUri = "data/collision.parquet".parse().unwrap();
    let left_store = store.clone();
    let left_key = key.clone();
    let right_store = store.clone();
    let right_key = key.clone();

    let (left, right) = tokio::join!(
        async move {
            let mut reader = std::io::Cursor::new(b"left".to_vec());
            left_store
                .create_from_reader(&left_key, &mut reader, None)
                .await
        },
        async move {
            let mut reader = std::io::Cursor::new(b"right".to_vec());
            right_store
                .create_from_reader(&right_key, &mut reader, None)
                .await
        }
    );

    assert_ne!(left.is_ok(), right.is_ok());
    let loser = if left.is_err() { left } else { right };
    assert!(matches!(loser, Err(StorageError::ImmutableConflict(_))));
    let stored = store.read(&key).await.unwrap().bytes;
    assert!(stored == b"left" || stored == b"right");
}

#[tokio::test]
async fn local_create_conflict_and_wrong_version_cleanup_preserve_existing_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(directory.path()).unwrap();
    let key: RelativeUri = "data/existing.parquet".parse().unwrap();
    let mut first = std::io::Cursor::new(b"winner".to_vec());
    let created = store
        .create_from_reader(&key, &mut first, None)
        .await
        .unwrap();
    let mut second = std::io::Cursor::new(b"loser".to_vec());

    let error = store
        .create_from_reader(&key, &mut second, None)
        .await
        .unwrap_err();
    assert!(matches!(error, StorageError::ImmutableConflict(_)));
    assert_eq!(store.read(&key).await.unwrap().bytes, b"winner");

    let wrong_version = ObjectVersion::from_sha256(Sha256::digest(b"different"));
    assert!(!store.delete_if_version(&key, &wrong_version).await.unwrap());
    assert_eq!(store.read(&key).await.unwrap().bytes, b"winner");
    assert!(
        store
            .delete_if_version(&key, &created.version)
            .await
            .unwrap()
    );
    assert!(matches!(
        store.read(&key).await,
        Err(StorageError::NotFound(_))
    ));
}
