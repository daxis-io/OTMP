use object_store::aws::AmazonS3Builder;
use otmp::{ConditionalWriteOutcome, ObjectStore, StorageError};
use otmp_protocol::RelativeUri;
use otmp_s3::S3ObjectStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[test]
fn opaque_version_round_trips_provider_etag_and_version_id() {
    let version = S3ObjectStore::object_version(Some("etag-1"), Some("version-7")).unwrap();
    assert_eq!(
        S3ObjectStore::provider_version(&version),
        Some((Some("etag-1".to_owned()), Some("version-7".to_owned())))
    );
}

#[tokio::test]
async fn conditional_delete_is_explicitly_unsupported() {
    let store = S3ObjectStore::from_object_store(std::sync::Arc::new(
        object_store::memory::InMemory::new(),
    ));
    let key: RelativeUri = "objects/one".parse().unwrap();
    let version = S3ObjectStore::object_version(Some("etag"), None).unwrap();

    let error = store.delete_if_version(&key, &version).await.unwrap_err();
    assert!(matches!(error, StorageError::Unsupported(_)));
}

#[tokio::test]
async fn rejects_a_reader_without_an_exact_bounded_length() {
    let store = S3ObjectStore::from_object_store(std::sync::Arc::new(
        object_store::memory::InMemory::new(),
    ));
    let key: RelativeUri = "objects/one".parse().unwrap();
    let mut reader = std::io::Cursor::new(b"body".to_vec());

    let error = store
        .create_from_reader(&key, &mut reader, None)
        .await
        .unwrap_err();
    assert!(matches!(error, StorageError::Unsupported(_)));
}

#[tokio::test]
async fn rejects_a_source_that_exceeds_its_declared_length_without_accepting_it() {
    let store = S3ObjectStore::from_object_store(std::sync::Arc::new(
        object_store::memory::InMemory::new(),
    ));
    let key: RelativeUri = "objects/one".parse().unwrap();
    let mut reader = std::io::Cursor::new(b"five!".to_vec());
    assert!(matches!(
        store.create_from_reader(&key, &mut reader, Some(4)).await,
        Err(StorageError::MaximumLengthExceeded)
    ));
}

#[test]
fn empty_provider_tokens_are_not_usable_versions() {
    assert!(S3ObjectStore::object_version(Some("  "), Some("")).is_err());
}

#[tokio::test]
async fn conditional_write_returns_a_version_that_can_drive_a_second_write() {
    let store = S3ObjectStore::from_object_store(std::sync::Arc::new(
        object_store::memory::InMemory::new(),
    ));
    let result = store.create_head(b"head").await;
    let ConditionalWriteOutcome::Applied { new_version } = result else {
        panic!("first conditional write should apply");
    };
    assert!(matches!(
        store.replace_head(&new_version, b"next").await,
        ConditionalWriteOutcome::Applied { .. }
    ));
}

#[tokio::test]
async fn stale_compare_and_swap_and_two_writers_have_one_winner_without_listing() {
    let store = S3ObjectStore::from_object_store(std::sync::Arc::new(
        object_store::memory::InMemory::new(),
    ));
    let ConditionalWriteOutcome::Applied { new_version: first } = store.create_head(b"one").await
    else {
        panic!("create")
    };
    let ConditionalWriteOutcome::Applied {
        new_version: current,
    } = store.replace_head(&first, b"two").await
    else {
        panic!("move")
    };
    assert!(matches!(
        store.replace_head(&first, b"stale").await,
        ConditionalWriteOutcome::Conflict { .. }
    ));
    let (left, right) = tokio::join!(
        store.replace_head(&current, b"left"),
        store.replace_head(&current, b"right")
    );
    assert_eq!(
        usize::from(matches!(left, ConditionalWriteOutcome::Applied { .. }))
            + usize::from(matches!(right, ConditionalWriteOutcome::Applied { .. })),
        1
    );
}

#[tokio::test]
async fn scripted_s3_endpoint_receives_create_and_compare_and_swap_headers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for (expected_header, etag) in [
            ("if-none-match: *", "etag-a"),
            ("if-match: etag-a", "etag-b"),
        ] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            assert!(
                request.starts_with("PUT /bucket/_otmp/HEAD HTTP/1.1"),
                "{request}"
            );
            assert!(
                request.to_ascii_lowercase().contains(expected_header),
                "{request}"
            );
            stream
                .write_all(format!("HTTP/1.1 200 OK\r\nETag: {etag}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes())
                .await
                .unwrap();
        }
    });
    let store = S3ObjectStore::from_amazon_s3(
        AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_skip_signature(true),
    )
    .unwrap();
    let ConditionalWriteOutcome::Applied { new_version } = store.create_head(b"one").await else {
        panic!("create should apply");
    };
    assert!(matches!(
        store.replace_head(&new_version, b"two").await,
        ConditionalWriteOutcome::Applied { .. }
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn scripted_response_loss_after_apply_is_indeterminate_and_readback_is_exact() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for _ in 0..12 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            assert!(!request.contains("?list-type="), "adapter must not list");
            if request.starts_with("PUT /bucket/_otmp/HEAD HTTP/1.1") {
                drop(stream); // The provider applied the body but the response was lost.
                continue;
            }
            assert!(
                request.starts_with("GET /bucket/_otmp/HEAD HTTP/1.1"),
                "{request}"
            );
            stream.write_all(b"HTTP/1.1 200 OK\r\nETag: response-loss-etag\r\nContent-Length: 4\r\nConnection: close\r\n\r\nhead").await.unwrap();
            return;
        }
        panic!("adapter never performed exact readback");
    });
    let store = scripted_store(endpoint);
    assert!(matches!(
        store.create_head(b"head").await,
        ConditionalWriteOutcome::Indeterminate { .. }
    ));
    let key: RelativeUri = "_otmp/HEAD".parse().unwrap();
    let recovered = store.read(&key).await.unwrap();
    assert_eq!(recovered.bytes, b"head");
    assert_eq!(
        S3ObjectStore::provider_version(&recovered.version),
        Some((Some("response-loss-etag".into()), None))
    );
    server.await.unwrap();
}

#[tokio::test]
async fn tokenless_immutable_create_reconciles_by_exact_readback() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut put, _) = listener.accept().await.unwrap();
        assert!(
            read_http_request(&mut put)
                .await
                .starts_with("PUT /bucket/objects/immutable HTTP/1.1")
        );
        put.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let (mut get, _) = listener.accept().await.unwrap();
        assert!(
            read_http_request(&mut get)
                .await
                .starts_with("GET /bucket/objects/immutable HTTP/1.1")
        );
        get.write_all(b"HTTP/1.1 200 OK\r\nETag: recovered-etag\r\nContent-Length: 9\r\nConnection: close\r\n\r\nimmutable").await.unwrap();
    });
    let store = scripted_store(endpoint);
    let key: RelativeUri = "objects/immutable".parse().unwrap();
    let created = store.create_bytes(&key, b"immutable").await.unwrap();
    assert_eq!(
        S3ObjectStore::provider_version(&created.version),
        Some((Some("recovered-etag".into()), None))
    );
    server.await.unwrap();
}

fn scripted_store(endpoint: String) -> S3ObjectStore {
    S3ObjectStore::from_amazon_s3(
        AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_skip_signature(true),
    )
    .unwrap()
}

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        let read = stream.read(&mut buffer).await.unwrap();
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(bytes).unwrap()
}
