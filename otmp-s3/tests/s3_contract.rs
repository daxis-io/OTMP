use object_store::aws::AmazonS3Builder;
use otmp::{ConditionalWriteOutcome, ObjectStore, StorageError};
use otmp_protocol::RelativeUri;
use otmp_s3::S3ObjectStore;
use std::fmt::Write as _;
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
async fn http_stale_cas_and_two_writers_have_exactly_one_winner() {
    let server = ScriptedS3::start().await;
    let store = &server.store;
    let first = applied(store.create_head(b"one").await);
    assert_eq!(
        S3ObjectStore::provider_version(&first),
        Some((Some("etag-1".into()), Some("version-1".into())))
    );
    let current = applied(store.replace_head(&first, b"two").await);
    assert!(matches!(
        store.replace_head(&first, b"stale").await,
        ConditionalWriteOutcome::Conflict { .. }
    ));
    let (left, right) = tokio::join!(
        store.replace_head(&current, b"left"),
        store.replace_head(&current, b"right")
    );
    let (winner, bytes) = match (left, right) {
        (
            ConditionalWriteOutcome::Applied { new_version },
            ConditionalWriteOutcome::Conflict { .. },
        ) => (new_version, b"left".as_slice()),
        (
            ConditionalWriteOutcome::Conflict { .. },
            ConditionalWriteOutcome::Applied { new_version },
        ) => (new_version, b"right".as_slice()),
        outcomes => panic!("expected one Applied and one Conflict: {outcomes:?}"),
    };
    let head = store.read(&"_otmp/HEAD".parse().unwrap()).await.unwrap();
    assert_eq!(head.bytes, bytes);
    assert_eq!(head.version, winner);
    assert_eq!(server.state.lock().unwrap().preconditions_failed, 2);
    assert_eq!(server.state.lock().unwrap().head_writes.len(), 3);
    server.finish().await;
}

#[tokio::test]
async fn http_immutable_collision_checks_exact_length_and_hash() {
    let server = ScriptedS3::start().await;
    let key = "objects/immutable".parse().unwrap();
    let first = server.store.create_bytes(&key, b"original").await.unwrap();
    let replay = server.store.create_bytes(&key, b"original").await.unwrap();
    assert_eq!(first.version, replay.version);
    for bytes in [b"different".as_slice(), b"modified".as_slice()] {
        assert!(matches!(
            server.store.create_bytes(&key, bytes).await,
            Err(StorageError::VerificationFailed(_))
        ));
    }
    assert_eq!(server.store.read(&key).await.unwrap().bytes, b"original");
    assert_eq!(server.state.lock().unwrap().preconditions_failed, 3);
    server.finish().await;
}

#[tokio::test]
async fn version_only_head_readback_stays_indeterminate() {
    let server = ScriptedS3::start().await;
    {
        let mut state = server.state.lock().unwrap();
        state.omit_put_etag = true;
        state.omit_get_etag = true;
    }
    assert!(matches!(
        server.store.create_head(b"head").await,
        ConditionalWriteOutcome::Indeterminate { .. }
    ));
    let head = server
        .store
        .read(&"_otmp/HEAD".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(head.bytes, b"head");
    assert_eq!(
        S3ObjectStore::provider_version(&head.version),
        Some((None, Some("version-1".into())))
    );
    assert_eq!(server.state.lock().unwrap().head_writes.len(), 1);
    server.finish().await;
}

#[tokio::test]
async fn missing_put_etag_recovers_a_token_that_drives_the_next_cas() {
    let server = ScriptedS3::start().await;
    server.state.lock().unwrap().omit_put_etag = true;
    let first = applied(server.store.create_head(b"one").await);
    assert_eq!(
        S3ObjectStore::provider_version(&first),
        Some((Some("etag-1".into()), Some("version-1".into())))
    );
    let next = applied(server.store.replace_head(&first, b"two").await);
    let head = server
        .store
        .read(&"_otmp/HEAD".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(head.bytes, b"two");
    assert_eq!(head.version, next);
    server.finish().await;
}

#[tokio::test]
async fn tokenless_immutable_create_reconciles_by_exact_readback() {
    let server = ScriptedS3::start().await;
    server.state.lock().unwrap().omit_put_etag = true;
    let key = "objects/immutable".parse().unwrap();
    let created = server.store.create_bytes(&key, b"immutable").await.unwrap();
    assert_eq!(
        S3ObjectStore::provider_version(&created.version),
        Some((Some("etag-1".into()), Some("version-1".into())))
    );
    assert_eq!(server.store.read(&key).await.unwrap().bytes, b"immutable");
    server.finish().await;
}

#[tokio::test]
async fn post_apply_response_loss_is_indeterminate_and_preserves_the_written_body() {
    let server = ScriptedS3::start().await;
    server.state.lock().unwrap().lose_next_head_response = true;
    assert!(matches!(
        server.store.create_head(b"authored body").await,
        ConditionalWriteOutcome::Indeterminate { .. }
    ));
    assert_eq!(
        server
            .store
            .read(&"_otmp/HEAD".parse().unwrap())
            .await
            .unwrap()
            .bytes,
        b"authored body"
    );
    assert_eq!(server.state.lock().unwrap().lost_responses, 1);
    server.finish().await;
}

#[tokio::test]
async fn transaction_reconciles_post_apply_response_loss_without_double_publication() {
    use otmp::{
        CommitMetadata, InitializeRequest, OperationRequest, Requirement, Table, TransactionRequest,
    };
    use otmp_protocol::CanonicalValue;
    let server = ScriptedS3::start().await;
    let table = Table::new(server.store.clone());
    let schema =
        serde_json::from_slice(include_bytes!("../../conformance/sources/schema.json")).unwrap();
    table
        .initialize(InitializeRequest::new(schema))
        .await
        .unwrap();
    let request = TransactionRequest {
        idempotency_key: "response-loss".into(),
        requirements: vec![Requirement::PropertyIs {
            key: "owner".into(),
            value: CanonicalValue::Null,
        }],
        operations: vec![OperationRequest::SetProperties {
            operation_id: "set".into(),
            updates: [("owner".into(), CanonicalValue::Bool(true))].into(),
            removals: vec![],
        }],
        commit_metadata: CommitMetadata::default(),
    };
    server.state.lock().unwrap().lose_next_head_response = true;
    let result = table.transact(&request).await.unwrap();
    assert_eq!(result.table_version, 1);
    assert_eq!(table.transact(&request).await.unwrap(), result);
    let status = table.pin().await.unwrap().status();
    assert_eq!(status.table_version, 1);
    assert_eq!(status.root_revision, 1);
    assert_eq!(status.semantic_state_sha256, result.semantic_state_sha256);
    {
        let state = server.state.lock().unwrap();
        assert_eq!(state.lost_responses, 1);
        assert_eq!(state.head_writes.len(), 2); // genesis and this one semantic transaction
        assert_ne!(state.head_writes[0], state.head_writes[1]);
    }
    table.verify_history().await.unwrap();
    server.finish().await;
}

fn applied(outcome: ConditionalWriteOutcome) -> otmp::ObjectVersion {
    match outcome {
        ConditionalWriteOutcome::Applied { new_version } => new_version,
        other => panic!("expected Applied: {other:?}"),
    }
}

#[derive(Clone)]
struct HttpObject {
    bytes: Vec<u8>,
    etag: String,
    version: String,
}
#[derive(Default)]
struct ServerState {
    objects: std::collections::BTreeMap<String, HttpObject>,
    revision: u64,
    preconditions_failed: u64,
    list_requests: u64,
    requests: u64,
    head_writes: Vec<Vec<u8>>,
    omit_put_etag: bool,
    omit_get_etag: bool,
    lose_next_head_response: bool,
    lost_responses: u64,
}
struct ScriptedS3 {
    store: S3ObjectStore,
    state: std::sync::Arc<std::sync::Mutex<ServerState>>,
    task: tokio::task::JoinHandle<()>,
}
impl ScriptedS3 {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = std::sync::Arc::new(std::sync::Mutex::new(ServerState::default()));
        let handler_state = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    let request = read_http_request(&mut stream).await;
                    // Process the complete body and condition atomically before sending
                    // or losing the response. Concurrent clients share this state.
                    let response = handle_request(&mut handler_state.lock().unwrap(), request);
                    if let Some(response) = response {
                        stream.write_all(&response).await.unwrap();
                    }
                })
                .await
                .expect("scripted S3 request timed out");
            }
        });
        let store = S3ObjectStore::from_amazon_s3(
            AmazonS3Builder::new()
                .with_bucket_name("bucket")
                .with_region("us-east-1")
                .with_endpoint(endpoint)
                .with_allow_http(true)
                .with_virtual_hosted_style_request(false)
                .with_skip_signature(true)
                .with_retry(object_store::RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                })
                .with_client_options(
                    object_store::ClientOptions::new()
                        .with_allow_http(true)
                        .with_timeout(std::time::Duration::from_secs(5)),
                ),
        )
        .unwrap();
        Self { store, state, task }
    }
    async fn finish(mut self) {
        assert_eq!(self.state.lock().unwrap().list_requests, 0);
        assert!(self.state.lock().unwrap().requests > 0);
        self.task.abort();
        if let Err(error) = (&mut self.task).await {
            assert!(error.is_cancelled(), "scripted S3 server failed: {error}");
        }
    }
}
impl Drop for ScriptedS3 {
    fn drop(&mut self) {
        self.task.abort();
    }
}
struct HttpRequest {
    method: String,
    path: String,
    headers: std::collections::BTreeMap<String, String>,
    body: Vec<u8>,
}
async fn read_http_request(stream: &mut tokio::net::TcpStream) -> HttpRequest {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 8192];
    let header_end = loop {
        let count = stream.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0, "EOF before request headers");
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        assert!(bytes.len() < 64 * 1024, "oversized HTTP headers");
    };
    let header = std::str::from_utf8(&bytes[..header_end]).unwrap();
    let mut lines = header.split("\r\n");
    let mut first = lines.next().unwrap().split_whitespace();
    let method = first.next().unwrap().to_string();
    let path = first.next().unwrap().to_string();
    let headers: std::collections::BTreeMap<_, _> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    assert!(
        !headers.contains_key("transfer-encoding"),
        "bounded single PUT must use a known body length"
    );
    let length = headers
        .get("content-length")
        .map_or(0, |value| value.parse::<usize>().unwrap());
    assert!(length <= 64 * 1024 * 1024);
    while bytes.len() < header_end + length {
        let count = stream.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0, "EOF before complete request body");
        bytes.extend_from_slice(&buffer[..count]);
    }
    assert_eq!(bytes.len(), header_end + length);
    HttpRequest {
        method,
        path,
        headers,
        body: bytes[header_end..].to_vec(),
    }
}
fn handle_request(state: &mut ServerState, request: HttpRequest) -> Option<Vec<u8>> {
    state.requests += 1;
    if request.path.contains('?') || request.path.trim_end_matches('/') == "/bucket" {
        state.list_requests += 1;
        return Some(response(400, None, b"unexpected listing"));
    }
    assert!(request.path.starts_with("/bucket/"));
    match request.method.as_str() {
        "PUT" => {
            let existing = state.objects.get(&request.path);
            let condition = match (
                request.headers.get("if-none-match"),
                request.headers.get("if-match"),
            ) {
                (Some(value), None) if value == "*" => existing.is_none(),
                (None, Some(value)) => existing.is_some_and(|object| &object.etag == value),
                other => panic!("missing or invalid write condition: {other:?}"),
            };
            if !condition {
                state.preconditions_failed += 1;
                return Some(response(412, None, b"<Error><Code>PreconditionFailed</Code><Message>condition failed</Message></Error>"));
            }
            state.revision += 1;
            let object = HttpObject {
                bytes: request.body,
                etag: format!("etag-{}", state.revision),
                version: format!("version-{}", state.revision),
            };
            state.objects.insert(request.path.clone(), object.clone());
            if request.path == "/bucket/_otmp/HEAD" {
                state.head_writes.push(object.bytes.clone());
                if std::mem::take(&mut state.lose_next_head_response) {
                    state.lost_responses += 1;
                    return None;
                }
            }
            Some(response(200, Some((&object, state.omit_put_etag)), b""))
        }
        "GET" => Some(match state.objects.get(&request.path) {
            Some(object) => response(200, Some((object, state.omit_get_etag)), &object.bytes),
            None => response(404, None, b"<Error><Code>NoSuchKey</Code></Error>"),
        }),
        other => panic!("unexpected S3 method: {other}"),
    }
}
fn response(status: u16, object: Option<(&HttpObject, bool)>, body: &[u8]) -> Vec<u8> {
    let mut header = format!(
        "HTTP/1.1 {status} Scripted\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some((object, omit_etag)) = object {
        write!(header, "x-amz-version-id: {}\r\n", object.version).unwrap();
        if !omit_etag {
            write!(header, "ETag: {}\r\n", object.etag).unwrap();
        }
    }
    header.push_str("\r\n");
    let mut bytes = header.into_bytes();
    bytes.extend_from_slice(body);
    bytes
}
