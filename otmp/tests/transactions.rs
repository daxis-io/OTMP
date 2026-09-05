use otmp::{
    CommitMetadata, InitializeRequest, LocalObjectStore, OperationRequest, Requirement, Table,
    TransactionRequest,
};
use otmp::{MetadataSelection, SnapshotSelection};
use otmp_protocol::{CanonicalValue, Schema, canonical_json};
fn schema() -> Schema {
    serde_json::from_slice(include_bytes!("../../conformance/sources/schema.json")).unwrap()
}
fn property(key: &str, value: CanonicalValue) -> TransactionRequest {
    TransactionRequest {
        idempotency_key: key.into(),
        requirements: vec![Requirement::PropertyIs {
            key: "owner".into(),
            value: CanonicalValue::Null,
        }],
        operations: vec![OperationRequest::SetProperties {
            operation_id: "properties".into(),
            updates: [("owner".into(), value)].into(),
            removals: vec![],
        }],
        commit_metadata: CommitMetadata::default(),
    }
}
#[tokio::test]
async fn property_preconditions_and_atomicity() {
    let dir = tempfile::tempdir().unwrap();
    let table = Table::new(LocalObjectStore::new(dir.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let before = table.pin().await.unwrap().status();
    assert!(
        table
            .transact(&property("null", CanonicalValue::Null))
            .await
            .is_err()
    );
    let mut request = property("overlap", CanonicalValue::String("team".into()));
    request.operations.push(OperationRequest::SetProperties {
        operation_id: "second".into(),
        updates: std::collections::BTreeMap::default(),
        removals: vec!["owner".into()],
    });
    assert!(table.transact(&request).await.is_err());
    assert_eq!(table.pin().await.unwrap().status(), before);
    let nested = property(
        "nested",
        canonical_json::parse(br#"{"nested":null}"#).unwrap(),
    );
    table.transact(&nested).await.unwrap();
    assert!(
        table
            .transact(&property("stale", CanonicalValue::Bool(true)))
            .await
            .is_err()
    );
}
#[tokio::test]
async fn metadata_publication_conflicts_ambiguity_and_idempotency() {
    let store = otmp::InMemoryObjectStore::default();
    let table = Table::new(store.clone());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let request = property("replay", CanonicalValue::Bool(true));
    store.inject_conditional(otmp::InjectedConditional::IndeterminateAfter);
    let result = table.transact(&request).await.unwrap();
    assert_eq!(table.transact(&request).await.unwrap(), result);
    assert!(matches!(
        table
            .transact(&property("replay", CanonicalValue::Bool(false)))
            .await,
        Err(otmp::RuntimeError::IdempotencyConflict)
    ));
    let mut next = property("next", CanonicalValue::Bool(false));
    next.requirements = vec![Requirement::PropertyIs {
        key: "other".into(),
        value: CanonicalValue::Null,
    }];
    next.operations = vec![OperationRequest::SetProperties {
        operation_id: "other".into(),
        updates: [("other".into(), CanonicalValue::Bool(false))].into(),
        removals: vec![],
    }];
    table.transact(&next).await.unwrap();
    assert_eq!(table.transact(&request).await.unwrap(), result);
}
#[tokio::test]
async fn metadata_only_version_has_no_snapshot_and_replays_hash() {
    let dir = tempfile::tempdir().unwrap();
    let table = Table::new(LocalObjectStore::new(dir.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let request = property("metadata", CanonicalValue::Bool(true));
    let result = table.transact(&request).await.unwrap();
    assert_eq!(result.table_version, 1);
    assert_eq!(table.transact(&request).await.unwrap(), result);
    let pin = table.pin().await.unwrap();
    assert_eq!(pin.status().current_snapshot_id, None);
    assert_eq!(
        pin.status().semantic_state_sha256,
        result.semantic_state_sha256
    );
    assert!(pin.files("main").unwrap().is_empty());
    table.verify().await.unwrap();
}

#[tokio::test]
async fn metadata_transactions_are_snapshot_free_and_replay_stable() {
    let dir = tempfile::tempdir().unwrap();
    let table = Table::new(LocalObjectStore::new(dir.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let request = property("owner", CanonicalValue::String("team".into()));
    let result = table.transact(&request).await.unwrap();
    assert_eq!(result.table_version, 1);
    assert_eq!(table.transact(&request).await.unwrap(), result);
    let current = table.pin().await.unwrap();
    assert_eq!(current.status().current_snapshot_id, None);
    assert_eq!(
        current.status().semantic_state_sha256,
        result.semantic_state_sha256
    );
    assert!(current.files("main").unwrap().is_empty());
    let old = table
        .pin_metadata(MetadataSelection::TableVersion(0))
        .await
        .unwrap();
    assert_eq!(old.coordinates().table_version, 0);
    assert_eq!(old.anchor().table_version, 1);
    assert!(
        old.resolve_snapshot(SnapshotSelection::Ref("main".into()))
            .unwrap()
            .descriptor()
            .is_none()
    );
    table.verify_history().await.unwrap();
}
