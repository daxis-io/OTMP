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

fn metadata(
    key: &str,
    requirements: Vec<Requirement>,
    operations: Vec<OperationRequest>,
) -> TransactionRequest {
    TransactionRequest {
        idempotency_key: key.into(),
        requirements,
        operations,
        commit_metadata: CommitMetadata::default(),
    }
}

async fn append(
    table: &Table<LocalObjectStore>,
    source: &std::path::Path,
    key: &str,
    branch: &str,
    schema_id: u32,
) -> otmp::AppendResult {
    let bytes = key.as_bytes();
    std::fs::write(source, bytes).unwrap();
    let mut request = otmp::AppendRequest::new(
        key,
        vec![otmp::AppendFile {
            source_path: source.into(),
            fingerprint: otmp::SourceFingerprint {
                sha256: otmp_protocol::Sha256::digest(bytes),
                length: bytes.len() as u64,
            },
            format: otmp::FileFormat::Parquet,
            record_count: 1,
            schema_id,
            partition_spec_id: 0,
            sort_order_id: 0,
            partition_values: std::collections::BTreeMap::default(),
            metrics: vec![],
            metadata: std::collections::BTreeMap::default(),
        }],
    );
    request.target_ref = branch.into();
    table.append_files(&request).await.unwrap()
}

#[tokio::test]
async fn null_branches_tags_and_invalid_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let source = tempfile::NamedTempFile::new().unwrap();
    let table = Table::new(LocalObjectStore::new(dir.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    table
        .transact(&metadata(
            "empty",
            vec![Requirement::RefAbsent {
                name: "empty".into(),
            }],
            vec![OperationRequest::CreateRef {
                operation_id: "empty".into(),
                name: "empty".into(),
                ref_type: otmp::RefType::Branch,
                snapshot_id: None,
            }],
        ))
        .await
        .unwrap();
    let snapshot = append(&table, source.path(), "root", "main", 1).await;
    table
        .transact(&metadata(
            "tag",
            vec![
                Requirement::RefAbsent { name: "v1".into() },
                Requirement::SnapshotExists {
                    snapshot_id: snapshot.snapshot_id,
                },
            ],
            vec![OperationRequest::CreateRef {
                operation_id: "tag".into(),
                name: "v1".into(),
                ref_type: otmp::RefType::Tag,
                snapshot_id: Some(snapshot.snapshot_id),
            }],
        ))
        .await
        .unwrap();
    let pin = table.pin().await.unwrap();
    assert_eq!(pin.files("main").unwrap().len(), 1);
    assert_eq!(
        pin.resolve_snapshot(SnapshotSelection::Ref("v1".into()))
            .unwrap()
            .files()
            .unwrap()
            .len(),
        1
    );
    assert!(
        pin.resolve_snapshot(SnapshotSelection::SequenceNumber(99))
            .is_err()
    );
    assert!(
        table
            .pin_metadata(MetadataSelection::TableVersion(99))
            .await
            .is_err()
    );
    let drop_main = metadata(
        "drop-main",
        vec![
            Requirement::RefExists {
                name: "main".into(),
                ref_type: otmp::RefType::Branch,
            },
            Requirement::RefSnapshotIs {
                name: "main".into(),
                snapshot_id: None,
            },
        ],
        vec![OperationRequest::DropRef {
            operation_id: "drop".into(),
            name: "main".into(),
        }],
    );
    assert!(table.transact(&drop_main).await.is_err());
    table.verify_history().await.unwrap();
}

#[tokio::test]
async fn ref_movement_rematerializes_live_membership() {
    let dir = tempfile::tempdir().unwrap();
    let source = tempfile::NamedTempFile::new().unwrap();
    let table = Table::new(LocalObjectStore::new(dir.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let a = append(&table, source.path(), "A", "main", 1).await;
    let before = table.pin().await.unwrap();
    let result = table
        .transact(&property("property", CanonicalValue::Bool(true)))
        .await
        .unwrap();
    assert_eq!(result.table_version, 2);
    assert_eq!(
        table.pin().await.unwrap().files("main").unwrap(),
        before.files("main").unwrap()
    );
    let create = metadata(
        "audit",
        vec![
            Requirement::RefAbsent {
                name: "audit".into(),
            },
            Requirement::SnapshotExists {
                snapshot_id: a.snapshot_id,
            },
        ],
        vec![OperationRequest::CreateRef {
            operation_id: "create".into(),
            name: "audit".into(),
            ref_type: otmp::RefType::Branch,
            snapshot_id: Some(a.snapshot_id),
        }],
    );
    table.transact(&create).await.unwrap();
    let b = append(&table, source.path(), "B", "main", 1).await;
    assert_eq!(b.sequence_number, 2);
    assert_eq!(b.table_version, 4);
    assert_eq!(table.pin().await.unwrap().files("audit").unwrap().len(), 1);
    table
        .transact(&metadata(
            "move",
            vec![
                Requirement::RefExists {
                    name: "audit".into(),
                    ref_type: otmp::RefType::Branch,
                },
                Requirement::RefSnapshotIs {
                    name: "audit".into(),
                    snapshot_id: Some(a.snapshot_id),
                },
                Requirement::SnapshotExists {
                    snapshot_id: b.snapshot_id,
                },
            ],
            vec![OperationRequest::ReplaceRef {
                operation_id: "move".into(),
                name: "audit".into(),
                snapshot_id: b.snapshot_id,
            }],
        ))
        .await
        .unwrap();
    assert_eq!(table.pin().await.unwrap().files("audit").unwrap().len(), 2);
    let stale = metadata(
        "stale",
        vec![
            Requirement::RefExists {
                name: "audit".into(),
                ref_type: otmp::RefType::Branch,
            },
            Requirement::RefSnapshotIs {
                name: "audit".into(),
                snapshot_id: Some(a.snapshot_id),
            },
            Requirement::SnapshotExists {
                snapshot_id: b.snapshot_id,
            },
        ],
        vec![OperationRequest::ReplaceRef {
            operation_id: "stale".into(),
            name: "audit".into(),
            snapshot_id: b.snapshot_id,
        }],
    );
    assert!(table.transact(&stale).await.is_err());
    for version in 0..=5 {
        assert_eq!(
            table
                .pin_metadata(MetadataSelection::TableVersion(version))
                .await
                .unwrap()
                .coordinates()
                .table_version,
            version
        );
    }
    table.verify_history().await.unwrap();
}
