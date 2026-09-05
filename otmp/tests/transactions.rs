use otmp::{
    CommitMetadata, InitializeRequest, LocalObjectStore, MetadataSelection, OperationRequest,
    Requirement, SnapshotSelection, Table, TransactionRequest,
};
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
#[allow(clippy::too_many_lines)]
async fn complete_transaction_history_refs_and_optional_schema() {
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
    let mut next = schema();
    next.schema_id = 2;
    next.parent_schema_id = Some(1);
    next.fields.push(otmp_protocol::Field {
        field_id: 2,
        name: "note".into(),
        required: false,
        field_type: otmp_protocol::LogicalType::String,
        doc: None,
        initial_default: None,
        write_default: None,
    });
    table
        .transact(&metadata(
            "schema",
            vec![
                Requirement::CurrentSchemaIs { schema_id: 1 },
                Requirement::SchemaIdAbsent { schema_id: 2 },
                Requirement::FieldIdsAbsent { field_ids: vec![2] },
            ],
            vec![
                OperationRequest::AddSchema {
                    operation_id: "add-schema".into(),
                    schema: next,
                },
                OperationRequest::SetCurrentSchema {
                    operation_id: "current-schema".into(),
                    schema_id: 2,
                },
            ],
        ))
        .await
        .unwrap();
    let c = append(&table, source.path(), "C", "main", 2).await;
    assert_eq!((c.table_version, c.sequence_number), (7, 3));
    for version in 0..=7 {
        let pinned = table
            .pin_metadata(MetadataSelection::TableVersion(version))
            .await
            .unwrap();
        assert_eq!(pinned.coordinates().table_version, version);
        assert_eq!(pinned.anchor().table_version, 7);
        let expected = match version {
            0 => 0,
            1..=3 => 1,
            4..=6 => 2,
            _ => 3,
        };
        assert_eq!(
            pinned
                .resolve_snapshot(SnapshotSelection::Ref("main".into()))
                .unwrap()
                .files()
                .unwrap()
                .len(),
            expected
        );
    }
    let branch = append(&table, source.path(), "D", "audit", 2).await;
    assert_eq!(branch.sequence_number, 4);
    assert_eq!(
        table
            .pin()
            .await
            .unwrap()
            .resolve_snapshot(SnapshotSelection::SnapshotId(branch.snapshot_id))
            .unwrap()
            .descriptor()
            .unwrap()
            .parent_snapshot_id,
        Some(b.snapshot_id)
    );
    assert_eq!(table.pin().await.unwrap().files("main").unwrap().len(), 3);
    let report = table
        .verify_with_report(otmp::VerificationScope::RetainedHistory)
        .await
        .unwrap();
    assert_eq!(report.generations_checked, 9);
    assert_eq!(report.commits_checked, 9);
    assert_eq!(before.status().table_version, 1);
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
    let snapshot = append(&table, source.path(), "root", "empty", 1).await;
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
    assert!(pin.files("main").unwrap().is_empty());
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
async fn failed_operation_order_and_schema_changes_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let table = Table::new(LocalObjectStore::new(dir.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let before = table.pin().await.unwrap().status();
    let mut request = property("atomic", CanonicalValue::Bool(true));
    request
        .requirements
        .push(Requirement::CurrentSchemaIs { schema_id: 1 });
    request.operations.push(OperationRequest::SetCurrentSchema {
        operation_id: "missing".into(),
        schema_id: 42,
    });
    assert!(table.transact(&request).await.is_err());
    assert_eq!(table.pin().await.unwrap().status(), before);
    for mutation in 0..6 {
        let mut next = schema();
        next.schema_id = 2;
        next.parent_schema_id = Some(1);
        match mutation {
            0 => next.fields[0].name = "renamed".into(),
            1 => next.fields.clear(),
            2 => next.fields[0].required = false,
            3 => next.fields[0].field_type = otmp_protocol::LogicalType::String,
            4 => next.identifier_field_ids.clear(),
            _ => next.fields.push(otmp_protocol::Field {
                field_id: 2,
                name: "required".into(),
                required: true,
                field_type: otmp_protocol::LogicalType::String,
                doc: None,
                initial_default: None,
                write_default: None,
            }),
        }
        let request = metadata(
            "invalid-schema",
            vec![
                Requirement::CurrentSchemaIs { schema_id: 1 },
                Requirement::SchemaIdAbsent { schema_id: 2 },
                Requirement::FieldIdsAbsent {
                    field_ids: if mutation == 5 { vec![2] } else { vec![] },
                },
            ],
            vec![OperationRequest::AddSchema {
                operation_id: "schema".into(),
                schema: next,
            }],
        );
        assert!(table.transact(&request).await.is_err());
        assert_eq!(table.pin().await.unwrap().status(), before);
    }
}
#[test]
fn manifest_requires_property_value_and_explicit_nullable_ref_target() {
    assert!(
        serde_json::from_str::<Requirement>(r#"{"type":"property_is","key":"owner"}"#).is_err()
    );
    assert!(
        serde_json::from_str::<Requirement>(r#"{"type":"ref_snapshot_is","ref":"main"}"#).is_err()
    );
    assert!(
        serde_json::from_str::<OperationRequest>(
            r#"{"type":"commit_snapshot","operation_id":"append"}"#
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<OperationRequest>(
            r#"{"type":"upgrade_features","operation_id":"features","add":[]}"#
        )
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
async fn verification_checks_historical_bytes_only_in_retained_scope() {
    let dir = tempfile::tempdir().unwrap();
    let source = tempfile::NamedTempFile::new().unwrap();
    let table = Table::new(LocalObjectStore::new(dir.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    table
        .transact(&metadata(
            "branch",
            vec![Requirement::RefAbsent {
                name: "temporary".into(),
            }],
            vec![OperationRequest::CreateRef {
                operation_id: "create".into(),
                name: "temporary".into(),
                ref_type: otmp::RefType::Branch,
                snapshot_id: None,
            }],
        ))
        .await
        .unwrap();
    let snapshot = append(&table, source.path(), "only-historical", "temporary", 1).await;
    table
        .transact(&metadata(
            "drop",
            vec![
                Requirement::RefExists {
                    name: "temporary".into(),
                    ref_type: otmp::RefType::Branch,
                },
                Requirement::RefSnapshotIs {
                    name: "temporary".into(),
                    snapshot_id: Some(snapshot.snapshot_id),
                },
            ],
            vec![OperationRequest::DropRef {
                operation_id: "drop".into(),
                name: "temporary".into(),
            }],
        ))
        .await
        .unwrap();
    std::fs::write(dir.path().join("unreachable-orphan"), b"not OTMP").unwrap();
    table.verify_history().await.unwrap();
    std::fs::remove_file(dir.path().join(snapshot.files[0].uri.as_str())).unwrap();
    table.verify().await.unwrap();
    assert!(matches!(
        table.verify_history().await,
        Err(otmp::RuntimeError::Storage(otmp::StorageError::NotFound(_)))
    ));
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

#[tokio::test]
async fn malformed_multi_operation_requests_preserve_the_base() {
    let table = Table::new(otmp::InMemoryObjectStore::default());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let before = table.pin().await.unwrap().status();
    for case in 0..5 {
        let mut request = property("invalid", CanonicalValue::Bool(true));
        match case {
            0 => request.operations.push(request.operations[0].clone()),
            1 => {
                if let OperationRequest::SetProperties { operation_id, .. } =
                    &mut request.operations[0]
                {
                    operation_id.clear();
                }
            }
            2 => {
                if let OperationRequest::SetProperties { removals, .. } = &mut request.operations[0]
                {
                    *removals = vec!["other".into(), "other".into()];
                }
            }
            3 => {
                if let OperationRequest::SetProperties { removals, .. } = &mut request.operations[0]
                {
                    removals.push("owner".into());
                }
            }
            _ => request.requirements.push(request.requirements[0].clone()),
        }
        assert!(table.transact(&request).await.is_err());
        assert_eq!(table.pin().await.unwrap().status(), before);
    }
}

#[tokio::test]
async fn tag_is_readable_but_cannot_be_replaced_or_appended() {
    let dir = tempfile::tempdir().unwrap();
    let source = tempfile::NamedTempFile::new().unwrap();
    let table = Table::new(LocalObjectStore::new(dir.path()).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let snapshot = append(&table, source.path(), "A", "main", 1).await;
    table
        .transact(&metadata(
            "tag",
            vec![
                Requirement::RefAbsent {
                    name: "release".into(),
                },
                Requirement::SnapshotExists {
                    snapshot_id: snapshot.snapshot_id,
                },
            ],
            vec![OperationRequest::CreateRef {
                operation_id: "tag".into(),
                name: "release".into(),
                ref_type: otmp::RefType::Tag,
                snapshot_id: Some(snapshot.snapshot_id),
            }],
        ))
        .await
        .unwrap();
    let before = table.pin().await.unwrap().status();
    let change = metadata(
        "replace",
        vec![
            Requirement::RefExists {
                name: "release".into(),
                ref_type: otmp::RefType::Tag,
            },
            Requirement::RefSnapshotIs {
                name: "release".into(),
                snapshot_id: Some(snapshot.snapshot_id),
            },
            Requirement::SnapshotExists {
                snapshot_id: snapshot.snapshot_id,
            },
        ],
        vec![OperationRequest::ReplaceRef {
            operation_id: "replace".into(),
            name: "release".into(),
            snapshot_id: snapshot.snapshot_id,
        }],
    );
    assert!(table.transact(&change).await.is_err());
    let mut request = otmp::AppendRequest::new("tag-append", vec![]);
    request.target_ref = "release".into();
    // A real source descriptor ensures rejection is about the target ref.
    request.files.push(otmp::AppendFile {
        source_path: source.path().into(),
        fingerprint: otmp::SourceFingerprint {
            sha256: otmp_protocol::Sha256::digest(b"A"),
            length: 1,
        },
        format: otmp::FileFormat::Parquet,
        record_count: 1,
        schema_id: 1,
        partition_spec_id: 0,
        sort_order_id: 0,
        partition_values: std::collections::BTreeMap::default(),
        metrics: vec![],
        metadata: std::collections::BTreeMap::default(),
    });
    assert!(table.append_files(&request).await.is_err());
    assert_eq!(table.pin().await.unwrap().status(), before);
    table.verify_history().await.unwrap();
}
