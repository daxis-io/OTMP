use otmp::{LocalObjectStore, Table};
use otmp_protocol::{CanonicalValue, Head, SemanticCommit, canonical_json};

#[tokio::test]
async fn static_genesis_and_append_packages_remain_readable() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/tables");
    let genesis = Table::new(LocalObjectStore::new(root.join("genesis")).unwrap());
    genesis.verify().await.unwrap();
    assert_eq!(genesis.pin().await.unwrap().status().table_version, 0);

    let append = Table::new(LocalObjectStore::new(root.join("append")).unwrap());
    append.verify().await.unwrap();
    assert_eq!(append.pin().await.unwrap().status().table_version, 1);
    assert_eq!(append.pin().await.unwrap().files("main").unwrap().len(), 1);

    let append_root = root.join("append");
    let head: Head = canonical_json::from_slice_canonical(
        &tokio::fs::read(append_root.join("_otmp/HEAD"))
            .await
            .unwrap(),
    )
    .unwrap();
    let commit: SemanticCommit = canonical_json::from_slice_canonical(
        &tokio::fs::read(append_root.join(head.semantic_commit.uri.as_str()))
            .await
            .unwrap(),
    )
    .unwrap();
    let CanonicalValue::Object(operation) = &commit.operations[0] else {
        panic!("commit_snapshot operation must be an object");
    };
    let Some(CanonicalValue::Object(snapshot)) = operation.get("snapshot") else {
        panic!("commit_snapshot snapshot must be an object");
    };
    assert_eq!(
        commit.metadata,
        canonical_json::parse(br#"{"io.daxis.otmp.fixture":{"scope":"commit"}}"#).unwrap()
    );
    assert_eq!(
        snapshot.get("metadata"),
        Some(&canonical_json::parse(br#"{"io.daxis.otmp.fixture":{"scope":"snapshot"}}"#).unwrap())
    );
}

#[tokio::test]
async fn canonical_transaction_package_retains_exact_version_sequence_and_refs() {
    use otmp::{MetadataSelection, SnapshotSelection};
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/tables/transactions");
    let table = Table::new(LocalObjectStore::new(root).unwrap());
    for version in 0..=7 {
        let pin = table
            .pin_metadata(MetadataSelection::TableVersion(version))
            .await
            .unwrap();
        let main = pin
            .resolve_snapshot(SnapshotSelection::Ref("main".into()))
            .unwrap();
        let sequence = main.descriptor().map_or(0, |d| d.sequence_number);
        assert_eq!(
            sequence,
            match version {
                0 => 0,
                1..=3 => 1,
                4..=6 => 2,
                _ => 3,
            }
        );
        if let Some(descriptor) = main.descriptor() {
            assert_eq!(descriptor.schema_id, if version == 7 { 2 } else { 1 });
        }
        assert_eq!(pin.anchor().table_version, 7);
        let audit = pin.resolve_snapshot(SnapshotSelection::Ref("audit".into()));
        if version < 3 {
            assert!(audit.is_err());
        } else {
            assert_eq!(
                audit.unwrap().descriptor().unwrap().sequence_number,
                if version < 5 { 1 } else { 2 }
            );
        }
    }
    let report = table
        .verify_with_report(otmp::VerificationScope::RetainedHistory)
        .await
        .unwrap();
    assert_eq!(report.generations_checked, 8);
    assert_eq!(report.commits_checked, 8);
    assert_eq!(report.snapshots_checked, 3);
}
