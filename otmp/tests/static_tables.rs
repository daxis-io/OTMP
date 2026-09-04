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
