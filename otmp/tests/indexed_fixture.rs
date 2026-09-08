use otmp::{LocalObjectStore, MetadataSelection, Table};

#[tokio::test]
async fn deterministic_indexed_fixture_verifies_and_retains_history() {
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/tables/indexed");
    let table = Table::new(LocalObjectStore::new(root).unwrap());
    table.verify().await.unwrap();
    for version in 0..=2 {
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
}
