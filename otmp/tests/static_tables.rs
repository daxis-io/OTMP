use otmp::{LocalObjectStore, Table};

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
}
