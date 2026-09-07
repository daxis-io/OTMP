use otmp::{InitializeRequest, LocalObjectStore, ObjectStore, Table};
use otmp_protocol::{Generation, Head, Schema, canonical_json};

fn schema() -> Schema {
    serde_json::from_slice(include_bytes!("../../conformance/sources/schema.json")).unwrap()
}

#[tokio::test]
async fn genesis_publishes_an_authenticated_checkpoint_page_index() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(directory.path()).unwrap();
    let table = Table::new(store.clone());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let head: Head = canonical_json::from_slice_canonical(
        &store
            .read(&"_otmp/HEAD".parse().unwrap())
            .await
            .unwrap()
            .bytes,
    )
    .unwrap();
    let generation: Generation = canonical_json::from_slice_canonical(
        &store
            .read(&head.metadata_generation.uri)
            .await
            .unwrap()
            .bytes,
    )
    .unwrap();
    let index = generation
        .metadata_image
        .checkpoint_page_index
        .as_ref()
        .unwrap();
    assert_eq!(index.page_count, generation.metadata_image.page_count);
    assert_eq!(
        index.checkpoint_sha256,
        generation.metadata_image.checkpoint.sha256
    );
    table.verify().await.unwrap();
}
