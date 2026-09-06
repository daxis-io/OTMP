use otmp::{
    CommitMetadata, InMemoryObjectStore, InitializeRequest, MetadataSelection, OperationRequest,
    Requirement, Table, TransactionRequest,
};
use otmp_protocol::CanonicalValue;

#[tokio::test]
async fn historical_pin_reads_each_parent_object_once() {
    let store = InMemoryObjectStore::default();
    let table = Table::new(store.clone());
    let schema =
        serde_json::from_slice(include_bytes!("../../conformance/sources/schema.json")).unwrap();
    table
        .initialize(InitializeRequest::new(schema))
        .await
        .unwrap();
    for index in 0..4 {
        table
            .transact(&TransactionRequest {
                idempotency_key: format!("property-{index}"),
                requirements: vec![Requirement::PropertyIs {
                    key: format!("owner-{index}"),
                    value: CanonicalValue::Null,
                }],
                operations: vec![OperationRequest::SetProperties {
                    operation_id: "set".into(),
                    updates: [(format!("owner-{index}"), CanonicalValue::Bool(true))].into(),
                    removals: vec![],
                }],
                commit_metadata: CommitMetadata::default(),
            })
            .await
            .unwrap();

        let before = store.read_count();
        table
            .pin_metadata(MetadataSelection::TableVersion(0))
            .await
            .unwrap();
        assert_eq!(store.read_count() - before, 4 + 3 * (index + 1));
    }
}
