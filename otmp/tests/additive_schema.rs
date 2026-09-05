use otmp::{
    CommitMetadata, InMemoryObjectStore, InitializeRequest, OperationRequest, Requirement, Table,
    TransactionRequest,
};
use otmp_protocol::Schema;
use serde_json::{Value, json};

fn request(schema: Schema, ids: Vec<u32>) -> TransactionRequest {
    let schema_id = schema.schema_id;
    TransactionRequest {
        idempotency_key: format!("schema-{}", schema.schema_id),
        requirements: vec![
            Requirement::CurrentSchemaIs {
                schema_id: schema.parent_schema_id.unwrap(),
            },
            Requirement::SchemaIdAbsent { schema_id },
            Requirement::FieldIdsAbsent { field_ids: ids },
        ],
        operations: vec![
            OperationRequest::AddSchema {
                operation_id: "add".into(),
                schema,
            },
            OperationRequest::SetCurrentSchema {
                operation_id: "select".into(),
                schema_id,
            },
        ],
        commit_metadata: CommitMetadata::default(),
    }
}
fn field(id: u32, name: &str, kind: Value) -> Value {
    let mut value = json!({"field_id":id,"name":name,"required":false});
    value["type"] = kind;
    value
}

#[tokio::test]
async fn optional_fields_can_extend_existing_map_value_and_list_element_structs() {
    for collection in ["map", "list"] {
        let element = field(
            4,
            "element",
            json!({"type":"struct","fields":[field(5,"old",json!({"type":"string"}))]}),
        );
        let kind = if collection == "map" {
            json!({"type":"map","key":{"field_id":3,"name":"key","required":true,"type":{"type":"string"}},"value":element})
        } else {
            json!({"type":"list","element":element})
        };
        let mut value = json!({"schema_id":1,"fields":[{"field_id":1,"name":"id","required":true,"type":{"type":"int64"}},field(2,"collection",kind)],"identifier_field_ids":[1]});
        let table = Table::new(InMemoryObjectStore::default());
        table
            .initialize(InitializeRequest::new(
                serde_json::from_value(value.clone()).unwrap(),
            ))
            .await
            .unwrap();
        value["schema_id"] = json!(2);
        value["parent_schema_id"] = json!(1);
        let child = if collection == "map" {
            "value"
        } else {
            "element"
        };
        value["fields"][1]["type"][child]["type"]["fields"]
            .as_array_mut()
            .unwrap()
            .push(field(6, "new", json!({"type":"string"})));
        let result = table
            .transact(&request(serde_json::from_value(value).unwrap(), vec![6]))
            .await
            .unwrap();
        assert_eq!(result.table_version, 1);
        table.verify_history().await.unwrap();
    }
}

#[tokio::test]
async fn schema_reorder_id_reuse_and_wrong_parent_leave_head_unchanged() {
    let original = json!({"schema_id":1,"fields":[field(1,"first",json!({"type":"int64"})),field(2,"second",json!({"type":"string"}))],"identifier_field_ids":[]});
    let table = Table::new(InMemoryObjectStore::default());
    table
        .initialize(InitializeRequest::new(
            serde_json::from_value(original.clone()).unwrap(),
        ))
        .await
        .unwrap();
    let before = table.pin().await.unwrap().status();
    for mutation in 0..5 {
        let mut value = original.clone();
        value["schema_id"] = json!(2);
        value["parent_schema_id"] = json!(1);
        let ids = match mutation {
            0 => {
                value["fields"].as_array_mut().unwrap().swap(0, 1);
                vec![]
            }
            1 => {
                value["schema_id"] = json!(1);
                vec![]
            }
            2 => {
                value["parent_schema_id"] = json!(99);
                vec![]
            }
            3 => {
                value["fields"].as_array_mut().unwrap().push(field(
                    2,
                    "reuse",
                    json!({"type":"string"}),
                ));
                vec![2]
            }
            _ => {
                value["fields"][0]["type"] = json!({"type":"float64"});
                vec![]
            }
        };
        assert!(
            table
                .transact(&request(serde_json::from_value(value).unwrap(), ids))
                .await
                .is_err()
        );
        assert_eq!(table.pin().await.unwrap().status(), before);
    }
}

#[tokio::test]
async fn field_ids_are_global_even_after_selecting_an_older_schema() {
    let original = json!({"schema_id":1,"fields":[field(1,"first",json!({"type":"int64"}))],"identifier_field_ids":[]});
    let table = Table::new(InMemoryObjectStore::default());
    table
        .initialize(InitializeRequest::new(
            serde_json::from_value(original.clone()).unwrap(),
        ))
        .await
        .unwrap();
    let mut next = original.clone();
    next["schema_id"] = json!(2);
    next["parent_schema_id"] = json!(1);
    next["fields"]
        .as_array_mut()
        .unwrap()
        .push(field(2, "second", json!({"type":"string"})));
    table
        .transact(&request(serde_json::from_value(next).unwrap(), vec![2]))
        .await
        .unwrap();
    table
        .transact(&TransactionRequest {
            idempotency_key: "select-old".into(),
            requirements: vec![Requirement::CurrentSchemaIs { schema_id: 2 }],
            operations: vec![OperationRequest::SetCurrentSchema {
                operation_id: "select".into(),
                schema_id: 1,
            }],
            commit_metadata: CommitMetadata::default(),
        })
        .await
        .unwrap();
    let before = table.pin().await.unwrap().status();
    let mut reused = original;
    reused["schema_id"] = json!(3);
    reused["parent_schema_id"] = json!(1);
    reused["fields"]
        .as_array_mut()
        .unwrap()
        .push(field(2, "reused", json!({"type":"string"})));
    assert!(
        table
            .transact(&request(serde_json::from_value(reused).unwrap(), vec![2]))
            .await
            .is_err()
    );
    assert_eq!(table.pin().await.unwrap().status(), before);
    table.verify_history().await.unwrap();
}
