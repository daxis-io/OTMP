use std::process::Command;

#[test]
fn cli_initializes_inspects_appends_and_reads() {
    let directory = tempfile::tempdir().unwrap();
    let table = directory.path().join("table");
    let schema = directory.path().join("schema.json");
    let source = directory.path().join("data.parquet");
    std::fs::write(
        &schema,
        r#"{"schema_id":1,"fields":[{"field_id":1,"name":"id","required":true,"type":{"type":"int64"}}],"identifier_field_ids":[1]}"#,
    )
    .unwrap();
    std::fs::write(&source, b"PAR1dataPAR1").unwrap();

    success(&[
        "init",
        table.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
    ]);
    let inspect = success(&["inspect-file", source.to_str().unwrap()]);
    let fingerprint: serde_json::Value = serde_json::from_slice(&inspect).unwrap();
    let manifest = directory.path().join("append.json");
    std::fs::write(
        &manifest,
        serde_json::to_vec(&serde_json::json!({
            "idempotency_key": "cli-batch",
            "files": [{
                "source": source,
                "sha256": fingerprint["sha256"],
                "length": fingerprint["length"],
                "record_count": 2,
                "schema_id": 1,
                "partition_spec_id": 0,
                "sort_order_id": 0
            }]
        }))
        .unwrap(),
    )
    .unwrap();

    let append = success(&[
        "append",
        table.to_str().unwrap(),
        "--manifest",
        manifest.to_str().unwrap(),
    ]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&append).unwrap()["table_version"],
        "1"
    );
    let status = success(&["status", table.to_str().unwrap()]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&status).unwrap()["table_version"],
        1
    );
    let files = success(&["files", table.to_str().unwrap()]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&files)
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    success(&["history", table.to_str().unwrap()]);
    success(&["verify", table.to_str().unwrap()]);
}

fn success(arguments: &[&str]) -> Vec<u8> {
    let output = Command::new(env!("CARGO_BIN_EXE_otmp"))
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}
