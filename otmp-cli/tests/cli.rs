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
            "commit_metadata": {
                "io.daxis.arco.catalog_coordination": {
                    "catalog_transaction_id": "txn-cli"
                }
            },
            "snapshot_metadata": {
                "com.example.pipeline": {
                    "source_watermark": "2026-09-03T18:00:00Z"
                }
            },
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
        serde_json::from_slice::<serde_json::Value>(&status).unwrap()["selected"]["table_version"],
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

#[test]
fn cli_rejects_legacy_application_metadata_with_migration_guidance() {
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
    let manifest = directory.path().join("legacy.json");
    std::fs::write(
        &manifest,
        serde_json::to_vec(&serde_json::json!({
            "idempotency_key": "legacy",
            "application_metadata": {"catalog_transaction_id": "old"},
            "files": [{
                "source": source,
                "sha256": "sha256:51e05457ab7bcdb3b32ea67f81a7701303d1d221f0b1c403c7abe86028361b2d",
                "length": 12,
                "record_count": 2,
                "schema_id": 1
            }]
        }))
        .unwrap(),
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_otmp"))
        .args([
            "append",
            table.to_str().unwrap(),
            "--manifest",
            manifest.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("application_metadata was replaced by commit_metadata")
            && stderr.contains("snapshot_metadata describes the immutable snapshot")
            && stderr.contains("is not copied automatically"),
        "{stderr}"
    );
}

#[test]
fn tracked_append_manifest_is_idempotent_with_static_package() {
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_otmp"))
        .current_dir(workspace)
        .args([
            "append",
            workspace
                .join("conformance/tables/append")
                .to_str()
                .unwrap(),
            "--manifest",
            workspace
                .join("conformance/sources/append-manifest.json")
                .to_str()
                .unwrap(),
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn omitted_and_empty_cli_metadata_have_one_intent_identity() {
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
    let fingerprint = success(&["inspect-file", source.to_str().unwrap()]);
    let fingerprint: serde_json::Value = serde_json::from_slice(&fingerprint).unwrap();
    let manifest = directory.path().join("append.json");
    let base = serde_json::json!({
        "idempotency_key": "empty-metadata",
        "files": [{
            "source": source,
            "sha256": fingerprint["sha256"],
            "length": fingerprint["length"],
            "record_count": 1,
            "schema_id": 1
        }]
    });
    std::fs::write(&manifest, serde_json::to_vec(&base).unwrap()).unwrap();
    let first = success(&[
        "append",
        table.to_str().unwrap(),
        "--manifest",
        manifest.to_str().unwrap(),
    ]);
    std::fs::remove_file(&source).unwrap();

    let mut explicit = base;
    explicit["commit_metadata"] = serde_json::json!({});
    explicit["snapshot_metadata"] = serde_json::json!({});
    std::fs::write(&manifest, serde_json::to_vec(&explicit).unwrap()).unwrap();
    let second = success(&[
        "append",
        table.to_str().unwrap(),
        "--manifest",
        manifest.to_str().unwrap(),
    ]);

    assert_eq!(first, second);
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

#[test]
fn metadata_manifest_historical_status_selectors_and_reports() {
    let directory = tempfile::tempdir().unwrap();
    let table = directory.path().join("table");
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    success(&[
        "init",
        table.to_str().unwrap(),
        "--schema",
        root.join("conformance/sources/schema.json")
            .to_str()
            .unwrap(),
    ]);
    let manifest = directory.path().join("transaction.json");
    std::fs::write(&manifest,r#"{"idempotency_key":"metadata","requirements":[{"type":"property_is","key":"owner","value":null}],"operations":[{"type":"set_properties","operation_id":"properties","updates":{"owner":"team"},"removals":[]}]}"#).unwrap();
    let result = success(&[
        "transact",
        table.to_str().unwrap(),
        "--manifest",
        manifest.to_str().unwrap(),
    ]);
    assert_eq!(
        success(&[
            "transact",
            table.to_str().unwrap(),
            "--manifest",
            manifest.to_str().unwrap()
        ]),
        result
    );
    let status: serde_json::Value = serde_json::from_slice(&success(&[
        "status",
        table.to_str().unwrap(),
        "--table-version",
        "0",
    ]))
    .unwrap();
    assert_eq!(status["anchor"]["table_version"], 1);
    assert_eq!(status["selected"]["table_version"], 0);
    assert!(status["selected"].get("root_revision").is_none());
    let report: serde_json::Value =
        serde_json::from_slice(&success(&["verify", table.to_str().unwrap(), "--history"]))
            .unwrap();
    assert_eq!(report["completed"], true);
    assert_eq!(report["generations_checked"], 2);
    let output = Command::new(env!("CARGO_BIN_EXE_otmp"))
        .args([
            "files",
            table.to_str().unwrap(),
            "--ref",
            "main",
            "--sequence-number",
            "1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stderr).unwrap()["code"],
        "OTMP_INVALID_ARGUMENT"
    );
    let files: serde_json::Value = serde_json::from_slice(&success(&[
        "files",
        table.to_str().unwrap(),
        "--table-version",
        "0",
    ]))
    .unwrap();
    assert_eq!(files, serde_json::json!([]));
}
