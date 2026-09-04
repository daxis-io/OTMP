use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use otmp_protocol::{
    COMMIT_MEDIA_TYPE, CanonicalValue, FeatureSet, Field, GENERATION_MEDIA_TYPE, Head, Id,
    IntentRecord, JsonI64, JsonU64, LogicalType, ObjectReference, ProtocolError, RelativeUri,
    Schema, SemanticCommit, Sha256, TypedScalar, UuidValue, canonical_json, decode_partition_tuple,
    decode_typed_scalar, encode_partition_tuple, encode_typed_scalar, image_root_hash,
};

fn object<const N: usize>(entries: [(&str, CanonicalValue); N]) -> CanonicalValue {
    CanonicalValue::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    )
}

fn string(value: &str) -> CanonicalValue {
    CanonicalValue::String(value.to_owned())
}

fn valid_snapshot_commit() -> SemanticCommit {
    let operation = object([
        ("operation_id", string("append-main")),
        ("type", string("commit_snapshot")),
        ("target_ref", string("main")),
        (
            "snapshot",
            object([
                (
                    "snapshot_id",
                    string("018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0c"),
                ),
                ("parent_snapshot_id", CanonicalValue::Null),
                ("sequence_number", string("1")),
                ("schema_id", string("1")),
                ("partition_spec_id", string("0")),
                ("sort_order_id", string("0")),
                ("operation", string("append")),
                ("summary", object([])),
                ("metadata", object([])),
            ]),
        ),
        ("added_files", CanonicalValue::Array(Vec::new())),
        ("removed_file_ids", CanonicalValue::Array(Vec::new())),
        ("scan_projection", CanonicalValue::Null),
        ("rebase_mode", string("append-safe")),
    ]);
    SemanticCommit {
        kind: "otmp.semantic-commit".into(),
        format_version: 1,
        table_id: "018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0a".parse().unwrap(),
        table_version: JsonU64(1),
        parent_table_version: Some(JsonU64(0)),
        commit_id: "018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0b".parse().unwrap(),
        parent_commit: Some(ObjectReference {
            uri: "_otmp/commits/0/parent.json".parse().unwrap(),
            sha256: Sha256::digest(b"parent"),
            length: None,
            media_type: Some(COMMIT_MEDIA_TYPE.into()),
        }),
        created_at_ms: JsonI64(1),
        intents: vec![IntentRecord {
            key: "append".into(),
            intent_sha256: Sha256::digest(b"intent"),
            operation_ids: vec!["append-main".into()],
            result: object([]),
        }],
        requirements: Vec::new(),
        operations: vec![operation],
        required_reader_features_after_commit: FeatureSet::new(vec!["otmp.core.v2".into()])
            .unwrap(),
        required_writer_features_after_commit: FeatureSet::new(vec!["otmp.core.v2".into()])
            .unwrap(),
        previous_semantic_state_sha256: Some(Sha256::digest(b"previous")),
        semantic_state_sha256: Sha256::digest(b"current"),
        metadata: object([]),
    }
}

fn valid_gate1_snapshot_commit() -> SemanticCommit {
    let mut commit = valid_snapshot_commit();
    let CanonicalValue::Object(operation) = &mut commit.operations[0] else {
        unreachable!();
    };
    operation.insert(
        "added_files".into(),
        CanonicalValue::Array(vec![object([
            ("file_id", string("018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0d")),
            ("uri", string("data/file.parquet")),
            ("object_identity", CanonicalValue::Null),
            ("file_format", string("parquet")),
            ("file_size_bytes", string("10")),
            ("record_count", string("1")),
            ("schema_id", string("1")),
            ("partition_spec_id", string("0")),
            ("sort_order_id", string("0")),
            (
                "content_sha256",
                string(&Sha256::digest(b"file").to_string()),
            ),
            ("partition_values", object([])),
            ("metrics", CanonicalValue::Array(Vec::new())),
            ("metadata", object([])),
        ])]),
    );
    commit
}

#[test]
fn canonical_json_sorts_keys_and_uses_minimal_encoding() {
    let value = canonical_json::parse(br#"{"z":false,"a":"\u0061","n":1}"#).unwrap();
    assert_eq!(
        canonical_json::encode(&value).unwrap(),
        br#"{"a":"a","n":1,"z":false}"#
    );
}

#[test]
fn canonical_json_rejects_duplicates_and_floats() {
    assert!(matches!(
        canonical_json::parse(br#"{"a":1,"a":2}"#),
        Err(ProtocolError::DuplicateJsonKey(key)) if key == "a"
    ));
    assert!(matches!(
        canonical_json::parse(br#"{"a":1.25}"#),
        Err(ProtocolError::FloatingPointJson)
    ));
}

#[test]
fn canonical_decode_rejects_noncanonical_input() {
    assert!(matches!(
        canonical_json::parse_canonical(br#"{"z":0, "a":1}"#),
        Err(ProtocolError::NonCanonicalJson)
    ));
}

#[test]
fn identifiers_hashes_features_and_relative_uris_are_strict() {
    let id = Id::from_str("018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0a").unwrap();
    assert_eq!(id.to_string(), "018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0a");
    assert!(Id::from_str("018F31F4-2BBD-7E47-A8BD-E5C9B36D8B0A").is_err());
    assert!(UuidValue::from_str("550e8400-e29b-41d4-a716-446655440000").is_ok());
    assert!(Id::from_str("550e8400-e29b-41d4-a716-446655440000").is_err());

    let hash = Sha256::digest(b"abc");
    assert_eq!(
        hash.to_string(),
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert!(Sha256::from_str("sha256:ABC").is_err());

    assert!(RelativeUri::from_str("data/a.parquet").is_ok());
    for unsafe_uri in [
        "/data/a",
        "../a",
        "data/../a",
        "data\\a",
        "data//a",
        "./a",
        "a/",
    ] {
        assert!(RelativeUri::from_str(unsafe_uri).is_err(), "{unsafe_uri}");
    }

    assert!(FeatureSet::new(vec!["a.v1".into(), "b.v1".into()]).is_ok());
    assert!(FeatureSet::new(vec!["b.v1".into(), "a.v1".into()]).is_err());
    assert!(FeatureSet::new(vec!["a.v1".into(), "a.v1".into()]).is_err());
}

#[test]
fn json_u64_is_a_decimal_string() {
    let value = JsonU64(42);
    assert_eq!(canonical_json::to_vec(&value).unwrap(), br#""42""#);
    assert!(serde_json::from_slice::<JsonU64>(b"42").is_err());
    assert!(serde_json::from_slice::<JsonU64>(br#""042""#).is_err());
}

#[test]
fn typed_scalar_json_never_uses_json_floating_point() {
    assert_eq!(
        canonical_json::to_vec(&TypedScalar::Int64(42)).unwrap(),
        br#"{"type":"int64","value":"42"}"#
    );
    assert_eq!(
        canonical_json::to_vec(&TypedScalar::Float32(-0.0)).unwrap(),
        br#"{"type":"float32","value":"0x80000000"}"#
    );
    assert!(
        canonical_json::from_slice::<TypedScalar>(br#"{"type":"float32","value":-0.0}"#).is_err()
    );
    assert!(serde_json::from_slice::<TypedScalar>(br#"{"type":"int64","value":"042"}"#).is_err());
    assert!(
        serde_json::from_slice::<TypedScalar>(br#"{"type":"time_micros","value":"86400000000"}"#)
            .is_err()
    );
}

#[test]
fn deterministic_typed_scalar_cbor_matches_golden_bytes() {
    assert_eq!(
        encode_typed_scalar(&TypedScalar::Null),
        vec![0x82, 0x00, 0xf6]
    );
    assert_eq!(
        encode_typed_scalar(&TypedScalar::Int64(42)),
        vec![0x82, 0x03, 0x18, 0x2a]
    );
    assert_eq!(
        encode_typed_scalar(&TypedScalar::String("hello".into())),
        b"\x82\x0b\x65hello"
    );
    assert_eq!(
        encode_typed_scalar(&TypedScalar::Float32(-0.0)),
        vec![0x82, 0x04, 0x44, 0x80, 0, 0, 0]
    );
}

#[test]
fn deterministic_cbor_decodes_and_rejects_noncanonical_forms() {
    let values = [
        TypedScalar::Null,
        TypedScalar::Boolean(true),
        TypedScalar::Int32(-24),
        TypedScalar::Int64(i64::MIN),
        TypedScalar::Float32(-0.0),
        TypedScalar::Float64(f64::NAN),
        TypedScalar::Decimal {
            precision: 9,
            scale: 2,
            unscaled: vec![0x01, 0x02],
        },
        TypedScalar::Date(-1),
        TypedScalar::TimeMicros(1),
        TypedScalar::TimestampMicros(-1),
        TypedScalar::TimestamptzMicros(2),
        TypedScalar::String("snowman ☃".into()),
        TypedScalar::Binary(vec![0, 1]),
        TypedScalar::Fixed(vec![2, 3]),
        TypedScalar::Uuid(UuidValue::from_bytes([7; 16])),
    ];
    for value in values {
        let encoded = encode_typed_scalar(&value);
        let decoded = decode_typed_scalar(&encoded).unwrap();
        if matches!(value, TypedScalar::Float64(number) if number.is_nan()) {
            assert!(matches!(decoded, TypedScalar::Float64(number) if number.is_nan()));
        } else {
            assert_eq!(decoded, value);
        }
    }
    assert!(decode_typed_scalar(&[0x82, 0x18, 0x03, 0x00]).is_err());
    assert!(decode_typed_scalar(&[0x9f, 0x03, 0x00, 0xff]).is_err());
    assert!(decode_typed_scalar(&[0x82, 0x08, 0x20]).is_err());

    let mut tuple = BTreeMap::new();
    tuple.insert(1, TypedScalar::Int32(7));
    tuple.insert(2, TypedScalar::String("x".into()));
    assert_eq!(
        decode_partition_tuple(&encode_partition_tuple(&tuple)).unwrap(),
        tuple
    );
    assert!(
        decode_partition_tuple(&[0xa2, 0x02, 0x82, 0x00, 0xf6, 0x01, 0x82, 0x00, 0xf6]).is_err()
    );
}

#[test]
fn partition_tuple_is_sorted_and_hashed_over_exact_cbor() {
    let mut tuple = BTreeMap::new();
    tuple.insert(2, TypedScalar::String("x".into()));
    tuple.insert(1, TypedScalar::Int32(7));
    let encoded = encode_partition_tuple(&tuple);
    assert_eq!(encoded[0], 0xa2);
    assert_eq!(
        otmp_protocol::partition_hash(0, &encoded),
        Sha256::digest(
            [
                b"OTMP-PARTITION\0".as_slice(),
                &0_u32.to_be_bytes(),
                encoded.as_slice()
            ]
            .concat()
        )
    );
}

#[test]
fn image_root_is_domain_separated_and_includes_zero_page_map_hash() {
    let table_id = Id::from_bytes([7; 16]);
    let checkpoint = Sha256::digest(b"checkpoint");
    let actual = image_root_hash(table_id, 3, 4096, 12, checkpoint, None);
    let expected = Sha256::digest(
        [
            b"OTMP-SQLITE-IMAGE\0".as_slice(),
            &[7; 16],
            &3_u64.to_be_bytes(),
            &4096_u32.to_be_bytes(),
            &12_u64.to_be_bytes(),
            checkpoint.as_bytes(),
            &[0; 32],
        ]
        .concat(),
    );
    assert_eq!(actual, expected);
    assert_ne!(actual, checkpoint);
}

#[test]
fn schema_validation_covers_nested_and_identifier_invariants() {
    let schema = Schema {
        schema_id: 1,
        parent_schema_id: None,
        fields: vec![Field {
            field_id: 1,
            name: "id".into(),
            required: true,
            field_type: LogicalType::Uuid,
            doc: None,
            initial_default: None,
            write_default: None,
        }],
        identifier_field_ids: vec![1],
        doc: None,
    };
    schema.validate().unwrap();

    let duplicate = Schema {
        fields: vec![schema.fields[0].clone(), schema.fields[0].clone()],
        ..schema.clone()
    };
    assert!(duplicate.validate().is_err());

    let optional_identifier = Schema {
        fields: vec![Field {
            required: false,
            ..schema.fields[0].clone()
        }],
        ..schema
    };
    assert!(optional_identifier.validate().is_err());
}

#[test]
#[allow(clippy::too_many_lines)]
fn initialization_schema_accepts_every_core_logical_type() {
    let primitive_types = vec![
        LogicalType::Boolean,
        LogicalType::Int32,
        LogicalType::Int64,
        LogicalType::Float32,
        LogicalType::Float64,
        LogicalType::Decimal {
            precision: 18,
            scale: 2,
        },
        LogicalType::Date,
        LogicalType::TimeMicros,
        LogicalType::TimestampMicros,
        LogicalType::TimestamptzMicros,
        LogicalType::String,
        LogicalType::Binary,
        LogicalType::Fixed { length: 16 },
        LogicalType::Uuid,
    ];
    let mut fields = primitive_types
        .into_iter()
        .enumerate()
        .map(|(index, field_type)| Field {
            field_id: u32::try_from(index + 1).unwrap(),
            name: format!("primitive_{index}"),
            required: index == 1,
            field_type,
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .collect::<Vec<_>>();
    fields.extend([
        Field {
            field_id: 20,
            name: "struct_value".into(),
            required: false,
            field_type: LogicalType::Struct {
                fields: vec![Field {
                    field_id: 21,
                    name: "child".into(),
                    required: false,
                    field_type: LogicalType::String,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                }],
            },
            doc: None,
            initial_default: None,
            write_default: None,
        },
        Field {
            field_id: 30,
            name: "list_value".into(),
            required: false,
            field_type: LogicalType::List {
                element: Box::new(Field {
                    field_id: 31,
                    name: "element".into(),
                    required: false,
                    field_type: LogicalType::Int64,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                }),
            },
            doc: None,
            initial_default: None,
            write_default: None,
        },
        Field {
            field_id: 40,
            name: "map_value".into(),
            required: false,
            field_type: LogicalType::Map {
                key: Box::new(Field {
                    field_id: 41,
                    name: "key".into(),
                    required: true,
                    field_type: LogicalType::String,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                }),
                value: Box::new(Field {
                    field_id: 42,
                    name: "value".into(),
                    required: false,
                    field_type: LogicalType::Binary,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                }),
            },
            doc: None,
            initial_default: None,
            write_default: None,
        },
    ]);
    Schema {
        schema_id: 1,
        parent_schema_id: None,
        fields,
        identifier_field_ids: vec![2],
        doc: None,
    }
    .validate()
    .unwrap();
}

#[test]
fn protocol_objects_reject_unknown_fields_noncanonical_integers_and_unsafe_refs() {
    let id = Id::from_str("018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0a").unwrap();
    let hash = Sha256::digest(b"x");
    let head = Head {
        protocol: "otmp".into(),
        protocol_version: "0.0.2-alpha".into(),
        table_id: id,
        table_version: JsonU64(0),
        root_revision: JsonU64(0),
        semantic_state_sha256: hash,
        semantic_commit: ObjectReference {
            uri: "_otmp/commits/0/a.json".parse().unwrap(),
            sha256: hash,
            length: None,
            media_type: Some(COMMIT_MEDIA_TYPE.into()),
        },
        metadata_generation: ObjectReference {
            uri: "_otmp/generations/0/a.json".parse().unwrap(),
            sha256: hash,
            length: None,
            media_type: Some(GENERATION_MEDIA_TYPE.into()),
        },
        required_reader_features: FeatureSet::new(vec!["otmp.core.v2".into()]).unwrap(),
        required_writer_features: FeatureSet::new(vec!["otmp.core.v2".into()]).unwrap(),
    };
    head.validate(&BTreeSet::from(["otmp.core.v2"])).unwrap();
    let mut unknown_feature = head.clone();
    unknown_feature.required_reader_features = FeatureSet::new(vec!["unknown.v1".into()]).unwrap();
    assert!(
        unknown_feature
            .validate(&BTreeSet::from(["otmp.core.v2"]))
            .is_err()
    );
    let mut value = canonical_json::to_value(&head).unwrap();
    if let CanonicalValue::Object(fields) = &mut value {
        fields.insert("unknown".into(), CanonicalValue::Null);
    }
    assert!(
        canonical_json::from_slice_canonical::<Head>(&canonical_json::encode(&value).unwrap())
            .is_err()
    );
    if let CanonicalValue::Object(fields) = &mut value {
        fields.remove("unknown");
        fields.insert("table_version".into(), CanonicalValue::Integer(0));
    }
    assert!(
        canonical_json::from_slice_canonical::<Head>(&canonical_json::encode(&value).unwrap())
            .is_err()
    );
    if let CanonicalValue::Object(fields) = &mut value {
        fields.insert(
            "required_reader_features".into(),
            CanonicalValue::Array(vec![
                CanonicalValue::String("z.v1".into()),
                CanonicalValue::String("a.v1".into()),
            ]),
        );
    }
    assert!(
        canonical_json::from_slice_canonical::<Head>(&canonical_json::encode(&value).unwrap())
            .is_err()
    );
}

#[test]
fn canonical_value_rejects_integer_overflow() {
    let parsed = canonical_json::parse(br#"{"n":18446744073709551616}"#);
    assert!(matches!(parsed, Err(ProtocolError::IntegerOutOfRange)));
    let value = CanonicalValue::Object(BTreeMap::new());
    assert_eq!(canonical_json::encode(&value).unwrap(), b"{}");
}

#[test]
fn semantic_commit_rejects_malformed_commit_snapshot_shapes() {
    let valid = valid_snapshot_commit();
    valid.validate().unwrap();

    let mut non_object_metadata = valid.clone();
    non_object_metadata.metadata = CanonicalValue::Null;
    assert!(non_object_metadata.validate().is_err());

    let mut missing_snapshot = valid.clone();
    let CanonicalValue::Object(operation) = &mut missing_snapshot.operations[0] else {
        unreachable!();
    };
    operation.remove("snapshot");
    assert!(missing_snapshot.validate().is_err());

    let mut flat_snapshot = valid.clone();
    let CanonicalValue::Object(operation) = &mut flat_snapshot.operations[0] else {
        unreachable!();
    };
    operation.remove("snapshot");
    operation.insert(
        "snapshot_id".into(),
        string("018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0c"),
    );
    assert!(flat_snapshot.validate().is_err());

    let mut missing_snapshot_metadata = valid.clone();
    let CanonicalValue::Object(operation) = &mut missing_snapshot_metadata.operations[0] else {
        unreachable!();
    };
    let Some(CanonicalValue::Object(snapshot)) = operation.get_mut("snapshot") else {
        unreachable!();
    };
    snapshot.remove("metadata");
    assert!(missing_snapshot_metadata.validate().is_err());

    let mut wrong_snapshot_version = valid;
    let CanonicalValue::Object(operation) = &mut wrong_snapshot_version.operations[0] else {
        unreachable!();
    };
    let Some(CanonicalValue::Object(snapshot)) = operation.get_mut("snapshot") else {
        unreachable!();
    };
    snapshot.insert("sequence_number".into(), string("01"));
    assert!(wrong_snapshot_version.validate().is_err());

    let mut malformed_file = valid_snapshot_commit();
    let CanonicalValue::Object(operation) = &mut malformed_file.operations[0] else {
        unreachable!();
    };
    operation.insert(
        "added_files".into(),
        CanonicalValue::Array(vec![object([])]),
    );
    assert!(malformed_file.validate().is_err());
}

#[test]
fn semantic_commit_rejects_a_malformed_initialize_operation() {
    let mut commit = valid_snapshot_commit();
    commit.table_version = JsonU64(0);
    commit.parent_table_version = None;
    commit.parent_commit = None;
    commit.previous_semantic_state_sha256 = None;
    commit.operations = vec![object([
        ("operation_id", string("append-main")),
        ("type", string("initialize_table")),
    ])];

    assert!(commit.validate().is_err());
}

#[test]
fn semantic_commit_rejects_initial_snapshots_and_unqualified_unknown_operations() {
    let mut genesis: SemanticCommit = canonical_json::from_slice_canonical(include_bytes!(
        "../../conformance/tables/genesis/_otmp/commits/0/01a067c2-4891-7c40-9557-0edcdf176cee.json"
    ))
    .unwrap();
    let valid_snapshot = valid_snapshot_commit();
    let CanonicalValue::Object(snapshot_operation) = &valid_snapshot.operations[0] else {
        unreachable!();
    };
    let snapshot = snapshot_operation.get("snapshot").unwrap().clone();
    let CanonicalValue::Object(initialize) = &mut genesis.operations[0] else {
        unreachable!();
    };
    initialize.insert("snapshot".into(), snapshot);
    assert!(genesis.validate().is_err());

    for operation_type in ["", "commit_snapsh0t", "set_properties"] {
        let mut commit = valid_snapshot_commit();
        let CanonicalValue::Object(operation) = &mut commit.operations[0] else {
            unreachable!();
        };
        operation.insert("type".into(), string(operation_type));
        assert!(commit.validate().is_err(), "accepted {operation_type:?}");
    }

    let mut extension = valid_snapshot_commit();
    let CanonicalValue::Object(operation) = &mut extension.operations[0] else {
        unreachable!();
    };
    operation.insert("type".into(), string("com.example.extension"));
    extension.validate().unwrap();
    assert!(extension.validate_gate1().is_err());
}

#[test]
fn gate1_semantic_validation_rejects_malformed_file_descriptors() {
    let valid = valid_gate1_snapshot_commit();
    valid.validate_gate1().unwrap();

    for (field, value) in [
        ("uri", string("../escape.parquet")),
        ("file_format", string("orc")),
        ("file_size_bytes", string("9223372036854775808")),
        ("schema_id", string("2")),
        ("content_sha256", string("not-a-hash")),
    ] {
        let mut malformed = valid.clone();
        let CanonicalValue::Object(operation) = &mut malformed.operations[0] else {
            unreachable!();
        };
        let Some(CanonicalValue::Array(files)) = operation.get_mut("added_files") else {
            unreachable!();
        };
        let CanonicalValue::Object(file) = &mut files[0] else {
            unreachable!();
        };
        file.insert(field.into(), value);
        assert!(malformed.validate_gate1().is_err(), "accepted {field}");
    }
}
