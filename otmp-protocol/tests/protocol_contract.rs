use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use otmp_protocol::{
    COMMIT_MEDIA_TYPE, CanonicalValue, FeatureSet, Field, GENERATION_MEDIA_TYPE, Head, Id, JsonU64,
    LogicalType, ObjectReference, ProtocolError, RelativeUri, Schema, Sha256, TypedScalar,
    UuidValue, canonical_json, decode_partition_tuple, decode_typed_scalar, encode_partition_tuple,
    encode_typed_scalar, image_root_hash,
};

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
