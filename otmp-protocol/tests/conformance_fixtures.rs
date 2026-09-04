use std::str::FromStr;

use otmp_protocol::{
    Id, Sha256, TypedScalar, canonical_json, decode_typed_scalar, encode_typed_scalar,
    genesis_state_hash, image_root_hash, intent_hash, next_state_hash, partition_hash,
};

#[test]
fn language_neutral_hash_fixtures_match() {
    let fixture: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../conformance/hashes/golden.json")).unwrap();
    let partition = &fixture["partition_empty_spec_0"];
    assert_eq!(
        partition_hash(
            0,
            &hex::decode(partition["tuple_cbor_hex"].as_str().unwrap()).unwrap()
        ),
        Sha256::from_str(partition["sha256"].as_str().unwrap()).unwrap()
    );
    let intent = &fixture["intent"];
    assert_eq!(
        intent_hash(intent["canonical_body"].as_str().unwrap().as_bytes()),
        Sha256::from_str(intent["sha256"].as_str().unwrap()).unwrap()
    );
    for name in [
        "metadata_split_intent",
        "metadata_split_commit_changed",
        "metadata_split_snapshot_changed",
    ] {
        let split_intent = &fixture[name];
        let split_body = split_intent["canonical_body"].as_str().unwrap();
        canonical_json::parse_canonical(split_body.as_bytes()).unwrap();
        assert_eq!(
            intent_hash(split_body.as_bytes()),
            Sha256::from_str(split_intent["sha256"].as_str().unwrap()).unwrap(),
            "{name}"
        );
    }
    let genesis = &fixture["genesis_state"];
    let genesis_hash = genesis_state_hash(genesis["canonical_body"].as_str().unwrap().as_bytes());
    assert_eq!(
        genesis_hash,
        Sha256::from_str(genesis["sha256"].as_str().unwrap()).unwrap()
    );
    let next = &fixture["next_state"];
    assert_eq!(
        next_state_hash(
            genesis_hash,
            next["canonical_body"].as_str().unwrap().as_bytes()
        ),
        Sha256::from_str(next["sha256"].as_str().unwrap()).unwrap()
    );
    let image = &fixture["image_root"];
    let table_id = Id::from_bytes(
        hex::decode(image["table_id_raw_hex"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap(),
    );
    assert_eq!(
        image_root_hash(
            table_id,
            image["table_version"].as_u64().unwrap(),
            u32::try_from(image["page_size"].as_u64().unwrap()).unwrap(),
            image["page_count"].as_u64().unwrap(),
            Sha256::from_str(image["checkpoint_sha256"].as_str().unwrap()).unwrap(),
            None,
        ),
        Sha256::from_str(image["sha256"].as_str().unwrap()).unwrap()
    );
}

#[test]
fn canonical_json_fixtures_have_expected_acceptance() {
    let canonical = include_bytes!("../../conformance/canonical-json/canonical.json");
    let canonical = canonical.strip_suffix(b"\n").unwrap_or(canonical);
    canonical_json::parse_canonical(canonical).unwrap();
    assert!(
        canonical_json::parse_canonical(include_bytes!(
            "../../conformance/canonical-json/noncanonical-whitespace.json"
        ))
        .is_err()
    );
    assert!(
        canonical_json::parse(include_bytes!(
            "../../conformance/canonical-json/invalid-duplicate.json"
        ))
        .is_err()
    );
    assert!(
        canonical_json::parse(include_bytes!(
            "../../conformance/canonical-json/invalid-float.json"
        ))
        .is_err()
    );
}

#[test]
fn language_neutral_typed_scalar_cbor_fixtures_match() {
    let fixture: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../conformance/cbor/typed-scalars.json"))
            .unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let value_bytes = canonical_json::to_vec(&case["value"]).unwrap();
        let value: TypedScalar = canonical_json::from_slice_canonical(&value_bytes).unwrap();
        let expected = hex::decode(case["cbor_hex"].as_str().unwrap()).unwrap();
        assert_eq!(encode_typed_scalar(&value), expected, "{}", case["name"]);
        let decoded = decode_typed_scalar(&expected).unwrap();
        assert_eq!(decoded, value, "{}", case["name"]);
    }
}
