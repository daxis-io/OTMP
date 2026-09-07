use otmp_protocol::{
    JsonU64, PageMapBranch, PageMapNode, PageObjectReference, Sha256, decode_page_map,
    encode_page_map,
};

#[test]
fn internal_node_has_byte_exact_core_deterministic_cbor() {
    let node = PageMapNode::Internal {
        level: 1,
        entries: vec![PageMapBranch {
            max_page: 1,
            child: PageObjectReference {
                uri: "a".parse().unwrap(),
                sha256: Sha256::from_bytes([0; 32]),
                length: JsonU64(1),
            },
        }],
    };
    let expected = hex::decode(concat!(
        "a4656c6576656c0167656e747269657381a2656368696c64a3637572696161",
        "666c656e67746801667368613235365820",
        "0000000000000000000000000000000000000000000000000000000000000000",
        "686d61785f70616765016776657273696f6e01696e6f64655f7479706568696e7465726e616c"
    ))
    .unwrap();
    assert_eq!(encode_page_map(&node).unwrap(), expected);
    assert_eq!(decode_page_map(&expected).unwrap(), node);
    for end in 0..expected.len() {
        assert!(decode_page_map(&expected[..end]).is_err());
    }
    let mut noncanonical = expected;
    noncanonical.splice(7..8, [0x18, 0x01]);
    assert!(decode_page_map(&noncanonical).is_err());
}

#[test]
fn duplicate_and_unknown_leaf_encodings_are_rejected() {
    use otmp_protocol::{PageCodec, PageMapEntry};
    let entry = |page_number| PageMapEntry {
        page_number,
        pack: PageObjectReference {
            uri: "pack".parse().unwrap(),
            sha256: Sha256::from_bytes([0; 32]),
            length: JsonU64(8192),
        },
        offset: 128,
        stored_length: 4096,
        raw_length: 4096,
        codec: PageCodec::None,
        page_sha256: Sha256::from_bytes([1; 32]),
    };
    let node = PageMapNode::Leaf {
        entries: vec![entry(1), entry(2)],
    };
    let mut raw = encode_page_map(&node).unwrap();
    let key = b"page_number";
    let position = raw.windows(key.len()).rposition(|w| w == key).unwrap() + key.len();
    raw[position] = 1;
    assert!(decode_page_map(&raw).is_err());
    let mut raw = encode_page_map(&node).unwrap();
    raw.extend_from_slice(&[0]);
    assert!(decode_page_map(&raw).is_err());
    assert!(
        encode_page_map(&PageMapNode::Leaf {
            entries: vec![entry(0)]
        })
        .is_err()
    );
}
