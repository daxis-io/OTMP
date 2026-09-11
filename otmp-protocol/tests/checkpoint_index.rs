use otmp_protocol::{
    Checkpoint, CheckpointIndexChild, CheckpointIndexNode, CheckpointPageIndex, JsonU64,
    PageMapRoot, PageObjectReference, Sha256, decode_checkpoint_index, encode_checkpoint_index,
};

fn reference(name: &str, byte: u8) -> PageObjectReference {
    PageObjectReference {
        uri: name.parse().unwrap(),
        sha256: Sha256::from_bytes([byte; 32]),
        length: JsonU64(100),
    }
}

#[test]
fn checkpoint_index_nodes_have_deterministic_cbor_and_contiguous_intervals() {
    let leaf = CheckpointIndexNode::Leaf {
        first_page: 1,
        hashes: vec![Sha256::from_bytes([1; 32]), Sha256::from_bytes([2; 32])],
    };
    let bytes = encode_checkpoint_index(&leaf).unwrap();
    assert_eq!(decode_checkpoint_index(&bytes).unwrap(), leaf);
    for end in 0..bytes.len() {
        assert!(decode_checkpoint_index(&bytes[..end]).is_err());
    }

    let internal = CheckpointIndexNode::Internal {
        level: 1,
        entries: vec![
            CheckpointIndexChild {
                first_page: 1,
                page_count: 2,
                child: reference("a", 3),
            },
            CheckpointIndexChild {
                first_page: 3,
                page_count: 4,
                child: reference("b", 4),
            },
        ],
    };
    assert_eq!(
        decode_checkpoint_index(&encode_checkpoint_index(&internal).unwrap()).unwrap(),
        internal
    );
}

#[test]
fn checkpoint_index_rejects_gaps_oversized_leaves_and_noncanonical_encoding() {
    assert!(
        encode_checkpoint_index(&CheckpointIndexNode::Leaf {
            first_page: 0,
            hashes: vec![Sha256::from_bytes([0; 32])],
        })
        .is_err()
    );
    assert!(
        encode_checkpoint_index(&CheckpointIndexNode::Leaf {
            first_page: 1,
            hashes: vec![Sha256::from_bytes([0; 32]); 129],
        })
        .is_err()
    );
    assert!(
        encode_checkpoint_index(&CheckpointIndexNode::Internal {
            level: 1,
            entries: vec![
                CheckpointIndexChild {
                    first_page: 1,
                    page_count: 1,
                    child: reference("a", 1)
                },
                CheckpointIndexChild {
                    first_page: 3,
                    page_count: 1,
                    child: reference("b", 2)
                },
            ],
        })
        .is_err()
    );
    let mut trailing = encode_checkpoint_index(&CheckpointIndexNode::Leaf {
        first_page: 1,
        hashes: vec![Sha256::from_bytes([9; 32])],
    })
    .unwrap();
    trailing.push(0);
    assert!(decode_checkpoint_index(&trailing).is_err());
}

#[test]
fn checkpoint_page_index_rejects_an_unbounded_root_height() {
    let checkpoint = Checkpoint {
        table_version: JsonU64(0),
        uri: "checkpoint".parse().unwrap(),
        sha256: Sha256::from_bytes([1; 32]),
        length: JsonU64(4096),
    };
    let index = CheckpointPageIndex {
        checkpoint_sha256: checkpoint.sha256,
        checkpoint_length: checkpoint.length,
        page_size: 4096,
        page_count: JsonU64(1),
        root: PageMapRoot {
            uri: "root".parse().unwrap(),
            sha256: Sha256::from_bytes([2; 32]),
            length: JsonU64(10),
            height: 65,
        },
    };
    assert!(index.validate(&checkpoint, 4096).is_err());
}
