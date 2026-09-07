use otmp_protocol::{decode_pack_index, encode_page_pack};
use std::collections::BTreeMap;

#[test]
fn pack_has_exact_header_index_and_payload() {
    let pages = BTreeMap::from([(7, vec![0; 512])]);
    let bytes = encode_page_pack(512, &pages).unwrap();
    assert_eq!(bytes.len(), 640);
    assert_eq!(
        &bytes[..40],
        &hex::decode(
            "4f544d505047504b0001000000000000000002000000000100000000000000400000000000000080"
        )
        .unwrap()
    );
    assert!(bytes[40..64].iter().all(|b| *b == 0));
    assert_eq!(
        &bytes[64..96],
        &hex::decode("0000000000000007000000000000008000000200000002000000000000000000").unwrap()
    );
    assert_eq!(
        &bytes[96..128],
        &hex::decode("076a27c79e5ace2a3d47f9dd2e83e4ff6ea8872b3c2218f66c92b89b55f36560").unwrap()
    );
    assert_eq!(decode_pack_index(&bytes).unwrap().entries[0].page_number, 7);
    for end in 0..bytes.len() {
        assert!(decode_pack_index(&bytes[..end]).is_err());
    }
    let mut corrupted = bytes;
    corrupted[79] = 127;
    assert!(decode_pack_index(&corrupted).is_err());
}

#[test]
fn pack_rejects_duplicate_pages_overlap_reserved_bytes_and_unknown_codec() {
    let pages = BTreeMap::from([(1, vec![1; 512]), (2, vec![2; 512])]);
    let raw = encode_page_pack(512, &pages).unwrap();
    for mutation in ["duplicate", "overlap", "reserved", "codec", "length"] {
        let mut bytes = raw.clone();
        match mutation {
            "duplicate" => bytes[135] = 1,
            "overlap" => {
                let offset: Vec<_> = bytes[72..80].into();
                bytes[136..144].copy_from_slice(&offset);
            }
            "reserved" => bytes[40] = 1,
            "codec" => bytes[88] = 2,
            "length" => bytes[84] = 255,
            _ => unreachable!(),
        }
        assert!(decode_pack_index(&bytes).is_err(), "accepted {mutation}");
    }
}
