use crate::{Id, Sha256};

#[must_use]
pub fn object_hash(bytes: &[u8]) -> Sha256 {
    Sha256::digest(bytes)
}

#[must_use]
pub fn intent_hash(canonical_intent: &[u8]) -> Sha256 {
    Sha256::digest([b"OTMP-INTENT\0".as_slice(), canonical_intent].concat())
}

#[must_use]
pub fn genesis_state_hash(canonical_genesis_body: &[u8]) -> Sha256 {
    Sha256::digest([b"OTMP-GENESIS\0".as_slice(), canonical_genesis_body].concat())
}

#[must_use]
pub fn next_state_hash(previous: Sha256, canonical_commit_body: &[u8]) -> Sha256 {
    Sha256::digest(
        [
            b"OTMP-STATE\0".as_slice(),
            previous.as_bytes(),
            canonical_commit_body,
        ]
        .concat(),
    )
}

#[must_use]
pub fn partition_hash(partition_spec_id: u32, tuple_cbor: &[u8]) -> Sha256 {
    Sha256::digest(
        [
            b"OTMP-PARTITION\0".as_slice(),
            &partition_spec_id.to_be_bytes(),
            tuple_cbor,
        ]
        .concat(),
    )
}

#[must_use]
pub fn image_root_hash(
    table_id: Id,
    table_version: u64,
    page_size: u32,
    page_count: u64,
    checkpoint: Sha256,
    page_map_root: Option<Sha256>,
) -> Sha256 {
    let page_map = page_map_root.map_or([0; 32], |hash| *hash.as_bytes());
    Sha256::digest(
        [
            b"OTMP-SQLITE-IMAGE\0".as_slice(),
            table_id.as_bytes(),
            &table_version.to_be_bytes(),
            &page_size.to_be_bytes(),
            &page_count.to_be_bytes(),
            checkpoint.as_bytes(),
            &page_map,
        ]
        .concat(),
    )
}
