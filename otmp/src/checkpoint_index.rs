//! Deterministic authenticated indexes for immutable `SQLite` checkpoints.

use crate::{RuntimeError, physical::Artifact};
use otmp_protocol::{
    CHECKPOINT_INDEX_CAPACITY, Checkpoint, CheckpointIndexChild, CheckpointIndexNode,
    CheckpointPageIndex, JsonU64, PageMapRoot, PageObjectReference, RelativeUri, Sha256,
    encode_checkpoint_index, object_hash,
};

fn corrupt(message: &str) -> RuntimeError {
    RuntimeError::Corrupt(message.into())
}

fn reference(bytes: &[u8]) -> Result<(PageObjectReference, Artifact), RuntimeError> {
    let sha256 = object_hash(bytes);
    let uri: RelativeUri = format!(
        "_otmp/checkpoint-page-index/{}.cbor",
        hex::encode(sha256.as_bytes())
    )
    .parse()?;
    Ok((
        PageObjectReference {
            uri: uri.clone(),
            sha256,
            length: JsonU64(bytes.len() as u64),
        },
        Artifact {
            uri,
            bytes: bytes.to_vec(),
        },
    ))
}

/// Builds a content-addressed 128-way tree over every physical checkpoint page.
pub(crate) fn build(
    checkpoint: &Checkpoint,
    page_size: u32,
    bytes: &[u8],
) -> Result<(CheckpointPageIndex, Vec<Artifact>), RuntimeError> {
    let page_size_usize = usize::try_from(page_size)
        .map_err(|_| corrupt("checkpoint page size exceeds platform size"))?;
    if page_size == 0
        || bytes.is_empty()
        || !bytes.len().is_multiple_of(page_size_usize)
        || object_hash(bytes) != checkpoint.sha256
        || bytes.len() as u64 != checkpoint.length.0
    {
        return Err(corrupt("invalid checkpoint bytes for page index"));
    }
    let page_count = bytes.len() / page_size_usize;
    let mut artifacts = Vec::new();
    let mut current = Vec::new();
    let leaf_bytes = page_size_usize
        .checked_mul(CHECKPOINT_INDEX_CAPACITY)
        .ok_or_else(|| corrupt("checkpoint index leaf size overflow"))?;
    for (chunk, pages) in bytes.chunks(leaf_bytes).enumerate() {
        let node = CheckpointIndexNode::Leaf {
            first_page: (chunk * CHECKPOINT_INDEX_CAPACITY + 1) as u64,
            hashes: pages.chunks(page_size_usize).map(Sha256::digest).collect(),
        };
        let encoded = encode_checkpoint_index(&node)?;
        let (reference, artifact) = reference(&encoded)?;
        current.push((node.first_page(), node.page_count(), reference));
        artifacts.push(artifact);
    }
    let mut height = 0;
    while current.len() > 1 {
        height += 1;
        let mut next = Vec::new();
        for group in current.chunks(CHECKPOINT_INDEX_CAPACITY) {
            let node = CheckpointIndexNode::Internal {
                level: height,
                entries: group
                    .iter()
                    .map(|(first_page, page_count, child)| CheckpointIndexChild {
                        first_page: *first_page,
                        page_count: *page_count,
                        child: child.clone(),
                    })
                    .collect(),
            };
            let encoded = encode_checkpoint_index(&node)?;
            let (reference, artifact) = reference(&encoded)?;
            next.push((node.first_page(), node.page_count(), reference));
            artifacts.push(artifact);
        }
        current = next;
    }
    let (_, indexed_pages, root) = current
        .pop()
        .ok_or_else(|| corrupt("empty checkpoint page index"))?;
    if indexed_pages != page_count as u64 {
        return Err(corrupt("checkpoint page index coverage mismatch"));
    }
    let root = PageMapRoot {
        uri: root.uri,
        sha256: root.sha256,
        length: root.length,
        height,
    };
    let index = CheckpointPageIndex {
        checkpoint_sha256: checkpoint.sha256,
        checkpoint_length: checkpoint.length,
        page_size,
        page_count: JsonU64(page_count as u64),
        root,
    };
    index.validate(checkpoint, page_size)?;
    Ok((index, artifacts))
}
