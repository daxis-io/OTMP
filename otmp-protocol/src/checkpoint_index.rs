use super::{Decoder, array_len, bytes, invalid, map_len, text, unsigned};
use crate::{JsonU64, PageObjectReference, ProtocolError, Sha256};

pub const CHECKPOINT_PAGE_INDEX_MEDIA_TYPE: &str =
    "application/vnd.otmp.checkpoint-page-index+cbor";
pub const CHECKPOINT_INDEX_CAPACITY: usize = 128;
pub const MAX_CHECKPOINT_INDEX_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointIndexChild {
    pub first_page: u64,
    pub page_count: u64,
    pub child: PageObjectReference,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckpointIndexNode {
    Leaf {
        first_page: u64,
        hashes: Vec<Sha256>,
    },
    Internal {
        level: u32,
        entries: Vec<CheckpointIndexChild>,
    },
}

impl CheckpointIndexNode {
    #[must_use]
    pub fn level(&self) -> u32 {
        match self {
            Self::Leaf { .. } => 0,
            Self::Internal { level, .. } => *level,
        }
    }
    #[must_use]
    pub fn first_page(&self) -> u64 {
        match self {
            Self::Leaf { first_page, .. } => *first_page,
            Self::Internal { entries, .. } => entries.first().map_or(0, |entry| entry.first_page),
        }
    }
    #[must_use]
    pub fn page_count(&self) -> u64 {
        match self {
            Self::Leaf { hashes, .. } => hashes.len() as u64,
            Self::Internal { entries, .. } => entries.iter().map(|entry| entry.page_count).sum(),
        }
    }
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Leaf { first_page, hashes } => {
                if *first_page == 0 || hashes.is_empty() || hashes.len() > CHECKPOINT_INDEX_CAPACITY
                {
                    return Err(invalid("invalid checkpoint-index leaf interval"));
                }
                first_page
                    .checked_add(hashes.len() as u64 - 1)
                    .ok_or_else(|| invalid("checkpoint-index leaf interval overflow"))?;
            }
            Self::Internal { level, entries } => {
                if *level == 0 || entries.is_empty() || entries.len() > CHECKPOINT_INDEX_CAPACITY {
                    return Err(invalid("invalid checkpoint-index internal level or count"));
                }
                let mut expected = None;
                for entry in entries {
                    if entry.first_page == 0
                        || entry.page_count == 0
                        || entry.child.length.0 == 0
                        || entry.child.length.0 > MAX_CHECKPOINT_INDEX_BYTES as u64
                        || expected.is_some_and(|page| page != entry.first_page)
                    {
                        return Err(invalid(
                            "noncontiguous checkpoint-index interval or reference",
                        ));
                    }
                    expected = Some(
                        entry
                            .first_page
                            .checked_add(entry.page_count)
                            .ok_or_else(|| invalid("checkpoint-index interval overflow"))?,
                    );
                }
            }
        }
        Ok(())
    }
}

fn encode_ref(reference: &PageObjectReference, output: &mut Vec<u8>) {
    map_len(3, output);
    text("uri", output);
    text(reference.uri.as_str(), output);
    text("length", output);
    unsigned(reference.length.0, output);
    text("sha256", output);
    bytes(reference.sha256.as_bytes(), output);
}
fn key(decoder: &mut Decoder<'_>, expected: &str) -> Result<(), ProtocolError> {
    if decoder.text()? != expected {
        return Err(invalid("unexpected or noncanonical checkpoint-index key"));
    }
    Ok(())
}
fn hash(decoder: &mut Decoder<'_>) -> Result<Sha256, ProtocolError> {
    Ok(Sha256::from_bytes(
        decoder
            .byte_string()?
            .try_into()
            .map_err(|_| invalid("checkpoint-index hash must be 32 raw bytes"))?,
    ))
}
fn reference(decoder: &mut Decoder<'_>) -> Result<PageObjectReference, ProtocolError> {
    if decoder.length(5)? != 3 {
        return Err(invalid("invalid checkpoint-index reference fields"));
    }
    key(decoder, "uri")?;
    let uri = decoder.text()?.parse()?;
    key(decoder, "length")?;
    let length = JsonU64(decoder.unsigned()?);
    key(decoder, "sha256")?;
    let sha256 = hash(decoder)?;
    Ok(PageObjectReference {
        uri,
        sha256,
        length,
    })
}
fn u32_value(decoder: &mut Decoder<'_>) -> Result<u32, ProtocolError> {
    u32::try_from(decoder.unsigned()?).map_err(|_| invalid("checkpoint-index integer overflow"))
}

pub fn encode_checkpoint_index(node: &CheckpointIndexNode) -> Result<Vec<u8>, ProtocolError> {
    node.validate()?;
    let mut output = Vec::new();
    match node {
        CheckpointIndexNode::Leaf { first_page, hashes } => {
            map_len(4, &mut output);
            text("hashes", &mut output);
            array_len(hashes.len(), &mut output);
            for hash in hashes {
                bytes(hash.as_bytes(), &mut output);
            }
            text("version", &mut output);
            unsigned(1, &mut output);
            text("node_type", &mut output);
            text("leaf", &mut output);
            text("first_page", &mut output);
            unsigned(*first_page, &mut output);
        }
        CheckpointIndexNode::Internal { level, entries } => {
            map_len(4, &mut output);
            text("level", &mut output);
            unsigned(u64::from(*level), &mut output);
            text("entries", &mut output);
            array_len(entries.len(), &mut output);
            for entry in entries {
                map_len(3, &mut output);
                text("child", &mut output);
                encode_ref(&entry.child, &mut output);
                text("page_count", &mut output);
                unsigned(entry.page_count, &mut output);
                text("first_page", &mut output);
                unsigned(entry.first_page, &mut output);
            }
            text("version", &mut output);
            unsigned(1, &mut output);
            text("node_type", &mut output);
            text("internal", &mut output);
        }
    }
    if output.len() > MAX_CHECKPOINT_INDEX_BYTES {
        return Err(invalid("checkpoint-index node exceeds 1 MiB"));
    }
    Ok(output)
}

pub fn decode_checkpoint_index(input: &[u8]) -> Result<CheckpointIndexNode, ProtocolError> {
    if input.len() > MAX_CHECKPOINT_INDEX_BYTES {
        return Err(invalid("checkpoint-index node exceeds 1 MiB"));
    }
    let d = &mut Decoder::new(input);
    let fields = d.length(5)?;
    if fields != 4 {
        return Err(invalid("invalid checkpoint-index node fields"));
    }
    let first_key = d.text()?;
    let node = if first_key == "hashes" {
        let count = d.length(4)?;
        if count == 0 || count > CHECKPOINT_INDEX_CAPACITY || count > input.len() {
            return Err(invalid("invalid checkpoint-index leaf count"));
        }
        let mut hashes = Vec::with_capacity(count);
        for _ in 0..count {
            hashes.push(hash(d)?);
        }
        key(d, "version")?;
        if d.unsigned()? != 1 {
            return Err(invalid("unsupported checkpoint-index version"));
        }
        key(d, "node_type")?;
        if d.text()? != "leaf" {
            return Err(invalid("invalid checkpoint-index node type"));
        }
        key(d, "first_page")?;
        CheckpointIndexNode::Leaf {
            first_page: d.unsigned()?,
            hashes,
        }
    } else if first_key == "level" {
        let level = u32_value(d)?;
        key(d, "entries")?;
        let count = d.length(4)?;
        if count == 0 || count > CHECKPOINT_INDEX_CAPACITY || count > input.len() {
            return Err(invalid("invalid checkpoint-index internal count"));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            if d.length(5)? != 3 {
                return Err(invalid("invalid checkpoint-index child fields"));
            }
            key(d, "child")?;
            let child = reference(d)?;
            key(d, "page_count")?;
            let page_count = d.unsigned()?;
            key(d, "first_page")?;
            let first_page = d.unsigned()?;
            entries.push(CheckpointIndexChild {
                first_page,
                page_count,
                child,
            });
        }
        key(d, "version")?;
        if d.unsigned()? != 1 {
            return Err(invalid("unsupported checkpoint-index version"));
        }
        key(d, "node_type")?;
        if d.text()? != "internal" {
            return Err(invalid("invalid checkpoint-index node type"));
        }
        CheckpointIndexNode::Internal { level, entries }
    } else {
        return Err(invalid("unexpected or noncanonical checkpoint-index key"));
    };
    d.finish()?;
    node.validate()?;
    Ok(node)
}
