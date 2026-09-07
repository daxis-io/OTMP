use super::{Decoder, array_len, bytes, invalid, map_len, text, unsigned};
use crate::{JsonU64, ProtocolError, RelativeUri, Sha256};
use serde::{Deserialize, Serialize};

pub const PAGE_MAP_MEDIA_TYPE: &str = "application/vnd.otmp.page-map+cbor";
pub const PAGE_PACK_MEDIA_TYPE: &str = "application/vnd.otmp.page-pack";
pub const MAX_PAGE_MAP_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageObjectReference {
    pub uri: RelativeUri,
    pub sha256: Sha256,
    pub length: JsonU64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageMapRoot {
    pub uri: RelativeUri,
    pub sha256: Sha256,
    pub length: JsonU64,
    pub height: u32,
}

impl PageMapRoot {
    #[must_use]
    pub fn reference(&self) -> PageObjectReference {
        PageObjectReference {
            uri: self.uri.clone(),
            sha256: self.sha256,
            length: self.length,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageCodec {
    None,
    Zstd,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageMapEntry {
    pub page_number: u64,
    pub pack: PageObjectReference,
    pub offset: u64,
    pub stored_length: u32,
    pub raw_length: u32,
    pub codec: PageCodec,
    pub page_sha256: Sha256,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageMapBranch {
    pub max_page: u64,
    pub child: PageObjectReference,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PageMapNode {
    Leaf {
        entries: Vec<PageMapEntry>,
    },
    Internal {
        level: u32,
        entries: Vec<PageMapBranch>,
    },
}

impl PageMapNode {
    #[must_use]
    pub fn level(&self) -> u32 {
        match self {
            Self::Leaf { .. } => 0,
            Self::Internal { level, .. } => *level,
        }
    }
    #[must_use]
    pub fn max_page(&self) -> Option<u64> {
        match self {
            Self::Leaf { entries } => entries.last().map(|e| e.page_number),
            Self::Internal { entries, .. } => entries.last().map(|e| e.max_page),
        }
    }
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let mut previous = 0;
        let mut check = |page, reference: &PageObjectReference| {
            if page <= previous || reference.length.0 == 0 {
                return Err(invalid("invalid page-map ordering or reference length"));
            }
            previous = page;
            Ok(())
        };
        match self {
            Self::Leaf { entries } => {
                if entries.is_empty() {
                    return Err(invalid("empty page-map node"));
                }
                for entry in entries {
                    check(entry.page_number, &entry.pack)?;
                    if entry.stored_length == 0
                        || !(512..=65536).contains(&entry.raw_length)
                        || !entry.raw_length.is_power_of_two()
                        || entry
                            .offset
                            .checked_add(u64::from(entry.stored_length))
                            .is_none_or(|end| end > entry.pack.length.0)
                        || entry.codec == PageCodec::None && entry.stored_length != entry.raw_length
                    {
                        return Err(invalid("invalid page-map payload range"));
                    }
                }
            }
            Self::Internal { level, entries } => {
                if *level == 0 || entries.is_empty() {
                    return Err(invalid("invalid internal page-map level or count"));
                }
                for entry in entries {
                    check(entry.max_page, &entry.child)?;
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

pub fn encode_page_map(node: &PageMapNode) -> Result<Vec<u8>, ProtocolError> {
    node.validate()?;
    let mut output = Vec::new();
    match node {
        PageMapNode::Leaf { entries } => {
            map_len(3, &mut output);
            text("entries", &mut output);
            array_len(entries.len(), &mut output);
            for entry in entries {
                map_len(7, &mut output);
                text("pack", &mut output);
                encode_ref(&entry.pack, &mut output);
                text("codec", &mut output);
                text(
                    match entry.codec {
                        PageCodec::None => "none",
                        PageCodec::Zstd => "zstd",
                    },
                    &mut output,
                );
                text("offset", &mut output);
                unsigned(entry.offset, &mut output);
                text("raw_length", &mut output);
                unsigned(u64::from(entry.raw_length), &mut output);
                text("page_number", &mut output);
                unsigned(entry.page_number, &mut output);
                text("page_sha256", &mut output);
                bytes(entry.page_sha256.as_bytes(), &mut output);
                text("stored_length", &mut output);
                unsigned(u64::from(entry.stored_length), &mut output);
            }
            text("version", &mut output);
            unsigned(1, &mut output);
            text("node_type", &mut output);
            text("leaf", &mut output);
        }
        PageMapNode::Internal { level, entries } => {
            map_len(4, &mut output);
            text("level", &mut output);
            unsigned(u64::from(*level), &mut output);
            text("entries", &mut output);
            array_len(entries.len(), &mut output);
            for entry in entries {
                map_len(2, &mut output);
                text("child", &mut output);
                encode_ref(&entry.child, &mut output);
                text("max_page", &mut output);
                unsigned(entry.max_page, &mut output);
            }
            text("version", &mut output);
            unsigned(1, &mut output);
            text("node_type", &mut output);
            text("internal", &mut output);
        }
    }
    if output.len() > MAX_PAGE_MAP_BYTES {
        return Err(invalid("page-map node exceeds 1 MiB"));
    }
    Ok(output)
}

fn key(decoder: &mut Decoder<'_>, expected: &str) -> Result<(), ProtocolError> {
    if decoder.text()? != expected {
        return Err(invalid("unexpected or noncanonical page-map key"));
    }
    Ok(())
}
fn hash(decoder: &mut Decoder<'_>) -> Result<Sha256, ProtocolError> {
    Ok(Sha256::from_bytes(
        decoder
            .byte_string()?
            .try_into()
            .map_err(|_| invalid("page-map hash must be 32 raw bytes"))?,
    ))
}
fn reference(decoder: &mut Decoder<'_>) -> Result<PageObjectReference, ProtocolError> {
    if decoder.length(5)? != 3 {
        return Err(invalid("invalid page-map reference fields"));
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
fn uint32(decoder: &mut Decoder<'_>) -> Result<u32, ProtocolError> {
    u32::try_from(decoder.unsigned()?).map_err(|_| invalid("page-map integer overflow"))
}

pub fn decode_page_map(input: &[u8]) -> Result<PageMapNode, ProtocolError> {
    if input.len() > MAX_PAGE_MAP_BYTES {
        return Err(invalid("page-map node exceeds 1 MiB"));
    }
    let d = &mut Decoder::new(input);
    let fields = d.length(5)?;
    let level = if fields == 4 {
        key(d, "level")?;
        uint32(d)?
    } else if fields == 3 {
        0
    } else {
        return Err(invalid("invalid page-map node fields"));
    };
    key(d, "entries")?;
    let count = d.length(4)?;
    // Each entry consumes at least one byte; reject malicious lengths before allocation.
    if count == 0 || count > input.len() {
        return Err(invalid("invalid page-map entry count"));
    }
    let node = if fields == 3 {
        let mut entries = Vec::new();
        for _ in 0..count {
            if d.length(5)? != 7 {
                return Err(invalid("invalid page-map leaf fields"));
            }
            key(d, "pack")?;
            let pack = reference(d)?;
            key(d, "codec")?;
            let codec = match d.text()? {
                "none" => PageCodec::None,
                "zstd" => PageCodec::Zstd,
                _ => return Err(invalid("unknown page codec")),
            };
            key(d, "offset")?;
            let offset = d.unsigned()?;
            key(d, "raw_length")?;
            let raw_length = uint32(d)?;
            key(d, "page_number")?;
            let page_number = d.unsigned()?;
            key(d, "page_sha256")?;
            let page_sha256 = hash(d)?;
            key(d, "stored_length")?;
            let stored_length = uint32(d)?;
            entries.push(PageMapEntry {
                page_number,
                pack,
                offset,
                stored_length,
                raw_length,
                codec,
                page_sha256,
            });
        }
        PageMapNode::Leaf { entries }
    } else {
        let mut entries = Vec::new();
        for _ in 0..count {
            if d.length(5)? != 2 {
                return Err(invalid("invalid page-map branch fields"));
            }
            key(d, "child")?;
            let child = reference(d)?;
            key(d, "max_page")?;
            let max_page = d.unsigned()?;
            entries.push(PageMapBranch { max_page, child });
        }
        PageMapNode::Internal { level, entries }
    };
    key(d, "version")?;
    if d.unsigned()? != 1 {
        return Err(invalid("unsupported page-map version"));
    }
    key(d, "node_type")?;
    if d.text()? != if fields == 3 { "leaf" } else { "internal" } {
        return Err(invalid("invalid page-map node type"));
    }
    d.finish()?;
    node.validate()?;
    Ok(node)
}
