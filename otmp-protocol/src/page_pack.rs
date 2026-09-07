use crate::{PageCodec, ProtocolError, Sha256};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackIndexEntry {
    pub page_number: u64,
    pub offset: u64,
    pub stored_length: u32,
    pub raw_length: u32,
    pub codec: PageCodec,
    pub page_sha256: Sha256,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackIndex {
    pub page_size: u32,
    pub entries: Vec<PackIndexEntry>,
}

fn invalid(message: &str) -> ProtocolError {
    ProtocolError::InvalidObject(message.into())
}
fn number<const N: usize>(bytes: &[u8], start: usize) -> Result<[u8; N], ProtocolError> {
    bytes
        .get(
            start
                ..start
                    .checked_add(N)
                    .ok_or_else(|| invalid("pack offset overflow"))?,
        )
        .ok_or_else(|| invalid("truncated pack"))?
        .try_into()
        .map_err(|_| invalid("truncated pack"))
}

pub fn decode_pack_index(bytes: &[u8]) -> Result<PackIndex, ProtocolError> {
    if bytes.len() < 64
        || &bytes[..8] != b"OTMPPGPK"
        || bytes[8..16] != [0, 1, 0, 0, 0, 0, 0, 0]
        || bytes[40..64].iter().any(|b| *b != 0)
    {
        return Err(invalid("invalid page-pack header"));
    }
    let page_size = u32::from_be_bytes(number(bytes, 16)?);
    if !(512..=65536).contains(&page_size) || !page_size.is_power_of_two() {
        return Err(invalid("invalid pack page size"));
    }
    let count = u32::from_be_bytes(number(bytes, 20)?) as usize;
    let index = usize::try_from(u64::from_be_bytes(number(bytes, 24)?))
        .map_err(|_| invalid("pack index overflow"))?;
    let payload = usize::try_from(u64::from_be_bytes(number(bytes, 32)?))
        .map_err(|_| invalid("pack payload overflow"))?;
    if count == 0
        || index < 64
        || payload > bytes.len()
        || count > bytes.len() / 64
        || index
            .checked_add(count * 64)
            .is_none_or(|end| end > payload)
    {
        return Err(invalid("invalid pack index bounds"));
    }
    let mut entries = Vec::with_capacity(count);
    let mut ranges = Vec::with_capacity(count);
    let mut previous = 0;
    for i in 0..count {
        let start = index + i * 64;
        let page_number = u64::from_be_bytes(number(bytes, start)?);
        let offset = u64::from_be_bytes(number(bytes, start + 8)?);
        let stored_length = u32::from_be_bytes(number(bytes, start + 16)?);
        let raw_length = u32::from_be_bytes(number(bytes, start + 20)?);
        let codec = match bytes[start + 24] {
            0 => PageCodec::None,
            1 => PageCodec::Zstd,
            _ => return Err(invalid("unsupported pack codec")),
        };
        if page_number <= previous
            || bytes[start + 25..start + 32].iter().any(|b| *b != 0)
            || raw_length != page_size
            || stored_length == 0
            || offset < payload as u64
            || offset
                .checked_add(u64::from(stored_length))
                .is_none_or(|end| end > bytes.len() as u64)
            || codec == PageCodec::None && stored_length != raw_length
        {
            return Err(invalid("invalid pack entry"));
        }
        previous = page_number;
        ranges.push((offset, offset + u64::from(stored_length)));
        entries.push(PackIndexEntry {
            page_number,
            offset,
            stored_length,
            raw_length,
            codec,
            page_sha256: Sha256::from_bytes(number(bytes, start + 32)?),
        });
    }
    ranges.sort_unstable();
    if ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(invalid("overlapping pack payloads"));
    }
    Ok(PackIndex { page_size, entries })
}

pub fn encode_page_pack(
    page_size: u32,
    pages: &BTreeMap<u64, Vec<u8>>,
) -> Result<Vec<u8>, ProtocolError> {
    if pages.is_empty()
        || !(512..=65536).contains(&page_size)
        || !page_size.is_power_of_two()
        || pages.keys().next() == Some(&0)
        || pages.values().any(|p| p.len() != page_size as usize)
    {
        return Err(invalid("invalid pages for page pack"));
    }
    let count = u32::try_from(pages.len()).map_err(|_| invalid("too many pack entries"))?;
    let payload = pages
        .len()
        .checked_mul(64)
        .and_then(|n| n.checked_add(64))
        .ok_or_else(|| invalid("pack size overflow"))?;
    let total = pages
        .len()
        .checked_mul(page_size as usize)
        .and_then(|n| n.checked_add(payload))
        .ok_or_else(|| invalid("pack size overflow"))?;
    let mut bytes = vec![0; total];
    bytes[..8].copy_from_slice(b"OTMPPGPK");
    bytes[8..10].copy_from_slice(&1u16.to_be_bytes());
    bytes[16..20].copy_from_slice(&page_size.to_be_bytes());
    bytes[20..24].copy_from_slice(&count.to_be_bytes());
    bytes[24..32].copy_from_slice(&64u64.to_be_bytes());
    bytes[32..40].copy_from_slice(&(payload as u64).to_be_bytes());
    for (i, (page_number, page)) in pages.iter().enumerate() {
        let start = 64 + 64 * i;
        let offset = payload + i * page_size as usize;
        bytes[start..start + 8].copy_from_slice(&page_number.to_be_bytes());
        bytes[start + 8..start + 16].copy_from_slice(&(offset as u64).to_be_bytes());
        bytes[start + 16..start + 20].copy_from_slice(&page_size.to_be_bytes());
        bytes[start + 20..start + 24].copy_from_slice(&page_size.to_be_bytes());
        bytes[start + 32..start + 64].copy_from_slice(Sha256::digest(page).as_bytes());
        bytes[offset..offset + page.len()].copy_from_slice(page);
    }
    Ok(bytes)
}
