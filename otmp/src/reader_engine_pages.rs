//! Bounded structural validation before Turso materializes `SQLite` records.
use crate::RuntimeError;
use std::collections::BTreeMap;
const PAGE: usize = 4096;
#[derive(Clone, Debug, Eq, PartialEq)]
enum Role {
    Btree {
        parent: u64,
        slot: usize,
        root: bool,
        schema_tree: bool,
    },
    Overflow {
        owner: u64,
        cell: u16,
        remaining: u64,
        record_bytes: u64,
    },
}
pub(crate) struct PageValidator {
    pages: u64,
    max: usize,
    roles: BTreeMap<u64, Role>,
    max_roles: usize,
}
impl PageValidator {
    pub(crate) fn role_count(&self) -> usize {
        self.roles.len()
    }
    pub(crate) fn new(image_bytes: u64, max_record_bytes: usize, max_roles: usize) -> Self {
        let mut roles = BTreeMap::new();
        roles.insert(
            1,
            Role::Btree {
                parent: 0,
                slot: 0,
                root: true,
                schema_tree: true,
            },
        );
        Self {
            pages: image_bytes / PAGE as u64,
            max: max_record_bytes,
            roles,
            max_roles,
        }
    }
    fn add(&mut self, n: u64, r: Role, new: &mut usize) -> Result<(), RuntimeError> {
        if n == 0 || n > self.pages {
            return Err(RuntimeError::Corrupt(
                "SQLite page reference is outside the authenticated image".into(),
            ));
        }
        if let Some(old) = self.roles.get(&n) {
            if old != &r {
                return Err(RuntimeError::Corrupt(
                    "SQLite page has conflicting structural roles".into(),
                ));
            }
            return Ok(());
        }
        if self.roles.len() >= self.max_roles {
            return Err(RuntimeError::ResourceExhausted(
                "SQLite page-role budget exhausted".into(),
            ));
        }
        self.roles.insert(n, r);
        *new += 1;
        Ok(())
    }
    // Keeping the byte cursor and role registration together makes all bounds visible.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn validate(&mut self, page: u64, bytes: &[u8]) -> Result<usize, RuntimeError> {
        if bytes.len() != PAGE {
            return Err(RuntimeError::Corrupt("invalid SQLite page length".into()));
        }
        if page == 0 || page > self.pages {
            return Err(RuntimeError::Corrupt("SQLite page is outside image".into()));
        }
        let mut added = 0;
        if let Some(Role::Overflow {
            owner,
            cell,
            remaining,
            record_bytes,
        }) = self.roles.get(&page).cloned()
        {
            // A B-tree leaf may contain an unselected, arbitrarily large
            // historical record beside the selected row. Turso requests its
            // first overflow page before allocating the record buffer. Enforce
            // the cap here, without rejecting unrelated records on that leaf.
            if usize::try_from(record_bytes).map_or(true, |size| size > self.max) {
                return Err(RuntimeError::ResourceExhausted(
                    "SQLite record exceeds configured bound".into(),
                ));
            }
            let take = remaining.min((PAGE - 4) as u64);
            let next = u64::from(u32::from_be_bytes(bytes[..4].try_into().unwrap()));
            if remaining > take {
                self.add(
                    next,
                    Role::Overflow {
                        owner,
                        cell,
                        remaining: remaining - take,
                        record_bytes,
                    },
                    &mut added,
                )?;
            } else if next != 0 {
                return Err(RuntimeError::Corrupt(
                    "overflow chain has an unexpected continuation".into(),
                ));
            }
            return Ok(added);
        }
        let schema_tree = match self.roles.get(&page) {
            Some(Role::Btree { schema_tree, .. }) => *schema_tree,
            Some(Role::Overflow { .. }) => unreachable!(),
            None => {
                return Err(RuntimeError::Corrupt(
                    "unregistered SQLite page role".into(),
                ));
            }
        };
        let off = if page == 1 {
            if &bytes[..16] != b"SQLite format 3\0" {
                return Err(RuntimeError::Corrupt("invalid SQLite header".into()));
            }
            if bytes[16..18] != [0x10, 0] || bytes[20..24] != [0, 64, 32, 32] {
                return Err(RuntimeError::Corrupt(
                    "unsupported SQLite page geometry".into(),
                ));
            }
            100
        } else {
            0
        };
        let ty = *bytes
            .get(off)
            .ok_or_else(|| RuntimeError::Corrupt("truncated btree header".into()))?;
        if !matches!(ty, 2 | 5 | 10 | 13) {
            return Err(RuntimeError::Corrupt(
                "page is neither registered overflow nor btree".into(),
            ));
        }
        let header = if ty == 2 || ty == 5 { 12 } else { 8 };
        let cells = u16::from_be_bytes([bytes[off + 3], bytes[off + 4]]) as usize;
        let start = off + header;
        if start + cells * 2 > PAGE {
            return Err(RuntimeError::Corrupt(
                "SQLite cell pointers exceed page".into(),
            ));
        }
        if ty == 2 || ty == 5 {
            let child = u64::from(u32::from_be_bytes(
                bytes
                    .get(off + 8..off + 12)
                    .ok_or_else(|| RuntimeError::Corrupt("truncated internal child".into()))?
                    .try_into()
                    .unwrap(),
            ));
            self.add(
                child,
                Role::Btree {
                    parent: page,
                    slot: usize::MAX,
                    root: false,
                    schema_tree,
                },
                &mut added,
            )?;
        }
        for i in 0..cells {
            let cell_start =
                u16::from_be_bytes([bytes[start + i * 2], bytes[start + i * 2 + 1]]) as usize;
            if cell_start < start + cells * 2 || cell_start >= PAGE {
                return Err(RuntimeError::Corrupt(
                    "SQLite cell pointer out of bounds".into(),
                ));
            }
            let mut cursor = cell_start;
            if ty == 2 || ty == 5 {
                let child = u64::from(u32::from_be_bytes(
                    bytes
                        .get(cursor..cursor + 4)
                        .ok_or_else(|| RuntimeError::Corrupt("truncated internal child".into()))?
                        .try_into()
                        .map_err(|_| RuntimeError::Corrupt("truncated internal child".into()))?,
                ));
                self.add(
                    child,
                    Role::Btree {
                        parent: page,
                        slot: i,
                        root: false,
                        schema_tree,
                    },
                    &mut added,
                )?;
                cursor += 4;
            }
            if ty == 5 {
                let _ = varint(bytes.get(cursor..).ok_or_else(|| {
                    RuntimeError::Corrupt("truncated table-internal rowid".into())
                })?)?;
                continue;
            }
            let (payload, varint_len) = varint(&bytes[cursor..])?;
            let local = local(payload, ty);
            let rowid = if ty == 13 {
                varint(
                    bytes
                        .get(cursor + varint_len..)
                        .ok_or_else(|| RuntimeError::Corrupt("truncated rowid".into()))?,
                )?
                .1
            } else {
                0
            };
            let payload_start = cursor + varint_len + rowid;
            let payload_end = payload_start
                .checked_add(
                    usize::try_from(local)
                        .map_err(|_| RuntimeError::Corrupt("invalid local payload".into()))?,
                )
                .ok_or_else(|| RuntimeError::Corrupt("payload bounds overflow".into()))?;
            if payload_end > PAGE {
                return Err(RuntimeError::Corrupt(
                    "SQLite local payload exceeds page".into(),
                ));
            }
            if schema_tree && ty == 13 {
                let start = payload_start;
                let end = start
                    + usize::try_from(local)
                        .map_err(|_| RuntimeError::Corrupt("invalid local payload".into()))?;
                if let Some(root) = schema_root(bytes.get(start..end).ok_or_else(|| {
                    RuntimeError::Corrupt("truncated schema local payload".into())
                })?)?
                    && root != 0
                {
                    self.add(
                        root,
                        Role::Btree {
                            parent: page,
                            slot: i,
                            root: true,
                            schema_tree: false,
                        },
                        &mut added,
                    )?;
                }
            }
            if payload > local {
                let overflow_offset = cursor
                    + varint_len
                    + rowid
                    + usize::try_from(local)
                        .map_err(|_| RuntimeError::Corrupt("invalid local payload".into()))?;
                let raw = bytes
                    .get(overflow_offset..overflow_offset + 4)
                    .ok_or_else(|| {
                        RuntimeError::Corrupt("truncated SQLite overflow pointer".into())
                    })?;
                let first = u64::from(u32::from_be_bytes(raw.try_into().unwrap()));
                let remain = payload - local;
                self.add(
                    first,
                    Role::Overflow {
                        owner: page,
                        cell: u16::try_from(i).expect("cell count is a u16"),
                        remaining: remain,
                        record_bytes: payload,
                    },
                    &mut added,
                )?;
            }
        }
        Ok(added)
    }
}
fn varint(b: &[u8]) -> Result<(u64, usize), RuntimeError> {
    let mut v = 0u64;
    for (i, &x) in b.iter().take(9).enumerate() {
        if i == 8 {
            return Ok(((v << 8) | u64::from(x), 9));
        }
        v = (v << 7) | u64::from(x & 127);
        if x < 128 {
            return Ok((v, i + 1));
        }
    }
    Err(RuntimeError::Corrupt("invalid SQLite varint".into()))
}
fn local(payload: u64, ty: u8) -> u64 {
    let usable = 4096u64;
    let max = if ty == 13 {
        usable - 35
    } else {
        ((usable - 12) * 64 / 255) - 23
    };
    if payload <= max {
        return payload;
    }
    let min = ((usable - 12) * 32 / 255) - 23;
    let n = min + (payload - min) % (usable - 4);
    if n > max { min } else { n }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ninth_varint_uses_all_eight_bits() {
        assert_eq!(
            varint(&[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0xff])
                .unwrap()
                .1,
            9
        );
    }
    #[test]
    fn table_internal_has_no_payload_bound() {
        let mut v = PageValidator::new(8192, 1, 8);
        let mut p = [0u8; 4096];
        p[0] = 5;
        p[3] = 0;
        p[4] = 1;
        p[8..12].copy_from_slice(&2u32.to_be_bytes());
        p[12..14].copy_from_slice(&20u16.to_be_bytes());
        p[20..24].copy_from_slice(&2u32.to_be_bytes());
        p[24] = 1;
        assert!(v.validate(1, &p).is_err());
    }
    #[test]
    fn local_payload_offset_is_after_payload() {
        assert!(local(5000, 13) > 0);
    }
    #[test]
    fn bounds_are_errors_not_panics() {
        let mut v = PageValidator::new(4096, 8, 8);
        let mut p = [0u8; 4096];
        p[0] = 5;
        p[3] = 0;
        p[4] = 1;
        p[12..14].copy_from_slice(&4095u16.to_be_bytes());
        assert!(v.validate(1, &p).is_err());
    }
}
fn schema_root(payload: &[u8]) -> Result<Option<u64>, RuntimeError> {
    let (h, hn) = varint(payload)?;
    let h = usize::try_from(h)
        .map_err(|_| RuntimeError::Corrupt("schema record header overflow".into()))?;
    if h < hn || h > payload.len() {
        return Err(RuntimeError::Corrupt(
            "schema record header exceeds local payload".into(),
        ));
    }
    let mut at = hn;
    let mut serial = Vec::new();
    while at < h {
        let (v, n) = varint(&payload[at..h])?;
        serial.push(v);
        at += n;
    }
    if serial.len() < 4 {
        return Err(RuntimeError::Corrupt(
            "sqlite_schema record has too few columns".into(),
        ));
    }
    let mut value_at = h;
    for (i, s) in serial.into_iter().enumerate() {
        let n = serial_len(s)?;
        if i == 3 {
            return Ok(match s {
                0 => None,
                8 => Some(0),
                9 => Some(1),
                1..=6 => {
                    if value_at + n > payload.len() {
                        return Err(RuntimeError::Corrupt(
                            "sqlite_schema rootpage exceeds local payload".into(),
                        ));
                    }
                    let mut v = 0u64;
                    let integer = &payload[value_at..value_at + n];
                    if integer[0] & 0x80 != 0 {
                        return Err(RuntimeError::Corrupt(
                            "negative sqlite_schema root page".into(),
                        ));
                    }
                    for b in integer {
                        v = (v << 8) | u64::from(*b);
                    }
                    Some(v)
                }
                _ => {
                    return Err(RuntimeError::Corrupt(
                        "sqlite_schema rootpage is not integer".into(),
                    ));
                }
            });
        }
        value_at = value_at
            .checked_add(n)
            .ok_or_else(|| RuntimeError::Corrupt("schema record size overflow".into()))?;
        if value_at > payload.len() {
            return Err(RuntimeError::Corrupt(
                "schema record exceeds local payload".into(),
            ));
        }
    }
    Ok(None)
}
fn serial_len(s: u64) -> Result<usize, RuntimeError> {
    Ok(match s {
        0 | 8 | 9 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 | 7 => 8,
        10 | 11 => return Err(RuntimeError::Corrupt("reserved SQLite serial type".into())),
        n if n >= 12 => usize::try_from((n - 12) / 2)
            .map_err(|_| RuntimeError::Corrupt("SQLite serial size overflow".into()))?,
        _ => return Err(RuntimeError::Corrupt("invalid SQLite serial type".into())),
    })
}
#[cfg(test)]
mod schema_tests {
    use super::*;
    #[test]
    fn parses_rootpage_from_local_schema_record() {
        // header length 5; four serials: null,null,null,one-byte integer; value 7
        assert_eq!(schema_root(&[5, 0, 0, 0, 1, 7]).unwrap(), Some(7));
    }
    #[test]
    fn rootpage_must_not_cross_local_boundary() {
        assert!(schema_root(&[5, 0, 0, 0, 6]).is_err());
    }
}
#[cfg(test)]
mod real_sqlite {
    use super::*;
    use std::collections::BTreeSet;
    #[test]
    fn validates_reachable_real_sqlite_pages_and_rejects_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.db");
        let c = rusqlite::Connection::open(&path).unwrap();
        c.pragma_update(None, "page_size", 4096).unwrap();
        c.execute_batch("VACUUM; CREATE TABLE blobs(v BLOB); CREATE INDEX blobs_v ON blobs(v);")
            .unwrap();
        for i in 0..100 {
            c.execute_batch(&format!("CREATE TABLE t{i}(x TEXT);"))
                .unwrap();
        }
        c.execute("INSERT INTO blobs VALUES (?1)", [vec![7u8; 65536]])
            .unwrap();
        drop(c);
        let image = std::fs::read(&path).unwrap();
        let mut v = PageValidator::new(image.len() as u64, 1 << 20, 4096);
        let mut seen = BTreeSet::new();
        loop {
            let next = v.roles.keys().copied().find(|p| !seen.contains(p));
            let Some(p) = next else { break };
            seen.insert(p);
            let start = usize::try_from(p - 1).unwrap() * PAGE;
            v.validate(p, &image[start..start + PAGE]).unwrap();
        }
        assert!(seen.len() > 2);
        let (&overflow, _) = v
            .roles
            .iter()
            .find(|(_, role)| matches!(role, Role::Overflow { remaining, .. } if *remaining > 4092))
            .unwrap();
        let start = usize::try_from(overflow - 1).unwrap() * PAGE;
        let mut cyclic = image[start..start + PAGE].to_vec();
        cyclic[..4].copy_from_slice(&u32::try_from(overflow).unwrap().to_be_bytes());
        assert!(matches!(
            v.validate(overflow, &cyclic),
            Err(RuntimeError::Corrupt(_))
        ));

        let mut bad_root = image[..PAGE].to_vec();
        assert_eq!(
            bad_root[100], 5,
            "the schema must exercise an internal page"
        );
        bad_root[108..112].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            v.validate(1, &bad_root),
            Err(RuntimeError::Corrupt(_))
        ));
        let mut bad_pointer = image[..PAGE].to_vec();
        bad_pointer[112..114].copy_from_slice(&4095_u16.to_be_bytes());
        assert!(matches!(
            v.validate(1, &bad_pointer),
            Err(RuntimeError::Corrupt(_))
        ));

        let mut low = PageValidator::new(image.len() as u64, 1024, 4096);
        let mut seen = BTreeSet::new();
        loop {
            let page = low
                .roles
                .keys()
                .copied()
                .find(|page| !seen.contains(page))
                .expect("must reach an oversized record");
            seen.insert(page);
            let start = usize::try_from(page - 1).unwrap() * PAGE;
            match low.validate(page, &image[start..start + PAGE]) {
                Err(RuntimeError::ResourceExhausted(_)) => break,
                result => {
                    result.unwrap();
                }
            }
        }
    }
}
