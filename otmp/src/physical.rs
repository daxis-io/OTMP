//! Immutable physical images: verified materialization and persistent page-map updates.
use crate::storage::StoredObject;
use crate::{ObjectStore, RuntimeError, Table};
use otmp_protocol::{
    Generation, JsonU64, ObjectReference, PAGE_MAP_MEDIA_TYPE, PAGE_PACK_MEDIA_TYPE, PageCodec,
    PageMapBranch, PageMapEntry, PageMapNode, PageMapRoot, PageObjectReference, RelativeUri,
    Sha256, decode_pack_index, decode_page_map, encode_page_map, encode_page_pack, image_root_hash,
    object_hash,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const CAPACITY: usize = 128;
const PACK_TARGET: usize = 1024 * 1024;

pub(crate) struct Tree {
    reference: PageObjectReference,
    node: PageMapNode,
    children: Vec<Arc<Tree>>,
    min: u64,
    max: u64,
}

pub(crate) struct ResolvedGeneration {
    pub checkpoint: Arc<StoredObject>,
    pub bytes: Arc<[u8]>,
    pub tree: Option<Arc<Tree>>,
}

pub(crate) struct Artifact {
    pub uri: RelativeUri,
    pub bytes: Vec<u8>,
}

fn corrupt(message: &str) -> RuntimeError {
    RuntimeError::Corrupt(message.into())
}
fn object(reference: &PageObjectReference, media_type: &str) -> ObjectReference {
    ObjectReference {
        uri: reference.uri.clone(),
        sha256: reference.sha256,
        length: Some(reference.length),
        media_type: Some(media_type.into()),
    }
}

impl<S: ObjectStore> Table<S> {
    async fn load_tree(
        &self,
        reference: PageObjectReference,
        level: u32,
        lower: u64,
        upper: u64,
        seen: &mut BTreeSet<String>,
    ) -> Result<Arc<Tree>, RuntimeError> {
        if level > 64
            || !seen.insert(reference.uri.as_str().to_owned())
            || reference.length.0 > otmp_protocol::MAX_PAGE_MAP_BYTES as u64
        {
            return Err(corrupt(
                "invalid page-map height, cycle, repeated subtree, or size",
            ));
        }
        let bytes = self
            .read_metadata(&object(&reference, PAGE_MAP_MEDIA_TYPE))
            .await?;
        let node = decode_page_map(&bytes.bytes)?;
        if node.level() != level {
            return Err(corrupt("page-map level mismatch"));
        }
        let mut children = Vec::new();
        let (min, max) = match &node {
            PageMapNode::Leaf { entries } => (
                entries.first().unwrap().page_number,
                entries.last().unwrap().page_number,
            ),
            PageMapNode::Internal { entries, .. } => {
                let mut previous = lower;
                for entry in entries {
                    if entry.max_page <= previous || entry.max_page > upper {
                        return Err(corrupt("invalid page-map subtree boundary"));
                    }
                    let child = Box::pin(self.load_tree(
                        entry.child.clone(),
                        level - 1,
                        previous,
                        entry.max_page,
                        seen,
                    ))
                    .await?;
                    if child.max != entry.max_page {
                        return Err(corrupt("page-map maximum disagrees with child"));
                    }
                    previous = entry.max_page;
                    children.push(child);
                }
                (children.first().unwrap().min, children.last().unwrap().max)
            }
        };
        if min <= lower || max > upper {
            return Err(corrupt("page-map entry outside subtree bounds"));
        }
        Ok(Arc::new(Tree {
            reference,
            node,
            children,
            min,
            max,
        }))
    }

    #[allow(clippy::too_many_lines)] // Cross-object checks remain adjacent to image assembly.
    pub(crate) async fn resolve_generation(
        &self,
        generation: &Generation,
    ) -> Result<ResolvedGeneration, RuntimeError> {
        let image = &generation.metadata_image;
        let checkpoint = self
            .read_metadata(&ObjectReference {
                uri: image.checkpoint.uri.clone(),
                sha256: image.checkpoint.sha256,
                length: Some(image.checkpoint.length),
                media_type: Some(otmp_protocol::CHECKPOINT_MEDIA_TYPE.into()),
            })
            .await?;
        let page_size = image.page_size as usize;
        if checkpoint.bytes.len() < page_size
            || checkpoint.bytes.len() % page_size != 0
            || &checkpoint.bytes[..16] != b"SQLite format 3\0"
        {
            return Err(corrupt("invalid base checkpoint layout"));
        }
        // The checkpoint has its own identity, independent of the selected image.
        let base = crate::image::materialize(&checkpoint.bytes)?;
        let connection = crate::image::open_readonly(&base.path)?;
        let (table, version): (Vec<u8>, i64) = connection.query_row(
            "SELECT table_id,table_version FROM otmp_meta WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let base_page_size: u32 = connection.query_row("PRAGMA page_size", [], |r| r.get(0))?;
        let base_pages: u64 = connection.query_row("PRAGMA page_count", [], |r| r.get(0))?;
        if table != generation.table_id.as_bytes()
            || u64::try_from(version).ok() != Some(image.checkpoint.table_version.0)
            || base_page_size != image.page_size
            || base_pages != (checkpoint.bytes.len() / page_size) as u64
        {
            return Err(corrupt("base checkpoint identity mismatch"));
        }
        drop(connection);
        let root_hash = image.page_map.as_ref().map(|r| r.sha256);
        if image_root_hash(
            generation.table_id,
            generation.table_version.0,
            image.page_size,
            image.page_count.0,
            image.checkpoint.sha256,
            root_hash,
        ) != image.image_root_sha256
        {
            return Err(corrupt("metadata image root mismatch"));
        }
        let length = usize::try_from(image.page_count.0)
            .ok()
            .and_then(|n| n.checked_mul(page_size))
            .ok_or_else(|| corrupt("image length overflow"))?;
        if length == 0 {
            return Err(corrupt("empty metadata image"));
        }
        let tree = if let Some(root) = &image.page_map {
            Some(
                self.load_tree(
                    root.reference(),
                    root.height,
                    0,
                    image.page_count.0,
                    &mut BTreeSet::new(),
                )
                .await?,
            )
        } else {
            if length != checkpoint.bytes.len() {
                return Err(corrupt("incomplete null-map checkpoint"));
            }
            None
        };
        let mut entries = BTreeMap::new();
        if let Some(tree) = &tree {
            flatten(tree, &mut entries);
        }
        // Never synthesize newly extended pages that no explicit object supplies.
        let base_pages = (checkpoint.bytes.len() / page_size) as u64;
        if entries.range(base_pages + 1..).count() as u64
            != image.page_count.0.saturating_sub(base_pages)
        {
            return Err(corrupt("missing extended image page"));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| corrupt("metadata image exceeds materialization capacity"))?;
        bytes.resize(length, 0);
        let copied = length.min(checkpoint.bytes.len());
        bytes[..copied].copy_from_slice(&checkpoint.bytes[..copied]);
        let mut packs = BTreeMap::<
            String,
            (
                PageObjectReference,
                Arc<StoredObject>,
                otmp_protocol::PackIndex,
            ),
        >::new();
        for entry in entries.values() {
            if entry.raw_length != image.page_size {
                return Err(corrupt("page-map raw length mismatch"));
            }
            if let Some((reference, _, _)) = packs.get(entry.pack.uri.as_str()) {
                if reference != &entry.pack {
                    return Err(corrupt("conflicting pack references"));
                }
            } else {
                let raw = self
                    .read_metadata(&object(&entry.pack, PAGE_PACK_MEDIA_TYPE))
                    .await?;
                let index = decode_pack_index(&raw.bytes)?;
                if index.page_size != image.page_size {
                    return Err(corrupt("pack page size mismatch"));
                }
                packs.insert(
                    entry.pack.uri.as_str().to_owned(),
                    (entry.pack.clone(), raw, index),
                );
            }
            let (_, raw, index) = &packs[entry.pack.uri.as_str()];
            let position = index
                .entries
                .binary_search_by_key(&entry.page_number, |e| e.page_number)
                .map_err(|_| corrupt("mapped page absent from pack index"))?;
            let indexed = &index.entries[position];
            if indexed.offset != entry.offset
                || indexed.codec != entry.codec
                || indexed.raw_length != entry.raw_length
                || indexed.stored_length != entry.stored_length
                || indexed.page_sha256 != entry.page_sha256
            {
                return Err(corrupt("map disagrees with pack index"));
            }
            let start =
                usize::try_from(entry.offset).map_err(|_| corrupt("pack offset overflow"))?;
            let stored = &raw.bytes[start..start + entry.stored_length as usize];
            let page = match entry.codec {
                PageCodec::None => stored.to_vec(),
                // One-shot decoding uses a bounded destination and also accepts
                // standard streaming frames with an unknown content size.
                PageCodec::Zstd => zstd::bulk::decompress(stored, page_size)?,
            };
            if page.len() != page_size || Sha256::digest(&page) != entry.page_sha256 {
                return Err(corrupt("invalid decompressed page length or hash"));
            }
            let start = (usize::try_from(entry.page_number)
                .map_err(|_| corrupt("page index overflow"))?
                - 1)
                * page_size;
            bytes[start..start + page_size].copy_from_slice(&page);
        }
        if u64::from(u32::from_be_bytes(bytes[28..32].try_into().unwrap())) != image.page_count.0 {
            return Err(corrupt("resolved header page count mismatch"));
        }
        Ok(ResolvedGeneration {
            checkpoint,
            bytes: bytes.into(),
            tree,
        })
    }
}

fn flatten<'a>(tree: &'a Tree, entries: &mut BTreeMap<u64, &'a PageMapEntry>) {
    match &tree.node {
        PageMapNode::Leaf { entries: pages } => {
            for page in pages {
                entries.insert(page.page_number, page);
            }
        }
        PageMapNode::Internal { .. } => {
            for child in &tree.children {
                flatten(child, entries);
            }
        }
    }
}

fn create_tree(
    node: PageMapNode,
    children: Vec<Arc<Tree>>,
    artifacts: &mut BTreeMap<String, Artifact>,
) -> Result<Arc<Tree>, RuntimeError> {
    let bytes = encode_page_map(&node)?;
    let sha256 = object_hash(&bytes);
    let uri: RelativeUri =
        format!("_otmp/page-maps/{}.cbor", hex::encode(sha256.as_bytes())).parse()?;
    let reference = PageObjectReference {
        uri: uri.clone(),
        sha256,
        length: JsonU64(bytes.len() as u64),
    };
    let min = match &node {
        PageMapNode::Leaf { entries } => entries[0].page_number,
        PageMapNode::Internal { .. } => children[0].min,
    };
    let max = node.max_page().unwrap();
    artifacts.insert(uri.as_str().to_owned(), Artifact { uri, bytes });
    Ok(Arc::new(Tree {
        reference,
        node,
        children,
        min,
        max,
    }))
}

fn leaf_nodes(
    mut entries: Vec<PageMapEntry>,
    artifacts: &mut BTreeMap<String, Artifact>,
) -> Result<Vec<Arc<Tree>>, RuntimeError> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let node = PageMapNode::Leaf {
        entries: entries.clone(),
    };
    node.validate()?;
    if entries.len() <= CAPACITY {
        match create_tree(node, Vec::new(), artifacts) {
            Ok(node) => return Ok(vec![node]),
            Err(error) if entries.len() == 1 => return Err(error),
            Err(_) => {} // A valid node exceeded the serialized byte limit.
        }
    }
    let right = entries.split_off(entries.len() / 2);
    let mut nodes = leaf_nodes(entries, artifacts)?;
    nodes.extend(leaf_nodes(right, artifacts)?);
    Ok(nodes)
}

fn internal_nodes(
    mut children: Vec<Arc<Tree>>,
    level: u32,
    artifacts: &mut BTreeMap<String, Artifact>,
) -> Result<Vec<Arc<Tree>>, RuntimeError> {
    if children.is_empty() {
        return Ok(Vec::new());
    }
    let entries = children
        .iter()
        .map(|c| PageMapBranch {
            max_page: c.max,
            child: c.reference.clone(),
        })
        .collect();
    let node = PageMapNode::Internal { level, entries };
    node.validate()?;
    if children.len() <= CAPACITY {
        match create_tree(node, children.clone(), artifacts) {
            Ok(node) => return Ok(vec![node]),
            Err(error) if children.len() == 1 => return Err(error),
            Err(_) => {} // A valid node exceeded the serialized byte limit.
        }
    }
    let right = children.split_off(children.len() / 2);
    let mut nodes = internal_nodes(children, level, artifacts)?;
    nodes.extend(internal_nodes(right, level, artifacts)?);
    Ok(nodes)
}

fn update(
    tree: &Arc<Tree>,
    changes: &[PageMapEntry],
    eof: u64,
    artifacts: &mut BTreeMap<String, Artifact>,
) -> Result<Vec<Arc<Tree>>, RuntimeError> {
    if changes.is_empty() && tree.max <= eof {
        return Ok(vec![tree.clone()]);
    }
    if tree.min > eof && changes.is_empty() {
        return Ok(Vec::new());
    }
    match &tree.node {
        PageMapNode::Leaf { entries } => {
            let mut pages: BTreeMap<_, _> = entries
                .iter()
                .filter(|e| e.page_number <= eof)
                .map(|e| (e.page_number, e.clone()))
                .collect();
            pages.extend(changes.iter().map(|e| (e.page_number, e.clone())));
            leaf_nodes(pages.into_values().collect(), artifacts)
        }
        PageMapNode::Internal { level, .. } => {
            let mut remaining = changes;
            let mut children = Vec::new();
            for (i, child) in tree.children.iter().enumerate() {
                let count = if i + 1 == tree.children.len() {
                    remaining.len()
                } else {
                    remaining.partition_point(|entry| entry.page_number <= child.max)
                };
                let (here, rest) = remaining.split_at(count);
                remaining = rest;
                children.extend(update(child, here, eof, artifacts)?);
            }
            internal_nodes(children, *level, artifacts)
        }
    }
}

fn reachable(
    tree: &Tree,
    references: &mut BTreeMap<String, PageObjectReference>,
) -> Result<(), RuntimeError> {
    let mut add = |reference: &PageObjectReference| -> Result<(), RuntimeError> {
        if let Some(old) = references.insert(reference.uri.as_str().to_owned(), reference.clone())
            && old != *reference
        {
            return Err(corrupt("conflicting reachable object references"));
        }
        Ok(())
    };
    add(&tree.reference)?;
    match &tree.node {
        PageMapNode::Leaf { entries } => {
            for entry in entries {
                add(&entry.pack)?;
            }
        }
        PageMapNode::Internal { .. } => {
            for child in &tree.children {
                reachable(child, references)?;
            }
        }
    }
    Ok(())
}

pub(crate) struct IncrementalImage {
    pub root: Option<PageMapRoot>,
    pub artifacts: Vec<Artifact>,
    pub reachable_bytes: u64,
}

pub(crate) fn persist(
    parent: Option<&Arc<Tree>>,
    changed: &BTreeMap<u64, Vec<u8>>,
    eof: u64,
) -> Result<IncrementalImage, RuntimeError> {
    let mut artifacts = BTreeMap::new();
    let mut mappings = Vec::new();
    let page_size = crate::image::PAGE_SIZE;
    let pack_capacity = (PACK_TARGET - 64) / (64 + page_size as usize);
    let pages: Vec<_> = changed.iter().collect();
    for chunk in pages.chunks(pack_capacity) {
        let pages: BTreeMap<_, _> = chunk
            .iter()
            .map(|(number, bytes)| (**number, (*bytes).clone()))
            .collect();
        if pages.keys().any(|number| *number > eof) {
            return Err(corrupt("candidate changed page beyond EOF"));
        }
        let bytes = encode_page_pack(page_size, &pages)?;
        let sha256 = object_hash(&bytes);
        let uri: RelativeUri =
            format!("_otmp/page-packs/{}.otmppg", hex::encode(sha256.as_bytes())).parse()?;
        let pack = PageObjectReference {
            uri: uri.clone(),
            sha256,
            length: JsonU64(bytes.len() as u64),
        };
        for entry in decode_pack_index(&bytes)?.entries {
            mappings.push(PageMapEntry {
                page_number: entry.page_number,
                pack: pack.clone(),
                offset: entry.offset,
                stored_length: entry.stored_length,
                raw_length: entry.raw_length,
                codec: entry.codec,
                page_sha256: entry.page_sha256,
            });
        }
        artifacts.insert(uri.as_str().to_owned(), Artifact { uri, bytes });
    }
    let mut roots = if let Some(parent) = parent {
        update(parent, &mappings, eof, &mut artifacts)?
    } else {
        leaf_nodes(mappings, &mut artifacts)?
    };
    while roots.len() > 1 {
        let level = roots[0].node.level() + 1;
        roots = internal_nodes(roots, level, &mut artifacts)?;
    }
    let mut root = roots.pop();
    while root.as_ref().is_some_and(|r| r.children.len() == 1) {
        root = root.map(|r| r.children[0].clone());
    }
    let mut references = BTreeMap::new();
    if let Some(root) = &root {
        reachable(root, &mut references)?;
    }
    let reachable_bytes = references.values().try_fold(0u64, |total, r| {
        total
            .checked_add(r.length.0)
            .ok_or_else(|| corrupt("reachable bytes overflow"))
    })?;
    let root = root.map(|tree| PageMapRoot {
        uri: tree.reference.uri.clone(),
        sha256: tree.reference.sha256,
        length: tree.reference.length,
        height: tree.node.level(),
    });
    Ok(IncrementalImage {
        root,
        reachable_bytes,
        artifacts: artifacts
            .into_iter()
            .filter_map(|(uri, artifact)| references.contains_key(&uri).then_some(artifact))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(count: u64) -> Arc<Tree> {
        let pack = PageObjectReference {
            uri: "pack".parse().unwrap(),
            length: JsonU64(4096),
            sha256: Sha256::from_bytes([0; 32]),
        };
        let pages = (1..=count)
            .map(|page_number| PageMapEntry {
                page_number,
                pack: pack.clone(),
                offset: 0,
                stored_length: 4096,
                raw_length: 4096,
                codec: PageCodec::None,
                page_sha256: Sha256::from_bytes([0; 32]),
            })
            .collect();
        let mut artifacts = BTreeMap::new();
        let mut roots = leaf_nodes(pages, &mut artifacts).unwrap();
        while roots.len() > 1 {
            let level = roots[0].node.level() + 1;
            roots = internal_nodes(roots, level, &mut artifacts).unwrap();
        }
        roots.pop().unwrap()
    }

    #[test]
    fn multilevel_updates_reuse_untouched_subtrees_and_prune_eof() {
        let parent = tree(20_000);
        assert_eq!(parent.node.level(), 2);
        let mut pages = BTreeMap::new();
        flatten(&parent, &mut pages);
        let mut changed = pages[&1].clone();
        changed.page_sha256 = Sha256::from_bytes([1; 32]);
        let mut artifacts = BTreeMap::new();
        let roots = update(&parent, &[changed], 20_000, &mut artifacts).unwrap();
        assert_eq!(roots.len(), 1);
        assert!(Arc::ptr_eq(&parent.children[1], &roots[0].children[1]));
        assert_eq!(artifacts.len(), 3, "one copied node at each level");
        let roots = update(&roots[0], &[], 10_001, &mut artifacts).unwrap();
        let mut pages = BTreeMap::new();
        flatten(&roots[0], &mut pages);
        assert_eq!(pages.len(), 10_001);
        assert_eq!(pages.last_key_value().unwrap().0, &10_001);
    }

    #[test]
    fn split_divides_at_midpoint_and_grows_tree() {
        let parent = tree(128);
        let mut entries = BTreeMap::new();
        flatten(&parent, &mut entries);
        let mut appended = entries[&128].clone();
        appended.page_number = 129;
        let roots = update(&parent, &[appended], 129, &mut BTreeMap::new()).unwrap();
        assert_eq!(roots.len(), 2);
        assert_eq!(roots[0].max, 64);
        assert_eq!(roots[1].min, 65);
        let image = persist(Some(&parent), &BTreeMap::from([(129, vec![1; 4096])]), 129).unwrap();
        assert_eq!(image.root.unwrap().height, 1);
    }
    #[test]
    fn serialized_size_splits_nodes_below_entry_capacity() {
        let parent = tree(40);
        let PageMapNode::Leaf { mut entries } = parent.node.clone() else {
            unreachable!()
        };
        for entry in &mut entries {
            entry.pack.uri = format!("pack/{}", "x".repeat(30_000)).parse().unwrap();
        }
        let nodes = leaf_nodes(entries, &mut BTreeMap::new()).unwrap();
        assert_eq!(nodes.len(), 2);
        for node in nodes {
            assert!(
                encode_page_map(&node.node).unwrap().len() <= otmp_protocol::MAX_PAGE_MAP_BYTES
            );
        }
    }

    #[tokio::test]
    async fn materializer_rejects_incorrect_subtree_maximum() {
        let parent = tree(256);
        let store = crate::InMemoryObjectStore::default();
        let mut pending = vec![parent.clone()];
        while let Some(tree) = pending.pop() {
            let bytes = encode_page_map(&tree.node).unwrap();
            store
                .create_from_reader(
                    &tree.reference.uri,
                    &mut bytes.as_slice(),
                    Some(bytes.len() as u64),
                )
                .await
                .unwrap();
            pending.extend(tree.children.iter().cloned());
        }
        let mut node = parent.node.clone();
        let PageMapNode::Internal { entries, .. } = &mut node else {
            unreachable!()
        };
        entries[0].max_page -= 1;
        let bytes = encode_page_map(&node).unwrap();
        let reference = PageObjectReference {
            uri: "bad-root".parse().unwrap(),
            sha256: object_hash(&bytes),
            length: JsonU64(bytes.len() as u64),
        };
        store
            .create_from_reader(
                &reference.uri,
                &mut bytes.as_slice(),
                Some(bytes.len() as u64),
            )
            .await
            .unwrap();
        assert!(
            Table::new(store)
                .load_tree(reference, 1, 0, 256, &mut BTreeSet::new())
                .await
                .is_err()
        );
    }
}
