//! Independent reconstruction for assertions against stock `SQLite` in integration tests.
use otmp::{ObjectStore, Table};
use otmp_protocol::{
    Generation, Head, PageCodec, PageMapNode, canonical_json, decode_pack_index, decode_page_map,
    object_hash,
};

pub async fn current<S: ObjectStore>(store: &S) -> Vec<u8> {
    Table::new(store.clone()).verify().await.unwrap();
    let head: Head = canonical_json::from_slice_canonical(
        &store
            .read(&"_otmp/HEAD".parse().unwrap())
            .await
            .unwrap()
            .bytes,
    )
    .unwrap();
    let generation: Generation = canonical_json::from_slice_canonical(
        &store
            .read(&head.metadata_generation.uri)
            .await
            .unwrap()
            .bytes,
    )
    .unwrap();
    image(store, &generation).await
}

pub async fn image<S: ObjectStore>(store: &S, generation: &Generation) -> Vec<u8> {
    let image = &generation.metadata_image;
    let mut bytes = store.read(&image.checkpoint.uri).await.unwrap().bytes;
    assert_eq!(object_hash(&bytes), image.checkpoint.sha256);
    bytes.resize(
        usize::try_from(image.page_count.0).unwrap() * image.page_size as usize,
        0,
    );
    let mut pending = image
        .page_map
        .iter()
        .map(otmp_protocol::PageMapRoot::reference)
        .collect::<Vec<_>>();
    while let Some(reference) = pending.pop() {
        let raw = store.read(&reference.uri).await.unwrap().bytes;
        assert_eq!(object_hash(&raw), reference.sha256);
        match decode_page_map(&raw).unwrap() {
            PageMapNode::Internal { entries, .. } => {
                pending.extend(entries.into_iter().map(|e| e.child));
            }
            PageMapNode::Leaf { entries } => {
                for entry in entries {
                    let pack = store.read(&entry.pack.uri).await.unwrap().bytes;
                    assert_eq!(object_hash(&pack), entry.pack.sha256);
                    let index = decode_pack_index(&pack).unwrap();
                    assert!(index.entries.iter().any(|e| e.page_number == entry.page_number && e.offset == entry.offset));
                    let stored = &pack[usize::try_from(entry.offset).unwrap()
                        ..usize::try_from(entry.offset).unwrap() + entry.stored_length as usize];
                    let page = match entry.codec {
                        PageCodec::None => stored.to_vec(),
                        PageCodec::Zstd => {
                            zstd::bulk::decompress(stored, image.page_size as usize).unwrap()
                        }
                    };
                    assert_eq!(object_hash(&page), entry.page_sha256);
                    let start = (usize::try_from(entry.page_number).unwrap() - 1)
                        * image.page_size as usize;
                    bytes[start..start + page.len()].copy_from_slice(&page);
                }
            }
        }
    }
    bytes
}
