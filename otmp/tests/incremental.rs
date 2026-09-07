use otmp::{
    CommitMetadata, InitializeRequest, LocalObjectStore, MetadataSelection, OperationRequest,
    Requirement, Table, TransactionRequest, VerificationScope,
};
use otmp_protocol::{
    CanonicalValue, Generation, Head, JsonU64, PageMapNode, PageMapRoot, Schema, canonical_json,
    decode_page_map, encode_page_map, image_root_hash, object_hash,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

fn schema() -> Schema {
    serde_json::from_slice(include_bytes!("../../conformance/sources/schema.json")).unwrap()
}
fn request(
    id: &str,
    updates: BTreeMap<String, CanonicalValue>,
    previous: Option<&str>,
) -> TransactionRequest {
    TransactionRequest {
        idempotency_key: id.into(),
        requirements: updates
            .keys()
            .map(|key| Requirement::PropertyIs {
                key: key.clone(),
                value: previous.map_or(CanonicalValue::Null, |s| CanonicalValue::String(s.into())),
            })
            .collect(),
        operations: vec![OperationRequest::SetProperties {
            operation_id: "set".into(),
            updates,
            removals: vec![],
        }],
        commit_metadata: CommitMetadata::default(),
    }
}
fn generation(root: &Path) -> (Head, Generation) {
    let head: Head =
        canonical_json::from_slice_canonical(&std::fs::read(root.join("_otmp/HEAD")).unwrap())
            .unwrap();
    let generation = canonical_json::from_slice_canonical(
        &std::fs::read(root.join(head.metadata_generation.uri.as_str())).unwrap(),
    )
    .unwrap();
    (head, generation)
}
fn nodes(root: &Path, map: &PageMapRoot, output: &mut BTreeSet<String>) {
    output.insert(map.uri.as_str().into());
    if let PageMapNode::Internal { entries, .. } =
        decode_page_map(&std::fs::read(root.join(map.uri.as_str())).unwrap()).unwrap()
    {
        for entry in entries {
            nodes(
                root,
                &PageMapRoot {
                    uri: entry.child.uri,
                    sha256: entry.child.sha256,
                    length: entry.child.length,
                    height: map.height - 1,
                },
                output,
            );
        }
    }
}
fn artifacts(root: &Path) -> BTreeMap<String, u64> {
    fn visit(path: &Path, output: &mut BTreeMap<String, u64>) {
        if !path.exists() {
            return;
        }
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, output);
            } else {
                output.insert(
                    path.to_string_lossy().into(),
                    path.metadata().unwrap().len(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    for name in ["checkpoints", "page-maps", "page-packs"] {
        visit(&root.join("_otmp").join(name), &mut result);
    }
    result
}

#[tokio::test]
async fn small_transaction_reuses_nodes_and_publishes_far_less_than_a_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let table = Table::new(LocalObjectStore::new(root).unwrap());
    table
        .initialize(InitializeRequest::new(schema()))
        .await
        .unwrap();
    let original = "a".repeat(4096);
    let warmed = "b".repeat(4096);
    let updates = (0..512)
        .map(|i| {
            (
                format!("key-{i:04}"),
                CanonicalValue::String(original.clone()),
            )
        })
        .collect();
    table
        .transact(&request("large", updates, None))
        .await
        .unwrap();
    let (_, full) = generation(root);
    assert!(
        full.metadata_image.page_map.is_none(),
        "large transaction checkpoints at threshold"
    );
    let updates = (0..128)
        .map(|i| {
            (
                format!("key-{i:04}"),
                CanonicalValue::String(warmed.clone()),
            )
        })
        .collect();
    table
        .transact(&request("warm", updates, Some(&original)))
        .await
        .unwrap();
    let (_, before) = generation(root);
    let mut old_nodes = BTreeSet::new();
    nodes(
        root,
        before.metadata_image.page_map.as_ref().unwrap(),
        &mut old_nodes,
    );
    assert!(old_nodes.len() > 1);
    let old_pin = table.pin().await.unwrap();
    let uploaded_before = artifacts(root);
    let transaction = request(
        "small",
        BTreeMap::from([("key-0064".into(), CanonicalValue::String("small".into()))]),
        Some(&warmed),
    );
    let result = table.transact(&transaction).await.unwrap();
    assert_eq!(table.transact(&transaction).await.unwrap(), result);
    let (_, after) = generation(root);
    let mut new_nodes = BTreeSet::new();
    nodes(
        root,
        after.metadata_image.page_map.as_ref().unwrap(),
        &mut new_nodes,
    );
    let reused = old_nodes.intersection(&new_nodes).count();
    assert!(reused > 0, "small update reuses an unchanged map node");
    let uploaded: u64 = artifacts(root)
        .iter()
        .filter(|(uri, _)| !uploaded_before.contains_key(*uri))
        .map(|(_, length)| *length)
        .sum();
    let full_image = after.metadata_image.page_count.0 * 4096;
    assert!(
        uploaded * 10 < full_image,
        "{uploaded} uploaded vs {full_image} full image bytes"
    );
    eprintln!(
        "publication evidence: full image={full_image} bytes, uploaded image artifacts={uploaded} bytes, reused map nodes={reused}"
    );
    assert_eq!(old_pin.status().table_version, 2);
    assert_eq!(
        table
            .pin_metadata(MetadataSelection::TableVersion(2))
            .await
            .unwrap()
            .coordinates()
            .table_version,
        2
    );
    let report = table
        .verify_with_report(VerificationScope::RetainedHistory)
        .await
        .unwrap();
    assert!(report.completed);
    assert!(report.objects_checked > 12);
}

fn replace_generation(root: &Path, mut head: Head, mut generation: Generation) {
    let image = &mut generation.metadata_image;
    image.image_root_sha256 = image_root_hash(
        generation.table_id,
        generation.table_version.0,
        image.page_size,
        image.page_count.0,
        image.checkpoint.sha256,
        image.page_map.as_ref().map(|map| map.sha256),
    );
    let bytes = canonical_json::to_vec(&generation).unwrap();
    std::fs::write(root.join(head.metadata_generation.uri.as_str()), &bytes).unwrap();
    head.metadata_generation.sha256 = object_hash(&bytes);
    head.metadata_generation.length = Some(JsonU64(bytes.len() as u64));
    std::fs::write(
        root.join("_otmp/HEAD"),
        canonical_json::to_vec(&head).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn reader_rejects_wrong_map_identity_height_and_missing_extended_pages() {
    for damage in [
        "hash",
        "length",
        "height",
        "missing",
        "checkpoint-version",
        "pack-index",
        "pack-hash",
        "conflicting-pack-reference",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let table = Table::new(LocalObjectStore::new(root).unwrap());
        table
            .initialize(InitializeRequest::new(schema()))
            .await
            .unwrap();
        table
            .transact(&request(
                "property",
                BTreeMap::from([("key".into(), CanonicalValue::String("value".into()))]),
                None,
            ))
            .await
            .unwrap();
        let (head, mut generation) = generation(root);
        let map = generation.metadata_image.page_map.as_mut().unwrap();
        match damage {
            "hash" => map.sha256 = otmp_protocol::Sha256::from_bytes([0; 32]),
            "length" => map.length.0 += 1,
            "height" => map.height += 1,
            "missing" => generation.metadata_image.page_count.0 += 1,
            "checkpoint-version" => generation.metadata_image.checkpoint.table_version.0 += 1,
            "pack-index" | "pack-hash" | "conflicting-pack-reference" => {
                let mut node =
                    decode_page_map(&std::fs::read(root.join(map.uri.as_str())).unwrap()).unwrap();
                let PageMapNode::Leaf { entries } = &mut node else {
                    panic!("expected leaf");
                };
                if damage == "pack-index" {
                    entries[0].page_sha256 = otmp_protocol::Sha256::from_bytes([0; 32]);
                } else if damage == "pack-hash" {
                    entries[0].pack.sha256 = otmp_protocol::Sha256::from_bytes([0; 32]);
                } else {
                    entries[1].pack = entries[0].pack.clone();
                    entries[1].pack.sha256 = otmp_protocol::Sha256::from_bytes([0; 32]);
                }
                let bytes = encode_page_map(&node).unwrap();
                map.sha256 = object_hash(&bytes);
                map.length = JsonU64(bytes.len() as u64);
                std::fs::write(root.join(map.uri.as_str()), bytes).unwrap();
            }
            _ => unreachable!(),
        }
        replace_generation(root, head, generation);
        assert!(table.pin().await.is_err(), "accepted {damage}");
    }
}

#[tokio::test]
async fn deterministic_incremental_fixture_verifies_retained_history() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/tables/incremental");
    let table = Table::new(LocalObjectStore::new(&root).unwrap());
    assert_eq!(table.pin().await.unwrap().status().table_version, 2);
    for version in 0..=2 {
        assert_eq!(
            table
                .pin_metadata(MetadataSelection::TableVersion(version))
                .await
                .unwrap()
                .coordinates()
                .table_version,
            version
        );
    }
    table.verify_history().await.unwrap();
}

#[tokio::test]
async fn compressed_pages_validate_and_overruns_or_truncation_are_rejected() {
    for damage in ["none", "streaming", "overrun", "truncated"] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let table = Table::new(LocalObjectStore::new(root).unwrap());
        table
            .initialize(InitializeRequest::new(schema()))
            .await
            .unwrap();
        table
            .transact(&request(
                "property",
                BTreeMap::from([("key".into(), CanonicalValue::String("value".into()))]),
                None,
            ))
            .await
            .unwrap();
        let (head, mut generation) = generation(root);
        let map = generation.metadata_image.page_map.as_mut().unwrap();
        let mut node =
            decode_page_map(&std::fs::read(root.join(map.uri.as_str())).unwrap()).unwrap();
        let PageMapNode::Leaf { entries } = &mut node else {
            panic!("expected leaf");
        };
        for entry in entries {
            let original = std::fs::read(root.join(entry.pack.uri.as_str())).unwrap();
            let offset = usize::try_from(entry.offset).unwrap();
            let mut page = original[offset..offset + 4096].to_vec();
            if damage == "overrun" {
                page.push(0);
            }
            let mut compressed = if damage == "streaming" {
                zstd::stream::encode_all(page.as_slice(), 1).unwrap()
            } else {
                zstd::bulk::compress(&page, 1).unwrap()
            };
            if damage == "truncated" {
                compressed.pop();
            }
            let length = u32::try_from(compressed.len()).unwrap();
            let mut pack = otmp_protocol::encode_page_pack(
                4096,
                &BTreeMap::from([(entry.page_number, page[..4096].to_vec())]),
            )
            .unwrap();
            pack.truncate(128);
            pack[80..84].copy_from_slice(&length.to_be_bytes());
            pack[88] = 1;
            pack.extend_from_slice(&compressed);
            let uri = format!("_otmp/page-packs/compressed-{}.otmppg", entry.page_number);
            std::fs::write(root.join(&uri), &pack).unwrap();
            entry.pack = otmp_protocol::PageObjectReference {
                uri: uri.parse().unwrap(),
                sha256: object_hash(&pack),
                length: JsonU64(pack.len() as u64),
            };
            entry.offset = 128;
            entry.stored_length = length;
            entry.codec = otmp_protocol::PageCodec::Zstd;
        }
        let bytes = encode_page_map(&node).unwrap();
        map.sha256 = object_hash(&bytes);
        map.length = JsonU64(bytes.len() as u64);
        std::fs::write(root.join(map.uri.as_str()), bytes).unwrap();
        replace_generation(root, head, generation);
        if damage == "none" || damage == "streaming" {
            table.verify_history().await.unwrap();
        } else {
            assert!(table.pin().await.is_err(), "accepted {damage}");
        }
    }
}
