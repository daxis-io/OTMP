//! Resolve explicit immutable metadata links without materializing `SQLite`.
use super::ReadContext;
use super::cache::Reservation;
use crate::{HeadAnchor, MetadataCoordinates, MetadataSelection, ObjectStore, RuntimeError};
use otmp_protocol::{
    COMMIT_MEDIA_TYPE, CORE_FEATURE, CanonicalValue, GENERATION_MEDIA_TYPE, Generation, Head,
    ObjectReference, PARQUET_FEATURE, SQLITE_COW_FEATURE, SemanticCommit, canonical_json,
    genesis_state_hash, next_state_hash,
};
use std::collections::BTreeSet;

pub(crate) struct Selection {
    pub generation: Generation,
    pub coordinates: MetadataCoordinates,
    pub anchor: HeadAnchor,
    pub commit: SemanticCommit,
    // Covers retained parsed envelopes and temporary canonical validation copies.
    pub reservations: Vec<Reservation>,
}

fn corrupt(message: &str) -> RuntimeError {
    RuntimeError::Corrupt(message.into())
}

async fn read_json<S: ObjectStore, T: serde::de::DeserializeOwned>(
    context: &ReadContext<S>,
    reference: &ObjectReference,
    media_type: &str,
) -> Result<(T, Reservation), RuntimeError> {
    if reference.media_type.as_deref() != Some(media_type) {
        return Err(corrupt("incorrect metadata object media type"));
    }
    let raw = context.object(reference).await?;
    let size = raw.as_ref().as_ref().len();
    let reservation = context.reserve_bytes(
        size.checked_mul(32)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| {
                RuntimeError::ResourceExhausted("metadata envelope allocation overflow".into())
            })?,
    )?;
    let object = canonical_json::from_slice_canonical(raw.as_ref().as_ref())?;
    Ok((object, reservation))
}

fn validate_commit(commit: &SemanticCommit, generation: &Generation) -> Result<(), RuntimeError> {
    commit.validate_runtime_profile()?;
    let supported = BTreeSet::from([
        CORE_FEATURE,
        PARQUET_FEATURE,
        SQLITE_COW_FEATURE,
        "otmp.refs.v1",
    ]);
    commit
        .required_reader_features_after_commit
        .require_supported(&supported)?;
    commit
        .required_writer_features_after_commit
        .require_supported(&supported)?;
    if commit.table_id != generation.table_id
        || commit.table_version != generation.table_version
        || commit.semantic_state_sha256 != generation.semantic_state_sha256
    {
        return Err(corrupt(
            "semantic commit does not match metadata generation",
        ));
    }
    let CanonicalValue::Object(mut body) = canonical_json::to_value(commit)? else {
        return Err(corrupt("commit body is not an object"));
    };
    body.remove("semantic_state_sha256");
    let bytes = canonical_json::encode(&CanonicalValue::Object(body))?;
    let hash = commit.previous_semantic_state_sha256.map_or_else(
        || genesis_state_hash(&bytes),
        |previous| next_state_hash(previous, &bytes),
    );
    if hash != commit.semantic_state_sha256 {
        return Err(corrupt("semantic state hash mismatch"));
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "generation and semantic ancestry validation share one authenticated traversal"
)]
pub(crate) async fn resolve<S: ObjectStore>(
    context: &ReadContext<S>,
    selection: MetadataSelection,
) -> Result<Selection, RuntimeError> {
    let uri = "_otmp/HEAD".parse()?;
    let metadata = context.stat(&uri).await?;
    if metadata.length == 0 || metadata.length > 1024 * 1024 {
        return Err(corrupt("HEAD exceeds the bounded reader envelope"));
    }
    let head_bytes = usize::try_from(metadata.length)
        .map_err(|_| RuntimeError::ResourceExhausted("HEAD length exceeds platform size".into()))?;
    let _head_reservation = context.reserve_bytes(
        head_bytes
            .checked_mul(32)
            .and_then(|size| size.checked_add(4096))
            .ok_or_else(|| RuntimeError::ResourceExhausted("HEAD reservation overflow".into()))?,
    )?;
    let raw = context.mutable_range(&uri, &metadata).await?;
    let head: Head = canonical_json::from_slice_canonical(&raw)?;
    let supported = BTreeSet::from([
        CORE_FEATURE,
        PARQUET_FEATURE,
        SQLITE_COW_FEATURE,
        "otmp.refs.v1",
    ]);
    head.validate(&supported)?;
    head.required_writer_features
        .require_supported(&supported)?;
    let anchor = HeadAnchor {
        table_id: head.table_id,
        table_version: head.table_version.0,
        root_revision: head.root_revision.0,
        semantic_state_sha256: head.semantic_state_sha256,
    };
    let wanted = match selection {
        MetadataSelection::Current => anchor.table_version,
        MetadataSelection::TableVersion(v) if v <= anchor.table_version => v,
        MetadataSelection::TableVersion(v) => return Err(RuntimeError::MetadataVersionNotFound(v)),
    };
    let (mut generation, mut generation_reservation): (Generation, _) =
        read_json(context, &head.metadata_generation, GENERATION_MEDIA_TYPE).await?;
    generation.validate_runtime_profile()?;
    if generation.table_id != head.table_id
        || generation.table_version != head.table_version
        || generation.semantic_state_sha256 != head.semantic_state_sha256
        || generation.semantic_commit != head.semantic_commit
    {
        return Err(corrupt("metadata generation does not match HEAD"));
    }
    let (mut commit, mut commit_reservation): (SemanticCommit, _) =
        read_json(context, &generation.semantic_commit, COMMIT_MEDIA_TYPE).await?;
    validate_commit(&commit, &generation)?;
    if commit.required_reader_features_after_commit != head.required_reader_features
        || commit.required_writer_features_after_commit != head.required_writer_features
    {
        return Err(corrupt("HEAD features disagree with semantic commit"));
    }
    let mut seen = BTreeSet::new();
    let mut seen_reservations = Vec::new();
    loop {
        seen_reservations.push(context.reserve_bytes(128)?);
        if !seen.insert(generation.generation_id) {
            return Err(corrupt("retained metadata generation cycle"));
        }
        if generation.table_version.0 == wanted {
            break;
        }
        let reference = generation
            .physical_parent
            .as_ref()
            .ok_or(RuntimeError::HistoryNotRetained(wanted))?;
        if reference.length.is_none() {
            return Err(corrupt("physical parent has no declared length"));
        }
        let (parent, parent_reservation): (Generation, _) =
            read_json(context, reference, GENERATION_MEDIA_TYPE).await?;
        parent.validate_runtime_profile()?;
        if parent.table_id != generation.table_id
            || parent.table_version > generation.table_version
            || parent.table_version.0 == generation.table_version.0
                && (parent.semantic_commit != generation.semantic_commit
                    || parent.semantic_state_sha256 != generation.semantic_state_sha256)
        {
            return Err(corrupt("invalid retained metadata ancestry"));
        }
        if parent.table_version.0 < wanted {
            return Err(RuntimeError::HistoryNotRetained(wanted));
        }
        if parent.table_version != generation.table_version {
            // A physical link can skip checkpoints. Follow semantic references
            // independently until it reaches the parent's exact committed state.
            let mut previous = commit;
            let mut previous_reservation = commit_reservation;
            loop {
                let semantic_reference = previous
                    .parent_commit
                    .as_ref()
                    .ok_or_else(|| corrupt("missing semantic parent link"))?;
                let (ancestor, ancestor_reservation): (SemanticCommit, _) =
                    read_json(context, semantic_reference, COMMIT_MEDIA_TYPE).await?;
                if ancestor.table_id != generation.table_id
                    || ancestor.table_version.0.checked_add(1) != Some(previous.table_version.0)
                    || Some(ancestor.semantic_state_sha256)
                        != previous.previous_semantic_state_sha256
                {
                    return Err(corrupt("physical and semantic ancestry disagree"));
                }
                if ancestor.table_version == parent.table_version {
                    if semantic_reference != &parent.semantic_commit {
                        return Err(corrupt(
                            "physical parent selects a different semantic commit",
                        ));
                    }
                    validate_commit(&ancestor, &parent)?;
                    commit = ancestor;
                    commit_reservation = ancestor_reservation;
                    break;
                }
                // Intermediate commits are also authenticated and internally valid.
                let intermediate = Generation {
                    table_version: ancestor.table_version,
                    semantic_state_sha256: ancestor.semantic_state_sha256,
                    ..generation.clone()
                };
                validate_commit(&ancestor, &intermediate)?;
                previous = ancestor;
                previous_reservation = ancestor_reservation;
            }
            drop(previous_reservation);
        }
        generation = parent;
        generation_reservation = parent_reservation;
    }
    let coordinates = MetadataCoordinates {
        table_id: generation.table_id,
        table_version: generation.table_version.0,
        commit_id: commit.commit_id,
        semantic_state_sha256: generation.semantic_state_sha256,
        main_snapshot_id: None, // Filled from the selected image's authenticated main ref.
    };
    Ok(Selection {
        generation,
        coordinates,
        anchor,
        commit,
        reservations: vec![generation_reservation, commit_reservation],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryObjectStore, InitializeRequest, Table};

    #[tokio::test]
    async fn authenticated_selection_preserves_current_anchor_for_history() {
        let store = InMemoryObjectStore::default();
        let table = Table::new(store.clone());
        let schema =
            serde_json::from_slice(include_bytes!("../../../conformance/sources/schema.json"))
                .unwrap();
        table
            .initialize(InitializeRequest::new(schema))
            .await
            .unwrap();
        let request = crate::TransactionRequest {
            idempotency_key: "property".into(),
            requirements: vec![crate::Requirement::PropertyIs {
                key: "key".into(),
                value: otmp_protocol::CanonicalValue::Null,
            }],
            operations: vec![crate::OperationRequest::SetProperties {
                operation_id: "set".into(),
                updates: std::collections::BTreeMap::from([(
                    "key".into(),
                    otmp_protocol::CanonicalValue::String("value".into()),
                )]),
                removals: vec![],
            }],
            commit_metadata: crate::CommitMetadata::default(),
        };
        table.transact(&request).await.unwrap();
        let context = ReadContext::new(store.clone(), crate::ReaderOptions::default()).unwrap();
        let reads = store.read_count();
        let selected = resolve(&context, MetadataSelection::TableVersion(0))
            .await
            .unwrap();
        assert_eq!(selected.anchor.table_version, 1);
        assert_eq!(selected.coordinates.table_version, 0);
        assert_eq!(selected.commit.table_version.0, 0);
        assert_eq!(
            store.read_count(),
            reads,
            "selection never calls full-object read"
        );
        assert!(matches!(
            resolve(&context, MetadataSelection::TableVersion(2)).await,
            Err(RuntimeError::MetadataVersionNotFound(2))
        ));
    }
}
