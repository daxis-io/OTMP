use super::{
    BTreeMap, BTreeSet, CanonicalValue, GENERATION_MEDIA_TYPE, Generation, Head, Id, JsonU64,
    LiveFile, ObjectReference, ObjectStore, PinnedTable, RefType, RuntimeError, SemanticCommit,
    Serialize, Sha256, StoredObject, Table, canonical_json, hash_from_blob, id_from_blob, image,
    nonnegative, string, transactions, verified_read,
};
use rusqlite::{Connection, OptionalExtension};

#[cfg(test)]
std::thread_local! {
    pub(super) static DATA_HASHES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(super) static VERIFIED_HASHES: std::cell::RefCell<BTreeMap<String, u64>> = const { std::cell::RefCell::new(BTreeMap::new()) };
    static SNAPSHOT_VISITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataSelection {
    Current,
    TableVersion(u64),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotSelection {
    Ref(String),
    SnapshotId(Id),
    SequenceNumber(u64),
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HeadAnchor {
    pub table_id: Id,
    pub table_version: u64,
    pub root_revision: u64,
    pub semantic_state_sha256: Sha256,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MetadataCoordinates {
    pub table_id: Id,
    pub table_version: u64,
    pub commit_id: Id,
    pub semantic_state_sha256: Sha256,
    pub main_snapshot_id: Option<Id>,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SnapshotDescriptor {
    pub snapshot_id: Id,
    pub parent_snapshot_id: Option<Id>,
    pub sequence_number: u64,
    pub committed_table_version: u64,
    pub schema_id: u32,
    pub partition_spec_id: u32,
    pub sort_order_id: u32,
    pub operation: String,
    pub committed_at_ms: i64,
    pub summary: CanonicalValue,
    pub metadata: CanonicalValue,
}
pub struct PinnedMetadata {
    state: PinnedTable,
    coordinates: MetadataCoordinates,
    anchor: HeadAnchor,
}
impl PinnedMetadata {
    #[must_use]
    pub const fn coordinates(&self) -> &MetadataCoordinates {
        &self.coordinates
    }
    #[must_use]
    pub const fn anchor(&self) -> &HeadAnchor {
        &self.anchor
    }
    pub fn resolve_snapshot(
        &self,
        selection: SnapshotSelection,
    ) -> Result<ResolvedSnapshot<'_>, RuntimeError> {
        self.state.resolve_snapshot(selection)
    }
}
pub struct ResolvedSnapshot<'pin> {
    state: &'pin PinnedTable,
    descriptor: Option<SnapshotDescriptor>,
}
impl ResolvedSnapshot<'_> {
    #[must_use]
    pub const fn descriptor(&self) -> Option<&SnapshotDescriptor> {
        self.descriptor.as_ref()
    }
    pub fn files(&self) -> Result<Vec<LiveFile>, RuntimeError> {
        let connection = image::open_readonly(&self.state.image.path)?;
        let mut files = Vec::new();
        for id in ancestry(&connection, self.descriptor.as_ref().map(|d| d.snapshot_id))? {
            let mut statement = connection.prepare("SELECT f.file_id,f.uri,f.file_format,f.file_size_bytes,f.record_count,f.content_sha256,s.sequence_number FROM otmp_files f JOIN otmp_snapshot_file_changes c USING(file_id) JOIN otmp_snapshots s USING(snapshot_id) WHERE c.snapshot_id=?1 AND c.change_kind='add'")?;
            for row in statement.query_map([id.as_bytes().as_slice()], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, Option<Vec<u8>>>(5)?,
                    r.get::<_, i64>(6)?,
                ))
            })? {
                let row = row?;
                files.push(LiveFile {
                    file_id: id_from_blob(row.0)?,
                    uri: row.1.parse()?,
                    file_format: row.2,
                    file_size_bytes: nonnegative(row.3, "length")?,
                    record_count: nonnegative(row.4, "record count")?,
                    content_sha256: row.5.map(hash_from_blob).transpose()?,
                    sequence_number: nonnegative(row.6, "sequence")?,
                });
            }
        }
        files.sort_by_key(|f| (f.sequence_number, f.file_id));
        Ok(files)
    }
}
impl PinnedTable {
    pub fn resolve_snapshot(
        &self,
        selection: SnapshotSelection,
    ) -> Result<ResolvedSnapshot<'_>, RuntimeError> {
        let connection = image::open_readonly(&self.image.path)?;
        let id = match selection {
            SnapshotSelection::Ref(name) => {
                transactions::ref_row(&connection, &name)?
                    .ok_or(RuntimeError::RefNotFound(name))?
                    .1
            }
            SnapshotSelection::SnapshotId(id) => Some(id),
            SnapshotSelection::SequenceNumber(sequence) => {
                let sequence =
                    i64::try_from(sequence).map_err(|_| RuntimeError::SnapshotNotFound)?;
                let mut stmt = connection
                    .prepare("SELECT snapshot_id FROM otmp_snapshots WHERE sequence_number=?1")?;
                let ids = stmt
                    .query_map([sequence], |r| r.get::<_, Vec<u8>>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                match ids.as_slice() {
                    [] => return Err(RuntimeError::SnapshotNotFound),
                    [id] => Some(id_from_blob(id.clone())?),
                    _ => return Err(RuntimeError::Corrupt("duplicate snapshot sequence".into())),
                }
            }
        };
        let descriptor = id.map(|id| descriptor(&connection, id)).transpose()?;
        Ok(ResolvedSnapshot {
            state: self,
            descriptor,
        })
    }
    fn anchor(&self) -> HeadAnchor {
        HeadAnchor {
            table_id: self.head.table_id,
            table_version: self.head.table_version.0,
            root_revision: self.head.root_revision.0,
            semantic_state_sha256: self.head.semantic_state_sha256,
        }
    }
    fn coordinates(&self) -> MetadataCoordinates {
        MetadataCoordinates {
            table_id: self.head.table_id,
            table_version: self.head.table_version.0,
            commit_id: self.commit.commit_id,
            semantic_state_sha256: self.head.semantic_state_sha256,
            main_snapshot_id: self.current_main,
        }
    }
}
fn descriptor(connection: &Connection, id: Id) -> Result<SnapshotDescriptor, RuntimeError> {
    let row = connection.query_row("SELECT parent_snapshot_id,sequence_number,committed_table_version,schema_id,partition_spec_id,sort_order_id,operation,committed_at_ms,summary_json,metadata_json FROM otmp_snapshots WHERE snapshot_id=?1",[id.as_bytes().as_slice()], |r| Ok((r.get::<_,Option<Vec<u8>>>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get::<_,String>(8)?,r.get::<_,String>(9)?))).optional()?.ok_or(RuntimeError::SnapshotNotFound)?;
    Ok(SnapshotDescriptor {
        snapshot_id: id,
        parent_snapshot_id: row.0.map(id_from_blob).transpose()?,
        sequence_number: nonnegative(row.1, "sequence")?,
        committed_table_version: nonnegative(row.2, "version")?,
        schema_id: row.3,
        partition_spec_id: row.4,
        sort_order_id: row.5,
        operation: row.6,
        committed_at_ms: row.7,
        summary: canonical_json::parse_canonical(row.8.as_bytes())?,
        metadata: canonical_json::parse_canonical(row.9.as_bytes())?,
    })
}

pub(crate) fn ancestry(
    connection: &Connection,
    mut tip: Option<Id>,
) -> Result<Vec<Id>, RuntimeError> {
    let mut seen = BTreeSet::new();
    let mut chain = Vec::new();
    let mut last_sequence = u64::MAX;
    while let Some(id) = tip {
        if !seen.insert(id) {
            return Err(RuntimeError::Corrupt("snapshot ancestry cycle".into()));
        }
        let snapshot = descriptor(connection, id).map_err(|e| match e {
            RuntimeError::SnapshotNotFound => {
                RuntimeError::Corrupt("dangling snapshot ancestry".into())
            }
            other => other,
        })?;
        if snapshot.sequence_number >= last_sequence {
            return Err(RuntimeError::Corrupt(
                "nondecreasing snapshot ancestry".into(),
            ));
        }
        last_sequence = snapshot.sequence_number;
        chain.push(id);
        tip = snapshot.parent_snapshot_id;
    }
    Ok(chain)
}
pub(super) fn validate_append_rebase(
    parent: &PinnedTable,
    name: &str,
    original: Option<(RefType, Option<Id>)>,
    version: u64,
) -> Result<(), RuntimeError> {
    let connection = image::open_readonly(&parent.image.path)?;
    let Some((RefType::Branch, old)) = original else {
        return Err(RuntimeError::SemanticConflict(
            "append target was not a branch".into(),
        ));
    };
    let Some((RefType::Branch, current)) = transactions::ref_row(&connection, name)? else {
        return Err(RuntimeError::SemanticConflict(
            "append target removed".into(),
        ));
    };
    let chain = ancestry(&connection, current)?;
    if old.is_some_and(|id| !chain.contains(&id)) {
        return Err(RuntimeError::SemanticConflict(
            "target is not an append descendant".into(),
        ));
    }
    // Even moving away and back to the same tip invalidates the prepared append.
    let mut stmt = connection
        .prepare("SELECT operation_summary_json FROM otmp_commits WHERE table_version>?1")?;
    for row in stmt.query_map(
        [i64::try_from(version).map_err(|_| RuntimeError::Corrupt("version overflow".into()))?],
        |r| r.get::<_, String>(0),
    )? {
        let operations: Vec<CanonicalValue> =
            canonical_json::from_slice_canonical(row?.as_bytes())?;
        for operation in operations {
            if let CanonicalValue::Object(fields) = operation {
                if fields.get("ref") == Some(&string(name))
                    && fields.get("type") != Some(&string("commit_snapshot"))
                {
                    return Err(RuntimeError::SemanticConflict(
                        "target ref changed during append".into(),
                    ));
                }
                if fields.get("type") == Some(&string("set_current_schema")) {
                    return Err(RuntimeError::SemanticConflict(
                        "current schema changed during append".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationScope {
    Current,
    RetainedHistory,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VerificationReport {
    pub scope: VerificationScope,
    pub anchor: HeadAnchor,
    pub completed: bool,
    pub generations_checked: u64,
    pub commits_checked: u64,
    pub snapshots_checked: u64,
    pub objects_checked: u64,
    pub bytes_checked: u64,
}
impl<S: ObjectStore> Table<S> {
    pub async fn pin_metadata(
        &self,
        selection: MetadataSelection,
    ) -> Result<PinnedMetadata, RuntimeError> {
        let mut state = self.pin().await?;
        let anchor = state.anchor();
        if let MetadataSelection::TableVersion(version) = selection {
            if version > anchor.table_version {
                return Err(RuntimeError::MetadataVersionNotFound(version));
            }
            let mut seen = BTreeSet::from([state.generation.generation_id]);
            while state.head.table_version.0 > version {
                let reference = state
                    .generation
                    .physical_parent
                    .clone()
                    .ok_or(RuntimeError::HistoryNotRetained(version))?;
                state = self.load_parent(&state, &reference, &mut seen).await?;
            }
            if state.head.table_version.0 != version {
                return Err(RuntimeError::HistoryNotRetained(version));
            }
        }
        Ok(PinnedMetadata {
            coordinates: state.coordinates(),
            state,
            anchor,
        })
    }
    async fn load_parent(
        &self,
        child: &PinnedTable,
        reference: &ObjectReference,
        seen: &mut BTreeSet<Id>,
    ) -> Result<PinnedTable, RuntimeError> {
        if reference.media_type.as_deref() != Some(GENERATION_MEDIA_TYPE)
            || reference.length.is_none()
        {
            return Err(RuntimeError::Corrupt(
                "invalid physical parent reference".into(),
            ));
        }
        let object = self.read_metadata(reference).await?;
        let generation: Generation = canonical_json::from_slice_canonical(&object.bytes)?;
        validate_generation_edge(&child.generation, &generation, seen)?;
        let commit_object = self.read_metadata(&generation.semantic_commit).await?;
        let commit: SemanticCommit = canonical_json::from_slice_canonical(&commit_object.bytes)?;
        // This internal envelope only drives common validation; historical coordinates never expose a root revision.
        let head = Head {
            table_version: generation.table_version,
            semantic_state_sha256: generation.semantic_state_sha256,
            semantic_commit: generation.semantic_commit.clone(),
            metadata_generation: reference.clone(),
            required_reader_features: commit.required_reader_features_after_commit.clone(),
            required_writer_features: commit.required_writer_features_after_commit.clone(),
            ..child.head.clone()
        };
        self.load_pin_objects(
            StoredObject {
                bytes: child.raw_head.clone(),
                version: child.head_version.clone(),
            },
            head,
            commit,
            generation,
        )
        .await
    }
    async fn verify_retained_commit_tail(
        &self,
        state: &PinnedTable,
        seen: &mut BTreeSet<Id>,
    ) -> Result<(), RuntimeError> {
        let mut reference = Some(state.head.semantic_commit.clone());
        let mut commits = Vec::new();
        let mut previous_version = None;
        while let Some(r) = reference.take() {
            if r.media_type.as_deref() != Some(super::COMMIT_MEDIA_TYPE) || r.length.is_none() {
                return Err(RuntimeError::Corrupt(
                    "invalid retained commit reference".into(),
                ));
            }
            let object = self.read_metadata(&r).await?;
            let commit: SemanticCommit = canonical_json::from_slice_canonical(&object.bytes)?;
            commit.validate_runtime_profile()?;
            if commit.table_id != state.head.table_id
                || previous_version
                    .is_some_and(|v: u64| commit.table_version.0.checked_add(1) != Some(v))
            {
                return Err(RuntimeError::Corrupt(
                    "invalid retained semantic ancestry".into(),
                ));
            }
            let hash = if let Some(previous) = commit.previous_semantic_state_sha256 {
                super::next_state_hash(previous, &super::commit_body(&commit)?)
            } else {
                super::genesis_state_hash(&super::commit_body(&commit)?)
            };
            if hash != commit.semantic_state_sha256 {
                return Err(RuntimeError::Corrupt(
                    "retained semantic state hash mismatch".into(),
                ));
            }
            commit
                .required_reader_features_after_commit
                .require_supported(&BTreeSet::from([
                    super::CORE_FEATURE,
                    super::PARQUET_FEATURE,
                    super::SQLITE_COW_FEATURE,
                    "otmp.refs.v1",
                ]))?;
            seen.insert(commit.commit_id);
            previous_version = Some(commit.table_version.0);
            reference.clone_from(&commit.parent_commit);
            commits.push((r, commit));
        }
        commits.reverse();
        let mut reconstructed: Option<image::CheckpointImage> = None;
        for (reference, commit) in commits {
            let checkpoint = if let Some(parent) = &reconstructed {
                let row = image::open_readonly(&parent.path)?.query_row(
                    "SELECT semantic_state_sha256 FROM otmp_meta",
                    [],
                    |r| r.get(0),
                )?;
                if Some(hash_from_blob(row)?) != commit.previous_semantic_state_sha256 {
                    return Err(RuntimeError::Corrupt(
                        "retained semantic hash chain mismatch".into(),
                    ));
                }
                image::replay_semantic_commit(&parent.path, &commit, &reference.uri)?
            } else {
                image::replay_genesis(&commit, &reference.uri)?
            };
            image::validate_commit_projection(&checkpoint.path, &commit)?;
            reconstructed = Some(checkpoint);
        }
        image::compare_logical_images(
            &reconstructed
                .ok_or_else(|| RuntimeError::Corrupt("empty semantic history".into()))?
                .path,
            &state.image.path,
        )
    }

    pub async fn verify(&self) -> Result<(), RuntimeError> {
        self.verify_with_report(VerificationScope::Current)
            .await
            .map(|_| ())
    }
    pub async fn verify_history(&self) -> Result<(), RuntimeError> {
        self.verify_with_report(VerificationScope::RetainedHistory)
            .await
            .map(|_| ())
    }
    pub async fn verify_with_report(
        &self,
        scope: VerificationScope,
    ) -> Result<VerificationReport, RuntimeError> {
        let mut table = Self::new(self.store.clone());
        table.metadata_cache = Some(std::sync::Arc::new(
            tokio::sync::Mutex::new(BTreeMap::new()),
        ));
        let mut state = table.pin().await?;
        let mut report = VerificationReport {
            scope,
            anchor: state.anchor(),
            completed: false,
            generations_checked: 0,
            commits_checked: 0,
            snapshots_checked: 0,
            objects_checked: 1, // the single anchored HEAD read
            bytes_checked: state.raw_head.len() as u64,
        };
        let mut seen = BTreeSet::from([state.generation.generation_id]);
        let mut commits = BTreeSet::new();
        let mut snapshots = BTreeSet::new();
        let mut file_references = BTreeMap::new();
        loop {
            report.generations_checked += 1;
            commits.insert(state.commit.commit_id);
            collect_snapshot_files(&state, scope, &mut snapshots, &mut file_references)?;
            if scope == VerificationScope::Current {
                break;
            }
            let Some(reference) = state.generation.physical_parent.clone() else {
                table
                    .verify_retained_commit_tail(&state, &mut commits)
                    .await?;
                break;
            };
            let parent = table.load_parent(&state, &reference, &mut seen).await?;
            image::validate_transition(&parent.image.path, &state.image.path, &state.commit)?;
            state = parent;
        }
        // Metadata objects share validated bytes, while user bytes are discarded
        // immediately. A URI appearing in both roles still uses one read/hash.
        let cache = table.metadata_cache.as_ref().expect("verification cache");
        for reference in file_references.values() {
            if let Some(cached) = cache.lock().await.get(reference.uri.as_str()) {
                cached.check(reference)?;
            } else {
                let object = verified_read(&self.store, reference).await?;
                report.objects_checked += 1;
                report.bytes_checked += object.bytes.len() as u64;
            }
        }
        let objects = cache.lock().await;
        report.objects_checked += objects.len() as u64;
        report.bytes_checked += objects
            .values()
            .map(|o| o.object.bytes.len() as u64)
            .sum::<u64>();
        report.commits_checked = commits.len() as u64;
        report.snapshots_checked = snapshots.len() as u64;
        report.completed = true;
        Ok(report)
    }
}
fn collect_snapshot_files(
    state: &PinnedTable,
    scope: VerificationScope,
    snapshots: &mut BTreeSet<Id>,
    file_references: &mut BTreeMap<String, ObjectReference>,
) -> Result<(), RuntimeError> {
    let connection = image::open_readonly(&state.image.path)?;
    let query = if scope == VerificationScope::RetainedHistory {
        "SELECT snapshot_id FROM otmp_snapshots"
    } else {
        "SELECT DISTINCT snapshot_id FROM otmp_refs WHERE snapshot_id IS NOT NULL"
    };
    let mut stmt = connection.prepare(query)?;
    let mut pending = stmt
        .query_map([], |r| r.get::<_, Vec<u8>>(0))?
        .map(|row| id_from_blob(row?))
        .collect::<Result<Vec<_>, RuntimeError>>()?;
    while let Some(id) = pending.pop() {
        // Snapshot rows and changes are immutable. Relational transition
        // validation below checks that older images agree with this image.
        if !snapshots.insert(id) {
            continue;
        }
        #[cfg(test)]
        SNAPSHOT_VISITS.with(|count| count.set(count.get() + 1));
        if let Some(parent) = descriptor(&connection, id)?.parent_snapshot_id {
            pending.push(parent);
        }
        let mut files = connection.prepare(
                    "SELECT f.uri,f.content_sha256,f.file_size_bytes FROM otmp_files f JOIN otmp_snapshot_file_changes c USING(file_id) WHERE c.snapshot_id=?1 AND c.change_kind='add'"
                )?;
        for row in files.query_map([id.as_bytes().as_slice()], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<Vec<u8>>>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })? {
            let (uri, hash, length) = row?;
            record_file_reference(
                file_references,
                ObjectReference {
                    uri: uri.parse()?,
                    sha256: hash_from_blob(
                        hash.ok_or_else(|| RuntimeError::Corrupt("missing content hash".into()))?,
                    )?,
                    length: Some(JsonU64(nonnegative(length, "file length")?)),
                    media_type: None,
                },
            )?;
        }
    }
    Ok(())
}

fn validate_generation_edge(
    child: &Generation,
    parent: &Generation,
    seen: &mut BTreeSet<Id>,
) -> Result<(), RuntimeError> {
    if !seen.insert(parent.generation_id)
        || child.table_id != parent.table_id
        || parent.table_version.0 > child.table_version.0
        || (parent.table_version == child.table_version
            && (parent.semantic_state_sha256 != child.semantic_state_sha256
                || parent.semantic_commit != child.semantic_commit))
    {
        return Err(RuntimeError::Corrupt(
            "invalid retained generation ancestry".into(),
        ));
    }
    Ok(())
}

pub(super) type VerifiedMetadataCache = BTreeMap<String, VerifiedMetadata>;

pub(super) struct VerifiedMetadata {
    object: std::sync::Arc<StoredObject>,
    sha256: Sha256,
}
impl VerifiedMetadata {
    fn check(&self, reference: &ObjectReference) -> Result<(), RuntimeError> {
        if self.sha256 != reference.sha256
            || reference
                .length
                .is_some_and(|length| length.0 != self.object.bytes.len() as u64)
        {
            return Err(RuntimeError::Corrupt(format!(
                "conflicting object reference: {}",
                reference.uri
            )));
        }
        Ok(())
    }
}
impl<S: ObjectStore> Table<S> {
    pub(super) async fn read_metadata(
        &self,
        reference: &ObjectReference,
    ) -> Result<std::sync::Arc<StoredObject>, RuntimeError> {
        let Some(cache) = &self.metadata_cache else {
            return Ok(std::sync::Arc::new(
                verified_read(&self.store, reference).await?,
            ));
        };
        let mut objects = cache.lock().await;
        if let Some(cached) = objects.get(reference.uri.as_str()) {
            cached.check(reference)?;
            return Ok(cached.object.clone());
        }
        let object = std::sync::Arc::new(verified_read(&self.store, reference).await?);
        objects.insert(
            reference.uri.to_string(),
            VerifiedMetadata {
                object: object.clone(),
                sha256: reference.sha256,
            },
        );
        Ok(object)
    }
}

fn record_file_reference(
    references: &mut BTreeMap<String, ObjectReference>,
    reference: ObjectReference,
) -> Result<(), RuntimeError> {
    if let Some(previous) = references.get(reference.uri.as_str()) {
        if previous.sha256 != reference.sha256 || previous.length != reference.length {
            return Err(RuntimeError::Corrupt(format!(
                "conflicting object reference: {}",
                reference.uri
            )));
        }
    } else {
        references.insert(reference.uri.to_string(), reference);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{head_key, new_id, object_reference};
    use crate::{
        CommitMetadata, InitializeRequest, OperationRequest, Requirement, TransactionRequest,
    };
    use crate::{ConditionalWriteOutcome, InMemoryObjectStore, StorageError};
    use otmp_protocol::object_hash;

    async fn setup() -> (InMemoryObjectStore, Table<InMemoryObjectStore>) {
        let store = InMemoryObjectStore::default();
        let table = Table::new(store.clone());
        let schema =
            serde_json::from_slice(include_bytes!("../../../conformance/sources/schema.json"))
                .unwrap();
        table
            .initialize(InitializeRequest::new(schema))
            .await
            .unwrap();
        (store, table)
    }
    async fn repack(store: &InMemoryObjectStore, change: impl FnOnce(&mut Generation)) {
        let raw = store.read(&head_key().unwrap()).await.unwrap();
        let mut head: Head = canonical_json::from_slice_canonical(&raw.bytes).unwrap();
        let mut generation: Generation = canonical_json::from_slice_canonical(
            &store
                .read(&head.metadata_generation.uri)
                .await
                .unwrap()
                .bytes,
        )
        .unwrap();
        generation.physical_parent = Some(head.metadata_generation.clone());
        generation.generation_id = new_id();
        change(&mut generation);
        let bytes = canonical_json::to_vec(&generation).unwrap();
        let uri = format!(
            "_otmp/generations/{}/{}.json",
            generation.table_version.0, generation.generation_id
        )
        .parse()
        .unwrap();
        store.create_bytes(&uri, &bytes).await.unwrap();
        head.metadata_generation = object_reference(
            uri,
            object_hash(&bytes),
            bytes.len() as u64,
            GENERATION_MEDIA_TYPE,
        );
        head.root_revision.0 += 1;
        assert!(matches!(
            store
                .replace_head(&raw.version, &canonical_json::to_vec(&head).unwrap())
                .await,
            ConditionalWriteOutcome::Applied { .. }
        ));
    }
    async fn property(table: &Table<InMemoryObjectStore>) {
        table
            .transact(&TransactionRequest {
                idempotency_key: "one".into(),
                requirements: vec![Requirement::PropertyIs {
                    key: "owner".into(),
                    value: CanonicalValue::Null,
                }],
                operations: vec![OperationRequest::SetProperties {
                    operation_id: "set".into(),
                    updates: [("owner".into(), CanonicalValue::Bool(true))].into(),
                    removals: vec![],
                }],
                commit_metadata: CommitMetadata::default(),
            })
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn metadata_cache_shares_bytes_and_checks_each_reference() {
        let (store, mut table) = setup().await;
        let head: Head = canonical_json::from_slice_canonical(
            &store.read(&head_key().unwrap()).await.unwrap().bytes,
        )
        .unwrap();
        table.metadata_cache = Some(std::sync::Arc::new(
            tokio::sync::Mutex::new(BTreeMap::new()),
        ));
        let before = store.read_count();
        let first = table.read_metadata(&head.semantic_commit).await.unwrap();
        let second = table.read_metadata(&head.semantic_commit).await.unwrap();
        assert!(std::sync::Arc::ptr_eq(&first, &second));
        for reference in [
            ObjectReference {
                sha256: Sha256::digest(b"wrong"),
                ..head.semantic_commit.clone()
            },
            ObjectReference {
                length: Some(JsonU64(1)),
                ..head.semantic_commit.clone()
            },
        ] {
            assert!(matches!(
                table.read_metadata(&reference).await,
                Err(RuntimeError::Corrupt(_))
            ));
        }
        assert_eq!(store.read_count() - before, 1);
    }

    #[test]
    fn repeated_file_uris_must_agree_on_hash_and_length() {
        let original = ObjectReference {
            uri: "data/one.parquet".parse().unwrap(),
            sha256: Sha256::digest(b"one"),
            length: Some(JsonU64(3)),
            media_type: None,
        };
        let mut references = BTreeMap::new();
        record_file_reference(&mut references, original.clone()).unwrap();
        record_file_reference(&mut references, original.clone()).unwrap();
        assert_eq!(references.len(), 1);
        for conflicting in [
            ObjectReference {
                sha256: Sha256::digest(b"two"),
                ..original.clone()
            },
            ObjectReference {
                length: Some(JsonU64(4)),
                ..original.clone()
            },
        ] {
            assert!(matches!(
                record_file_reference(&mut references, conflicting),
                Err(RuntimeError::Corrupt(_))
            ));
        }
        record_file_reference(
            &mut references,
            ObjectReference {
                uri: "data/two.parquet".parse().unwrap(),
                ..original
            },
        )
        .unwrap();
        assert_eq!(references.len(), 2);
    }

    #[tokio::test]
    async fn retained_verification_visits_and_hashes_each_snapshot_and_file_once() {
        for count in [8_u64, 16, 32] {
            let (store, table) = setup().await;
            let source = tempfile::NamedTempFile::new().unwrap();
            // Equal contents deliberately get distinct URIs: hashes are not identities.
            std::fs::write(source.path(), b"same bytes").unwrap();
            for index in 0..count {
                table
                    .append_files(&crate::AppendRequest::new(
                        format!("append-{index}"),
                        vec![crate::AppendFile {
                            source_path: source.path().into(),
                            fingerprint: crate::SourceFingerprint {
                                sha256: Sha256::digest(b"same bytes"),
                                length: 10,
                            },
                            format: crate::FileFormat::Parquet,
                            record_count: 1,
                            schema_id: 1,
                            partition_spec_id: 0,
                            sort_order_id: 0,
                            partition_values: BTreeMap::new(),
                            metrics: vec![],
                            metadata: BTreeMap::new(),
                        }],
                    ))
                    .await
                    .unwrap();
            }
            DATA_HASHES.with(|c| c.set(0));
            SNAPSHOT_VISITS.with(|c| c.set(0));
            let before = store.read_count();
            let report = table
                .verify_with_report(VerificationScope::RetainedHistory)
                .await
                .unwrap();
            assert_eq!(
                DATA_HASHES.with(std::cell::Cell::get),
                count,
                "hash work at {count} snapshots"
            );
            assert_eq!(
                SNAPSHOT_VISITS.with(std::cell::Cell::get),
                count,
                "snapshot visits at {count} snapshots"
            );
            assert_eq!(report.snapshots_checked, count);
            assert_eq!(report.generations_checked, count + 1);
            assert_eq!(store.read_count() - before, report.objects_checked);
        }
    }

    #[tokio::test]
    async fn same_version_generations_have_separate_anchor_and_deduplicated_verification() {
        let (store, table) = setup().await;
        for _ in 0..8 {
            repack(&store, |_| {}).await;
        }
        let selected = table
            .pin_metadata(MetadataSelection::TableVersion(0))
            .await
            .unwrap();
        assert_eq!(selected.anchor().root_revision, 8);
        assert_eq!(selected.coordinates().table_version, 0);
        let before = store.read_count();
        VERIFIED_HASHES.with(|counts| counts.borrow_mut().clear());
        let report = table
            .verify_with_report(VerificationScope::RetainedHistory)
            .await
            .unwrap();
        VERIFIED_HASHES.with(|counts| {
            for (uri, count) in counts.borrow().iter() {
                assert_eq!(*count, 1, "repeated content verification for {uri}");
            }
        });
        assert_eq!(report.generations_checked, 9);
        assert_eq!(report.commits_checked, 1);
        assert_eq!(store.read_count() - before, report.objects_checked);
    }
    #[tokio::test]
    async fn retention_boundary_is_distinct_from_missing_explicit_parent() {
        let (store, table) = setup().await;
        property(&table).await;
        repack(&store, |g| g.physical_parent = None).await;
        let report = table
            .verify_with_report(VerificationScope::RetainedHistory)
            .await
            .unwrap();
        assert_eq!(report.generations_checked, 1);
        assert_eq!(report.commits_checked, 2);
        assert!(matches!(
            table.pin_metadata(MetadataSelection::TableVersion(0)).await,
            Err(RuntimeError::HistoryNotRetained(0))
        ));
        repack(&store, |g| {
            g.physical_parent.as_mut().unwrap().uri =
                "_otmp/generations/missing.json".parse().unwrap();
        })
        .await;
        assert!(matches!(
            table.pin_metadata(MetadataSelection::TableVersion(0)).await,
            Err(RuntimeError::Storage(StorageError::NotFound(_)))
        ));
        table.verify().await.unwrap();
    }
    #[tokio::test]
    async fn unknown_required_feature_is_rejected_before_metadata_reads() {
        let (store, table) = setup().await;
        let object = store.read(&head_key().unwrap()).await.unwrap();
        let mut head: Head = canonical_json::from_slice_canonical(&object.bytes).unwrap();
        let mut features = head.required_reader_features.as_slice().to_vec();
        features.push("otmp.unknown.v1".into());
        features.sort();
        head.required_reader_features = otmp_protocol::FeatureSet::new(features).unwrap();
        store.replace_object_for_test(&head_key().unwrap(), canonical_json::to_vec(&head).unwrap());
        let before = store.read_count();
        assert!(table.pin().await.is_err());
        assert_eq!(store.read_count() - before, 1);
    }

    #[tokio::test]
    async fn null_and_real_empty_snapshots_are_distinct_without_file_queries() {
        let (_, table) = setup().await;
        let state = table.pin().await.unwrap();
        assert!(
            state
                .resolve_snapshot(SnapshotSelection::Ref("main".into()))
                .unwrap()
                .descriptor()
                .is_none()
        );
        let id = new_id();
        let connection = Connection::open(&state.image.path).unwrap();
        connection.execute("INSERT INTO otmp_snapshots(snapshot_id,parent_snapshot_id,sequence_number,schema_id,partition_spec_id,sort_order_id,operation,committed_table_version,committed_at_ms) VALUES(?1,NULL,1,1,0,0,'append',1,1)",[id.as_bytes().as_slice()]).unwrap();
        let selected = state
            .resolve_snapshot(SnapshotSelection::SnapshotId(id))
            .unwrap();
        assert!(selected.descriptor().is_some());
        assert!(selected.files().unwrap().is_empty());
        connection
            .execute("DROP TABLE otmp_snapshot_file_changes", [])
            .unwrap();
        assert!(
            state
                .resolve_snapshot(SnapshotSelection::SequenceNumber(1))
                .unwrap()
                .descriptor()
                .is_some()
        );
    }

    #[tokio::test]
    async fn parsed_generation_graph_rejects_cycles_increases_and_divergence() {
        let (_, table) = setup().await;
        let pinned = table.pin().await.unwrap();
        let child = pinned.generation;
        let mut seen = BTreeSet::from([child.generation_id]);
        assert!(validate_generation_edge(&child, &child, &mut seen).is_err());
        let mut parent = child.clone();
        parent.generation_id = new_id();
        parent.table_version.0 = 1;
        assert!(validate_generation_edge(&child, &parent, &mut BTreeSet::new()).is_err());
        parent.table_version.0 = 0;
        parent.semantic_state_sha256 = Sha256::digest(b"different");
        assert!(validate_generation_edge(&child, &parent, &mut BTreeSet::new()).is_err());
    }
}
