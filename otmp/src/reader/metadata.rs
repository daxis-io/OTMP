//! A retained, authenticated metadata generation. Every SQL read uses verified pages.
use super::cache::Reservation;
use super::{AuthenticatedImage, ReadContext, ReaderStatistics};
use crate::reader_engine::{Engine, PageSource};
use crate::{
    FileMetric, HeadAnchor, LiveFile, MetadataCoordinates, MetadataSelection, ObjectStore,
    RuntimeError, SnapshotDescriptor, SnapshotSelection,
};
use async_trait::async_trait;
use otmp_protocol::{FeatureSet, Id, Schema, SemanticCommit, Sha256, canonical_json};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use turso_core::{Numeric, Value};

#[cfg(not(test))]
const COMMIT_OPERATION_PAGE_ROWS: usize = 4096;
#[cfg(test)]
const COMMIT_OPERATION_PAGE_ROWS: usize = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileCursor {
    pub(crate) pin: Id,
    pub(crate) ranges: Vec<CanonicalRange>,
    pub(crate) state: CursorState,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileMetricRange {
    Int32 {
        field_id: u32,
        lower: Bound<i32>,
        upper: Bound<i32>,
    },
    Int64 {
        field_id: u32,
        lower: Bound<i64>,
        upper: Bound<i64>,
    },
    /// Signed days from the Unix epoch, matching Arrow `Date32`.
    Date {
        field_id: u32,
        lower: Bound<i32>,
        upper: Bound<i32>,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RangeType {
    Int32,
    Int64,
    Date,
}
impl RangeType {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::Date => "date",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CanonicalRange {
    pub(crate) field_id: u32,
    pub(crate) range_type: RangeType,
    pub(crate) lower: Bound<i64>,
    pub(crate) upper: Bound<i64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CursorState {
    Branch {
        file: Id,
    },
    Snapshot {
        snapshot: Id,
        before_sequence: u64,
        file: Option<Id>,
    },
}
#[derive(Clone, Debug)]
pub struct ReaderFile {
    pub file: LiveFile,
    pub schema_id: u32,
    pub metrics: Vec<FileMetric>,
}
pub struct FileBatch {
    /// Files retained by catalog pruning. Callers may apply another conservative
    /// metric-pruning pass before opening the corresponding data objects.
    pub files: Vec<ReaderFile>,
    /// Continuation for the catalog query, including historical ancestry.
    pub next_cursor: Option<FileCursor>,
    pub(crate) _reservations: Vec<Reservation>,
}
struct RetainedSchema {
    schema: Arc<Schema>,
    _reservation: Reservation,
}
pub struct MetadataReader<S> {
    pub(crate) context: ReadContext<S>,
    pub(crate) engine: Engine,
    schema: Arc<Schema>,
    schemas: tokio::sync::Mutex<BTreeMap<u32, RetainedSchema>>,
    coordinates: MetadataCoordinates,
    pub(crate) last_sequence: u64,
    anchor: HeadAnchor,
    pub(crate) head: otmp_protocol::Head,
    pub(crate) raw_head: Vec<u8>,
    pub(crate) head_version: crate::ObjectVersion,
    pub(crate) generation: otmp_protocol::Generation,
    pub(crate) image: Arc<AuthenticatedImage<S>>,
    pub(crate) snapshot: Option<SnapshotDescriptor>,
    pub(crate) branch: Option<String>,
    pub(crate) pin_id: Id,
    _reservations: Vec<Reservation>,
}

#[async_trait]
impl<S: ObjectStore> PageSource for AuthenticatedImage<S> {
    fn length(&self) -> u64 {
        self.length()
    }
    fn maximum_record_bytes(&self) -> usize {
        self.maximum_record_bytes()
    }
    fn reserve(&self, bytes: usize) -> Result<Reservation, RuntimeError> {
        self.reserve_engine_bytes(bytes)
    }
    async fn read_page(&self, page: u64, out: &mut [u8]) -> Result<(), RuntimeError> {
        self.copy_page(page, out).await
    }
}

impl<S: ObjectStore> MetadataReader<S> {
    pub(crate) async fn idempotency(
        &self,
        key: &str,
    ) -> Result<Option<(Sha256, String, Sha256)>, RuntimeError> {
        let rows = self
            .engine
            .query(
                "SELECT i.intent_sha256,i.result_json,c.semantic_state_sha256 FROM otmp_idempotency i JOIN otmp_commits c ON c.commit_id=i.commit_id AND c.table_version=i.table_version WHERE i.idempotency_key=?1",
                vec![Value::build_text(key.to_owned())],
                1,
                1024 * 1024,
            )
            .await?;
        rows.first()
            .map(|row| Ok((hash(&row[0])?, text(&row[1])?, hash(&row[2])?)))
            .transpose()
    }

    pub(crate) async fn ref_row(
        &self,
        name: &str,
    ) -> Result<Option<(crate::RefType, Option<Id>)>, RuntimeError> {
        let rows = self
            .engine
            .query(
                "SELECT ref_type,snapshot_id FROM otmp_refs WHERE ref_name=?1",
                vec![Value::build_text(name.to_owned())],
                1,
                4096,
            )
            .await?;
        rows.first()
            .map(|row| {
                Ok((
                    match text(&row[0])?.as_str() {
                        "branch" => crate::RefType::Branch,
                        "tag" => crate::RefType::Tag,
                        _ => return Err(corrupt("invalid ref type")),
                    },
                    if matches!(row[1], Value::Null) {
                        None
                    } else {
                        Some(id(&row[1])?)
                    },
                ))
            })
            .transpose()
    }

    pub(crate) async fn commit_operations_page_after(
        &self,
        version: u64,
    ) -> Result<Vec<(u64, String)>, RuntimeError> {
        self.engine
            .query(
                "SELECT table_version,operation_summary_json FROM otmp_commits WHERE table_version>?1 AND table_version<=?2 ORDER BY table_version LIMIT ?3",
                vec![
                    sqlite(version)?,
                    sqlite(self.coordinates.table_version)?,
                    sqlite(COMMIT_OPERATION_PAGE_ROWS as u64)?,
                ],
                COMMIT_OPERATION_PAGE_ROWS,
                8 * 1024 * 1024,
            )
            .await?
            .iter()
            .map(|row| Ok((uint(&row[0])?, text(&row[1])?)))
            .collect()
    }

    pub(crate) async fn snapshot_descends_from(
        &self,
        mut tip: Option<Id>,
        ancestor: Id,
    ) -> Result<bool, RuntimeError> {
        if tip == Some(ancestor) {
            return Ok(true);
        }
        let mut last_sequence = u64::MAX;
        while let Some(snapshot_id) = tip {
            let rows = self
                .engine
                .query(
                    "SELECT parent_snapshot_id,sequence_number FROM otmp_snapshots WHERE snapshot_id=?1",
                    vec![Value::Blob(snapshot_id.as_bytes().to_vec())],
                    1,
                    4096,
                )
                .await?;
            let row = rows
                .first()
                .ok_or_else(|| corrupt("dangling snapshot ancestry"))?;
            let sequence = uint(&row[1])?;
            if sequence >= last_sequence {
                return Err(corrupt("nondecreasing snapshot ancestry"));
            }
            last_sequence = sequence;
            if snapshot_id == ancestor {
                return Ok(true);
            }
            tip = if matches!(row[0], Value::Null) {
                None
            } else {
                Some(id(&row[0])?)
            };
        }
        Ok(false)
    }

    pub(crate) async fn open(
        context: ReadContext<S>,
        metadata: MetadataSelection,
        snapshot: SnapshotSelection,
    ) -> Result<Self, RuntimeError> {
        let mut selected = super::selection::resolve(&context, metadata).await?;
        let image = Arc::new(context.image(&selected.generation).await?);
        // An incremental checkpoint has its own table-version identity. Opening
        // this view checks only identity pages, never materializes the image.
        let checkpoint = Engine::open(
            Arc::new(image.checkpoint_view()),
            context.options().engine_page_cache_bytes,
        )
        .await?;
        let base = read_meta(&checkpoint).await?;
        if base.table != selected.generation.table_id
            || base.version
                != selected
                    .generation
                    .metadata_image
                    .checkpoint
                    .table_version
                    .0
        {
            return Err(corrupt("checkpoint table or version identity mismatch"));
        }
        drop(checkpoint);
        let engine = Engine::open(image.clone(), context.options().engine_page_cache_bytes).await?;
        let meta = read_meta(&engine).await?;
        if meta.table != selected.generation.table_id
            || meta.version != selected.generation.table_version.0
            || meta.state != selected.generation.semantic_state_sha256
            || meta.commit != selected.commit.commit_id
            || meta.commit_hash != selected.generation.semantic_commit.sha256
            || meta.readers != selected.commit.required_reader_features_after_commit
            || meta.writers != selected.commit.required_writer_features_after_commit
        {
            return Err(corrupt(
                "logical metadata identity differs from generation or commit",
            ));
        }
        validate_commit_row(
            &context,
            &engine,
            &selected.commit,
            &selected.generation.semantic_commit,
        )
        .await?;
        let (schema, schema_reservation) =
            super::schema::load_schema(&context, &engine, meta.schema).await?;
        let (main, _) =
            super::snapshot::resolve(&engine, SnapshotSelection::Ref("main".into()), meta.version)
                .await?;
        selected.coordinates.main_snapshot_id = main.map(|d| d.snapshot_id);
        let (snapshot, branch) = super::snapshot::resolve(&engine, snapshot, meta.version).await?;
        if snapshot
            .as_ref()
            .is_some_and(|s| s.sequence_number > meta.sequence)
        {
            return Err(corrupt("snapshot sequence exceeds selected metadata"));
        }
        // Descriptor strings and the retained envelope remain charged as long
        // as any provider holds this reader.
        selected
            .reservations
            .push(context.reserve_bytes(64 * 1024)?);
        let schemas = BTreeMap::from([(
            schema.schema_id,
            RetainedSchema {
                schema: schema.clone(),
                _reservation: schema_reservation,
            },
        )]);
        Ok(Self {
            context,
            engine,
            schema,
            schemas: tokio::sync::Mutex::new(schemas),
            coordinates: selected.coordinates,
            last_sequence: meta.sequence,
            anchor: selected.anchor,
            head: selected.head,
            raw_head: selected.raw_head,
            head_version: selected.head_version,
            generation: selected.generation,
            image,
            snapshot,
            branch,
            pin_id: Id::try_from_bytes(*uuid::Uuid::now_v7().as_bytes())?,
            _reservations: selected.reservations,
        })
    }
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    pub async fn file_schema(&self, schema_id: u32) -> Result<Arc<Schema>, RuntimeError> {
        let mut schemas = self.schemas.lock().await;
        if let Some(schema) = schemas.get(&schema_id) {
            return Ok(schema.schema.clone());
        }
        let (schema, reservation) =
            super::schema::load_schema(&self.context, &self.engine, schema_id).await?;
        schemas.insert(
            schema_id,
            RetainedSchema {
                schema: schema.clone(),
                _reservation: reservation,
            },
        );
        Ok(schema)
    }
    pub fn coordinates(&self) -> &MetadataCoordinates {
        &self.coordinates
    }
    pub fn anchor(&self) -> &HeadAnchor {
        &self.anchor
    }
    pub fn snapshot(&self) -> Option<&SnapshotDescriptor> {
        self.snapshot.as_ref()
    }
    pub fn statistics(&self) -> ReaderStatistics {
        self.context.statistics()
    }
    pub(crate) fn writer_statistics(&self) -> super::WriterReadStatistics {
        self.context.writer_statistics()
    }
    pub fn store(&self) -> &S {
        self.context.store()
    }
    pub async fn files(
        &self,
        cursor: Option<FileCursor>,
        metric_fields: &[u32],
        limit: usize,
    ) -> Result<FileBatch, RuntimeError> {
        self.files_matching(cursor, metric_fields, &[], limit).await
    }
    pub async fn files_matching(
        &self,
        cursor: Option<FileCursor>,
        metric_fields: &[u32],
        ranges: &[FileMetricRange],
        limit: usize,
    ) -> Result<FileBatch, RuntimeError> {
        super::files::enumerate(self, cursor, metric_fields, ranges, limit).await
    }
}

pub(crate) fn corrupt(message: &str) -> RuntimeError {
    RuntimeError::Corrupt(message.into())
}
pub(crate) fn integer(n: i64) -> Value {
    Value::Numeric(Numeric::Integer(n))
}
pub(crate) fn int(v: &Value) -> Result<i64, RuntimeError> {
    match v {
        Value::Numeric(Numeric::Integer(n)) => Ok(*n),
        _ => Err(corrupt("metadata integer has invalid type")),
    }
}
pub(crate) fn uint(v: &Value) -> Result<u64, RuntimeError> {
    u64::try_from(int(v)?).map_err(|_| corrupt("negative metadata integer"))
}
pub(crate) fn text(v: &Value) -> Result<String, RuntimeError> {
    match v {
        Value::Text(s) => Ok(s.to_string()),
        _ => Err(corrupt("metadata text has invalid type")),
    }
}
pub(crate) fn blob(v: &Value) -> Result<&[u8], RuntimeError> {
    match v {
        Value::Blob(b) => Ok(b),
        _ => Err(corrupt("metadata blob has invalid type")),
    }
}
pub(crate) fn id(v: &Value) -> Result<Id, RuntimeError> {
    Ok(Id::try_from_bytes(
        blob(v)?
            .try_into()
            .map_err(|_| corrupt("invalid metadata ID"))?,
    )?)
}
pub(crate) fn hash(v: &Value) -> Result<Sha256, RuntimeError> {
    Ok(Sha256::from_bytes(
        blob(v)?
            .try_into()
            .map_err(|_| corrupt("invalid metadata hash"))?,
    ))
}

pub(crate) fn sqlite(n: u64) -> Result<Value, RuntimeError> {
    Ok(integer(
        i64::try_from(n).map_err(|_| corrupt("metadata integer overflow"))?,
    ))
}

struct Meta {
    table: Id,
    version: u64,
    state: Sha256,
    commit: Id,
    commit_hash: Sha256,
    schema: u32,
    sequence: u64,
    readers: FeatureSet,
    writers: FeatureSet,
}
async fn read_meta(engine: &Engine) -> Result<Meta, RuntimeError> {
    for (name, value) in [
        ("application_id", crate::image::APPLICATION_ID),
        ("user_version", crate::image::USER_VERSION),
        ("page_size", 4096),
    ] {
        let rows = engine
            .query(&format!("PRAGMA {name}"), vec![], 1, 128)
            .await?;
        if !matches!(rows.as_slice(), [row] if row.len()==1 && int(&row[0])?==value) {
            return Err(corrupt("invalid SQLite metadata profile"));
        }
    }
    let rows = engine.query("SELECT protocol,protocol_version,table_id,table_version,semantic_state_sha256,last_commit_id,last_commit_sha256,current_schema_id,last_sequence_number,default_partition_spec_id,default_sort_order_id,required_reader_features_json,required_writer_features_json FROM otmp_meta WHERE singleton=1", vec![], 1, 64*1024).await?;
    let [r] = rows.as_slice() else {
        return Err(corrupt("metadata identity row missing"));
    };
    if r.len() != 13
        || text(&r[0])? != otmp_protocol::PROTOCOL
        || text(&r[1])? != otmp_protocol::PROTOCOL_VERSION
        || int(&r[9])? != 0
        || int(&r[10])? != 0
    {
        return Err(corrupt("unsupported metadata runtime profile"));
    }
    Ok(Meta {
        table: id(&r[2])?,
        version: uint(&r[3])?,
        state: hash(&r[4])?,
        commit: id(&r[5])?,
        commit_hash: hash(&r[6])?,
        schema: u32::try_from(uint(&r[7])?).map_err(|_| corrupt("schema ID overflow"))?,
        sequence: uint(&r[8])?,
        readers: canonical_json::from_slice_canonical(text(&r[11])?.as_bytes())?,
        writers: canonical_json::from_slice_canonical(text(&r[12])?.as_bytes())?,
    })
}

async fn validate_commit_row<S: ObjectStore>(
    context: &ReadContext<S>,
    engine: &Engine,
    commit: &SemanticCommit,
    reference: &otmp_protocol::ObjectReference,
) -> Result<(), RuntimeError> {
    let _reservation = context.reserve_bytes(4 * 1024 * 1024)?;
    let rows = engine.query("SELECT commit_id,semantic_state_sha256,commit_object_uri,commit_object_sha256,parent_table_version,intent_count,operation_summary_json,result_json,metadata_json FROM otmp_commits WHERE table_version=?1", vec![sqlite(commit.table_version.0)?], 1, context.options().maximum_record_bytes).await?;
    let [r] = rows.as_slice() else {
        return Err(corrupt("selected relational commit is missing"));
    };
    if r.len() != 9
        || id(&r[0])? != commit.commit_id
        || hash(&r[1])? != commit.semantic_state_sha256
        || text(&r[2])? != reference.uri.as_str()
        || hash(&r[3])? != reference.sha256
        || uint(&r[5])? != commit.intents.len() as u64
        || text(&r[6])?.as_bytes() != canonical_json::to_vec(&commit.operations)?
        || text(&r[8])?.as_bytes() != canonical_json::to_vec(&commit.metadata)?
        || commit.intents.len() == 1
            && text(&r[7])?.as_bytes() != canonical_json::to_vec(&commit.intents[0].result)?
        || match commit.parent_table_version {
            None => !matches!(r[4], Value::Null),
            Some(parent) => uint(&r[4])? != parent.0,
        }
    {
        return Err(corrupt(
            "relational commit disagrees with authenticated semantic commit",
        ));
    }
    // Parent commit replay and global relational consistency belong to verify().
    // Turso materializes an entire record when fetching any of its columns;
    // querying a historical commit here would read all of its operations.
    // The selected commit's authenticated body already binds its parent reference
    // and previous state. Historical selection checks those explicit links.

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CommitMetadata, InMemoryObjectStore, InitializeRequest, OperationRequest, ReaderOptions,
        Requirement, Table, TransactionRequest,
    };
    use otmp_protocol::CanonicalValue;
    use std::str::FromStr;

    #[tokio::test]
    async fn commit_operations_are_paged_beyond_one_query_budget() {
        let table = Table::new(InMemoryObjectStore::default());
        let schema =
            serde_json::from_slice(include_bytes!("../../../conformance/sources/schema.json"))
                .unwrap();
        table
            .initialize(InitializeRequest::new(schema))
            .await
            .unwrap();
        for index in 0..3 {
            let property = format!("property-{index}");
            table
                .transact(&TransactionRequest {
                    idempotency_key: format!("metadata-{index}"),
                    requirements: vec![Requirement::PropertyIs {
                        key: property.clone(),
                        value: CanonicalValue::Null,
                    }],
                    operations: vec![OperationRequest::SetProperties {
                        operation_id: "set".into(),
                        updates: [(property, CanonicalValue::Bool(true))].into(),
                        removals: Vec::new(),
                    }],
                    commit_metadata: CommitMetadata::default(),
                })
                .await
                .unwrap();
        }
        let reader = table
            .open_metadata_reader(
                MetadataSelection::Current,
                SnapshotSelection::Ref("main".into()),
                ReaderOptions::default(),
            )
            .await
            .unwrap();

        let first = reader.commit_operations_page_after(0).await.unwrap();
        assert_eq!(first.len(), COMMIT_OPERATION_PAGE_ROWS);
        let second = reader
            .commit_operations_page_after(first.last().unwrap().0)
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
    }

    #[tokio::test]
    async fn matching_tip_does_not_walk_snapshot_history() {
        let store = InMemoryObjectStore::default();
        let table = Table::new(store.clone());
        let schema =
            serde_json::from_slice(include_bytes!("../../../conformance/sources/schema.json"))
                .unwrap();
        table
            .initialize(InitializeRequest::new(schema))
            .await
            .unwrap();
        let reader = MetadataReader::open(
            ReadContext::new(store, ReaderOptions::default()).unwrap(),
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
        )
        .await
        .unwrap();
        let tip = Id::from_str("018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0c").unwrap();
        let queries = reader.engine.query_count();

        assert!(reader.snapshot_descends_from(Some(tip), tip).await.unwrap());
        assert_eq!(reader.engine.query_count(), queries);
    }
}
