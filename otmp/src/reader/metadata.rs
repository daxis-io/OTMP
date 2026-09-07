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
use std::sync::Arc;
use turso_core::{Numeric, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileCursor {
    pub(crate) pin: Id,
    pub(crate) state: CursorState,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CursorState {
    Branch {
        sequence: u64,
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
    pub files: Vec<ReaderFile>,
    /// Derived from the metadata batch before file pruning. Always follow it,
    /// including after an empty candidate batch.
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
    anchor: HeadAnchor,
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
    pub(crate) async fn open(
        context: ReadContext<S>,
        metadata: MetadataSelection,
        snapshot: SnapshotSelection,
    ) -> Result<Self, RuntimeError> {
        let mut selected = super::selection::resolve(&context, metadata).await?;
        let image = context.image(&selected.generation).await?;
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
        let engine =
            Engine::open(Arc::new(image), context.options().engine_page_cache_bytes).await?;
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
            anchor: selected.anchor,
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
    pub fn store(&self) -> &S {
        self.context.store()
    }
    pub async fn files(
        &self,
        cursor: Option<FileCursor>,
        metric_fields: &[u32],
        limit: usize,
    ) -> Result<FileBatch, RuntimeError> {
        super::files::enumerate(self, cursor, metric_fields, limit).await
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
    let rows = engine.query("SELECT commit_id,semantic_state_sha256,commit_object_uri,commit_object_sha256,parent_table_version,intent_count,operation_summary_json,result_json,metadata_json FROM otmp_commits WHERE table_version=?1", vec![sqlite(commit.table_version.0)?], 1, 1024*1024).await?;
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
