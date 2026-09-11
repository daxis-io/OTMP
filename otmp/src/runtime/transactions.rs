use super::{
    BTreeMap, BTreeSet, Candidate, CanonicalValue, CommitMetadata, Deserialize, Id, IntentRecord,
    JsonI64, JsonU64, LogicalType, ObjectStore, RelativeUri, RuntimeError, Schema, SemanticCommit,
    Serialize, Sha256, Table, WritePin, canonical_json, canonical_text, commit_body,
    finish_candidate, id_from_blob, image, intent_hash, new_id, next_state_hash, now_ms,
};
use crate::sql_writer::Writer;
use rusqlite::params;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Requirement {
    PropertyIs {
        key: String,
        value: CanonicalValue,
    },
    RefAbsent {
        #[serde(rename = "ref")]
        name: String,
    },
    RefExists {
        #[serde(rename = "ref")]
        name: String,
        ref_type: RefType,
    },
    RefSnapshotIs {
        #[serde(rename = "ref")]
        name: String,
        #[serde(deserialize_with = "required_snapshot")]
        snapshot_id: Option<Id>,
    },
    SnapshotExists {
        snapshot_id: Id,
    },
    CurrentSchemaIs {
        #[serde(with = "id_number")]
        schema_id: u32,
    },
    SchemaIdAbsent {
        #[serde(with = "id_number")]
        schema_id: u32,
    },
    FieldIdsAbsent {
        #[serde(with = "id_numbers")]
        field_ids: Vec<u32>,
    },
    DefaultPartitionSpecIs {
        #[serde(with = "id_number")]
        partition_spec_id: u32,
    },
    DefaultSortOrderIs {
        #[serde(with = "id_number")]
        sort_order_id: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RefType {
    Branch,
    Tag,
}
impl RefType {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Branch => "branch",
            Self::Tag => "tag",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperationRequest {
    SetProperties {
        operation_id: String,
        updates: BTreeMap<String, CanonicalValue>,
        removals: Vec<String>,
    },
    CreateRef {
        operation_id: String,
        #[serde(rename = "ref")]
        name: String,
        ref_type: RefType,
        #[serde(deserialize_with = "required_snapshot")]
        snapshot_id: Option<Id>,
    },
    ReplaceRef {
        operation_id: String,
        #[serde(rename = "ref")]
        name: String,
        snapshot_id: Id,
    },
    DropRef {
        operation_id: String,
        #[serde(rename = "ref")]
        name: String,
    },
    AddSchema {
        operation_id: String,
        schema: Schema,
    },
    SetCurrentSchema {
        operation_id: String,
        #[serde(with = "id_number")]
        schema_id: u32,
    },
}
impl OperationRequest {
    fn id(&self) -> &str {
        match self {
            Self::SetProperties { operation_id, .. }
            | Self::CreateRef { operation_id, .. }
            | Self::ReplaceRef { operation_id, .. }
            | Self::DropRef { operation_id, .. }
            | Self::AddSchema { operation_id, .. }
            | Self::SetCurrentSchema { operation_id, .. } => operation_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionRequest {
    pub idempotency_key: String,
    pub requirements: Vec<Requirement>,
    pub operations: Vec<OperationRequest>,
    #[serde(default)]
    pub commit_metadata: CommitMetadata,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperationResult {
    Properties {
        operation_id: String,
        keys: Vec<String>,
    },
    Ref {
        operation_id: String,
        #[serde(rename = "ref")]
        name: String,
        ref_type: RefType,
        #[serde(deserialize_with = "required_snapshot")]
        snapshot_id: Option<Id>,
    },
    Schema {
        operation_id: String,
        #[serde(with = "id_number")]
        schema_id: u32,
    },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionResult {
    pub table_version: u64,
    pub commit_id: Id,
    pub semantic_state_sha256: Sha256,
    pub operation_results: Vec<OperationResult>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableResult {
    pub table_version: u64,
    pub commit_id: Id,
    pub operation_results: Vec<OperationResult>,
}

fn invalid(message: &str) -> RuntimeError {
    RuntimeError::InvalidTransaction(message.into())
}
fn required(request: &TransactionRequest, requirement: &Requirement) -> Result<(), RuntimeError> {
    if request
        .requirements
        .iter()
        .filter(|r| *r == requirement)
        .count()
        != 1
    {
        return Err(invalid(
            "operation requires exactly one matching precondition",
        ));
    }
    Ok(())
}

pub(crate) fn ref_row(
    connection: &Writer<'_>,
    name: &str,
) -> Result<Option<(RefType, Option<Id>)>, RuntimeError> {
    let row: Option<(String, Option<Vec<u8>>)> = connection.query_optional(
        "SELECT ref_type, snapshot_id FROM otmp_refs WHERE ref_name=?1",
        params![name],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    row.map(|(kind, id)| {
        Ok((
            match kind.as_str() {
                "branch" => RefType::Branch,
                "tag" => RefType::Tag,
                _ => return Err(RuntimeError::Corrupt("invalid ref type".into())),
            },
            id.map(id_from_blob).transpose()?,
        ))
    })
    .transpose()
}

pub(crate) fn evaluate(
    connection: &Writer<'_>,
    requirements: &[Requirement],
) -> Result<(), RuntimeError> {
    for requirement in requirements {
        let matched = match requirement {
            Requirement::PropertyIs { key, value } => {
                let actual: Option<String> = connection.query_optional(
                    "SELECT value_json FROM otmp_properties WHERE property_key=?1",
                    params![key],
                    |r| r.get(0),
                )?;
                match actual {
                    None => *value == CanonicalValue::Null,
                    Some(actual) => canonical_json::parse_canonical(actual.as_bytes())? == *value,
                }
            }
            Requirement::RefAbsent { name } => ref_row(connection, name)?.is_none(),
            Requirement::RefExists { name, ref_type } => {
                ref_row(connection, name)?.is_some_and(|r| r.0 == *ref_type)
            }
            Requirement::RefSnapshotIs { name, snapshot_id } => {
                ref_row(connection, name)?.is_some_and(|r| r.1 == *snapshot_id)
            }
            Requirement::SnapshotExists { snapshot_id } => connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM otmp_snapshots WHERE snapshot_id=?1)",
                params![snapshot_id.as_bytes().as_slice()],
                |r| r.get(0),
            )?,
            Requirement::CurrentSchemaIs { schema_id } => connection.query_row(
                "SELECT current_schema_id=?1 FROM otmp_meta",
                params![schema_id],
                |r| r.get(0),
            )?,
            Requirement::SchemaIdAbsent { schema_id } => !connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM otmp_schemas WHERE schema_id=?1)",
                params![schema_id],
                |r| r.get::<bool>(0),
            )?,
            Requirement::FieldIdsAbsent { field_ids } => {
                let mut absent = BTreeSet::new();
                for id in field_ids {
                    if *id == 0
                        || !absent.insert(id)
                        || connection.query_row(
                            "SELECT EXISTS(SELECT 1 FROM otmp_field_ids WHERE field_id=?1)",
                            params![id],
                            |r| r.get::<bool>(0),
                        )?
                    {
                        return Err(RuntimeError::SemanticConflict(
                            "field ID already exists or is invalid".into(),
                        ));
                    }
                }
                true
            }
            Requirement::DefaultPartitionSpecIs { partition_spec_id } => connection.query_row(
                "SELECT default_partition_spec_id=?1 FROM otmp_meta",
                params![partition_spec_id],
                |r| r.get(0),
            )?,
            Requirement::DefaultSortOrderIs { sort_order_id } => connection.query_row(
                "SELECT default_sort_order_id=?1 FROM otmp_meta",
                params![sort_order_id],
                |r| r.get(0),
            )?,
        };
        if !matched {
            return Err(RuntimeError::SemanticConflict(format!(
                "requirement failed: {requirement:?}"
            )));
        }
    }
    Ok(())
}

pub(super) fn build_transaction_candidate<S: ObjectStore>(
    base: &WritePin<S>,
    request: &TransactionRequest,
    logical_hash: Sha256,
) -> Result<Candidate<DurableResult>, RuntimeError> {
    let reads_before = base.reader.statistics();
    let writer = crate::cow_writer::CandidateWriter::from_pages(
        base.reader.image.clone(),
        tokio::runtime::Handle::try_current()
            .map_err(|error| RuntimeError::Turso(format!("Tokio runtime is required: {error}")))?,
    )?;
    let operation_results = {
        #[cfg(feature = "write-latency-qualification")]
        let _phase = crate::write_latency_qualification::phase("operation_preparation");
        prepare_operations(&writer.sql(), request)?
    };
    let table_version = base
        .reader
        .head
        .table_version
        .0
        .checked_add(1)
        .ok_or_else(|| invalid("version exhausted"))?;
    let commit_id = new_id();
    let result = DurableResult {
        table_version,
        commit_id,
        operation_results,
    };
    let result_value = canonical_json::to_value(&result)?;
    let mut commit = SemanticCommit {
        kind: "otmp.semantic-commit".into(),
        format_version: 1,
        table_id: base.reader.head.table_id,
        table_version: JsonU64(table_version),
        parent_table_version: Some(base.reader.head.table_version),
        commit_id,
        parent_commit: Some(base.reader.head.semantic_commit.clone()),
        created_at_ms: JsonI64(now_ms()?),
        intents: vec![IntentRecord {
            key: request.idempotency_key.clone(),
            intent_sha256: logical_hash,
            operation_ids: request
                .operations
                .iter()
                .map(|o| o.id().to_owned())
                .collect(),
            result: result_value,
        }],
        requirements: request
            .requirements
            .iter()
            .map(canonical_json::to_value)
            .collect::<Result<_, _>>()?,
        operations: request
            .operations
            .iter()
            .map(canonical_json::to_value)
            .collect::<Result<_, _>>()?,
        required_reader_features_after_commit: base.reader.head.required_reader_features.clone(),
        required_writer_features_after_commit: base.reader.head.required_writer_features.clone(),
        previous_semantic_state_sha256: Some(base.reader.head.semantic_state_sha256),
        semantic_state_sha256: Sha256::from_bytes([0; 32]),
        metadata: canonical_json::to_value(&request.commit_metadata)?,
    };
    commit.semantic_state_sha256 = next_state_hash(
        base.reader.head.semantic_state_sha256,
        &commit_body(&commit)?,
    );
    let commit_bytes = canonical_json::to_vec(&commit)?;
    let commit_hash = otmp_protocol::object_hash(&commit_bytes);
    let commit_uri: RelativeUri =
        format!("_otmp/commits/{table_version}/{commit_id}.json").parse()?;
    {
        #[cfg(feature = "write-latency-qualification")]
        let _phase = crate::write_latency_qualification::phase("turso_sql");
        image::mutate_metadata(&writer.sql(), &commit, &commit_uri, &request.operations)?;
    }
    {
        #[cfg(feature = "write-latency-qualification")]
        let _phase = crate::write_latency_qualification::phase("commit_projection_validation");
        image::validate_targeted_candidate(
            &writer.sql(),
            &commit,
            commit_uri.as_str(),
            commit_hash,
        )?;
    }
    let checkpoint = image::finish_turso(writer, false)?;
    let candidate = finish_candidate(base, &commit, commit_uri, commit_bytes, checkpoint, result);
    super::trace_writer_reads(reads_before, base.reader.statistics());
    candidate
}
fn validate_ref_name<'a>(name: &'a str, refs: &mut BTreeSet<&'a str>) -> Result<(), RuntimeError> {
    if name.is_empty() || name.chars().any(char::is_control) || !refs.insert(name) {
        return Err(invalid("invalid or repeated ref name"));
    }
    Ok(())
}

pub(crate) fn apply_operations(
    tx: &Writer<'_>,
    operations: &[OperationRequest],
    version: u64,
) -> Result<(), RuntimeError> {
    let version = i64::try_from(version).map_err(|_| invalid("version exhausted"))?;
    for operation in operations {
        match operation {
            OperationRequest::SetProperties {
                updates, removals, ..
            } => {
                for (key, value) in updates {
                    tx.execute("INSERT INTO otmp_properties VALUES(?1,?2,?3) ON CONFLICT(property_key) DO UPDATE SET value_json=excluded.value_json, updated_version=excluded.updated_version", params![key, canonical_text(value)?, version])?;
                }
                for key in removals {
                    tx.execute(
                        "DELETE FROM otmp_properties WHERE property_key=?1",
                        params![key],
                    )?;
                }
            }
            OperationRequest::CreateRef {
                name,
                ref_type,
                snapshot_id,
                ..
            } => {
                tx.execute("INSERT INTO otmp_refs(ref_name,ref_type,snapshot_id,created_version,updated_version) VALUES(?1,?2,?3,?4,?4)", params![name, ref_type.as_str(), snapshot_id.map(|id| id.as_bytes().to_vec()), version])?;
                if *ref_type == RefType::Branch {
                    materialize_branch(tx, name, *snapshot_id)?;
                }
            }
            OperationRequest::ReplaceRef {
                name, snapshot_id, ..
            } => {
                tx.execute(
                    "UPDATE otmp_refs SET snapshot_id=?1, updated_version=?2 WHERE ref_name=?3",
                    params![snapshot_id.as_bytes().as_slice(), version, name],
                )?;
                materialize_branch(tx, name, Some(*snapshot_id))?;
            }
            OperationRequest::DropRef { name, .. } => {
                tx.execute(
                    "DELETE FROM otmp_ref_live_files WHERE ref_name=?1",
                    params![name],
                )?;
                tx.execute("DELETE FROM otmp_refs WHERE ref_name=?1", params![name])?;
            }
            OperationRequest::AddSchema { schema, .. } => image::insert_schema(
                tx,
                schema,
                u64::try_from(version).map_err(|_| invalid("negative version"))?,
            )?,
            OperationRequest::SetCurrentSchema { schema_id, .. } => {
                tx.query_row(
                    "SELECT schema_id FROM otmp_schemas WHERE schema_id=?1",
                    params![schema_id],
                    |row| row.get::<i64>(0),
                )
                .map_err(|_| invalid("selected schema must already exist in operation order"))?;
                tx.execute(
                    "UPDATE otmp_meta SET current_schema_id=?1",
                    params![schema_id],
                )?;
            }
        }
    }
    Ok(())
}
fn materialize_branch(
    tx: &Writer<'_>,
    name: &str,
    snapshot: Option<Id>,
) -> Result<(), RuntimeError> {
    tx.execute(
        "DELETE FROM otmp_ref_live_files WHERE ref_name=?1",
        params![name],
    )?;
    for id in tx.ancestry(snapshot)? {
        tx.execute("INSERT INTO otmp_ref_live_files SELECT ?1,c.file_id,c.snapshot_id,s.sequence_number,s.sequence_number FROM otmp_snapshot_file_changes c JOIN otmp_snapshots s USING(snapshot_id) WHERE c.snapshot_id=?2 AND c.change_kind='add'", params![name, id.as_bytes().as_slice()])?;
    }
    Ok(())
}

fn additive_schema(old: &Schema, new: &Schema) -> Result<Vec<u32>, RuntimeError> {
    new.validate()?;
    if new.parent_schema_id != Some(old.schema_id)
        || new.identifier_field_ids != old.identifier_field_ids
    {
        return Err(invalid("schema parent or identifiers changed"));
    }
    let mut ids = BTreeSet::new();
    additive_fields(&old.fields, &new.fields, &mut ids)?;
    Ok(ids.into_iter().collect())
}
fn additive_fields(
    old: &[otmp_protocol::Field],
    new: &[otmp_protocol::Field],
    ids: &mut BTreeSet<u32>,
) -> Result<(), RuntimeError> {
    if new.len() < old.len() {
        return Err(invalid("field removal"));
    }
    for (a, b) in old.iter().zip(new) {
        let mut comparable = b.clone();
        comparable.field_type = a.field_type.clone();
        if *a != comparable {
            return Err(invalid("existing field changed or reordered"));
        }
        match (&a.field_type, &b.field_type) {
            (LogicalType::Struct { fields: a }, LogicalType::Struct { fields: b }) => {
                additive_fields(a, b, ids)?;
            }
            (LogicalType::List { element: a }, LogicalType::List { element: b }) => {
                additive_fields(
                    std::slice::from_ref(a.as_ref()),
                    std::slice::from_ref(b.as_ref()),
                    ids,
                )?;
            }
            (LogicalType::Map { key: ak, value: av }, LogicalType::Map { key: bk, value: bv })
                if ak == bk =>
            {
                additive_fields(
                    std::slice::from_ref(av.as_ref()),
                    std::slice::from_ref(bv.as_ref()),
                    ids,
                )?;
            }
            (a, b) if a == b => {}
            _ => return Err(invalid("type change is not additive")),
        }
    }
    for field in &new[old.len()..] {
        collect_new_field(field, ids)?;
    }
    Ok(())
}

impl<S: ObjectStore> Table<S> {
    pub async fn transact(
        &self,
        request: &TransactionRequest,
    ) -> Result<TransactionResult, RuntimeError> {
        let logical_hash = intent_hash(&canonical_json::to_vec(request)?);
        let (result, semantic_state_sha256) = self
            .publish_transaction(request.clone(), logical_hash)
            .await?;
        Ok(TransactionResult {
            table_version: result.table_version,
            commit_id: result.commit_id,
            semantic_state_sha256,
            operation_results: result.operation_results,
        })
    }

    #[cfg(feature = "write-latency-qualification")]
    pub(crate) async fn qualification_write_pin(&self) -> Result<WritePin<S>, RuntimeError> {
        self.write_pin("main").await
    }

    #[cfg(feature = "write-latency-qualification")]
    pub(crate) async fn transact_pre_pinned(
        &self,
        request: &TransactionRequest,
        pinned: WritePin<S>,
    ) -> Result<TransactionResult, RuntimeError> {
        let logical_hash = intent_hash(&canonical_json::to_vec(request)?);
        crate::write_latency_qualification::skipped_phase("parent_pin", 0);
        crate::write_latency_qualification::skipped_phase("parent_validation", 1);
        crate::write_latency_qualification::skipped_phase("generation_resolution", 2);
        crate::write_latency_qualification::skipped_phase("logical_image_materialization", 2);
        crate::write_latency_qualification::add_bytes(
            "parent_logical_bytes",
            pinned.reader.image.length(),
        );
        let (result, semantic_state_sha256) = self
            .publish_transaction_from_parent(request.clone(), logical_hash, pinned)
            .await?;
        Ok(TransactionResult {
            table_version: result.table_version,
            commit_id: result.commit_id,
            semantic_state_sha256,
            operation_results: result.operation_results,
        })
    }
}

mod id_number {
    use super::{Deserialize, JsonU64, Serialize};
    #[allow(clippy::trivially_copy_pass_by_ref)] // Serde with-module signature.
    pub fn serialize<S: serde::Serializer>(v: &u32, s: S) -> Result<S::Ok, S::Error> {
        JsonU64(u64::from(*v)).serialize(s)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u32, D::Error> {
        u32::try_from(JsonU64::deserialize(d)?.0).map_err(serde::de::Error::custom)
    }
}
mod id_numbers {
    use super::{Deserialize, JsonU64, Serialize};
    pub fn serialize<S: serde::Serializer>(v: &[u32], s: S) -> Result<S::Ok, S::Error> {
        v.iter()
            .map(|v| JsonU64(u64::from(*v)))
            .collect::<Vec<_>>()
            .serialize(s)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<u32>, D::Error> {
        Vec::<JsonU64>::deserialize(d)?
            .into_iter()
            .map(|v| u32::try_from(v.0).map_err(serde::de::Error::custom))
            .collect()
    }
}

#[allow(clippy::too_many_lines)] // One exhaustive operation/precondition matrix.
pub(crate) fn prepare_operations(
    connection: &Writer<'_>,
    request: &TransactionRequest,
) -> Result<Vec<OperationResult>, RuntimeError> {
    if request.idempotency_key.is_empty()
        || request.idempotency_key == "otmp.genesis"
        || request.operations.is_empty()
    {
        return Err(invalid("nonempty key and operations required"));
    }
    evaluate(connection, &request.requirements)?;
    let mut ids = BTreeSet::new();
    let mut keys = BTreeSet::new();
    let mut refs = BTreeSet::new();
    let mut add_schema = false;
    let mut set_schema = false;
    let mut results = Vec::new();
    for operation in &request.operations {
        if operation.id().is_empty() || !ids.insert(operation.id()) {
            return Err(invalid("operation IDs must be nonempty and unique"));
        }
        let result = match operation {
            OperationRequest::SetProperties {
                operation_id,
                updates,
                removals,
            } => {
                if updates.values().any(|v| *v == CanonicalValue::Null) {
                    return Err(invalid("top-level null property value"));
                }
                let mut touched = Vec::new();
                for key in updates.keys().chain(removals.iter()) {
                    if key.is_empty() || key.starts_with("otmp.") || !keys.insert(key) {
                        return Err(invalid(
                            "reserved, empty, overlapping, or repeated property key",
                        ));
                    }
                    if request
                        .requirements
                        .iter()
                        .filter(|r| matches!(r, Requirement::PropertyIs { key: k, .. } if k == key))
                        .count()
                        != 1
                    {
                        return Err(invalid(
                            "each property key requires exactly one property_is",
                        ));
                    }
                    touched.push(key.clone());
                }
                touched.sort();
                OperationResult::Properties {
                    operation_id: operation_id.clone(),
                    keys: touched,
                }
            }
            OperationRequest::CreateRef {
                operation_id,
                name,
                ref_type,
                snapshot_id,
            } => {
                required(request, &Requirement::RefAbsent { name: name.clone() })?;
                if *ref_type == RefType::Tag && snapshot_id.is_none() {
                    return Err(invalid("tags require a snapshot"));
                }
                if let Some(id) = snapshot_id {
                    required(request, &Requirement::SnapshotExists { snapshot_id: *id })?;
                }
                validate_ref_name(name, &mut refs)?;
                OperationResult::Ref {
                    operation_id: operation_id.clone(),
                    name: name.clone(),
                    ref_type: *ref_type,
                    snapshot_id: *snapshot_id,
                }
            }
            OperationRequest::ReplaceRef {
                operation_id,
                name,
                snapshot_id,
            } => {
                let (_, old) = ref_row(connection, name)?
                    .ok_or_else(|| RuntimeError::RefNotFound(name.clone()))?;
                required(
                    request,
                    &Requirement::RefExists {
                        name: name.clone(),
                        ref_type: RefType::Branch,
                    },
                )?;
                required(
                    request,
                    &Requirement::RefSnapshotIs {
                        name: name.clone(),
                        snapshot_id: old,
                    },
                )?;
                required(
                    request,
                    &Requirement::SnapshotExists {
                        snapshot_id: *snapshot_id,
                    },
                )?;
                validate_ref_name(name, &mut refs)?;
                OperationResult::Ref {
                    operation_id: operation_id.clone(),
                    name: name.clone(),
                    ref_type: RefType::Branch,
                    snapshot_id: Some(*snapshot_id),
                }
            }
            OperationRequest::DropRef { operation_id, name } => {
                if name == "main" {
                    return Err(invalid("main cannot be dropped"));
                }
                let (kind, old) = ref_row(connection, name)?
                    .ok_or_else(|| RuntimeError::RefNotFound(name.clone()))?;
                required(
                    request,
                    &Requirement::RefExists {
                        name: name.clone(),
                        ref_type: kind,
                    },
                )?;
                required(
                    request,
                    &Requirement::RefSnapshotIs {
                        name: name.clone(),
                        snapshot_id: old,
                    },
                )?;
                validate_ref_name(name, &mut refs)?;
                OperationResult::Ref {
                    operation_id: operation_id.clone(),
                    name: name.clone(),
                    ref_type: kind,
                    snapshot_id: old,
                }
            }
            OperationRequest::AddSchema {
                operation_id,
                schema,
            } => {
                if std::mem::replace(&mut add_schema, true) {
                    return Err(invalid("only one add_schema is supported"));
                }
                let current: u32 =
                    connection
                        .query_row("SELECT current_schema_id FROM otmp_meta", &[], |r| r.get(0))?;
                required(
                    request,
                    &Requirement::CurrentSchemaIs { schema_id: current },
                )?;
                required(
                    request,
                    &Requirement::SchemaIdAbsent {
                        schema_id: schema.schema_id,
                    },
                )?;
                let old = image::read_schema_with(connection, current)?;
                let new_ids = additive_schema(&old, schema)?;
                let mut candidates = request.requirements.iter().filter_map(|r| match r {
                    Requirement::FieldIdsAbsent { field_ids } => Some(field_ids.clone()),
                    _ => None,
                });
                let mut actual = candidates
                    .next()
                    .ok_or_else(|| invalid("field_ids_absent required"))?;
                actual.sort_unstable();
                if candidates.next().is_some() || actual != new_ids {
                    return Err(invalid("field_ids_absent must cover exactly all new IDs"));
                }
                OperationResult::Schema {
                    operation_id: operation_id.clone(),
                    schema_id: schema.schema_id,
                }
            }
            OperationRequest::SetCurrentSchema {
                operation_id,
                schema_id,
            } => {
                if std::mem::replace(&mut set_schema, true) {
                    return Err(invalid("only one set_current_schema is supported"));
                }
                let current =
                    connection
                        .query_row("SELECT current_schema_id FROM otmp_meta", &[], |r| r.get(0))?;
                required(
                    request,
                    &Requirement::CurrentSchemaIs { schema_id: current },
                )?;
                OperationResult::Schema {
                    operation_id: operation_id.clone(),
                    schema_id: *schema_id,
                }
            }
        };
        results.push(result);
    }
    Ok(results)
}

fn required_snapshot<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Id>, D::Error> {
    Option::<Id>::deserialize(d)
}

fn collect_new_field(
    field: &otmp_protocol::Field,
    ids: &mut BTreeSet<u32>,
) -> Result<(), RuntimeError> {
    if field.required {
        return Err(invalid("new fields must be optional"));
    }
    ids.insert(field.field_id);
    match &field.field_type {
        LogicalType::Struct { fields } => {
            for f in fields {
                collect_new_field(f, ids)?;
            }
        }
        LogicalType::List { element } => collect_new_field(element, ids)?,
        LogicalType::Map { key, value } => {
            collect_new_field(key, ids)?;
            collect_new_field(value, ids)?;
        }
        _ => {}
    }
    Ok(())
}
