use super::{
    BTreeMap, BTreeSet, Candidate, CanonicalValue, CommitMetadata, Deserialize, Id, IntentRecord,
    JsonI64, JsonU64, ObjectStore, PinnedTable, RelativeUri, RuntimeError, SemanticCommit,
    Serialize, Sha256, Table, canonical_json, canonical_text, commit_body, finish_candidate,
    id_from_blob, image, intent_hash, new_id, next_state_hash, now_ms,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Requirement {
    PropertyIs {
        key: String,
        value: CanonicalValue,
    },

    CurrentSchemaIs {
        #[serde(with = "id_number")]
        schema_id: u32,
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperationRequest {
    SetProperties {
        operation_id: String,
        updates: BTreeMap<String, CanonicalValue>,
        removals: Vec<String>,
    },
}
impl OperationRequest {
    fn id(&self) -> &str {
        match self {
            Self::SetProperties { operation_id, .. } => operation_id,
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

pub(crate) fn ref_row(
    connection: &Connection,
    name: &str,
) -> Result<Option<(RefType, Option<Id>)>, RuntimeError> {
    let row: Option<(String, Option<Vec<u8>>)> = connection
        .query_row(
            "SELECT ref_type, snapshot_id FROM otmp_refs WHERE ref_name=?1",
            [name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
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
    connection: &Connection,
    requirements: &[Requirement],
) -> Result<(), RuntimeError> {
    for requirement in requirements {
        let matched = match requirement {
            Requirement::PropertyIs { key, value } => {
                let actual: Option<String> = connection
                    .query_row(
                        "SELECT value_json FROM otmp_properties WHERE property_key=?1",
                        [key],
                        |r| r.get(0),
                    )
                    .optional()?;
                match actual {
                    None => *value == CanonicalValue::Null,
                    Some(actual) => canonical_json::parse_canonical(actual.as_bytes())? == *value,
                }
            }

            Requirement::CurrentSchemaIs { schema_id } => connection.query_row(
                "SELECT current_schema_id=?1 FROM otmp_meta",
                [schema_id],
                |r| r.get(0),
            )?,

            Requirement::DefaultPartitionSpecIs { partition_spec_id } => connection.query_row(
                "SELECT default_partition_spec_id=?1 FROM otmp_meta",
                [partition_spec_id],
                |r| r.get(0),
            )?,
            Requirement::DefaultSortOrderIs { sort_order_id } => connection.query_row(
                "SELECT default_sort_order_id=?1 FROM otmp_meta",
                [sort_order_id],
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

/// The base is borrowed for the complete preparation; no candidate pages are reused.
struct PreparedTransaction<'a> {
    base: &'a PinnedTable,
    request: &'a TransactionRequest,
    logical_hash: Sha256,
    operation_results: Vec<OperationResult>,
}
impl<'a> PreparedTransaction<'a> {
    fn new(
        base: &'a PinnedTable,
        request: &'a TransactionRequest,
        logical_hash: Sha256,
    ) -> Result<Self, RuntimeError> {
        let connection = image::open_readonly(&base.image.path)?;
        let results = prepare_operations(&connection, request)?;
        Ok(Self {
            base,
            request,
            logical_hash,
            operation_results: results,
        })
    }
    fn build(self) -> Result<Candidate<DurableResult>, RuntimeError> {
        let table_version = self
            .base
            .head
            .table_version
            .0
            .checked_add(1)
            .ok_or_else(|| invalid("version exhausted"))?;
        let commit_id = new_id();
        let result = DurableResult {
            table_version,
            commit_id,
            operation_results: self.operation_results,
        };
        let result_value = canonical_json::to_value(&result)?;
        let mut commit = SemanticCommit {
            kind: "otmp.semantic-commit".into(),
            format_version: 1,
            table_id: self.base.head.table_id,
            table_version: JsonU64(table_version),
            parent_table_version: Some(self.base.head.table_version),
            commit_id,
            parent_commit: Some(self.base.head.semantic_commit.clone()),
            created_at_ms: JsonI64(now_ms()?),
            intents: vec![IntentRecord {
                key: self.request.idempotency_key.clone(),
                intent_sha256: self.logical_hash,
                operation_ids: self
                    .request
                    .operations
                    .iter()
                    .map(|o| o.id().to_owned())
                    .collect(),
                result: result_value,
            }],
            requirements: self
                .request
                .requirements
                .iter()
                .map(canonical_json::to_value)
                .collect::<Result<_, _>>()?,
            operations: self
                .request
                .operations
                .iter()
                .map(canonical_json::to_value)
                .collect::<Result<_, _>>()?,
            required_reader_features_after_commit: self.base.head.required_reader_features.clone(),
            required_writer_features_after_commit: self.base.head.required_writer_features.clone(),
            previous_semantic_state_sha256: Some(self.base.head.semantic_state_sha256),
            semantic_state_sha256: Sha256::from_bytes([0; 32]),
            metadata: canonical_json::to_value(&self.request.commit_metadata)?,
        };
        commit.semantic_state_sha256 =
            next_state_hash(self.base.head.semantic_state_sha256, &commit_body(&commit)?);
        let commit_bytes = canonical_json::to_vec(&commit)?;
        let commit_uri: RelativeUri =
            format!("_otmp/commits/{table_version}/{commit_id}.json").parse()?;
        let checkpoint = image::apply_metadata(
            &self.base.checkpoint_bytes,
            &commit,
            &commit_uri,
            &self.request.operations,
        )?;
        finish_candidate(
            self.base,
            &commit,
            commit_uri,
            commit_bytes,
            checkpoint,
            result,
        )
    }
}

pub(crate) fn apply_operations(
    tx: &Transaction<'_>,
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
                    tx.execute("DELETE FROM otmp_properties WHERE property_key=?1", [key])?;
                }
            }
        }
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
            .publish_transaction(&request.idempotency_key, logical_hash, &[], |base| {
                PreparedTransaction::new(base, request, logical_hash)?.build()
            })
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

#[allow(clippy::too_many_lines)] // One exhaustive operation/precondition matrix.
pub(crate) fn prepare_operations(
    connection: &Connection,
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
        };
        results.push(result);
    }
    Ok(results)
}
