use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    CanonicalValue, FeatureSet, Id, JsonI64, JsonU64, PROTOCOL, PROTOCOL_VERSION, ProtocolError,
    RelativeUri, Schema, Sha256, TypedScalar, canonical_json,
};

pub const HEAD_MEDIA_TYPE: &str = "application/vnd.otmp.head+json";
pub const COMMIT_MEDIA_TYPE: &str = "application/vnd.otmp.commit+json";
pub const GENERATION_MEDIA_TYPE: &str = "application/vnd.otmp.generation+json";
pub const CHECKPOINT_MEDIA_TYPE: &str = "application/vnd.sqlite3";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectReference {
    pub uri: RelativeUri,
    pub sha256: Sha256,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length: Option<JsonU64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Head {
    pub protocol: String,
    pub protocol_version: String,
    pub table_id: Id,
    pub table_version: JsonU64,
    pub root_revision: JsonU64,
    pub semantic_state_sha256: Sha256,
    pub semantic_commit: ObjectReference,
    pub metadata_generation: ObjectReference,
    pub required_reader_features: FeatureSet,
    pub required_writer_features: FeatureSet,
}

impl Head {
    pub fn validate(&self, supported: &BTreeSet<&str>) -> Result<(), ProtocolError> {
        if self.protocol != PROTOCOL || self.protocol_version != PROTOCOL_VERSION {
            return Err(ProtocolError::InvalidObject(
                "unsupported protocol or protocol version".into(),
            ));
        }
        self.required_reader_features.require_supported(supported)?;
        if self.semantic_commit.media_type.as_deref() != Some(COMMIT_MEDIA_TYPE)
            || self.metadata_generation.media_type.as_deref() != Some(GENERATION_MEDIA_TYPE)
        {
            return Err(ProtocolError::InvalidObject(
                "incorrect object media type".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntentRecord {
    pub key: String,
    pub intent_sha256: Sha256,
    pub operation_ids: Vec<String>,
    pub result: CanonicalValue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticCommit {
    pub kind: String,
    pub format_version: u32,
    pub table_id: Id,
    pub table_version: JsonU64,
    pub parent_table_version: Option<JsonU64>,
    pub commit_id: Id,
    pub parent_commit: Option<ObjectReference>,
    pub created_at_ms: JsonI64,
    pub intents: Vec<IntentRecord>,
    pub requirements: Vec<CanonicalValue>,
    pub operations: Vec<CanonicalValue>,
    pub required_reader_features_after_commit: FeatureSet,
    pub required_writer_features_after_commit: FeatureSet,
    pub previous_semantic_state_sha256: Option<Sha256>,
    pub semantic_state_sha256: Sha256,
    pub metadata: CanonicalValue,
}

impl SemanticCommit {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.kind != "otmp.semantic-commit" || self.format_version != 1 {
            return Err(ProtocolError::InvalidObject(
                "invalid semantic commit kind".into(),
            ));
        }
        let genesis = self.table_version.0 == 0;
        if genesis
            != (self.parent_table_version.is_none()
                && self.parent_commit.is_none()
                && self.previous_semantic_state_sha256.is_none())
        {
            return Err(ProtocolError::InvalidObject(
                "invalid semantic parent shape".into(),
            ));
        }
        if !genesis
            && self
                .parent_table_version
                .and_then(|value| value.0.checked_add(1))
                != Some(self.table_version.0)
        {
            return Err(ProtocolError::InvalidObject(
                "nonconsecutive table version".into(),
            ));
        }
        if self
            .parent_commit
            .as_ref()
            .is_some_and(|parent| parent.media_type.as_deref() != Some(COMMIT_MEDIA_TYPE))
        {
            return Err(ProtocolError::InvalidObject(
                "incorrect parent commit media type".into(),
            ));
        }
        if self.intents.is_empty() || self.operations.is_empty() {
            return Err(ProtocolError::InvalidObject("empty semantic commit".into()));
        }
        if !matches!(self.metadata, CanonicalValue::Object(_)) {
            return Err(ProtocolError::InvalidObject(
                "semantic commit metadata must be an object".into(),
            ));
        }
        let mut keys = BTreeSet::new();
        if self.intents.iter().any(|intent| !keys.insert(&intent.key)) {
            return Err(ProtocolError::InvalidObject(
                "duplicate idempotency key".into(),
            ));
        }
        let operation_ids = validate_operations(&self.operations, genesis, self.table_id)?;
        let mut referenced = BTreeSet::new();
        for intent in &self.intents {
            if intent.key.is_empty() || intent.operation_ids.is_empty() {
                return Err(ProtocolError::InvalidObject(
                    "intent key and operation IDs must be nonempty".into(),
                ));
            }
            for operation_id in &intent.operation_ids {
                if !operation_ids.contains(operation_id.as_str()) {
                    return Err(ProtocolError::InvalidObject(
                        "intent references an unknown operation".into(),
                    ));
                }
                referenced.insert(operation_id.as_str());
            }
        }
        if referenced != operation_ids {
            return Err(ProtocolError::InvalidObject(
                "every operation must be referenced by an intent".into(),
            ));
        }
        if genesis {
            let is_initialize = self.operations.len() == 1
                && matches!(
                    &self.operations[0],
                    CanonicalValue::Object(fields)
                        if fields.get("type")
                            == Some(&CanonicalValue::String("initialize_table".into()))
                );
            if !is_initialize {
                return Err(ProtocolError::InvalidObject(
                    "genesis must contain exactly one initialize_table operation".into(),
                ));
            }
        }
        Ok(())
    }

    /// Validates the deliberately narrow operation profile implemented by the
    /// Gate 1 runtime. Generic semantic validation permits namespaced extension
    /// operations; a Gate 1 reader must fail closed instead of silently
    /// projecting an operation it does not understand.
    pub fn validate_gate1(&self) -> Result<(), ProtocolError> {
        self.validate()?;
        let genesis = self.table_version.0 == 0;
        if self.operations.len() != 1 {
            return Err(ProtocolError::InvalidObject(
                "Gate 1 commits must contain exactly one operation".into(),
            ));
        }
        let CanonicalValue::Object(operation) = &self.operations[0] else {
            unreachable!("validate rejects non-object operations");
        };
        match operation.get("type") {
            Some(CanonicalValue::String(operation_type))
                if genesis && operation_type == "initialize_table" =>
            {
                Ok(())
            }
            Some(CanonicalValue::String(operation_type))
                if !genesis && operation_type == "commit_snapshot" =>
            {
                validate_gate1_commit_snapshot(operation)
            }
            _ => Err(ProtocolError::InvalidObject(
                "semantic operation is outside the Gate 1 profile".into(),
            )),
        }
    }
}

fn validate_operations(
    operations: &[CanonicalValue],
    genesis: bool,
    table_id: Id,
) -> Result<BTreeSet<&str>, ProtocolError> {
    let mut operation_ids = BTreeSet::new();
    let mut committed_snapshot_ids = BTreeSet::new();
    for operation in operations {
        let CanonicalValue::Object(fields) = operation else {
            return Err(ProtocolError::InvalidObject(
                "semantic operation must be an object".into(),
            ));
        };
        let Some(CanonicalValue::String(operation_id)) = fields.get("operation_id") else {
            return Err(ProtocolError::InvalidObject(
                "semantic operation has no operation_id".into(),
            ));
        };
        if operation_id.is_empty() || !operation_ids.insert(operation_id.as_str()) {
            return Err(ProtocolError::InvalidObject(
                "semantic operation IDs must be nonempty and unique".into(),
            ));
        }
        let Some(CanonicalValue::String(operation_type)) = fields.get("type") else {
            return Err(ProtocolError::InvalidObject(
                "semantic operation has no string type".into(),
            ));
        };
        match operation_type.as_str() {
            "initialize_table" if genesis => validate_initialize_table(fields, table_id)?,
            "initialize_table" if !genesis => {
                return Err(ProtocolError::InvalidObject(
                    "initialize_table is only valid at genesis".into(),
                ));
            }
            "commit_snapshot" => {
                let snapshot_id = validate_commit_snapshot(fields)?;
                if !committed_snapshot_ids.insert(snapshot_id) {
                    return Err(ProtocolError::InvalidObject(
                        "commit_snapshot snapshot IDs must be unique".into(),
                    ));
                }
            }
            extension_type if extension_type.contains('.') => {}
            _ => {
                return Err(ProtocolError::InvalidObject(format!(
                    "unsupported semantic operation type {operation_type:?}"
                )));
            }
        }
    }
    Ok(operation_ids)
}

fn validate_initialize_table(
    operation: &BTreeMap<String, CanonicalValue>,
    table_id: Id,
) -> Result<(), ProtocolError> {
    require_exact_fields(
        operation,
        &[
            "operation_id",
            "type",
            "table_id",
            "schema",
            "partition_spec_id",
            "sort_order_id",
            "target_ref",
        ],
        "initialize_table",
    )?;
    if parse_id(operation, "table_id", "initialize_table")? != table_id {
        return Err(ProtocolError::InvalidObject(
            "initialize_table table ID differs from the semantic commit".into(),
        ));
    }
    let schema_value = operation
        .get("schema")
        .ok_or_else(|| ProtocolError::InvalidObject("initialize_table has no schema".into()))?;
    let schema: Schema = canonical_json::from_slice(&canonical_json::to_vec(schema_value)?)?;
    schema.validate()?;
    if require_decimal_u64(operation, "partition_spec_id", "initialize_table")? != 0
        || require_decimal_u64(operation, "sort_order_id", "initialize_table")? != 0
        || require_nonempty_string(operation, "target_ref", "initialize_table")? != "main"
    {
        return Err(ProtocolError::InvalidObject(
            "initialize_table must create partition spec 0, sort order 0, and main".into(),
        ));
    }
    Ok(())
}

fn validate_gate1_commit_snapshot(
    operation: &BTreeMap<String, CanonicalValue>,
) -> Result<(), ProtocolError> {
    if require_nonempty_string(operation, "target_ref", "commit_snapshot")? != "main"
        || require_nonempty_string(operation, "rebase_mode", "commit_snapshot")? != "append-safe"
        || !matches!(operation.get("scan_projection"), Some(CanonicalValue::Null))
    {
        return Err(ProtocolError::InvalidObject(
            "Gate 1 commit_snapshot must append safely to main without a scan projection".into(),
        ));
    }
    let Some(CanonicalValue::Object(snapshot)) = operation.get("snapshot") else {
        unreachable!("validate_commit_snapshot rejects invalid snapshots");
    };
    if require_nonempty_string(snapshot, "operation", "commit_snapshot snapshot")? != "append"
        || require_u32(snapshot, "partition_spec_id", "commit_snapshot snapshot")? != 0
        || require_u32(snapshot, "sort_order_id", "commit_snapshot snapshot")? != 0
    {
        return Err(ProtocolError::InvalidObject(
            "Gate 1 snapshot must be an unpartitioned, unsorted append".into(),
        ));
    }
    let snapshot_schema_id =
        require_positive_u32(snapshot, "schema_id", "commit_snapshot snapshot")?;
    require_gate1_i64(snapshot, "sequence_number", "commit_snapshot snapshot")?;
    let Some(CanonicalValue::Array(added_files)) = operation.get("added_files") else {
        unreachable!("validate_commit_snapshot rejects invalid added_files");
    };
    if added_files.is_empty() {
        return Err(ProtocolError::InvalidObject(
            "Gate 1 append must add at least one file".into(),
        ));
    }
    for file in added_files {
        let CanonicalValue::Object(file) = file else {
            unreachable!("validate_commit_snapshot rejects non-object added files");
        };
        validate_gate1_added_file(file, snapshot_schema_id)?;
    }
    if !matches!(
        operation.get("removed_file_ids"),
        Some(CanonicalValue::Array(removed)) if removed.is_empty()
    ) {
        return Err(ProtocolError::InvalidObject(
            "Gate 1 append cannot remove files".into(),
        ));
    }
    Ok(())
}

fn validate_gate1_added_file(
    file: &BTreeMap<String, CanonicalValue>,
    snapshot_schema_id: u32,
) -> Result<(), ProtocolError> {
    require_exact_fields(
        file,
        &[
            "file_id",
            "uri",
            "object_identity",
            "file_format",
            "file_size_bytes",
            "record_count",
            "schema_id",
            "partition_spec_id",
            "sort_order_id",
            "content_sha256",
            "partition_values",
            "metrics",
            "metadata",
        ],
        "Gate 1 added file",
    )?;
    require_nonempty_string(file, "uri", "Gate 1 added file")?.parse::<RelativeUri>()?;
    if !matches!(file.get("object_identity"), Some(CanonicalValue::Null))
        || require_nonempty_string(file, "file_format", "Gate 1 added file")? != "parquet"
        || require_positive_u32(file, "schema_id", "Gate 1 added file")? != snapshot_schema_id
        || require_u32(file, "partition_spec_id", "Gate 1 added file")? != 0
        || require_u32(file, "sort_order_id", "Gate 1 added file")? != 0
        || !matches!(file.get("partition_values"), Some(CanonicalValue::Object(values)) if values.is_empty())
        || !matches!(file.get("metadata"), Some(CanonicalValue::Object(_)))
    {
        return Err(ProtocolError::InvalidObject(
            "Gate 1 file descriptor is outside the Parquet append profile".into(),
        ));
    }
    require_gate1_i64(file, "file_size_bytes", "Gate 1 added file")?;
    require_gate1_i64(file, "record_count", "Gate 1 added file")?;
    require_nonempty_string(file, "content_sha256", "Gate 1 added file")?.parse::<Sha256>()?;
    validate_gate1_metrics(file)
}

fn validate_gate1_metrics(file: &BTreeMap<String, CanonicalValue>) -> Result<(), ProtocolError> {
    let Some(CanonicalValue::Array(metrics)) = file.get("metrics") else {
        return Err(ProtocolError::InvalidObject(
            "Gate 1 file metrics must be an array".into(),
        ));
    };
    let mut field_ids = BTreeSet::new();
    for metric in metrics {
        let CanonicalValue::Object(metric) = metric else {
            return Err(ProtocolError::InvalidObject(
                "Gate 1 metrics entries must be objects".into(),
            ));
        };
        require_exact_fields(
            metric,
            &[
                "field_id",
                "column_size_bytes",
                "value_count",
                "null_count",
                "nan_count",
                "distinct_count",
                "lower_bound",
                "upper_bound",
                "metadata",
            ],
            "Gate 1 metric",
        )?;
        let field_id = require_positive_u32(metric, "field_id", "Gate 1 metric")?;
        if !field_ids.insert(field_id) {
            return Err(ProtocolError::InvalidObject(
                "Gate 1 metric field IDs must be unique".into(),
            ));
        }
        for field in [
            "column_size_bytes",
            "value_count",
            "null_count",
            "nan_count",
            "distinct_count",
        ] {
            if !matches!(metric.get(field), Some(CanonicalValue::Null)) {
                require_gate1_i64(metric, field, "Gate 1 metric")?;
            }
        }
        for field in ["lower_bound", "upper_bound"] {
            match metric.get(field) {
                Some(CanonicalValue::Null) => {}
                Some(value @ CanonicalValue::Object(_)) => {
                    let scalar: TypedScalar =
                        canonical_json::from_slice(&canonical_json::to_vec(value)?)?;
                    scalar.validate()?;
                }
                _ => {
                    return Err(ProtocolError::InvalidObject(format!(
                        "Gate 1 metric {field} must be null or a typed scalar"
                    )));
                }
            }
        }
        if !matches!(metric.get("metadata"), Some(CanonicalValue::Object(_))) {
            return Err(ProtocolError::InvalidObject(
                "Gate 1 metric metadata must be an object".into(),
            ));
        }
    }
    Ok(())
}

fn validate_commit_snapshot(
    operation: &BTreeMap<String, CanonicalValue>,
) -> Result<Id, ProtocolError> {
    require_exact_fields(
        operation,
        &[
            "operation_id",
            "type",
            "target_ref",
            "snapshot",
            "added_files",
            "removed_file_ids",
            "scan_projection",
            "rebase_mode",
        ],
        "commit_snapshot",
    )?;
    require_nonempty_string(operation, "target_ref", "commit_snapshot")?;
    require_nonempty_string(operation, "rebase_mode", "commit_snapshot")?;
    let Some(CanonicalValue::Object(snapshot)) = operation.get("snapshot") else {
        return Err(ProtocolError::InvalidObject(
            "commit_snapshot snapshot must be an object".into(),
        ));
    };
    let snapshot_id = validate_snapshot(snapshot)?;
    validate_snapshot_file_changes(operation)?;
    if !matches!(
        operation.get("scan_projection"),
        Some(CanonicalValue::Null | CanonicalValue::Object(_))
    ) {
        return Err(ProtocolError::InvalidObject(
            "commit_snapshot scan_projection must be null or an object".into(),
        ));
    }
    Ok(snapshot_id)
}

fn validate_snapshot(snapshot: &BTreeMap<String, CanonicalValue>) -> Result<Id, ProtocolError> {
    require_exact_fields(
        snapshot,
        &[
            "snapshot_id",
            "parent_snapshot_id",
            "sequence_number",
            "schema_id",
            "partition_spec_id",
            "sort_order_id",
            "operation",
            "summary",
            "metadata",
        ],
        "commit_snapshot snapshot",
    )?;
    let snapshot_id = parse_id(snapshot, "snapshot_id", "commit_snapshot snapshot")?;
    match snapshot.get("parent_snapshot_id") {
        Some(CanonicalValue::Null) => {}
        Some(CanonicalValue::String(value)) => {
            value.parse::<Id>()?;
        }
        _ => {
            return Err(ProtocolError::InvalidObject(
                "commit_snapshot parent_snapshot_id must be null or an ID".into(),
            ));
        }
    }
    if require_decimal_u64(snapshot, "sequence_number", "commit_snapshot snapshot")? == 0 {
        return Err(ProtocolError::InvalidObject(
            "commit_snapshot sequence_number must be positive".into(),
        ));
    }
    require_positive_u32(snapshot, "schema_id", "commit_snapshot snapshot")?;
    require_u32(snapshot, "partition_spec_id", "commit_snapshot snapshot")?;
    require_u32(snapshot, "sort_order_id", "commit_snapshot snapshot")?;
    let operation_label =
        require_nonempty_string(snapshot, "operation", "commit_snapshot snapshot")?;
    if !matches!(
        operation_label,
        "append"
            | "overwrite"
            | "rewrite"
            | "delete"
            | "update"
            | "merge"
            | "optimize"
            | "metadata"
    ) {
        return Err(ProtocolError::InvalidObject(
            "commit_snapshot has an unknown snapshot operation label".into(),
        ));
    }
    for field in ["summary", "metadata"] {
        if !matches!(snapshot.get(field), Some(CanonicalValue::Object(_))) {
            return Err(ProtocolError::InvalidObject(format!(
                "commit_snapshot snapshot {field} must be an object"
            )));
        }
    }
    Ok(snapshot_id)
}

fn validate_snapshot_file_changes(
    operation: &BTreeMap<String, CanonicalValue>,
) -> Result<(), ProtocolError> {
    let Some(CanonicalValue::Array(added_files)) = operation.get("added_files") else {
        return Err(ProtocolError::InvalidObject(
            "commit_snapshot added_files must be an array".into(),
        ));
    };
    let mut added_file_ids = BTreeSet::new();
    for file in added_files {
        let CanonicalValue::Object(file) = file else {
            return Err(ProtocolError::InvalidObject(
                "commit_snapshot added_files entries must be objects".into(),
            ));
        };
        let file_id = validate_added_file(file)?;
        if !added_file_ids.insert(file_id) {
            return Err(ProtocolError::InvalidObject(
                "commit_snapshot added file IDs must be unique".into(),
            ));
        }
    }
    let Some(CanonicalValue::Array(removed_file_ids)) = operation.get("removed_file_ids") else {
        return Err(ProtocolError::InvalidObject(
            "commit_snapshot removed_file_ids must be an array".into(),
        ));
    };
    let mut removed = BTreeSet::new();
    for file_id in removed_file_ids {
        let CanonicalValue::String(file_id) = file_id else {
            return Err(ProtocolError::InvalidObject(
                "commit_snapshot removed_file_ids entries must be IDs".into(),
            ));
        };
        let file_id = file_id.parse::<Id>()?;
        if !removed.insert(file_id) || added_file_ids.contains(&file_id) {
            return Err(ProtocolError::InvalidObject(
                "commit_snapshot file changes must be unique and disjoint".into(),
            ));
        }
    }
    Ok(())
}

fn validate_added_file(file: &BTreeMap<String, CanonicalValue>) -> Result<Id, ProtocolError> {
    // Descriptor fields are feature-extensible; core validation binds the
    // stable identity and location while feature validators own the rest.
    let file_id = parse_id(file, "file_id", "commit_snapshot added file")?;
    require_nonempty_string(file, "uri", "commit_snapshot added file")?;
    if file
        .get("metadata")
        .is_some_and(|metadata| !matches!(metadata, CanonicalValue::Object(_)))
    {
        return Err(ProtocolError::InvalidObject(
            "commit_snapshot added file metadata must be an object".into(),
        ));
    }
    Ok(file_id)
}

fn require_exact_fields(
    object: &BTreeMap<String, CanonicalValue>,
    required: &[&str],
    context: &str,
) -> Result<(), ProtocolError> {
    require_fields(object, required, &[], context)
}

fn require_fields(
    object: &BTreeMap<String, CanonicalValue>,
    required: &[&str],
    optional: &[&str],
    context: &str,
) -> Result<(), ProtocolError> {
    if required.iter().any(|field| !object.contains_key(*field))
        || object
            .keys()
            .any(|field| !required.contains(&field.as_str()) && !optional.contains(&field.as_str()))
    {
        return Err(ProtocolError::InvalidObject(format!(
            "{context} has missing or unknown fields"
        )));
    }
    Ok(())
}

fn require_nonempty_string<'a>(
    object: &'a BTreeMap<String, CanonicalValue>,
    field: &str,
    context: &str,
) -> Result<&'a str, ProtocolError> {
    match object.get(field) {
        Some(CanonicalValue::String(value)) if !value.is_empty() => Ok(value),
        _ => Err(ProtocolError::InvalidObject(format!(
            "{context} {field} must be a nonempty string"
        ))),
    }
}

fn require_decimal_u64(
    object: &BTreeMap<String, CanonicalValue>,
    field: &str,
    context: &str,
) -> Result<u64, ProtocolError> {
    let value = require_nonempty_string(object, field, context)?;
    let parsed = value
        .parse::<u64>()
        .map_err(|_| ProtocolError::InvalidIntegerString(value.into()))?;
    if parsed.to_string() != value {
        return Err(ProtocolError::InvalidIntegerString(value.into()));
    }
    Ok(parsed)
}

fn require_gate1_i64(
    object: &BTreeMap<String, CanonicalValue>,
    field: &str,
    context: &str,
) -> Result<u64, ProtocolError> {
    let value = require_decimal_u64(object, field, context)?;
    if value > i64::MAX as u64 {
        return Err(ProtocolError::InvalidObject(format!(
            "{context} {field} exceeds the Gate 1 SQLite INTEGER range"
        )));
    }
    Ok(value)
}

fn require_u32(
    object: &BTreeMap<String, CanonicalValue>,
    field: &str,
    context: &str,
) -> Result<u32, ProtocolError> {
    let value = require_decimal_u64(object, field, context)?;
    u32::try_from(value).map_err(|_| {
        ProtocolError::InvalidObject(format!("{context} {field} is outside the u32 range"))
    })
}

fn require_positive_u32(
    object: &BTreeMap<String, CanonicalValue>,
    field: &str,
    context: &str,
) -> Result<u32, ProtocolError> {
    let value = require_u32(object, field, context)?;
    if value == 0 {
        return Err(ProtocolError::InvalidObject(format!(
            "{context} {field} must be positive"
        )));
    }
    Ok(value)
}

fn parse_id(
    object: &BTreeMap<String, CanonicalValue>,
    field: &str,
    context: &str,
) -> Result<Id, ProtocolError> {
    require_nonempty_string(object, field, context)?.parse()
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub table_version: JsonU64,
    pub uri: RelativeUri,
    pub sha256: Sha256,
    pub length: JsonU64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataImage {
    pub codec: String,
    pub page_size: u32,
    pub page_count: JsonU64,
    pub checkpoint: Checkpoint,
    pub page_map: Option<CanonicalValue>,
    pub image_root_sha256: Sha256,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Generation {
    pub kind: String,
    pub format_version: u32,
    pub table_id: Id,
    pub table_version: JsonU64,
    pub generation_id: Id,
    pub created_at_ms: JsonI64,
    pub semantic_state_sha256: Sha256,
    pub semantic_commit: ObjectReference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_parent: Option<ObjectReference>,
    pub metadata_image: MetadataImage,
    pub scan_projection: Option<CanonicalValue>,
    pub metadata: BTreeMap<String, CanonicalValue>,
}

impl Generation {
    pub fn validate_gate1(&self) -> Result<(), ProtocolError> {
        if self.kind != "otmp.metadata-generation"
            || self.format_version != 1
            || self.metadata_image.codec != "otmp.metadata.sqlite3-cow.v1"
            || self.metadata_image.page_size != 4096
            || self.metadata_image.page_map.is_some()
            || self.scan_projection.is_some()
            || self.metadata_image.checkpoint.table_version != self.table_version
        {
            return Err(ProtocolError::InvalidObject(
                "generation is outside the Gate 1 full-image profile".into(),
            ));
        }
        Ok(())
    }
}
