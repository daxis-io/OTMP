use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    CanonicalValue, FeatureSet, Id, JsonI64, JsonU64, PROTOCOL, PROTOCOL_VERSION, ProtocolError,
    RelativeUri, Sha256,
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
        let mut keys = BTreeSet::new();
        if self.intents.iter().any(|intent| !keys.insert(&intent.key)) {
            return Err(ProtocolError::InvalidObject(
                "duplicate idempotency key".into(),
            ));
        }
        let mut operation_ids = BTreeSet::new();
        for operation in &self.operations {
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
        }
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
