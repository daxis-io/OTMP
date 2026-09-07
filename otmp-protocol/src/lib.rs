//! Portable values and codecs for OTMP 0.0.2-alpha.

pub mod canonical_json;
mod cbor;
mod error;
mod hash;
mod objects;
mod page_pack;
mod types;
mod value;

pub use cbor::{
    MAX_PAGE_MAP_BYTES, PAGE_MAP_MEDIA_TYPE, PAGE_PACK_MEDIA_TYPE, PageCodec, PageMapBranch,
    PageMapEntry, PageMapNode, PageMapRoot, PageObjectReference, decode_page_map,
    decode_partition_tuple, decode_typed_scalar, encode_page_map, encode_partition_tuple,
    encode_typed_scalar,
};
pub use error::{ErrorPayload, ProtocolError};
pub use hash::{
    genesis_state_hash, image_root_hash, intent_hash, next_state_hash, object_hash, partition_hash,
};
pub use objects::{
    CHECKPOINT_MEDIA_TYPE, COMMIT_MEDIA_TYPE, Checkpoint, GENERATION_MEDIA_TYPE, Generation,
    HEAD_MEDIA_TYPE, Head, IntentRecord, MetadataImage, ObjectReference, SemanticCommit,
};
pub use page_pack::{PackIndex, PackIndexEntry, decode_pack_index, encode_page_pack};
pub use types::{Field, LogicalType, Schema};
pub use value::{
    CanonicalValue, FeatureSet, Id, JsonI64, JsonU64, RelativeUri, Sha256, TypedScalar, UuidValue,
};

pub const PROTOCOL: &str = "otmp";
pub const PROTOCOL_VERSION: &str = "0.0.2-alpha";
pub const CORE_FEATURE: &str = "otmp.core.v2";
pub const SQLITE_COW_FEATURE: &str = "otmp.metadata.sqlite3-cow.v1";
pub const PARQUET_FEATURE: &str = "otmp.data.parquet.v1";
