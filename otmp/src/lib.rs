//! Catalog-free OTMP runtime with incremental writes and materialized readers.

mod checkpoint_index;
mod cow_writer;
mod error;
mod image;
mod physical;
mod runtime;
mod sql_writer;
pub mod storage;

pub use error::RuntimeError;
pub use runtime::{
    AppendFile, AppendRequest, AppendResult, CommitMetadata, CommittedFile, FileFormat, FileMetric,
    HistoryEntry, InitializeRequest, LiveFile, PinnedTable, SnapshotMetadata, SourceFingerprint,
    Status, Table, TransactionRetryPolicy, VerifiedStagedFile,
};
pub use storage::{
    ConditionalWriteOutcome, InMemoryObjectStore, InjectedConditional, LocalObjectStore,
    ObjectMetadata, ObjectStore, ObjectVersion, StorageError, StoredRange,
};

pub use runtime::{
    HeadAnchor, MetadataCoordinates, MetadataSelection, OperationRequest, OperationResult,
    PinnedMetadata, RefType, Requirement, ResolvedSnapshot, SnapshotDescriptor, SnapshotSelection,
    TransactionRequest, TransactionResult, VerificationReport, VerificationScope,
};
