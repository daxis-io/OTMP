//! Catalog-free OTMP runtime with incremental writes and materialized readers.

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
    ObjectStore, ObjectVersion, StorageError,
};

pub use runtime::{
    HeadAnchor, MetadataCoordinates, MetadataSelection, OperationRequest, OperationResult,
    PinnedMetadata, RefType, Requirement, ResolvedSnapshot, SnapshotDescriptor, SnapshotSelection,
    TransactionRequest, TransactionResult, VerificationReport, VerificationScope,
};
