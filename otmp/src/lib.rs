//! Catalog-free local/full-image OTMP runtime.

mod error;
mod image;
mod runtime;
pub mod storage;

pub use error::RuntimeError;
pub use runtime::{
    AppendFile, AppendRequest, AppendResult, AppendRetryPolicy, CommitMetadata, CommittedFile,
    FileFormat, FileMetric, HistoryEntry, InitializeRequest, LiveFile, PinnedTable,
    SnapshotMetadata, SourceFingerprint, Status, Table, VerifiedStagedFile,
};
pub use storage::{
    ConditionalWriteOutcome, InMemoryObjectStore, InjectedConditional, LocalObjectStore,
    ObjectStore, ObjectVersion, StorageError,
};
