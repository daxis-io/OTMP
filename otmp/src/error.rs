use std::collections::BTreeMap;

use otmp_protocol::{ErrorPayload, ProtocolError};
use thiserror::Error;

use crate::storage::StorageError;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("transaction is invalid: {0}")]
    InvalidTransaction(String),
    #[error("semantic conflict: {0}")]
    SemanticConflict(String),
    #[error("snapshot not found")]
    SnapshotNotFound,
    #[error("metadata version not found: {0}")]
    MetadataVersionNotFound(u64),
    #[error("history not retained for version: {0}")]
    HistoryNotRetained(u64),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("SQLite failure: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("I/O failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("table already exists")]
    AlreadyExists,
    #[error("idempotency key was reused for a different logical intent")]
    IdempotencyConflict,
    #[error("source or staged object does not match the expected fingerprint")]
    FingerprintMismatch,
    #[error("verified staged files do not match the logical request: {0}")]
    StagingMismatch(String),
    #[error("append request is invalid: {0}")]
    InvalidAppend(String),
    #[error("initialize request is invalid: {0}")]
    InvalidInitialize(String),
    #[error("conditional publication remained indeterminate after reconciliation")]
    PublicationIndeterminate,
    #[error("append-safe rebase retry limit was exhausted")]
    RebaseExhausted,
    #[error("table is corrupt: {0}")]
    Corrupt(String),
    #[error("requested ref does not exist: {0}")]
    RefNotFound(String),
}

impl RuntimeError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidTransaction(_) => "OTMP_INVALID_TRANSACTION",
            Self::SemanticConflict(_) => "OTMP_SEMANTIC_CONFLICT",
            Self::SnapshotNotFound => "OTMP_SNAPSHOT_NOT_FOUND",
            Self::MetadataVersionNotFound(_) => "OTMP_METADATA_VERSION_NOT_FOUND",
            Self::HistoryNotRetained(_) => "OTMP_HISTORY_NOT_RETAINED",
            Self::Protocol(error) => error.code(),
            Self::Storage(error) => error.code(),
            Self::Sqlite(_) => "OTMP_SQLITE_ERROR",
            Self::Io(_) => "OTMP_IO_ERROR",
            Self::AlreadyExists => "OTMP_ALREADY_EXISTS",
            Self::IdempotencyConflict => "OTMP_IDEMPOTENCY_CONFLICT",
            Self::FingerprintMismatch => "OTMP_FINGERPRINT_MISMATCH",
            Self::StagingMismatch(_) => "OTMP_STAGING_MISMATCH",
            Self::InvalidAppend(_) => "OTMP_INVALID_APPEND",
            Self::InvalidInitialize(_) => "OTMP_INVALID_INITIALIZE",
            Self::PublicationIndeterminate => "OTMP_PUBLICATION_INDETERMINATE",
            Self::RebaseExhausted => "OTMP_REBASE_EXHAUSTED",
            Self::Corrupt(_) => "OTMP_CORRUPT_TABLE",
            Self::RefNotFound(_) => "OTMP_REF_NOT_FOUND",
        }
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        match self {
            Self::PublicationIndeterminate | Self::RebaseExhausted => true,
            Self::Storage(error) => error.retryable(),
            _ => false,
        }
    }

    #[must_use]
    pub fn payload(&self) -> ErrorPayload {
        ErrorPayload {
            code: self.code().to_owned(),
            message: self.to_string(),
            retryable: self.retryable(),
            details: BTreeMap::new(),
        }
    }
}
