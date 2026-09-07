use std::collections::{BTreeMap, VecDeque, btree_map};
use std::fs::{self, OpenOptions as StdOpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use fs2::FileExt;
use otmp_protocol::{RelativeUri, Sha256};
use sha2::{Digest, Sha256 as Sha256Hasher};
use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// The largest request accepted by the authenticated range interface.
pub const MAXIMUM_RANGE_LENGTH: u64 = 64 * 1024 * 1024;

static HEAD_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectVersion(String);

impl ObjectVersion {
    #[must_use]
    pub fn from_sha256(hash: Sha256) -> Self {
        Self(hash.to_string())
    }

    /// Constructs a version from a provider-defined opaque token.
    ///
    /// Providers must keep this token out of protocol objects and treat it as
    /// private runtime state.
    #[must_use]
    pub fn from_opaque(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    /// Returns the provider-defined opaque token.
    #[must_use]
    pub fn as_opaque(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct StoredObject {
    pub bytes: Vec<u8>,
    pub version: ObjectVersion,
}

/// Runtime metadata that pins a bounded read to one object revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    pub length: u64,
    pub version: ObjectVersion,
}

/// An exact bounded object response and the metadata used to authenticate it.
#[derive(Clone, Debug)]
pub struct StoredRange {
    pub bytes: Vec<u8>,
    pub range: std::ops::Range<u64>,
    pub metadata: ObjectMetadata,
}

impl StoredRange {
    /// Checks that a provider returned precisely the requested bytes from the
    /// object revision selected by the caller.
    pub fn validate(
        &self,
        expected: &ObjectMetadata,
        range: &std::ops::Range<u64>,
    ) -> Result<(), StorageError> {
        validate_range_request(expected, range)?;
        if &self.metadata != expected
            || &self.range != range
            || self.bytes.len() as u64 != range.end - range.start
        {
            return Err(StorageError::VerificationFailed(
                "range response did not match the requested object revision".into(),
            ));
        }
        Ok(())
    }
}

/// Validates a range before an adapter performs I/O.
pub fn validate_range_request(
    metadata: &ObjectMetadata,
    range: &std::ops::Range<u64>,
) -> Result<(), StorageError> {
    let length = range
        .end
        .checked_sub(range.start)
        .ok_or_else(|| StorageError::VerificationFailed("range end precedes its start".into()))?;
    if length == 0 || length > MAXIMUM_RANGE_LENGTH || range.end > metadata.length {
        return Err(StorageError::VerificationFailed(
            "range is empty, oversized, or outside the object".into(),
        ));
    }
    usize::try_from(length).map_err(|_| {
        StorageError::VerificationFailed("range cannot fit this platform's memory model".into())
    })?;
    Ok(())
}

#[derive(Clone, Debug)]
pub struct CreatedObject {
    pub version: ObjectVersion,
    pub sha256: Sha256,
    pub length: u64,
}

#[derive(Debug, Error, Clone)]
pub enum StorageError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("immutable object already exists: {0}")]
    ImmutableConflict(String),
    #[error("unsafe object key: {0}")]
    UnsafeKey(String),
    #[error("storage I/O failure: {0}")]
    Io(String),
    #[error("source exceeded the declared maximum length")]
    MaximumLengthExceeded,
    #[error("stored object failed exact byte verification: {0}")]
    VerificationFailed(String),
    #[error("injected storage failure: {0}")]
    Injected(String),
    #[error("storage operation is unsupported: {0}")]
    Unsupported(String),
}

impl StorageError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "OTMP_OBJECT_NOT_FOUND",
            Self::ImmutableConflict(_) => "OTMP_IMMUTABLE_OBJECT_CONFLICT",
            Self::UnsafeKey(_) => "OTMP_UNSAFE_URI",
            Self::MaximumLengthExceeded | Self::VerificationFailed(_) => {
                "OTMP_FINGERPRINT_MISMATCH"
            }
            Self::Io(_) | Self::Injected(_) | Self::Unsupported(_) => "OTMP_STORAGE_ERROR",
        }
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        matches!(self, Self::Io(_) | Self::Injected(_))
    }
}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

#[derive(Clone, Debug)]
pub enum ConditionalWriteOutcome {
    Applied {
        new_version: ObjectVersion,
    },
    Conflict {
        current_version: Option<ObjectVersion>,
    },
    Indeterminate {
        source: StorageError,
    },
}

#[async_trait]
pub trait ObjectStore: Clone + Send + Sync + 'static {
    async fn read(&self, key: &RelativeUri) -> Result<StoredObject, StorageError>;

    async fn stat(&self, _key: &RelativeUri) -> Result<ObjectMetadata, StorageError> {
        Err(StorageError::Unsupported(
            "object metadata is not implemented by this storage adapter".into(),
        ))
    }

    /// Reads one bounded byte range. Implementations must not fall back to a
    /// full-object read when their transport does not support ranges.
    async fn read_range(
        &self,
        _key: &RelativeUri,
        _range: std::ops::Range<u64>,
        _expected: &ObjectMetadata,
    ) -> Result<StoredRange, StorageError> {
        Err(StorageError::Unsupported(
            "bounded range reads are not implemented by this storage adapter".into(),
        ))
    }

    async fn create_from_reader(
        &self,
        key: &RelativeUri,
        reader: &mut (dyn AsyncRead + Send + Unpin),
        maximum_length: Option<u64>,
    ) -> Result<CreatedObject, StorageError>;

    async fn create_bytes(
        &self,
        key: &RelativeUri,
        bytes: &[u8],
    ) -> Result<CreatedObject, StorageError> {
        let mut reader = std::io::Cursor::new(bytes.to_vec());
        self.create_from_reader(key, &mut reader, Some(bytes.len() as u64))
            .await
    }

    async fn create_head(&self, bytes: &[u8]) -> ConditionalWriteOutcome;

    async fn replace_head(&self, expected: &ObjectVersion, bytes: &[u8])
    -> ConditionalWriteOutcome;

    async fn delete_if_version(
        &self,
        key: &RelativeUri,
        version: &ObjectVersion,
    ) -> Result<bool, StorageError>;

    async fn confirm_readable(
        &self,
        key: &RelativeUri,
        sha256: Sha256,
        length: u64,
    ) -> Result<ObjectVersion, StorageError> {
        let object = self.read(key).await?;
        if object.bytes.len() as u64 != length || Sha256::digest(&object.bytes) != sha256 {
            return Err(StorageError::VerificationFailed(format!(
                "object verification failed for {key}"
            )));
        }
        Ok(object.version)
    }
}

#[derive(Clone, Debug)]
pub struct LocalObjectStore {
    root: Arc<PathBuf>,
}

impl LocalObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root: Arc::new(root),
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, key: &RelativeUri) -> PathBuf {
        self.root.join(key.as_str())
    }

    // This token is deliberately separate from the SHA-256 conditional-head
    // token: range reads need a cheap local identity and never hash a whole
    // object merely to service a page request.
    fn metadata_for(metadata: &std::fs::Metadata) -> Result<ObjectMetadata, StorageError> {
        let modified = metadata
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| StorageError::Io(error.to_string()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(ObjectMetadata {
                length: metadata.len(),
                version: ObjectVersion::from_opaque(format!(
                    "otmp-local-range-v1:{}:{}:{}:{}:{}:{}:{}",
                    metadata.dev(),
                    metadata.ino(),
                    metadata.len(),
                    modified.as_secs(),
                    modified.subsec_nanos(),
                    metadata.ctime(),
                    metadata.ctime_nsec()
                )),
            })
        }
        #[cfg(not(unix))]
        Ok(ObjectMetadata {
            length: metadata.len(),
            version: ObjectVersion::from_opaque(format!(
                "otmp-local-range-v1:{}:{}:{}",
                metadata.len(),
                modified.as_secs(),
                modified.subsec_nanos()
            )),
        })
    }

    fn head_key() -> RelativeUri {
        "_otmp/HEAD".parse().expect("constant HEAD URI is safe")
    }

    fn lock_head(&self) -> Result<std::fs::File, StorageError> {
        let directory = self.root.join("_otmp");
        fs::create_dir_all(&directory)?;
        let lock = StdOpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join(".HEAD.lock"))?;
        lock.lock_exclusive()?;
        Ok(lock)
    }

    fn write_head_locked(&self, bytes: &[u8]) -> Result<ObjectVersion, StorageError> {
        let directory = self.root.join("_otmp");
        fs::create_dir_all(&directory)?;
        let nonce = HEAD_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temp = directory.join(format!(".HEAD.{}.{nonce}.tmp", std::process::id()));
        let mut file = StdOpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        storage_failpoint("during_temporary_head_creation");
        fs::rename(&temp, directory.join("HEAD"))?;
        StdOpenOptions::new()
            .read(true)
            .open(&directory)?
            .sync_all()?;
        storage_failpoint("after_final_head_rename");
        Ok(ObjectVersion::from_sha256(Sha256::digest(bytes)))
    }
}

fn storage_failpoint(name: &str) {
    if std::env::var("OTMP_FAILPOINT").as_deref() == Ok(name) {
        std::process::exit(86);
    }
}

#[async_trait]
impl ObjectStore for LocalObjectStore {
    async fn read(&self, key: &RelativeUri) -> Result<StoredObject, StorageError> {
        let bytes = tokio::fs::read(self.path(key))
            .await
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::NotFound => StorageError::NotFound(key.to_string()),
                _ => error.into(),
            })?;
        Ok(StoredObject {
            version: ObjectVersion::from_sha256(Sha256::digest(&bytes)),
            bytes,
        })
    }

    async fn stat(&self, key: &RelativeUri) -> Result<ObjectMetadata, StorageError> {
        tokio::fs::metadata(self.path(key))
            .await
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::NotFound => StorageError::NotFound(key.to_string()),
                _ => error.into(),
            })
            .and_then(|metadata| Self::metadata_for(&metadata))
    }

    async fn read_range(
        &self,
        key: &RelativeUri,
        range: std::ops::Range<u64>,
        expected: &ObjectMetadata,
    ) -> Result<StoredRange, StorageError> {
        validate_range_request(expected, &range)?;
        let path = self.path(key);
        let before = tokio::fs::metadata(&path)
            .await
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::NotFound => StorageError::NotFound(key.to_string()),
                _ => error.into(),
            })?;
        if Self::metadata_for(&before)? != *expected {
            return Err(StorageError::VerificationFailed(
                "local object version changed before range read".into(),
            ));
        }
        let length = usize::try_from(range.end - range.start).map_err(|_| {
            StorageError::VerificationFailed("range cannot fit this platform's memory model".into())
        })?;
        let mut file = tokio::fs::File::open(&path).await?;
        if Self::metadata_for(&file.metadata().await?)? != *expected {
            return Err(StorageError::VerificationFailed(
                "local object changed while opening range".into(),
            ));
        }
        file.seek(std::io::SeekFrom::Start(range.start)).await?;
        let mut bytes = vec![0; length];
        file.read_exact(&mut bytes).await?;
        let after = tokio::fs::metadata(path).await?;
        if Self::metadata_for(&after)? != *expected
            || Self::metadata_for(&file.metadata().await?)? != *expected
        {
            return Err(StorageError::VerificationFailed(
                "local object version changed during range read".into(),
            ));
        }
        Ok(StoredRange {
            bytes,
            range,
            metadata: expected.clone(),
        })
    }

    async fn create_from_reader(
        &self,
        key: &RelativeUri,
        reader: &mut (dyn AsyncRead + Send + Unpin),
        maximum_length: Option<u64>,
    ) -> Result<CreatedObject, StorageError> {
        let path = self.path(key);
        let parent = path
            .parent()
            .ok_or_else(|| StorageError::UnsafeKey(key.to_string()))?;
        tokio::fs::create_dir_all(parent).await?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::AlreadyExists => {
                    StorageError::ImmutableConflict(key.to_string())
                }
                _ => error.into(),
            })?;
        let copy = async {
            let mut hasher = Sha256Hasher::new();
            let mut length = 0_u64;
            let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
            loop {
                let read = reader.read(&mut buffer).await?;
                if read == 0 {
                    break;
                }
                length = length
                    .checked_add(read as u64)
                    .ok_or_else(|| StorageError::Io("object length overflow".into()))?;
                if maximum_length.is_some_and(|maximum| length > maximum) {
                    return Err(StorageError::MaximumLengthExceeded);
                }
                hasher.update(&buffer[..read]);
                file.write_all(&buffer[..read]).await?;
            }
            file.flush().await?;
            file.sync_all().await?;
            drop(file);
            StdOpenOptions::new().read(true).open(parent)?.sync_all()?;
            let hash = Sha256::from_bytes(hasher.finalize().into());
            Ok(CreatedObject {
                version: ObjectVersion::from_sha256(hash),
                sha256: hash,
                length,
            })
        }
        .await;
        if copy.is_err() && tokio::fs::remove_file(&path).await.is_ok() {
            let _ = StdOpenOptions::new()
                .read(true)
                .open(parent)
                .and_then(|directory| directory.sync_all());
        }
        copy
    }

    async fn create_head(&self, bytes: &[u8]) -> ConditionalWriteOutcome {
        let result = (|| {
            let _lock = self.lock_head()?;
            let path = self.path(&Self::head_key());
            if path.exists() {
                return Ok(ConditionalWriteOutcome::Conflict {
                    current_version: fs::read(path)
                        .ok()
                        .map(|bytes| ObjectVersion::from_sha256(Sha256::digest(bytes))),
                });
            }
            self.write_head_locked(bytes)
                .map(|new_version| ConditionalWriteOutcome::Applied { new_version })
        })();
        result.unwrap_or_else(|source| ConditionalWriteOutcome::Indeterminate { source })
    }

    async fn replace_head(
        &self,
        expected: &ObjectVersion,
        bytes: &[u8],
    ) -> ConditionalWriteOutcome {
        let result = (|| {
            let _lock = self.lock_head()?;
            let path = self.path(&Self::head_key());
            let current = match fs::read(path) {
                Ok(current) => Some(ObjectVersion::from_sha256(Sha256::digest(current))),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            if current.as_ref() != Some(expected) {
                return Ok(ConditionalWriteOutcome::Conflict {
                    current_version: current,
                });
            }
            self.write_head_locked(bytes)
                .map(|new_version| ConditionalWriteOutcome::Applied { new_version })
        })();
        result.unwrap_or_else(|source| ConditionalWriteOutcome::Indeterminate { source })
    }

    async fn delete_if_version(
        &self,
        key: &RelativeUri,
        version: &ObjectVersion,
    ) -> Result<bool, StorageError> {
        let path = self.path(key);
        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if ObjectVersion::from_sha256(Sha256::digest(bytes)) != *version {
            return Ok(false);
        }
        let parent = path
            .parent()
            .ok_or_else(|| StorageError::UnsafeKey(key.to_string()))?
            .to_path_buf();
        tokio::fs::remove_file(path).await?;
        StdOpenOptions::new().read(true).open(parent)?.sync_all()?;
        Ok(true)
    }
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryObjectStore {
    inner: Arc<Mutex<MemoryState>>,
}

#[derive(Debug, Default)]
struct MemoryState {
    objects: BTreeMap<String, MemoryObject>,
    next_revision: u64,
    conditional_outcomes: VecDeque<InjectedConditional>,
    reads: u64,
    listings: u64,
}

#[derive(Debug)]
struct MemoryObject {
    bytes: Vec<u8>,
    revision: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum InjectedConditional {
    Conflict,
    IndeterminateBefore,
    IndeterminateAfter,
}

impl InMemoryObjectStore {
    pub fn inject_conditional(&self, outcome: InjectedConditional) {
        self.inner
            .lock()
            .expect("memory store lock poisoned")
            .conditional_outcomes
            .push_back(outcome);
    }

    /// Replaces bytes without observing immutability, solely for corruption tests.
    pub fn replace_object_for_test(&self, key: &RelativeUri, bytes: Vec<u8>) {
        let mut state = self.inner.lock().expect("memory store lock poisoned");
        state.next_revision += 1;
        let revision = state.next_revision;
        state
            .objects
            .insert(key.to_string(), MemoryObject { bytes, revision });
    }

    #[must_use]
    pub fn read_count(&self) -> u64 {
        self.inner.lock().expect("memory store lock poisoned").reads
    }

    #[must_use]
    pub fn listing_count(&self) -> u64 {
        self.inner
            .lock()
            .expect("memory store lock poisoned")
            .listings
    }

    fn conditional(
        &self,
        expected: Option<&ObjectVersion>,
        bytes: &[u8],
    ) -> ConditionalWriteOutcome {
        let mut state = self.inner.lock().expect("memory store lock poisoned");
        let injected = state.conditional_outcomes.pop_front();
        let current = state
            .objects
            .get("_otmp/HEAD")
            .map(|value| ObjectVersion::from_sha256(Sha256::digest(&value.bytes)));
        if matches!(injected, Some(InjectedConditional::IndeterminateBefore)) {
            return ConditionalWriteOutcome::Indeterminate {
                source: StorageError::Injected("before conditional write".into()),
            };
        }
        if current.as_ref() != expected {
            return ConditionalWriteOutcome::Conflict {
                current_version: current,
            };
        }
        if matches!(injected, Some(InjectedConditional::Conflict)) {
            return ConditionalWriteOutcome::Conflict {
                current_version: current,
            };
        }
        state.next_revision += 1;
        let revision = state.next_revision;
        state.objects.insert(
            "_otmp/HEAD".into(),
            MemoryObject {
                bytes: bytes.to_vec(),
                revision,
            },
        );
        let new_version = ObjectVersion::from_sha256(Sha256::digest(bytes));
        if matches!(injected, Some(InjectedConditional::IndeterminateAfter)) {
            ConditionalWriteOutcome::Indeterminate {
                source: StorageError::Injected("after conditional write".into()),
            }
        } else {
            ConditionalWriteOutcome::Applied { new_version }
        }
    }
}

#[async_trait]
impl ObjectStore for InMemoryObjectStore {
    async fn read(&self, key: &RelativeUri) -> Result<StoredObject, StorageError> {
        let mut state = self.inner.lock().expect("memory store lock poisoned");
        state.reads += 1;
        let bytes = state
            .objects
            .get(key.as_str())
            .map(|object| object.bytes.clone())
            .ok_or_else(|| StorageError::NotFound(key.to_string()))?;
        Ok(StoredObject {
            version: ObjectVersion::from_sha256(Sha256::digest(&bytes)),
            bytes,
        })
    }

    async fn stat(&self, key: &RelativeUri) -> Result<ObjectMetadata, StorageError> {
        let state = self.inner.lock().expect("memory store lock poisoned");
        let object = state
            .objects
            .get(key.as_str())
            .ok_or_else(|| StorageError::NotFound(key.to_string()))?;
        Ok(ObjectMetadata {
            length: object.bytes.len() as u64,
            version: ObjectVersion::from_opaque(format!(
                "otmp-memory-range-v1:{}",
                object.revision
            )),
        })
    }

    async fn read_range(
        &self,
        key: &RelativeUri,
        range: std::ops::Range<u64>,
        expected: &ObjectMetadata,
    ) -> Result<StoredRange, StorageError> {
        validate_range_request(expected, &range)?;
        let state = self.inner.lock().expect("memory store lock poisoned");
        let object = state
            .objects
            .get(key.as_str())
            .ok_or_else(|| StorageError::NotFound(key.to_string()))?;
        let actual = ObjectMetadata {
            length: object.bytes.len() as u64,
            version: ObjectVersion::from_opaque(format!(
                "otmp-memory-range-v1:{}",
                object.revision
            )),
        };
        if actual != *expected {
            return Err(StorageError::VerificationFailed(
                "in-memory object version changed before range read".into(),
            ));
        }
        let start = usize::try_from(range.start).map_err(|_| {
            StorageError::VerificationFailed("range cannot fit this platform's memory model".into())
        })?;
        let end = usize::try_from(range.end).map_err(|_| {
            StorageError::VerificationFailed("range cannot fit this platform's memory model".into())
        })?;
        Ok(StoredRange {
            bytes: object.bytes[start..end].to_vec(),
            range,
            metadata: actual,
        })
    }

    async fn create_from_reader(
        &self,
        key: &RelativeUri,
        reader: &mut (dyn AsyncRead + Send + Unpin),
        maximum_length: Option<u64>,
    ) -> Result<CreatedObject, StorageError> {
        {
            let state = self.inner.lock().expect("memory store lock poisoned");
            if state.objects.contains_key(key.as_str()) {
                return Err(StorageError::ImmutableConflict(key.to_string()));
            }
        }
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await?;
        if maximum_length.is_some_and(|maximum| bytes.len() as u64 > maximum) {
            return Err(StorageError::MaximumLengthExceeded);
        }
        let hash = Sha256::digest(&bytes);
        let mut state = self.inner.lock().expect("memory store lock poisoned");
        if state.objects.contains_key(key.as_str()) {
            return Err(StorageError::ImmutableConflict(key.to_string()));
        }
        state.next_revision += 1;
        let revision = state.next_revision;
        match state.objects.entry(key.to_string()) {
            btree_map::Entry::Vacant(entry) => {
                entry.insert(MemoryObject {
                    bytes: bytes.clone(),
                    revision,
                });
            }
            btree_map::Entry::Occupied(_) => {
                return Err(StorageError::ImmutableConflict(key.to_string()));
            }
        }
        Ok(CreatedObject {
            version: ObjectVersion::from_sha256(hash),
            sha256: hash,
            length: bytes.len() as u64,
        })
    }

    async fn create_head(&self, bytes: &[u8]) -> ConditionalWriteOutcome {
        self.conditional(None, bytes)
    }

    async fn replace_head(
        &self,
        expected: &ObjectVersion,
        bytes: &[u8],
    ) -> ConditionalWriteOutcome {
        self.conditional(Some(expected), bytes)
    }

    async fn delete_if_version(
        &self,
        key: &RelativeUri,
        version: &ObjectVersion,
    ) -> Result<bool, StorageError> {
        let mut state = self.inner.lock().expect("memory store lock poisoned");
        let matches = state.objects.get(key.as_str()).is_some_and(|object| {
            ObjectVersion::from_sha256(Sha256::digest(&object.bytes)) == *version
        });
        if matches {
            state.objects.remove(key.as_str());
        }
        Ok(matches)
    }
}
