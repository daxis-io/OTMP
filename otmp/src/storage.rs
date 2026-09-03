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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

static HEAD_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectVersion(String);

impl ObjectVersion {
    #[must_use]
    pub fn from_sha256(hash: Sha256) -> Self {
        Self(hash.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct StoredObject {
    pub bytes: Vec<u8>,
    pub version: ObjectVersion,
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
            Self::Io(_) | Self::Injected(_) => "OTMP_STORAGE_ERROR",
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
    objects: BTreeMap<String, Vec<u8>>,
    conditional_outcomes: VecDeque<InjectedConditional>,
    reads: u64,
    listings: u64,
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
        self.inner
            .lock()
            .expect("memory store lock poisoned")
            .objects
            .insert(key.to_string(), bytes);
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
            .map(|value| ObjectVersion::from_sha256(Sha256::digest(value)));
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
        state.objects.insert("_otmp/HEAD".into(), bytes.to_vec());
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
            .cloned()
            .ok_or_else(|| StorageError::NotFound(key.to_string()))?;
        Ok(StoredObject {
            version: ObjectVersion::from_sha256(Sha256::digest(&bytes)),
            bytes,
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
        match state.objects.entry(key.to_string()) {
            btree_map::Entry::Vacant(entry) => {
                entry.insert(bytes.clone());
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
        let matches = state
            .objects
            .get(key.as_str())
            .is_some_and(|bytes| ObjectVersion::from_sha256(Sha256::digest(bytes)) == *version);
        if matches {
            state.objects.remove(key.as_str());
        }
        Ok(matches)
    }
}
