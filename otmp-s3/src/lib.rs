//! S3-compatible object-store adapter for OTMP's local/full-image runtime.
//!
//! The adapter uses Apache `object_store` conditional `PutObject` support. It
//! intentionally buffers only exact-length objects up to 64 MiB and does not
//! emulate conditional deletion with a read followed by delete.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{
    ObjectStore as ApacheObjectStore, ObjectStoreExt as _, PutMode, PutOptions, UpdateVersion,
};
use otmp::storage::{CreatedObject, StoredObject};
use otmp::{ConditionalWriteOutcome, ObjectStore, ObjectVersion, StorageError};
use otmp_protocol::{RelativeUri, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Largest object the first adapter profile sends as one buffered `PutObject`.
pub const MAXIMUM_BUFFERED_OBJECT_LENGTH: u64 = 64 * 1024 * 1024;

const HEAD_KEY: &str = "_otmp/HEAD";
const VERSION_PREFIX: &str = "otmp-s3-v1:";

/// An OTMP [`ObjectStore`] backed by an S3-compatible Apache `object_store`.
#[derive(Clone)]
pub struct S3ObjectStore {
    inner: Arc<dyn ApacheObjectStore>,
}

impl fmt::Debug for S3ObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3ObjectStore")
            .finish_non_exhaustive()
    }
}

impl S3ObjectStore {
    /// Wraps an already-configured Apache object store.
    #[must_use]
    pub fn from_object_store(inner: Arc<dyn ApacheObjectStore>) -> Self {
        Self { inner }
    }

    /// Builds the Apache S3 client with its conditional-put support enabled.
    pub fn from_amazon_s3(builder: AmazonS3Builder) -> Result<Self, StorageError> {
        builder
            .build()
            .map(|store| Self::from_object_store(Arc::new(store)))
            .map_err(storage_error)
    }

    /// Builds an S3 store isolated beneath a provider-side prefix.
    pub fn from_amazon_s3_with_prefix(
        builder: AmazonS3Builder,
        prefix: impl Into<Path>,
    ) -> Result<Self, StorageError> {
        builder
            .build()
            .map(|store| {
                Self::from_object_store(Arc::new(object_store::prefix::PrefixStore::new(
                    store, prefix,
                )))
            })
            .map_err(storage_error)
    }

    /// Encodes the provider `ETag` and version ID into OTMP's opaque runtime token.
    pub fn object_version(
        e_tag: Option<&str>,
        version: Option<&str>,
    ) -> Result<ObjectVersion, StorageError> {
        let e_tag = e_tag
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned);
        let version = version
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned);
        if e_tag.is_none() && version.is_none() {
            return Err(StorageError::Io(
                "S3 response did not include an ETag or version ID".into(),
            ));
        }
        let encoded = serde_json::to_string(&(e_tag, version))
            .map_err(|error| StorageError::Io(error.to_string()))?;
        Ok(ObjectVersion::from_opaque(format!(
            "{VERSION_PREFIX}{encoded}"
        )))
    }

    /// Decodes an opaque token produced by [`Self::object_version`].
    #[must_use]
    pub fn provider_version(version: &ObjectVersion) -> Option<(Option<String>, Option<String>)> {
        let encoded = version.as_opaque().strip_prefix(VERSION_PREFIX)?;
        serde_json::from_str(encoded).ok()
    }

    fn provider_version_owned(
        version: &ObjectVersion,
    ) -> Result<(Option<String>, Option<String>), StorageError> {
        let encoded = version
            .as_opaque()
            .strip_prefix(VERSION_PREFIX)
            .ok_or_else(|| {
                StorageError::Io("object version did not originate from otmp-s3".into())
            })?;
        serde_json::from_str(encoded).map_err(|error| StorageError::Io(error.to_string()))
    }

    fn path(key: &RelativeUri) -> Path {
        Path::from(key.as_str())
    }

    async fn read_with_meta(&self, key: &RelativeUri) -> Result<StoredObject, StorageError> {
        let result = self
            .inner
            .get(&Self::path(key))
            .await
            .map_err(storage_error)?;
        // Runtime reconciliation treats a successful HEAD read as a writable
        // anchor. Version IDs alone are sufficient only for immutable objects.
        let version = if key.as_str() == HEAD_KEY {
            Self::head_version(result.meta.e_tag.as_deref(), result.meta.version.as_deref())?
        } else {
            Self::object_version(result.meta.e_tag.as_deref(), result.meta.version.as_deref())?
        };
        let bytes = result.bytes().await.map_err(storage_error)?;
        Ok(StoredObject {
            bytes: bytes.to_vec(),
            version,
        })
    }

    async fn current_head_version(&self) -> Option<ObjectVersion> {
        let key: RelativeUri = HEAD_KEY.parse().expect("constant HEAD URI is safe");
        self.read_with_meta(&key)
            .await
            .ok()
            .map(|object| object.version)
    }

    fn head_version(
        e_tag: Option<&str>,
        version: Option<&str>,
    ) -> Result<ObjectVersion, StorageError> {
        if e_tag.is_none_or(|value| value.trim().is_empty()) {
            return Err(StorageError::Unsupported(
                "S3 conditional replacement requires an ETag".into(),
            ));
        }
        Self::object_version(e_tag, version)
    }

    async fn reconcile_head(&self, bytes: &[u8]) -> Result<ObjectVersion, StorageError> {
        let key: RelativeUri = HEAD_KEY.parse().expect("constant HEAD URI is safe");
        let object = self.read_with_meta(&key).await?;
        if object.bytes == bytes {
            let (e_tag, version) = Self::provider_version_owned(&object.version)?;
            Self::head_version(e_tag.as_deref(), version.as_deref())
        } else {
            Err(StorageError::Io(
                "conditional HEAD response lacked a version token and read-back was not authored by this attempt".into(),
            ))
        }
    }

    async fn put_head(&self, mode: PutMode, bytes: &[u8]) -> ConditionalWriteOutcome {
        if bytes.len() as u64 > MAXIMUM_BUFFERED_OBJECT_LENGTH {
            return ConditionalWriteOutcome::Indeterminate {
                source: StorageError::Unsupported(
                    "otmp-s3 HEAD exceeds the single-put limit".into(),
                ),
            };
        }
        let key: RelativeUri = HEAD_KEY.parse().expect("constant HEAD URI is safe");
        let result = self
            .inner
            .put_opts(
                &Self::path(&key),
                bytes.to_vec().into(),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await;
        match result {
            Ok(result) => {
                match Self::head_version(result.e_tag.as_deref(), result.version.as_deref()) {
                    Ok(new_version) => ConditionalWriteOutcome::Applied { new_version },
                    Err(_) => match self.reconcile_head(bytes).await {
                        Ok(new_version) => ConditionalWriteOutcome::Applied { new_version },
                        Err(source) => ConditionalWriteOutcome::Indeterminate { source },
                    },
                }
            }
            Err(error) if is_conflict(&error) => ConditionalWriteOutcome::Conflict {
                current_version: self.current_head_version().await,
            },
            Err(error)
                if error
                    .to_string()
                    .contains("ETag Header missing from response") =>
            {
                match self.reconcile_head(bytes).await {
                    Ok(new_version) => ConditionalWriteOutcome::Applied { new_version },
                    Err(source) => ConditionalWriteOutcome::Indeterminate { source },
                }
            }
            Err(error) => ConditionalWriteOutcome::Indeterminate {
                source: storage_error(error),
            },
        }
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn read(&self, key: &RelativeUri) -> Result<StoredObject, StorageError> {
        self.read_with_meta(key).await
    }

    async fn create_from_reader(
        &self,
        key: &RelativeUri,
        reader: &mut (dyn AsyncRead + Send + Unpin),
        maximum_length: Option<u64>,
    ) -> Result<CreatedObject, StorageError> {
        let declared_length = maximum_length.ok_or_else(|| {
            StorageError::Unsupported("otmp-s3 requires a known object length".into())
        })?;
        if declared_length > MAXIMUM_BUFFERED_OBJECT_LENGTH {
            return Err(StorageError::Unsupported(format!(
                "otmp-s3 only supports single-put objects up to {MAXIMUM_BUFFERED_OBJECT_LENGTH} bytes"
            )));
        }
        let capacity = usize::try_from(declared_length).map_err(|_| {
            StorageError::Unsupported(
                "object length cannot fit this platform's memory model".into(),
            )
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut bounded = reader.take(declared_length.saturating_add(1));
        bounded.read_to_end(&mut bytes).await?;
        if bytes.len() as u64 != declared_length {
            return Err(StorageError::MaximumLengthExceeded);
        }
        let sha256 = Sha256::digest(&bytes);
        match self
            .inner
            .put_opts(
                &Self::path(key),
                bytes.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(result) => {
                let version = match Self::object_version(
                    result.e_tag.as_deref(),
                    result.version.as_deref(),
                ) {
                    Ok(version) => version,
                    Err(_) => self.confirm_readable(key, sha256, declared_length).await?,
                };
                Ok(CreatedObject {
                    version,
                    sha256,
                    length: declared_length,
                })
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                let version = self.confirm_readable(key, sha256, declared_length).await?;
                Ok(CreatedObject {
                    version,
                    sha256,
                    length: declared_length,
                })
            }
            Err(error)
                if error
                    .to_string()
                    .contains("ETag Header missing from response") =>
            {
                let version = self.confirm_readable(key, sha256, declared_length).await?;
                Ok(CreatedObject {
                    version,
                    sha256,
                    length: declared_length,
                })
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn create_head(&self, bytes: &[u8]) -> ConditionalWriteOutcome {
        self.put_head(PutMode::Create, bytes).await
    }

    async fn replace_head(
        &self,
        expected: &ObjectVersion,
        bytes: &[u8],
    ) -> ConditionalWriteOutcome {
        let (e_tag, version) = match Self::provider_version_owned(expected) {
            Ok(version) => version,
            Err(source) => return ConditionalWriteOutcome::Indeterminate { source },
        };
        if e_tag.as_deref().is_none_or(|value| value.trim().is_empty()) {
            return ConditionalWriteOutcome::Indeterminate {
                source: StorageError::Unsupported(
                    "S3 conditional replacement requires an ETag".into(),
                ),
            };
        }
        self.put_head(PutMode::Update(UpdateVersion { e_tag, version }), bytes)
            .await
    }

    async fn delete_if_version(
        &self,
        _key: &RelativeUri,
        _version: &ObjectVersion,
    ) -> Result<bool, StorageError> {
        Err(StorageError::Unsupported(
            "otmp-s3 does not emulate conditional deletion with GET then DELETE".into(),
        ))
    }
}

fn is_conflict(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }
    )
}

fn storage_error(error: object_store::Error) -> StorageError {
    match error {
        object_store::Error::NotFound { path, .. } => StorageError::NotFound(path),
        object_store::Error::AlreadyExists { path, .. } => StorageError::ImmutableConflict(path),
        object_store::Error::NotSupported { source } => {
            StorageError::Unsupported(source.to_string())
        }
        object_store::Error::NotImplemented {
            operation,
            implementer,
        } => StorageError::Unsupported(format!("{implementer}: {operation}")),
        error => StorageError::Io(error.to_string()),
    }
}
