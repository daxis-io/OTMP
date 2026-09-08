//! Read-only `ObjectStore` 0.13 bridge over OTMP's version-pinned range API.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, BoxStream, StreamExt};
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore as DataFusionObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result as StoreResult, path::Path,
};
use otmp::{ObjectMetadata, ObjectStore as OtmpObjectStore, RuntimeError};
use otmp_protocol::RelativeUri;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::BTreeMap, fmt, sync::Arc};

const MAXIMUM_RANGE: u64 = otmp::storage::MAXIMUM_RANGE_LENGTH;

#[derive(Clone, Debug)]
pub struct ImmutableObject {
    pub uri: RelativeUri,
    /// Full user-file verification is exhaustive; normal scans use the
    /// immutable object version and may not have a digest recorded.
    pub sha256: Option<otmp_protocol::Sha256>,
    pub length: u64,
    pub version: otmp::ObjectVersion,
}

/// Immutable coordinates used to validate footer-cache entries.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FooterIdentity {
    pub uri: String,
    pub sha256: Option<otmp_protocol::Sha256>,
    pub length: u64,
    pub version: String,
}

#[derive(Clone)]
pub struct ReadOnlyStore<S> {
    store: S,
    objects: Arc<BTreeMap<String, ImmutableObject>>,
    counters: Arc<ReadCounters>,
}

#[derive(Debug, Default)]
pub(crate) struct ReadCounters {
    pub bytes: AtomicU64,
    pub requests: AtomicU64,
    pub files_opened: AtomicU64,
}

impl<S: fmt::Debug> fmt::Debug for ReadOnlyStore<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadOnlyStore")
            .field("objects", &self.objects.len())
            .finish_non_exhaustive()
    }
}
impl<S> fmt::Display for ReadOnlyStore<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("otmp-read-only")
    }
}

fn unsupported(operation: &str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: operation.into(),
        implementer: "otmp-read-only".into(),
    }
}
fn bridge(error: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "otmp",
        source: Box::new(error),
    }
}

impl<S: OtmpObjectStore> ReadOnlyStore<S> {
    pub async fn new(
        store: S,
        descriptors: impl IntoIterator<Item = (RelativeUri, Option<otmp_protocol::Sha256>, u64)>,
    ) -> Result<Self, RuntimeError> {
        Self::with_counters(store, descriptors, Arc::new(ReadCounters::default())).await
    }

    pub(crate) async fn with_counters(
        store: S,
        descriptors: impl IntoIterator<Item = (RelativeUri, Option<otmp_protocol::Sha256>, u64)>,
        counters: Arc<ReadCounters>,
    ) -> Result<Self, RuntimeError> {
        let mut objects: BTreeMap<String, ImmutableObject> = BTreeMap::new();
        for (uri, sha256, length) in descriptors {
            if let Some(previous) = objects.get(uri.as_str()) {
                if previous.sha256 != sha256 || previous.length != length {
                    return Err(RuntimeError::Corrupt(
                        "conflicting immutable data descriptors use the same URI".into(),
                    ));
                }
                // The immutable descriptor was already version-pinned by the
                // first occurrence. Avoid a second stat that could observe a
                // later version while constructing this one scan.
                continue;
            }
            counters.requests.fetch_add(1, Ordering::Relaxed);
            let metadata = store.stat(&uri).await?;
            if metadata.length != length {
                return Err(RuntimeError::Corrupt(
                    "immutable data descriptor length changed before bridge creation".into(),
                ));
            }
            objects.insert(
                uri.to_string(),
                ImmutableObject {
                    uri,
                    sha256,
                    length,
                    version: metadata.version,
                },
            );
        }
        Ok(Self {
            store,
            objects: Arc::new(objects),
            counters,
        })
    }
    pub(crate) fn counters(&self) -> Arc<ReadCounters> {
        self.counters.clone()
    }
    fn object(&self, path: &Path) -> StoreResult<&ImmutableObject> {
        self.objects
            .get(&path.to_string())
            .ok_or_else(|| object_store::Error::NotFound {
                path: path.to_string(),
                source: "object is not an OTMP-selected immutable file".into(),
            })
    }

    pub fn footer_identity(&self, path: &Path) -> StoreResult<FooterIdentity> {
        let object = self.object(path)?;
        Ok(FooterIdentity {
            uri: object.uri.to_string(),
            sha256: object.sha256,
            length: object.length,
            version: object.version.as_opaque().to_owned(),
        })
    }
    fn meta(object: &ImmutableObject) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(object.uri.as_str()),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size: object.length,
            e_tag: Some(object.version.as_opaque().into()),
            version: Some(object.version.as_opaque().into()),
        }
    }
}

#[async_trait]
impl<S: OtmpObjectStore + fmt::Debug> DataFusionObjectStore for ReadOnlyStore<S> {
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> StoreResult<PutResult> {
        Err(unsupported("put_opts"))
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        Err(unsupported("put_multipart_opts"))
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let object = self.object(location)?;
        let meta = Self::meta(object);
        options.check_preconditions(&meta)?;
        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::empty().boxed()),
                meta,
                range: 0..0,
                attributes: Attributes::new(),
            });
        }
        let range = options
            .range
            .as_ref()
            .map(|range| range.as_range(object.length))
            .transpose()
            .map_err(bridge)?
            .unwrap_or(0..object.length);
        if range.start >= range.end {
            return Err(bridge(std::io::Error::other("range is empty")));
        }
        let expected = ObjectMetadata {
            length: object.length,
            version: object.version.clone(),
        };
        // Parquet's object-store reader may request an entire object. Keep
        // OTMP's range ceiling while producing each validated chunk as it is
        // read, rather than collecting the full object before the consumer can
        // make progress or apply backpressure.
        let store = self.store.clone();
        let uri = object.uri.clone();
        let counters = self.counters.clone();
        let payload = stream::try_unfold(
            (range.start, store, uri, expected, range.end, counters),
            |(start, store, uri, expected, end, counters)| async move {
                if start == end {
                    return Ok(None);
                }
                let next = start.saturating_add(MAXIMUM_RANGE).min(end);
                let part = start..next;
                counters.requests.fetch_add(1, Ordering::Relaxed);
                let response = store
                    .read_range(&uri, part.clone(), &expected)
                    .await
                    .map_err(bridge)?;
                response.validate(&expected, &part).map_err(bridge)?;
                counters
                    .bytes
                    .fetch_add(response.bytes.len() as u64, Ordering::Relaxed);
                Ok(Some((
                    Bytes::from(response.bytes),
                    (next, store, uri, expected, end, counters),
                )))
            },
        )
        .boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes: Attributes::new(),
        })
    }
    fn delete_stream(
        &self,
        _: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        stream::once(async { Err(unsupported("delete_stream")) }).boxed()
    }
    fn list(&self, _: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        stream::once(async { Err(unsupported("list")) }).boxed()
    }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> StoreResult<ListResult> {
        Err(unsupported("list_with_delimiter"))
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> StoreResult<()> {
        Err(unsupported("copy_opts"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt as _;

    #[tokio::test]
    async fn serves_only_selected_version_pinned_ranges() {
        let store = otmp::InMemoryObjectStore::default();
        let uri: RelativeUri = "data/file.parquet".parse().unwrap();
        let created = store.create_bytes(&uri, b"abcdef").await.unwrap();
        let bridge =
            ReadOnlyStore::new(store, [(uri.clone(), Some(created.sha256), created.length)])
                .await
                .unwrap();
        let bytes = bridge
            .get_range(&Path::from(uri.as_str()), 1..4)
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"bcd"));
        assert!(bridge.get_range(&Path::from("other"), 0..1).await.is_err());
    }

    #[tokio::test]
    async fn rejects_listing_and_writes() {
        let bridge = ReadOnlyStore::new(otmp::InMemoryObjectStore::default(), [])
            .await
            .unwrap();
        assert!(bridge.list_with_delimiter(None).await.is_err());
        assert!(
            bridge
                .put_opts(
                    &Path::from("x"),
                    PutPayload::from("x"),
                    PutOptions::default()
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_conflicting_descriptors_for_one_uri() {
        let store = otmp::InMemoryObjectStore::default();
        let uri: RelativeUri = "data/file.parquet".parse().unwrap();
        let created = store.create_bytes(&uri, b"abcdef").await.unwrap();
        let conflicting = otmp_protocol::Sha256::digest(b"uvwxyz");
        let error = ReadOnlyStore::new(
            store,
            [
                (uri.clone(), Some(created.sha256), created.length),
                (uri, Some(conflicting), created.length),
            ],
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), "OTMP_CORRUPT_TABLE");
    }
}
