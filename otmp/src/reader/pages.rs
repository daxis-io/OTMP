use super::cache::{Cache, CachedBytes, Identity, PageKey};
use super::{ReaderOptions, ReaderStatistics};
use crate::storage::ObjectMetadata;
use crate::{ObjectStore, RuntimeError};
use futures_util::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use otmp_protocol::{
    CheckpointIndexNode, Generation, ObjectReference, PageCodec, PageMapEntry, PageMapNode,
    PageObjectReference, Sha256, decode_checkpoint_index, decode_pack_header,
    decode_pack_index_parts, decode_page_map, image_root_hash,
};
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

type RangeFuture = Shared<BoxFuture<'static, Result<Arc<CachedBytes>, Arc<RuntimeError>>>>;
type RangeKey = (String, Sha256, u64, String, u64, u64);
type RangeLoads = Arc<Mutex<BTreeMap<RangeKey, Weak<RangeLoad>>>>;
struct RangeLoad {
    future: RangeFuture,
    id: u64,
    key: RangeKey,
    registry: RangeLoads,
    _reservation: super::cache::Reservation,
}
impl Drop for RangeLoad {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().unwrap();
        if registry
            .get(&self.key)
            .is_some_and(|weak| std::ptr::eq(weak.as_ptr(), self))
        {
            registry.remove(&self.key);
        }
    }
}

static NEXT_RANGE_LOAD: AtomicU64 = AtomicU64::new(1);
struct Context<S> {
    store: S,
    options: ReaderOptions,
    cache: Cache,
    loads: RangeLoads,
    inflight: tokio::sync::Semaphore,
    bytes: AtomicU64,
    requests: AtomicU64,
    pages: AtomicU64,
    hits: AtomicU64,
}

/// Clones share only the immutable objects from this fixed storage instance.
#[derive(Clone)]
pub(crate) struct ReadContext<S> {
    inner: Arc<Context<S>>,
}

pub(crate) struct WeakReadContext<S> {
    inner: std::sync::Weak<Context<S>>,
}

impl<S> WeakReadContext<S> {
    pub(crate) fn upgrade(&self) -> Option<ReadContext<S>> {
        self.inner.upgrade().map(|inner| ReadContext { inner })
    }
}

fn corrupt(message: &str) -> RuntimeError {
    RuntimeError::Corrupt(message.into())
}

impl<S: ObjectStore> ReadContext<S> {
    pub(crate) fn downgrade(&self) -> WeakReadContext<S> {
        WeakReadContext {
            inner: Arc::downgrade(&self.inner),
        }
    }
    pub(crate) fn new(store: S, options: ReaderOptions) -> Result<Self, RuntimeError> {
        options.validate()?;
        Ok(Self {
            inner: Arc::new(Context {
                store,
                cache: Cache::new(options.cache_budget_bytes),
                loads: Arc::default(),
                inflight: tokio::sync::Semaphore::new(options.max_inflight_reads),
                options,
                bytes: AtomicU64::new(0),
                requests: AtomicU64::new(0),
                pages: AtomicU64::new(0),
                hits: AtomicU64::new(0),
            }),
        })
    }
    pub(crate) fn options(&self) -> &ReaderOptions {
        &self.inner.options
    }
    pub(crate) fn store(&self) -> &S {
        &self.inner.store
    }
    pub(crate) fn reserve_bytes(
        &self,
        bytes: usize,
    ) -> Result<super::cache::Reservation, RuntimeError> {
        self.inner.cache.reserve(bytes)
    }
    pub(crate) fn statistics(&self) -> ReaderStatistics {
        ReaderStatistics {
            bytes: self.inner.bytes.load(Ordering::Relaxed),
            requests: self.inner.requests.load(Ordering::Relaxed),
            pages: self.inner.pages.load(Ordering::Relaxed),
            cache_hits: self.inner.hits.load(Ordering::Relaxed),
            cache_bytes: self.inner.cache.used(),
            peak_cache_bytes: self.inner.cache.peak(),
        }
    }
    #[tracing::instrument(name = "otmp.load", skip_all, fields(kind = "metadata_stat", uri = uri.as_str()))]
    pub(crate) async fn stat(
        &self,
        uri: &otmp_protocol::RelativeUri,
    ) -> Result<ObjectMetadata, RuntimeError> {
        let mut trace = RangeTrace::new();
        let _permit = self
            .inner
            .inflight
            .acquire()
            .await
            .map_err(|_| RuntimeError::Cancelled)?;
        self.inner.requests.fetch_add(1, Ordering::Relaxed);
        trace.requests = 1;
        let result = self.inner.store.stat(uri).await;
        trace.outcome = if result.is_ok() { "success" } else { "error" };
        Ok(result?)
    }
    /// Mutable HEAD bypasses the immutable cache but still uses exact bounded reads.
    pub(crate) async fn mutable_range(
        &self,
        uri: &otmp_protocol::RelativeUri,
        metadata: &ObjectMetadata,
    ) -> Result<Vec<u8>, RuntimeError> {
        let _permit = self
            .inner
            .inflight
            .acquire()
            .await
            .map_err(|_| RuntimeError::Cancelled)?;
        self.inner.requests.fetch_add(1, Ordering::Relaxed);
        let range = 0..metadata.length;
        let response = self
            .inner
            .store
            .read_range(uri, range.clone(), metadata)
            .await?;
        response.validate(metadata, &range)?;
        self.inner
            .bytes
            .fetch_add(response.bytes.len() as u64, Ordering::Relaxed);
        Ok(response.bytes)
    }
    async fn range(
        &self,
        reference: &PageObjectReference,
        metadata: &ObjectMetadata,
        range: Range<u64>,
    ) -> Result<Arc<CachedBytes>, RuntimeError> {
        if reference.length.0 != metadata.length {
            return Err(corrupt(
                "object length differs from authenticated reference",
            ));
        }
        crate::storage::validate_range_request(metadata, &range)?;
        let identity = Identity {
            uri: reference.uri.to_string(),
            hash: reference.sha256,
            length: reference.length.0,
            version: Some(metadata.version.clone()),
        };
        if let Some(bytes) = self.inner.cache.get(&identity, &range)? {
            self.inner.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }
        let key = (
            identity.uri.clone(),
            identity.hash,
            identity.length,
            metadata.version.as_opaque().to_owned(),
            range.start,
            range.end,
        );
        let load = {
            let mut loads = self.inner.loads.lock().unwrap();
            if loads
                .keys()
                .any(|old| old.0 == key.0 && (old.1 != key.1 || old.2 != key.2 || old.3 != key.3))
            {
                return Err(corrupt("conflicting in-flight object reference"));
            }
            if let Some(load) = loads.get(&key).and_then(Weak::upgrade) {
                load
            } else {
                let reservation = self.inner.cache.reserve(
                    512 + identity.uri.len() * 4 + metadata.version.as_opaque().len() * 2,
                )?;
                let context = self.clone();
                let reference = reference.clone();
                let metadata = metadata.clone();
                let pinned = identity.clone();
                let range = range.clone();
                let load_id = NEXT_RANGE_LOAD.fetch_add(1, Ordering::Relaxed);
                let future = async move {
                    context
                        .fill_range(reference, metadata, pinned, range, load_id)
                        .await
                        .map_err(Arc::new)
                }
                .boxed()
                .shared();
                let load = Arc::new(RangeLoad {
                    future,
                    id: load_id,
                    key: key.clone(),
                    registry: self.inner.loads.clone(),
                    _reservation: reservation,
                });
                loads.insert(key, Arc::downgrade(&load));
                load
            }
        };
        tracing::debug!(target: "otmp.load", load_id = load.id, kind = "metadata_range", "load waiter");
        let result = load.future.clone().await;
        drop(load);
        result.map_err(RuntimeError::from_shared)
    }
    #[tracing::instrument(name = "otmp.load", skip_all, fields(load_id = load_id, kind = "metadata_range", uri = reference.uri.as_str(), start = range.start, end = range.end))]
    async fn fill_range(
        &self,
        reference: PageObjectReference,
        metadata: ObjectMetadata,
        identity: Identity,
        range: Range<u64>,
        load_id: u64,
    ) -> Result<Arc<CachedBytes>, RuntimeError> {
        if let Some(bytes) = self.inner.cache.get(&identity, &range)? {
            self.inner.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }
        let mut trace = RangeTrace::new();
        let result = async {
            let size = usize::try_from(range.end - range.start)
                .map_err(|_| corrupt("range size overflow"))?;
            let allocation = size
                .checked_mul(2)
                .and_then(|n| {
                    n.checked_add(
                        1024 + identity.uri.len() * 4 + metadata.version.as_opaque().len() * 2,
                    )
                })
                .ok_or_else(|| {
                    RuntimeError::ResourceExhausted("range allocation overflow".into())
                })?;
            let reservation = self.inner.cache.reserve(allocation)?;
            let _permit = self
                .inner
                .inflight
                .acquire()
                .await
                .map_err(|_| RuntimeError::Cancelled)?;
            trace.requests = 1;
            self.inner.requests.fetch_add(1, Ordering::Relaxed);
            let result = self
                .inner
                .store
                .read_range(&reference.uri, range.clone(), &metadata)
                .await?;
            result.validate(&metadata, &range)?;
            self.inner
                .bytes
                .fetch_add(result.bytes.len() as u64, Ordering::Relaxed);
            trace.bytes = result.bytes.len();
            self.inner
                .cache
                .insert(identity, range, result.bytes, reservation)
        }
        .await;
        trace.outcome = if result.is_ok() { "success" } else { "error" };
        result
    }
    pub(crate) async fn object(
        &self,
        reference: &ObjectReference,
    ) -> Result<Arc<CachedBytes>, RuntimeError> {
        let metadata = self.stat(&reference.uri).await?;
        if reference.length.is_some_and(|n| n.0 != metadata.length) {
            return Err(corrupt("object reference length mismatch"));
        }
        let reference = PageObjectReference {
            uri: reference.uri.clone(),
            sha256: reference.sha256,
            length: otmp_protocol::JsonU64(metadata.length),
        };
        self.node(&reference, &metadata).await
    }
    async fn node(
        &self,
        reference: &PageObjectReference,
        metadata: &ObjectMetadata,
    ) -> Result<Arc<CachedBytes>, RuntimeError> {
        let bytes = self
            .range(reference, metadata, 0..reference.length.0)
            .await?;
        if Sha256::digest(bytes.as_ref().as_ref()) != reference.sha256 {
            return Err(corrupt("authenticated metadata object hash mismatch"));
        }
        Ok(bytes)
    }
    /// Reuse a retained immutable revision; uncached ranges still carry its
    /// conditional version token and every reference is checked on lookup.
    async fn immutable_metadata(
        &self,
        reference: &PageObjectReference,
    ) -> Result<ObjectMetadata, RuntimeError> {
        if let Some(metadata) = self.inner.cache.metadata_for(reference)? {
            return Ok(metadata);
        }
        self.stat(&reference.uri).await
    }

    async fn tree_node(
        &self,
        reference: &PageObjectReference,
    ) -> Result<Arc<CachedBytes>, RuntimeError> {
        if reference.length.0 == 0 || reference.length.0 > 1024 * 1024 {
            return Err(corrupt("invalid authenticated tree node size"));
        }
        let metadata = self.immutable_metadata(reference).await?;
        self.node(reference, &metadata).await
    }
    pub(crate) async fn image(
        &self,
        generation: &Generation,
    ) -> Result<AuthenticatedImage<S>, RuntimeError> {
        generation.validate_runtime_profile()?;
        let image = &generation.metadata_image;
        let index = image
            .checkpoint_page_index
            .as_ref()
            .ok_or(RuntimeError::AuthenticatedRangesUnavailable)?;
        index.validate(&image.checkpoint, image.page_size)?;
        if image.page_count.0 == 0
            || image.page_count.0 > u64::from(u32::MAX)
            || image.page_map.as_ref().is_some_and(|r| r.height > 64)
            || image_root_hash(
                generation.table_id,
                generation.table_version.0,
                image.page_size,
                image.page_count.0,
                image.checkpoint.sha256,
                image.page_map.as_ref().map(|r| r.sha256),
            ) != image.image_root_sha256
        {
            return Err(corrupt("invalid logical image identity or geometry"));
        }
        if image.page_map.is_none() && image.page_count.0 != index.page_count.0 {
            return Err(corrupt("null map does not cover image"));
        }
        let checkpoint = self.stat(&image.checkpoint.uri).await?;
        if checkpoint.length != image.checkpoint.length.0 {
            return Err(corrupt("checkpoint length mismatch"));
        }
        Ok(AuthenticatedImage {
            context: self.clone(),
            generation: Arc::new(generation.clone()),
            fingerprint: generation_fingerprint(generation, &checkpoint)?,
            checkpoint,
            base_only: false,
        })
    }
}

#[derive(Clone)]
pub(crate) struct AuthenticatedImage<S> {
    context: ReadContext<S>,
    generation: Arc<Generation>,
    checkpoint: ObjectMetadata,
    fingerprint: Sha256,
    base_only: bool,
}

/// Internal cache identity, not a protocol object digest. Streaming the complete
/// deterministic Generation serialization binds every reference and declared
/// length without allocating another copy of its metadata envelope. The pinned
/// checkpoint revision also separates reopened views of a changed storage object.
fn generation_fingerprint(
    generation: &Generation,
    checkpoint: &ObjectMetadata,
) -> Result<Sha256, RuntimeError> {
    use sha2::Digest;
    struct HashWriter(sha2::Sha256);
    impl std::io::Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = HashWriter(sha2::Sha256::new());
    writer.0.update(b"otmp.reader.authenticated-page.v1\0");
    serde_json::to_writer(&mut writer, generation)
        .map_err(|error| corrupt(&format!("generation fingerprint: {error}")))?;
    writer.0.update(b"\0checkpoint-revision\0");
    writer.0.update(checkpoint.version.as_opaque().as_bytes());
    Ok(Sha256::from_bytes(writer.0.finalize().into()))
}

impl<S: ObjectStore> AuthenticatedImage<S> {
    pub(crate) fn reserve_engine_bytes(
        &self,
        bytes: usize,
    ) -> Result<super::cache::Reservation, RuntimeError> {
        self.context.reserve_bytes(bytes)
    }
    pub(crate) fn maximum_record_bytes(&self) -> usize {
        self.context.options().maximum_record_bytes
    }
    pub(crate) fn length(&self) -> u64 {
        if self.base_only {
            self.checkpoint.length
        } else {
            self.generation.metadata_image.page_count.0 * 4096
        }
    }
    pub(crate) fn checkpoint_view(&self) -> Self {
        Self {
            base_only: true,
            ..self.clone()
        }
    }

    async fn mapping(&self, page: u64) -> Result<Option<PageMapEntry>, RuntimeError> {
        if self.base_only {
            return Ok(None);
        }
        let Some(root) = &self.generation.metadata_image.page_map else {
            return Ok(None);
        };
        let (mut reference, mut height, mut lower, mut upper, mut is_root) =
            (root.reference(), root.height, 0, self.length() / 4096, true);
        loop {
            let raw = self.context.tree_node(&reference).await?;
            // Decoded references, strings and entries stay charged while in use.
            let _decoded = self
                .context
                .inner
                .cache
                .reserve(raw.as_ref().as_ref().len().saturating_mul(6) + 4096)?;
            let node = decode_page_map(raw.as_ref().as_ref())?;
            if node.level() != height
                || node
                    .max_page()
                    .is_none_or(|n| n > upper || !is_root && n != upper)
            {
                return Err(corrupt("invalid page-map height or interval"));
            }
            match node {
                PageMapNode::Leaf { entries } => {
                    if entries[0].page_number <= lower {
                        return Err(corrupt("overlapping page-map interval"));
                    }
                    return Ok(entries
                        .binary_search_by_key(&page, |e| e.page_number)
                        .ok()
                        .map(|i| entries[i].clone()));
                }
                PageMapNode::Internal { entries, .. } => {
                    if entries[0].max_page <= lower {
                        return Err(corrupt("overlapping page-map child interval"));
                    }
                    let position = entries.partition_point(|e| e.max_page < page);
                    let Some(child) = entries.get(position) else {
                        return Ok(None);
                    };
                    if position > 0 {
                        lower = entries[position - 1].max_page;
                    }
                    upper = child.max_page;
                    reference = child.child.clone();
                    height -= 1;
                    is_root = false;
                }
            }
        }
    }

    async fn checkpoint_hash(&self, page: u64) -> Result<Sha256, RuntimeError> {
        let index = self
            .generation
            .metadata_image
            .checkpoint_page_index
            .as_ref()
            .ok_or(RuntimeError::AuthenticatedRangesUnavailable)?;
        if page == 0 || page > index.page_count.0 {
            return Err(corrupt("missing extended metadata page"));
        }
        let (mut reference, mut height, mut first, mut count) = (
            index.root.reference(),
            index.root.height,
            1,
            index.page_count.0,
        );
        loop {
            let raw = self.context.tree_node(&reference).await?;
            let _decoded = self
                .context
                .inner
                .cache
                .reserve(raw.as_ref().as_ref().len().saturating_mul(6) + 4096)?;
            let node = decode_checkpoint_index(raw.as_ref().as_ref())?;
            if node.level() != height || node.first_page() != first || node.page_count() != count {
                return Err(corrupt("checkpoint index interval or height mismatch"));
            }
            match node {
                CheckpointIndexNode::Leaf { first_page, hashes } => {
                    let offset = usize::try_from(page - first_page)
                        .map_err(|_| corrupt("checkpoint leaf index exceeds platform size"))?;
                    return hashes
                        .get(offset)
                        .copied()
                        .ok_or_else(|| corrupt("checkpoint hash missing"));
                }
                CheckpointIndexNode::Internal { entries, .. } => {
                    let child = entries
                        .iter()
                        .find(|e| e.first_page <= page && page - e.first_page < e.page_count)
                        .ok_or_else(|| corrupt("checkpoint index gap"))?;
                    reference = child.child.clone();
                    height -= 1;
                    first = child.first_page;
                    count = child.page_count;
                }
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "page-map, pack-index, and checkpoint authentication must remain in one auditable read path"
    )]
    pub(crate) async fn copy_page(&self, page: u64, output: &mut [u8]) -> Result<(), RuntimeError> {
        if page == 0 || page > self.length() / 4096 || output.len() > 4096 {
            return Err(corrupt("metadata page request outside image"));
        }
        let key = PageKey {
            generation: self.fingerprint,
            checkpoint: self.base_only,
            page,
        };
        if let Some(bytes) = self.context.inner.cache.get_page(key)? {
            output.copy_from_slice(&bytes.as_ref().as_ref()[..output.len()]);
            self.context.inner.hits.fetch_add(1, Ordering::Relaxed);
            self.context.inner.pages.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let page_bytes;
        let window;
        let data: &[u8];
        let _page_reservation = self.context.inner.cache.reserve(4096)?;
        if let Some(entry) = self.mapping(page).await? {
            if entry.raw_length != 4096 {
                return Err(corrupt("incorrect mapped page size"));
            }
            let metadata = self.context.immutable_metadata(&entry.pack).await?;
            let header = self.context.range(&entry.pack, &metadata, 0..64).await?;
            let parsed = decode_pack_header(header.as_ref().as_ref(), entry.pack.length.0)?;
            if parsed.page_size != 4096 {
                return Err(corrupt("pack page size mismatch"));
            }
            let end = parsed.index_offset + u64::from(parsed.entry_count) * 64;
            let index_raw = self
                .context
                .range(&entry.pack, &metadata, parsed.index_offset..end)
                .await?;
            let _decoded = self
                .context
                .inner
                .cache
                .reserve(index_raw.as_ref().as_ref().len().saturating_mul(3) + 4096)?;
            let index = decode_pack_index_parts(
                header.as_ref().as_ref(),
                index_raw.as_ref().as_ref(),
                entry.pack.length.0,
            )?;
            let position = index
                .entries
                .binary_search_by_key(&page, |e| e.page_number)
                .map_err(|_| corrupt("mapped page absent from pack index"))?;
            let indexed = &index.entries[position];
            if indexed.offset != entry.offset
                || indexed.stored_length != entry.stored_length
                || indexed.raw_length != entry.raw_length
                || indexed.codec != entry.codec
                || indexed.page_sha256 != entry.page_sha256
            {
                return Err(corrupt("page map and pack index disagree"));
            }
            let stored = self
                .context
                .range(
                    &entry.pack,
                    &metadata,
                    entry.offset..entry.offset + u64::from(entry.stored_length),
                )
                .await?;
            page_bytes = match entry.codec {
                PageCodec::None => stored.as_ref().as_ref().to_vec(),
                PageCodec::Zstd => {
                    let mut decoder = zstd::bulk::Decompressor::new()?;
                    decoder.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(12))?;
                    decoder
                        .decompress(stored.as_ref().as_ref(), 4096)
                        .map_err(|_| corrupt("invalid or oversized compressed metadata page"))?
                }
            };
            if page_bytes.len() != 4096 || Sha256::digest(&page_bytes) != entry.page_sha256 {
                return Err(corrupt("metadata page hash or decoded length mismatch"));
            }
            data = &page_bytes;
        } else {
            let hash = self.checkpoint_hash(page).await?;
            let start = (page - 1) * 4096;
            let size = self.context.inner.options.checkpoint_window_bytes as u64;
            let window_start = start / size * size;
            let window_end = window_start
                .saturating_add(size)
                .min(self.checkpoint.length);
            let cp = &self.generation.metadata_image.checkpoint;
            let reference = PageObjectReference {
                uri: cp.uri.clone(),
                sha256: cp.sha256,
                length: cp.length,
            };
            window = self
                .context
                .range(&reference, &self.checkpoint, window_start..window_end)
                .await?;
            let offset = usize::try_from(start - window_start)
                .map_err(|_| corrupt("checkpoint window offset exceeds platform size"))?;
            data = window
                .as_ref()
                .as_ref()
                .get(offset..offset + 4096)
                .ok_or_else(|| corrupt("short checkpoint window"))?;
            if Sha256::digest(data) != hash {
                return Err(corrupt("checkpoint page hash mismatch"));
            }
        }
        if page == 1
            && (data.get(..16) != Some(b"SQLite format 3\0")
                || data[16..18] != [16, 0]
                || u64::from(u32::from_be_bytes(data[28..32].try_into().unwrap()))
                    != self.length() / 4096)
        {
            return Err(corrupt("logical SQLite header mismatch"));
        }
        // Retain only after all authoritative path and page checks succeeded.
        // This allocation shares the raw-range budget and its eviction policy.
        let reservation = self.context.inner.cache.reserve(4096 + 256)?;
        self.context
            .inner
            .cache
            .insert_page(key, data.to_vec(), reservation)?;
        output.copy_from_slice(&data[..output.len()]);
        self.context.inner.pages.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct RangeTrace {
    started: std::time::Instant,
    requests: u64,
    bytes: usize,
    outcome: &'static str,
}
impl RangeTrace {
    fn new() -> Self {
        Self {
            started: std::time::Instant::now(),
            requests: 0,
            bytes: 0,
            outcome: "cancelled",
        }
    }
}
impl Drop for RangeTrace {
    fn drop(&mut self) {
        tracing::info!(target: "otmp.load", requests = self.requests, bytes = self.bytes,
            elapsed_us = self.started.elapsed().as_micros(), outcome = self.outcome, "physical load summary");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{CreatedObject, StoredObject};
    use crate::{
        ConditionalWriteOutcome, InMemoryObjectStore, InitializeRequest, ObjectMetadata,
        ObjectStore, ObjectVersion, StorageError, StoredRange, Table,
    };
    use async_trait::async_trait;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::{Notify, Semaphore};

    #[derive(Clone)]
    struct BlockingRangeStore {
        inner: InMemoryObjectStore,
        started: Arc<AtomicUsize>,
        notified: Arc<Notify>,
        release: Arc<Semaphore>,
    }

    impl BlockingRangeStore {
        fn new(inner: InMemoryObjectStore) -> Self {
            Self {
                inner,
                started: Arc::new(AtomicUsize::new(0)),
                notified: Arc::new(Notify::new()),
                release: Arc::new(Semaphore::new(0)),
            }
        }

        async fn wait_for_ranges(&self, expected: usize) {
            loop {
                let notified = self.notified.notified();
                if self.started.load(Ordering::Acquire) >= expected {
                    return;
                }
                notified.await;
            }
        }
    }

    #[async_trait]
    impl ObjectStore for BlockingRangeStore {
        async fn read(
            &self,
            key: &otmp_protocol::RelativeUri,
        ) -> Result<StoredObject, StorageError> {
            self.inner.read(key).await
        }

        async fn stat(
            &self,
            key: &otmp_protocol::RelativeUri,
        ) -> Result<ObjectMetadata, StorageError> {
            self.inner.stat(key).await
        }

        async fn read_range(
            &self,
            key: &otmp_protocol::RelativeUri,
            range: std::ops::Range<u64>,
            expected: &ObjectMetadata,
        ) -> Result<StoredRange, StorageError> {
            self.started.fetch_add(1, Ordering::AcqRel);
            self.notified.notify_waiters();
            let _permit = self
                .release
                .acquire()
                .await
                .map_err(|_| StorageError::Injected("blocking range gate closed".into()))?;
            self.inner.read_range(key, range, expected).await
        }

        async fn create_from_reader(
            &self,
            key: &otmp_protocol::RelativeUri,
            reader: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
            maximum_length: Option<u64>,
        ) -> Result<CreatedObject, StorageError> {
            self.inner
                .create_from_reader(key, reader, maximum_length)
                .await
        }

        async fn create_head(&self, bytes: &[u8]) -> ConditionalWriteOutcome {
            self.inner.create_head(bytes).await
        }

        async fn replace_head(
            &self,
            expected: &ObjectVersion,
            bytes: &[u8],
        ) -> ConditionalWriteOutcome {
            self.inner.replace_head(expected, bytes).await
        }

        async fn delete_if_version(
            &self,
            key: &otmp_protocol::RelativeUri,
            version: &ObjectVersion,
        ) -> Result<bool, StorageError> {
            self.inner.delete_if_version(key, version).await
        }
    }

    #[tokio::test]
    async fn abandoned_and_failed_shared_ranges_remove_registry_and_keep_error_policy() {
        let inner = InMemoryObjectStore::default();
        let uri = "_otmp/test".parse().unwrap();
        let created = inner.create_bytes(&uri, b"abcd").await.unwrap();
        let store = BlockingRangeStore::new(inner);
        let context = ReadContext::new(store.clone(), ReaderOptions::default()).unwrap();
        let metadata = context.stat(&uri).await.unwrap();
        let reference = PageObjectReference {
            uri,
            sha256: created.sha256,
            length: otmp_protocol::JsonU64(4),
        };
        let mut first = Box::pin(context.range(&reference, &metadata, 0..4));
        assert!(futures_util::poll!(&mut first).is_pending());
        let mut conflict = reference.clone();
        conflict.sha256 = Sha256::digest(b"other");
        assert!(matches!(
            context.range(&conflict, &metadata, 0..2).await,
            Err(RuntimeError::Corrupt(_))
        ));
        drop(first);
        assert!(context.inner.loads.lock().unwrap().is_empty());
        assert_eq!(context.statistics().cache_bytes, 0);
        store.release.close();
        let error = context
            .range(&reference, &metadata, 0..4)
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error, RuntimeError::Storage(_)),
            "unique causes recover their original variant"
        );
        assert_eq!(error.code(), "OTMP_STORAGE_ERROR");
        assert!(error.retryable());
        assert!(context.inner.loads.lock().unwrap().is_empty());
        assert_eq!(context.statistics().cache_bytes, 0);
        assert_eq!(
            context.inner.inflight.available_permits(),
            ReaderOptions::default().max_inflight_reads
        );
    }

    #[tokio::test]
    async fn identical_range_misses_share_io_and_survive_one_cancelled_waiter() {
        let inner = InMemoryObjectStore::default();
        let uri = "_otmp/test".parse().unwrap();
        let created = inner.create_bytes(&uri, b"abcd").await.unwrap();
        let store = BlockingRangeStore::new(inner);
        let context = ReadContext::new(store.clone(), ReaderOptions::default()).unwrap();
        let metadata = context.stat(&uri).await.unwrap();
        let reference = PageObjectReference {
            uri,
            sha256: created.sha256,
            length: otmp_protocol::JsonU64(4),
        };
        let mut first = Box::pin(context.range(&reference, &metadata, 0..4));
        let mut second = Box::pin(context.range(&reference, &metadata, 0..4));
        // Explicit polling establishes overlap without timing assumptions.
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(first.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(second.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        assert_eq!(store.started.load(Ordering::Acquire), 1);
        drop(first);
        store.release.add_permits(1);
        assert_eq!(second.await.unwrap().as_ref().as_ref(), b"abcd");
        assert_eq!(context.statistics().requests, 2);
    }

    #[tokio::test]
    async fn authenticated_pages_match_the_materialized_oracle() {
        let store = InMemoryObjectStore::default();
        let table = Table::new(store.clone());
        let schema =
            serde_json::from_slice(include_bytes!("../../../conformance/sources/schema.json"))
                .unwrap();
        table
            .initialize(InitializeRequest::new(schema))
            .await
            .unwrap();
        let head: otmp_protocol::Head = otmp_protocol::canonical_json::from_slice_canonical(
            &store
                .read(&"_otmp/HEAD".parse().unwrap())
                .await
                .unwrap()
                .bytes,
        )
        .unwrap();
        let generation: Generation = otmp_protocol::canonical_json::from_slice_canonical(
            &store
                .read(&head.metadata_generation.uri)
                .await
                .unwrap()
                .bytes,
        )
        .unwrap();
        let oracle = table.resolve_generation(&generation).await.unwrap();
        let context = ReadContext::new(store, ReaderOptions::default()).unwrap();
        let image = context.image(&generation).await.unwrap();
        for number in 1..=generation.metadata_image.page_count.0 {
            let mut page = vec![0; 4096];
            image.copy_page(number, &mut page).await.unwrap();
            let start = (usize::try_from(number).unwrap() - 1) * 4096;
            assert_eq!(page, &oracle.bytes[start..start + 4096]);
        }
        assert!(
            context.statistics().peak_cache_bytes <= ReaderOptions::default().cache_budget_bytes
        );
        let mut absent = vec![0; 4096];
        assert!(
            image
                .copy_page(generation.metadata_image.page_count.0 + 1, &mut absent)
                .await
                .is_err()
        );
    }

    async fn incremental() -> (InMemoryObjectStore, Table<InMemoryObjectStore>, Generation) {
        let store = InMemoryObjectStore::default();
        let table = Table::new(store.clone());
        let schema =
            serde_json::from_slice(include_bytes!("../../../conformance/sources/schema.json"))
                .unwrap();
        table
            .initialize(InitializeRequest::new(schema))
            .await
            .unwrap();
        table
            .transact(&crate::TransactionRequest {
                idempotency_key: "set".into(),
                requirements: vec![crate::Requirement::PropertyIs {
                    key: "key".into(),
                    value: otmp_protocol::CanonicalValue::Null,
                }],
                operations: vec![crate::OperationRequest::SetProperties {
                    operation_id: "set".into(),
                    updates: std::collections::BTreeMap::from([(
                        "key".into(),
                        otmp_protocol::CanonicalValue::String("value".into()),
                    )]),
                    removals: vec![],
                }],
                commit_metadata: crate::CommitMetadata::default(),
            })
            .await
            .unwrap();
        let head: otmp_protocol::Head = otmp_protocol::canonical_json::from_slice_canonical(
            &store
                .read(&"_otmp/HEAD".parse().unwrap())
                .await
                .unwrap()
                .bytes,
        )
        .unwrap();
        let generation = otmp_protocol::canonical_json::from_slice_canonical(
            &store
                .read(&head.metadata_generation.uri)
                .await
                .unwrap()
                .bytes,
        )
        .unwrap();
        (store, table, generation)
    }

    #[tokio::test]
    async fn incremental_pages_match_exhaustive_sqlite_image() {
        let (store, table, generation) = incremental().await;
        let oracle = table.resolve_generation(&generation).await.unwrap();
        let context = ReadContext::new(store, ReaderOptions::default()).unwrap();
        let image = context.image(&generation).await.unwrap();
        for number in 1..=generation.metadata_image.page_count.0 {
            let mut page = [0; 4096];
            image.copy_page(number, &mut page).await.unwrap();
            let start = (usize::try_from(number).unwrap() - 1) * 4096;
            assert_eq!(&page, &oracle.bytes[start..start + 4096]);
        }
    }

    #[tokio::test]
    async fn cached_immutable_nodes_reuse_versions_and_check_every_reference() {
        let (store, _, generation) = incremental().await;
        let context = ReadContext::new(store, ReaderOptions::default()).unwrap();
        let reference = generation
            .metadata_image
            .checkpoint_page_index
            .unwrap()
            .root
            .reference();
        let first = context.tree_node(&reference).await.unwrap();
        let before = context.statistics();
        let again = context.tree_node(&reference).await.unwrap();
        assert_eq!(first.as_ref().as_ref(), again.as_ref().as_ref());
        assert_eq!(
            context.statistics().requests,
            before.requests,
            "an immutable cached node already pins its revision"
        );
        let mut changed = reference.clone();
        changed.length.0 += 1;
        assert!(context.tree_node(&changed).await.is_err());
        changed = reference;
        changed.sha256 = Sha256::digest(b"different");
        assert!(context.tree_node(&changed).await.is_err());
        assert_eq!(
            context.statistics().requests,
            before.requests,
            "conflicting references fail before any re-stat"
        );
    }

    #[tokio::test]
    async fn authenticated_page_reuse_avoids_storage_requests_across_images() {
        let (store, table, generation) = incremental().await;
        let oracle = table.resolve_generation(&generation).await.unwrap();
        let context = ReadContext::new(store, ReaderOptions::default()).unwrap();
        let image = context.image(&generation).await.unwrap();
        let mut output = [0; 4096];
        image.copy_page(1, &mut output).await.unwrap();
        assert_eq!(&output, &oracle.bytes[..4096]);
        let before_reopen = context.statistics();
        let reopened = context.image(&generation).await.unwrap();
        let before = context.statistics();
        assert_eq!(
            before.requests,
            before_reopen.requests + 1,
            "reopening pins the checkpoint revision with one stat"
        );
        assert_eq!(before.bytes, before_reopen.bytes);
        reopened.copy_page(1, &mut output).await.unwrap();
        let after = context.statistics();
        assert_eq!(
            after.requests, before.requests,
            "authenticated page reuse must not repeat remote authentication reads"
        );
        assert_eq!(after.bytes, before.bytes);
        assert_eq!(after.pages, before.pages + 1);
        assert_eq!(&output, &oracle.bytes[..4096]);
        assert!(after.peak_cache_bytes <= ReaderOptions::default().cache_budget_bytes);
    }

    #[tokio::test]
    async fn page_reuse_keeps_checkpoint_and_logical_views_separate() {
        let (store, table, generation) = incremental().await;
        let logical = table.resolve_generation(&generation).await.unwrap();
        let checkpoint = store
            .read(&generation.metadata_image.checkpoint.uri)
            .await
            .unwrap();
        let index = logical
            .bytes
            .chunks_exact(4096)
            .zip(checkpoint.bytes.chunks_exact(4096))
            .position(|(logical, checkpoint)| logical != checkpoint)
            .expect("incremental override");
        let page = index as u64 + 1;
        let offset = index * 4096;
        let context = ReadContext::new(store, ReaderOptions::default()).unwrap();
        let image = context.image(&generation).await.unwrap();
        let mut output = [0; 4096];
        for _ in 0..2 {
            image.copy_page(page, &mut output).await.unwrap();
            assert_eq!(&output, &logical.bytes[offset..offset + 4096]);
            image
                .checkpoint_view()
                .copy_page(page, &mut output)
                .await
                .unwrap();
            assert_eq!(&output, &checkpoint.bytes[offset..offset + 4096]);
        }
    }

    #[tokio::test]
    async fn cached_page_cannot_hide_changed_authenticated_references() {
        let (store, _, generation) = incremental().await;
        let context = ReadContext::new(store, ReaderOptions::default()).unwrap();
        let image = context.image(&generation).await.unwrap();
        let mut output = [0; 4096];
        image.copy_page(1, &mut output).await.unwrap();
        image
            .checkpoint_view()
            .copy_page(1, &mut output)
            .await
            .unwrap();
        for checkpoint in [false, true] {
            let mut changed = generation.clone();
            if checkpoint {
                changed
                    .metadata_image
                    .checkpoint_page_index
                    .as_mut()
                    .unwrap()
                    .root
                    .length
                    .0 += 1;
            } else {
                changed.metadata_image.page_map.as_mut().unwrap().length.0 += 1;
            }
            // The old image-root digest omits these declared lengths.
            assert_eq!(
                changed.metadata_image.image_root_sha256,
                generation.metadata_image.image_root_sha256
            );
            let changed = context.image(&changed).await.unwrap();
            let changed = if checkpoint {
                changed.checkpoint_view()
            } else {
                changed
            };
            assert!(changed.copy_page(1, &mut output).await.is_err());
        }
    }

    #[tokio::test]
    async fn compressed_page_reads_reject_decompression_overruns() {
        use otmp_protocol::{JsonU64, PageMapRoot, encode_page_map, encode_page_pack};
        for oversized in [false, true] {
            let (store, table, mut generation) = incremental().await;
            let raw = table.resolve_generation(&generation).await.unwrap().bytes[..4096].to_vec();
            let mut input = raw.clone();
            if oversized {
                input.extend_from_slice(&[0; 4096]);
            }
            let compressed = zstd::bulk::compress(&input, 1).unwrap();
            let mut pack =
                encode_page_pack(4096, &std::collections::BTreeMap::from([(1, raw.clone())]))
                    .unwrap();
            pack.truncate(128);
            pack[80..84].copy_from_slice(&u32::try_from(compressed.len()).unwrap().to_be_bytes());
            pack[88] = 1;
            pack.extend_from_slice(&compressed);
            let uri = "_otmp/page-packs/zstd-test.pgpk".parse().unwrap();
            let object = store.create_bytes(&uri, &pack).await.unwrap();
            let map = encode_page_map(&PageMapNode::Leaf {
                entries: vec![PageMapEntry {
                    page_number: 1,
                    pack: PageObjectReference {
                        uri,
                        sha256: object.sha256,
                        length: JsonU64(object.length),
                    },
                    offset: 128,
                    stored_length: u32::try_from(compressed.len()).unwrap(),
                    raw_length: 4096,
                    codec: PageCodec::Zstd,
                    page_sha256: Sha256::digest(&raw),
                }],
            })
            .unwrap();
            let uri = "_otmp/page-maps/zstd-test.cbor".parse().unwrap();
            let object = store.create_bytes(&uri, &map).await.unwrap();
            generation.metadata_image.page_map = Some(PageMapRoot {
                uri,
                sha256: object.sha256,
                length: JsonU64(object.length),
                height: 0,
            });
            let image = &mut generation.metadata_image;
            image.image_root_sha256 = image_root_hash(
                generation.table_id,
                generation.table_version.0,
                image.page_size,
                image.page_count.0,
                image.checkpoint.sha256,
                Some(object.sha256),
            );
            let context = ReadContext::new(store, ReaderOptions::default()).unwrap();
            let image = context.image(&generation).await.unwrap();
            let mut output = [0; 4096];
            let result = image.copy_page(1, &mut output).await;
            if oversized {
                assert!(matches!(result, Err(RuntimeError::Corrupt(_))));
            } else {
                result.unwrap();
                assert_eq!(output.as_slice(), raw);
            }
        }
    }

    #[tokio::test]
    async fn corrupt_authenticated_paths_fail_before_returning_a_page() {
        use otmp_protocol::{JsonU64, encode_checkpoint_index, encode_page_map};
        for damage in [
            "index-hash",
            "index-interval",
            "checkpoint-page",
            "map-height",
            "missing-extension",
            "pack-index",
            "pack-payload",
        ] {
            let (store, _, mut generation) = incremental().await;
            let mut page = 1;
            let mut base = false;
            match damage {
                "index-hash" => {
                    generation
                        .metadata_image
                        .checkpoint_page_index
                        .as_mut()
                        .unwrap()
                        .root
                        .sha256 = Sha256::from_bytes([0; 32]);
                    base = true;
                }
                "index-interval" => {
                    let index = generation
                        .metadata_image
                        .checkpoint_page_index
                        .as_mut()
                        .unwrap();
                    let raw = store.read(&index.root.uri).await.unwrap().bytes;
                    let mut node = decode_checkpoint_index(&raw).unwrap();
                    let CheckpointIndexNode::Leaf { first_page, .. } = &mut node else {
                        panic!("small checkpoint leaf")
                    };
                    *first_page = 2;
                    let bytes = encode_checkpoint_index(&node).unwrap();
                    index.root.sha256 = Sha256::digest(&bytes);
                    index.root.length = JsonU64(bytes.len() as u64);
                    store.replace_object_for_test(&index.root.uri, bytes);
                    base = true;
                }
                "checkpoint-page" => {
                    let cp = &generation.metadata_image.checkpoint;
                    let mut bytes = store.read(&cp.uri).await.unwrap().bytes;
                    bytes[100] ^= 1;
                    store.replace_object_for_test(&cp.uri, bytes);
                    base = true;
                }
                "map-height" => {
                    generation.metadata_image.page_map.as_mut().unwrap().height += 1;
                }
                "missing-extension" => {
                    generation.metadata_image.page_count.0 += 1;
                    page = generation.metadata_image.page_count.0;
                }
                "pack-index" | "pack-payload" => {
                    let map = generation.metadata_image.page_map.as_mut().unwrap();
                    let raw = store.read(&map.uri).await.unwrap().bytes;
                    let node = decode_page_map(&raw).unwrap();
                    let PageMapNode::Leaf { entries } = node else {
                        panic!("small map leaf")
                    };
                    let entry = &entries[0];
                    page = entry.page_number;
                    let mut bytes = store.read(&entry.pack.uri).await.unwrap().bytes;
                    if damage == "pack-index" {
                        bytes[64 + 32] ^= 1;
                    } else {
                        bytes[usize::try_from(entry.offset).unwrap()] ^= 1;
                    }
                    store.replace_object_for_test(&entry.pack.uri, bytes);
                    // encode remains usable; corruption is in ranged pack reads.
                    assert!(
                        !encode_page_map(&PageMapNode::Leaf { entries })
                            .unwrap()
                            .is_empty()
                    );
                }
                _ => unreachable!(),
            }
            let image = &mut generation.metadata_image;
            image.image_root_sha256 = image_root_hash(
                generation.table_id,
                generation.table_version.0,
                image.page_size,
                image.page_count.0,
                image.checkpoint.sha256,
                image.page_map.as_ref().map(|m| m.sha256),
            );
            let context = ReadContext::new(store, ReaderOptions::default()).unwrap();
            let image = context.image(&generation).await.unwrap();
            let image = if base { image.checkpoint_view() } else { image };
            let result = image.copy_page(page, &mut [0; 4096]).await;
            assert!(result.is_err(), "accepted {damage}");
            assert!(
                context.statistics().peak_cache_bytes
                    <= ReaderOptions::default().cache_budget_bytes
            );
        }
    }

    #[tokio::test]
    async fn pinned_checkpoint_rejects_a_changed_object_revision() {
        let (store, _, generation) = incremental().await;
        let context = ReadContext::new(store.clone(), ReaderOptions::default()).unwrap();
        let image = context.image(&generation).await.unwrap().checkpoint_view();
        let cp = &generation.metadata_image.checkpoint;
        let bytes = store.read(&cp.uri).await.unwrap().bytes;
        store.replace_object_for_test(&cp.uri, bytes);
        assert!(image.copy_page(1, &mut [0; 4096]).await.is_err());
    }

    #[tokio::test]
    async fn index_free_generations_report_unavailable() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../conformance/tables/incremental");
        let store = crate::LocalObjectStore::new(root).unwrap();
        let head: otmp_protocol::Head = otmp_protocol::canonical_json::from_slice_canonical(
            &store
                .read(&"_otmp/HEAD".parse().unwrap())
                .await
                .unwrap()
                .bytes,
        )
        .unwrap();
        let generation: Generation = otmp_protocol::canonical_json::from_slice_canonical(
            &store
                .read(&head.metadata_generation.uri)
                .await
                .unwrap()
                .bytes,
        )
        .unwrap();
        let context = ReadContext::new(store, ReaderOptions::default()).unwrap();
        assert!(matches!(
            context.image(&generation).await,
            Err(RuntimeError::AuthenticatedRangesUnavailable)
        ));
    }

    #[tokio::test]
    async fn cancelling_saturated_range_reads_releases_permits_and_reservations() {
        let inner = InMemoryObjectStore::default();
        let table = Table::new(inner.clone());
        let schema =
            serde_json::from_slice(include_bytes!("../../../conformance/sources/schema.json"))
                .unwrap();
        table
            .initialize(InitializeRequest::new(schema))
            .await
            .unwrap();
        let head: otmp_protocol::Head = otmp_protocol::canonical_json::from_slice_canonical(
            &inner
                .read(&"_otmp/HEAD".parse().unwrap())
                .await
                .unwrap()
                .bytes,
        )
        .unwrap();
        let generation: Generation = otmp_protocol::canonical_json::from_slice_canonical(
            &inner
                .read(&head.metadata_generation.uri)
                .await
                .unwrap()
                .bytes,
        )
        .unwrap();
        let store = BlockingRangeStore::new(inner);
        let context = ReadContext::new(store.clone(), ReaderOptions::default()).unwrap();
        let image = context.image(&generation).await.unwrap();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let image = image.clone();
            tasks.push(tokio::spawn(async move {
                image.copy_page(1, &mut [0; 4096]).await
            }));
        }
        store.wait_for_ranges(1).await;
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            assert!(task.await.unwrap_err().is_cancelled());
        }
        assert_eq!(context.statistics().cache_bytes, 0);
        assert!(context.inner.loads.lock().unwrap().is_empty());

        let image = image.clone();
        let ninth = tokio::spawn(async move { image.copy_page(1, &mut [0; 4096]).await });
        store.wait_for_ranges(2).await;
        // Page one resolves both an index node and its checkpoint bytes; leave
        // enough permits for the complete ninth request without unblocking any
        // of the aborted tasks.
        store.release.add_permits(8);
        ninth.await.unwrap().unwrap();
        assert!(context.statistics().requests >= 10);
    }
}
