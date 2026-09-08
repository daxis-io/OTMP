//! Provider-scoped Parquet footer caching with active-entry leases.

use crate::store::{FooterIdentity, ReadOnlyStore};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, ParquetFileReaderFactory,
};
use datafusion::parquet::arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader};
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures_util::{FutureExt, future::BoxFuture};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

pub const DEFAULT_FOOTER_CACHE_BYTES: usize = 64 * 1024 * 1024;

struct Budget {
    used: AtomicUsize,
    peak: AtomicUsize,
    limit: usize,
}
struct Reservation {
    budget: Arc<Budget>,
    amount: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.amount, Ordering::AcqRel);
    }
}
impl Reservation {
    fn shrink(&mut self, amount: usize) {
        self.amount -= amount;
        self.budget.used.fetch_sub(amount, Ordering::AcqRel);
    }
}

/// A lease retains its reservation even after FIFO eviction drops the cache map reference.
pub struct FooterEntry {
    pub metadata: Arc<ParquetMetaData>,
    tail: bytes::Bytes,
    _reservation: Reservation,
}
struct Entries {
    values: BTreeMap<FooterIdentity, Arc<FooterEntry>>,
    order: VecDeque<FooterIdentity>,
    identities: BTreeMap<String, FooterIdentity>,
}

pub struct FooterCache {
    budget: Arc<Budget>,
    entries: Mutex<Entries>,
    hits: std::sync::atomic::AtomicU64,
}
impl fmt::Debug for FooterCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FooterCache")
            .field("limit", &self.budget.limit)
            .finish_non_exhaustive()
    }
}
impl FooterCache {
    pub fn new(limit: usize) -> datafusion::error::Result<Arc<Self>> {
        if limit == 0 {
            return Err(datafusion::error::DataFusionError::ResourcesExhausted(
                "OTMP footer cache budget must be positive".into(),
            ));
        }
        Ok(Arc::new(Self {
            budget: Arc::new(Budget {
                used: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                limit,
            }),
            entries: Mutex::new(Entries {
                values: BTreeMap::new(),
                order: VecDeque::new(),
                identities: BTreeMap::new(),
            }),
            hits: std::sync::atomic::AtomicU64::new(0),
        }))
    }
    pub fn statistics(&self) -> (usize, usize, u64) {
        (
            self.budget.used.load(Ordering::Relaxed),
            self.budget.peak.load(Ordering::Relaxed),
            self.hits.load(Ordering::Relaxed),
        )
    }
    fn check(entries: &Entries, key: &FooterIdentity) -> ParquetResult<()> {
        if entries
            .identities
            .get(&key.uri)
            .is_some_and(|old| old != key)
        {
            return Err(ParquetError::General(
                "conflicting cached immutable object reference".into(),
            ));
        }
        Ok(())
    }
    fn reserve(&self, amount: usize) -> ParquetResult<Reservation> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| ParquetError::General("footer cache lock poisoned".into()))?;
        loop {
            let used = self.budget.used.load(Ordering::Acquire);
            if let Some(next) = used.checked_add(amount).filter(|n| *n <= self.budget.limit) {
                if self
                    .budget
                    .used
                    .compare_exchange(used, next, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.budget.peak.fetch_max(next, Ordering::Relaxed);
                    return Ok(Reservation {
                        budget: self.budget.clone(),
                        amount,
                    });
                }
                continue;
            }
            let Some(key) = entries.order.pop_front() else {
                return Err(ParquetError::General(format!(
                    "OTMP footer cache exhausted: {amount} requested, {used} active, {} limit",
                    self.budget.limit
                )));
            };
            entries.values.remove(&key);
            if !entries.values.keys().any(|other| other.uri == key.uri) {
                entries.identities.remove(&key.uri);
            }
        }
    }
    fn get(&self, key: &FooterIdentity) -> ParquetResult<Option<Arc<FooterEntry>>> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| ParquetError::General("footer cache lock poisoned".into()))?;
        Self::check(&entries, key)?;
        let found = entries.values.get(key).cloned();
        if found.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
        Ok(found)
    }
    fn insert(
        &self,
        key: FooterIdentity,
        metadata: Arc<ParquetMetaData>,
        tail: bytes::Bytes,
        reservation: Reservation,
    ) -> ParquetResult<Arc<FooterEntry>> {
        if metadata.memory_size().saturating_add(tail.len()) > reservation.amount {
            return Err(ParquetError::General(
                "decoded footer exceeded its bounded reservation".into(),
            ));
        }
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| ParquetError::General("footer cache lock poisoned".into()))?;
        Self::check(&entries, &key)?;
        if let Some(entry) = entries.values.get(&key) {
            return Ok(entry.clone());
        }
        let entry = Arc::new(FooterEntry {
            metadata,
            tail,
            _reservation: reservation,
        });
        entries.identities.insert(key.uri.clone(), key.clone());
        entries.order.push_back(key.clone());
        entries.values.insert(key, entry.clone());
        Ok(entry)
    }
}

pub struct FooterReaderFactory<S> {
    bridge: Arc<ReadOnlyStore<S>>,
    inner: DefaultParquetFileReaderFactory,
    cache: Arc<FooterCache>,
}
impl<S> fmt::Debug for FooterReaderFactory<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FooterReaderFactory").finish()
    }
}
impl<S: otmp::ObjectStore + fmt::Debug> FooterReaderFactory<S> {
    pub fn new(bridge: Arc<ReadOnlyStore<S>>, cache: Arc<FooterCache>) -> Self {
        Self {
            inner: DefaultParquetFileReaderFactory::new(bridge.clone()),
            bridge,
            cache,
        }
    }
}
impl<S: otmp::ObjectStore + fmt::Debug> ParquetFileReaderFactory for FooterReaderFactory<S> {
    fn create_reader(
        &self,
        p: usize,
        file: PartitionedFile,
        hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::common::Result<Box<dyn AsyncFileReader + Send>> {
        let key = self.bridge.footer_identity(&file.object_meta.location)?;
        Ok(Box::new(CachedReader {
            inner: self.inner.create_reader(p, file, hint, metrics)?,
            key,
            cache: self.cache.clone(),
            lease: None,
            option_lease: None,
            counters: self.bridge.counters(),
            opened: false,
        }))
    }
}
struct CachedReader {
    inner: Box<dyn AsyncFileReader + Send>,
    key: FooterIdentity,
    cache: Arc<FooterCache>,
    lease: Option<Arc<FooterEntry>>,
    option_lease: Option<Arc<FooterEntry>>,
    counters: Arc<crate::store::ReadCounters>,
    opened: bool,
}
struct ValidatedTail<'a> {
    inner: &'a mut Box<dyn AsyncFileReader + Send>,
    bytes: bytes::Bytes,
    start: u64,
    length: u64,
}

impl datafusion::parquet::arrow::async_reader::MetadataFetch for ValidatedTail<'_> {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<bytes::Bytes>> {
        async move {
            if range.start > range.end || range.end > self.length {
                return Err(ParquetError::General(
                    "metadata range exceeds pinned object".into(),
                ));
            }
            if range.start >= self.start {
                let start = usize::try_from(range.start - self.start)
                    .map_err(|_| ParquetError::General("footer offset overflow".into()))?;
                let end = usize::try_from(range.end - self.start)
                    .map_err(|_| ParquetError::General("footer offset overflow".into()))?;
                return Ok(self.bytes.slice(start..end));
            }
            self.inner.get_bytes(range).await
        }
        .boxed()
    }
}

impl CachedReader {
    fn reserve_footer(&self, footer_len: usize) -> ParquetResult<Reservation> {
        let estimate = footer_len
            .checked_mul(128)
            .and_then(|n| n.checked_add(64 * 1024))
            .ok_or_else(|| ParquetError::General("Parquet footer reservation overflow".into()))?;
        self.cache.reserve(estimate)
    }

    async fn read_tail(&mut self) -> ParquetResult<(bytes::Bytes, Reservation)> {
        // Non-default options can reuse authenticated serialized bytes, but
        // they must decode their own metadata with the requested policies.
        if let Some(entry) = self.cache.get(&self.key)?
            && entry.tail.len() >= 8
        {
            let reservation = self.reserve_footer(entry.tail.len() - 8)?;
            return Ok((entry.tail.clone(), reservation));
        }
        // Parquet's trailer declares the serialized footer length. Validate it
        // before decoding, reserve a conservative decoded bound, then reduce
        // the active lease to the actual retained metadata footprint.
        if self.key.length < 8 {
            return Err(ParquetError::General(
                "Parquet object is shorter than its trailer".into(),
            ));
        }
        let trailer = self
            .inner
            .get_bytes((self.key.length - 8)..self.key.length)
            .await?;
        if trailer.len() != 8 || &trailer[4..] != b"PAR1" {
            return Err(ParquetError::General(
                "invalid Parquet footer trailer".into(),
            ));
        }
        let footer_len = u64::from(u32::from_le_bytes(
            trailer[..4].try_into().expect("four bytes"),
        ));
        if footer_len > self.key.length - 8 {
            return Err(ParquetError::General(
                "Parquet footer length exceeds object length".into(),
            ));
        }
        let reservation = self.reserve_footer(
            usize::try_from(footer_len)
                .map_err(|_| ParquetError::General("footer length overflow".into()))?,
        )?;
        let raw = self
            .inner
            .get_bytes((self.key.length - 8 - footer_len)..(self.key.length - 8))
            .await?;
        crate::footer_bounds::validate(&raw)?;
        let mut tail = Vec::with_capacity(raw.len() + 8);
        tail.extend_from_slice(&raw);
        tail.extend_from_slice(&trailer);
        Ok((bytes::Bytes::from(tail), reservation))
    }

    async fn load_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> ParquetResult<Arc<FooterEntry>> {
        let (tail, mut reservation) = self.read_tail().await?;
        // Let Parquet apply every ArrowReaderOptions setting while serving its
        // footer requests from the already validated bytes. Index requests use
        // the same version-pinned inner reader and original object bounds.
        let fetch = ValidatedTail {
            inner: &mut self.inner,
            bytes: tail.clone(),
            start: self.key.length - tail.len() as u64,
            length: self.key.length,
        };
        let metadata = Arc::new(
            datafusion::parquet::file::metadata::ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(fetch, self.key.length)
                .await?,
        );
        let actual = metadata
            .memory_size()
            .checked_add(256 + self.key.uri.len() * 4 + tail.len())
            .ok_or_else(|| ParquetError::General("Parquet footer accounting overflow".into()))?;
        if actual > reservation.amount {
            return Err(ParquetError::General(
                "decoded footer exceeded its bounded reservation".into(),
            ));
        }
        reservation.shrink(reservation.amount - actual);
        if options.is_none() {
            self.cache
                .insert(self.key.clone(), metadata, tail, reservation)
        } else {
            Ok(Arc::new(FooterEntry {
                metadata,
                tail,
                _reservation: reservation,
            }))
        }
    }
    fn mark_opened(&mut self) {
        if !self.opened {
            self.opened = true;
            self.counters.files_opened.fetch_add(1, Ordering::Relaxed);
        }
    }
}
impl AsyncFileReader for CachedReader {
    fn get_bytes(&mut self, r: Range<u64>) -> BoxFuture<'_, ParquetResult<bytes::Bytes>> {
        self.mark_opened();
        self.inner.get_bytes(r)
    }
    fn get_byte_ranges(
        &mut self,
        r: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<bytes::Bytes>>> {
        if !r.is_empty() {
            self.mark_opened();
        }
        self.inner.get_byte_ranges(r)
    }
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            if options.is_some() {
                let entry = self.load_metadata(options).await?;
                let metadata = entry.metadata.clone();
                self.option_lease = Some(entry);
                return Ok(metadata);
            }
            if self.lease.is_none() {
                self.lease = Some(match self.cache.get(&self.key)? {
                    Some(entry) => entry,
                    None => self.load_metadata(None).await?,
                });
            }
            Ok(self
                .lease
                .as_ref()
                .expect("lease populated")
                .metadata
                .clone())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicted_footer_entries_stay_charged_until_the_reader_releases_its_lease() {
        use datafusion::parquet::file::metadata::FileMetaData;
        use datafusion::parquet::schema::types::{SchemaDescriptor, Type};
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("schema").build().unwrap(),
        )));
        let metadata = Arc::new(ParquetMetaData::new(
            FileMetaData::new(1, 0, None, None, schema, None),
            vec![],
        ));
        let charge = metadata.memory_size() + 1024;
        let cache = FooterCache::new(charge * 3).unwrap();
        let first = identity("first.parquet");
        let held = cache
            .insert(
                first.clone(),
                metadata.clone(),
                bytes::Bytes::new(),
                cache.reserve(charge).unwrap(),
            )
            .unwrap();
        assert!(Arc::ptr_eq(&cache.get(&first).unwrap().unwrap(), &held));
        let second = identity("second.parquet");
        drop(
            cache
                .insert(
                    second,
                    metadata,
                    bytes::Bytes::new(),
                    cache.reserve(charge).unwrap(),
                )
                .unwrap(),
        );
        let transient = cache.reserve(charge * 2).unwrap();
        assert!(
            cache.get(&first).unwrap().is_none(),
            "the FIFO entry was actually evicted"
        );
        assert_eq!(
            cache.statistics().0,
            charge * 3,
            "the active reader still owns the first entry"
        );
        assert!(cache.reserve(1).is_err());
        drop(transient);
        let budget = cache.budget.clone();
        drop(cache);
        assert_eq!(budget.used.load(Ordering::Acquire), charge);
        drop(held);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }

    async fn real_reader() -> (
        Box<dyn AsyncFileReader + Send>,
        Arc<crate::store::ReadCounters>,
    ) {
        use datafusion::arrow::{
            array::Int64Array,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use datafusion::parquet::arrow::ArrowWriter;
        use otmp::ObjectStore as _;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let store = otmp::InMemoryObjectStore::default();
        let uri: otmp_protocol::RelativeUri = "data/one.parquet".parse().unwrap();
        let created = store.create_bytes(&uri, &bytes).await.unwrap();
        let bridge = Arc::new(
            ReadOnlyStore::new(store, [(uri, Some(created.sha256), created.length)])
                .await
                .unwrap(),
        );
        let counters = bridge.counters();
        let factory = FooterReaderFactory::new(
            bridge,
            FooterCache::new(DEFAULT_FOOTER_CACHE_BYTES).unwrap(),
        );
        let reader = factory
            .create_reader(
                0,
                PartitionedFile::new("data/one.parquet", created.length),
                None,
                &ExecutionPlanMetricsSet::new(),
            )
            .unwrap();
        (reader, counters)
    }

    #[tokio::test]
    async fn preflight_fetches_the_bounded_parquet_footer_once() {
        let (mut reader, counters) = real_reader().await;
        assert_eq!(
            reader
                .get_metadata(None)
                .await
                .unwrap()
                .file_metadata()
                .num_rows(),
            2
        );
        assert_eq!(
            counters.requests.load(Ordering::Relaxed),
            3,
            "one stat plus trailer and footer, with no redundant decode reads"
        );
        assert_eq!(
            reader
                .get_metadata(None)
                .await
                .unwrap()
                .file_metadata()
                .num_rows(),
            2
        );
        assert_eq!(counters.requests.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn optioned_decoding_reuses_validated_tail_without_extra_reads() {
        let (mut reader, counters) = real_reader().await;
        reader.get_metadata(None).await.unwrap();
        let before = counters.requests.load(Ordering::Relaxed);
        reader
            .get_metadata(Some(&ArrowReaderOptions::new()))
            .await
            .unwrap();
        assert_eq!(
            counters.requests.load(Ordering::Relaxed),
            before,
            "default Arrow options can decode the bounded cached tail without more storage I/O"
        );
    }

    #[tokio::test]
    async fn required_indexes_cannot_reuse_a_default_footer_lease() {
        use datafusion::parquet::file::metadata::PageIndexPolicy;
        let (mut reader, _) = real_reader().await;
        let default = reader.get_metadata(None).await.unwrap();
        assert!(default.offset_index().is_none());
        let options = ArrowReaderOptions::new().with_offset_index_policy(PageIndexPolicy::Required);
        let indexed = reader.get_metadata(Some(&options)).await.unwrap();
        assert!(
            indexed.offset_index().is_some(),
            "a default preflight cache entry must not hide required offset indexes"
        );
        assert!(
            reader
                .get_metadata(None)
                .await
                .unwrap()
                .offset_index()
                .is_none()
        );
    }

    fn identity(uri: &str) -> FooterIdentity {
        FooterIdentity {
            uri: uri.into(),
            sha256: None,
            length: 32,
            version: "v1".into(),
        }
    }

    #[test]
    fn active_reservation_survives_fifo_eviction() {
        let cache = FooterCache::new(100).unwrap();
        let active = cache.reserve(75).unwrap();
        // There is no map entry to evict, so an active lease must prevent a
        // second allocation from silently exceeding the configured budget.
        assert!(cache.reserve(26).is_err());
        drop(active);
        assert!(cache.reserve(100).is_ok());
    }

    #[test]
    fn conflicts_are_rejected_even_on_cache_hit_lookup() {
        let cache = FooterCache::new(1024).unwrap();
        let first = identity("file.parquet");
        let mut entries = cache.entries.lock().unwrap();
        entries.identities.insert(first.uri.clone(), first.clone());
        drop(entries);
        assert!(cache.get(&first).unwrap().is_none());
        let conflicting = FooterIdentity {
            version: "v2".into(),
            ..first
        };
        assert!(cache.get(&conflicting).is_err());
    }

    #[test]
    fn reservation_releases_after_last_lease_is_dropped() {
        let cache = FooterCache::new(64).unwrap();
        let reservation = cache.reserve(64).unwrap();
        assert_eq!(cache.budget.used.load(Ordering::Acquire), 64);
        drop(reservation);
        assert_eq!(cache.budget.used.load(Ordering::Acquire), 0);
        assert!(cache.reserve(64).is_ok());
    }

    #[test]
    fn oversized_predecode_reservation_is_refused() {
        let cache = FooterCache::new(64).unwrap();
        // This is the same admission failure a declared footer bound produces
        // before any footer bytes are decoded.
        assert!(cache.reserve(65).is_err());
        assert_eq!(cache.budget.used.load(Ordering::Acquire), 0);
    }
}
