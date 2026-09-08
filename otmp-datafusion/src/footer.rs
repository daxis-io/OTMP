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
        reservation: Reservation,
    ) -> ParquetResult<Arc<FooterEntry>> {
        if metadata.memory_size() > reservation.amount {
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
    counters: Arc<crate::store::ReadCounters>,
    opened: bool,
}
impl CachedReader {
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
            if self.lease.is_none() {
                self.lease = if let Some(entry) = self.cache.get(&self.key)? {
                    Some(entry)
                } else {
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
                    let estimate = usize::try_from(footer_len)
                        .ok()
                        .and_then(|n| n.checked_mul(128))
                        .and_then(|n| n.checked_add(64 * 1024))
                        .ok_or_else(|| {
                            ParquetError::General("Parquet footer reservation overflow".into())
                        })?;
                    let mut reservation = self.cache.reserve(estimate)?;
                    let raw = self
                        .inner
                        .get_bytes((self.key.length - 8 - footer_len)..(self.key.length - 8))
                        .await?;
                    crate::footer_bounds::validate(&raw)?;
                    let metadata = self.inner.get_metadata(options).await?;
                    let actual = metadata
                        .memory_size()
                        .checked_add(256 + self.key.uri.len() * 4)
                        .ok_or_else(|| {
                            ParquetError::General("Parquet footer accounting overflow".into())
                        })?;
                    if actual > reservation.amount {
                        return Err(ParquetError::General(
                            "decoded footer exceeded its bounded reservation".into(),
                        ));
                    }
                    reservation.shrink(reservation.amount - actual);
                    Some(self.cache.insert(self.key.clone(), metadata, reservation)?)
                };
            }
            Ok(self
                .lease
                .as_ref()
                .expect("lease assigned")
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
                cache.reserve(charge).unwrap(),
            )
            .unwrap();
        assert!(Arc::ptr_eq(&cache.get(&first).unwrap().unwrap(), &held));
        let second = identity("second.parquet");
        drop(
            cache
                .insert(second, metadata, cache.reserve(charge).unwrap())
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
