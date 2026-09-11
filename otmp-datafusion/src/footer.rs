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
use futures_util::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    ops::Range,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

pub const DEFAULT_FOOTER_CACHE_BYTES: usize = 64 * 1024 * 1024;

struct Budget {
    used: AtomicUsize,
    peak: AtomicUsize,
    limit: usize,
    transient: AtomicUsize,
    keys: AtomicUsize,
    preflight_hits: AtomicUsize,
    changed: tokio::sync::Notify,
}
struct Reservation {
    budget: Arc<Budget>,
    amount: usize,
    phase: ReservationPhase,
}
enum ReservationPhase {
    Payload,
    Key,
    Preflight,
    Retained,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.amount, Ordering::AcqRel);
        self.finish_transient();
        self.budget.changed.notify_waiters();
    }
}
impl Reservation {
    fn finish_transient(&mut self) {
        match std::mem::replace(&mut self.phase, ReservationPhase::Retained) {
            ReservationPhase::Payload => {
                self.budget.transient.fetch_sub(1, Ordering::AcqRel);
            }
            ReservationPhase::Key => {
                self.budget.keys.fetch_sub(1, Ordering::AcqRel);
            }
            ReservationPhase::Preflight => {
                self.budget.preflight_hits.fetch_sub(1, Ordering::AcqRel);
            }
            ReservationPhase::Retained => return,
        }
        self.budget.changed.notify_waiters();
    }
    fn mark_key(&mut self) {
        debug_assert!(matches!(self.phase, ReservationPhase::Payload));
        self.budget.keys.fetch_add(1, Ordering::AcqRel);
        self.finish_transient();
        self.phase = ReservationPhase::Key;
    }
    fn shrink(&mut self, amount: usize) {
        self.amount -= amount;
        self.budget.used.fetch_sub(amount, Ordering::AcqRel);
        self.budget.changed.notify_waiters();
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
    admission: tokio::sync::Mutex<()>,
    loads: FooterLoads,
    registration: Arc<tokio::sync::Mutex<()>>,
    #[cfg(test)]
    before_decode: Mutex<Option<[Arc<tokio::sync::Notify>; 2]>>,
    #[cfg(test)]
    pressure: tokio::sync::Notify,
}
type FooterFuture = Shared<BoxFuture<'static, Result<Arc<FooterEntry>, Arc<ParquetError>>>>;
type FooterLoads = Arc<Mutex<BTreeMap<FooterIdentity, Weak<FooterLoad>>>>;
struct FooterLoad {
    future: FooterFuture,
    id: u64,
    key: FooterIdentity,
    registry: FooterLoads,
    _reservation: Reservation,
    _registration: Registration,
}
type Registration = Arc<Mutex<Option<tokio::sync::OwnedMutexGuard<()>>>>;
impl Drop for FooterLoad {
    fn drop(&mut self) {
        if let Ok(mut registry) = self.registry.lock()
            && registry
                .get(&self.key)
                .is_some_and(|weak| std::ptr::eq(weak.as_ptr(), self))
        {
            registry.remove(&self.key);
        }
    }
}
#[derive(Debug)]
struct SharedFooterError(Arc<ParquetError>);
impl fmt::Display for SharedFooterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedFooterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
enum Admission {
    Reserved(Reservation),
    Pressure,
    CannotFit,
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
                transient: AtomicUsize::new(0),
                keys: AtomicUsize::new(0),
                preflight_hits: AtomicUsize::new(0),
                changed: tokio::sync::Notify::new(),
            }),
            entries: Mutex::new(Entries {
                values: BTreeMap::new(),
                order: VecDeque::new(),
                identities: BTreeMap::new(),
            }),
            hits: std::sync::atomic::AtomicU64::new(0),
            admission: tokio::sync::Mutex::new(()),
            loads: Arc::default(),
            registration: Arc::default(),
            #[cfg(test)]
            before_decode: Mutex::default(),
            #[cfg(test)]
            pressure: tokio::sync::Notify::new(),
        }))
    }
    pub fn statistics(&self) -> (usize, usize, u64) {
        (
            self.budget.used.load(Ordering::Relaxed),
            self.budget.peak.load(Ordering::Relaxed),
            self.hits.load(Ordering::Relaxed),
        )
    }
    #[cfg(test)]
    pub(crate) fn assert_idle(&self) {
        assert_eq!(self.budget.keys.load(Ordering::Acquire), 0);
        assert_eq!(self.budget.preflight_hits.load(Ordering::Acquire), 0);
        assert_eq!(self.budget.transient.load(Ordering::Acquire), 0);
        assert!(self.loads.lock().unwrap().is_empty());
        assert!(self.admission.try_lock().is_ok());
        assert!(self.registration.try_lock().is_ok());
    }
    #[cfg(test)]
    pub(crate) async fn wait_for_pressure(&self) {
        self.pressure.notified().await;
    }
    #[cfg(test)]
    pub(crate) fn assert_preflight_lease(&self) {
        assert!(
            self.budget.keys.load(Ordering::Acquire)
                + self.budget.preflight_hits.load(Ordering::Acquire)
                > 0
        );
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
    fn try_reserve(
        &self,
        amount: usize,
        owned_keys: Option<usize>,
        owned_hits: usize,
    ) -> ParquetResult<Admission> {
        if amount > self.budget.limit {
            return Ok(Admission::CannotFit);
        }
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
                    self.budget.transient.fetch_add(1, Ordering::AcqRel);
                    return Ok(Admission::Reserved(Reservation {
                        budget: self.budget.clone(),
                        amount,
                        phase: ReservationPhase::Payload,
                    }));
                }
                continue;
            }
            let Some(key) = entries.order.pop_front() else {
                let releasable = self.budget.transient.load(Ordering::Acquire) > 0
                    || self.budget.preflight_hits.load(Ordering::Acquire) > owned_hits
                    || owned_keys.is_some_and(|own| self.budget.keys.load(Ordering::Acquire) > own);
                if self.budget.used.load(Ordering::Acquire) != used {
                    continue;
                }
                return Ok(if releasable {
                    Admission::Pressure
                } else {
                    Admission::CannotFit
                });
            };
            entries.values.remove(&key);
            if !entries.values.keys().any(|other| other.uri == key.uri) {
                entries.identities.remove(&key.uri);
            }
        }
    }
    fn exhausted(&self, amount: usize) -> ParquetError {
        ParquetError::External(Box::new(otmp::RuntimeError::ResourceExhausted(format!(
            "OTMP footer cache exhausted: {amount} requested, {} active, {} limit",
            self.budget.used.load(Ordering::Acquire),
            self.budget.limit
        ))))
    }
    #[cfg(test)]
    fn reserve(&self, amount: usize) -> ParquetResult<Reservation> {
        match self.try_reserve(amount, None, 0)? {
            Admission::Reserved(reservation) => Ok(reservation),
            Admission::Pressure | Admission::CannotFit => Err(self.exhausted(amount)),
        }
    }
    async fn admit(
        &self,
        amount: usize,
        owned_keys: Option<usize>,
        owned_hits: usize,
    ) -> ParquetResult<Reservation> {
        // Tokio's FIFO mutex grants one byte waiter exclusive admission until
        // it fits or fails. It never guards the cache map or active decoders.
        let _turn = self.admission.lock().await;
        loop {
            let changed = self.budget.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.try_reserve(amount, owned_keys, owned_hits)? {
                Admission::Reserved(reservation) => return Ok(reservation),
                Admission::CannotFit => return Err(self.exhausted(amount)),
                Admission::Pressure => {
                    #[cfg(test)]
                    self.pressure.notify_one();
                    changed.await;
                }
            }
        }
    }
    fn get(
        &self,
        key: &FooterIdentity,
        preflight_lease: Option<&mut Option<Reservation>>,
    ) -> ParquetResult<Option<Arc<FooterEntry>>> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| ParquetError::General("footer cache lock poisoned".into()))?;
        Self::check(&entries, key)?;
        let found = entries.values.get(key).cloned();
        if found.is_some() {
            if let Some(lease) = preflight_lease
                && lease.is_none()
            {
                // A cache hit has no FooterLoad key, but its validation lease
                // can still release evicted bytes. Publish that ownership under
                // the cache lock before an admission can observe the lease.
                self.budget.preflight_hits.fetch_add(1, Ordering::AcqRel);
                *lease = Some(Reservation {
                    budget: self.budget.clone(),
                    amount: 0,
                    phase: ReservationPhase::Preflight,
                });
            }
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
        Ok(found)
    }
    fn insert(
        &self,
        key: FooterIdentity,
        metadata: Arc<ParquetMetaData>,
        tail: bytes::Bytes,
        mut reservation: Reservation,
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
        reservation.finish_transient();
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

#[derive(Clone)]
pub struct FooterReaderFactory<S> {
    bridge: Arc<ReadOnlyStore<S>>,
    inner: Arc<DefaultParquetFileReaderFactory>,
    cache: Arc<FooterCache>,
    preflight: bool,
}
impl<S> fmt::Debug for FooterReaderFactory<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FooterReaderFactory").finish()
    }
}
impl<S: otmp::ObjectStore + fmt::Debug> FooterReaderFactory<S> {
    pub(crate) fn for_preflight(mut self) -> Self {
        self.preflight = true;
        self
    }
    pub fn new(bridge: Arc<ReadOnlyStore<S>>, cache: Arc<FooterCache>) -> Self {
        Self {
            inner: Arc::new(DefaultParquetFileReaderFactory::new(bridge.clone())),
            bridge,
            cache,
            preflight: false,
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
            factory: self.inner.clone(),
            key,
            cache: self.cache.clone(),
            lease: None,
            option_lease: None,
            hit_lease: None,
            counters: self.bridge.counters(),
            opened: false,
            preflight: self.preflight,
            load: None,
            registration: Arc::default(),
            key_reservation_bytes: 0,
            trace: LoadTrace::default(),
        }))
    }
}
struct CachedReader {
    inner: Box<dyn AsyncFileReader + Send>,
    factory: Arc<DefaultParquetFileReaderFactory>,
    key: FooterIdentity,
    cache: Arc<FooterCache>,
    lease: Option<Arc<FooterEntry>>,
    option_lease: Option<Arc<FooterEntry>>,
    hit_lease: Option<Reservation>,
    counters: Arc<crate::store::ReadCounters>,
    opened: bool,
    preflight: bool,
    load: Option<Arc<FooterLoad>>,
    registration: Registration,
    key_reservation_bytes: usize,
    trace: LoadTrace,
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
    fn pending_load(&self) -> ParquetResult<Option<Arc<FooterLoad>>> {
        let loads = self
            .cache
            .loads
            .lock()
            .map_err(|_| ParquetError::General("footer load lock poisoned".into()))?;
        if loads
            .keys()
            .any(|old| old.uri == self.key.uri && old != &self.key)
        {
            return Err(ParquetError::General(
                "conflicting in-flight footer reference".into(),
            ));
        }
        Ok(loads.get(&self.key).and_then(Weak::upgrade))
    }
    async fn shared_metadata(&mut self) -> ParquetResult<Arc<FooterEntry>> {
        let load = if let Some(load) = self.pending_load()? {
            load
        } else {
            // Only one new registry entry may wait for payload admission. It
            // cannot crowd out the payload of the previous admitted load.
            let turn = self.cache.registration.clone().lock_owned().await;
            if let Some(entry) = self
                .cache
                .get(&self.key, self.preflight.then_some(&mut self.hit_lease))?
            {
                return Ok(entry);
            }
            if let Some(load) = self.pending_load()? {
                load
            } else {
                let amount = 512 + self.key.uri.len() * 4 + self.key.version.len() * 2;
                let mut reservation = self.cache.admit(amount, Some(0), 0).await?;
                reservation.mark_key();
                let registration = Arc::new(Mutex::new(Some(turn)));
                let id = NEXT_LOAD.fetch_add(1, Ordering::Relaxed);
                let span = tracing::info_span!(
                    "otmp.load",
                    load_id = id,
                    kind = "footer",
                    uri = self.key.uri
                );
                let mut loader = Self {
                    inner: self
                        .factory
                        .create_reader(
                            0,
                            PartitionedFile::new(&self.key.uri, self.key.length),
                            None,
                            &ExecutionPlanMetricsSet::new(),
                        )
                        .map_err(|e| ParquetError::External(Box::new(e)))?,
                    factory: self.factory.clone(),
                    key: self.key.clone(),
                    cache: self.cache.clone(),
                    lease: None,
                    option_lease: None,
                    hit_lease: None,
                    counters: self.counters.clone(),
                    opened: false,
                    preflight: self.preflight,
                    load: None,
                    registration: registration.clone(),
                    key_reservation_bytes: amount,
                    trace: LoadTrace {
                        span,
                        ..LoadTrace::default()
                    },
                };
                let future = async move {
                    let result = loader.load_metadata(None).await;
                    loader.trace.outcome = if result.is_ok() { "success" } else { "error" };
                    drop(loader);
                    result.map_err(Arc::new)
                }
                .boxed()
                .shared();
                let load = Arc::new(FooterLoad {
                    future,
                    id,
                    key: self.key.clone(),
                    registry: self.cache.loads.clone(),
                    _reservation: reservation,
                    _registration: registration,
                });
                self.cache
                    .loads
                    .lock()
                    .map_err(|_| ParquetError::General("footer load lock poisoned".into()))?
                    .insert(self.key.clone(), Arc::downgrade(&load));
                // Keep the registration turn through trailer sizing and byte
                // admission. Otherwise an un-sized key can crowd out a later
                // footer that fits sequentially. Admitted payloads overlap.
                load
            }
        };
        tracing::debug!(target: "otmp.load", load_id = load.id, kind = "footer", "load waiter");
        let result = load.future.clone().await;
        if self.preflight && result.is_ok() {
            self.load = Some(load.clone());
        }
        drop(load);
        result.map_err(|error| {
            Arc::try_unwrap(error)
                .unwrap_or_else(|error| ParquetError::External(Box::new(SharedFooterError(error))))
        })
    }
    async fn reserve_footer(&mut self, footer_len: usize) -> ParquetResult<Reservation> {
        let estimate = footer_len
            .checked_mul(128)
            .and_then(|n| n.checked_add(64 * 1024))
            .ok_or_else(|| ParquetError::General("Parquet footer reservation overflow".into()))?;
        // A load's own transient key cannot release before this admission.
        // Reject that intrinsic impossibility instead of waiting on ourselves.
        if estimate.saturating_add(self.key_reservation_bytes) > self.cache.budget.limit {
            return Err(self.cache.exhausted(estimate));
        }
        let admitted = std::time::Instant::now();
        // Registered fills may wait for earlier keys. An option decode cannot
        // wait on key-only loads queued behind it for payload admission.
        let owned_keys = (self.key_reservation_bytes != 0).then_some(1);
        // Peer cache-hit validation can release evicted bytes even for an
        // option decode. Its own cached lease cannot release while it waits.
        let reservation = self
            .cache
            .admit(estimate, owned_keys, usize::from(self.hit_lease.is_some()))
            .await?;
        self.trace.admission_us += elapsed_us(admitted);
        let spare = self
            .cache
            .budget
            .limit
            .saturating_sub(self.cache.budget.used.load(Ordering::Acquire));
        let next =
            estimate.saturating_add(512 + self.key.uri.len() * 4 + self.key.version.len() * 2);
        if spare >= next {
            self.registration
                .lock()
                .map_err(|_| ParquetError::General("footer registration lock poisoned".into()))?
                .take();
        }
        Ok(reservation)
    }

    async fn read_tail(&mut self) -> ParquetResult<(bytes::Bytes, Reservation)> {
        // Non-default options can reuse authenticated serialized bytes, but
        // they must decode their own metadata with the requested policies.
        if let Some(entry) = self.cache.get(&self.key, None)?
            && entry.tail.len() >= 8
        {
            let reservation = self.reserve_footer(entry.tail.len() - 8).await?;
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
        self.trace.requests += 1;
        let trailer = self
            .inner
            .get_bytes((self.key.length - 8)..self.key.length)
            .await?;
        self.trace.bytes += trailer.len() as u64;
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
        let reservation = self
            .reserve_footer(
                usize::try_from(footer_len)
                    .map_err(|_| ParquetError::General("footer length overflow".into()))?,
            )
            .await?;
        self.trace.requests += 1;
        let raw = self
            .inner
            .get_bytes((self.key.length - 8 - footer_len)..(self.key.length - 8))
            .await?;
        self.trace.bytes += raw.len() as u64;
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
        #[cfg(test)]
        {
            let gate = self.cache.before_decode.lock().unwrap().take();
            if let Some(gate) = gate {
                gate[0].notify_one();
                gate[1].notified().await;
            }
        }
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
            reservation.finish_transient();
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
                self.lease = Some(
                    match self
                        .cache
                        .get(&self.key, self.preflight.then_some(&mut self.hit_lease))?
                    {
                        Some(entry) => entry,
                        None => self.shared_metadata().await?,
                    },
                );
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

static NEXT_LOAD: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
fn elapsed_us(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}
struct LoadTrace {
    span: tracing::Span,
    started: std::time::Instant,
    requests: u64,
    bytes: u64,
    admission_us: u64,
    outcome: &'static str,
}
impl Default for LoadTrace {
    fn default() -> Self {
        Self {
            span: tracing::Span::none(),
            started: std::time::Instant::now(),
            requests: 0,
            bytes: 0,
            admission_us: 0,
            outcome: "cancelled",
        }
    }
}
impl Drop for LoadTrace {
    fn drop(&mut self) {
        if !self.span.is_disabled() {
            self.span.in_scope(|| {
                tracing::info!(target: "otmp.load", elapsed_us = elapsed_us(self.started),
                requests = self.requests, bytes = self.bytes, admission_wait_us = self.admission_us,
                outcome = self.outcome, "physical load summary");
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn optioned_reader_waits_for_a_peer_cached_preflight_at_its_sequential_budget() {
        let (measured, first_length) = real_factory(false, 4096).await;
        let key = measured
            .bridge
            .footer_identity(&"data/two.parquet".into())
            .unwrap();
        let make = |factory: &FooterReaderFactory<otmp::InMemoryObjectStore>, uri, length| {
            factory
                .create_reader(
                    0,
                    PartitionedFile::new(uri, length),
                    None,
                    &ExecutionPlanMetricsSet::new(),
                )
                .unwrap()
        };
        let mut probe = make(&measured, "data/two.parquet", key.length);
        probe.get_metadata(None).await.unwrap();
        let entry = measured.cache.get(&key, None).unwrap().unwrap();
        let decode = (entry.tail.len() - 8) * 128 + 64 * 1024;
        let limit = measured.cache.statistics().0 + decode;
        let cache = FooterCache::new(limit).unwrap();
        let execution = FooterReaderFactory::new(measured.bridge.clone(), cache.clone());
        let preflight = execution.clone().for_preflight();
        let mut running = make(&execution, "data/two.parquet", key.length);
        running.get_metadata(None).await.unwrap();
        let mut warm = make(&preflight, "data/one.parquet", first_length);
        warm.get_metadata(None).await.unwrap();
        drop(warm);
        let mut hit = make(&preflight, "data/one.parquet", first_length);
        hit.get_metadata(None).await.unwrap();
        let options = ArrowReaderOptions::new();
        let mut decode = Box::pin(running.get_metadata(Some(&options)));
        assert!(
            futures_util::poll!(&mut decode).is_pending(),
            "option decode can wait on its peer's cached preflight, but not queued load keys"
        );
        drop(hit);
        assert_eq!(decode.await.unwrap().file_metadata().num_rows(), 2);
        drop(running);
        cache.assert_idle();
        assert!(cache.statistics().1 <= limit);
    }
    #[tokio::test]
    async fn cached_preflight_lease_allows_a_miss_to_wait_at_the_sequential_minimum() {
        let (factory, length) = real_factory(true, 0).await;
        let factory = factory.for_preflight();
        let make = |uri| {
            factory
                .create_reader(
                    0,
                    PartitionedFile::new(uri, length),
                    None,
                    &ExecutionPlanMetricsSet::new(),
                )
                .unwrap()
        };
        let mut first = make("data/one.parquet");
        first.get_metadata(None).await.unwrap();
        drop(first);
        let mut hit = make("data/one.parquet");
        hit.get_metadata(None).await.unwrap();
        let mut miss = make("data/two.parquet");
        let mut loading = Box::pin(miss.get_metadata(None));
        assert!(
            futures_util::poll!(&mut loading).is_pending(),
            "the active cached preflight can release its lease"
        );
        drop(hit);
        assert_eq!(loading.await.unwrap().file_metadata().num_rows(), 2);
        drop(miss);
        factory.cache.assert_idle();
        assert!(factory.cache.statistics().1 <= factory.cache.budget.limit);
    }

    #[tokio::test]
    async fn cancellation_after_validated_tail_releases_the_unfinished_decode() {
        let (factory, length) = real_factory(true, 0).await;
        let gate = [
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        ];
        *factory.cache.before_decode.lock().unwrap() = Some(gate.clone());
        let reader = factory.for_preflight();
        let cache = reader.cache.clone();
        let mut file = reader
            .create_reader(
                0,
                PartitionedFile::new("data/one.parquet", length),
                None,
                &ExecutionPlanMetricsSet::new(),
            )
            .unwrap();
        let task = tokio::spawn(async move { file.get_metadata(None).await });
        gate[0].notified().await;
        assert!(cache.statistics().0 > 0);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        cache.assert_idle();
        assert_eq!(cache.statistics().0, 0);
        // A retry proves registration/admission were not stranded by cancellation.
        let mut file = reader
            .create_reader(
                0,
                PartitionedFile::new("data/one.parquet", length),
                None,
                &ExecutionPlanMetricsSet::new(),
            )
            .unwrap();
        file.get_metadata(None).await.unwrap();
    }

    #[tokio::test]
    async fn required_indexes_obey_the_exact_budget_with_a_default_footer_lease() {
        use datafusion::parquet::file::metadata::PageIndexPolicy;
        let (factory, length) = real_factory(false, 0).await;
        let make = |factory: &FooterReaderFactory<otmp::InMemoryObjectStore>| {
            factory
                .create_reader(
                    0,
                    PartitionedFile::new("data/one.parquet", length),
                    None,
                    &ExecutionPlanMetricsSet::new(),
                )
                .unwrap()
        };
        let mut measured = make(&factory);
        measured.get_metadata(None).await.unwrap();
        let retained = factory.cache.statistics().0;
        let key = factory
            .bridge
            .footer_identity(&"data/one.parquet".into())
            .unwrap();
        let tail = factory.cache.get(&key, None).unwrap().unwrap();
        let decode = (tail.tail.len() - 8) * 128 + 64 * 1024;
        for shortfall in [0, 1] {
            let cache = FooterCache::new(retained + decode - shortfall).unwrap();
            let factory = FooterReaderFactory::new(factory.bridge.clone(), cache.clone());
            let mut reader = make(&factory);
            assert!(
                reader
                    .get_metadata(None)
                    .await
                    .unwrap()
                    .offset_index()
                    .is_none()
            );
            let options =
                ArrowReaderOptions::new().with_offset_index_policy(PageIndexPolicy::Required);
            let result = reader.get_metadata(Some(&options)).await;
            if shortfall == 0 {
                assert!(result.unwrap().offset_index().is_some());
            } else {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("footer cache exhausted")
                );
            }
            assert!(cache.statistics().1 <= cache.budget.limit);
            drop(reader);
            cache.assert_idle();
        }
    }

    #[tokio::test]
    async fn a_load_key_does_not_make_nontransient_pressure_wait_on_itself() {
        let cache = FooterCache::new(10).unwrap();
        let mut lease = cache.reserve(3).unwrap();
        lease.finish_transient();
        cache.budget.preflight_hits.fetch_add(1, Ordering::AcqRel);
        let hit = Reservation {
            budget: cache.budget.clone(),
            amount: 0,
            phase: ReservationPhase::Preflight,
        };
        assert!(
            cache
                .admit(8, None, 1)
                .now_or_never()
                .expect("an option decode cannot wait for its own cached lease")
                .is_err()
        );
        drop(hit);
        let mut key = cache.admit(2, Some(0), 0).await.unwrap();
        key.mark_key();
        assert!(
            cache
                .admit(8, Some(1), 0)
                .now_or_never()
                .expect("only this load's key is transient; no other work can release capacity")
                .is_err()
        );
        drop(lease);
        assert!(
            cache
                .admit(10, None, 0)
                .now_or_never()
                .expect("option decodes cannot wait on key-only loads behind them")
                .is_err()
        );
        drop(key);
        assert_eq!(cache.statistics().0, 0);
    }

    #[tokio::test]
    async fn registered_payload_waits_for_an_earlier_preflight_key() {
        let cache = FooterCache::new(10).unwrap();
        let mut earlier = cache.admit(2, Some(0), 0).await.unwrap();
        earlier.mark_key();
        let mut own = cache.admit(2, Some(0), 0).await.unwrap();
        own.mark_key();
        let mut payload = Box::pin(cache.admit(8, Some(1), 0));
        assert!(futures_util::poll!(&mut payload).is_pending());
        drop(earlier);
        let admitted = payload.await.unwrap();
        drop(own);
        drop(admitted);
        assert_eq!(cache.statistics().0, 0);
        assert_eq!(cache.budget.keys.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn byte_admission_waits_fifo_and_cancellation_releases_its_turn() {
        let cache = FooterCache::new(100).unwrap();
        let mut held = cache.admit(100, None, 0).await.unwrap();
        let mut first = Box::pin(cache.admit(80, None, 0));
        let mut second = Box::pin(cache.admit(20, None, 0));
        assert!(futures_util::poll!(&mut first).is_pending());
        assert!(futures_util::poll!(&mut second).is_pending());
        held.shrink(20);
        assert!(futures_util::poll!(&mut first).is_pending());
        assert!(futures_util::poll!(&mut second).is_pending());
        drop(first);
        let admitted = second.await.unwrap();
        assert_eq!(cache.statistics().0, 100);
        drop(admitted);
        drop(held);
        assert_eq!(cache.statistics().0, 0);
        assert_eq!(cache.budget.transient.load(Ordering::Acquire), 0);
        assert!(cache.admit(101, None, 0).await.is_err());
    }

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
        assert!(Arc::ptr_eq(
            &cache.get(&first, None).unwrap().unwrap(),
            &held
        ));
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
            cache.get(&first, None).unwrap().is_none(),
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

    async fn real_factory(
        tight: bool,
        padding: usize,
    ) -> (FooterReaderFactory<otmp::InMemoryObjectStore>, u64) {
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
        let second: otmp_protocol::RelativeUri = "data/two.parquet".parse().unwrap();
        let second_bytes = if padding == 0 {
            bytes.clone()
        } else {
            let props = datafusion::parquet::file::properties::WriterProperties::builder()
                .set_key_value_metadata(Some(vec![
                    datafusion::parquet::file::metadata::KeyValue::new(
                        "padding".into(),
                        "x".repeat(padding),
                    ),
                ]))
                .build();
            let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.into_inner().unwrap()
        };
        let second_created = store.create_bytes(&second, &second_bytes).await.unwrap();
        let bridge = Arc::new(
            ReadOnlyStore::new(
                store,
                [
                    (uri, Some(created.sha256), created.length),
                    (second, Some(second_created.sha256), second_created.length),
                ],
            )
            .await
            .unwrap(),
        );
        let key = bridge.footer_identity(&"data/two.parquet".into()).unwrap();
        let footer_len =
            u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap())
                as usize;
        let minimum =
            footer_len * 128 + 64 * 1024 + 512 + key.uri.len() * 4 + key.version.len() * 2;
        let factory = FooterReaderFactory::new(
            bridge,
            FooterCache::new(if tight {
                minimum
            } else {
                DEFAULT_FOOTER_CACHE_BYTES
            })
            .unwrap(),
        );
        (factory, created.length)
    }

    async fn real_reader() -> (
        Box<dyn AsyncFileReader + Send>,
        Arc<crate::store::ReadCounters>,
    ) {
        let (factory, length) = real_factory(false, 0).await;
        let counters = factory.bridge.counters();
        let reader = factory
            .create_reader(
                0,
                PartitionedFile::new("data/one.parquet", length),
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
            4,
            "two fixture pins plus trailer and footer, with no redundant decode reads"
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
        assert_eq!(counters.requests.load(Ordering::Relaxed), 4);
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
        assert!(cache.get(&first, None).unwrap().is_none());
        let conflicting = FooterIdentity {
            version: "v2".into(),
            ..first
        };
        assert!(cache.get(&conflicting, None).is_err());
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
