use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Serialize)]
pub struct Counts {
    pub stat_requests: u64,
    pub range_requests: u64,
    pub full_reads: u64,
    pub bytes: u64,
    pub errors: u64,
    pub cancelled: u64,
    pub elapsed_us: u64,
    pub injected_us: u64,
    pub active: u64,
    pub peak_inflight: u64,
    #[cfg(test)]
    pub metadata_data_overlaps: u64,
    pub by_class: BTreeMap<String, Counts>,
}
impl Counts {
    pub fn delta(&self, before: &Self) -> Self {
        Self {
            stat_requests: self.stat_requests - before.stat_requests,
            range_requests: self.range_requests - before.range_requests,
            full_reads: self.full_reads - before.full_reads,
            bytes: self.bytes - before.bytes,
            errors: self.errors - before.errors,
            cancelled: self.cancelled - before.cancelled,
            elapsed_us: self.elapsed_us - before.elapsed_us,
            injected_us: self.injected_us - before.injected_us,
            active: self.active,
            peak_inflight: self.peak_inflight,
            #[cfg(test)]
            metadata_data_overlaps: self.metadata_data_overlaps - before.metadata_data_overlaps,
            by_class: self
                .by_class
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        value.delta(before.by_class.get(key).unwrap_or(&Self::default())),
                    )
                })
                .collect(),
        }
    }
}
fn object_class(uri: &str) -> &'static str {
    if uri == "_otmp/HEAD" {
        return "head";
    }
    for (prefix, class) in [
        ("_otmp/checkpoint-page-index/", "checkpoint_index"),
        ("_otmp/checkpoints/", "checkpoint"),
        ("_otmp/page-packs/", "page_pack"),
        ("_otmp/page-maps/", "page_map"),
        ("_otmp/generations/", "generation"),
        ("_otmp/commits/", "commit"),
        ("data/", "data"),
    ] {
        if uri.starts_with(prefix) {
            return class;
        }
    }
    "other"
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn classifies_explicit_metadata_and_data_references() {
        assert_eq!(
            object_class("_otmp/checkpoint-page-index/a.cbor"),
            "checkpoint_index"
        );
        assert_eq!(object_class("_otmp/page-packs/a.otmppg"), "page_pack");
        assert_eq!(object_class("_otmp/HEAD"), "head");
        assert_eq!(object_class("data/a.parquet"), "data");
    }
    #[test]
    fn phase_deltas_preserve_cumulative_peak_and_separate_operation_counts() {
        let before = Counts {
            stat_requests: 4,
            range_requests: 3,
            bytes: 1024,
            peak_inflight: 2,
            ..Counts::default()
        };
        let after = Counts {
            stat_requests: 5,
            range_requests: 5,
            bytes: 2048,
            peak_inflight: 3,
            ..Counts::default()
        };
        let delta = after.delta(&before);
        assert_eq!(
            (delta.stat_requests, delta.range_requests, delta.bytes),
            (1, 2, 1024)
        );
        assert_eq!(delta.peak_inflight, 3);
    }
}

use async_trait::async_trait;
use otmp::storage::{CreatedObject, StoredObject};
use otmp::{
    ConditionalWriteOutcome, ObjectMetadata, ObjectStore, ObjectVersion, StorageError, StoredRange,
};
use otmp_protocol::RelativeUri;
use std::{
    ops::Range,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::AsyncRead;

#[derive(Clone, Debug)]
pub struct MeasuredStore<S> {
    inner: S,
    delay: Duration,
    counts: Arc<Mutex<Counts>>,
    data_delays: BTreeMap<String, u64>,
    fault: Option<DataFault>,
    data_operations: Arc<Mutex<BTreeMap<String, u64>>>,
    #[cfg(test)]
    trailer_gate: TrailerGate,
}
#[cfg(test)]
type TrailerGate = Arc<Mutex<Option<[Arc<tokio::sync::Notify>; 2]>>>;
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DataFault {
    pub operation: String,
    pub request: u64,
}
impl<S> MeasuredStore<S> {
    pub fn new(inner: S, delay: Duration) -> Self {
        Self {
            inner,
            delay,
            counts: Arc::default(),
            data_delays: BTreeMap::new(),
            fault: None,
            data_operations: Arc::default(),
            #[cfg(test)]
            trailer_gate: Arc::default(),
        }
    }
    #[cfg(test)]
    pub fn pause_next_trailer(&self) -> [Arc<tokio::sync::Notify>; 2] {
        let gate = [Arc::default(), Arc::default()];
        *self.trailer_gate.lock().unwrap() = Some(gate.clone());
        gate
    }
    pub fn with_data_controls(
        mut self,
        delays: BTreeMap<String, u64>,
        fault: Option<DataFault>,
    ) -> Self {
        self.data_delays = delays;
        self.fault = fault;
        self
    }
    fn data_control(&self, uri: &RelativeUri, operation: &str) -> (Duration, bool) {
        if object_class(uri.as_str()) != "data" {
            return (self.delay, false);
        }
        let mut operations = self.data_operations.lock().unwrap();
        let count = operations.entry(operation.into()).or_default();
        *count += 1;
        let fail = self
            .fault
            .as_ref()
            .is_some_and(|fault| fault.operation == operation && fault.request == *count);
        (
            self.data_delays
                .get(operation)
                .map_or(self.delay, |ms| Duration::from_millis(*ms)),
            fail,
        )
    }
    pub fn snapshot(&self) -> Counts {
        self.counts.lock().unwrap().clone()
    }
}
fn unsupported() -> StorageError {
    StorageError::Unsupported("qualification store is read-only and forbids full reads".into())
}
#[async_trait]
impl<S: ObjectStore> ObjectStore for MeasuredStore<S> {
    async fn read(&self, uri: &RelativeUri) -> Result<StoredObject, StorageError> {
        let mut guard = self.begin(uri, Operation::Full, self.delay).await;
        guard.finish(0, true);
        Err(unsupported())
    }
    async fn stat(&self, uri: &RelativeUri) -> Result<ObjectMetadata, StorageError> {
        let (delay, fail) = self.data_control(uri, "stat");
        let mut guard = self.begin(uri, Operation::Stat, delay).await;
        if fail {
            guard.finish(0, true);
            return Err(StorageError::Unsupported(
                "injected data stat failure".into(),
            ));
        }
        let result = self.inner.stat(uri).await;
        guard.finish(0, result.is_err());
        result
    }
    async fn read_range(
        &self,
        uri: &RelativeUri,
        range: Range<u64>,
        expected: &ObjectMetadata,
    ) -> Result<StoredRange, StorageError> {
        let operation =
            if range.end == expected.length && range.end.saturating_sub(range.start) == 8 {
                "trailer"
            } else if range.end == expected.length.saturating_sub(8) {
                "footer"
            } else {
                "data"
            };
        let (delay, fail) = self.data_control(uri, operation);
        let mut guard = self.begin(uri, Operation::Range, delay).await;
        #[cfg(test)]
        if operation == "trailer" && object_class(uri.as_str()) == "data" {
            let gate = self.trailer_gate.lock().unwrap().take();
            if let Some([entered, release]) = gate {
                entered.notify_one();
                release.notified().await;
            }
        }
        if fail {
            guard.finish(0, true);
            return Err(StorageError::Unsupported(format!(
                "injected data {operation} failure"
            )));
        }
        let result = self
            .inner
            .read_range(uri, range.clone(), expected)
            .await
            .and_then(|value| {
                value.validate(expected, &range)?;
                Ok(value)
            });
        guard.finish(
            result.as_ref().map_or(0, |r| r.bytes.len() as u64),
            result.is_err(),
        );
        result
    }
    async fn create_from_reader(
        &self,
        _: &RelativeUri,
        _: &mut (dyn AsyncRead + Send + Unpin),
        _: Option<u64>,
    ) -> Result<CreatedObject, StorageError> {
        Err(unsupported())
    }
    async fn create_head(&self, _: &[u8]) -> ConditionalWriteOutcome {
        ConditionalWriteOutcome::Indeterminate {
            source: unsupported(),
        }
    }
    async fn replace_head(&self, _: &ObjectVersion, _: &[u8]) -> ConditionalWriteOutcome {
        ConditionalWriteOutcome::Indeterminate {
            source: unsupported(),
        }
    }
    async fn delete_if_version(
        &self,
        _: &RelativeUri,
        _: &ObjectVersion,
    ) -> Result<bool, StorageError> {
        Err(unsupported())
    }
}

#[cfg(test)]
mod store_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn delayed_parallel_requests_report_overlap_and_exact_range_bytes() {
        let inner = otmp::InMemoryObjectStore::default();
        let uri: RelativeUri = "data/example.parquet".parse().unwrap();
        inner.create_bytes(&uri, b"abcdefgh").await.unwrap();
        let store = MeasuredStore::new(inner, Duration::from_millis(10));
        let (a, b) = tokio::join!(store.stat(&uri), store.stat(&uri));
        assert_eq!(a.unwrap().length, b.unwrap().length);
        let meta = store.stat(&uri).await.unwrap();
        assert_eq!(
            store.read_range(&uri, 2..6, &meta).await.unwrap().bytes,
            b"cdef"
        );
        let counts = store.snapshot();
        assert_eq!(
            (counts.stat_requests, counts.range_requests, counts.bytes),
            (3, 1, 4)
        );
        assert_eq!(
            (counts.active, counts.peak_inflight, counts.injected_us),
            (0, 2, 40_000)
        );
        assert_eq!(counts.by_class["data"].range_requests, 1);
    }
    #[tokio::test(start_paused = true)]
    async fn cancellation_releases_inflight_measurements() {
        let store = MeasuredStore::new(
            otmp::InMemoryObjectStore::default(),
            Duration::from_secs(100),
        );
        let copy = store.clone();
        let task = tokio::spawn(async move { copy.stat(&"_otmp/HEAD".parse().unwrap()).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        task.abort();
        let _ = task.await;
        let counts = store.snapshot();
        assert_eq!(counts.injected_us, 1_000_000);
        assert_eq!(
            (counts.active, counts.cancelled, counts.stat_requests),
            (0, 1, 1)
        );
    }
    #[tokio::test]
    async fn transport_errors_and_forbidden_full_reads_are_visible() {
        let store = MeasuredStore::new(otmp::InMemoryObjectStore::default(), Duration::ZERO);
        let uri: RelativeUri = "_otmp/HEAD".parse().unwrap();
        assert!(store.stat(&uri).await.is_err());
        assert!(matches!(
            store.read(&uri).await,
            Err(StorageError::Unsupported(_))
        ));
        let counts = store.snapshot();
        assert_eq!((counts.errors, counts.full_reads, counts.active), (2, 1, 0));
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Stat,
    Range,
    Full,
}
impl Counts {
    fn enter(&mut self, operation: Operation) {
        match operation {
            Operation::Stat => self.stat_requests += 1,
            Operation::Range => self.range_requests += 1,
            Operation::Full => self.full_reads += 1,
        }
        self.active += 1;
        self.peak_inflight = self.peak_inflight.max(self.active);
    }
}
struct Request {
    counts: Arc<Mutex<Counts>>,
    class: &'static str,
    started: tokio::time::Instant,
    injected_us: u64,
    finished: bool,
}
impl<S> MeasuredStore<S> {
    async fn begin(&self, uri: &RelativeUri, operation: Operation, delay: Duration) -> Request {
        let class = object_class(uri.as_str());
        {
            let mut counts = self.counts.lock().unwrap();
            #[cfg(test)]
            {
                let data = counts.by_class.get("data").map_or(0, |value| value.active);
                if (class == "data" && counts.active > data) || (class != "data" && data > 0) {
                    counts.metadata_data_overlaps += 1;
                }
            }
            counts.enter(operation);
            counts
                .by_class
                .entry(class.into())
                .or_default()
                .enter(operation);
        }
        let guard = Request {
            counts: self.counts.clone(),
            class,
            started: tokio::time::Instant::now(),
            injected_us: u64::try_from(delay.as_micros()).unwrap_or(u64::MAX),
            finished: false,
        };
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        guard
    }
}
impl Request {
    fn finish(&mut self, bytes: u64, error: bool) {
        self.complete(bytes, error, false);
    }
    fn complete(&mut self, bytes: u64, error: bool, cancelled: bool) {
        let elapsed = u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let update = |counts: &mut Counts| {
            counts.active -= 1;
            counts.bytes += bytes;
            counts.errors += u64::from(error);
            counts.cancelled += u64::from(cancelled);
            counts.elapsed_us += elapsed;
            counts.injected_us += self.injected_us.min(elapsed);
        };
        let mut counts = self.counts.lock().unwrap();
        update(&mut counts);
        update(counts.by_class.get_mut(self.class).unwrap());
        self.finished = true;
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        if !self.finished {
            self.complete(0, false, true);
        }
    }
}
