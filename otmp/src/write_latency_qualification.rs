use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use async_trait::async_trait;
use otmp_protocol::{RelativeUri, Sha256};
use serde::Serialize;
use tokio::io::AsyncRead;

use crate::storage::{CreatedObject, ObjectMetadata, ObjectVersion, StoredObject, StoredRange};
use crate::{ConditionalWriteOutcome, ObjectStore, StorageError};

pub mod worker;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PhaseMeasurement {
    pub name: String,
    pub duration_ns: u64,
    pub depth: u32,
    pub performed: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ObjectIo {
    pub requests: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProbeReport {
    pub acknowledged_latency_ns: u64,
    pub phases: Vec<PhaseMeasurement>,
    pub counters: BTreeMap<String, u64>,
    pub object_store: BTreeMap<String, ObjectIo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeError(String);

impl fmt::Display for ProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ProbeError {}

struct ActiveSession {
    started: Instant,
    last_top_end: Option<Instant>,
    last_top_phase: Option<usize>,
    active_phases: Vec<ActivePhase>,
    phases: Vec<PhaseMeasurement>,
    counters: BTreeMap<String, u64>,
    object_store: BTreeMap<String, ObjectIo>,
    nesting_error: Option<String>,
}

struct ActivePhase {
    name: &'static str,
    started: Instant,
    depth: u32,
}

fn state() -> &'static Mutex<Option<ActiveSession>> {
    static STATE: OnceLock<Mutex<Option<ActiveSession>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(None))
}

#[must_use]
pub struct ProbeSession {
    finished: bool,
}

impl ProbeSession {
    pub fn finish(mut self) -> Result<ProbeReport, ProbeError> {
        let mut slot = state().lock().unwrap();
        let finished_at = Instant::now();
        let mut active = slot
            .take()
            .ok_or_else(|| ProbeError("probe session is not active".into()))?;
        self.finished = true;
        if !active.active_phases.is_empty() {
            return Err(ProbeError(format!(
                "unfinished probe phases: {}",
                active
                    .active_phases
                    .iter()
                    .map(|phase| phase.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        if let Some(error) = active.nesting_error {
            return Err(ProbeError(error));
        }
        if let (Some(last_top_end), Some(last_top_phase)) =
            (active.last_top_end, active.last_top_phase)
        {
            active.phases[last_top_phase].duration_ns = active.phases[last_top_phase]
                .duration_ns
                .saturating_add(nanos(finished_at.duration_since(last_top_end)));
        }
        Ok(ProbeReport {
            acknowledged_latency_ns: nanos(finished_at.duration_since(active.started)),
            phases: active.phases,
            counters: active.counters,
            object_store: active.object_store,
        })
    }
}

impl Drop for ProbeSession {
    fn drop(&mut self) {
        if !self.finished {
            *state().lock().unwrap() = None;
        }
    }
}

pub fn start() -> Result<ProbeSession, ProbeError> {
    let mut slot = state().lock().unwrap();
    if slot.is_some() {
        return Err(ProbeError(
            "a write-latency probe session is already active".into(),
        ));
    }
    let counters = [
        "parent_logical_bytes",
        "candidate_logical_bytes",
        "changed_pages",
        "temporary_file_bytes",
        "published_image_artifact_bytes",
    ]
    .into_iter()
    .map(|name| (name.into(), 0))
    .collect();
    let object_store = [
        "head",
        "commit",
        "generation",
        "checkpoint",
        "page_map",
        "page_pack",
        "other",
    ]
    .into_iter()
    .map(|name| (name.into(), ObjectIo::default()))
    .collect();
    *slot = Some(ActiveSession {
        started: Instant::now(),
        last_top_end: None,
        last_top_phase: None,
        active_phases: Vec::new(),
        phases: Vec::new(),
        counters,
        object_store,
        nesting_error: None,
    });
    Ok(ProbeSession { finished: false })
}

#[must_use]
pub struct PhaseGuard {
    name: Option<&'static str>,
}

pub fn phase(name: &'static str) -> PhaseGuard {
    let mut slot = state().lock().unwrap();
    let Some(active) = slot.as_mut() else {
        return PhaseGuard { name: None };
    };
    let depth = u32::try_from(active.active_phases.len()).unwrap_or(u32::MAX);
    let now = Instant::now();
    active.active_phases.push(ActivePhase {
        name,
        started: if depth == 0 {
            active.last_top_end.unwrap_or(active.started)
        } else {
            now
        },
        depth,
    });
    PhaseGuard { name: Some(name) }
}

impl Drop for PhaseGuard {
    fn drop(&mut self) {
        let Some(name) = self.name else {
            return;
        };
        let mut slot = state().lock().unwrap();
        let Some(active) = slot.as_mut() else {
            return;
        };
        let Some(phase) = active.active_phases.pop() else {
            active.nesting_error = Some(format!("probe phase nesting error at {name}"));
            return;
        };
        if phase.name != name {
            active.nesting_error = Some(format!("probe phase nesting error at {name}"));
        }
        let ended = Instant::now();
        let measurement = active.phases.len();
        active.phases.push(PhaseMeasurement {
            name: name.into(),
            duration_ns: nanos(ended.duration_since(phase.started)),
            depth: phase.depth,
            performed: true,
        });
        if phase.depth == 0 {
            active.last_top_end = Some(ended);
            active.last_top_phase = Some(measurement);
        }
    }
}

pub(crate) fn skipped_phase(name: &'static str, depth: u32) {
    if let Some(active) = state().lock().unwrap().as_mut() {
        active.phases.push(PhaseMeasurement {
            name: name.into(),
            duration_ns: 0,
            depth,
            performed: false,
        });
    }
}

pub fn add_bytes(name: &'static str, bytes: u64) {
    if let Some(active) = state().lock().unwrap().as_mut() {
        let counter = active.counters.entry(name.into()).or_default();
        *counter = counter.saturating_add(bytes);
    }
}

fn record_io(category: &str, bytes_read: u64, bytes_written: u64) {
    if let Some(active) = state().lock().unwrap().as_mut() {
        let io = active.object_store.entry(category.into()).or_default();
        io.requests = io.requests.saturating_add(1);
        io.bytes_read = io.bytes_read.saturating_add(bytes_read);
        io.bytes_written = io.bytes_written.saturating_add(bytes_written);
    }
}

fn category(key: &RelativeUri) -> &'static str {
    let key = key.as_str();
    if key == "_otmp/HEAD" {
        "head"
    } else if key.starts_with("_otmp/commits/") {
        "commit"
    } else if key.starts_with("_otmp/generations/") {
        "generation"
    } else if key.starts_with("_otmp/checkpoints/") {
        "checkpoint"
    } else if key.starts_with("_otmp/page-maps/") {
        "page_map"
    } else if key.starts_with("_otmp/page-packs/") {
        "page_pack"
    } else {
        "other"
    }
}

fn nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug)]
pub struct QualificationStore<S>(S);

impl<S> QualificationStore<S> {
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self(store)
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for QualificationStore<S> {
    async fn read(&self, key: &RelativeUri) -> Result<StoredObject, StorageError> {
        let result = self.0.read(key).await;
        record_io(
            category(key),
            result
                .as_ref()
                .map_or(0, |object| object.bytes.len() as u64),
            0,
        );
        result
    }

    async fn stat(&self, key: &RelativeUri) -> Result<ObjectMetadata, StorageError> {
        let result = self.0.stat(key).await;
        record_io(category(key), 0, 0);
        result
    }

    async fn read_range(
        &self,
        key: &RelativeUri,
        range: std::ops::Range<u64>,
        expected: &ObjectMetadata,
    ) -> Result<StoredRange, StorageError> {
        let result = self.0.read_range(key, range, expected).await;
        record_io(
            category(key),
            result
                .as_ref()
                .map_or(0, |stored| stored.bytes.len() as u64),
            0,
        );
        result
    }

    async fn create_from_reader(
        &self,
        key: &RelativeUri,
        reader: &mut (dyn AsyncRead + Send + Unpin),
        maximum_length: Option<u64>,
    ) -> Result<CreatedObject, StorageError> {
        let result = self.0.create_from_reader(key, reader, maximum_length).await;
        record_io(
            category(key),
            0,
            result.as_ref().map_or(0, |object| object.length),
        );
        result
    }

    async fn create_head(&self, bytes: &[u8]) -> ConditionalWriteOutcome {
        let result = self.0.create_head(bytes).await;
        record_io("head", 0, bytes.len() as u64);
        result
    }

    async fn replace_head(
        &self,
        expected: &ObjectVersion,
        bytes: &[u8],
    ) -> ConditionalWriteOutcome {
        let result = self.0.replace_head(expected, bytes).await;
        record_io("head", 0, bytes.len() as u64);
        result
    }

    async fn delete_if_version(
        &self,
        key: &RelativeUri,
        version: &ObjectVersion,
    ) -> Result<bool, StorageError> {
        let result = self.0.delete_if_version(key, version).await;
        record_io(category(key), 0, 0);
        result
    }

    async fn confirm_readable(
        &self,
        key: &RelativeUri,
        sha256: Sha256,
        length: u64,
    ) -> Result<ObjectVersion, StorageError> {
        let result = self.0.confirm_readable(key, sha256, length).await;
        record_io(category(key), if result.is_ok() { length } else { 0 }, 0);
        result
    }
}
