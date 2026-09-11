//! Authenticated, bounded metadata reading. Exhaustive verification remains separate.

pub(crate) mod cache;
mod files;
mod metadata;
mod pages;
mod schema;
mod selection;
mod snapshot;
pub use metadata::{FileBatch, FileCursor, FileMetricRange, MetadataReader, ReaderFile};
pub(crate) use pages::{AuthenticatedImage, ReadContext, WeakReadContext};

/// Budgets shared by readers opened through a cloned table instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaderOptions {
    pub cache_budget_bytes: usize,
    pub engine_page_cache_bytes: usize,
    pub checkpoint_window_bytes: usize,
    pub max_inflight_reads: usize,
    /// Upper bound checked in `SQLite` cells before the engine reads overflow pages.
    pub maximum_record_bytes: usize,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            cache_budget_bytes: 64 * 1024 * 1024,
            engine_page_cache_bytes: crate::reader_engine::DEFAULT_PAGE_CACHE_BYTES,
            checkpoint_window_bytes: 64 * 1024,
            max_inflight_reads: 8,
            maximum_record_bytes: 1024 * 1024,
        }
    }
}

impl ReaderOptions {
    pub(crate) fn validate(&self) -> Result<(), crate::RuntimeError> {
        if self.cache_budget_bytes < 4096
            || self.engine_page_cache_bytes < 4096
            || self.checkpoint_window_bytes < 4096
            || !self.checkpoint_window_bytes.is_multiple_of(4096)
            || self.checkpoint_window_bytes > self.cache_budget_bytes
            || !(1..=8).contains(&self.max_inflight_reads)
            || self.maximum_record_bytes < 4096
            || self.maximum_record_bytes > self.cache_budget_bytes / 8
        {
            return Err(crate::RuntimeError::ResourceExhausted(
                "invalid reader budgets".into(),
            ));
        }
        Ok(())
    }
}

/// Cumulative counters for one shared metadata storage context.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReaderStatistics {
    pub bytes: u64,
    pub requests: u64,
    pub pages: u64,
    pub cache_hits: u64,
    pub cache_bytes: usize,
    pub peak_cache_bytes: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct WriterReadStatistics {
    pub(crate) total: ReaderStatistics,
    pub(crate) page_map_bytes: u64,
    pub(crate) page_map_requests: u64,
}
