use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use turso_core::io::FileSyncType;
use turso_core::{
    Buffer, CheckpointMode, Completion, Database, DatabaseStorage, IOContext, LimboError,
    PlatformIO, Result,
};

const PAGE: usize = 4096;

struct State {
    pub(crate) length: usize,
    parent_limit: usize,
    pages: BTreeMap<usize, Vec<u8>>,
}

pub struct Overlay {
    parent: Parent,
    state: Mutex<State>,
    failed: AtomicBool,
    failure: Mutex<Option<crate::RuntimeError>>,
    #[cfg(test)]
    pub fail_writes: AtomicBool,
}

#[derive(Clone)]
enum Parent {
    Bytes(Arc<[u8]>),
    Pages {
        source: Arc<dyn crate::reader_engine::PageSource>,
        handle: tokio::runtime::Handle,
    },
}

impl Overlay {
    pub fn new(parent: Arc<[u8]>) -> Self {
        let length = parent.len();
        Self {
            state: Mutex::new(State {
                length,
                parent_limit: length,
                pages: BTreeMap::new(),
            }),
            parent: Parent::Bytes(parent),
            failed: AtomicBool::new(false),
            failure: Mutex::new(None),
            #[cfg(test)]
            fail_writes: AtomicBool::new(false),
        }
    }

    pub fn from_pages(
        source: Arc<dyn crate::reader_engine::PageSource>,
        handle: tokio::runtime::Handle,
    ) -> Result<Self> {
        let length = usize::try_from(source.length()).map_err(|_| LimboError::IntegerOverflow)?;
        Ok(Self {
            state: Mutex::new(State {
                length,
                parent_limit: length,
                pages: BTreeMap::new(),
            }),
            parent: Parent::Pages { source, handle },
            failed: AtomicBool::new(false),
            failure: Mutex::new(None),
            #[cfg(test)]
            fail_writes: AtomicBool::new(false),
        })
    }

    fn parent_page(&self, index: usize) -> Result<Option<Vec<u8>>, crate::RuntimeError> {
        match &self.parent {
            Parent::Bytes(parent) => Ok(parent
                .get(index * PAGE..(index + 1) * PAGE)
                .map(<[u8]>::to_vec)),
            Parent::Pages { source, handle } => {
                if index * PAGE >= usize::try_from(source.length()).unwrap_or(usize::MAX) {
                    return Ok(None);
                }
                let mut page = vec![0; PAGE];
                crate::runtime::failpoint("during_parent_page_read");
                std::thread::scope(|scope| {
                    scope
                        .spawn(|| {
                            handle.block_on(
                                source.read_page(
                                    u64::try_from(index).unwrap_or(u64::MAX) + 1,
                                    &mut page,
                                ),
                            )
                        })
                        .join()
                        .map_err(|_| {
                            crate::RuntimeError::Turso("parent page worker panicked".into())
                        })?
                })?;
                Ok(Some(page))
            }
        }
    }

    fn read_at(
        &self,
        state: &State,
        offset: usize,
        bytes: &mut [u8],
    ) -> Result<usize, crate::RuntimeError> {
        bytes.fill(0);
        let count = bytes.len().min(state.length.saturating_sub(offset));
        let mut copied = 0;
        while copied < count {
            let pos = offset + copied;
            let chunk = (PAGE - pos % PAGE).min(count - copied);
            if let Some(page) = state.pages.get(&(pos / PAGE)) {
                bytes[copied..copied + chunk]
                    .copy_from_slice(&page[pos % PAGE..pos % PAGE + chunk]);
            } else if pos < state.parent_limit {
                let from_parent = chunk.min(state.parent_limit - pos);
                let page = self
                    .parent_page(pos / PAGE)?
                    .ok_or_else(|| crate::RuntimeError::Corrupt("missing parent page".into()))?;
                bytes[copied..copied + from_parent]
                    .copy_from_slice(&page[pos % PAGE..pos % PAGE + from_parent]);
            }
            copied += chunk;
        }
        Ok(count)
    }

    fn read(&self, offset: usize, c: Completion) -> Result<Completion> {
        let state = self.state.lock().unwrap();
        let count = self
            .read_at(&state, offset, c.as_read().buf().as_mut_slice())
            .map_err(|error| self.io_error(error))?;
        drop(state);
        c.complete(i32::try_from(count).map_err(|_| LimboError::IntegerOverflow)?);
        Ok(c)
    }

    #[cfg(test)]
    pub fn export(&self) -> Vec<u8> {
        let state = self.state.lock().unwrap();
        let mut bytes = vec![0; state.length];
        self.read_at(&state, 0, &mut bytes).unwrap();
        bytes
    }

    fn io_error(&self, error: crate::RuntimeError) -> LimboError {
        if let Ok(mut failure) = self.failure.lock()
            && failure.is_none()
        {
            *failure = Some(error);
        }
        turso_core::io_error(
            std::io::Error::other("authenticated parent page failed"),
            "read_page",
        )
    }

    fn take_failure(&self) -> Result<(), crate::RuntimeError> {
        self.failure
            .lock()
            .map_err(|_| crate::RuntimeError::Turso("writer failure lock poisoned".into()))?
            .take()
            .map_or(Ok(()), Err)
    }
}

impl Overlay {
    fn read_header_inner(&self, c: Completion) -> Result<Completion> {
        self.read(0, c)
    }
    fn read_page_inner(&self, page_idx: usize, _: &IOContext, c: Completion) -> Result<Completion> {
        self.read(
            page_idx
                .checked_sub(1)
                .and_then(|p| p.checked_mul(PAGE))
                .ok_or(LimboError::IntegerOverflow)?,
            c,
        )
    }
    fn write_page_inner(
        &self,
        page_idx: usize,
        buffer: &Buffer,
        _: &IOContext,
        c: Completion,
    ) -> Result<Completion> {
        #[cfg(test)]
        if self.fail_writes.load(Ordering::Relaxed) {
            return Err(turso_core::io_error(
                std::io::Error::other("injected checkpoint failure"),
                "write_page",
            ));
        }
        if buffer.len() != PAGE || page_idx == 0 {
            return Err(LimboError::InvalidArgument("expected 4 KiB page".into()));
        }
        let end = page_idx
            .checked_mul(PAGE)
            .ok_or(LimboError::IntegerOverflow)?;
        let mut state = self.state.lock().unwrap();
        state.pages.insert(page_idx - 1, buffer.as_slice().to_vec());
        state.length = state.length.max(end);
        drop(state);
        c.complete(4096);
        Ok(c)
    }
    fn write_pages_inner(
        &self,
        first: usize,
        page_size: usize,
        buffers: Vec<Arc<Buffer>>,
        ctx: &IOContext,
        c: Completion,
    ) -> Result<Completion> {
        if page_size != PAGE {
            return Err(LimboError::InvalidArgument("expected 4 KiB page".into()));
        }
        let count = buffers
            .len()
            .checked_mul(PAGE)
            .ok_or(LimboError::IntegerOverflow)?;
        for (index, buffer) in buffers.into_iter().enumerate() {
            drop(
                self.write_page(
                    first
                        .checked_add(index)
                        .ok_or(LimboError::IntegerOverflow)?,
                    buffer,
                    ctx,
                    Completion::new_write(|_| {}),
                )?,
            );
        }
        c.complete(i32::try_from(count).map_err(|_| LimboError::IntegerOverflow)?);
        Ok(c)
    }
}

impl Overlay {
    fn capture(&self, result: Result<Completion>) -> Result<Completion> {
        // Turso 0.7.2 shutdown can swallow non-Busy checkpoint errors. Keep a
        // sticky failure independent of the engine's return value.
        if match &result {
            Err(_) => true,
            Ok(completion) => completion.failed(),
        } {
            self.failed.store(true, Ordering::Release);
        }
        result
    }
}
impl DatabaseStorage for Overlay {
    fn read_header(&self, c: Completion) -> Result<Completion> {
        self.capture(self.read_header_inner(c))
    }
    fn read_page(&self, page: usize, context: &IOContext, c: Completion) -> Result<Completion> {
        self.capture(self.read_page_inner(page, context, c))
    }
    fn write_page(
        &self,
        page: usize,
        buffer: Arc<Buffer>,
        context: &IOContext,
        c: Completion,
    ) -> Result<Completion> {
        self.capture(self.write_page_inner(page, &buffer, context, c))
    }
    fn write_pages(
        &self,
        first: usize,
        size: usize,
        buffers: Vec<Arc<Buffer>>,
        context: &IOContext,
        c: Completion,
    ) -> Result<Completion> {
        self.capture(self.write_pages_inner(first, size, buffers, context, c))
    }
    fn sync(&self, c: Completion, _: FileSyncType) -> Result<Completion> {
        c.complete(0);
        self.capture(Ok(c))
    }
    fn size(&self) -> Result<u64> {
        Ok(self.state.lock().unwrap().length as u64)
    }
    fn truncate(&self, len: usize, c: Completion) -> Result<Completion> {
        let mut state = self.state.lock().unwrap();
        if len < state.length {
            if !len.is_multiple_of(PAGE) {
                let mut page = vec![0; PAGE];
                self.read_at(&state, len / PAGE * PAGE, &mut page)
                    .map_err(|error| self.io_error(error))?;
                page[len % PAGE..].fill(0);
                state.pages.insert(len / PAGE, page);
            }
            state.pages.retain(|index, _| *index < len.div_ceil(PAGE));
            state.parent_limit = state.parent_limit.min(len);
        }
        state.length = len;
        drop(state);
        c.complete(0);
        self.capture(Ok(c))
    }
}

pub(crate) struct CandidateWriter {
    _directory: tempfile::TempDir,
    database: Arc<Database>,
    connection: Arc<turso_core::Connection>,
    storage: Arc<Overlay>,
}

pub(crate) struct FrozenImage {
    parent: Parent,
    pub(crate) length: usize,
    #[cfg(test)]
    pub(crate) pages_compared: usize,
    pub(crate) changed: BTreeMap<u64, Vec<u8>>,
}

impl FrozenImage {
    pub(crate) fn materialize(&self) -> Result<Vec<u8>, crate::RuntimeError> {
        let mut bytes = vec![0; self.length];
        match &self.parent {
            Parent::Bytes(parent) => {
                let copied = parent.len().min(self.length);
                bytes[..copied].copy_from_slice(&parent[..copied]);
            }
            Parent::Pages { source, handle } => {
                let parent_length = usize::try_from(source.length())
                    .map_err(|_| crate::RuntimeError::Corrupt("parent image too large".into()))?;
                for index in 0..self.length.min(parent_length).div_ceil(PAGE) {
                    let mut page = vec![0; PAGE];
                    std::thread::scope(|scope| {
                        scope
                            .spawn(|| {
                                handle.block_on(source.read_page((index + 1) as u64, &mut page))
                            })
                            .join()
                            .map_err(|_| {
                                crate::RuntimeError::Turso("parent page worker panicked".into())
                            })?
                    })?;
                    let start = index * PAGE;
                    let count = PAGE.min(self.length - start);
                    bytes[start..start + count].copy_from_slice(&page[..count]);
                }
            }
        }
        for (number, page) in &self.changed {
            let start = (usize::try_from(*number).expect("frozen page index originated as usize")
                - 1)
                * PAGE;
            let count = PAGE.min(self.length.saturating_sub(start));
            bytes[start..start + count].copy_from_slice(&page[..count]);
        }
        Ok(bytes)
    }
}

impl CandidateWriter {
    pub(crate) fn new(parent: Arc<[u8]>, schema: Option<&str>) -> Result<Self> {
        Self::open(Arc::new(Overlay::new(parent)), schema)
    }

    pub(crate) fn from_pages(
        source: Arc<dyn crate::reader_engine::PageSource>,
        handle: tokio::runtime::Handle,
    ) -> Result<Self> {
        Self::open(Arc::new(Overlay::from_pages(source, handle)?), None)
    }

    fn open(storage: Arc<Overlay>, schema: Option<&str>) -> Result<Self> {
        #[cfg(feature = "write-latency-qualification")]
        let _phase = crate::write_latency_qualification::phase("turso_open");
        let directory = tempfile::tempdir().map_err(|e| turso_core::io_error(e, "tempdir"))?;
        let path = directory.path().join("candidate.sqlite3");
        let database = Database::open(
            Arc::new(PlatformIO::new()?),
            path.to_str().unwrap(),
            storage.clone(),
        )?;
        let connection = database.connect()?;
        connection.execute("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;")?;
        if let Some(schema) = schema {
            connection.execute(schema)?;
        }
        connection.execute("BEGIN;")?;
        Ok(Self {
            _directory: directory,
            database,
            connection,
            storage,
        })
    }

    pub(crate) fn sql(&self) -> crate::sql_writer::Writer<'_> {
        crate::sql_writer::Writer::Turso(&self.connection)
    }

    pub(crate) fn finish(self) -> Result<FrozenImage, crate::RuntimeError> {
        self.connection.execute("COMMIT;")?;
        self.connection.checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })?;
        self.close_and_freeze()
    }

    fn close_and_freeze(self) -> Result<FrozenImage, crate::RuntimeError> {
        self.connection.close()?;
        drop(self.connection);
        drop(self.database);
        self.storage.take_failure()?;
        if self.storage.failed.load(Ordering::Acquire) {
            return Err(
                LimboError::InternalError("candidate storage failed before freeze".into()).into(),
            );
        }
        let state = self.storage.state.lock().unwrap();
        if !state.length.is_multiple_of(PAGE) {
            return Err(LimboError::InvalidArgument("unaligned frozen image".into()).into());
        }
        let mut touched: std::collections::BTreeSet<usize> = state.pages.keys().copied().collect();
        touched.extend(state.parent_limit / PAGE..state.length.div_ceil(PAGE));
        let mut changed = BTreeMap::new();
        #[cfg(test)]
        let pages_compared = touched.len();
        for index in touched {
            crate::runtime::failpoint("during_changed_page_freeze");
            if index * PAGE >= state.length {
                continue;
            }
            let mut bytes = vec![0; PAGE];
            self.storage.read_at(&state, index * PAGE, &mut bytes)?;
            if self.storage.parent_page(index)?.as_deref() != Some(bytes.as_slice()) {
                changed.insert(index as u64 + 1, bytes);
            }
        }
        Ok(FrozenImage {
            parent: self.storage.parent.clone(),
            length: state.length,
            #[cfg(test)]
            pages_compared,
            changed,
        })
    }
}

#[cfg(test)]
mod tests;
