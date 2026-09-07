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
    length: usize,
    parent_limit: usize,
    pages: BTreeMap<usize, Vec<u8>>,
}

pub struct Overlay {
    parent: Arc<[u8]>,
    state: Mutex<State>,
    failed: AtomicBool,
    #[cfg(test)]
    pub fail_writes: AtomicBool,
}

impl Overlay {
    pub fn new(parent: Arc<[u8]>) -> Self {
        Self {
            state: Mutex::new(State {
                length: parent.len(),
                parent_limit: parent.len(),
                pages: BTreeMap::new(),
            }),
            parent,
            failed: AtomicBool::new(false),
            #[cfg(test)]
            fail_writes: AtomicBool::new(false),
        }
    }

    fn read_at(&self, state: &State, offset: usize, bytes: &mut [u8]) -> usize {
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
                bytes[copied..copied + from_parent]
                    .copy_from_slice(&self.parent[pos..pos + from_parent]);
            }
            copied += chunk;
        }
        count
    }

    fn read(&self, offset: usize, c: Completion) -> Result<Completion> {
        let state = self.state.lock().unwrap();
        let count = self.read_at(&state, offset, c.as_read().buf().as_mut_slice());
        drop(state);
        c.complete(i32::try_from(count).map_err(|_| LimboError::IntegerOverflow)?);
        Ok(c)
    }

    #[cfg(test)]
    pub fn export(&self) -> Vec<u8> {
        let state = self.state.lock().unwrap();
        let mut bytes = vec![0; state.length];
        self.read_at(&state, 0, &mut bytes);
        bytes
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
                self.read_at(&state, len / PAGE * PAGE, &mut page);
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
    parent: Arc<[u8]>,
    length: usize,
    #[cfg(test)]
    pub(crate) pages_compared: usize,
    pub(crate) changed: BTreeMap<u64, Vec<u8>>,
}

impl FrozenImage {
    pub(crate) fn materialize(&self) -> Vec<u8> {
        let mut bytes = vec![0; self.length];
        let copied = self.parent.len().min(self.length);
        bytes[..copied].copy_from_slice(&self.parent[..copied]);
        for (number, page) in &self.changed {
            let start = (usize::try_from(*number).expect("frozen page index originated as usize")
                - 1)
                * PAGE;
            let count = PAGE.min(self.length.saturating_sub(start));
            bytes[start..start + count].copy_from_slice(&page[..count]);
        }
        bytes
    }
}

impl CandidateWriter {
    pub(crate) fn new(parent: Arc<[u8]>, schema: Option<&str>) -> Result<Self> {
        let directory = tempfile::tempdir().map_err(|e| turso_core::io_error(e, "tempdir"))?;
        let path = directory.path().join("candidate.sqlite3");
        let storage = Arc::new(Overlay::new(parent));
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

    pub(crate) fn finish(self) -> Result<FrozenImage> {
        self.connection.execute("COMMIT;")?;
        self.connection.checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })?;
        self.close_and_freeze()
    }

    fn close_and_freeze(self) -> Result<FrozenImage> {
        self.connection.close()?;
        drop(self.connection);
        drop(self.database);
        if self.storage.failed.load(Ordering::Acquire) {
            return Err(LimboError::InternalError(
                "candidate storage failed before freeze".into(),
            ));
        }
        let state = self.storage.state.lock().unwrap();
        if !state.length.is_multiple_of(PAGE) {
            return Err(LimboError::InvalidArgument("unaligned frozen image".into()));
        }
        let mut touched: std::collections::BTreeSet<usize> = state.pages.keys().copied().collect();
        touched.extend(state.parent_limit / PAGE..state.length.div_ceil(PAGE));
        let mut changed = BTreeMap::new();
        #[cfg(test)]
        let pages_compared = touched.len();
        for index in touched {
            if index * PAGE >= state.length {
                continue;
            }
            let mut bytes = vec![0; PAGE];
            self.storage.read_at(&state, index * PAGE, &mut bytes);
            if self.storage.parent.get(index * PAGE..(index + 1) * PAGE) != Some(bytes.as_slice()) {
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
