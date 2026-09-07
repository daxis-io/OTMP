//! Serialized, read-only Turso access to authenticated metadata pages.
//!
//! `turso_core` exposes a synchronous page-storage callback.  The OTMP source
//! is asynchronous, so every engine operation runs on Tokio's blocking pool;
//! only there may the storage callback enter the runtime to await a page.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::Notify;
use turso_core::{
    Buffer, Completion, Database, DatabaseOpts, DatabaseStorage, IOContext, LimboError, OpenFlags,
    PlatformIO, io::FileSyncType,
};
use uuid::Uuid;

use crate::RuntimeError;

const PAGE_SIZE: usize = 4096;
pub(crate) const DEFAULT_PAGE_CACHE_BYTES: usize = 4 * 1024 * 1024;

/// An authenticated logical `SQLite` image.  Page one may be requested with a
/// short output buffer for the `SQLite` header; implementations still validate
/// the full page before returning that prefix.
#[async_trait]
pub(crate) trait PageSource: Send + Sync + 'static {
    fn length(&self) -> u64;
    fn maximum_record_bytes(&self) -> usize {
        1024 * 1024
    }
    fn reserve(&self, bytes: usize) -> Result<crate::reader::cache::Reservation, RuntimeError>;

    async fn read_page(&self, page_number: u64, output: &mut [u8]) -> Result<(), RuntimeError>;
}

#[derive(Default)]
struct Cancellation {
    cancelled: AtomicBool,
    notify: Notify,
}

impl Cancellation {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        // `notify_one` retains a permit when the range future has not reached
        // its first poll yet, closing the cancellation-before-select race.
        self.notify.notify_one();
    }

    fn check(&self) -> Result<(), RuntimeError> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(RuntimeError::Cancelled)
        } else {
            Ok(())
        }
    }
}

struct CancelOnDrop(Arc<Cancellation>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

struct ReadOnlyStorage {
    source: Arc<dyn PageSource>,
    handle: tokio::runtime::Handle,
    active: Mutex<Arc<Cancellation>>,
    failure: Mutex<Option<RuntimeError>>,
    validator: Mutex<crate::reader_engine_pages::PageValidator>,
    roles: Mutex<Vec<crate::reader::cache::Reservation>>,
}

impl ReadOnlyStorage {
    fn new(source: Arc<dyn PageSource>, handle: tokio::runtime::Handle) -> Self {
        Self {
            validator: Mutex::new(crate::reader_engine_pages::PageValidator::new(
                source.length(),
                source.maximum_record_bytes(),
                usize::MAX,
            )),
            roles: Mutex::new(Vec::new()),
            source,
            handle,
            active: Mutex::new(Arc::new(Cancellation::default())),
            failure: Mutex::new(None),
        }
    }

    fn set_active(&self, cancellation: Arc<Cancellation>) -> Result<(), RuntimeError> {
        *self
            .active
            .lock()
            .map_err(|_| RuntimeError::Turso("reader cancellation lock poisoned".into()))? =
            cancellation;
        Ok(())
    }

    fn active(&self) -> Result<Arc<Cancellation>, RuntimeError> {
        self.active
            .lock()
            .map(|cancellation| cancellation.clone())
            .map_err(|_| RuntimeError::Turso("reader cancellation lock poisoned".into()))
    }

    fn remember(&self, error: RuntimeError) {
        if let Ok(mut failure) = self.failure.lock()
            && failure.is_none()
        {
            *failure = Some(error);
        }
    }

    fn take_failure(&self) -> Result<(), RuntimeError> {
        self.failure
            .lock()
            .map_err(|_| RuntimeError::Turso("reader failure lock poisoned".into()))?
            .take()
            .map_or(Ok(()), Err)
    }

    fn read(&self, page_number: u64, output: &mut [u8]) -> Result<(), RuntimeError> {
        if output.is_empty() || output.len() > PAGE_SIZE || page_number == 0 {
            return Err(RuntimeError::Corrupt(
                "invalid SQLite page read requested by Turso".into(),
            ));
        }
        let cancellation = self.active()?;
        cancellation.check()?;
        let cancelled = cancellation.clone();
        let source = self.source.clone();
        let mut page = [0_u8; PAGE_SIZE];
        self.handle.block_on(async {
            let notified = cancelled.notify.notified();
            tokio::pin!(notified);
            if cancelled.check().is_err() {
                return Err(RuntimeError::Cancelled);
            }
            tokio::select! {
                biased;
                () = &mut notified => Err(RuntimeError::Cancelled),
                result = source.read_page(page_number, &mut page) => result,
            }
        })?;
        // A single SQLite page cannot register more than two references per
        // cell. Admission precedes the validator's B-tree map allocations.
        let mut transient = self.source.reserve(PAGE_SIZE * 128)?;
        let mut validator = self
            .validator
            .lock()
            .map_err(|_| RuntimeError::Turso("page validator lock poisoned".into()))?;
        let before = validator.role_count();
        let result = validator.validate(page_number, &page);
        let added = validator.role_count() - before;
        // A malformed page can fail after registering a bounded prefix. Its
        // prefix stays charged, so retries cannot bypass memory admission.
        if added != 0 {
            transient.shrink_to(added * 128 + 64);
            self.roles
                .lock()
                .map_err(|_| RuntimeError::Turso("page role lock poisoned".into()))?
                .push(transient);
        }
        result?;
        output.copy_from_slice(&page[..output.len()]);
        Ok(())
    }

    fn io_error(&self, error: RuntimeError) -> LimboError {
        self.remember(error);
        turso_core::io_error(
            std::io::Error::other("authenticated page source failed"),
            "read_page",
        )
    }
}

impl DatabaseStorage for ReadOnlyStorage {
    fn read_header(&self, completion: Completion) -> turso_core::Result<Completion> {
        let output = completion.as_read().buf().as_mut_slice();
        match self.read(1, output) {
            Ok(()) => {
                completion.complete(
                    i32::try_from(output.len()).map_err(|_| LimboError::IntegerOverflow)?,
                );
                Ok(completion)
            }
            Err(error) => Err(self.io_error(error)),
        }
    }

    fn read_page(
        &self,
        page_idx: usize,
        _: &IOContext,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        let output = completion.as_read().buf().as_mut_slice();
        match self.read(
            u64::try_from(page_idx).map_err(|_| LimboError::IntegerOverflow)?,
            output,
        ) {
            Ok(()) => {
                completion.complete(
                    i32::try_from(output.len()).map_err(|_| LimboError::IntegerOverflow)?,
                );
                Ok(completion)
            }
            Err(error) => Err(self.io_error(error)),
        }
    }

    fn write_page(
        &self,
        _: usize,
        _: Arc<Buffer>,
        _: &IOContext,
        _: Completion,
    ) -> turso_core::Result<Completion> {
        Err(LimboError::ReadOnly)
    }

    fn write_pages(
        &self,
        _: usize,
        _: usize,
        _: Vec<Arc<Buffer>>,
        _: &IOContext,
        _: Completion,
    ) -> turso_core::Result<Completion> {
        Err(LimboError::ReadOnly)
    }

    fn sync(&self, _: Completion, _: FileSyncType) -> turso_core::Result<Completion> {
        Err(LimboError::ReadOnly)
    }

    fn size(&self) -> turso_core::Result<u64> {
        Ok(self.source.length())
    }

    fn truncate(&self, _: usize, _: Completion) -> turso_core::Result<Completion> {
        Err(LimboError::ReadOnly)
    }
}

struct Worker {
    // A unique path keeps Turso's process-wide identity registry from sharing
    // connections between independently pinned OTMP metadata images.
    _identity: tempfile::TempDir,
    _database: Arc<Database>,
    connection: Arc<turso_core::Connection>,
    storage: Arc<ReadOnlyStorage>,
    _page_cache_bytes: usize,
}

/// A cloneable handle to one pinned, read-only metadata engine.
#[derive(Clone)]
pub(crate) struct Engine {
    worker: Arc<Mutex<Worker>>,
}

impl Engine {
    pub(crate) async fn open(
        source: Arc<dyn PageSource>,
        page_cache_bytes: usize,
    ) -> Result<Self, RuntimeError> {
        if page_cache_bytes == 0 {
            return Err(RuntimeError::ResourceExhausted(
                "Turso page cache budget must be non-zero".into(),
            ));
        }
        let cache_kib = page_cache_bytes / 1024;
        if !page_cache_bytes.is_multiple_of(1024) || cache_kib == 0 {
            return Err(RuntimeError::ResourceExhausted(
                "Turso page cache budget must be a whole number of KiB".into(),
            ));
        }
        let cache_kib = i32::try_from(cache_kib).map_err(|_| {
            RuntimeError::ResourceExhausted("Turso page cache budget is too large".into())
        })?;
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|error| RuntimeError::Turso(format!("Tokio runtime is required: {error}")))?;
        let cancellation = Arc::new(Cancellation::default());
        let guard = CancelOnDrop(cancellation.clone());
        let opened = tokio::task::spawn_blocking(move || {
            let _working =
                source.reserve(source.maximum_record_bytes().checked_mul(8).ok_or_else(
                    || RuntimeError::ResourceExhausted("engine working budget overflow".into()),
                )?)?;
            let identity = tempfile::tempdir()?;
            let path = identity
                .path()
                .join(format!("otmp-reader-{}.sqlite3", Uuid::now_v7()));
            let storage = Arc::new(ReadOnlyStorage::new(source, handle));
            storage.set_active(cancellation)?;
            let database = match Database::open_with_flags(
                Arc::new(PlatformIO::new()?),
                path.to_str()
                    .ok_or_else(|| RuntimeError::Turso("invalid temporary engine path".into()))?,
                storage.clone(),
                OpenFlags::ReadOnly,
                DatabaseOpts::new(),
                None,
                None,
            ) {
                Ok(database) => database,
                Err(error) => {
                    storage.take_failure()?;
                    return Err(error.into());
                }
            };
            let connection = match database.connect() {
                Ok(connection) => connection,
                Err(error) => {
                    storage.take_failure()?;
                    return Err(error.into());
                }
            };
            // Turso's pragma translator calls `Pager::change_page_cache_size`,
            // unlike Connection::set_cache_size alone, which only changes the
            // connection value used by sort buffers.  A negative SQLite value
            // declares KiB rather than pages, so this is exactly the requested
            // byte budget for the 4 KiB OTMP metadata images.
            connection
                .prepare(format!("PRAGMA cache_size=-{cache_kib}").as_str())?
                .run_ignore_rows()?;
            storage.take_failure()?;
            Ok::<_, RuntimeError>(Worker {
                _identity: identity,
                _database: database,
                connection,
                storage,
                _page_cache_bytes: page_cache_bytes,
            })
        })
        .await
        .map_err(|error| RuntimeError::Turso(format!("reader worker failed: {error}")))?;
        drop(guard);
        Ok(Self {
            worker: Arc::new(Mutex::new(opened?)),
        })
    }

    pub(crate) async fn query(
        &self,
        sql: &str,
        params: Vec<turso_core::Value>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Vec<Vec<turso_core::Value>>, RuntimeError> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(RuntimeError::ResourceExhausted(
                "query row and byte budgets must be non-zero".into(),
            ));
        }
        if !is_read_only_sql(sql) {
            return Err(RuntimeError::Turso(
                "reader engine accepts only SELECT, WITH, EXPLAIN, and PRAGMA queries".into(),
            ));
        }
        let worker = self.worker.clone();
        let sql = sql.to_owned();
        let cancellation = Arc::new(Cancellation::default());
        let guard = CancelOnDrop(cancellation.clone());
        let result = tokio::task::spawn_blocking(move || {
            let worker = worker
                .lock()
                .map_err(|_| RuntimeError::Turso("reader worker lock poisoned".into()))?;
            let working_bytes = worker
                .storage
                .source
                .maximum_record_bytes()
                .checked_mul(8)
                .and_then(|n| n.checked_add(max_bytes.checked_mul(2)?))
                .and_then(|n| n.checked_add(64 * 1024))
                .ok_or_else(|| {
                    RuntimeError::ResourceExhausted("metadata query working budget overflow".into())
                })?;
            let _working = worker.storage.source.reserve(working_bytes)?;
            worker.storage.set_active(cancellation.clone())?;
            cancellation.check()?;
            let mut statement = worker.connection.prepare(&sql)?;
            for (index, value) in params.into_iter().enumerate() {
                statement.bind_at(
                    NonZeroUsize::new(index + 1)
                        .ok_or_else(|| RuntimeError::Turso("parameter index overflow".into()))?,
                    value,
                )?;
            }
            let mut rows = Vec::new();
            let mut used = 0usize;
            let mut callback_failure = None;
            let statement_result = statement.run_with_row_callback(|row| {
                if let Err(error) = cancellation.check() {
                    callback_failure = Some(error);
                    return Err(LimboError::Interrupt);
                }
                if rows.len() == max_rows {
                    callback_failure = Some(RuntimeError::ResourceExhausted(
                        "metadata query row budget exhausted".into(),
                    ));
                    return Err(LimboError::TooBig);
                }
                let row_bytes = row
                    .get_values()
                    .map(|value| {
                        value_bytes(value).saturating_add(std::mem::size_of::<turso_core::Value>())
                    })
                    .sum::<usize>();
                let Some(next) = used.checked_add(row_bytes) else {
                    callback_failure = Some(RuntimeError::ResourceExhausted(
                        "metadata query byte budget overflowed".into(),
                    ));
                    return Err(LimboError::TooBig);
                };
                used = next;
                if used > max_bytes {
                    callback_failure = Some(RuntimeError::ResourceExhausted(
                        "metadata query byte budget exhausted".into(),
                    ));
                    return Err(LimboError::TooBig);
                }
                rows.push(row.get_values().cloned().collect());
                Ok(())
            });
            worker.storage.take_failure()?;
            if let Some(error) = callback_failure {
                return Err(error);
            }
            statement_result?;
            cancellation.check()?;
            Ok::<_, RuntimeError>(rows)
        })
        .await
        .map_err(|error| RuntimeError::Turso(format!("reader worker failed: {error}")))?;
        drop(guard);
        result
    }
}

fn is_read_only_sql(sql: &str) -> bool {
    matches!(
        sql.split_whitespace()
            .next()
            .map(str::to_ascii_uppercase)
            .as_deref(),
        Some("SELECT" | "WITH" | "EXPLAIN" | "PRAGMA")
    )
}

fn value_bytes(value: &turso_core::Value) -> usize {
    match value {
        turso_core::Value::Null => 0,
        turso_core::Value::Numeric(_) => std::mem::size_of::<i64>(),
        turso_core::Value::Text(value) => value.value.len(),
        turso_core::Value::Blob(value) => value.len(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct FixtureSource {
        bytes: Arc<[u8]>,
        reads: AtomicUsize,
        entered: AtomicUsize,
        fail: AtomicBool,
        wait: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
    }

    #[async_trait]
    impl PageSource for FixtureSource {
        fn length(&self) -> u64 {
            self.bytes.len() as u64
        }
        fn reserve(&self, bytes: usize) -> Result<crate::reader::cache::Reservation, RuntimeError> {
            Ok(crate::reader::cache::Reservation::for_test(bytes))
        }

        async fn read_page(&self, page_number: u64, output: &mut [u8]) -> Result<(), RuntimeError> {
            if self.fail.load(Ordering::Acquire) {
                return Err(RuntimeError::Corrupt(
                    "injected page transport failure".into(),
                ));
            }
            self.entered.fetch_add(1, Ordering::Release);
            let wait = self.wait.lock().unwrap().clone();
            if let Some((started, wait)) = wait {
                started.notify_one();
                wait.notified().await;
            }
            let start = usize::try_from(page_number - 1)
                .ok()
                .and_then(|page| page.checked_mul(PAGE_SIZE))
                .ok_or_else(|| RuntimeError::Corrupt("page offset overflow".into()))?;
            let end = start
                .checked_add(output.len())
                .ok_or_else(|| RuntimeError::Corrupt("page range overflow".into()))?;
            output.copy_from_slice(
                self.bytes
                    .get(start..end)
                    .ok_or_else(|| RuntimeError::Corrupt("short fixture page read".into()))?,
            );
            self.reads.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn fixture() -> Arc<[u8]> {
        std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../conformance/tables/genesis/_otmp/checkpoints/0/01a06fa0-2d19-73b2-9adc-d660f9ff29a1.sqlite3"),
        )
        .unwrap()
        .into()
    }

    fn source(bytes: Arc<[u8]>) -> Arc<FixtureSource> {
        Arc::new(FixtureSource {
            bytes,
            reads: AtomicUsize::new(0),
            entered: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            wait: Mutex::new(None),
        })
    }

    #[tokio::test]
    async fn exact_fixture_bytes_produce_the_published_metadata_rows() {
        let source = source(fixture());
        let engine = Engine::open(source.clone(), DEFAULT_PAGE_CACHE_BYTES)
            .await
            .unwrap();
        let rows = engine
            .query("SELECT current_schema_id FROM otmp_meta", vec![], 1, 64)
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![turso_core::Value::Numeric(
                turso_core::Numeric::Integer(1)
            )]]
        );
        assert!(source.reads.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn mutation_is_rejected_without_changing_the_published_source() {
        let source = source(fixture());
        let engine = Engine::open(source.clone(), DEFAULT_PAGE_CACHE_BYTES)
            .await
            .unwrap();
        let result = engine.query("DELETE FROM otmp_meta", vec![], 1, 64).await;
        assert!(matches!(result, Err(RuntimeError::Turso(_))));
        assert_eq!(source.bytes, fixture());
    }

    #[tokio::test]
    async fn isolated_engines_do_not_share_a_database_identity() {
        let first = source(fixture());
        let second = source(fixture());
        let first_engine = Engine::open(first, DEFAULT_PAGE_CACHE_BYTES).await.unwrap();
        let second_engine = Engine::open(second, DEFAULT_PAGE_CACHE_BYTES)
            .await
            .unwrap();
        let first_rows = first_engine
            .query("SELECT current_schema_id FROM otmp_meta", vec![], 1, 64)
            .await
            .unwrap();
        let second_rows = second_engine
            .query("SELECT current_schema_id FROM otmp_meta", vec![], 1, 64)
            .await
            .unwrap();
        assert_eq!(first_rows, second_rows);
        assert!(!Arc::ptr_eq(&first_engine.worker, &second_engine.worker));
    }

    #[tokio::test]
    async fn source_failure_is_returned_as_its_original_runtime_error() {
        let source = source(fixture());
        source.fail.store(true, Ordering::Release);
        let result = Engine::open(source, DEFAULT_PAGE_CACHE_BYTES).await;
        assert!(
            matches!(result, Err(RuntimeError::Corrupt(message)) if message == "injected page transport failure")
        );
    }

    #[tokio::test]
    async fn result_collection_enforces_row_and_byte_budgets() {
        let engine = Engine::open(source(fixture()), DEFAULT_PAGE_CACHE_BYTES)
            .await
            .unwrap();
        let rows = engine
            .query(
                "SELECT name FROM otmp_fields UNION ALL SELECT name FROM otmp_fields",
                vec![],
                1,
                4096,
            )
            .await;
        assert!(matches!(rows, Err(RuntimeError::ResourceExhausted(_))));
        let bytes = engine
            .query("SELECT name FROM otmp_fields", vec![], 100, 1)
            .await;
        assert!(matches!(bytes, Err(RuntimeError::ResourceExhausted(_))));
    }

    #[tokio::test]
    async fn pragma_sets_the_actual_four_mebibyte_pager_cache_budget() {
        let engine = Engine::open(source(fixture()), DEFAULT_PAGE_CACHE_BYTES)
            .await
            .unwrap();
        // `PRAGMA cache_size` readback is SQLite's header default, which is
        // deliberately distinct from the active connection/pager setting.
        // The setting below is the exact negative KiB value passed through
        // Turso's pragma translator to `Pager::change_page_cache_size`.
        assert_eq!(
            engine.worker.lock().unwrap().connection.get_cache_size(),
            -4096
        );
    }

    #[tokio::test]
    async fn dropping_a_query_cancels_the_inflight_page_read_and_releases_the_worker() {
        let source = source(fixture());
        let engine = Engine::open(source.clone(), DEFAULT_PAGE_CACHE_BYTES)
            .await
            .unwrap();
        let started = Arc::new(Notify::new());
        *source.wait.lock().unwrap() = Some((started.clone(), Arc::new(Notify::new())));
        source.entered.store(0, Ordering::Release);
        let cancelled = tokio::spawn({
            let engine = engine.clone();
            async move {
                engine
                    .query("SELECT current_schema_id FROM otmp_meta", vec![], 1, 64)
                    .await
            }
        });
        started.notified().await;
        assert!(source.entered.load(Ordering::Acquire) > 0);
        cancelled.abort();
        let _ = cancelled.await;
        *source.wait.lock().unwrap() = None;
        let rows = engine
            .query("SELECT current_schema_id FROM otmp_meta", vec![], 1, 64)
            .await
            .expect("cancelled range must release the serialized worker");
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn record_limit_precedes_materialization_and_does_not_reject_neighbor_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.sqlite3");
        let db = rusqlite::Connection::open(&path).unwrap();
        db.execute_batch(
            "PRAGMA page_size=4096; CREATE TABLE records(id INTEGER PRIMARY KEY, payload BLOB);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO records VALUES (1, ?1)",
            [vec![7_u8; 2 * 1024 * 1024]],
        )
        .unwrap();
        db.execute("INSERT INTO records VALUES (2, ?1)", [b"small".as_slice()])
            .unwrap();
        drop(db);
        let source = source(std::fs::read(path).unwrap().into());
        let engine = Engine::open(source.clone(), DEFAULT_PAGE_CACHE_BYTES)
            .await
            .unwrap();
        let small = engine
            .query("SELECT payload FROM records WHERE id=2", vec![], 1, 128)
            .await
            .unwrap();
        assert_eq!(
            small,
            vec![vec![turso_core::Value::Blob(b"small".to_vec())]]
        );
        let before = source.reads.load(Ordering::Relaxed);
        let too_large = engine
            .query(
                "SELECT payload FROM records WHERE id=1",
                vec![],
                1,
                4 * 1024 * 1024,
            )
            .await;
        assert!(matches!(too_large, Err(RuntimeError::ResourceExhausted(_))));
        assert!(
            source.reads.load(Ordering::Relaxed) - before < 8,
            "stop at the first overflow page"
        );
        assert_eq!(
            engine
                .query("SELECT payload FROM records WHERE id=2", vec![], 1, 128)
                .await
                .unwrap(),
            small
        );
    }

    #[tokio::test]
    async fn native_readonly_flags_reject_a_writing_pragma() {
        let source = source(fixture());
        let engine = Engine::open(source.clone(), DEFAULT_PAGE_CACHE_BYTES)
            .await
            .unwrap();
        assert!(
            engine
                .query("PRAGMA user_version=999", vec![], 1, 128)
                .await
                .is_err()
        );
        assert_eq!(source.bytes, fixture());
    }
}
