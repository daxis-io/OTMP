# DataFusion reader and authenticated metadata access

`otmp-datafusion` binds application SQL to one OTMP metadata generation and
selected snapshot. DataFusion 55.0.0 plans SQL, prunes typed statistics, and
executes native Parquet scans. OTMP resolves schema and immutable file membership;
read-only Turso 0.7.2 executes internal relational metadata queries on blocking
workers. Stock SQLite and the existing materialized pins remain the exhaustive
validation and replay oracle.

```text
SQL → DataFusion planning → pinned OTMP provider
    → authenticated metadata pages → bounded file batches and typed pruning
    → native Parquet scans → Arrow record batches
```

The division follows the [DuckLake scanner](https://github.com/duckdb/ducklake/blob/0f569a6bba3028637c5020ccd31af24739e3fb42/src/storage/ducklake_scan.cpp#L217)
and [DataFusion TableProvider integration](https://github.com/datafusion-contrib/datafusion-ducklake/blob/1383c2537aa69da314809ec747656517ecb4cc21/src/table.rs#L3924).
The crate does not depend on DuckLake or adopt its catalog schema.

## Open and query

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use otmp::{LocalObjectStore, MetadataSelection, ReaderOptions, SnapshotSelection, Table};
use otmp_datafusion::{OtmpTableProvider, ProviderOptions};

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let table = Table::new(LocalObjectStore::new("table")?);
    let provider = OtmpTableProvider::open(
        &table,
        MetadataSelection::Current,
        SnapshotSelection::Ref("main".into()),
        ReaderOptions::default(),
        ProviderOptions::default(),
    ).await?;
    let context = SessionContext::new();
    context.register_table("t", Arc::new(provider))?;
    let batches = context.sql("SELECT * FROM t LIMIT 20").await?.collect().await?;
    Ok(())
}
```

The runnable equivalent is:

```sh
cargo run -p otmp-datafusion --example query -- TABLE 'SELECT count(*) FROM t'
```

The constructor accepts the existing metadata-version and snapshot selectors,
including branches, tags, explicit snapshot IDs, and sequence numbers. Opening
retains a generation, the selected metadata version's `current_schema_id`, and
snapshot descriptors. It does not enumerate snapshot files. Commit envelopes
are authenticated in full and bounded by the metadata budget; their size can
affect registration cost. A historical selection keeps current HEAD coordinates
in `reader().anchor()` separately from `reader().coordinates()`.

Planning, repeated execution, and self-joins retain the same provider pin.
Open a new provider to observe publication or a metadata-only schema addition.
Clones of the same `Table` share a cache scoped to that storage context and
matching reader options. Independent table/storage instances have independent
caches and engine identities.

## File planning and schemas

Each `scan()` asks for at most 256 file descriptors and only the metric fields
referenced by filters. Branches use their live membership projection. Tags and
historical snapshots follow immutable append-only ancestry. `FileBatch` returns
an opaque continuation cursor from the raw metadata batch. Always follow that
cursor, including after an empty batch; cursors cannot cross reader pins.

CBOR metrics become typed Arrow statistics for DataFusion's pruning machinery.
Unknown, missing, reversed, incompatible, and NaN-affected bounds retain files.
Pushdown is `Inexact`; residual row filters remain in the plan. Filtered scans do
not push down a limit that could stop before enough matching rows are found.
Caller-declared record counts are never exact provider statistics and cannot
replace a data scan with a metadata-only aggregate. Invalid SQL column references
are planning errors; malformed authoritative metadata is an integrity error.
Set `ProviderOptions::file_pruning` to false for an unpruned native scan over the
same selected membership during qualification.

Every file retains its immutable schema ID. Physical Parquet field IDs take
precedence. Fields without IDs bind names through that file's recorded schema.
Arrow/Parquet may canonicalize collection wrapper names: after binding the
container, the unique list-element and map-key/value roles use their structural
positions, with explicit child IDs still required to match. Ordinary struct
children keep the recorded-name/ID rules. Missing optional fields use the query
schema's initial default or null. Missing required fields, ambiguous IDs/names,
and unsupported conversions fail during footer inspection, even if SQL projects
the offending field away.

Arrow mappings include structs, lists, maps, binary/fixed/UUID fields, temporal
types, Decimal128 and Decimal256. Decimal precision above 76 fails at provider
construction. This delivery adds no schema-evolution operations.

## Authentication and budgets

The range interface returns exact bounds, total length, and an opaque object
version. Local, in-memory, and S3 adapters reject short, oversized, ignored-range,
and inconsistent-version responses. Unsupported range operations return an
explicit error. The read-only DataFusion bridge uses object_store 0.13.2;
the independent S3 adapter retains object_store 0.14.1.

An override page follows the authenticated page-map path and must agree with
the pack index's offset, codec, lengths, and page digest. A base page follows
the checkpoint hash index and is checked against its raw SHA-256. Missing
extended pages fail. Index-free fixtures remain usable by materialized pins;
the async reader returns `OTMP_AUTHENTICATED_RANGES_UNAVAILABLE` for them.

| Resource | Default | Accounting and failure |
|---|---:|---|
| Shared metadata budget | 64 MiB | Cached ranges, decoded nodes, schemas, active buffers, engine working reservations and structural references |
| Turso page cache | 4 MiB per engine | Pager cache configured directly, separate from the shared metadata budget |
| Checkpoint fetch window | 64 KiB | Aligned, version-pinned ranges |
| Concurrent metadata I/O | 8 | Permits and pending reservations release on cancellation |
| SQLite record payload | 1 MiB | Configurable `maximum_record_bytes`, checked before overflow-record allocation |
| Retained scan descriptors | 64 MiB per scan | Also reserved through the session's DataFusion memory pool and held for the physical plan's lifetime |
| Shared Parquet footer cache | 64 MiB per provider | Admission before decode; active leases stay charged after eviction |

Turso page reads preserve the logical bytes and reject writes, truncation and
sync. A structural page validator tracks B-tree and overflow references, rejects
cycles and invalid bounds, and checks oversized records at the first overflow
read. A large unselected historical record can share a leaf with a selected
small row. Engine operations run serially off async executor threads; dropping
a query cancels its pending storage work.

Resource exhaustion is an explicit error. The budgets bound retained objects
and supported internal query buffers; they are not a promise that total process
RSS equals their sum. DataFusion, Arrow, native engine bookkeeping, executable
pages, and allocator overhead contribute to RSS.

The async API verifies selected linkage/identity/schema/references and every
consumed page. `verify()` and `verify_history()` additionally traverse all index
objects and validate whole trees, whole checkpoint/pack hashes, global relational
invariants, and semantic replay. Normal Parquet scans read selected immutable
files by consistent ranges; full user-file SHA-256 remains exhaustive verification.

## Qualification

The [local evidence](qualification/2026-09-07-reader-local-performance.md)
separates registration, metadata planning, and Parquet execution. Bounded batches
can still visit every file's metadata; this is not indexed or sublinear metric
planning. The existing [qualification matrix](QUALIFICATION.md) includes the new
tests while retaining full-image fixtures, the SQLite oracle, and COW upload
measurements. S3 range tests use a deterministic local HTTP endpoint. AWS/R2 and
Turso Cloud qualification remain separate and were not performed.

This slice supports the current append-only, unpartitioned runtime. Indexed
metric pruning, automatic per-statement refresh, SQL writes, deletes, partition
evolution, GC and MVCC remain later capabilities. Draft identifiers are unchanged.
