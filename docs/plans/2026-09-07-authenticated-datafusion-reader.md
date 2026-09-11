# Authenticated OTMP DataFusion reader implementation plan

> Use the executing-plans workflow to carry these tasks through implementation and verification. The user supplied and approved the product contract; this document records file-level execution choices.

Goal: retain one authenticated metadata generation and selected snapshot for a native DataFusion 55.0.0 table provider, with bounded on-demand metadata access and exhaustive SQLite validation preserved.

Architecture: storage provides version-consistent bounded ranges; OTMP authenticates map and checkpoint pages; read-only Turso executes internal relational queries on a worker. DataFusion owns SQL, pruning, Parquet execution, and residual predicates. The integration has no DuckLake dependency.

Technology: Rust 1.95.0, turso_core 0.7.2, existing object_store 0.14.1 for S3, DataFusion exactly 55.0.0 and its independent storage version.

## Landing prerequisite

- Reviewed 7415d564 against 679711712; implementation unchanged, review evidence committed as b0772016.
- PR https://github.com/daxis-io/OTMP/pull/10.
- Local 136/136 tests, all other qualification passed.
- All six PR CI jobs in run 34076903750 and Audit 34076903754 passed against b0772016. Squash merged as `78c3311a5d883fdda3542ff6e5a918bb24fee3ca`; the resulting tree matched the reviewed candidate. All six main CI jobs in run 34077325293 passed before reader work began.
- Retained /private/tmp/otmp-turso-cow and its target/evidence. Created /private/tmp/otmp-datafusion from the verified squash commit. The original checkout remains clean at the COW squash commit.

## 1. Authenticated formats and range storage

Files: otmp-protocol/src/{objects,cbor,page_map,page_pack,checkpoint_index,lib}.rs;
otmp-protocol/tests/{checkpoint_index,page_pack}.rs;
otmp/src/{storage,checkpoint_index,physical,runtime}.rs;
otmp-s3/src/lib.rs; storage/range and S3 contract tests; draft spec.

1. Write range contract tests first for local and memory stores: exact bytes and total length, invalid/empty/overflow/EOF bounds, version change, no full-read fallback. Implement metadata and read-range methods with explicit Unsupported defaults.
2. Add deterministic HTTP range cases to the existing S3 harness: ignored ranges, wrong Content-Range, short/oversized bodies, missing or inconsistent version, conditional version/ETag selection. Preserve bounded buffering and object_store 0.14.1.
3. Define optional checkpoint index binding in MetadataImage. Root binds checkpoint SHA/length, page geometry and root object/height. Nodes use deterministic CBOR, raw 32-byte hashes, contiguous intervals, height zero leaves, fanout 128, serialized size <=1 MiB.
4. Write byte-exact, truncation, noncanonical, overflow, empty, malformed coverage and binding tests, then encode/decode validators.
5. Refactor page-pack header/index parsing to accept bounded header/index bytes plus full declared object length. Retain full-object decoder behavior.
6. Generate index objects only from validated checkpoint bytes in genesis/fallback. Reuse their immutable root for incremental children. Existing index-free fixtures remain byte-identical and valid for materialized reads.
7. Extend exhaustive resolve_generation traversal through index nodes, checking every page against checkpoint bytes and counting index objects through the existing verified-object cache.
8. Add indexed fixtures alongside the existing fixtures and independent reconstruction/checking. Run protocol/native fixture tests, formatting, Clippy and WASM. Commit this slice.

## 2. Lazy metadata engine and validation

Files: otmp/src/reader/{mod,cache,pages,engine,metadata}.rs;
otmp/src/{lib,error,runtime}.rs; otmp/tests/{reader_integrity,reader_metadata,reader_engine}.rs.

1. Build a bounded cache and RAII accounting that includes raw bytes, decoded nodes, active entries and in-flight allocations. Scope cache sharing to a fixed store/table context; validate URI/hash/length reference agreement on every hit. Default 64 MiB, 64 KiB checkpoint windows and eight in-flight reads.
2. Implement authenticated page-map path lookup, pack-index agreement and bounded decompression, and checkpoint page-index lookup. Missing pages and index-free generations are explicit errors. Range responses are validated again at this boundary.
3. Qualify a read-only Turso DatabaseStorage adapter before exposing metadata queries: exact raw header/page bytes, writes/truncation rejected, unique database identities, async transport failure propagation, and close without published mutations. Open with ReadOnly flags and a private engine identity; serialize engine work outside async executor threads. Configure 4 MiB engine page cache.
4. Implement cancellation for worker queries and pending storage work, with reservations released on cancellation/failure.
5. Add async metadata selection that reads HEAD once and follows explicit generation parents. Validate feature support, commit linkage/state hash, generation edges, image identity, checkpoint identity, selected schema, ref and snapshot descriptors without file enumeration. Retain HEAD anchor separately from historical coordinates.
6. Read the current schema of the selected metadata version, including normalized fields and identifier rows; validate against the existing recursive schema contract. Read each file's immutable schema as needed.
7. Implement FileBatch with explicit continuation from the unfiltered batch, at most 256 files. Use otmp_ref_live_files keyset ordering for branches; use monotonically decreasing immutable snapshot ancestry for tags/historical snapshots. Bound rows and metrics buffers and validate authoritative descriptors. Expose only typed queries, no application SQL through Turso.
8. Qualify old pins during concurrent publication, explicit reopen, metadata-only schema additions, branches/tags/history, empty snapshots, cursor progress, corrupt references, cancellation and budget failure. Commit this slice.

## 3. DataFusion provider and planning

Files: otmp-datafusion/{Cargo.toml,src/{lib,schema,pruning,storage,footer,plan}.rs,examples/query.rs,tests/...}; workspace Cargo files.

1. Add an independent crate pinned to DataFusion =55.0.0 with sql/parquet features and a public async OtmpTableProvider constructor taking an OTMP Table, existing selectors and options.
2. Map all representable OTMP types, including nested structs/lists/maps, temporal values, fixed binary/UUID and Arrow-supported decimals. Reject excessive decimal precision and unsupported types at construction.
3. Build schema adapters using stable IDs. Physical Parquet IDs take precedence; ID-free physical fields resolve through the recorded file schema. Validate ambiguity/required fields/conversions. Fill newly absent optional fields using initial defaults/null. Preserve nested nulls and IDs. Reuse DataFusion's native Parquet expression-adapter seam.
4. During scan, decode typed CBOR statistics into DataFusion PruningStatistics in batches. Missing/unusable optimization information retains files. Handle floats with NaNs conservatively. Avoid raw CBOR SQL comparisons and a second predicate translator. Invalid references remain logical planning errors.
5. Report Inexact filter pushdown, keep residual filters, avoid unsafe limit pushdown, and never expose caller-declared row counts as exact provider/scan statistics.
6. Build native Parquet plans from explicit survivors. Read-only storage bridge uses consistent bounded ranges and an independent object_store version. A shared bounded footer cache supplies schema inspection and scanner metadata.
7. Reserve retained descriptors and planning allocations through DataFusion's MemoryPool and a configurable 64 MiB planning limit; retain reservations for plan lifetime and return resource exhaustion on overflow.
8. Add a runnable SQL example and real Parquet fixtures. Compare optimized SQL results with an unpruned provider over the same selected membership. Commit this slice.

## 4. Integrated qualification and documentation

1. Test projection, filters, joins/self-joins, aggregates, limits, emptiness, references, historical metadata, nested fields, defaults and additive schemas.
2. Add randomized pruning parity; include missing metrics, null/NaN/infinities, decimals/temporal bounds, unsupported expressions, projected-away filter columns and a fully pruned intermediate batch.
3. Exercise retained providers/repeated execution/concurrent publication, explicit refresh, cancellation, transport failure and memory exhaustion.
4. Exercise index/map/pack corruption, bounds, decompression overruns, cached reference conflicts and every malformed range response.
5. Generate a deterministic metadata image >=128 MiB. Measure registration, scan planning and Parquet execution separately. Cold registration must fetch <10% image bytes with zero image materialization; enforce cache and planning limits. Report subprocess peak RSS and bytes, requests, pages, candidate/pruned/opened file counts, cache hits and latency. Document that bounded pruning may read every file's metadata.
6. Run every existing qualification command and new reader/S3 range tests; preserve SQLite oracle/full-image fixtures/COW upload evidence. Record exact local versus CI evidence; live provider qualification remains not run.

## Source pointers inspected

- Published registry source: turso_core 0.7.2 storage/database.rs, lib.rs open_with_flags, statement.rs step/rows, connection.rs cache and interrupt APIs.
- Published DataFusion 55 source: TableProvider; ParquetSource; PhysicalExprAdapterFactory; FileScanConfigBuilder; PruningStatistics/PruningPredicate; native Parquet reader factory.
- Exact prior art retrieved to /private/tmp/otmp-prior-art-{table.rs,metadata.rs,scan.cpp}; references match the user's commits. Prior-art pagination defaults are not a bounded OTMP contract; continuation must survive filtering.
