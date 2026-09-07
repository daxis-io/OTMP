# Authenticated DataFusion reader local qualification

The final source candidate `8c708591d419c7fdafe967662be8cc1683fbe854` passed 214 workspace tests, with zero failures or
skips, on macOS 26.6 arm64 with Rust 1.95.0. The workload below qualified cold
registration against a metadata image larger than 128 MiB, followed by typed
file pruning and native DataFusion Parquet execution. These are local,
credential-free results. Live AWS, R2 and Turso Cloud were not exercised.

## Large metadata and real Parquet workload

`otmp/examples/generate_reader_fixture.rs` creates a coherent table through the
existing writer: genesis, 32,768 properties containing 4,096-byte values, then a
small metadata-only update. It validates the result with the materialized
SQLite-backed verifier. The large checkpoint is 288,923,648 bytes and its
checkpoint index contains 559 immutable objects. A copy receives sixteen real
Parquet files with 1,000 consecutive `id: int64` values per file and declared
typed bounds. The selected version is 3; its logical metadata image is
288,935,936 bytes (275.55 MiB).

The workload and values are deterministic. Table IDs, commit IDs and timestamps
come from normal writer publication, so independently generated packages are not
byte-identical fixtures. The checked-in indexed conformance package supplies
the separate byte-fixed format fixture.

The measured SQL was:

```sql
SELECT count(*) AS n, sum(id) AS total FROM t WHERE id >= 14000
```

| Measurement | Cold registration | Metadata and scan planning | Native Parquet execution |
|---|---:|---:|---:|
| Metadata bytes transferred | 421,914 | 24,576 | 0 |
| Metadata requests, including object metadata checks | 130 | 18 | 0 |
| Metadata pages consumed | 35 | 6 | 0 |
| Metadata cache hits | 131 | 18 | 0 |
| Files considered / pruned | 0 / 0 | 16 / 14 | — |
| Parquet bytes transferred | 0 | 1,956 | 18,580 |
| Parquet range requests | 0 | 10 | 2 |
| Files opened for data ranges | 0 | 0 | 2 |
| Footer cache hits during execution | — | — | 2 |
| Phase latency | 79 ms | 33 ms | 4 ms |

Cold registration transferred **0.146%** of the logical metadata image, below
the required 10%, without materializing the image. Planning still visited all
sixteen files' metadata. The pruning algorithm is bounded by batches; this
measurement does not demonstrate indexed or sublinear metadata planning.
Execution returned count **2,000** and sum **29,999,000**, as asserted by the
qualification driver.

The shared metadata budget peaked at 22,923,438 bytes (21.86 MiB) against its
64 MiB limit, including engine working reservations and active structural
references. Retained scan descriptors reserved 3,736 bytes in DataFusion's
memory pool against the 64 MiB per-scan budget. The footer cache peaked at
129,066 bytes against its 64 MiB limit. Turso's separate page cache used its
configured 4 MiB limit. Budget exhaustion and reservation release also have
deterministic tests; the workload alone is not the exhaustion test.

Darwin `/usr/bin/time -l` measured a subprocess peak RSS of **82,067,456 bytes
(78.27 MiB)** and wall time of **21.60 seconds**. Whole-process wall time includes
work outside the three phase timers, including startup and teardown; it must
not be reported as their sum. RSS includes native libraries, executable pages,
allocator overhead and other engine allocations outside the explicit caches.
The [JSON artifact](2026-09-07-reader-local-performance.json) preserves the
phase counters, process measurements, source commit and retained log hashes.

## Reproduce

Use new directories: the generator refuses an existing target and preparation
requires an empty snapshot. Fixture construction and exhaustive verification
run separately from the measured subprocess and use substantially more memory
than the on-demand reader.

```sh
cargo run -p otmp --example generate_reader_fixture -- /tmp/otmp-reader-large
cp -R /tmp/otmp-reader-large /tmp/otmp-reader-parquet
cargo run -p otmp-datafusion --example qualification -- --prepare /tmp/otmp-reader-parquet
cargo build -p otmp-datafusion --example qualification
/usr/bin/time -l target/debug/examples/qualification /tmp/otmp-reader-parquet
```

On Linux, use `/usr/bin/time -v`; its maximum RSS is reported in KiB. This run
used `CARGO_TARGET_DIR=/private/tmp/otmp-datafusion-target` and the retained
package `/private/tmp/otmp-reader-parquet-perf`. The driver asserts image size,
the registration transfer threshold, configured budgets and query results.
Its metadata/page caches start empty in each fresh process; the OS filesystem
cache is not flushed. Report these as cold reader-cache measurements.

## Complete local matrix

| Check | Terminal result |
|---|---|
| `cargo fmt --all --check` | Pass |
| Strict workspace Clippy, all targets and features | Pass |
| Workspace nextest, all features | 214 passed, 0 skipped |
| Workspace doctests | Pass (no doctests defined) |
| Provider evidence harness example | 2 passed; no live provider calls |
| Canonical fixture regeneration and independent COW/index checker | Pass |
| Append process-crash harness | 4 failpoints passed |
| Metadata process-crash harness | 6 scenarios passed |
| Protocol `wasm32-unknown-unknown` check | Pass |
| `cargo deny check` | Pass |
| `cargo audit` | Pass, 447 dependencies scanned |

The tests include result parity against unpruned native DataFusion scans,
branches/tags/history, schema additions and defaults, nested structs/lists/maps,
stable physical IDs, invalid references, 256 randomized pruning cases, nulls,
NaNs/infinities, temporal/decimal bounds, projected-away filter columns, and a
fully pruned 256-file batch followed by a surviving file. Reader and storage
tests cover concurrent publication, retained pins, cancellation, transport
failure, cache/planning exhaustion, corrupt indexes and cached references,
missing pages, oversized records/containers, bounded decompression, malformed
pack indexes, and deterministic S3 range-response violations.

The initial full run found a stale history object-count assertion: the new
checkpoint-index root adds one verified object per full checkpoint. The
assertion now includes it. One earlier run reported a subprocess leak on an
existing COW test; its isolated rerun and the final full run passed without a
leak. The final nextest log records 214/214 clean terminal results.

The first two delivery commits were also verified in independent staged
snapshots: format/range storage passed 150 tests, and the metadata reader passed
182 tests without DataFusion. Their logs are retained with the final evidence.

## Retained evidence and validation boundary

Final local logs are retained under `/private/tmp/otmp-reader-`:
`nextest-terminal.log`, `clippy-final.log`, `doctests-final.log`,
`provider-harness.log`, `conformance.log`, `crash-local.log`, `wasm.log`,
`deny-final.log`, `audit.log`, and `parquet-performance-final.{json,stderr}`.
The original large metadata package remains at
`/private/tmp/otmp-reader-perf-valid`.

The earlier empty-snapshot reader experiment transferred 372,818 bytes during
registration. Its retained positive and negative traces are in
`/private/tmp/otmp-reader-perf/registration-*`. The negative trace exposed an
unnecessary historical-parent relational-row read that materialized a large
record. The selected commit's relational validation and authenticated envelope
linkage remain; global historical row/replay checks belong to exhaustive
verification. The final implementation additionally bounds overflow record
allocation before the native engine reads it. The three-phase measurements
above supersede the earlier reader-only memory measurements.

Materialized `verify_history()` also passed on the large writer-generated
package, traversing three generations, three semantic commits and 570 objects.
Its 429,627,459 verified bytes and approximately 4.76 GiB peak RSS describe
exhaustive verification, not the on-demand reader. Full-image fixtures, the
stock SQLite oracle and prior [COW upload measurements](2026-09-06-turso-cow.md)
remain intact. Whole-file Parquet SHA-256, whole metadata-object coverage,
global relational invariants and semantic replay remain exhaustive operations.
