# Reader planning performance qualification

This qualification compares the optimized OTMP reader with its archived
baseline and with Apache Iceberg Rust on the same DataFusion 55.0.0 dependency
graph. It does not establish Delta/Iceberg parity. It identifies improvements
that are ready to review and the remaining measured gap.

## Contract and workload

Source candidate: 83d3e6ad475c51cca0cce5a45eff6878b36d1c8a, based on
ca11c04c999649cf0f26e38fa3640478cd2469f2. Metadata cache remains 64 MiB,
Turso's page cache 4 MiB, metadata batches at most 256 files, and planning
reservations at most 64 MiB. Opening a provider still pins one generation;
repeated queries retain it. Registration does not enumerate files.

The frozen writer-produced growth fixtures contain 128 consecutive int64 IDs
per Parquet file. At 4,096 / 16,384 files they contain 524,288 / 2,097,152 rows.
These deliberately small files expose metadata and per-file overhead; they do
not model the throughput of large production Parquet objects. Both formats
execute `SELECT count(*), coalesce(sum(id), 0) FROM t WHERE id >= threshold`.
Selective thresholds keep the final two files, returning 256 rows; broad scans
keep every file. Every sample checks exact count and sum.

The comparison pins Iceberg Rust 28ede505ebc3a274d4840624e5c85eae480a77ae,
DataFusion 55.0.0 and Parquet 59.3.0. One binary contains both providers, with
one Arrow/Parquet/DataFusion version graph, target_partitions=4 and the same
256 MiB DataFusion pool. Iceberg uses an explicitly pinned static snapshot and
one unpartitioned manifest. Its derived data files are byte-identical hardlinks
of the OTMP data. Full content verification runs outside the timed samples.

Fresh processes alternate in balanced, seeded order. Each provider executes
twice. "Cold" below means a fresh provider/process, not a flushed OS cache;
OS caches are uncontrolled and untimed verification warms files. Percentiles
are nearest-rank over all successful samples, with failures retained separately.
No compilation or other qualification workload overlaps timed samples.

Iceberg defers file enumeration into execution. Therefore the comparable SQL
measure is logical/physical planning plus execution through the first aggregate
result. Initialization, physical planning, first result, drain, process wall
time and subprocess peak RSS remain separate in the raw data. Separate Iceberg
`plan_files` processes consume file tasks without opening data files. Their
numbers must not be substituted for complete SQL-query latency.

Delta is not timed: the inspected stock delta-rs providers use a different
DataFusion/Arrow major graph. A local dependency forward-port would introduce
an additional implementation under test. These are local measurements and
simulated per-request delays; no live AWS/R2 or Turso Cloud evidence is claimed.

## Implemented changes

- Branch pagination seeks in the existing `(ref_name,file_id)` primary-key
  index. The first page has no artificial lower bound, so invalid IDs still
  reach integrity checks. Continuation stays derived from unfiltered metadata.
- Metrics use bounded 16-file batches and indexed probes by file/field ID.
  Sparse and multi-field association, exact row bounds and aggregate query
  budgets are tested. An IN-list experiment caused a full index scan and was
  rejected; its source and failed measurements remain in history/evidence.
- Fully authenticated pages share the existing bounded FIFO cache. Keys bind
  the complete serialized generation, checkpoint revision, logical/checkpoint
  view and page. Changed references cannot reuse old authentication.
- Retained immutable range identities supply version tokens after exact
  URI/hash/length checks. Subsequent uncached ranges remain conditional on that
  token. Mutable HEAD and checkpoint revision acquisition retain their checks.
- Parquet footer decoding reuses validated trailer/footer bytes. Default
  metadata preflight uses one stat and two ranges instead of one stat and four
  ranges. Non-default Arrow options decode separately and cannot consume a
  default lease that lacks required indexes. Serialized tails and active reader
  leases are charged to the footer budget.

Complete user-file hashes, global relational validation and replay remain
exhaustive operations. Optimizations do not silently materialize metadata,
raise default caches, advertise exact caller row counts, or remove residual
filters. Cache and planning budgets are not a cap on total process RSS.

## Final interleaved baseline comparison

20 samples per binary for each local case; 10 per binary for each delay case.
Times are planning p50 / p95 in milliseconds, excluding registration and execution.

| Case | Baseline cold | Candidate cold | Baseline retained | Candidate retained |
|---|---:|---:|---:|---:|
| local-4096-2 | 229.63 / 235.65 | 78.31 / 80.14 | 169.89 / 171.59 | 32.98 / 33.42 |
| local-16384-2 | 3,185.06 / 3,206.76 | 311.76 / 317.61 | 3,432.42 / 3,471.92 | 130.68 / 132.91 |
| local-4096-all | 1,504.43 / 1,510.30 | 750.89 / 761.17 | 243.59 / 245.60 | 101.28 / 102.97 |
| local-16384-all | 8,281.99 / 8,318.77 | 2,992.84 / 3,005.63 | 3,733.92 / 3,750.42 | 401.85 / 405.36 |
| delay-16-2 | 365.62 / 370.20 | 161.80 / 166.19 | 30.61 / 32.49 | 28.84 / 30.36 |
| delay-1024-2 | 5,686.83 / 5,704.84 | 1,131.08 / 1,145.66 | 75.54 / 76.82 | 45.80 / 48.56 |
| delay-256-all | 17,580.60 / 17,606.74 | 9,864.95 / 9,899.68 | 3,210.13 / 3,246.39 | 3,219.06 / 3,231.54 |

## Matched Iceberg comparison

20 samples per format/case. The main metric includes planning through the first
aggregate result. Each cell is p50 / p95 milliseconds.

| Files / query | OTMP cold | Iceberg cold | OTMP retained | Iceberg retained |
|---|---:|---:|---:|---:|
| growth-16384-broad-all | 3,735.37 / 3,779.47 | 733.09 / 742.08 | 1,165.93 / 1,178.02 | 664.63 / 675.47 |
| growth-16384-selective-2 | 299.67 / 306.17 | 55.83 / 57.19 | 124.00 / 125.56 | 13.72 / 13.93 |
| growth-4096-broad-all | 939.67 / 951.20 | 185.29 / 187.98 | 288.67 / 294.59 | 167.39 / 171.31 |
| growth-4096-selective-2 | 75.96 / 76.38 | 15.15 / 17.26 | 30.18 / 30.55 | 3.75 / 3.95 |

Provider initialization is separate from those query times:

| Case | OTMP initialization p50 / p95 ms | Iceberg initialization p50 / p95 ms |
|---|---:|---:|
| growth-16384-broad-all | 5.83 / 6.47 | 0.13 / 0.15 |
| growth-16384-selective-2 | 5.81 / 5.90 | 0.12 / 0.14 |
| growth-4096-broad-all | 5.47 / 5.64 | 0.12 / 0.14 |
| growth-4096-selective-2 | 5.40 / 5.50 | 0.12 / 0.12 |

The direct Iceberg file-task probe runs in separate processes (10 per case),
consuming all tasks without opening Parquet files:

| Case | Cold p50 / p95 ms | Retained p50 / p95 ms |
|---|---:|---:|
| growth-16384-broad-all | 58.91 / 60.98 | 16.05 / 16.47 |
| growth-16384-selective-2 | 56.24 / 59.64 | 12.86 / 13.21 |
| growth-4096-broad-all | 15.67 / 17.17 | 3.90 / 4.36 |
| growth-4096-selective-2 | 14.74 / 15.91 | 3.13 / 3.19 |

## Retained planning state and remote requests

Matched-format subprocess peak RSS, p50 / p95 MiB:

| Case | OTMP | Iceberg |
|---|---:|---:|
| growth-16384-broad-all | 474.80 / 476.91 | 80.19 / 80.78 |
| growth-16384-selective-2 | 75.19 / 75.56 | 77.89 / 78.12 |
| growth-4096-broad-all | 146.31 / 149.38 | 47.41 / 48.08 |
| growth-4096-selective-2 | 47.28 / 47.36 | 46.44 / 46.55 |

Candidate metadata operations and retained descriptor reservations during planning
(p50; counters are phase deltas, reservations are values at the phase boundary):

| Case | Metadata requests cold / retained | Data requests cold / retained | Retained descriptor MiB | Peak metadata cache MiB |
|---|---:|---:|---:|---:|
| local-16384-2 | 592 / 0 | 6 / 2 | 0.004 | 60.942 |
| local-16384-all | 592 / 0 | 49152 / 16384 | 19.189 | 60.942 |
| delay-1024-2 | 76 / 0 | 6 / 2 | 0.004 | 23.311 |
| delay-256-all | 6 / 0 | 768 / 256 | 0.301 | 21.457 |

The delay wrapper injects 10 ms for every stat and range call, including timer
overshoot in observed latency. It is not an HTTP/provider latency model.
Broad planning remains sequential during required Parquet schema preflight;
increasing a request ceiling alone does not make that traversal concurrent.
The process-RSS gap does not by itself identify a leak or the precise owner of
every allocation. The descriptor, metadata and footer budgets bound their
specific retained state, not DataFusion and allocator memory in aggregate.

## Interpretation and next optimization

The indexed pagination, bounded metric probes and authenticated caching remove
large avoidable costs. The repeated comparison still does **not** meet the
Iceberg performance target. Warm selective OTMP scans have no metadata I/O,
yet their latency grows with the number of files examined. The next selective
optimization should reduce per-file SQL dispatch, decoding and allocation,
then test compact decoded file/metric reuse within an explicit existing budget.
It should be measured against the same pinned Iceberg workload before adding
an index or a new format projection.

Broad scans additionally pay for schema inspection of every retained Parquet
file and retain more native planning/execution state. The next broad-scan
investigation should inspect file grouping and allocation ownership, then
qualify bounded concurrent preflight with admission based on byte reservations.
Task-count-only parallelism can exhaust the footer budget when sequential
preflight would evict and proceed. Do not remove required-field validation or
move its cost across timing boundaries and call that a speedup.

The fixtures are unpartitioned, and both readers may inspect every file's
metadata. No indexed or sublinear metadata-planning claim follows from these
results. No universal Delta or Iceberg latency target is inferred from one
engine/version/layout.

## Large metadata and availability boundaries

A final confirmation used a 288,935,936-byte (275.55 MiB) metadata image.
Registration transferred 421,914 bytes (0.146%) in 32 requests, reading 35 pages,
with 107 cache hits and a 23,023,534-byte peak metadata cache. Registration,
metadata planning and Parquet execution measured 14 / 14 / 3 ms respectively.
Planning considered 16 files, pruned 14 and reserved 3,736 descriptor bytes;
execution opened two files and returned the expected count and sum. Whole-process
wall time was 1.93 seconds and peak RSS 47,906,816 bytes (45.69 MiB). This is
one confirmation of bounded registration, not a latency distribution.

The existing availability limits remain: a current 2,048-file append and a
current 1 MiB property commit exceed the SQLite record bound; a current 2 MiB
property commit exceeds metadata-cache admission. All report ResourceExhausted.
Their three small-tail controls succeed. A 1 KiB descriptor budget and a 1 KiB
DataFusion pool independently reject planning as expected. All eight checks
release requests and pool reservations without invariant violations. A small
tail is a diagnostic control, not an automatic fix for large current commits.

At 256 retained files with injected 10 ms delays, cold planning improves from
17.58 to 9.86 seconds, but warm planning remains about 3.2 seconds because each
new scan still obtains 256 Parquet object-version stats. Footer caching alone
does not remove that remote-request floor.

## Verification and evidence

The final runtime source is 83d3e6ad475c51cca0cce5a45eff6878b36d1c8a. The later
publication commits only add the evidence and CI/README wiring. All 227 workspace
tests passed, including deterministic S3 range-contract tests. Strict workspace
Clippy, documentation tests, conformance regeneration, protocol WASM check,
example tests, comparator Rust tests, Python harness tests, six native CLI smoke
samples and cargo-deny passed locally. The first sandboxed local-HTTP test and
advisory-lock attempts were blocked by environment permissions; their logs and
successful permitted reruns are retained. Linux crash qualification and all six
remote CI jobs remain pending publication. The separate comparator workspace
now has explicit locked Rust tests and strict Clippy in the CI workflow; both
commands also passed locally ([command evidence](ci-validation.txt)).

Independent review found a first-page sentinel that could skip a malformed
zero ID and a comparison runner that could exit successfully after failed
samples. Both findings have regression checks and were resolved before the
final 420 successful timing samples. The review then approved the runtime
candidate with no remaining P1/P2 findings. Failed optimization experiments and
incompatible comparator attempts remain recorded; they are not pooled into
final latency distributions.

[manifest.json](manifest.json) pins sources, binaries and artifact hashes.
[results.json.gz](results.json.gz) contains the complete phase distributions and
boundary results. [raw-samples.json.gz](raw-samples.json.gz) maps relative paths
to complete final process captures, schedules, manifests and verification logs;
[raw-index.json.gz](raw-index.json.gz) checks their byte lengths and SHA-256 hashes.
The full local investigation, binaries and derived fixtures remain at
`/private/tmp/otmp-reader-planning-evidence`. Original writer/SQLite/COW evidence
and baseline worktrees remain preserved. These results do not qualify live
providers and do not establish Delta or Iceberg performance parity.
