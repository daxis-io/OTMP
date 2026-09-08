# Reader concurrency local qualification

The frozen V7 candidate passes the 45-gate concurrency matrix: 340 successful processes, exact results, and unchanged fixtures. Default preflight concurrency remains eight, with one available for comparison and operational fallback. This report records local qualification; publication, merge, deployment, and live provider qualification are outside this delivery.

## Source and measurement contract

Base: `88cefa7f6cab3bd7670668cb99c43b974f63bd6e`. Local worktree: `/private/tmp/otmp-reader-concurrency`, branch `perf/reader-concurrency`. Build source is frozen in `candidate-source-v7.tar.gz`; per-file hashes and binary hashes are in the accompanying manifest. Delivery documentation and evidence are added or updated after the executable source freeze. All changes remain local and uncommitted.

DataFusion 55.0.0, Parquet 59.3.0, Turso 0.7.2, and Iceberg Rust `28ede505ebc3a274d4840624e5c85eae480a77ae` remain pinned. The reader binary uses the default release profile; the comparator uses its existing thin-LTO, one-codegen-unit release profile. Rustc is 1.95.0 on Apple M4 Max (arm64), 64 GiB, macOS 26.6.

Metadata cache, footer cache, and per-scan planning budgets remain 64 MiB each; the Turso page-cache setting remains 4 MiB and a soft capacity. The DataFusion harness pool remains 256 MiB. These are distinct ownership budgets, not a process-RSS bound.

The original growth fixtures and derived Iceberg layout were reused unchanged. Growth fixtures contain 128 consecutive int64 IDs per file. A separate 256-file fixture varies footer padding across 0, 4,096, and 32,768 bytes. Every query checks exact count and sum. Correctness tests additionally compare native selected-file URI membership and exact rows at concurrency 1, 2, 4, 8, 16, and 32.

Processes run in balanced seeded order: twenty samples per local/mixed case and ten per delayed case. Cold means a fresh process/provider, not a flushed OS cache. Percentiles use nearest rank over successful samples, with failures retained separately. No build or other qualification run from this task overlapped timed matrices. Other work on the host was observed and recorded. The failed V6 matrix and three-way V6/V7/control experiment demonstrate that host variability affects these local tails.

The new overlapping-query harness times each query directly. Physical I/O and reservation peaks belong to the complete shared round and are never attributed by subtracting overlapping provider snapshots. Planning, planning through first aggregate result, complete execution, and subprocess RSS are separate. The aggregate result requires scanning the selected rows; it is not time to the first input record.

### Sequential controls

The instrumentation-only binary preserves sequential reader behavior while adding the extended harness. The original paired matrix uses the previously frozen `reader_scale-83d3e6a` binary, whose reader source matches the requested base. Both binaries and source provenance are preserved.

**The original instrumentation baseline fails all twenty eight-query rounds from eager metadata descriptor reservations.** The eight-query latency/throughput comparison therefore uses a separately archived reservation-only sequential control. It changes only `otmp/src/reader/files.rs` and `otmp/src/reader_engine.rs` to reserve the bounded actual descriptor result behind the original worker admission; it retains sequential preflight and per-statement dispatch. Budgets are unchanged. Every other case uses the original instrumentation baseline. The failed original eight-query runs are not successful baseline measurements.

## Concurrency gates

Planning times below are p50 / p95 milliseconds. Local rows use the retained-provider pass; the delayed row uses the fresh-provider pass and injects 10 ms into each data stat, trailer, and footer request.

| Case | Sequential | Concurrency 8 | Gate |
|---|---|---|---|
| local-broad | 6.01 / 6.27 | 3.98 / 4.68 | Pass |
| local-small | 31.49 / 34.08 | 22.76 / 23.56 | Pass |
| local-heterogeneous | 5.92 / 6.19 | 4.13 / 4.61 | Pass |
| delayed-broad | 9,742.60 / 9,762.73 | 3,310.37 / 3,320.61 | Pass |

Delayed broad planning improves 2.94× at p50 and 2.94× at p95, exceeding the required 2×. All local warm p50/p95 values meet the larger of baseline × 1.05 or baseline + 1 ms.

Mixed-query small-query p95 is tested separately at each query position. The table reports the worst candidate/baseline ratio across the small queries; throughput is completed queries per second at p50. All meet the 10% regression allowance. The eight-query baseline is the reservation-only control described above.

| Case | Worst small-query p95 ratio | Sequential throughput | Candidate throughput |
|---|---|---|---|
| mixed-broad-small | 1.046× | 68.82 | 92.10 |
| mixed-homogeneous-1 | 0.878× | 168.48 | 197.94 |
| mixed-homogeneous-2 | 0.946× | 307.38 | 322.16 |
| mixed-homogeneous-4 | 0.980× | 410.61 | 410.75 |
| mixed-homogeneous-8 | 1.002× | 503.50 | 501.29 |

### Separate query timings and RSS

Candidate timings are p50 / p95 milliseconds; RSS is whole-subprocess p50 / p95 MiB and includes both passes when present. The complete per-query and per-round distributions are retained in `results.json.gz`.

| Case / query 0 | Planning | Through first result | Execution | Complete | Process RSS MiB |
|---|---|---|---|---|---|
| local-broad | 24.97 / 31.39 | 38.39 / 46.93 | 13.27 / 15.47 | 38.40 / 46.94 | 59.31 / 60.69 |
| local-small | 69.12 / 71.65 | 69.55 / 72.13 | 0.45 / 0.51 | 69.56 / 72.13 | 54.80 / 54.92 |
| local-heterogeneous | 24.75 / 30.63 | 37.36 / 45.32 | 12.53 / 14.78 | 37.36 / 45.33 | 61.84 / 62.00 |
| delayed-broad | 3,310.37 / 3,320.61 | 3,336.09 / 3,347.78 | 26.10 / 29.74 | 3,336.10 / 3,347.79 | 59.53 / 65.30 |
| mixed-broad-small | 29.45 / 36.28 | 43.33 / 52.60 | 13.64 / 16.48 | 43.33 / 52.61 | 58.28 / 61.89 |
| mixed-homogeneous-8 | 13.78 / 14.42 | 14.40 / 15.08 | 0.55 / 0.72 | 14.40 / 15.09 | 49.42 / 49.56 |

## Implementation and ownership

`ProviderOptions::preflight_concurrency` accepts 1 through 32 and defaults to
8. One provider-wide FIFO semaphore covers admitted stat/trailer/footer/schema
preflight work. Each scan queues at most one admission and adjusts its rolling
window to the active scan count. Concurrency 1 remains an operational fallback.
This does not limit native Parquet execution or total process memory.

One metadata retrieval overlaps the active preflight set. A scan holds at most
one reservation-bearing `FileBatch` and one compact pending batch of at most
256 candidates. Pruning and schema binding remain inline. Every raw descriptor,
including pruned descriptors, enters the scan-wide URI identity registry;
conflicts fail before any plan is returned. Each distinct selected URI receives
one version acquisition, and final assembly uses only validated pinned objects.
Existing group order, residual filters, projections, limits, and continuation
semantics are preserved.

Footer payload admission uses a FIFO async mutex and typed outcomes: reserve,
wait for releasable transient work, or fail because capacity cannot become
available. Registry keys, active payloads, and retained leases have distinct
accounting states. Shrink/drop wakes waiters. Trailer sizing for new default
footer loads holds the registration turn through payload admission; this
prevents a small, stalled trailer key from crowding out a later large footer
that fits sequentially. Admitted payload fetches can overlap. With a tight
budget, registration remains held through validation and lease release. This
is an intentional scheduling limit, measured by the delayed matrix.

Footer and exact metadata-range misses use the existing `futures-util::Shared`
primitive. Waiters own strong load references; registries contain weak references
and remove an entry only if it still points to the dropping load. A cancelled
waiter cannot cancel a peer's physical fill. Dropping the final waiter drops
unfinished asynchronous work. Keys and retained results are charged, failed
entries are removed, and no map lock is held across an await. Full immutable
identities are checked before joining a load. Authenticated pages remain
separate from raw cached ranges. Non-default Arrow options decode with their
requested policies and index checks, optionally reusing validated tail bytes.

The metadata reader retains one Turso connection and the original worker mutex.
A FIFO async permit is acquired before `spawn_blocking` and owned by the closure.
One bounded SQL implementation supports 1–16 statements per dispatch; metric
retrieval uses groups of four existing 16-file statements to avoid monopolizing
the lane. Each raw result is decoded and released before the next statement.
Cancellation is checked between statements, rows, page operations, and surfaced
Turso I/O/yield points. Descriptor-result reservations are acquired after engine
admission from the bounded actual raw footprint, with the original eightfold
allowance. Metric byte accounting retains the exact previous charge while
avoiding redundant canonical JSON parse/serialization.

Mutable cancellation and failure state belongs to the active operation.
Immutable authenticated page-role facts remain pinned to the engine's image:
Turso can reuse cached B-tree/overflow pages across statements, so clearing those
facts on every operation would discard required validation context. This is
not shared mutable failure state.

Aggregate statistics remain cumulative. `otmp.scan` and `otmp.load` spans record
scan/load outcomes, elapsed time, admission and dispatch waits, validation time,
and physical request/byte counts. Shared physical fills are counted once at
load scope; waiter events carry the shared load ID. Enable the parent module
spans as well as summary targets when collecting correlation, for example
`RUST_LOG=otmp=info,otmp_datafusion=info,otmp.scan=info,otmp.load=debug`.

`RuntimeError::SharedCause` preserves shared error policy and causes; unique
errors recover the original variant. `code()` and `retryable()` are now ordinary
methods because the shared Arc cause cannot be dereferenced in a const method.
Callers using these methods in const evaluation must adjust. Footer wrappers
retain the original Parquet error as their source.

## Failed experiments retained

- V1 completed the local/delayed cases, but both binaries failed every
  eight-query round from eager metadata descriptor reservations. Its evaluator
  also assumed throughput existed for failed rounds; the evaluator now reports
  a failed gate instead of crashing. Original failures and recovered gates are
  retained. Mixed-query latency/throughput gates also failed.
- V2 moved descriptor-result reservation behind engine admission without
  changing budgets. Query completion recovered, but mixed-query gates failed.
- V3 added cooperative completion yields and removed redundant canonical
  metric re-encoding. Focused broad/small results improved; homogeneous gates
  still failed. An initial equivalence test used integers outside the accepted
  canonical metadata range; the failed test and corrected valid-boundary test
  are retained.
- V4 reduced metric dispatch groups from sixteen statements to four. Focused
  homogeneous cases passed, while broad/small tail latency failed.
- V5 added a per-scan fair share of admitted preflights. All focused mixed gates
  passed. A subsequent controlled stalled-trailer test exposed premature
  resource exhaustion at the sequential minimum footer budget; V5 was not
  accepted as the final candidate.
- V6 keeps registration through trailer sizing/admission and distinguishes
  payload, registry-key, and retained reservations. It covers intrinsic
  impossibility, earlier-key progress, non-default decoding, and reservation
  shrink/drop races. The overlap test now observes simultaneous metadata/data
  activity directly instead of comparing peaks reached at different instants.

Initial localhost adapter test failures caused by sandbox socket restrictions
are retained separately from the subsequent successful unrestricted local runs.
No failed sample has been removed or reclassified as successful.

- V6 completed all 340 matrix processes with exact results, but six mixed
  timing gates failed under recorded host contention.
- V7 removes a grouping regression: attaching each metric searched all 256
  files rather than its original 16-file operation. The decoded row now carries
  the already-validated file position; a small vector replaces a temporary tree.
  A 240-process interleaved baseline/V6/V7 experiment passed every focused gate.
  V6 also passed in that comparison, so the earlier timing failures cannot be
  assigned solely to the lookup change. Host variability is a measured limit;
  the full V6 failure remains part of the qualification record.


## Reservation and request accounting

Maximum observed cumulative reservation peaks across candidate rounds are shown below, in MiB. Both configured limits remain 64 MiB. Every accepted round ends with zero active logical storage requests and zero retained DataFusion pool reservations. Cache entries may remain retained between rounds; transient-waiter and lease cleanup are checked separately by deterministic tests.

| Case | Metadata reservation peak | Footer reservation peak |
|---|---|---|
| delayed-broad | 21.457 | 1.079 |
| local-broad | 21.457 | 1.079 |
| local-heterogeneous | 21.457 | 14.847 |
| local-small | 23.257 | 0.246 |
| mixed-broad-small | 23.237 | 0.961 |
| mixed-homogeneous-1 | 21.457 | 0.246 |
| mixed-homogeneous-2 | 21.457 | 0.248 |
| mixed-homogeneous-4 | 23.237 | 0.246 |
| mixed-homogeneous-8 | 34.346 | 0.246 |

Across all twenty accepted eight-query rounds, both binaries issue 16 independent data stats; physical data range requests fall from 48 for the reservation-only control to 20 for the candidate, including execution reads.

The controlled eight-caller single-file test records eight independent version stats and exactly two footer ranges for one shared physical footer fill. Full physical round counts are retained in the raw samples; they are not duplicated across per-query timing records.

## Original paired matrix

The preserved `reader_scale-83d3e6a` control and V7 each completed 110 processes. All four original local warm cases also meet the 5% / 1 ms planning allowance. Times are planning p50 / p95 milliseconds; no samples were discarded.

| Case | Baseline cold | Candidate cold | Baseline retained | Candidate retained |
|---|---|---|---|---|
| delay-1024-2 | 1,057.73 / 1,065.36 | 1,024.64 / 1,042.39 | 35.44 / 37.06 | 18.87 / 19.78 |
| delay-16-2 | 154.63 / 158.71 | 129.23 / 130.52 | 27.15 / 28.04 | 12.92 / 13.99 |
| delay-256-all | 9,835.43 / 9,878.13 | 3,386.54 / 3,405.78 | 3,205.85 / 3,248.60 | 407.31 / 412.36 |
| local-16384-2 | 334.31 / 486.96 | 316.45 / 441.80 | 138.86 / 189.33 | 104.55 / 139.99 |
| local-16384-all | 4,554.86 / 11,565.55 | 3,221.37 / 7,550.04 | 881.07 / 1,355.70 | 535.81 / 618.25 |
| local-4096-2 | 78.01 / 82.33 | 69.46 / 71.85 | 31.56 / 34.18 | 23.09 / 23.69 |
| local-4096-all | 1,009.46 / 1,330.44 | 610.95 / 973.68 | 123.77 / 162.27 | 78.77 / 121.30 |

Whole-subprocess RSS is p50 / p95 MiB. Native execution, plan release, registration, and teardown remain separate in the raw phase records.

| Case | Baseline RSS | Candidate RSS |
|---|---|---|
| delay-1024-2 | 48.67 / 48.73 | 48.88 / 49.09 |
| delay-16-2 | 46.36 / 46.50 | 46.44 / 46.59 |
| delay-256-all | 61.16 / 66.88 | 60.78 / 69.45 |
| local-16384-2 | 75.03 / 75.16 | 78.33 / 78.50 |
| local-16384-all | 469.66 / 477.02 | 469.89 / 549.31 |
| local-4096-2 | 53.92 / 54.09 | 54.75 / 54.78 |
| local-4096-all | 152.86 / 154.36 | 153.02 / 169.38 |

The 16,384-file broad case reaches candidate process RSS p95 of 549.31 MiB versus 477.02 MiB for the baseline. The explicit reservation limits hold, but this implementation does not close total-process memory growth or provide an RSS cap.

## Pinned Iceberg comparison

The unchanged comparator and derived fixtures completed 160 provider processes and 40 separate Iceberg file-planning probes. Source, lockfile, binary, and fixture provenance matched before and after. Iceberg defers file enumeration into execution, so planning through the first aggregate result is the primary matched SQL measure. OTMP remains slower than Iceberg at p50 in every measured SQL case and pass; the matched-format latency gap remains. These results do not establish general format or production parity.

| Case | OTMP cold through first | Iceberg cold through first | OTMP retained through first | Iceberg retained through first |
|---|---|---|---|---|
| growth-16384-broad-all | 4,226.70 / 4,862.01 | 1,310.21 / 1,654.87 | 2,432.13 / 2,646.07 | 859.88 / 1,204.43 |
| growth-16384-selective-2 | 322.40 / 373.51 | 62.69 / 70.31 | 106.29 / 112.81 | 14.19 / 14.55 |
| growth-4096-broad-all | 1,394.24 / 1,864.38 | 389.52 / 461.67 | 539.93 / 616.37 | 349.56 / 396.21 |
| growth-4096-selective-2 | 180.87 / 185.20 | 37.50 / 39.88 | 55.01 / 56.22 | 10.08 / 11.14 |

Separate candidate/comparator phase measurements follow. Times are p50 / p95 ms; execution includes first-result work and drain. Complete time is calculated per sample from planning + execution before taking percentiles. RSS is p50 / p95 MiB for the complete two-pass process.

| Case | Format | Pass | Planning | Execution | Complete | RSS MiB |
|---|---|---|---|---|---|---|
| growth-16384-broad-all | otmp | cold | 2,590.07 / 3,347.25 | 1,439.37 / 1,873.09 | 4,226.70 / 4,862.01 | 468.89 / 476.78 |
| growth-16384-broad-all | otmp | retained | 564.71 / 621.00 | 1,860.05 / 2,131.17 | 2,432.13 / 2,646.08 | 468.89 / 476.78 |
| growth-16384-broad-all | iceberg | cold | 2.29 / 2.73 | 1,307.79 / 1,652.14 | 1,310.21 / 1,654.87 | 79.72 / 80.02 |
| growth-16384-broad-all | iceberg | retained | 0.77 / 1.18 | 859.16 / 1,203.42 | 859.88 / 1,204.43 | 79.72 / 80.02 |
| growth-16384-selective-2 | otmp | cold | 321.96 / 373.02 | 0.46 / 0.53 | 322.40 / 373.51 | 78.45 / 78.73 |
| growth-16384-selective-2 | otmp | retained | 105.96 / 112.47 | 0.33 / 0.43 | 106.29 / 112.82 | 78.45 / 78.73 |
| growth-16384-selective-2 | iceberg | cold | 1.18 / 1.24 | 61.50 / 69.11 | 62.69 / 70.32 | 77.75 / 77.81 |
| growth-16384-selective-2 | iceberg | retained | 0.58 / 0.65 | 13.57 / 13.90 | 14.19 / 14.55 | 77.75 / 77.81 |
| growth-4096-broad-all | otmp | cold | 1,025.46 / 1,355.75 | 395.97 / 501.75 | 1,394.24 / 1,864.39 | 146.81 / 148.62 |
| growth-4096-broad-all | otmp | retained | 119.03 / 129.61 | 427.65 / 488.01 | 539.93 / 616.38 | 146.81 / 148.62 |
| growth-4096-broad-all | iceberg | cold | 2.13 / 2.37 | 387.42 / 459.31 | 389.53 / 461.67 | 47.67 / 48.42 |
| growth-4096-broad-all | iceberg | retained | 1.04 / 1.18 | 348.56 / 395.18 | 349.57 / 396.21 | 47.67 / 48.42 |
| growth-4096-selective-2 | otmp | cold | 179.79 / 183.81 | 1.06 / 1.25 | 180.88 / 185.21 | 48.41 / 48.77 |
| growth-4096-selective-2 | otmp | retained | 54.22 / 55.40 | 0.78 / 0.94 | 55.02 / 56.23 | 48.41 / 48.77 |
| growth-4096-selective-2 | iceberg | cold | 2.08 / 2.26 | 35.39 / 37.83 | 37.50 / 39.88 | 46.59 / 47.44 |
| growth-4096-selective-2 | iceberg | retained | 0.96 / 1.03 | 9.14 / 10.14 | 10.08 / 11.14 | 46.59 / 47.44 |

The separate file-task probes consume Iceberg planning tasks without opening Parquet data. Their times must not replace SQL-query completion measurements. Full probe distributions and task counts are retained in the compressed results.

## Validation and reproduction

All checks in `repository-checks-v7.json` have exit code zero. `validation.txt` retains full logs, including 239 workspace nextest tests, 20 reader-scale example tests, four comparator Rust tests, strict workspace/comparator Clippy, locked doctests, both Python harness suites, canonical conformance, protocol WASM, provider harness checks, reader smoke, supply-chain checks, and the applicable append/metadata crash cases. These are local results, not remote CI results.

Reproduce from the repository root, with `EVIDENCE=/private/tmp/otmp-reader-concurrency-evidence`. Output directories must be new. The concurrency command is in [the qualification README](../../README.md). The original matrix and comparator commands are:

```sh
python3 qualification/reader-planning/run.py \
  --baseline /private/tmp/otmp-reader-planning-evidence/reader_scale-83d3e6a \
  --candidate "$EVIDENCE/reader_scale-candidate-v7" \
  --fixtures /private/tmp/otmp-reader-scale-evidence/fixtures \
  --out "$EVIDENCE/original-paired-rerun"

python3 qualification/reader-comparison/run.py \
  --binary "$EVIDENCE/comparison-candidate-v7" \
  --lock qualification/reader-comparison/Cargo.lock \
  --source-repo /private/tmp/otmp-reader-concurrency \
  --source-4096 /private/tmp/otmp-reader-scale-evidence/fixtures/growth-4096 \
  --derived-4096 /private/tmp/otmp-reader-planning-evidence/comparison-fixtures/growth-4096 \
  --source-16384 /private/tmp/otmp-reader-scale-evidence/fixtures/growth-16384 \
  --derived-16384 /private/tmp/otmp-reader-planning-evidence/comparison-fixtures/growth-16384 \
  --out "$EVIDENCE/iceberg-rerun"
```

The [final source check](final-source-verification.json) confirms that compiled source matches the frozen build and both original checkouts remain clean at their initial SHAs. The delivery README is the sole modified pre-existing file after the executable source freeze.

The [manifest](manifest.json) binds source, binary, lockfile and capture identities. `raw-samples.json.gz` contains the final matrices and the original failed eight-query captures; `raw-index.json.gz` hashes the complete final and diagnostic capture trees. `results.json.gz` includes final distributions and prior failed experiments. Full frozen source archives, binaries, fixtures, and raw working directories remain under the recorded external paths. `failed-checks.txt` preserves the controlled trailer failure, invalid initial test input, superseded overlap assertion, and sandbox-only test failures.

The separate [runtime-isolation follow-up](../../../../docs/plans/2026-09-08-reader-runtime-isolation-follow-up.md) covers stream-driver placement, complete storage-body routing, runtime lifetime, and shutdown ownership. It adds no runtime, CPU pool, or provider integration.

## Local readiness audit

### 1. Verdict

Approve for the requested local delivery, using the explicitly disclosed
reservation-only control for the otherwise failing eight-query baseline.
The timing gates, exact-result matrices, and repository checks are recorded in this report and its evidence.
This is not merge, deployment, or live-provider approval.

### 2. Completion Check

| Criterion | Status and evidence |
|---|---|
| Attributable baseline and overlapping-query harness | Met; frozen instrumentation source/binary, direct query timers, shared round accounting, raw failures retained |
| Rolling provider-scoped admission | Met; concurrency 1–32, default 8, one pending admission per scan, fair-share rolling window |
| Concurrent progress under unchanged budgets | Met; mixed-size fixtures, sequential-minimum budget, controlled stalled trailer, eviction/lease and impossible-fit checks |
| Compatible shared fills | Met; full identities, weak registries, last-waiter cleanup, independent version pins, one physical fill, preserved error policy |
| One metadata engine lane with bounded dispatch groups | Met; permit before spawn, max 16 operations, four-operation metric groups, raw-result bounds and decode lifetime retained |
| Metadata/preflight overlap | Met; direct simultaneous-request observation across the 256-file boundary, one batch/compact pending buffer, scan-wide late conflict registry |
| Local performance gates | Met with the disclosed eight-query control; original baseline failures and V1–V6 failures remain recorded |
| Original paired and pinned Iceberg reruns | Met; exact source/fixture provenance and all processes checked |
| Runtime isolation follow-up | Met as a separate brief; no runtime-isolation implementation in this change |

### 3. Correctness and Edge Cases

No unresolved correctness finding remains in the reviewed scope. Descriptor
reconciliation includes pruned and late batches; native assembly happens after
all selected identities and schemas validate. Projection cannot hide missing
required fields. Repeated URI pinning, conflicting immutable identities,
malformed/oversized footers, schema evolution, sparse metric association, and
selected-file membership are covered by the focused and workspace checks.
The V7 position carried from each SQL operation preserves the original bounded
16-file membership check and duplicate-metric detection.

### 4. Design Quality

The two caches remain independent and provider/context scoped. Sharing uses the
installed futures primitive rather than a new cache framework. The engine keeps
one connection and its original mutex. The new option and `SharedCause` variant
are public API additions; the loss of const evaluation for `code()` and
`retryable()` is documented in this report. No runtime pool, parallel metadata
lane, prepared-statement cache, or schema-binding memoization was added.

### 5. Reliability

Byte admission distinguishes active payloads, preflight keys, and retained
leases so a waiter does not wait on its own key or fail while earlier preflight
work can release capacity. The controlled small-trailer/large-footer failure
was repaired before qualification. Engine admission remains owned by the blocking
closure until it exits. Cancellation checks and RAII release cover controlled
admission/storage/shared-fill waits; bounded inline validation is not preempted
mid-call. Registry cleanup, retryability, and lease accounting have runnable
checks. There are no unfinished operations in the accepted matrices.

### 6. Security

Version pinning, authenticated-page validation, metadata row/record/byte bounds,
footer bounds, and read-only storage behavior remain enforced. Raw range sharing
does not mark bytes authenticated. Non-default Arrow options perform their own
decode/index checks. The supply-chain checks pass. No external credential,
provider, publication, or deployment action is part of this delivery.

### 7. Performance

The accepted run meets the required delayed, warm, mixed-tail, and throughput
gates without increasing budgets or changing the default. Trailer sizing remains
serialized through payload admission, and the metadata lane remains serialized.
The host is shared; V6's failed full run and later passing three-way comparison
limit causal claims about individual micro-optimizations. OTMP-owned budgets
and preflight admission are not native execution or RSS limits. The Iceberg
comparison remains a local, matched-fixture comparison rather than general
format or production parity.

### 8. Tests

The evidence records locked workspace nextest and doctests, the reader-scale
example, Python harness checks, comparator tests/Clippy, canonical conformance,
protocol WASM, supply-chain checks, and crash qualification. Deterministic
concurrency checks run through the existing normal CI commands. The two Loom
models cover abstract OTMP-owned reservation and weak-registry lifetime
transitions; they do not model Tokio, Parquet, or Turso internals. Timing
thresholds remain in the manual qualification harness.

### 9. Style and Hygiene

Formatting and strict Clippy pass. Exact executable-source, lockfile, fixture,
binary, and raw-capture identities are preserved. Original worktrees and frozen
artifacts are retained. The source remains uncommitted for review.

### 10. Action Items

- P0: None remaining in this local delivery scope.
- P1: Keep the eight-query control caveat with any publication of these results;
  the original instrumentation baseline has no successful eight-query latency.
- P2: Execute the separately scoped runtime-isolation investigation when an
  application workload requires it, covering stream-driver placement, complete
  body routing, runtime lifetime, and shutdown ownership. Qualify providers and
  production behavior separately before making those claims.
