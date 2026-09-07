# Reader scale qualification — September 2026

The reader opens a 16,384-file table without materializing its metadata image, but selective planning becomes slow at that size. Broad scans incur substantial footer work and process memory. Large current commits expose registration limits even when the same history becomes readable after a small follow-up commit. This report records the measured limits; the production reader implementation is unchanged.

## Workload and measurement

The main matrix contains 62 configurations and 1,070 fresh processes: 900 successful measurements (including 40 planning-only runs) and 170 observed resource errors. The errors comprise 90 registration failures in large-current-commit probes and 80 planning failures under lowered budgets. Every successful executed query matched the fixture’s expected count and sum. No sample reported a secondary invariant violation, forbidden full read, fixture mutation, or unreleased planning reservation at teardown.

Each fixture was created with the normal OTMP writer and exhaustively verified. The growth fixtures contain 16, 256, 1,024, 4,096, and 16,384 real Parquet files, each with 128 sequential int64 IDs in one column. Growth fixtures use 128-file append batches and a small final property commit to isolate file count from current-commit size. The separate single-append series uses 128, 512, 1,024 and 2,048 files. The largest fixture contains 2,097,152 rows, 23.51 MiB of logical metadata, and 26.64 MiB of Parquet data across 129 published table versions. These small files isolate file-management cost; this is not a wide-schema or large-row-group throughput benchmark.

Runs used an Apple M4 Max, 64 GiB RAM, macOS 26.6 arm64, Rust 1.95.0, and DataFusion 55.0.0 in release mode. Tokio uses four worker threads and DataFusion target_partitions is four. The default metadata cache, footer cache, and descriptor planning budget are each 64 MiB; the engine page cache is 4 MiB and the DataFusion memory pool is 256 MiB. Fixture preparation, exhaustive verification, compilation, and provenance hashing are outside measured subprocess intervals.

“Cold” means fresh reader caches. Verification reads the fixture objects beforehand and may warm the OS cache, which is not flushed or controlled. Local cases use 20 samples; delay and raised-budget probes use 10. Tables show p50 / p95 using nearest rank, with failures excluded from successful distributions and retained separately. For ten samples p95 is the maximum. Full distributions include count, minimum, and maximum in the machine-readable evidence. Main-matrix cases run sequentially on a workstation whose OS cache and background workload are uncontrolled, so small differences across separately timed cases should not be treated as causal speedups. No qualification builds or fixture preparation ran concurrently with the timed samples. The cache follow-up interleaves configurations to reduce ordering effects.

## File-count growth

| Files | Registration ms | Registration payload bytes | Two-file planning ms | Broad planning ms |
|---:|---:|---:|---:|---:|
| 16 | 6.56 / 7.49 | 309,198 | 4.65 / 4.97 | 9.45 / 10.24 |
| 256 | 6.22 / 6.76 | 353,102 | 13.83 / 14.09 | 101.16 / 117.52 |
| 1,024 | 8.60 / 9.01 | 427,655 | 58.21 / 64.06 | 399.23 / 416.71 |
| 4,096 | 8.93 / 10.13 | 463,190 | 267.97 / 279.17 | 1,581.51 / 1,611.93 |
| 16,384 | 9.50 / 9.98 | 448,920 | 3,701.13 / 3,730.23 | 9,050.72 / 9,303.64 |

At 16,384 files, registration transfers 448,920 bytes through 106 stat calls and 27 range reads—1.82% of the logical metadata-image size. This supports lazy registration for this fixture. Transfer counts are successful response payload bytes, include repeated regions, and exclude HTTP headers and wire overhead.

Two-file planning considers all 16,384 descriptors and prunes 16,382. It reserves only 3,736 bytes for scan descriptors and performs ten Parquet preflight operations totaling 1,932 bytes. Execution opens two files, transfers 2,350 bytes, and takes 0.64 / 0.73 ms. Provider scan construction takes about 3.700 seconds of the 3.701-second median planning phase, locating the cost inside the integration rather than surrounding DataFusion SQL planning.

The fourfold increase from 4,096 to 16,384 files produces a 13.8-fold selective-planning increase. That is an observed scaling change, not a proven asymptotic diagnosis. The metadata implementation currently issues a metric lookup per file and requested field; SQL statement counts are inferred from the code, not measured by this harness. Cache capacity must also be controlled before attributing the slowdown to those lookups.

## Retained providers and cache pressure

| Files | First planning ms | Second planning ms | Second metadata requests | Second metadata bytes |
|---:|---:|---:|---:|---:|
| 16 | 4.80 / 5.30 | 1.70 / 1.89 | 0 | 0 |
| 256 | 13.76 / 14.65 | 8.79 / 9.23 | 0 | 0 |
| 1,024 | 57.24 / 59.77 | 34.20 / 37.90 | 0 | 0 |
| 4,096 | 266.43 / 270.17 | 187.99 / 191.67 | 0 | 0 |
| 16,384 | 3,570.62 / 3,627.24 | 3,765.64 / 3,911.60 | 52,973 | 0 |

Through 4,096 files, retained-provider planning eliminates metadata requests but still performs enumeration and pruning work. At 16,384 files, the second plan issues 52,973 metadata stat requests with zero range reads or transferred payload. Two additional stat calls validate the selected Parquet objects. This pattern is consistent with engine page-cache pressure and prevents interpreting the 16k retained run as pure query CPU. The separate capacity experiment below tests that distinction.

The controlled follow-up uses the identical 16,384-file fixture and selective query, with 20 fresh processes per cache capacity and two plans per retained provider. Capacities are interleaved in balanced rotation blocks (seed 7): each appears six or seven times in each position. All 60 processes succeed with expected results, unchanged binary and fixture identities, and no reported invariant violation.

| Engine page cache | First planning ms | Second planning ms | Second metadata requests | Peak process RSS MiB |
|---|---:|---:|---:|---:|
| 4 MiB | 3,668.21 / 17,633.19 | 3,882.04 / 22,885.51 | 52,973 | 86.17 / 91.47 |
| 16 MiB | 2,057.88 / 3,873.91 | 1,752.10 / 4,878.53 | 0 | 89.44 / 91.91 |
| 32 MiB | 2,072.91 / 3,653.74 | 1,768.62 / 3,226.82 | 0 | 89.41 / 95.50 |

Increasing the engine page cache from 4 to 16 MiB eliminates the repeated metadata stat requests and reduces median second planning time by 54.9% (a 2.22× speedup). The 32 MiB case does not provide a comparable additional median gain. This controlled change identifies page-cache pressure as a contributor to the 16k slowdown. It does not explain all planning cost: the 16 MiB second plan still takes 1.75 seconds without metadata I/O. Metadata SQL execution, decoding, enumeration and pruning remain inside that phase and need profiling before selecting a precise implementation change. The engine cache setting already exists; this slice exposes it in the harness and leaves runtime defaults unchanged.

The wide cache-experiment tails are retained, including 4 MiB rounds 12 and 13 at 40.55 and 76.65 seconds whole-process wall time. Their recorded user-plus-system CPU times are 16.35 and 15.12 seconds; the 16 MiB round 13 also takes 14.65 seconds wall versus 5.66 seconds CPU. The gap shows substantial off-CPU time but does not identify its cause. No host-wide scheduler or storage trace was collected, so these p95 values cannot isolate engine behavior from workstation contention or waiting. The exact disappearance of metadata requests is stronger evidence for the cache effect than a claim about production tail latency. No slow sample was removed or replaced.

## Broad scans and memory

| Files | Descriptor reservation MiB | Footer peak MiB | Metadata cache peak MiB | Planning-only RSS MiB | Executed broad RSS MiB |
|---:|---:|---:|---:|---:|---:|
| 4,096 | 4.80 | 7.78 | 26.34 | 97.09 / 97.84 | 147.94 / 148.55 |
| 16,384 | 19.19 | 30.77 | 53.10 | 252.44 / 253.94 | 440.02 / 442.19 |

At 16,384 files, broad planning makes 81,920 Parquet preflight operations and transfers 15,826,944 bytes before execution. Broad execution then opens all files, transfers 19,251,200 bytes, and takes 977.76 / 1,019.93 ms. Disabling OTMP file pruning for the selective query retains the same 16,384 descriptors and preflight work. Residual Parquet pruning still reads only two files. Execution nevertheless takes 148.22 / 165.20 ms; the run retains 16,384 descriptors and records 16,384 footer-cache hits. The harness does not report the final optimized file-group topology.

Descriptor reservations follow 1,280 + 1,228 × retained files bytes for this fixture. The individual metadata, footer, and descriptor budgets are respected; they are not a 64 MiB process memory cap. Planning-only peak RSS is already 252.44 MiB at 16k, and executed broad peak RSS is 440.02 MiB. The difference includes execution/runtime and allocator activity outside these retained counters. Peak RSS alone does not identify live retained allocations or prove a leak.

At 4,096 files, descriptor budgets of 32 KiB, 256 KiB, and 1 MiB all fail before any Parquet preflight request. An 8 MiB budget succeeds with 5,031,168 bytes reserved. A separate 1 MiB DataFusion-pool limit also fails before Parquet I/O. Every failed case releases its reservations and leaves no active storage operation.

## Request-delay behavior

| Case | Registration ms | First planning ms | Execution ms |
|---|---:|---:|---:|
| remote-16-10ms-cap1 | 974.05 / 979.77 | 355.60 / 358.97 | 13.68 / 13.69 |
| remote-16-10ms-cap8 | 975.36 / 987.65 | 356.55 / 358.85 | 12.57 / 13.74 |
| remote-256-10ms-cap1 | 1,243.34 / 1,258.00 | 1,379.22 / 1,386.04 | 13.54 / 13.80 |
| remote-256-10ms-cap8 | 1,170.48 / 1,193.84 | 1,294.89 / 1,517.96 | 12.66 / 12.81 |
| remote-1024-10ms-cap1 | 1,519.94 / 1,615.53 | 5,284.18 / 5,524.36 | 12.75 / 12.94 |
| remote-1024-10ms-cap8 | 1,514.22 / 1,522.25 | 5,257.56 / 5,281.32 | 12.67 / 12.79 |
| remote-256-all-10ms | 1,165.33 / 1,170.25 | 16,446.82 / 16,616.43 | 28.27 / 35.21 |
| remote-256-unpruned-10ms | 1,171.75 / 1,404.21 | 16,546.04 / 17,405.26 | 15.54 / 18.43 |

The wrapper delays each stat and range operation. These are deterministic local delay simulations, not HTTP or live AWS/R2 measurements. Actual phase latency includes timer overshoot, local work, and scheduling; “10 ms” is the configured injected delay, not a claim about measured network RTT. The evidence separately records nominal injected time, observed operation time, and phase wall time.

Registration and first planning show one active operation under both metadata caps of one and eight. Raising a concurrency ceiling does not parallelize this single-provider traversal. Broad 256-file planning incurs 1,280 Parquet preflight operations plus 98 metadata operations. Data preflights dominate the total 13.78 seconds of nominal injected delay; observed operation time is 16.413 seconds and phase wall time is 16.447 seconds, including timer overshoot and local work. Execution can overlap data reads: target_partitions is a planning setting, not a universal request cap. The 256-file local broad case reaches 228–256 concurrent data operations, while the 4k and 16k broad cases reach four. Peak counters are cumulative within a process; compare the object-class counters and phase order before interpreting them.

## Large current commits

| Current commit | Commit bytes | Default registration | Copied fixture after small tail: registration ms |
|---|---:|---|---:|
| append-128 | 95,966 | 12.99 / 14.42 ms | 6.38 / 6.84 |
| append-512 | 378,590 | 36.28 / 37.23 ms | 6.94 / 7.33 |
| append-1024 | 755,910 | 66.43 / 85.80 ms | 4.66 / 5.07 |
| append-2048 | 1,511,622 | Resource error (20/20) | 5.33 / 7.11 |
| property-16384 | 17,890 | 5.68 / 6.32 ms | 5.19 / 6.52 |
| property-131072 | 132,578 | 11.86 / 12.83 ms | 6.16 / 6.87 |
| property-524288 | 525,794 | 30.85 / 33.15 ms | 5.99 / 6.34 |
| property-1048576 | 1,050,082 | Resource error (20/20) | 6.33 / 7.26 |
| property-2097152 | 2,098,658 | Resource error (20/20) | 5.40 / 6.04 |

The 2,048-file append and 1 MiB property payload exceed the default SQLite-record bound. The 2 MiB property payload instead exhausts the metadata-cache reservation while validating its commit envelope. Raising maximum record size to 4 MiB and metadata cache to 256 MiB still fails all three cases at the selected-commit query’s internal byte limit (10/10 per case). All copied small-tail controls succeed. The controls are experimental isolation, not a change to published source fixtures.

These are valid, exhaustively verified tables whose current commit shape exceeds reader limits. File count alone is insufficient to describe the supported operating envelope. The selected-commit path needs bounded handling of large commit records if such transaction sizes are a target; the limit must not be worked around by weakening authentication.

## Recommended next work

1. **Fix large-current-commit handling as a compatibility task.** Valid writer output should not depend on adding a small subsequent commit to become readable. Design bounded selected-commit validation across record, query-result and envelope reservations; retain authentication and explicit exhaustion. Reuse every failing/current and succeeding/tail fixture as acceptance evidence.
2. **Profile and batch metadata planning.** The cache experiment supplies a useful existing configuration control, but substantial planning time remains without storage I/O. Trace query counts and time inside the per-file metric loop, keyset enumeration, decoding and pruning. Evaluate batched metric reads within the existing 256-file contract before building an index. Preserve typed pruning, explicit continuation and no-false-negative tests.
3. **Reduce request count and add bounded overlap where useful.** Inspect repeated immutable-object stat validation and serialized footer preflight. Cache hits must still validate every expected reference and preserve version consistency. A higher in-flight setting alone did not speed the measured metadata traversal; measure actual overlapping operations after a change.
4. **Account for broad-plan and execution memory before increasing scale.** Descriptor accounting is predictable, but it explains only part of RSS. Profile allocations and native plan/execution scheduling at 4k and 16k, then attach meaningful reservations or bounds to the dominant retained state. Do not infer a leak from peak RSS alone.

Re-run the same frozen fixtures and distributions after each optimization. The current measurements justify this order of investigation; they do not establish indexed or sublinear metadata planning, network-provider latency, production tail-latency guarantees, or behavior beyond 16,384 files.

## Verification and provenance

Local evidence includes 214 workspace tests, 11 native harness tests, 18 Python qualification tests, and six-sample native/reporting smoke runs. Strict workspace Clippy, doctests, conformance regeneration, deterministic S3/provider evidence tests, crash tests, protocol WASM, cargo-deny, and cargo-audit passed. The existing 128 MiB-plus metadata-image qualification, SQLite oracle, indexed/full-image fixtures, and COW upload evidence remain retained; this slice does not replace them or claim larger-file-count coverage than 16,384.

The provisional first run is retained separately. Python’s timeout-based process wait introduced polling-sized completion delays. The final runner uses a dedicated blocking wait thread and reports result arrival, process exit, capture completion, and both gaps. No provisional wall-time samples are mixed into the final distributions. This correction does not establish the cause of the separate earlier 21.6-second development-build observation.

Main runner source: `44e2a602247d9e8ae1ab9ae0be13c07ffc939960`. Native binary source: `62a8143bc7468b0b50b091d4a26816dec6f45a91`. Main binary SHA-256: `ad04d67c8a3f0c8a9cb90a6ac29510cea1cdf0ff1fdc7dfd73cb521849cb729a`. The binary is archived at `/private/tmp/otmp-reader-scale-evidence-v2/reader_scale-ad04d67c`. Raw main evidence is `/private/tmp/otmp-reader-scale-evidence-v2`; fixtures and preparation logs remain in `/private/tmp/otmp-reader-scale-evidence`. Cache experiment source: `4135b824b5713a81c53b3c78de321cbe084e86a6`; binary SHA-256: `f513ef5cef59045f5fbe1ed31c38edab1e66aca4a2d138782610ce06193a1d4f`. Its binary is archived at `/private/tmp/otmp-reader-scale-cache-evidence/reader_scale-f513ef5c` and its raw evidence is `/private/tmp/otmp-reader-scale-cache-evidence`. The manifest records the exact interleaved order.

The six CI jobs and Audit passed on the main-matrix runner revision `44e2a60` ([CI](https://github.com/daxis-io/OTMP/actions/runs/34153759248), [Audit](https://github.com/daxis-io/OTMP/actions/runs/34153759265)). That run predates the cache-experiment harness. Final-candidate CI is tracked separately on [stacked PR #12](https://github.com/daxis-io/OTMP/pull/12); earlier green checks are not evidence for a later revision.

Full distributions for all 65 configurations are in `results.json.gz`; `manifest.json` records source/build and fixture identities. `raw-index.json.gz` records lengths and SHA-256 hashes for retained raw samples, verification logs and provenance files. The raw artifacts themselves remain at the recorded local evidence roots. No live provider was used, neither reader PR was merged, and no runtime optimization is included in this qualification change.
