# Reader concurrency review repairs

All review findings are resolved. The repaired V8 implementation passes the repository checks, independent follow-up review, and local performance gates. The original V7 evidence remains preserved.

## Scope and provenance

This repair continues the uncommitted `perf/reader-concurrency` worktree at
`/private/tmp/otmp-reader-concurrency`, based on
`88cefa7f6cab3bd7670668cb99c43b974f63bd6e`. The V7 source, binaries, raw captures,
and [original report](../2026-09-08/report.md) remain unchanged. New source and
binary archives, validation logs, failures, and timed captures are retained at
`/private/tmp/otmp-reader-review-repairs`.

DataFusion 55.0.0, Parquet 59.3.0, Turso 0.7.2, and the pinned Iceberg source and
fixture layout remain unchanged. Preflight concurrency defaults to eight;
concurrency one and every existing byte budget remain available unchanged.
This is local implementation and qualification, without a commit, publication,
merge, deployment, or live-provider qualification.

## Findings and repairs

| Finding | Resolution |
|---|---|
| Loom tests omitted the risky OTMP transitions | Four bounded standalone models cover weak upgrade and remove-if-same replacement, shared-waiter cancellation, Payload/Key/Preflight/Retained ownership, reserve/shrink/drop, and FIFO wakeup/head cancellation. The accounting model explores all schedules within a two-preemption bound; it does not model Tokio, Shared, Parquet, or Turso internals. |
| Provider identity reconciliation lacked integration coverage | A 48-case provider matrix covers repeated and conflicting hash/length descriptors, same and later batches, pruned conflicts, branch/history traversal, and concurrency one/eight. It asserts one version acquisition per repeated selected object, exact rows, failure without a plan, and cleanup. Separate checks cover version changes and original error causes. |
| Incomplete qualification evidence could pass | Gates enumerate all required query positions and all 45 named gates. Completion requires exact 20/10 sample totals plus counts for all four latency metrics and throughput. Missing cases, positions, or metric counts now fail. The original V7 data passes the stricter evaluator. |
| Cached preflight leases caused false resource exhaustion | Active cached preflights publish a zero-byte Preflight guard under the cache-entry lock. Admission can wait for those leases to release evicted footer bytes. A separate counter distinguishes active hits from queued load keys, and excludes the requesting reader's own hit. |
| Optioned execution also ignored a peer cached preflight | A deterministic red reproduction confirmed this during re-review. The separate Preflight counter lets option decoding wait for peer validation while avoiding self-wait or waiting on queued load keys. The same exact-budget schedule now passes. |
| Final schema rebinding used an execution reader | Repeated URIs with another recorded schema now use a cloned preflight factory for final validation. A regression test verifies the original pin/footer is reused and the validation lease stays visible to admission. Native execution retains its execution factory. |
| Remaining validation and compatibility items | Added malformed trailer/length/container and projected-away required-field failures under concurrent scans; historical multi-batch pruning through schema evolution; cancellation at decode and schema-validation boundaries; and exact-budget default-to-required-index checks. The alpha enum and lost-const compatibility changes are documented in `docs/DATAFUSION-READER.md`. |

The tight-budget option test explicitly covers the retained default footer plus
its separate conservative option-decode reservation. That exact budget succeeds;
one byte less fails promptly. A budget sufficient only for a cold default decode
is not asserted to fit two different decodes. Synchronous bounded decode/schema
calls are not interrupted mid-stack; cancellation is observed at asynchronous
boundaries, where the new tests assert reservation, registry, and permit cleanup.

The independent [follow-up review](follow-up-review.md) reports **Go** for the
repaired implementation and behavioral-test scope, with no open P0–P2 findings.
Its performance decision was intentionally left to the subsequent timed runs.

## Validation

- Locked workspace nextest, all features: 251 passed, none skipped.
- Strict workspace and comparator Clippy, and both formatting checks: passed.
- Workspace doctests: passed; the targets contain no doctests.
- Reader-scale example: 20 passed; provider example: two passed.
- Python reader-scale harness: 23 passed; comparator harness: four passed.
- Comparator Rust checks: four passed.
- Canonical conformance and protocol WASM: passed.
- Supply-chain checks: passed.
- Local process-crash qualification: four append failpoints and six metadata scenarios passed. This Darwin run does not replace Linux CI qualification.

## Performance qualification

All **45 concurrency gates** and **four original warm gates** passed. All 760 timed processes completed successfully: 340 concurrency, 220 original paired, and 200 pinned-comparator processes. Source/binary hashes, exact sample counts, query positions, raw-derived summaries, and fixture provenance were checked after the runs.

Planning milliseconds, p50 / p95:

| Case | Sequential baseline | V8 |
|---|---:|---:|
| Delayed 256-file broad, cold | 9729.75 / 9766.77 | 3313.56 / 3318.17 |
| Local 256-file broad, warm | 6.01 / 6.28 | 4.22 / 4.35 |
| Local 4096-file selective, warm | 32.48 / 33.10 | 22.77 / 23.07 |
| Local heterogeneous 256-file broad, warm | 5.97 / 6.18 | 4.34 / 4.58 |

Delayed broad planning improves 2.94× at p50 and 2.94× at p95. Warm planning remains within the larger of baseline × 1.05 or baseline + 1 ms.

| Mixed case | Worst small-query p95 ratio, V8/baseline | Throughput ratio, V8/baseline |
|---|---:|---:|
| mixed-broad-small | 0.944 | 1.245 |
| mixed-homogeneous-1 | 0.851 | 1.149 |
| mixed-homogeneous-2 | 0.934 | 1.094 |
| mixed-homogeneous-4 | 0.947 | 1.071 |
| mixed-homogeneous-8 | 0.935 | 1.063 |

The eight-query row uses the preserved reservation-only control described below. These are comparisons with the frozen sequential controls, not evidence that the repair itself improved latency relative to V7.

Separate direct query timers and process RSS for the delayed broad cold case (milliseconds, p50 / p95; RSS in MiB):

| Binary | Planning | Planning through first result | Execution | Complete query | Process RSS p95 |
|---|---:|---:|---:|---:|---:|
| baseline | 9729.75 / 9766.77 | 9760.52 / 9796.10 | 29.35 / 32.26 | 9760.52 / 9796.11 | 64.12 |
| candidate | 3313.56 / 3318.17 | 3340.64 / 3348.05 | 27.10 / 30.77 | 3340.64 / 3348.06 | 60.88 |

Original paired warm planning (milliseconds, p50 / p95):

| Case | Original frozen baseline | V8 |
|---|---:|---:|
| local-4096-2 | 31.32 / 33.63 | 21.88 / 22.31 |
| local-4096-all | 100.04 / 101.83 | 60.32 / 61.50 |
| local-16384-2 | 129.59 / 131.39 | 92.19 / 93.58 |
| local-16384-all | 401.89 / 408.00 | 249.17 / 277.08 |

The 16,384-file broad case records process RSS p95 of 600.34 MiB for V8 versus 507.56 MiB for the frozen baseline. Explicit reservation limits hold. This delivery does not claim an RSS reduction or a total-process memory cap.

Pinned Iceberg comparison: planning through first result (milliseconds, p50 / p95), preserving the original source and fixture layout:

| Case | Pass | OTMP V8 | Iceberg |
|---|---|---:|---:|
| growth-16384-broad-all | cold | 2826.33 / 2855.75 | 728.11 / 741.75 |
| growth-16384-broad-all | retained | 1008.53 / 1028.40 | 659.72 / 671.59 |
| growth-16384-selective-2 | cold | 265.31 / 267.97 | 55.22 / 58.95 |
| growth-16384-selective-2 | retained | 91.52 / 92.72 | 13.43 / 13.93 |
| growth-4096-broad-all | cold | 709.69 / 717.68 | 183.33 / 187.30 |
| growth-4096-broad-all | retained | 251.75 / 255.49 | 166.43 / 170.24 |
| growth-4096-selective-2 | cold | 66.55 / 67.62 | 15.09 / 16.11 |
| growth-4096-selective-2 | retained | 21.62 / 22.03 | 3.70 / 3.79 |

Iceberg remains faster at p50 in every matched SQL case/pass. The separate file-selection probes also completed; their distributions and exact task counts are in the raw results. These local results do not establish general format parity or provider qualification. Every query timer, physical I/O count, memory peak, failure, and process record is retained in the compressed results and raw archive.

The eight-query comparison retains the separately archived reservation-only
sequential control because the original instrumentation baseline cannot complete
those rounds under its eager descriptor reservations. The original failed rounds
remain preserved in the V7 evidence. Every other concurrency case uses the
original instrumentation baseline. Timed matrices run sequentially after builds,
with frozen fixtures, balanced order, twenty local/mixed rounds and ten delayed
rounds. Process RSS remains separate from explicit OTMP reservation budgets.

## Preserved failures and limits

The first cache-hit progress test, final-binding ownership test, and option-peer
progress test each failed before their corresponding repair. Their red logs are
retained. The gate test also demonstrated the missing-evidence failure before the
fix. Test-development failures from duplicate logical fixture entries and missing
schema transaction preconditions are retained and identified as fixture errors.
The initial unrestricted atomic accounting model was interrupted because its
schedule space was too large; the completed model explicitly bounds preemptions
instead of truncating an unknown number of permutations. Initial Clippy findings
were corrected before final validation.

The original V1–V7 failed experiments remain untouched. Runtime isolation,
parallel metadata execution, total-process memory limits, and live-provider
qualification remain outside this repair. The existing runtime-isolation follow-up
continues to define that separate work.

## Evidence and reproduction

`candidate-v8-manifest.json` binds all 239 frozen source files, the source archive,
build profiles, toolchain, commands, and both binary hashes. `validation.json` and
`validation.txt` retain repository checks and development failures.
`qualification.json` records the exact commands and successful process exits for
all three matrices. `final-verification.json` records source, original-evidence,
raw-summary, sample-count, gate, fixture-provenance, and worktree checks.

`raw-samples.json.gz` packages the raw JSON records; `raw-index.json.gz` hashes
every raw file, including stderr and process captures. `results.json.gz` contains
all final distributions. Full source archives, binaries, logs, and raw files remain
at `/private/tmp/otmp-reader-review-repairs`. Run the recorded commands with new
output directories to reproduce; retain the same baseline and fixture paths.

The original worktree and reader-planning checkout were inspected after validation.
Their final heads/status are recorded, and no change was made to either by this
repair. The concurrency source remains uncommitted for review.
