# Reader scale qualification implementation plan

> Execute with the executing-plans workflow. The user approved this qualification slice; routine implementation choices below do not require a new design approval.

Goal: identify whether file enumeration, current commit validation, transport round trips, or retained descriptors limit the existing reader before selecting an optimization.

Base: reader PR #11, c72c3ef0df001852a0d468978397b3cb0facf28d. Preserve existing worktrees and evidence. This branch adds qualification tooling and tests; measured limits are findings, not reasons to weaken reader validation or hide failed runs.

## 1. Instrumented read-only workload driver

Create otmp-datafusion/examples/reader_scale.rs and a support module under examples/reader_scale/. Build writer-generated real Parquet fixtures with configurable file count, rows/file and append batch size. Keep preparation and exhaustive verification outside measurement. Record exact HEAD/generation/commit identities and metadata image, commit and data sizes. Offer a small follow-up commit to isolate file-count effects from current commit size.

Write tests first for operation classification/accounting, delayed read concurrency, cancellation release and fixture/query result calculation. Implement a read-only wrapper over local storage that forbids full reads and mutations, applies explicit simulated latency, and records stat versus range counts, bytes, elapsed operation time and peak in-flight operations by object class. The same wrapper supplies metadata and data I/O.

Measure provider registration, context setup, planning, execution and teardown separately, including partial failures and structured error chains. Assert expected SQL results and configured budgets. Support optimized/unpruned comparison, selective/all-file queries, retained-provider warm execution, and configurable reader/plan budgets and metadata concurrency.

## 2. Subprocess matrix and statistical reporting

Create qualification/reader-scale/run.py, tests for its statistical aggregation, and a documented matrix. Run each cold sample in a fresh process with no compilation inside the timed interval. Record raw JSON, stderr, process wall time, result-emission time, exit status and peak RSS. Never combine failures with successful latency distributions. Use nearest-rank percentiles and report sample counts, median, p95, min/max; OS caches remain uncontrolled and explicitly labelled.

Start with file counts 16, 256, 1,024, 4,096 and 16,384 (and extend where practical). Exercise fixed survivor count, all survivors, and unpruned parity. Probe large append commits and property payloads around the existing record/envelope limits, both as current HEAD and followed by a small commit. Probe lowered planning budgets to establish retained-descriptor slopes and failure boundaries.

Compare local zero-delay runs with simulated 1 ms and 10 ms per storage operation on representative sizes. Compare metadata in-flight caps of 1 and 8 and cold versus retained-provider passes. Remote simulation is not HTTP/S3 or live-provider qualification; retain and run the existing deterministic S3 HTTP contract suite separately.

## 3. Qualification and delivery

Run meaningful driver/aggregation tests, a small automatic matrix, the complete workspace matrix and existing deterministic S3 tests. Run the larger matrix locally with retained datasets and per-case evidence; preserve every failure. Check disk headroom before extending scale. Record source identity and benchmark environment. Explain observed scaling, supported operating envelope, uncertainty and the next optimization justified by the measurements.

Commit tooling separately from evidence/report, push a stacked PR against the existing reader branch if PR #11 remains open, and verify remote CI against the final candidate. Do not merge either reader PR or run live cloud qualification as part of this slice.
