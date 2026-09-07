# Reader planning performance

The user requires planning performance competitive with Delta and Iceberg. The
qualification baseline is ca11c04 and its frozen 4k/16k real-Parquet fixtures;
keep that branch, both archived binaries, all failures and all measurements.
Work here is isolated in /private/tmp/otmp-reader-planning.

## Diagnosis and acceptance

The archived binary reproduces 16k retained planning at 3.79 seconds median.
The first investigation budget is 250 ms for this selective local workload;
this is an internal progress gate, not a claimed Delta/Iceberg benchmark result.
A matched comparison must pin the query engine, source, files, schema, predicate,
cache state and snapshot lifetime and measure initialization separately.

A sampled baseline and Turso EXPLAIN show that branch pagination scans membership
by ref_name and sorts on every 256-file batch. Merely using tuple comparison or
ordering by file ID still lets the engine choose an unsuitable index. Forcing
the existing draft-schema primary-key index yields ref_name equality plus a
file_id range seek, without a sorter. A deterministic engine-plan regression
test reproduces the defect before the change. Existing branch/tag/history
oracle tests verify membership remains the same despite changing traversal order.

## Delivery sequence

1. Use the existing primary-key index and an opaque file-ID cursor. Measure this
   change alone against the archived baseline before proceeding.
2. Measure and eliminate per-file metric query dispatch with bounded batched
   queries. Preserve per-record checks, metric decoding, memory reservations,
   missing-stat conservatism, continuation, pinned histories and cancellation.
3. Profile any remaining dominant cost. Optimize repeated authenticated object
   work or footer preflight only with demonstrated benefit and integrity tests.
4. Compare real Delta/Iceberg planning where a compatible pinned implementation
   is available. Do not represent raw DataFusion scans as table-format parity.
5. Run repeated cold/retained, broad/selective and request-delay measurements;
   preserve all distributions and verify result parity and configured budgets.
6. Run the repository qualification checks, document exact measured gains and
   remaining gaps, and publish a stacked PR with final-commit CI. No merges or
   live provider tests are part of this performance work.

No format change, new metric index, weakened authentication, materialized
registration, automatic refresh, or larger default cache is required for the
first two changes. Keep large-current-commit availability findings separate.
