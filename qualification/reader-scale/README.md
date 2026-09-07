# Reader scale qualification

This directory contains a small, standard-library-only runner for the native
`reader_scale` example. It measures file-count growth, large current commits,
repeat latency distributions, and deterministic request-delay behavior. It does
not compile the example, prepare fixtures automatically, or flush the operating
system cache. `matrix.py` and `cache_matrix.py` provide the documented experiment
drivers.

The [September 7 qualification report](evidence/2026-09-07/report.md) records
1,130 samples across 65 configurations, the observed limits, source identities,
and the next optimization priorities. Compressed distributions and a raw-evidence
hash index accompany the report.

## Build before measuring

Build the native example before starting a timed run. Use the resulting binary
path for every sample in a comparison:

```sh
cargo build --release -p otmp-datafusion --example reader_scale
READER_SCALE=target/release/examples/reader_scale
```

Compilation is deliberately outside the runner's timer.

## Prepare immutable fixtures

The native CLI accepts a preparation JSON object with these fields and rejects
unknown fields:

```json
{
  "files": 10000,
  "rows_per_file": 128,
  "batch_size": 128,
  "property_bytes": 0,
  "small_tail": true
}
```

Prepare requires a root that does not exist. It writes real Parquet files with
the OTMP writer, verifies the table, and records the fixture identity in
`ROOT/qualification.json`:

```sh
"$READER_SCALE" prepare /private/tmp/otmp-reader-10k prepare-10k.json
```

Treat a prepared root as immutable during measurements. To measure a small
metadata commit after a large current commit, first make a filesystem copy and
then run `tail` only on that explicit copy:

```sh
cp -R /private/tmp/otmp-reader-large-current /private/tmp/otmp-reader-large-tail
"$READER_SCALE" tail /private/tmp/otmp-reader-large-tail
```

`tail` rewrites the copied fixture's qualification manifest. Never point it at
the retained source fixture.

Useful independent fixture axes are 1,000, 10,000, and 100,000 files; and a
fixed file count with increasing `property_bytes` for current-commit size. Start
with the smaller fixtures and inspect resource use before preparing the largest
case.

## Run repeated samples

The native run configuration also rejects unknown fields. A baseline is:

```json
{
  "survivors": 10,
  "pruning": true,
  "passes": 1,
  "delay_ms": 0,
  "metadata_inflight": 8,
  "metadata_budget": 67108864,
  "planning_budget": 67108864,
  "record_budget": 1048576,
  "df_pool_bytes": 268435456,
  "execute": true
}
```

Set `survivors` to `null` to retain every file. Set `pruning` to `false` for an
explicit unpruned comparison. `delay_ms` injects deterministic delay into each
storage operation, while `metadata_inflight` controls the reader concurrency.

Run each sample in a fresh process:

```sh
python3 qualification/reader-scale/run.py run \
  --binary "$READER_SCALE" \
  --root /private/tmp/otmp-reader-10k \
  --config run-pruned.json \
  --out /private/tmp/otmp-reader-10k-pruned-evidence \
  --samples 20 \
  --timeout 300
```

The output path must not already exist. Every `sample-NNNN` directory retains
the exact stdout and stderr, parsed result when valid, process timing, exit
status, timeout state, RSS status, and fixture identity before and after the
sample. A timeout kills the complete process group and remains a failed sample.
Reader errors printed with exit status zero also remain failures for percentile
purposes while preserving all phases observed before the error.

Fixture identity hashes the actual `ROOT/_otmp/HEAD` bytes. The runner also
hashes `qualification.json` and checks its declared `head_sha256` against the
actual HEAD digest. This detects a mutated HEAD even if a stale qualification
manifest remains unchanged.

The top-level manifest records the binary SHA-256, repository commit and dirty
state, original configuration, fixture identity, host, timeout, and sample
count. It explicitly marks the OS cache as uncontrolled. Each process is fresh,
but filesystem pages may remain warm between samples.

`summary.json` reports nearest-rank p50 and p95 along with count, minimum, and
maximum. Successful samples alone feed success latency and resource
distributions. Timeouts, malformed output, nonzero exits, and observed reader
errors are listed separately; failure phases have their own summaries. The
report covers phase latency, whole-process wall time, JSON-result arrival,
post-result teardown, RSS when the platform timer works, I/O by bounded object
class, file counts, reader `cache_bytes`/`peak_cache_bytes`, provider
`footer_cache_bytes`/`peak_footer_cache_bytes`, and DataFusion pool reservations.
Reader bytes, requests, pages, and cache hits are retained alongside provider
Parquet bytes, requests, file counts, and footer-cache hits. Phase keys include
the pass number, such as `planning:0` and `planning:1`, so cold and retained-pin
passes never enter the same distribution. Failure summaries preserve the same
I/O classes, reader/provider counters, memory readings, wall time, and RSS.

Transport `bytes` count successful range-response payload bytes. They exclude
HTTP headers, TLS framing, retries below the object-store interface, and other
wire overhead. `elapsed_us` is summed request time and may exceed phase wall
time when operations overlap. `metadata_inflight` is capped at eight and limits
metadata-reader work only. Reported `peak_inflight` is the cumulative maximum
for the measured store, so later phases can retain a peak established earlier
and data reads can establish a different peak.

On Darwin the runner uses `/usr/bin/time -l`; on Linux it uses
`/usr/bin/time -v`. It probes the timer first because restricted Darwin hosts
can make `time -l` fail after a successful child. When measurement is not
usable, RSS is recorded as missing instead of changing the reader outcome.

## Compare request latency and planning growth

Keep each axis isolated so the next optimization is attributable:

1. Hold commit size and query selectivity fixed while increasing `files`. This
   exposes descriptor enumeration and retained planning-state growth.
2. Hold file count fixed while increasing `property_bytes`. Compare the large
   current commit with the copied fixture after `tail` to isolate current-anchor
   and commit-envelope cost.
3. Repeat the same fixture and config at least 20 times. Use the reported count,
   p50, p95, minimum, and maximum; do not remove slow samples.
4. Repeat with `delay_ms` such as 5, 20, and 50, then vary
   `metadata_inflight`. Compare injected time, storage elapsed time, request
   counts, peak concurrency, and phase wall time. This simulates request delay;
   it is not evidence for AWS, R2, or another live provider.
5. Compare small `survivors`, all survivors, and pruning disabled. File metadata
   may still be enumerated even when almost every Parquet file is pruned.

An example with many metadata files, 20 ms injected request delay, and bounded
concurrency is just another configuration file:

```json
{
  "survivors": 10,
  "pruning": true,
  "passes": 1,
  "delay_ms": 20,
  "metadata_inflight": 8,
  "metadata_budget": 67108864,
  "planning_budget": 67108864,
  "record_budget": 1048576,
  "df_pool_bytes": 268435456,
  "execute": true
}
```

To aggregate already captured evidence without rerunning the child:

```sh
python3 qualification/reader-scale/run.py summarize \
  /private/tmp/otmp-reader-10k-pruned-evidence \
  /private/tmp/otmp-reader-10k-pruned-repeat-evidence \
  --out /private/tmp/otmp-reader-10k-combined-summary.json
```

The summary output must also be a new path. Keep configurations homogeneous
when combining directories; the tool preserves provenance but does not claim
that unlike workloads form one meaningful distribution.

## Reproduce the qualification matrix

`matrix.py` defines 23 fixtures and 62 cases totaling 1,070 fresh-process
samples. Inspect the complete recipe before allocating disk or starting work:

```sh
python3 qualification/reader-scale/matrix.py list
```

Preparation remains outside all reader timers. Preparation can be run by itself,
followed later by measurement, or both can run sequentially:

```sh
python3 qualification/reader-scale/matrix.py prepare --binary "$READER_SCALE" --out EVIDENCE
python3 qualification/reader-scale/matrix.py measure --binary "$READER_SCALE" --out EVIDENCE
python3 qualification/reader-scale/matrix.py all --binary "$READER_SCALE" --out EVIDENCE
```

Use repeated `--case NAME` arguments for an exact subset. An existing fixture is
reused only when its verified manifest, configuration, and actual HEAD identity
match the recipe. Partial or mismatched fixtures fail closed. Existing case
results are never overwritten or silently skipped.

CI uses the smaller native integration smoke, which prepares a 16-file fixture
and runs three optimized plus three unpruned samples while checking that
`planning:0` and `planning:1` remain separate:

```sh
python3 qualification/reader-scale/smoke.py --binary "$READER_SCALE"
```

## Test the runner

The tests use a deterministic fake child and do not build Rust or create scale
fixtures:

```sh
python3 -W error::ResourceWarning -m unittest discover \
  -s qualification/reader-scale/tests -v
```

They cover nearest-rank definitions, separation of failures from success
percentiles, partial failure phases, malformed output, result-arrival versus
teardown time, RSS parsing, timeout process-group cleanup, and output overwrite
protection.

Before `matrix.py measure` starts any timed process, it exhaustively re-verifies
all selected fixtures, including files that selective queries will prune. The
verification logs are retained under `preparation/verify-*`. The standalone
`run.py` command assumes its supplied fixture was verified and stays immutable.
Native results always include a `violations` array; the runner distinguishes
secondary invariant violations from an expected reader resource error. Provider
`planning_micros` measures successful OTMP scan construction (including footer
inspection); the planning phase timer includes DataFusion planning as well.
Failed scan construction may not update the provider timer, so use the separate
failure phase timer for those cases.

The two largest growth fixtures also run with `execute: false`. Comparing their
process RSS with executed broad scans separates memory already reached during
planning from additional execution memory. RSS includes the allocator and all
engine overhead; pool reservations account for the charged descriptors, not all
process allocations. Exhaustive pre-verification reads fixture objects and can
warm the OS cache; “cold” here always refers to new reader caches.

Process completion uses a dedicated blocking wait thread; timeout enforcement
joins that thread against a deadline. This avoids the polling sleeps used by
Python's timeout-based process wait. `process_exit_ms` records process exit;
`capture_complete_ms` (also `wall_ms`) includes output-pipe completion.
`result_to_process_exit_ms` and `capture_after_exit_ms` expose the two gaps.

## Isolate engine page-cache pressure

After the main matrix, compare 4, 16, and 32 MiB engine page caches on the same
16,384-file fixture. This option is separate from the 64 MiB shared metadata
cache. Each process plans and executes twice through one retained provider.
The driver interleaves capacities in balanced rotation blocks (seed 7), with
20 processes per capacity and six or seven appearances in each position. It
exhaustively verifies the fixture first and checks binary and fixture identities
around every sample:

```sh
python3 qualification/reader-scale/cache_matrix.py \
  --binary "$READER_SCALE" \
  --root /private/tmp/otmp-reader-scale-evidence/fixtures/growth-16384 \
  --out /private/tmp/otmp-reader-scale-cache-evidence
```

This is a separate 60-sample experiment, so the complete qualification has
1,130 samples across 65 configurations. `engine_page_cache_bytes` is also
accepted in an ordinary `reader_scale run` configuration; its default remains
4 MiB. Preserve the original measured binary before rebuilding it for this
additional harness option. The report records both source and binary identities.
