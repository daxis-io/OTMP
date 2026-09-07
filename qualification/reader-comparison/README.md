# OTMP and Iceberg reader comparison

This standalone binary compares the current OTMP `TableProvider` with Apache
Iceberg's snapshot-pinned `IcebergStaticTableProvider` over the same Parquet
bytes and the same DataFusion query. It is an independent Cargo workspace under
`qualification/` so it does not change the root workspace dependency graph or
lockfile.

The dependency graph pins DataFusion `55.0.0` and Apache Iceberg Rust revision
`28ede505ebc3a274d4840624e5c85eae480a77ae`. That Iceberg revision declares
DataFusion 55. The initially requested revision
`d82481f96dd04d42eaf66d10f480d42131d8d4c3` actually declares DataFusion 54.1
and Arrow / Parquet 58.4; its failed compatibility build is retained with the
qualification evidence. The published `iceberg-datafusion 0.10.1` crate declares
DataFusion 53.1 and is intentionally not used. The OTMP path dependencies are
repository-relative paths to the candidate checkout.

Do not benchmark until `Cargo.lock` has been generated, reviewed, and retained.
Before measuring, confirm the lock has one DataFusion 55 graph and one Arrow /
Parquet 59 graph:

```sh
cargo tree --manifest-path qualification/reader-comparison/Cargo.toml --locked -d
CARGO_TARGET_DIR=/private/tmp/otmp-reader-comparison-target \
  cargo build --manifest-path qualification/reader-comparison/Cargo.toml \
  --locked --release
```

Preparation creates a new Iceberg table and never writes into `SOURCE_ROOT`.
It hard-links each source Parquet object when possible and otherwise copies it,
checks every source and derived SHA-256, reads the real Parquet rows to derive
file bounds, creates an unpartitioned Iceberg v2 table with one manifest, and
records all identities in `DERIVED_ROOT/qualification.json`.

```sh
BIN=/private/tmp/otmp-reader-comparison-target/release/otmp-reader-comparison
$BIN prepare \
  /private/tmp/otmp-reader-scale-evidence/fixtures/growth-4096 \
  /private/tmp/otmp-reader-comparison-fixtures/growth-4096
$BIN verify \
  /private/tmp/otmp-reader-scale-evidence/fixtures/growth-4096 \
  /private/tmp/otmp-reader-comparison-fixtures/growth-4096
```

Run `verify` in a separate subprocess before a measurement series. It hashes
all source data, derived data, and Iceberg metadata. `run` checks identity
anchors only, avoiding a full pre-query read that would warm every measured
file. Each measurement sample should use a fresh subprocess. OS cache state is
explicitly uncontrolled, so run repeated interleaved samples and report the
distribution rather than a single value.

The default query retains the last two files and runs twice through one pinned
provider:

```sh
$BIN run otmp SOURCE_ROOT DERIVED_ROOT
$BIN run iceberg SOURCE_ROOT DERIVED_ROOT
$BIN run otmp SOURCE_ROOT DERIVED_ROOT --survivors all --passes 2
$BIN run iceberg SOURCE_ROOT DERIVED_ROOT --survivors all --passes 2
```

Both formats execute:

```sql
SELECT count(*) AS n, coalesce(sum(id), 0) AS total
FROM t
WHERE id >= :first
```

Both use `SessionConfig::with_target_partitions(4)`, a 256 MiB DataFusion
`GreedyMemoryPool`, and the same compiled DataFusion / Arrow / Parquet graph.
OTMP uses its default 64 MiB metadata and planning budgets with a 4 MiB engine
page cache.

The JSON phases are `fixture_validation`, `initialization`, `setup`,
`planning`, `execution_to_first_batch`, `execution_rest`, `plan_release`, and
`teardown`. `planning` has exactly the OTMP qualification boundary:
`SessionContext::sql` plus `DataFrame::create_physical_plan`.

Iceberg's provider returns an `IcebergTableScan` at that boundary. It reads the
manifest list, manifests, and file tasks only when execution is polled. For
that reason, provider `planning` alone is not a matched performance result.
The primary matched readiness measure is `planning` plus
`execution_to_first_batch`. With this aggregate query, the first result arrives
after selected data has been read, so the JSON describes that phase as metadata
selection plus data scan and aggregation for Iceberg. OTMP reports its observed
files considered, pruned, and opened during planning; Iceberg does not expose
equivalent provider counters.

Use a separate process to isolate Iceberg's metadata-only file-selection cost:

```sh
$BIN plan-files SOURCE_ROOT DERIVED_ROOT --survivors 2 --passes 2
```

`plan-files` calls `TableScan::plan_files`, consumes all returned tasks, checks
the task count, and opens no data files. Keep these samples separate from the
provider-query samples because they populate Iceberg's table metadata cache.
Report initialization, direct metadata file selection, provider planning, and
end-to-end query latency independently.

`run.py` drives the complete comparison after both fixtures have been prepared.
It verifies each fixture before and after the sample loop, then runs 20 fresh
processes per format for each file-count/selection case. OTMP and Iceberg run as
adjacent pairs, with each format in the first position ten times. It separately
runs ten fresh `plan-files` processes per Iceberg case. Raw stdout, stderr,
process timing, RSS, failures, schedules, and verification records remain under
the new output directory. Successful samples use nearest-rank p50 and p95;
failed samples are reported separately and never enter those distributions.

```sh
python3 run.py \
  --binary /private/tmp/otmp-reader-comparison-target/release/otmp-reader-comparison \
  --lock qualification/reader-comparison/Cargo.lock \
  --source-repo . \
  --source-4096 /private/tmp/otmp-reader-scale-evidence/fixtures/growth-4096 \
  --derived-4096 /private/tmp/otmp-reader-comparison-evidence/fixtures/growth-4096 \
  --source-16384 /private/tmp/otmp-reader-scale-evidence/fixtures/growth-16384 \
  --derived-16384 /private/tmp/otmp-reader-comparison-evidence/fixtures/growth-16384 \
  --out /private/tmp/otmp-reader-comparison-results
```

This is 160 provider-query processes and 40 direct Iceberg file-selection
processes, plus four untimed verification processes. The runner refuses to
overwrite an existing output directory and records the binary, lockfile,
source Git state, and fixture identities before and after the run. Run it only
after the OTMP candidate source and release binary are frozen.

Fixture generation is part of provenance, not a timed benchmark phase. The
Iceberg fixture has one manifest containing all current data files, while OTMP
uses its authenticated relational metadata image. Compare observed latency and
growth under these stated layouts; do not claim that one layout predicts every
production Delta or Iceberg catalog. Delta is excluded because current stock
delta-rs provider releases do not share DataFusion 55 and Arrow 59 with OTMP.
