# Reader concurrency qualification

The concurrency driver is `../reader-scale/concurrency.py`; it reuses the
reader-scale sample capture, exact-result checks, and summary implementation.
Timing thresholds belong to this manual qualification, while deterministic
Rust and Python checks run through the normal workspace CI commands.

The frozen baseline accepts the extended harness configuration but retains
sequential reader behavior. The eight-query reservation control changes only
when descriptor-result bytes are reserved. It preserves the original reader's
sequential preflight and per-statement engine dispatch. Original baseline
failures remain evidence and must not be described as successful measurements.

Run with separately archived binaries and verified immutable fixture paths:

```sh
python3 qualification/reader-scale/concurrency.py \
  --baseline "$EVIDENCE/reader_scale-instrumentation" \
  --eight-query-baseline "$EVIDENCE/reader_scale-reservation-control" \
  --candidate "$EVIDENCE/reader_scale-candidate-v7" \
  --fixtures "$EVIDENCE/concurrency-fixtures" \
  --out "$EVIDENCE/concurrency-v7"
```

The driver records balanced seeded order, exact commands and configuration,
stdout/stderr, process exit status, per-query timers, shared physical I/O,
reservation peaks, and process RSS where `/usr/bin/time -l` is available.
It uses twenty local/mixed rounds and ten delayed rounds per binary. Fixture
verification runs before and after measurements. A failed gate exits nonzero;
failed or unfinished samples remain in the raw evidence.

The original paired matrix remains `../reader-planning/run.py`. The pinned
Iceberg comparison remains `../reader-comparison/run.py`; its source and
fixture layout are unchanged. Run these separately from builds and other
qualification workloads. A fresh process does not imply a cold OS cache.

See `evidence/2026-09-08/report.md` for the delivery result, source/binary
identities, measured limits, failed experiments, and reproduction commands.

The subsequent [review repairs](evidence/2026-09-08-review-repairs/report.md)
retain V7 unchanged. Their frozen binaries, new measurements, and validation
logs are under `/private/tmp/otmp-reader-review-repairs`; use
`reader_scale-candidate-v8` there as the candidate for a repair rerun.
