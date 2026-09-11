# Planning regression measurements

Run the frozen baseline and candidate through the existing reader-scale harness
in balanced interleaved fresh processes. The matrix measures 4k/16k selective
and broad scans (20 samples per binary), plus 10 ms per-request simulations
(10 samples per binary). Each process plans and executes twice through the same
provider. No operating-system cache flush is claimed.

```sh
python3 qualification/reader-planning/run.py \
  --baseline /path/to/archived/reader_scale \
  --candidate /path/to/candidate/reader_scale \
  --fixtures /path/to/frozen/fixtures \
  --out /path/to/new/evidence-directory
```

The existing writer-produced fixtures must include growth-16, growth-256,
growth-1024, growth-4096, and growth-16384. Full verification runs outside the
timed samples, before and after the matrix. All raw outputs and errors are
retained. Simulated delay tests are not live object-store qualification.

The [2026-09-07 measured report](evidence/2026-09-07/report.md) includes source pins, p50/p95,
request counts, memory, raw captures and the remaining Iceberg performance gap.
