# OTMP metadata write-latency qualification

Source: `95914fedf421904ae34a7b060afb83d15e2e31dd` from exact base `78c3311a5d883fdda3542ff6e5a918bb24fee3ca`.
This is local qualification with uncontrolled OS cache state, not provider or production qualification.

| mode | logical image bytes | count | min ms | p50 ms | p95 ms | max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fresh | 266240 | 20 | 43.493 | 142.953 | 199.124 | 208.061 |
| pre_pinned | 266240 | 20 | 35.367 | 50.776 | 205.579 | 270.257 |
| fresh | 528384 | 20 | 41.501 | 164.533 | 280.039 | 282.706 |
| pre_pinned | 528384 | 20 | 36.868 | 132.965 | 197.328 | 219.018 |
| fresh | 4460544 | 20 | 63.280 | 119.918 | 198.265 | 242.033 |
| pre_pinned | 4460544 | 20 | 37.540 | 69.451 | 114.029 | 115.390 |
| fresh | 33853440 | 20 | 192.055 | 206.224 | 245.474 | 246.849 |
| pre_pinned | 33853440 | 20 | 50.529 | 66.273 | 95.424 | 105.042 |

Scaling classification: **distributed**.

| mode | ranked large-minus-small p50 phase deltas |
| --- | --- |
| fresh | parent_pin=139.734 ms, candidate_build=9.786 ms, head_cas=0.873 ms, idempotency=0.041 ms, immutable_publication=-84.401 ms |
| pre_pinned | candidate_build=9.916 ms, immutable_publication=8.974 ms, head_cas=1.281 ms, idempotency=0.049 ms, parent_pin=0.000 ms |

Recommendation: Treat write scaling as distributed; do not optimize one phase until a follow-up profile identifies a stable shared leader.

Excluded: append staging, conflicts/rebases, live S3/R2, Turso Cloud, production throughput, named branches, generation caching, and incremental validation.
