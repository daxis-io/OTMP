# OTMP metadata write-latency qualification

Source: `b4bf8294ae7ff18f58c53d311e76c1a586786592` from exact base `78c3311a5d883fdda3542ff6e5a918bb24fee3ca`.
This is local qualification with uncontrolled OS cache state, not provider or production qualification.

| mode | logical image bytes | count | min ms | p50 ms | p95 ms | max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fresh | 266240 | 20 | 39.935 | 116.713 | 240.918 | 273.426 |
| pre_pinned | 266240 | 20 | 37.124 | 189.114 | 269.427 | 355.808 |
| fresh | 528384 | 20 | 42.496 | 175.110 | 219.838 | 284.494 |
| pre_pinned | 528384 | 20 | 35.579 | 161.067 | 219.599 | 232.330 |
| fresh | 4460544 | 20 | 60.981 | 119.298 | 193.477 | 235.587 |
| pre_pinned | 4460544 | 20 | 40.438 | 70.921 | 119.251 | 164.232 |
| fresh | 33853440 | 20 | 192.509 | 206.841 | 310.519 | 391.776 |
| pre_pinned | 33853440 | 19 | 50.740 | 64.867 | 118.676 | 118.676 |

Scaling classification: **distributed**.

| mode | ranked large-minus-small p50 phase deltas |
| --- | --- |
| fresh | parent_pin=139.209 ms, candidate_build=9.290 ms, idempotency=0.069 ms, head_cas=0.062 ms, immutable_publication=-5.713 ms |
| pre_pinned | candidate_build=9.642 ms, idempotency=0.072 ms, parent_pin=0.000 ms, head_cas=-0.594 ms, immutable_publication=-138.684 ms |

Recommendation: Treat write scaling as distributed; do not optimize one phase until a follow-up profile identifies a stable shared leader.

Excluded: append staging, conflicts/rebases, live S3/R2, Turso Cloud, production throughput, named branches, generation caching, and incremental validation.
