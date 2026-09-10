# OTMP metadata write-latency qualification

Source: `9cc1f040edaa7485396b31c2e16a7a704c290b52` from exact base `78c3311a5d883fdda3542ff6e5a918bb24fee3ca`.
This is local qualification with uncontrolled OS cache state, not provider or production qualification.

| mode | logical image bytes | count | min ms | p50 ms | p95 ms | max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fresh | 266240 | 20 | 45.317 | 195.303 | 269.940 | 1123.019 |
| pre_pinned | 266240 | 20 | 39.714 | 227.965 | 319.968 | 373.128 |
| fresh | 528384 | 20 | 46.262 | 231.443 | 336.683 | 364.750 |
| pre_pinned | 528384 | 20 | 36.454 | 204.530 | 316.604 | 364.331 |
| fresh | 4460544 | 20 | 61.801 | 142.032 | 289.271 | 2787.536 |
| pre_pinned | 4460544 | 20 | 42.605 | 62.814 | 178.993 | 190.967 |
| fresh | 33853440 | 20 | 194.414 | 211.843 | 256.801 | 258.791 |
| pre_pinned | 33853440 | 20 | 51.111 | 72.606 | 191.611 | 2166.178 |

Scaling classification: **distributed**.

| mode | ranked large-minus-small p50 phase deltas |
| --- | --- |
| fresh | parent_pin=138.372 ms, candidate_build=9.501 ms, idempotency=0.062 ms, head_cas=-0.174 ms, immutable_publication=-127.737 ms |
| pre_pinned | candidate_build=9.703 ms, idempotency=0.037 ms, parent_pin=0.000 ms, head_cas=-0.038 ms, immutable_publication=-164.288 ms |

Recommendation: Treat write scaling as distributed; do not optimize one phase until a follow-up profile identifies a stable shared leader.

Excluded: append staging, conflicts/rebases, live S3/R2, Turso Cloud, production throughput, named branches, generation caching, and incremental validation.
