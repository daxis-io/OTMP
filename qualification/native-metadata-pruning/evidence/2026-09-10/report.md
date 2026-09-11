# Native metadata pruning and writer qualification

Status: local qualification on the committed source reconciled with `origin/main`. Local PostgreSQL and S3-compatible services were used only by the DuckLake comparison harness. This is not remote CI, live-provider qualification, deployment, or production evidence.

## Provenance

- Base and observed `origin/main`: `9a4e09baecf7c536618dd716b12377b7e0cda10f`
- Slice 1: `5b3f4ce` (`Add v3 ordered metric range queries`)
- Slice 2: `6af4c4d` (`Prune catalog candidates and reuse validated scans`)
- Slice 3: `3b9df6d` (`Write metadata through authenticated page overlays`)
- Review repair: `7213d70` (`Address native metadata pruning audit findings`)
- Reconciliation repairs: `b7e0980` (`Bound reconciled writer qualification futures`), `5c689fd` (`Keep shared validation fills alive for peer scans`), `f6c0081` (`Preserve ambiguous appends and deep snapshot histories`), `6beca77` (`Make shared validation cache assertion deterministic`), `fbfaa23` (`Remove racy duplicate cancellation test`), and `363c57a` (`Page append rebase validation history`)
- Final source: `363c57ad21619d88d8cc4fe971a01f71bd324dcc`
- Final source diff SHA-256 (`git diff --binary origin/main...363c57a -- otmp otmp-datafusion`): `c929a5df5a4576d86df8e061ab5291539e749196d6c988ea358362f2f0722bcd`
- Host: Apple arm64, Darwin 25.6.0
- Rust: `rustc 1.95.0 (59807616e 2026-04-14)`
- Cargo: `cargo 1.95.0 (f2d3ce0bd 2026-03-21)`
- Writer qualifier binary SHA-256: `7e514124d383235662222f1466f84a68bc9864cdbc899bed45bd8067671c2119`
- Frozen Parquet append: 1,705 bytes, SHA-256 `427cc3a1b8834cdfa1da1779a74885f40e3cc6f121f5827e5bf3a1ef4efda42c`
- Fixture manifest SHA-256 values: 256 `747c71cce04408a2b4c1a542b68bbf4c0e487792942339e5a192f28fc26c50a1`; 4,096 `ef659184fc1fbed6d0da9a1ed1812648cca005ad9e608225d5625c193fd5ec54`; 16,384 `b21cd4937129cc1ff5d72f4259f276f5d4db7398a76458ccdbe5013c0743f358`

The final-source one-shot fixtures are retained under `/private/tmp/otmp-native-writer-363.84iyfX`; earlier diagnostic samples remain retained separately. The structured counts are in `writer-matrix.json` beside this report. Every reported fixture was copied from a frozen source into a fresh owned root and mutated once by its measured append.

## Writer structural gates

| Files before | Parent SQLite bytes / requests | Page-map bytes / requests | Limit | Full materializations | Temporary image bytes | Checkpoint fallback | Changed pages | Verified |
| ---: | ---: | ---: | ---: | ---: | ---: | :---: | ---: | :---: |
| 256 | 405,504 / 99 | 33,765 / 1 | informational | 1 | 671,744 | yes | 33 | yes |
| 4,096 | 490,291 / 130 | 43,412 / 3 | informational | 0 | 0 | no | 35 | yes |
| 16,384 | 5,236,415 / 1,318 | 1,162,101 / 70 | 7,382,742 | 0 | 0 | no | 30 | yes |

The ordinary 16,384-file write consumed 7.09% of the frozen 73,827,422-byte parent-SQLite baseline and passed every structural release gate. Page-map traversal is reported separately instead of being hidden inside the SQLite-page count. The 256-file case selected the existing reachable-byte checkpoint fallback; that fallback is outside the ordinary 16,384-file zero-fallback gate.

Two earlier failed 16,384-file samples are retained, not discarded: 11,609,815 bytes before creation-identity indexes, and 8,344,754 bytes with those indexes but the reader-oriented 64 KiB page window. The final source keeps the 4 KiB writer window and reads 5,236,415 parent SQLite bytes on the fresh final fixture.

## OTMP-versus-DuckLake matrix

The final matrix is retained under `/private/tmp/otmp-ducklake-reconciled-evidence.2dLJux/final-matrix-363`. It used the final OTMP source and a worker with SHA-256 `f5a2e7212841fa35a96a6975fcdda28ed2b1fa64b854a62bcff564887f97f495`. The 180 bindings had SHA-256 `b43e4bb47dbd9f642d44cb268d9402cdb45dc7f7648f8a0f2c151b9e00aaad0b`; there were no missing or extra groups.

- Untimed smoke: 172 of 180 groups passed. Results SHA-256: `caf73d72d398ee5938248a5aad1a6ef70cc2220a7d2852f4966e3722642448a2`.
- Timed matrix: 2,990 of 3,150 declared samples passed; all 3,150 were recorded. Manifest SHA-256: `0d0e0ade9a14d13c3afa093bacbcd5e6c89878d6e1b2820123b55788204d8726`; outcomes SHA-256: `4175642b5d4b21365ed88c0547ff93afa817adc85b88e20cdf20f2ed0d4c483b`; detailed report SHA-256: `7f746facb960342b142d82891ad4e0e92d7b048535d413380b2fc0687867f3f6`.
- All before/after artifact and fixture guards passed. Every successful sample matched its frozen row hash and logical types.
- The 160 failures were retained: 120 samples in six OTMP 16,384-file broad/evolved/history groups exhausted the default 64 MiB validated-footer cache, and 40 samples in the two OTMP 256-file evolved/history groups exhausted DataFusion's fixed 256 MiB external-sort pool. No comparator lane failed.
- For the 16,384-file two-row local scan, OTMP planning p50/p95 was 207.253/212.641 ms cold and 35.426/36.225 ms retained; complete-query p50/p95 was 207.796/213.136 ms cold and 35.705/36.555 ms retained. The complete-query DuckLake SQLite comparator was 16.120/16.490 ms cold and 14.325/15.244 ms retained.

The matrix is therefore complete but not qualified. Latency values are observations, not release gates; structural work counts and result parity remain the gates for this change.

## Verification

The exact committed, reconciled source passed:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo test --workspace --all-targets --all-features --locked`
- `cargo test --workspace --doc --all-features --locked`
- `python3 conformance/regenerate.py --check`
- `python3 conformance/cow.py --check`
- `bash tests/run-subprocess.sh`
- `cargo check -p otmp-protocol --target wasm32-unknown-unknown --locked`
- `cargo deny check`
- `cargo audit`

The subprocess suite covers nine append failpoints plus six metadata crash cases. The workspace suite covers targeted consumed-page and affected-projection corruption, exhaustive detection of unrelated corruption, cache fill sharing and eviction, exact object-version behavior, pruning fallbacks, historical selection, and pagination.

## Open qualification boundaries

An independent fresh-context correctness review follows this final-source evidence commit. No live cloud qualification was attempted. Push, remote CI, merge, deployment, and production qualification remain separate boundaries.
