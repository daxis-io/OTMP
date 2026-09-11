# Native metadata pruning and writer qualification

Status: local, credential-free qualification on an uncommitted review-repair candidate. This is not a frozen source tree, remote CI, live-provider qualification, deployment, or production evidence.

## Provenance

- Base: `10ceea0d3543c2c288df09d6887f83820143e413`
- Slice 1: `8882984` (`Add v3 ordered metric range queries`)
- Slice 2: `1a85852` (`Prune catalog candidates and reuse validated scans`)
- Slice 3: `6c33b49d98c668d23e77ffb70575995c3b26f893` (`Write metadata through authenticated page overlays`)
- Review-repair source diff SHA-256 (`git diff --binary -- otmp otmp-datafusion`): `1b60e5384633e7a7c4cd4d1baf2b531aa6d74e1ea6005fc71d62eac8edc319b9`
- Observed `origin/main`: `9a4e09baecf7c536618dd716b12377b7e0cda10f`; this candidate has not been rebased onto it.
- Host: Apple arm64, Darwin 25.6.0
- Rust: `rustc 1.95.0 (59807616e 2026-04-14)`
- Cargo: `cargo 1.95.0 (f2d3ce0bd 2026-03-21)`
- Writer qualifier binary SHA-256: `02471c03c7aa35a604736f108d889d1f042fde2b1ece7be8d377d51392973874`
- Frozen Parquet append: 1,705 bytes, SHA-256 `427cc3a1b8834cdfa1da1779a74885f40e3cc6f121f5827e5bf3a1ef4efda42c`
- Fixture configuration SHA-256 values: 256 `351368bab9e464bc0ccc12fbff52661ec78701497f8edf7cdb109f158c43d3ef`; 4,096 `8cb8aff550828d6da0204591ae6543500c118c282b42d137a2144fd980930309`; 16,384 `202ed595b0d5fdb0ea08f7803b4adbc01a31e77d74b522e4fe4e905e73663c49`
- Prepared fixture manifest SHA-256 values: 256 `b7cb9723a66eb3578cb7f0a28f6f14790f0a5b76a2a37085ee9606ff8f861e5f`; 4,096 `5866d612419a3184533ef2f02402e95bdc51a12010abb972be15afb7711ebe76`; 16,384 `ca2fcb61220acafbdc7169fc597e48b1db4b40d196cada4997fd0c81a79cf49e`

The fresh one-shot fixtures are retained under `/private/tmp/otmp-native-writer-final.GFZJte`; the earlier diagnostic samples remain under `/private/tmp/otmp-native-writer-qual`. The structured counts are in `writer-matrix.json` beside this report. Every reported fixture was prepared from scratch and mutated once by its measured append.

## Writer structural gates

| Files before | Parent SQLite bytes / requests | Page-map bytes / requests | Limit | Full materializations | Temporary image bytes | Checkpoint fallback | Changed pages | Verified |
| ---: | ---: | ---: | ---: | ---: | ---: | :---: | ---: | :---: |
| 256 | 405,504 / 99 | 33,765 / 1 | informational | 1 | 671,744 | yes | 33 | yes |
| 4,096 | 490,291 / 130 | 43,412 / 3 | informational | 0 | 0 | no | 35 | yes |
| 16,384 | 1,647,803 / 437 | 1,434,448 / 82 | 7,382,742 | 0 | 0 | no | 32 | yes |

The ordinary 16,384-file write consumed 2.23% of the frozen 73,827,422-byte parent-SQLite baseline and passed every structural release gate. Page-map traversal is reported separately instead of being hidden inside the SQLite-page count. The 256-file case selected the existing reachable-byte checkpoint fallback; that fallback is outside the ordinary 16,384-file zero-fallback gate.

Two failed 16,384-file samples are retained, not discarded: 11,609,815 bytes before creation-identity indexes, and 8,344,754 bytes with those indexes but the reader-oriented 64 KiB page window. The review-repair candidate keeps the 4 KiB writer window and reads 1,647,803 parent SQLite bytes.

## Verification

The exact uncommitted review-repair source passed:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo test --workspace --all-features --locked`
- `python3 conformance/regenerate.py --check`
- `python3 conformance/cow.py --check`
- `bash tests/run-subprocess.sh`
- `cargo check -p otmp-protocol --target wasm32-unknown-unknown --locked`
- `cargo deny check`
- `cargo audit --no-fetch`

The subprocess suite covers nine append failpoints plus six metadata crash cases. The workspace suite covers targeted consumed-page and affected-projection corruption, exhaustive detection of unrelated corruption, cache fill sharing and eviction, exact object-version behavior, pruning fallbacks, historical selection, and pagination.

## Open qualification boundaries

The frozen OTMP-versus-DuckLake matrix was not rerun. Its retained worktree is dirty and bound to v2 fixtures; the v3 hard cut requires fresh isolated bindings and service fixtures. No live cloud qualification was attempted. An independent fresh-context review was not performed in this run. The review repair is uncommitted, and reconciliation with the advanced `origin/main` remains a separate authorization boundary.
