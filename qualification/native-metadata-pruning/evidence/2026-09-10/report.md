# Native metadata pruning and writer qualification

Status: local, credential-free qualification on the final source tree. This is not remote CI, live-provider qualification, deployment, or production evidence.

## Provenance

- Base: `10ceea0d3543c2c288df09d6887f83820143e413`
- Slice 1: `8882984` (`Add v3 ordered metric range queries`)
- Slice 2: `1a85852` (`Prune catalog candidates and reuse validated scans`)
- Host: Apple arm64, Darwin 25.6.0
- Rust: `rustc 1.95.0 (59807616e 2026-04-14)`
- Cargo: `cargo 1.95.0 (f2d3ce0bd 2026-03-21)`
- Writer qualifier binary SHA-256: `5158a529e3109d5ca4e56ea09be5eb95b5dc5c8705f06427f84e3120013ee1ac`
- Frozen Parquet append: 1,705 bytes, SHA-256 `427cc3a1b8834cdfa1da1779a74885f40e3cc6f121f5827e5bf3a1ef4efda42c`
- Fixture configuration SHA-256 values: 256 `351368bab9e464bc0ccc12fbff52661ec78701497f8edf7cdb109f158c43d3ef`; 4,096 `8cb8aff550828d6da0204591ae6543500c118c282b42d137a2144fd980930309`; 16,384 `202ed595b0d5fdb0ea08f7803b4adbc01a31e77d74b522e4fe4e905e73663c49`
- Prepared fixture manifest SHA-256 values: 256 `628ba4608e7d929b84d0e19eb27e1a24bf4274ba4e828c5b88b1b1d2867db4f9`; 4,096 `2ed1f7a74d86bd19b2f3a8645b514d49493d29f5cf5bfaaa5bc214cf641e36c3`; 16,384 `30440d6f38975c684886b1832fc89943e1b1a267576b8a93120ada291d403ace`

The retained mutable fixtures and diagnostic samples are under `/private/tmp/otmp-native-writer-qual`. The structured counts are in `writer-matrix.json` beside this report.

## Writer structural gates

| Files before | Parent SQLite bytes | Limit | Full materializations | Temporary image bytes | Checkpoint fallback | Changed pages | Verified |
| ---: | ---: | ---: | ---: | ---: | :---: | ---: | :---: |
| 256 | 405,504 | informational | 1 | 671,744 | yes | 33 | yes |
| 4,096 | 490,291 | informational | 0 | 0 | no | 35 | yes |
| 16,384 | 2,323,634 | 7,382,742 | 0 | 0 | no | 32 | yes |

The ordinary 16,384-file write consumed 3.15% of the frozen 73,827,422-byte baseline and passed every structural release gate. The 256-file case selected the existing reachable-byte checkpoint fallback; that fallback is outside the ordinary 16,384-file zero-fallback gate.

Two failed 16,384-file samples are retained, not discarded: 11,609,815 bytes before creation-identity indexes, and 8,344,754 bytes with those indexes but the reader-oriented 64 KiB page window. The final writer pin uses a 4 KiB window and reads 2,323,634 bytes.

## Verification

The final source passed:

- `cargo fmt --all --check`
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

The frozen OTMP-versus-DuckLake matrix was not rerun. Its retained worktree is dirty and bound to v2 fixtures; the v3 hard cut requires fresh isolated bindings and service fixtures. No live cloud qualification was attempted. An independent fresh-context review was not performed in this run.
