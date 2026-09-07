# COW landing review

Reviewed candidate: `7415d564792a09ac02fa4544ae68510db6592772`.
Actual base: `679711712abb707f370a44b8da220116e9a1ff19`.
Worktree: `/private/tmp/otmp-turso-cow`, clean before and after qualification.
Environment: Darwin arm64, repository-pinned Rust 1.95.0.

## Verdict

Approve the implementation for the repository CI gate. No demonstrated P0, P1,
or P2 finding remains from this review. Remote Linux execution and the six-job
CI matrix must pass before squash merging. This report records local evidence;
the PR checks supply remote evidence.

## Completion check

| Criterion | Evidence | Status |
| --- | --- | --- |
| Ordered authenticated maps and validated pack references | Protocol fixtures, malformed maps/packs, subtree maxima, conflicting references, bounded decompression | Met |
| Private overlays and truncation/regrowth | Exact ordinary-file comparison, repeated writes, discarded tails, freeze failure guard | Met |
| Checkpoint fallback and structural reuse | Large-update checkpoint, small-update upload probe, multilevel tree tests | Met |
| Publication and fresh-parent retry | Partial artifact failures, indeterminate results, concurrent writers, retained staging, replay oracle | Met |
| Existing qualification matrix locally | Commands below | Met |
| All six remote CI jobs and resulting main CI | Required PR and main checks | Pending at review time |

## Correctness and edge cases

The materializer verifies the checkpoint's own table/version/page geometry,
the generation image root, page-map ordering and child maxima, explicit coverage
of extended pages, pack index agreement, raw page hashes, and final header length.
Updates prune mappings beyond EOF and reuse untouched subtrees. Overlay shrinkage
reduces the surviving parent prefix; later growth cannot expose discarded bytes.
No material correctness finding was demonstrated.

## Design quality

Semantic mutation SQL is shared with the stock SQLite replay oracle. Turso owns
private candidate execution; persistent physical objects remain an OTMP concern.
Pinned logical bytes and checkpoint identity are retained separately. No material
design finding was demonstrated within the accepted materialized-reader scope.

## Reliability

Only a successful commit, explicit checkpoint, close, and sticky storage-failure
check can freeze a candidate. Publication writes immutable artifacts before HEAD.
A definite conflict rebuilds against a newly pinned winner. Tests exercise both
applied-but-lost responses and two real writers from one parent. No material
reliability finding was demonstrated.

## Security

Protocol parsing checks canonical encodings, integer/range bounds, reserved bytes,
hashes, and repeated references. Decompression uses a page-sized destination.
Existing relative-URI validation and conditional storage publication remain in
force. No credentialed provider activity was performed. No material security
finding was demonstrated.

## Performance

The existing evidence measures reduced writable copying and uploaded artifacts.
Pins, SQLite validation, reachable-object accounting, and historical replay still
perform exhaustive work and can retain full images and object trees. These are
documented costs, not evidence for bounded remote reading. The separate reader
delivery must establish its own cache, range, registration, and planning budgets.

## Tests

Fresh terminal results for the reviewed implementation:

- `cargo nextest run --workspace --all-features --locked --no-fail-fast`:
  136 passed, zero skipped, no leak annotation. The first sandboxed attempt failed
  at a loopback bind; the authorized rerun includes all deterministic S3 contracts.
- `cargo fmt --all --check` and
  `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`:
  passed.
- `cargo test --workspace --doc --locked` and
  `cargo test -p otmp-s3 --example provider_evidence --locked`: passed; the example
  executes two harness tests and makes no live-provider claim.
- `python3 conformance/regenerate.py --check`: passed, including independent COW
  reconstruction of versions 0–2. `git diff --exit-code` remained clean afterward.
- `cargo check -p otmp-protocol --target wasm32-unknown-unknown --locked`: passed.
- `bash tests/run-subprocess.sh`: four append and six metadata crash scenarios
  passed with independent SQLite reopening.
- `cargo deny check` and `cargo audit`: passed after enabling advisory-cache
  writes; audit scanned 350 locked dependencies.
- `git diff --check`: passed.

## Style and hygiene

The draft identifiers and retained full-image fixtures are preserved. Formatting,
Clippy, dependency policy, and fixture cleanliness passed. The prior qualification
report and worktree remain retained. No material hygiene finding was demonstrated.

## Action items

- P0: none demonstrated.
- P1: none demonstrated.
- P2: none demonstrated.
- Delivery gate: require all six PR CI jobs, squash merge, verify the resulting
  main commit and its CI before creating the reader branch.

Live AWS/R2 qualification remains outside this evidence.
