# Local/full-image qualification

OTMP is an experimental catalog-optional runtime. The evidence below applies to
the local/full-image implementation and deterministic S3 adapter contract. It is
not full protocol conformance, live AWS/R2 qualification, or production readiness.
The draft keeps its `0.0.2-alpha` identifiers and can regenerate fixtures before
the official release without compatibility or migration promises.

## Demonstrated capabilities

- Self-contained initialization with schema 1, null main, version/revision/sequence
  zero, full SQLite checkpoints, and `otmp.refs.v1` advertised at genesis.
- Canonical JSON/CBOR and domain-separated hashes; exact object length/hash checks,
  feature capability checks, SQLite integrity/foreign keys, relational invariants,
  and selected commit projection validation.
- Atomic metadata transactions for properties, named branches/tags, and additive
  optional schemas, with ordered operations and requirements against pinned bases.
  Metadata transactions advance table version without creating snapshots.
- Byte-verified staging and append to any existing branch. Snapshot sequence is
  global; parent ancestry follows the selected branch. Parquet remains opaque.
- Stable idempotency results, definite-conflict semantic rebase, and bounded
  indeterminate-publication reconciliation through a shared publication kernel.
- Isolated current and historical pins, separate current anchors and selected
  coordinates, exact ref/snapshot/sequence selectors, and ancestry-derived files.
- Current verification of all branch/tag tips and retained-history verification
  of explicit generations, semantic transitions, and historical user bytes.
  URI-based read deduplication; no listing or orphan discovery. Verification
  enumerates each immutable snapshot's changes once and hashes each distinct user
  object once, discarding its bytes afterward. Deterministic counters cover
  histories of 8, 16, and 32 snapshots and eight physical repacks that share a
  checkpoint. Cached metadata uses shared bytes and checks each reference against
  its previously verified hash and length; relational transition validation still
  processes the retained full images.
- A bounded S3-compatible single-put adapter with a stateful local HTTP test
  endpoint that consumes complete bodies and enforces conditional writes.
  Scenarios cover actual HTTP 412 responses, stale CAS, two competing writers,
  immutable create collisions with matching and mismatching bytes, token
  round-trip, missing-ETag readback, and response loss after applying a write.
  Runtime tests prove reconciliation without double publication and reject
  initialization/publication success until HEAD reads provide a usable ETag.
  Every HTTP scenario asserts zero list requests. Separate unit tests cover body
  limits and unsupported conditional deletion. Live provider evidence remains
  separate and missing credentials produce `not_run`.

[Transactions](TRANSACTIONS.md) defines the request matrix and failure behavior.
The canonical `conformance/tables/transactions` package retains versions 0–7,
including metadata-only versions and schema evolution. Its retained commits
regenerate byte-identical checkpoints with the pinned SQLite implementation.

## Reproduce from a clean clone

Install rustup, Python 3, bash, and a native C/C++ build toolchain with CMake
(required by the bundled SQLite and TLS dependencies). `rust-toolchain.toml`
selects Rust 1.95.0, rustfmt, Clippy, and the Wasm target. Install the pinned
verification tools as documented in [CONTRIBUTING.md](../CONTRIBUTING.md).
No maintainer-local files, credentials, cloud accounts, or environment variables
are needed. The S3 endpoint tests bind loopback sockets.

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features
cargo test --workspace --doc
cargo test -p otmp-s3 --example provider_evidence
python3 conformance/regenerate.py --check
cargo test -p otmp --test static_tables
cargo check -p otmp-protocol --target wasm32-unknown-unknown
bash tests/run-subprocess.sh
cargo deny check
cargo audit
git diff --exit-code
```

The default CI jobs are `quality`, `test`, `conformance`, `crash-linux`,
`wasm-protocol`, and `supply-chain`. `audit` also runs on PRs, main, a schedule,
and manual dispatch, but is not a required ruleset check. Provider workflows are
manual/nightly and never use secrets in default PR CI.

## Crash evidence and visibility

Publication orders immutable semantic commit, checkpoint, and generation before
conditional HEAD replacement. The local adapter flushes files/directories,
compares HEAD under a lock, writes a same-directory temporary HEAD, and renames
atomically. Candidates remain invisible until publication; abandoned immutable
objects are allowed.

The subprocess suite runs four append failpoints (staging flush, temporary HEAD,
immutable uploads, final rename) and three publication failpoints for each of
property-only and ref-only transactions. A new process opens/verifies either the
old or complete new version and checks SQLite integrity. Metadata crash cases
assert zero snapshots and unchanged sequence. This is process-crash evidence,
not proof for machine power loss, every filesystem/device, or Windows semantics.

## Exclusions and evidence boundaries

Excluded: production guarantees, complete Core Reader/Direct Writer conformance,
physical Parquet validation, deletes/rewrites, partition/sort evolution, feature
upgrades, GC, listing, page maps/packs, remote VFS, projection, catalog coordination,
deployment, and release compatibility. Live AWS and R2 behavior requires actual
provider artifacts. Deterministic success or credential-missing runs cannot be
promoted into provider-qualified status.

Storage CAS tokens remain private runtime state. They never become portable
protocol object identities or public metadata coordinates. Unreachable staged
and immutable objects may require later lifecycle cleanup; no GC is implemented.
