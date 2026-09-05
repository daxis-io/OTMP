# Local/full-image qualification

## Qualified claim

> **Qualification demonstrated for the Rust local/full-image OTMP subset: self-contained
> genesis, pinned catalog-free reads, byte-verified table-relative staging,
> idempotent Parquet-descriptor append, conditional-publication reconciliation,
> append-safe rebase, atomic property transactions, historical metadata selection, and retained-history verification.**

This is a local proof of concept, not a production-readiness statement and not a
claim of complete Core Reader or Direct Writer conformance.

## Included behavior

- Genesis starts at `table_version = 0` and `root_revision = 0` with schema ID
  `1`, partition spec `0`, sort order `0`, and a null `main` snapshot.
- Every published version is a complete, sidecar-free SQLite checkpoint using
  `otmp.metadata.sqlite3-cow.v1`, with `page_map: null` and the normative
  domain-separated image-root calculation.
- A pin owns the exact raw `HEAD`, its storage version, the verified semantic
  commit and generation, and a read-only materialized checkpoint. Its status,
  files, and history calls do not reread `HEAD`.
- Data is staged at a table-relative UUIDv7 destination. The runtime checks the
  expected length and SHA-256 while copying, reopens and rehashes the stored
  object, and revalidates it immediately before publication.
- One non-empty atomic Parquet-descriptor append batch to `main` is supported.
  The runtime validates metadata assertions but treats Parquet contents as
  opaque bytes.
- The local/full-image profile accepts its exact `initialize_table`, `set_properties`, and `commit_snapshot`
  operation shapes. It fails closed on every other core or extension operation
  instead of silently treating an unimplemented semantic mutation as valid.
- Pinning verifies the complete supported semantic projection: snapshot fields,
  target ref, add-only change set, immutable file descriptors, partition
  tuples and hashes, and file metrics must agree with the relational image.
- The relational history has no snapshot at genesis and a contiguous global snapshot sequence independent of metadata-only table versions. The local/full-image profile exposes exactly one
  `main` branch, whose ancestry, current snapshot, and sequence allocator are
  checked against that history before reads or writes proceed.
- Property updates and removals share one private SQLite transaction and conditional publication. Every touched key requires an exact `property_is` precondition. Metadata-only commits advance table version without allocating a snapshot or sequence.
- An idempotency key binds to the canonical logical intent. A committed retry
  returns the originally stored result, including the winning file identities.
- Append commit metadata and snapshot metadata are independent stable intent
  inputs. Commit metadata is projected only into semantic and relational commit
  history; snapshot metadata is projected only into the snapshot operation and
  relational snapshot row. Neither is propagated into the other.
- Definite conditional-write conflicts repin and semantically rebase. A rebase
  preserves staged file identities and rebuilds the candidate from the winning
  relational state; it never merges SQLite pages.
- Indeterminate publication is reconciled against readable `HEAD` and bounded
  by a configurable retry policy. Retry exhaustion is reported as retryable.
- The local adapter uses create-only immutable writes, file and directory
  `fsync`, a locked `HEAD` comparison, a same-directory temporary file, atomic
  rename, and final directory `fsync`.

Metadata selection pins current HEAD once, then follows retained content-hashed physical ancestry. Snapshot selectors resolve against that selected image. Current verification checks current tips; retained-history verification replays semantic transitions and verifies historical bytes. Neither scope lists objects or repairs state. CLI status separates the selected metadata coordinates from the current anchor.

## Publication and visibility

The immutable dependency and upload order is:

```text
logical intent -> semantic commit -> SQLite checkpoint -> generation -> HEAD
```

The semantic commit does not refer to the checkpoint or generation. Readers see
a candidate only after conditional `HEAD` publication; abandoned immutable
artifacts are unreachable.

High-level staging owns cleanup until its second idempotency check. If another
attempt already committed, duplicate staged objects are conditionally cleaned
before the stored result is returned. Once candidate construction starts,
verified staged data is retained after later failures for safe retry. The
lower-level `commit_staged_files` API never deletes caller-owned staging.

## Verification evidence

Install rustup, Python 3, bash, and a native C build toolchain. Install the pinned
verification tools in [CONTRIBUTING.md](../CONTRIBUTING.md). The checked-in
Rust toolchain selects all required compiler components. No maintainer-local
files, cloud accounts, credentials, or environment variables are needed.

The workspace qualification commands are:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features
cargo test --workspace --doc
cargo check -p otmp-protocol --target wasm32-unknown-unknown
python3 conformance/regenerate.py --check
bash tests/run-subprocess.sh
cargo deny check
cargo audit
git diff --exit-code
```

The deterministic state-machine tests distinguish definite conflicts from
indeterminate outcomes, cover applied-but-response-lost recovery, two-writer
rebase, stable concurrent idempotency, pinned-reader isolation, invisible
partial uploads, staged-object mutation, retry exhaustion, and the no-listing
normal read path.

`tests/run-subprocess.sh` terminates the CLI at four test-only failpoints: after
staging flush, during temporary `HEAD` creation, after immutable artifact
uploads, and after final `HEAD` rename. A new process verifies and opens either
the old state or the fully committed new state. This is process-crash evidence;
it is not proof for every filesystem, kernel, storage device, or machine-power
failure.

## Deferred work

The local/full-image profile deliberately does not include:

- a complete Core Reader or Direct Writer profile;
- cloud object-store correctness or a production storage adapter;
- Parquet magic, footer, row-count, physical-schema, statistics, partition, or
  sort-order validation;
- page maps, page packs, remote VFS, partial reads, scan projection, or row-order
  semantics;
- deletes, schema evolution, partition evolution, sort evolution, garbage
  collection, or orphan discovery;
- Windows filesystem semantics;
- managed coordination, catalog integration, Durable Objects, deployment, or
  production operations.

Local object versions are fencing values only and are never serialized as
portable object identity. Unreachable immutable artifacts can remain after a
crash or a failed candidate publication; normal readers do not list storage and
The local/full-image profile provides no garbage collector.
