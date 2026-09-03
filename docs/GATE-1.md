# OTMP 0.0.2-alpha Gate 1 qualification

## Qualified claim

> **Gate 1 passed for the Rust local/full-image OTMP subset: self-contained
> genesis, pinned catalog-free reads, byte-verified table-relative staging,
> idempotent Parquet-descriptor append, conditional-publication reconciliation,
> and append-safe rebase.**

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
- An idempotency key binds to the canonical logical intent. A committed retry
  returns the originally stored result, including the winning file identities.
- Definite conditional-write conflicts repin and semantically rebase. A rebase
  preserves staged file identities and rebuilds the candidate from the winning
  relational state; it never merges SQLite pages.
- Indeterminate publication is reconciled against readable `HEAD` and bounded
  by a configurable retry policy. Retry exhaustion is reported as retryable.
- The local adapter uses create-only immutable writes, file and directory
  `fsync`, a locked `HEAD` comparison, a same-directory temporary file, atomic
  rename, and final directory `fsync`.

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

The workspace qualification commands are:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features
cargo test --workspace --doc
cargo check -p otmp-protocol --target wasm32-unknown-unknown
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

Gate 1 deliberately does not include:

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
Gate 1 provides no garbage collector.
