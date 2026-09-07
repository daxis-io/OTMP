# Incremental metadata images

OTMP executes candidates with embedded `turso_core = "=0.7.2"` and retains stock
SQLite for materialized reads, integrity checks, foreign-key checks, and semantic
replay. This is ordinary private WAL execution, not Turso Cloud or MVCC concurrency.
The public table, pin, append, and transaction interfaces remain unchanged.

## Candidate execution

A pin retains the base checkpoint separately from its resolved logical bytes and
verified persistent map. A candidate borrows the resolved bytes through `Arc`.
The Turso `DatabaseStorage` adapter reads its private overlay first and then the
parent. It captures writes starting with engine initialization. Each attempt has
its own temporary database/WAL identity, including retries after a CAS conflict.

The overlay owns its logical length and the surviving prefix of the parent.
Truncation removes overlay pages and zeros partial tails. Later growth cannot
reveal discarded parent or overlay bytes. After SQL commit, an explicit truncate
checkpoint backfills the WAL, and the connection closes before the overlay is
frozen. Any execution, checkpoint, or close error prevents a frozen candidate.
Turso 0.7.2 can return success when a shutdown checkpoint encounters a non-Busy
storage error. The adapter keeps a sticky storage-failure flag and refuses to
freeze even if the engine swallows that error; a failure-injection regression
covers this path. Only touched pages and ranges affected by truncation/growth are compared with the
parent. There is no writable parent copy or whole-file change-discovery scan.

The frozen view materializes once into the existing validation file. Mutation
SQL and parameters are shared with the SQLite replay oracle through a private
writer interface. Bound integers, canonical values, ordered schema/ref operations,
and stable transaction results retain their existing validation.

Published Turso 0.7.2 needs its `uuid` feature even when SQL UUID functions are not
used: its incremental module references that optional dependency unconditionally.
The dependency therefore disables defaults and enables `fs` and `uuid`. No engine
fork or alternate candidate writer is used. Engine errors use `OTMP_TURSO_ERROR`;
preparation retains the existing domain error codes.

## Physical objects and readers

Packs use the specified 64-byte header and 64-byte sorted index entries. The
writer uses codec `none` and packs at most 1 MiB, with no padding requirement.
Readers also support independent `zstd` pages, including streaming frames with
unknown content size. One-shot decoding uses a destination bounded to the page
size and rejects larger output.
Object lengths/hashes, index agreement, exact raw length, and raw page hashes are
checked before applying pages.

Map nodes use text field names, native CBOR unsigned integers, raw 32-byte hashes,
and RFC 8949 core deterministic ordering of encoded keys. A root carries URI,
SHA-256, byte length, and height; leaves have height zero and internal `level`
is the height above leaves. The writer limits nodes to 128 entries and 1 MiB,
splitting at the midpoint. Updates copy affected paths, reuse unchanged nodes,
and prune mappings beyond EOF. Mappings that happen to match the base checkpoint
remain until a subsequent checkpoint replaces the map.

Current pins, historical pins, verification, and candidate reconstruction share
one generation materializer. It checks checkpoint identity independently, then
checks tree height, ordering and subtree bounds, explicit coverage beyond the
checkpoint, pack references, and the image-root hash. The final header page count
must agree with the exact materialized length. Historical selection and retained
verification cache immutable objects by URI and check each repeated reference's
declared length and hash. Verification totals include nodes and packs.

## Publication and checkpointing

Publication writes the semantic commit, required immutable image artifacts, and
generation before conditional HEAD replacement. The existing idempotency and
indeterminate-result reconciliation state machine remains in charge. A definite
conflict opens the winner and reexecutes semantic operations in a fresh Turso
candidate; losing pages never feed the retry.

Genesis publishes a complete checkpoint. A later transaction normally publishes
packs and copied map nodes over the previous base checkpoint. If the total bytes
of uniquely reachable override packs plus map nodes would equal or exceed the
candidate's complete image size, the same transaction publishes that validated
image as a new checkpoint with a null map. This adds no semantic version or root
revision beyond the transaction. There is no background checkpoint API or GC.

## Evidence and remaining costs

The deterministic large metadata fixture publishes a 5,279,744-byte image. Its
small update uploads 103,414 image-artifact bytes (98.0% fewer), and reuses one
unchanged map node. These totals exclude semantic commit/generation/HEAD bytes.
The independent candidate-copy probe uses a 2,367,488-byte parent: it preserves
the parent's allocation, compares four pages, and retains three changed pages.
Turso may allocate an extra page during this update.

Reproduce these measurements with:

```sh
cargo test -p otmp --lib small_candidate_borrows_parent -- --nocapture
cargo test -p otmp --test incremental small_transaction -- --nocapture
```

Those are copy-discovery and publication improvements. Pinning still reads and
materializes a complete logical image, and candidate validation still writes and
checks a complete file. Historical semantic replay also retains its full-image
cost. No remote VFS, incremental validation, provider latency, memory ceiling,
production throughput, or live S3/R2 result is claimed by these measurements.

`conformance/tables/incremental` retains versions 0–2 using deterministic pack/map
identities. `conformance/cow.py --check` independently reconstructs byte-identical
images from the full-image transaction fixtures; those fixtures and their SQLite
replay oracle remain intact. Crash checks reconstruct the published generation
before the independent Python SQLite checks. The complete command matrix is in
[QUALIFICATION.md](QUALIFICATION.md).
