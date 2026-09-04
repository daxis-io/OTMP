# Catalog integration guidance

Status: non-normative guidance for OTMP `0.0.2-alpha`.

This note describes how a catalog can coordinate OTMP tables without moving
catalog transaction semantics into the table protocol. OTMP defines no catalog
coordination object, core coordination field, or coordination feature flag.

## Authority boundary

```text
Catalog transaction
    = atomic catalog-visible resolution across participants

OTMP semantic commit
    = atomic durable mutation of one table root
```

A successful OTMP participant commit is durable table state, but it is not
proof that the enclosing catalog transaction committed. An external transaction
ID is correlation evidence. The catalog's own transaction record and atomic
catalog-root publication determine the final multi-table outcome.

Within one OTMP table:

- `table_version` identifies every committed semantic transaction.
- `snapshot_id` identifies a referenceable data state.
- A metadata-only commit can advance `table_version` without creating or moving
  to a new snapshot.
- A data-changing commit normally creates a snapshot and can update refs.
- `root_revision` identifies a physical `HEAD` replacement; it is not catalog
  transaction identity.

A physical metadata generation can be replaced at the same table version and
semantic state. Catalog snapshots therefore pin semantic state rather than a
particular checkpoint or compaction result.

## Catalog table coordinate

A catalog snapshot should resolve a table to at least:

```rust
pub struct CatalogTableCoordinate {
    pub table_id: TableId,
    pub table_root: TableRoot,
    pub table_version: u64,
    pub semantic_state_sha256: Sha256,
    pub snapshot_id: Option<SnapshotId>,
}
```

The selected snapshot is null for an empty table. For stronger audit
verification, the catalog may also retain the semantic commit object SHA-256.

Catalog snapshot identity should generally exclude:

- `root_revision`;
- generation ID;
- checkpoint URI or SHA-256;
- page-map root; and
- other physical image or cache-resolution details.

Those values can change without changing table semantics.

## Application-defined correlation metadata

A catalog may correlate an OTMP participant by placing stable values in the
semantic commit's existing top-level `metadata` object. The top-level key should
use a reverse-domain namespace controlled by the application:

```json
{
  "metadata": {
    "io.daxis.arco.catalog_coordination": {
      "catalog_id": "019a...",
      "catalog_transaction_id": "019b...",
      "participant_id": "orders",
      "target_catalog_snapshot_id": "019c..."
    }
  }
}
```

This metadata is optional, opaque to an ordinary OTMP reader, and preserved in
the semantic commit and relational commit history. OTMP correctness does not
depend on contacting the named catalog. The table commit states only that it
was produced as a participant in an external transaction; it does not state
that the external transaction committed.

Stable coordination identity is part of the caller's logical request and
should be included in intent identity. Suitable values include catalog ID,
catalog transaction ID, participant ID, and a preallocated target catalog
snapshot ID.

Attempt-local values do not belong in immutable semantic metadata, including:

- retry or attempt number;
- worker identity;
- trace or span ID;
- temporary coordinator address;
- current coordinator phase;
- authorization-decision details; and
- attempt timestamp.

Changing stable coordination metadata while reusing an idempotency key must
produce an idempotency conflict instead of returning a result associated with a
different external transaction.

## Commit and snapshot metadata

Commit metadata describes the transaction and its external context. Snapshot
metadata describes the immutable data state created by a `commit_snapshot`
operation. They have separate semantic owners:

```text
commit_metadata
    -> semantic_commit.metadata
    -> otmp_commits.metadata_json

snapshot_metadata
    -> commit_snapshot.snapshot.metadata
    -> otmp_snapshots.metadata_json
```

The values are not copied, merged, or inherited. Both participate independently
in logical intent identity and remain stable across retries and rebases.

## Catalog transaction flow

A catalog can implement atomic catalog visibility as follows:

1. Allocate the catalog transaction ID.
2. Persist intended participants and their expected base coordinates.
3. Publish each table-local OTMP participant commit.
4. Verify each resulting table ID, table version, semantic state hash, and
   selected snapshot ID.
5. Atomically publish one catalog transaction, root, or snapshot.
6. Resolve catalog-mediated reads through that new catalog snapshot.

The catalog-root publication—not the last participant `HEAD` replacement—is the
multi-table visibility boundary.

## Direct-reader visibility

If participant commits move public table refs, direct readers opening individual
table roots can observe partial catalog progress before the catalog snapshot is
published. That is compatible with OTMP's catalog-free read model.

A catalog requiring stronger isolation can prepare table snapshots under
catalog-owned private refs, then atomically publish a catalog snapshot that
selects those prepared states. Prepared-ref naming, lifecycle, authorization,
promotion, and cleanup remain catalog policy rather than OTMP core semantics.

## Recovery

The catalog transaction record should retain intended base coordinates and
observed result coordinates for every participant. Recovery can then:

- continue safely retryable participant commits;
- publish the catalog snapshot after verifying all exact results;
- recompute or fail after incompatible intervening table state;
- abandon privately prepared states; or
- issue later compensating semantic commits.

A successful OTMP `HEAD` replacement cannot be physically uncommitted. Recovery
is roll-forward, compensation, or abandonment—not destructive rollback of
immutable table history. An ambiguous catalog-root publication should be
reconciled against the catalog's durable root just as an OTMP writer reconciles
an ambiguous table `HEAD` publication.

## Future standardization threshold

An optional OTMP correlation profile should be considered only after at least
two independent catalogs demonstrate that:

1. table-local correlation data is needed for interoperability or recovery;
2. their identifiers and lifecycle substantially converge;
3. field meanings remain stable across retries, rebases, and recovery;
4. an OTMP tool benefits from understanding the fields without catalog access;
5. unknown implementations can safely ignore the profile; and
6. the profile does not make a catalog necessary to interpret table state.

Even then, such a profile should standardize correlation structure only, not
multi-table atomicity.
