# Transactions, history, and refs

This is the evolving pre-release v1 contract for the `0.0.2-alpha` local/full-image
runtime. Fixtures may be regenerated. There is no release compatibility promise,
migration path, feature-upgrade operation, or baseline release tag.

This contract is implemented in the following capability slices. For behavior
already demonstrated on this branch, see [qualification](QUALIFICATION.md).

## Independent coordinates

A transaction advances `table_version` exactly once, regardless of operation
count. A successful conditional `HEAD` replacement advances `root_revision`
exactly once. Only creating a snapshot advances the table-global
`last_sequence_number`. Property, ref, and schema transactions create no snapshot.
A physical generation may replace another at the same table version and must
preserve its commit reference and semantic-state hash. Equality between root
revision, table version, and snapshot sequence in a fixture is incidental.

A snapshot is immutable. Its parent follows its branch's ancestry; its sequence
comes from the global allocator. Branches are mutable names and may be null.
Tags are immutable names for real snapshots. Ref context is not snapshot identity.

## Runtime requests and durable values

`Table::transact(&TransactionRequest)` accepts an idempotency key, requirements,
ordered metadata operations, and object-valued commit metadata. Each operation
has a nonempty caller-supplied `operation_id`, unique within the request. One
request becomes one durable intent referring to every operation ID. Requirements
and operations in `SemanticCommit` remain canonical protocol JSON values, not
closed Rust enums. Runtime request enums are adapters into those values.

Requirements are evaluated against the pinned base, before any operation runs.
Operations execute in order in one private SQLite transaction. A failure discards
the private image. Immutable candidate artifacts may remain unreachable; `HEAD`
is the publication boundary. Readers and writers never merge SQLite pages or
rows from competing candidates.

Supported requirements and operation preconditions:

| Operation | Required preconditions | Result |
| --- | --- | --- |
| `set_properties` | Exactly one `property_is` for each touched key | Operation ID and sorted affected keys |
| `create_ref` | `ref_absent`; `snapshot_exists` for a non-null target | Operation ID, name, type, snapshot |
| `replace_ref` | `ref_exists` as branch, exact `ref_snapshot_is`, `snapshot_exists` for new target | Operation ID, name, branch type, snapshot |
| `drop_ref` | `ref_exists` with expected type and exact `ref_snapshot_is` | Operation ID and affected ref identity |
| `add_schema` | `current_schema_is(parent)`, `schema_id_absent`, `field_ids_absent(all new IDs)` | Operation ID and schema ID |
| `set_current_schema` | Exact base `current_schema_is` | Operation ID and schema ID |
| Ergonomic append | Current schema and default spec/order; existing branch and append-only ancestry on retry | Snapshot, sequence, version, commit, branch, files |

The default partition-spec and sort-order requirements are also accepted in
metadata transactions. Known core operations outside this matrix and required
extensions outside the supported feature set fail closed. `commit_snapshot` is
not exposed through `OperationRequest`. `upgrade_features` is unimplemented;
new tables advertise `otmp.refs.v1` from genesis.

Property updates and removals are atomic. Top-level JSON null is not a stored
property value in this runtime; nested null is valid. `property_is.value: null`
means absence, while missing `value` is malformed. Empty or undefined reserved
`otmp.*` keys, duplicate removals, update/removal overlaps, and touching the same
key in multiple operations are rejected. No implicit last-writer behavior exists.

At most one operation may mutate a given ref name. A tag cannot be replaced;
`main` cannot be dropped. Branch creation and replacement reconstruct membership
from snapshot ancestry and materialize live rows. Dropping a branch removes its
live rows. Tags have no materialized live rows.

## Idempotency and publication

The logical intent includes requirements, operations, operation IDs, and commit
metadata. Reusing a key with the same intent returns the original stable result;
using it for different intent fails. Durable metadata results contain table
version, commit ID, and operation results. They omit semantic-state hash to avoid
a commit-body hash cycle. First success and replay return the hash belonging to
the committed row, not the latest table's hash. No result contains checkpoint,
generation, root-revision, or storage fencing identity.

Append and metadata transactions share `TransactionRetryPolicy` and publication:

```text
pinned base -> requirements -> private relational transaction
  -> immutable semantic commit -> full checkpoint -> generation -> conditional HEAD
```

After a definite conflict the writer repins, checks idempotency, reevaluates all
requirements, and rebuilds from the winner. Unrelated property/ref changes can
rebase. A stale touched-property requirement is a definite semantic conflict.
Append rebases only across append descendants of its prepared branch tip with
unchanged schema/default requirements. Ref movement/drop, incompatible ancestry,
or current-schema changes conflict. Another branch's append may rebase.

Candidate physical IDs and timestamps are attempt-scoped. An indeterminate
attempt retains its identities during bounded reconciliation. Its idempotency
entry proves publication even if later commits advanced `HEAD`. An unchanged
anchor permits retry of the same candidate. Exhausted ambiguity is retryable
`PublicationIndeterminate`; bounded definite conflicts yield `RebaseExhausted`.

## Historical selection and snapshot resolution

Select metadata first with `Current` or `TableVersion(n)`, then resolve a ref,
snapshot ID, or sequence in that exact image. `PinnedTable` is current writable
state; `PinnedMetadata` exposes read-only coordinates and a separate current
`HeadAnchor`. No historical root revision or storage CAS token is exposed.
`status()` uses the validated cached main snapshot and does not hide SQL failures.

Historical selection reads `HEAD` once, then follows content-hashed
`physical_parent` references without listing. Every fetched generation, commit,
and image is validated. Repeated generation identities, increasing versions,
and same-version semantic divergence are corruption. The first encountered
requested version is the newest retained physical representation. A requested
version newer than the anchor is `MetadataVersionNotFound`. A well-formed chain
ending above the target is `HistoryNotRetained`. A missing explicitly referenced
parent is a storage/integrity failure. Transport errors remain storage errors.
The writer retains its physical ancestry; garbage collection is out of scope.
The broader protocol keeps `physical_parent` optional and informational.

A missing ref returns `RefNotFound`. A null branch resolves to no descriptor and
no files; a real empty snapshot has a descriptor and no files. Missing snapshot
IDs/sequences return `SnapshotNotFound`. Duplicate sequences or dangling refs
are corruption. Descriptor lookup does not enumerate files. Files come from
immutable ancestry; branch live rows are a validated projection.

## Verification

`verify()` selects `Current`; `verify_history()` selects `RetainedHistory`.
`verify_with_report` returns a completed report with the single current anchor
and counts of generations, commits, snapshots, objects, and bytes checked.
Failures return structured errors, never partial success reports.

Current verification checks metadata plus user bytes reachable from every branch
and tag tip. Retained verification additionally follows all retained generations,
validates their commits and logical relational transitions, and checks bytes
reachable only from historical snapshots. Reads are deduplicated by canonical
URI, not content hash. Verification never lists, repairs, follows a moving HEAD,
or checks unreachable orphan objects. Byte identity is not Parquet semantics.

## Additive optional schemas

One transaction may add one schema and set current schema at most once. A
selection may refer to an existing schema or one added earlier in operation
order. IDs are caller supplied. The declared parent must be the base current
schema, and all newly introduced field IDs must be globally unused.

Existing fields preserve identity, name, structure, sibling order, defaults,
and annotations. New fields are appended and optional, recursively at existing
struct positions. Identifier membership is unchanged. Rename, removal, reorder,
required-field addition, and type changes/promotions are rejected. Old snapshots
and files retain their schema IDs. Appends prepared under an old current schema
fail after concurrent schema changes rather than silently promoting.

## CLI

```sh
otmp transact TABLE --manifest REQUEST.json
otmp status TABLE [--table-version N]
otmp files TABLE [--table-version N] [--ref NAME | --snapshot-id ID | --sequence-number N]
otmp verify TABLE [--history]
```

No file selector means `--ref main`. Selectors are mutually exclusive during
argument parsing. Status always returns `anchor` and `selected` objects.
Success uses JSON stdout; errors use structured JSON stderr. Metadata manifests
use decimal strings for protocol ID counters and `ref` for ref names. A
`replace_ref` operation carries the new `snapshot_id`; its expected old value
belongs in the mandatory `ref_snapshot_is` requirement.

## Exclusions

No production readiness, compatibility migration, cloud qualification, GC,
listing-based reads, deletes/rewrites, partition/sort evolution, page maps,
remote SQLite VFS, catalog coordination, public multi-snapshot transactions,
Parquet validation, or feature upgrades are claimed. Provider evidence is a
separate AWS/R2 track; missing credentials are `not_run`, never qualification.
