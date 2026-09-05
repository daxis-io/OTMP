# Open Table Metadata Protocol (OTMP)

## Version 0.0.2-alpha

**Status:** Working Draft
**Date:** 2026-09-03
**Stability:** Experimental. No backward-compatibility guarantee is made before 0.1.0.
**Working name:** “OTMP” is provisional.
**Supersedes:** OTMP 0.0.1-alpha.

---

## 1. Abstract

The Open Table Metadata Protocol (OTMP) defines a language-agnostic, catalog-optional open table format whose metadata is represented as a self-contained relational database and persisted as immutable object-storage artifacts.

Each OTMP table owns its own:

- stable table identity;
- schema and field identities;
- partition and sort definitions;
- snapshots and references;
- data-file and delete-file metadata;
- statistics;
- semantic commit history;
- immediately queryable relational metadata state; and
- atomic current-version pointer.

A table can be read or written when only its storage location is known. A catalog MAY resolve names, enforce policy, vend credentials, cache state, or coordinate commits, but a catalog is not required for protocol correctness.

The durable table consists of:

1. **`HEAD`** — the only mutable protocol object; it identifies the current semantic table version and current physical metadata generation.
2. **Semantic commits** — immutable, typed descriptions of what changed and why.
3. **Metadata generations** — immutable, complete logical relational database snapshots.
4. **SQLite checkpoints** — portable, complete database images.
5. **Copy-on-write page maps and page packs** — an incremental physical representation of a complete SQLite-compatible metadata image.
6. **Optional scan projections** — immutable columnar metadata indexes optimized for distributed scan planning.
7. **Immutable data and delete files** — the table’s actual contents.

Normal readers do **not** replay semantic commits. They pin `HEAD`, open the referenced metadata generation as a ready-to-query relational database, resolve the required files, and read them directly.

Writers create immutable data files, apply a typed table transaction to a private relational database view, publish immutable semantic and physical artifacts, and atomically advance `HEAD` with compare-and-swap. Concurrent writers prepare independently; a losing writer reloads and semantically rebases rather than merging raw database pages.

When no reader, writer, coordinator, or maintenance process is active, an OTMP table requires no table-specific compute. Its durable state is only immutable storage objects plus `HEAD`.

---

## 2. Conformance language

The terms **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **NOT RECOMMENDED**, **MAY**, and **OPTIONAL** are normative.

A conforming implementation MUST implement one or more profiles defined in Section 32.

Non-normative rationale and examples are explicitly identified.

---

## 3. Design principles

OTMP is governed by the following principles.

### 3.1 The table is self-contained

The table location is sufficient to discover and interpret the table. The authoritative schema, snapshots, file inventory, statistics, and history MUST live under or be explicitly referenced by the table root.

### 3.2 A catalog is optional

A catalog MAY map a name such as `production.analytics.sales` to a table URI, but the catalog MUST NOT be required to understand the table once the URI is known.

### 3.3 Relational semantics are the logical model

Table metadata is modeled as entities, relationships, constraints, and transactions rather than as an application-specific tree of metadata documents.

### 3.4 Immutable files are the physical model

The relational state is persisted through immutable checkpoints, page-map nodes, page packs, semantic commits, and scan projections suitable for object storage.

### 3.5 The read state is already materialized

The current metadata generation MUST represent a complete queryable relational state. Ordinary readers MUST NOT be required to replay semantic history before executing metadata queries.

### 3.6 Semantic and physical histories are separate

Semantic commits explain table operations. Page packs and page maps materialize database state efficiently. Neither raw page changes nor arbitrary SQL statements define table meaning.

### 3.7 Only `HEAD` is mutable

All other protocol objects are immutable after publication. A successful conditional replacement of `HEAD` is the sole atomic visibility boundary.

### 3.8 Readers never lock writers

Readers pin one immutable generation. Writers create new generations. Existing generations are never modified in place.

### 3.9 Many writers are optimistic and distributed

Multiple writers MAY prepare candidates concurrently. Publication is ordered at the table `HEAD`. Failed publishers re-evaluate typed requirements and operations against the new current state.

### 3.10 Zero resident compute is possible

No protocol server, database process, catalog process, actor, lease holder, or coordinator is required to remain running while the table is idle.

### 3.11 Open artifacts outrank implementations

SQLite, Turso, a custom Rust engine, a catalog service, or a commit broker MAY implement OTMP. No implementation is the protocol authority.

---

## 4. Goals

OTMP 0.0.2-alpha is designed to provide:

- A self-describing open table format.
- Catalog-free reads and writes.
- A normalized, versioned relational metadata model at the individual-table boundary.
- Standard SQL access to table metadata.
- Stable IDs independent of names.
- Immutable snapshots and time travel.
- Schema, partition, and sort-order evolution.
- Hidden partitioning.
- Unlimited concurrent readers without metadata locks.
- Distributed optimistic writers without an always-running database.
- Idempotent retries.
- Atomic metadata publication through compare-and-swap.
- A complete ready-to-query metadata generation at every committed version.
- A portable standard SQLite checkpoint.
- Incremental copy-on-write physical updates without uploading the whole database.
- Optional Iceberg-like columnar scan planning without changing the relational authority.
- Customer-owned metadata and data in object storage.
- Pluggable catalogs and commit coordinators.
- Deterministic validation and recovery.
- A path to browser, edge, embedded, and serverless implementations.

---

## 5. Non-goals

OTMP 0.0.2-alpha does not define:

- Namespace or table-name discovery.
- A warehouse-wide metastore.
- A Unity Catalog, REST catalog, Hive Metastore, Nessie, or authorization API.
- Cross-table atomic transactions.
- Query execution over user rows.
- An always-running database service.
- Arbitrary concurrent SQL mutation of one shared SQLite file.
- A distributed lock service.
- A mandatory actor runtime or commit broker.
- A mandatory cloud provider.
- A mandatory query engine.
- A mandatory implementation language.
- A mandatory embedded SQL engine.
- A required row-level delete encoding in the core profile.
- A required data-file format beyond feature negotiation.
- Destructive garbage collection without an explicit retention policy.
- Generic CRDT conflict resolution.
- Git-style catalog branches or merge commits.

Extensions MAY define additional capabilities.

---

## 6. Table package model

### 6.1 Table root

An OTMP table is rooted at a URI called the **table root**.

Example:

```text
s3://example-warehouse/tables/0198f8ab-0c3e-7f2a-95f4-907ec2589631/
```

All relative protocol URIs are resolved against this root.

### 6.2 Recommended layout

```text
<table-root>/
├── _otmp/
│   ├── HEAD
│   ├── commits/
│   │   └── <table-version>/<commit-id>.json
│   ├── generations/
│   │   └── <table-version>/<generation-id>.json
│   ├── checkpoints/
│   │   └── <table-version>/<checkpoint-id>.sqlite3
│   ├── page-maps/
│   │   └── <hash-prefix>/<node-hash>.cbor
│   ├── page-packs/
│   │   └── <table-version>/<pack-id>.otmppg
│   ├── scan/
│   │   └── <snapshot-id>/
│   │       ├── root.json
│   │       └── manifests/*.parquet
│   └── artifacts/
├── data/
├── deletes/
└── auxiliary/
```

The paths are recommendations. Correctness depends on explicit references and hashes, not directory listing or exact naming.

### 6.3 Table identity

Every table has one immutable 128-bit `table_id`.

The table ID:

- MUST be globally unique with overwhelming probability;
- MUST NOT change when the table is renamed, moved, copied, or registered in a different catalog;
- MUST be stored in `HEAD`, every semantic commit, every metadata generation, and the relational checkpoint;
- SHOULD be encoded as a UUIDv7 value; and
- MUST be treated as opaque by protocol implementations.

A physical clone MAY retain the table ID only when it is intended to be another physical copy of the same logical table. A fork intended to evolve independently SHOULD receive a new table ID and record its origin in optional metadata.

### 6.4 Atomicity domain

One table root is the core protocol’s:

- semantic version domain;
- commit-order domain;
- compare-and-swap contention domain;
- metadata checkpoint domain; and
- failure-isolation domain.

Transactions across separate table roots require a catalog or higher-level transaction protocol and are outside the core specification.

### 6.5 Table version

`table_version` is the semantic commit number.

It:

- begins at `0` for the genesis commit;
- increases by exactly one for each committed semantic transaction;
- is globally ordered within one table;
- is unchanged by physical checkpoint compaction; and
- MUST fit within the signed 64-bit range `0..2^63-1`.

### 6.6 Root revision

`root_revision` orders every successful `HEAD` replacement.

The genesis `HEAD` begins with `root_revision = 0`.

It increases by exactly one for:

- a semantic commit; or
- a physical replacement of the metadata generation at the same semantic table version.

Therefore:

```text
table_version changes  => logical table state changed
root_revision changes  => HEAD changed
```

### 6.7 Immutable objects

Every object other than `_otmp/HEAD` MUST be immutable after publication.

An implementation MUST NOT overwrite a referenced:

- semantic commit;
- generation descriptor;
- SQLite checkpoint;
- page-map node;
- page pack;
- scan root;
- scan manifest;
- data file;
- delete file; or
- auxiliary artifact.

Create-only writes are RECOMMENDED.

### 6.8 Visibility

An object existing in storage is not necessarily committed.

An artifact becomes part of the table only when it is reachable from a successfully published `HEAD` or from another retained immutable object reachable from that `HEAD`.

### 6.9 No listing on the normal path

A normal reader or writer MUST be able to operate by following explicit references.

Object listing MAY be used for:

- orphan discovery;
- maintenance;
- diagnostics;
- administrative recovery; and
- garbage collection.

---

## 7. Common encodings and types

### 7.1 Canonical JSON

`HEAD`, generation descriptors, semantic commits, and scan roots use canonical UTF-8 JSON.

Canonical JSON MUST follow these rules:

- object keys are sorted lexicographically by Unicode code point;
- strings use UTF-8;
- duplicate keys are forbidden;
- insignificant whitespace is omitted for hashing;
- NaN and infinity are forbidden;
- floating-point numbers are forbidden in core protocol fields;
- signed and unsigned 64-bit integers are encoded as decimal strings;
- arrays with semantic order preserve that order;
- set-like feature arrays are sorted and contain no duplicates.

An implementation MAY use an RFC 8785 implementation when it produces the same bytes required above.

### 7.2 Deterministic CBOR

Compact typed values and page-map nodes use deterministic CBOR.

Encoders MUST use deterministic ordering, shortest integer forms, definite-length items, and no duplicate map keys.

### 7.3 IDs

The following are 16-byte opaque identifiers and are normally displayed as canonical UUID strings:

- `table_id`;
- `commit_id`;
- `generation_id`;
- `snapshot_id`;
- `file_id`;
- `artifact_id`.

Field, schema, partition-spec, partition-field, and sort-order IDs are non-negative or positive integers as defined by their sections.

### 7.4 Hashes

Core hashes use SHA-256.

JSON form:

```text
sha256:<64 lowercase hexadecimal digits>
```

SQLite form:

```text
32 raw bytes
```

A reader MUST verify the hash of every fetched immutable protocol object before trusting its contents.

### 7.5 URIs

Protocol object URIs MAY be absolute or table-root-relative.

Writers SHOULD use table-root-relative URIs for table-owned artifacts so a table directory can be copied or relocated.

A relative URI:

- MUST use `/` separators;
- MUST NOT begin with `/`;
- MUST NOT contain `..` path traversal;
- MUST NOT resolve outside the table root.

Data files MAY be outside the table root when explicitly referenced and permitted by deployment policy.

### 7.6 Timestamps

Timestamps are signed 64-bit Unix milliseconds encoded as decimal strings in JSON and SQLite `INTEGER` values in checkpoints.

Timestamps are descriptive. Ordering authority comes from `table_version`, snapshot sequence numbers, and immutable parent links.

### 7.7 Typed scalar values

Typed scalar values in semantic JSON use:

```json
{
  "type": "int64",
  "value": "42"
}
```

Null is represented as:

```json
{
  "type": "null"
}
```

The deterministic CBOR scalar profile is defined in Appendix B.

---

## 8. Required durable objects

Every non-empty OTMP table has:

- exactly one current `HEAD`;
- exactly one semantic commit for each committed `table_version` retained by policy;
- one current metadata generation;
- one or more immutable physical metadata artifacts reachable from that generation; and
- zero or more snapshots, data files, delete files, and projections.

### 8.1 Object reference

A core object reference has this shape:

```json
{
  "uri": "_otmp/generations/42/018f....json",
  "sha256": "sha256:...",
  "length": "1842",
  "media_type": "application/vnd.otmp.generation+json"
}
```

`length` is OPTIONAL but RECOMMENDED.

### 8.2 `HEAD`

`HEAD` is the only mutable protocol object.

Required media type:

```text
application/vnd.otmp.head+json
```

Required shape:

```json
{
  "protocol": "otmp",
  "protocol_version": "0.0.2-alpha",
  "table_id": "018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0a",
  "table_version": "42",
  "root_revision": "57",
  "semantic_state_sha256": "sha256:...",
  "semantic_commit": {
    "uri": "_otmp/commits/42/018f43a0-bf2a-7bd4-8b32-5bb2d2ac9321.json",
    "sha256": "sha256:...",
    "media_type": "application/vnd.otmp.commit+json"
  },
  "metadata_generation": {
    "uri": "_otmp/generations/42/018f43a0-ca2a-7fd4-9a11-0a8c385f41f3.json",
    "sha256": "sha256:...",
    "media_type": "application/vnd.otmp.generation+json"
  },
  "required_reader_features": [
    "otmp.core.v2",
    "otmp.metadata.sqlite3-cow.v1"
  ],
  "required_writer_features": [
    "otmp.core.v2",
    "otmp.metadata.sqlite3-cow.v1"
  ]
}
```

`HEAD` invariants:

- `protocol` MUST equal `otmp`.
- `table_id` MUST match every reachable OTMP object.
- `semantic_commit` MUST describe `table_version`.
- `metadata_generation` MUST materialize the same `table_version` and `semantic_state_sha256`.
- `semantic_state_sha256` MUST match the semantic commit and relational metadata image.
- `root_revision` MUST increase by exactly one from the previous committed `HEAD`.
- A semantic commit MUST increase `table_version` by exactly one.
- A physical-only checkpoint publication MUST retain `table_version`, semantic commit, and semantic state hash.

### 8.3 Storage version token

The storage system’s ETag, generation number, version identifier, or equivalent conditional-write token is not serialized inside `HEAD`.

A direct writer MUST obtain an opaque storage version token while reading `HEAD` and MUST use it for the final compare-and-swap.

### 8.4 Compare-and-swap requirement

A storage backend supports catalog-free direct writes only when it provides a linearizable operation equivalent to:

```text
replace HEAD with new bytes if current version token equals expected token
```

Table creation requires an operation equivalent to:

```text
create HEAD only if HEAD does not already exist
```

A backend lacking these operations MAY support read-only access or MAY require a catalog/coordinator that provides equivalent fencing.

---

## 9. Semantic commit object

### 9.1 Purpose

A semantic commit defines the language-independent meaning of one table transaction.

It is used for:

- validation;
- conflict detection;
- idempotency;
- audit;
- replication;
- change feeds;
- recovery;
- authorization; and
- rebuilding the relational metadata state.

Normal readers do not apply semantic commits before querying the current metadata generation.

### 9.2 Required shape

```json
{
  "kind": "otmp.semantic-commit",
  "format_version": 1,
  "table_id": "018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0a",
  "table_version": "42",
  "parent_table_version": "41",
  "commit_id": "018f43a0-bf2a-7bd4-8b32-5bb2d2ac9321",
  "parent_commit": {
    "uri": "_otmp/commits/41/....json",
    "sha256": "sha256:..."
  },
  "created_at_ms": "1786548000000",
  "intents": [
    {
      "key": "ingest-job-8732-batch-44",
      "intent_sha256": "sha256:...",
      "operation_ids": ["op-1"],
      "result": {}
    }
  ],
  "requirements": [],
  "operations": [
    {
      "operation_id":"op-1",
      "type":"commit_snapshot",
      "target_ref":"main",
      "snapshot":{
        "snapshot_id":"018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0c",
        "parent_snapshot_id":null,
        "sequence_number":"1",
        "schema_id":"1",
        "partition_spec_id":"0",
        "sort_order_id":"0",
        "operation":"append",
        "summary":{},
        "metadata":{}
      },
      "added_files":[],
      "removed_file_ids":[],
      "scan_projection":null,
      "rebase_mode":"append-safe"
    }
  ],
  "required_reader_features_after_commit": ["otmp.core.v2"],
  "required_writer_features_after_commit": ["otmp.core.v2"],
  "previous_semantic_state_sha256": "sha256:...",
  "semantic_state_sha256": "sha256:...",
  "metadata": {}
}
```

### 9.3 Genesis commit

Version `0` is a genesis semantic commit.

It:

- has `parent_table_version: null`;
- has `parent_commit: null`;
- contains exactly one `initialize_table` operation;
- creates schema ID `1` or another positive initial schema ID;
- creates unpartitioned spec ID `0`;
- creates unsorted order ID `0`;
- creates branch `main` with a null snapshot; and
- establishes the first semantic state hash.

### 9.4 Commit invariants

For every non-genesis commit:

```text
table_version = parent_table_version + 1
```

The commit MUST:

- refer to exactly one parent semantic commit;
- contain at least one operation;
- contain one or more intent records whose idempotency keys are unique among committed intents for the table;
- encode all requirements and operations canonically;
- declare the post-commit required feature sets;
- produce one complete relational state; and
- be immutable after upload.

### 9.5 Intents and group commit

Most direct commits contain one intent. A coordinator MAY combine multiple compatible caller intents into one atomic semantic commit. Each intent has its own idempotency key, intent hash, referenced operation IDs, and stable result.

`intent_sha256` identifies one caller’s logical request independently of retries, rebases, and grouping.

It SHOULD be computed over a canonical intent document that excludes:

- parent version;
- candidate table version;
- publication URI;
- creation timestamp;
- physical metadata artifacts; and
- retry-specific metadata.

Reusing an idempotency key with a different intent hash is an error.

Every operation MUST have a commit-unique `operation_id`. Every operation MUST be referenced by at least one intent. Multiple intents MAY reference one combined operation, such as a grouped append snapshot.

A commit containing multiple intents MUST apply all included operations atomically. If publication fails, none of the intents is committed.

### 9.6 Semantic state hash

The semantic state hash is a chain hash:

```text
state_hash(0) = SHA256("OTMP-GENESIS\0" || canonical_genesis_body)

state_hash(n) = SHA256(
    "OTMP-STATE\0" ||
    state_hash(n-1) ||
    canonical_commit_body_without_semantic_state_sha256
)
```

A physical checkpoint change MUST NOT change the semantic state hash.

### 9.7 Intent result

Each intent’s `result` object records stable outcomes useful for idempotent retry, such as:

```json
{
  "snapshot_id": "018f...",
  "ref": "main",
  "sequence_number": "108"
}
```

A retry using the same idempotency key and intent hash MUST return the result stored for that intent, even when the intent was group-committed with other requests.

### 9.8 Commit metadata and snapshot metadata

Commit metadata is opaque caller-controlled metadata that describes the semantic
transaction and its external execution or coordination context. It is stored in
the semantic commit's top-level `metadata` object and in the corresponding
`otmp_commits.metadata_json` row.

Snapshot metadata is opaque caller-controlled metadata that describes one
immutable table snapshot created by a `commit_snapshot` operation. It is stored
in that operation's `snapshot.metadata` object and in the corresponding
`otmp_snapshots.metadata_json` row.

The top-level commit `metadata` value and every `snapshot.metadata` value MUST
be JSON objects. A committed `commit_snapshot` operation MUST use the complete
nested `snapshot` shape defined in Section 25.13. A reader MUST reject a flat
snapshot encoding or an operation with missing or unknown core fields.

An implementation MUST NOT implicitly copy, merge, or inherit commit metadata
into snapshot metadata, or snapshot metadata into commit metadata. A caller MAY
explicitly place related values in both when those values genuinely describe
both semantic objects.

Both metadata objects are stable semantic inputs and MUST participate
independently in the caller's logical intent identity. An omitted metadata
object and an empty metadata object MUST have the same logical intent identity.
The two objects MUST remain unchanged across publication retries and semantic
rebases.

Attempt-local telemetry and runtime-assigned values, including retry counters,
worker identities, trace IDs, candidate IDs, candidate timestamps, and current
execution phase, MUST NOT be inserted into either object by the committer.

---

## 10. Metadata generation descriptor

### 10.1 Purpose

A metadata generation describes one complete, immutable, ready-to-query relational metadata database for one semantic table version.

It is a physical read model, not the semantic history.

Required media type:

```text
application/vnd.otmp.generation+json
```

### 10.2 Required shape

```json
{
  "kind": "otmp.metadata-generation",
  "format_version": 1,
  "table_id": "018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0a",
  "table_version": "42",
  "generation_id": "018f43a0-ca2a-7fd4-9a11-0a8c385f41f3",
  "created_at_ms": "1786548000100",
  "semantic_state_sha256": "sha256:...",
  "semantic_commit": {
    "uri": "_otmp/commits/42/....json",
    "sha256": "sha256:..."
  },
  "physical_parent": {
    "uri": "_otmp/generations/41/....json",
    "sha256": "sha256:..."
  },
  "metadata_image": {
    "codec": "otmp.metadata.sqlite3-cow.v1",
    "page_size": 4096,
    "page_count": "8124",
    "checkpoint": {
      "table_version": "40",
      "uri": "_otmp/checkpoints/40/....sqlite3",
      "sha256": "sha256:...",
      "length": "33275904"
    },
    "page_map": {
      "uri": "_otmp/page-maps/ab/....cbor",
      "sha256": "sha256:...",
      "height": 2
    },
    "image_root_sha256": "sha256:..."
  },
  "scan_projection": null,
  "metadata": {}
}
```

### 10.3 Completeness

The generation MUST resolve every logical SQLite page from `1` through `page_count` exactly once:

- an override in the page map supplies the page; otherwise
- the page is read from the base checkpoint.

A generation MUST NOT require semantic commit replay.

### 10.4 Physical parent

`physical_parent` is OPTIONAL and informational. Correct reading MUST depend only on the generation’s explicit checkpoint, page map, page count, and object references.

### 10.5 Same-version physical replacement

A checkpointer MAY publish another generation for the same `table_version` and semantic state hash.

The replacement generation MAY use a newer complete checkpoint and a smaller or empty page map.

---

## 11. Metadata image profile: `otmp.metadata.sqlite3-cow.v1`

### 11.1 Logical file

The metadata image is logically one complete SQLite Format 3 database file.

The file is physically represented as:

```text
base SQLite checkpoint
        +
current copy-on-write page map
        =
complete logical SQLite image
```

### 11.2 Required properties

The logical image MUST:

- begin with the standard `SQLite format 3\0` header;
- use the normative relational schema in Appendix A;
- have `PRAGMA application_id = 0x4F544D50`;
- have `PRAGMA user_version = 2`;
- contain no committed state that exists only in a WAL, rollback journal, shared-memory file, Turso MVCC log, or other sidecar;
- be readable by a conforming SQLite 3 reader;
- pass `PRAGMA integrity_check`;
- pass `PRAGMA foreign_key_check` when validated by an engine supporting it;
- encode the same `table_id`, `table_version`, commit hash, and semantic state hash as the generation and `HEAD`; and
- be immutable for the lifetime of the generation.

### 11.3 Engine independence

A writer MAY use:

- upstream SQLite;
- Turso;
- libSQL;
- a custom SQLite VFS;
- a purpose-built Rust engine;
- another runtime that can export the normative SQLite image.

Private WAL, MVCC, or local temporary state MAY be used while building a candidate generation. None may be required to read the published generation.

### 11.4 Page size

A writer MAY choose any SQLite-supported page size from 512 through 65,536 bytes.

The reference writer uses 4,096 bytes.

A reader MUST honor the page size in the database header and generation descriptor. The two MUST match.

### 11.5 Page count

`page_count` is the exact number of logical pages in the file.

The logical file length is:

```text
page_size * page_count
```

Pages beyond `page_count` are not part of the generation.

### 11.6 Simple materialization

A simple reader MAY materialize the generation by:

1. copying or downloading the checkpoint;
2. resolving every page-map override;
3. overwriting the corresponding page offsets;
4. extending or truncating the file to `page_count * page_size`;
5. verifying the resulting SQLite image; and
6. opening it read-only.

This process applies physical pages, not semantic operations.

### 11.7 Remote VFS

An advanced reader MAY expose the generation through a read-only SQLite VFS.

The VFS SHOULD:

- cache immutable objects by content hash;
- coalesce adjacent range requests;
- prefetch the SQLite header and schema pages;
- prefetch B-tree siblings when beneficial;
- share checkpoint and page-pack caches across readers;
- verify every page hash before return; and
- pin one generation for the lifetime of a database connection.


---

## 12. SQLite checkpoint object

### 12.1 Purpose

A checkpoint is a complete standalone SQLite database image for one semantic table version.

It provides:

- a portable recovery artifact;
- interoperability with ordinary SQLite tooling;
- a bounded base for page-map lookup;
- fast local opening after download; and
- an implementation-independent export from private runtimes such as Turso.

### 12.2 Checkpoint invariants

A checkpoint MUST:

- be immutable;
- represent exactly one `table_id` and `table_version`;
- use the normative schema;
- require no sidecar files;
- be uncompressed at the protocol byte level so page offsets remain range-addressable;
- have a verified SHA-256 digest;
- have a file length that is a multiple of its SQLite page size; and
- be closed with no active transaction before upload.

Transparent storage-layer compression is allowed only when range reads preserve the original byte offsets and returned bytes.

### 12.3 Checkpoint creation

A conforming implementation MAY create a checkpoint using:

- `VACUUM INTO`;
- the SQLite Backup API;
- a Turso export;
- direct materialization of a generation; or
- another process that emits the exact normative SQLite image.

Before publication, a checkpoint writer MUST validate:

```sql
PRAGMA integrity_check;
PRAGMA foreign_key_check;
```

The expected `integrity_check` result is exactly `ok`.

### 12.4 Checkpoint identity

A checkpoint reference includes:

- the semantic table version represented;
- URI;
- SHA-256;
- byte length; and
- media type `application/vnd.sqlite3`.

A checkpoint MAY replace an older physical base without changing semantic state.

---

## 13. Page-pack format

### 13.1 Purpose

A page pack stores the final committed bytes of one or more changed SQLite pages.

A writer SHOULD group pages into range-readable packs rather than create one object per page.

Required media type:

```text
application/vnd.otmp.page-pack
```

File suffix recommendation:

```text
.otmppg
```

### 13.2 Byte order

All integer fields in the binary page-pack format are unsigned big-endian integers.

### 13.3 Header

The fixed header is 64 bytes:

| Offset | Length | Field |
|---:|---:|---|
| 0 | 8 | ASCII magic `OTMPPGPK` |
| 8 | 2 | major version, currently `1` |
| 10 | 2 | minor version, currently `0` |
| 12 | 4 | flags |
| 16 | 4 | SQLite page size |
| 20 | 4 | entry count |
| 24 | 8 | byte offset of index |
| 32 | 8 | byte offset of payload region |
| 40 | 24 | reserved, all zero |

Unknown nonzero reserved bytes MUST cause rejection in version 1.

### 13.4 Index entry

Each index entry is 64 bytes:

| Offset | Length | Field |
|---:|---:|---|
| 0 | 8 | logical SQLite page number, beginning at 1 |
| 8 | 8 | absolute payload byte offset within the pack |
| 16 | 4 | stored length |
| 20 | 4 | raw length; MUST equal the generation page size |
| 24 | 1 | codec: `0 = none`, `1 = zstd` |
| 25 | 7 | reserved, all zero |
| 32 | 32 | SHA-256 of the uncompressed page bytes |

Entries MUST be sorted by logical page number and MUST NOT contain duplicates.

### 13.5 Payload

Payload bytes MAY be compressed independently per page.

A reader MUST:

1. verify the whole-object hash from the object reference;
2. parse the header and index;
3. fetch or decode the selected payload;
4. verify the page hash; and
5. return exactly `page_size` uncompressed bytes.

### 13.6 Pack constraints

A pack:

- MUST contain pages from exactly one candidate generation;
- MUST use one page size;
- MUST NOT contain page number 0;
- SHOULD be between 256 KiB and 16 MiB unless workload measurements justify otherwise; and
- SHOULD place pages likely to be fetched together near each other.

A semantic commit MAY produce multiple page packs.

---

## 14. Persistent page map

### 14.1 Purpose

The page map resolves a logical SQLite page number directly to its newest immutable page-pack location.

A reader MUST NOT need to scan every earlier delta or page pack.

If a logical page has no override in the map, it is read from the base checkpoint at:

```text
offset = (page_number - 1) * page_size
```

### 14.2 Data structure

The core page-map profile is an immutable persistent B-tree encoded as deterministic CBOR.

Required media type:

```text
application/vnd.otmp.page-map+cbor
```

Every node is independently immutable and content-hashed.

### 14.3 Internal node

An internal node has this logical shape:

```text
{
  version: 1,
  node_type: "internal",
  level: N,
  entries: [
    {
      max_page: P,
      child: { uri, sha256, length }
    },
    ...
  ]
}
```

Rules:

- entries are sorted by `max_page`;
- ranges MUST NOT overlap;
- the final `max_page` covers the maximum page represented by that subtree;
- a lookup chooses the first child whose `max_page >= requested_page`;
- non-root internal nodes SHOULD contain between 64 and 1,024 entries; and
- node serialized size MUST NOT exceed 1 MiB.

### 14.4 Leaf node

A leaf node has this logical shape:

```text
{
  version: 1,
  node_type: "leaf",
  entries: [
    {
      page_number: P,
      pack: { uri, sha256, length },
      offset: O,
      stored_length: L,
      raw_length: R,
      codec: "none" | "zstd",
      page_sha256: "sha256:..."
    },
    ...
  ]
}
```

Rules:

- entries are sorted by `page_number`;
- page numbers are unique;
- each entry MUST agree with the page-pack index;
- a page absent from all leaves falls through to the checkpoint; and
- a leaf MUST NOT reference a page greater than generation `page_count`.

### 14.5 Copy-on-write update

A writer creating a generation:

1. begins with the parent page-map root;
2. replaces mappings for changed pages;
3. removes mappings for pages that now exactly match the base checkpoint only when safe and useful;
4. writes new leaf nodes for changed key ranges;
5. writes new internal nodes on paths to the root; and
6. reuses every unaffected node.

Raw page-map nodes from two conflicting candidate generations MUST NOT be merged. A writer that loses publication reruns the semantic transaction against the new current generation.

### 14.6 Empty map

A generation whose checkpoint already contains the complete image MAY set `page_map` to null.

### 14.7 Image root hash

`image_root_sha256` is computed as:

```text
SHA256(
    ASCII("OTMP-SQLITE-IMAGE\0") ||
    table_id_raw_16_bytes ||
    table_version_u64_big_endian ||
    page_size_u32_big_endian ||
    page_count_u64_big_endian ||
    checkpoint_sha256_raw_32_bytes ||
    page_map_root_sha256_raw_32_bytes_or_all_zero
)
```

It identifies the exact physical database view, not merely the semantic state.

---

## 15. Normative relational metadata model

### 15.1 Scope

The relational image describes exactly one table.

It is not a namespace catalog or warehouse metastore.

The normative SQL schema is provided as the companion artifact:

```text
OTMP-0.0.2-alpha-table-schema.sql
```

Appendix A summarizes every table.

### 15.2 Required entities

The metadata database contains:

- `otmp_meta` — table identity and current defaults;
- `otmp_commits` — committed semantic transaction records;
- `otmp_idempotency` — stable retry results;
- `otmp_properties` — table properties;
- `otmp_features` — enabled protocol features;
- `otmp_schemas` and `otmp_fields` — immutable schema definitions;
- `otmp_field_ids` — table-global field-ID registry;
- `otmp_partition_specs` and `otmp_partition_fields`;
- `otmp_sort_orders` and `otmp_sort_fields`;
- `otmp_snapshots` and `otmp_snapshot_summary`;
- `otmp_refs` — branches and tags;
- `otmp_files` — immutable data/delete-file descriptors;
- `otmp_file_metrics` — per-column metrics;
- `otmp_delete_file_details` — optional delete semantics;
- `otmp_snapshot_file_changes` — snapshot additions and removals;
- `otmp_ref_live_files` — complete current file membership for mutable branches;
- `otmp_artifacts` — scan indexes and auxiliary immutable objects; and
- `otmp_live_files` — a convenience view.

### 15.3 Current state versus history

The relational image intentionally stores both:

- immutable historical entities such as schemas, snapshots, files, and commits; and
- materialized current state such as branch heads and live branch file membership.

This ensures ordinary current-state reads do not reconstruct state from semantic history.

### 15.4 Database invariants

At publication:

- `otmp_meta.table_version` MUST equal `HEAD.table_version`;
- `otmp_meta.table_id` MUST equal `HEAD.table_id`;
- the last commit row MUST identify the semantic commit referenced by `HEAD`;
- `otmp_meta.semantic_state_sha256` MUST equal `HEAD.semantic_state_sha256`;
- every foreign key MUST resolve;
- every branch live-file row MUST refer to a file in `otmp_files`;
- every active ref snapshot MUST exist;
- each `otmp_commits.intent_count` MUST equal the number of `otmp_idempotency` rows for that commit;
- schema, partition, and sort defaults MUST exist;
- field IDs and partition-field IDs MUST never be reused; and
- current mutable-branch membership MUST equal the result of applying the branch’s snapshot changes.

### 15.5 Arbitrary metadata reads

Readers MAY execute arbitrary read-only SQL against the relational image.

Writers MUST NOT define table semantics by issuing arbitrary updates to the normative tables. Writes MUST be derived from semantic operations defined in Section 24 or enabled extensions.

### 15.6 Non-normative indexes

An implementation MAY add additional SQLite indexes, views, generated columns, or statistics when:

- the normative rows and constraints remain unchanged;
- an ordinary conforming reader can ignore the additions;
- no table meaning depends on an implementation-only SQL function or collation; and
- checkpoint validation still succeeds.

Such additions SHOULD use a vendor-prefixed name outside the `otmp_` namespace.

---

## 16. Logical type system

### 16.1 Primitive types

Core OTMP types are:

```text
boolean
int32
int64
float32
float64
decimal(precision, scale)
date
time_micros
timestamp_micros
timestamptz_micros
string
binary
fixed(length)
uuid
```

### 16.2 Nested types

Nested types are:

```text
struct
list
map
```

Every nested field has a stable field ID.

List element and map key/value fields are modeled as fields with their own IDs, names, requiredness, and types.

### 16.3 Type JSON

Primitive example:

```json
{"type":"decimal","precision":18,"scale":2}
```

Struct example:

```json
{
  "type": "struct",
  "fields": [
    {"field_id": 10, "name": "city", "required": false, "type": {"type":"string"}},
    {"field_id": 11, "name": "zip", "required": false, "type": {"type":"string"}}
  ]
}
```

The normalized `otmp_fields` rows are authoritative in the SQLite image. Type JSON MUST agree with the row graph.

### 16.4 Scalar canonicalization

Typed scalar CBOR values are used for:

- partition values;
- metric bounds;
- defaults; and
- operation requirements.

Scalar equality is type-aware. For example, `int32(1)` and `int64(1)` are not byte-identical but MAY compare equal after schema-defined promotion.

NaN has no pruning order. Implementations MUST use `nan_count` and MUST NOT infer ordering from a NaN bound.

---

## 17. Schema semantics

### 17.1 Stable field identity

Field names are not identity.

A field ID:

- is positive;
- is unique for the lifetime of the table;
- remains unchanged across renames and reorders;
- MUST NOT be assigned to an unrelated field after drop; and
- is registered in `otmp_field_ids`.

### 17.2 Immutable schemas

Each schema is immutable and identified by `schema_id`.

Schema evolution creates a new schema whose `parent_schema_id` identifies the previous definition when applicable.

`otmp_meta.current_schema_id` selects the default schema for new writes.

Each snapshot records the schema used to interpret its files.

### 17.3 Core evolution rules

Core-compatible changes include:

- add optional field;
- add required field with a valid initial and write default;
- rename while preserving field ID;
- reorder fields;
- required to optional;
- drop while reserving the ID;
- `int32` to `int64`;
- `float32` to `float64`;
- decimal precision increase with unchanged scale;
- fixed binary to variable binary; and
- compatible nested evolution preserving nested field IDs.

Core-incompatible changes include:

- field-ID replacement;
- ID reuse;
- numeric narrowing;
- decimal scale change;
- optional to required without proof or rewrite;
- incompatible primitive conversion; and
- changing a map key type or identity.

An extension MAY define additional promotions.

### 17.4 Defaults

`initial_default` describes the value observed for historical rows lacking a newly added field.

`write_default` describes the value writers use when a new row omits the field.

A required field added after data exists MUST define an initial default and a write default unless every historical file is rewritten in the same commit.

### 17.5 Identifier fields

A schema MAY identify fields that form a logical row identity.

Identifier fields:

- MUST be required;
- MUST be primitive;
- MUST NOT be float types;
- MUST NOT be nested beneath an optional struct; and
- retain stable field IDs.

---

## 18. Partition semantics

### 18.1 Hidden partitioning

Queries are expressed against source fields. Writers and readers use the active partition spec to transform source values into partition values.

Physical partition column names are not part of query semantics.

### 18.2 Immutable partition specs

A partition spec is immutable and identified by `partition_spec_id`.

Spec ID `0` is the required unpartitioned specification and contains no partition fields.

New partitioning creates a new spec. Existing files retain their original spec ID and partition tuple.

### 18.3 Stable partition-field identity

Partition field IDs are table-global and MUST NOT be reused.

Renaming a partition field does not change its ID.

### 18.4 Core transforms

Core transforms are:

```text
identity
year
month
day
hour
truncate(width)
bucket(num_buckets)
void
```

Transform JSON examples:

```json
{"transform":"day"}
```

```json
{"transform":"bucket","num_buckets":32}
```

### 18.5 Bucket transform

The core bucket transform is:

```text
positive_mod(murmur3_x86_32(canonical_source_bytes), num_buckets)
```

Canonical source bytes are defined in Appendix C.

### 18.6 Partition tuple

Each file stores a deterministic CBOR map:

```text
partition_field_id -> typed scalar value
```

Keys are sorted by numeric partition-field ID.

A file’s `partition_hash` is:

```text
SHA256("OTMP-PARTITION\0" || deterministic_cbor_partition_tuple)
```

### 18.7 Partition evolution

Readers MUST interpret each file using the file’s own `partition_spec_id`.

A query planner MAY evaluate predicates across multiple specs by projecting source-field predicates through each spec independently.

---

## 19. Sort semantics

### 19.1 Immutable sort orders

Sort order ID `0` means unsorted.

Every other sort order is immutable and contains ordered sort fields.

### 19.2 Sort field

A sort field defines:

- source field ID;
- optional transform;
- direction `asc` or `desc`; and
- null ordering.

### 19.3 Evolution

New sort behavior creates a new sort-order ID.

Files retain the sort order under which they were written. A snapshot records the default sort order for new files.

Sort order is advisory for readers unless a feature explicitly requires ordering guarantees.

---

## 20. File descriptors and metrics

### 20.1 Immutability

A committed file descriptor identifies immutable bytes.

A writer MUST NOT overwrite a committed object at the same identity.

File identity consists of:

- `file_id` inside OTMP; and
- URI plus optional storage object identity, version, or content hash for physical bytes.

### 20.2 File kinds

The core relational schema recognizes:

```text
data
position_delete
equality_delete
```

Use of delete files requires the corresponding reader/writer feature.

### 20.3 Data-file descriptor

A data file records:

- file ID;
- URI;
- object identity when available;
- file format feature;
- byte length;
- record count;
- schema ID;
- partition spec ID and tuple;
- optional sort-order ID;
- content hash when available;
- data sequence number;
- file sequence number;
- creating snapshot; and
- per-column metrics.

### 20.4 Sequence numbers

Each committed snapshot receives one table-wide `sequence_number` greater than every previous snapshot sequence number, including snapshots on other branches.

A file receives:

- `data_sequence_number` — the logical data version relevant to delete applicability; and
- `file_sequence_number` — the snapshot sequence at which the physical file was added.

For a newly appended data file, both normally equal the creating snapshot sequence.

A rewrite MAY preserve the original data sequence while assigning a new file sequence.

### 20.5 Metrics

Per-field metrics MAY include:

- column size;
- value count;
- null count;
- NaN count;
- distinct count;
- lower bound;
- upper bound; and
- bloom-filter reference.

Bounds use typed deterministic CBOR.

Metrics MUST NOT be used when their field type or encoding is unknown to the reader.

### 20.6 Relative paths

Writers SHOULD use relative URIs for table-owned data and delete files.

External absolute URIs are allowed but reduce table-move portability.

### 20.7 Delete-file feature

The optional `otmp.delete-files.v1` feature defines descriptor-level semantics:

- a position-delete file may reference one data file or encode data-file URI plus row position;
- an equality-delete file declares equality field IDs;
- delete applicability is bounded by data sequence numbers; and
- the physical delete-file schema is selected by its data-format feature.

A reader that does not support required delete features MUST reject the snapshot rather than ignore deletes.

---

## 21. Snapshot semantics

### 21.1 Snapshot identity

A snapshot is immutable and identified by a 16-byte `snapshot_id`.

It records:

- zero or one parent snapshot;
- one table-wide sequence number;
- schema, partition spec, and sort order;
- operation kind;
- creating table version;
- commit timestamp;
- summary; and
- optional scan projection.

### 21.2 Snapshot operations

Core operation labels are:

```text
append
overwrite
rewrite
delete
update
merge
optimize
metadata
```

The label is descriptive. File additions/removals and semantic requirements determine actual behavior.

### 21.3 Snapshot file changes

A snapshot contains an immutable set of file additions and removals.

For a linear parent chain:

```text
Live(snapshot) = Live(parent) - removed_files + added_files
```

The relational database materializes the complete live file set for every mutable branch head in `otmp_ref_live_files`.

### 21.4 Empty table

The required `main` branch MAY have a null snapshot before the first data or metadata snapshot.

### 21.5 Historical reads

A historical snapshot MAY be planned through:

- its scan projection;
- a retained materialized branch/tag state;
- snapshot file-change traversal; or
- a catalog/engine cache.

The current mutable branch read path MUST NOT require semantic commit replay.

### 21.6 Snapshot integrity

A committed snapshot MUST NOT:

- add the same file ID twice;
- remove a file not live in the target parent unless the operation explicitly allows idempotent absence;
- reference missing schema/spec/order IDs;
- create a parent cycle;
- reuse a sequence number; or
- reference files whose immutable identity conflicts with an existing file.

---

## 22. References: branches and tags

### 22.1 Required main branch

Every table has a mutable branch named `main`.

### 22.2 Branch

A branch points to zero or one current snapshot and MAY advance through future commits.

The complete live file membership for each branch MUST be materialized in `otmp_ref_live_files` when `otmp.refs.v1` is enabled.

### 22.3 Tag

A tag points to exactly one snapshot and is immutable after creation.

Replacing a tag requires dropping it and creating a new tag with a different name unless a future extension permits mutable tags.

### 22.4 Branch creation

Creating a branch from a snapshot logically gives the branch that snapshot’s live file set.

An implementation MAY materialize the set by:

- bulk inserting file IDs into `otmp_ref_live_files`;
- using an internal copy-on-write index; or
- using another equivalent physical optimization.

The published relational image MUST expose the same result.

### 22.5 Retention

Refs MAY carry retention properties. Retention influences snapshot and object garbage collection but does not change snapshot semantics.

---

## 23. Scan projection profile: `otmp.scan.parquet.v1`

### 23.1 Purpose

The relational metadata image is optimized for targeted SQL metadata access. A scan projection is an optional immutable columnar index optimized for:

- planning scans over millions of files;
- reading selected statistic columns;
- parallel planning by distributed engines; and
- avoiding remote B-tree traversal for broad scans.

The projection is derived from the same semantic snapshot. It is not a second authority.

### 23.2 Scan root

A scan root is canonical JSON with media type:

```text
application/vnd.otmp.scan-root+json
```

It contains:

```json
{
  "kind": "otmp.scan-root",
  "format_version": 1,
  "table_id": "...",
  "snapshot_id": "...",
  "table_version": "42",
  "semantic_state_sha256": "sha256:...",
  "schema_id": "12",
  "partition_spec_ids": ["0", "4"],
  "manifests": [
    {
      "uri": "_otmp/scan/<snapshot>/manifests/0001.parquet",
      "sha256": "sha256:...",
      "length": "...",
      "file_count": "...",
      "content_kinds": ["data"],
      "partition_summary_cbor": "base64url:..."
    }
  ]
}
```

### 23.3 Manifest rows

A Parquet manifest row represents one live data or delete file and MUST include:

- `file_id` fixed 16 bytes;
- `file_kind` integer or enum;
- `uri` UTF-8;
- optional object identity;
- file format;
- size and record count;
- schema ID;
- partition spec ID;
- optional sort-order ID;
- partition tuple deterministic CBOR;
- data and file sequence numbers;
- optional content hash;
- deterministic CBOR metrics;
- optional delete details; and
- creating snapshot ID.

### 23.4 Equivalence

A scan projection MUST represent exactly the live file set of its snapshot.

The projection root MUST carry the same:

- table ID;
- snapshot ID;
- table version; and
- semantic state hash

as the relational image.

A writer advertising `otmp.scan.parquet.v1` as required MUST publish the projection before advancing `HEAD`.

### 23.5 Planner choice

A reader MAY choose:

- relational SQL indexes for targeted metadata access; or
- the scan projection for broad distributed planning.

Both paths MUST produce semantically equivalent file tasks.


---

## 24. Requirements

### 24.1 Purpose

Requirements describe facts that MUST remain true when a semantic transaction is applied.

Requirements are evaluated against the current relational metadata generation, not against raw object listings.

A writer preparing from an older version MUST re-evaluate every requirement after any publication conflict.

### 24.2 Common shape

```json
{
  "type": "ref_snapshot_is",
  "ref": "main",
  "snapshot_id": "018f..."
}
```

### 24.3 Core requirement kinds

#### `table_version_is`

```json
{"type":"table_version_is","table_version":"41"}
```

Useful for strict compare-and-swap semantics. It prevents semantic rebase.

#### `semantic_state_is`

```json
{"type":"semantic_state_is","sha256":"sha256:..."}
```

#### `ref_exists`

```json
{"type":"ref_exists","ref":"main","ref_type":"branch"}
```

#### `ref_absent`

```json
{"type":"ref_absent","ref":"experiment"}
```

#### `ref_snapshot_is`

```json
{
  "type":"ref_snapshot_is",
  "ref":"main",
  "snapshot_id":"018f..."
}
```

A null snapshot MAY be represented with `snapshot_id: null`.

#### `snapshot_exists`

```json
{"type":"snapshot_exists","snapshot_id":"018f..."}
```

#### `current_schema_is`

```json
{"type":"current_schema_is","schema_id":"12"}
```

#### `default_partition_spec_is`

```json
{"type":"default_partition_spec_is","partition_spec_id":"4"}
```

#### `default_sort_order_is`

```json
{"type":"default_sort_order_is","sort_order_id":"2"}
```

#### `property_is`

```json
{
  "type":"property_is",
  "key":"write.target-file-size-bytes",
  "value": "536870912"
}
```

Absence is expressed with `value: null`.

#### `file_live`

```json
{
  "type":"file_live",
  "ref":"main",
  "file_id":"018f..."
}
```

#### `file_not_live`

```json
{
  "type":"file_not_live",
  "ref":"main",
  "file_id":"018f..."
}
```

#### `file_identity_absent`

```json
{
  "type":"file_identity_absent",
  "uri":"data/part-001.parquet",
  "object_identity":"etag-or-version"
}
```

#### `feature_enabled`

```json
{"type":"feature_enabled","feature":"otmp.scan.parquet.v1"}
```

### 24.4 Requirement failure

A failed requirement is a semantic conflict. The transaction MUST NOT be published in its current form.

An implementation MUST report which requirement failed when doing so does not violate security policy.

---

## 25. Core semantic operations

Operations are applied in array order inside one private relational transaction.

Every operation has a commit-unique UTF-8 `operation_id`. Operation examples below omit it only when the surrounding prose makes the identity irrelevant.

All operations in one semantic commit are atomic. A commit MAY create multiple snapshots, including snapshots on different branches of the same table, provided their sequence numbers are unique and all changes publish together.

### 25.1 `initialize_table`

Genesis-only operation.

It defines:

- table ID;
- initial schema;
- initial properties;
- initial required features;
- unpartitioned spec `0`;
- unsorted order `0`; and
- `main` branch with a null snapshot.

Version `0` MUST NOT create a snapshot. Initial data, when present, is committed
by the first non-genesis `commit_snapshot` operation. This keeps snapshot
creation aligned with the relational invariant that snapshots have a positive
`committed_table_version`.

It MUST NOT appear after version 0.

### 25.2 `set_properties`

```json
{
  "type": "set_properties",
  "updates": {
    "write.target-file-size-bytes": "536870912",
    "history.min-reader-retention-ms": "86400000"
  },
  "removals": ["legacy.property"]
}
```

Property keys are UTF-8 strings. Values are canonical JSON values.

Reserved `otmp.` keys require specification-defined semantics.

### 25.3 `upgrade_features`

```json
{
  "type": "upgrade_features",
  "add": [
    {"name":"otmp.scan.parquet.v1","requirement":"reader"}
  ]
}
```

Feature requirements are monotonic unless a later protocol explicitly defines safe downgrade.

### 25.4 `add_schema`

Adds one immutable schema and all newly allocated field IDs.

```json
{
  "type": "add_schema",
  "schema_id": "13",
  "parent_schema_id": "12",
  "fields": [],
  "identifier_field_ids": []
}
```

The operation MUST satisfy the evolution rules in Section 17.

### 25.5 `set_current_schema`

```json
{"type":"set_current_schema","schema_id":"13"}
```

This changes the default for new writes. Existing snapshots and files retain their schema IDs.

### 25.6 `add_partition_spec`

```json
{
  "type":"add_partition_spec",
  "partition_spec_id":"5",
  "parent_partition_spec_id":"4",
  "fields":[
    {
      "partition_field_id":"1002",
      "source_field_id":"7",
      "name":"sale_day",
      "transform":{"transform":"day"},
      "result_type":{"type":"date"}
    }
  ]
}
```

### 25.7 `set_default_partition_spec`

```json
{"type":"set_default_partition_spec","partition_spec_id":"5"}
```

### 25.8 `add_sort_order`

```json
{
  "type":"add_sort_order",
  "sort_order_id":"3",
  "parent_sort_order_id":"2",
  "fields":[
    {
      "source_field_id":"7",
      "transform":{"transform":"identity"},
      "direction":"asc",
      "null_order":"nulls_last"
    }
  ]
}
```

### 25.9 `set_default_sort_order`

```json
{"type":"set_default_sort_order","sort_order_id":"3"}
```

### 25.10 `create_ref`

```json
{
  "type":"create_ref",
  "ref":"experiment",
  "ref_type":"branch",
  "snapshot_id":"018f...",
  "retention":{}
}
```

The ref name MUST be absent.

### 25.11 `replace_ref`

```json
{
  "type":"replace_ref",
  "ref":"main",
  "expected_snapshot_id":"018f...",
  "new_snapshot_id":"018f..."
}
```

This is primarily for controlled rollback or branch movement. Normal snapshot commits advance the ref as part of `commit_snapshot`.

A tag MUST NOT be replaced in core OTMP.

### 25.12 `drop_ref`

```json
{"type":"drop_ref","ref":"experiment"}
```

The `main` branch MUST NOT be dropped.

### 25.13 `commit_snapshot`

`commit_snapshot` is the principal data-changing operation.

Example:

```json
{
  "type":"commit_snapshot",
  "target_ref":"main",
  "snapshot": {
    "snapshot_id":"018f...",
    "parent_snapshot_id":"018e...",
    "sequence_number":"108",
    "schema_id":"13",
    "partition_spec_id":"5",
    "sort_order_id":"3",
    "operation":"append",
    "summary": {
      "added-data-files":"2",
      "added-records":"194244"
    },
    "metadata": {}
  },
  "added_files": [],
  "removed_file_ids": [],
  "scan_projection": null,
  "rebase_mode":"append-safe"
}
```

Application effects:

1. validate the target ref and expected parent;
2. allocate the next unique table sequence number;
3. insert the snapshot;
4. insert new immutable file descriptors and metrics;
5. insert snapshot file-change rows;
6. remove requested files from target-branch live membership;
7. add new files to target-branch live membership;
8. advance the branch to the new snapshot;
9. update `otmp_meta.last_sequence_number`; and
10. record optional scan projection references.

`sequence_number` MAY be omitted by a client and assigned by the committer. The committed semantic object MUST contain the final assigned value.

The caller-supplied `snapshot.metadata` object describes the created immutable
data state. It is distinct from the semantic commit's top-level `metadata`
object and from derived snapshot summary values.

### 25.14 `add_file_metrics`

Adds metrics to an existing immutable file descriptor without changing its bytes.

```json
{
  "type":"add_file_metrics",
  "file_id":"018f...",
  "metrics":[]
}
```

This operation normally creates a metadata snapshot when the newly visible metrics must be tied to table history.

### 25.15 Extension operations

An extension operation MUST use a globally unique namespaced type, such as:

```text
com.example.otmp.optimize-index
```

A reader or writer MUST reject an unknown operation when the corresponding feature is required.

---

## 26. Transaction application semantics

### 26.1 Private application

A writer applies one semantic commit candidate to a private writable database view derived from the pinned parent metadata generation.

No published generation is modified.

### 26.2 One local transaction

All requirements are evaluated and all operations are applied inside one local transaction or an equivalent serializable unit.

The local runtime MUST provide all-or-nothing behavior for:

- normative row changes;
- constraints;
- branch live-file materialization;
- commit history;
- idempotency results; and
- post-commit metadata values.

### 26.3 Required post-state

Before artifact publication, the candidate relational image MUST:

- pass relational constraints;
- agree with the semantic commit;
- have the new table version;
- have the new semantic state hash;
- contain all intent results;
- contain the exact branch live-file state; and
- be a valid SQLite image.

### 26.4 Commit-object hash in relational state

The semantic commit bytes SHOULD be finalized before the relational transaction is committed so the commit URI and hash can be inserted into `otmp_commits`.

When a runtime requires a different order, it MUST still produce a final database image whose commit row exactly matches the uploaded immutable commit object.

### 26.5 No arbitrary state divergence

A writer MUST NOT publish a metadata image containing normative changes absent from the semantic commit, except for deterministic maintenance fields explicitly declared non-semantic by the protocol.

A writer MUST NOT publish a semantic operation whose required relational effects are absent from the metadata image.

---

## 27. Catalog-free read protocol

### 27.1 Entry point

A reader begins with a table root URI.

No catalog call is required.

### 27.2 Pin `HEAD`

The reader:

1. fetches `_otmp/HEAD`;
2. validates canonical JSON and feature support;
3. records the exact bytes and storage version token when available; and
4. pins the referenced table version and metadata generation for the operation.

The reader MUST NOT silently switch generations during one planning operation.

### 27.3 Fetch generation

The reader fetches and verifies the generation descriptor.

It validates:

- table ID;
- table version;
- semantic state hash;
- semantic commit reference;
- metadata image codec;
- page size and count; and
- required features.

### 27.4 Open relational metadata

A reader chooses one of two core paths.

#### Materialized-file path

1. fetch the base checkpoint;
2. resolve all page overrides;
3. construct a local SQLite file;
4. verify its metadata identity and integrity; and
5. open read-only.

#### Remote-VFS path

1. open a read-only SQLite connection through an OTMP VFS;
2. resolve each requested page through the page map;
3. fetch ranges from the checkpoint or page packs;
4. verify and cache immutable bytes; and
5. return pages to SQLite.

### 27.5 Resolve current snapshot

For default reads:

```sql
SELECT snapshot_id
FROM otmp_refs
WHERE ref_name = 'main'
  AND ref_type = 'branch';
```

A named branch or tag MAY be selected when `otmp.refs.v1` is supported.

### 27.6 Targeted file planning

A targeted planner MAY query:

```sql
SELECT f.*
FROM otmp_live_files f
WHERE f.ref_name = ?
  AND f.file_kind = 'data'
  AND f.partition_spec_id = ?
  AND f.partition_hash = ?;
```

It MAY join `otmp_file_metrics` for pruning.

### 27.7 Broad scan planning

When a snapshot has an `otmp.scan.parquet.v1` projection and the query is broad, the reader MAY use the scan root and manifests instead of traversing the relational file tables.

### 27.8 Delete planning

A reader MUST include all applicable delete files required by the snapshot and sequence semantics.

A reader lacking a required delete feature MUST fail closed.

### 27.9 Data read

The query engine reads selected data and delete files directly from their URIs.

The metadata runtime is not in the row-processing path after planning unless an implementation chooses to retain it.

### 27.10 Reader completion

After planning or query completion, the embedded runtime MAY exit and caches MAY be discarded.

No table-specific process is required to remain running.

---

## 28. Direct distributed write protocol

### 28.1 Overview

A direct writer performs:

```text
write immutable data files
        ↓
pin HEAD
        ↓
open private relational metadata view
        ↓
validate intents and requirements
        ↓
apply semantic operations atomically
        ↓
capture changed SQLite pages
        ↓
publish immutable artifacts
        ↓
CAS HEAD
```

### 28.2 Stage immutable user files

The writer first writes every new data, delete, or auxiliary file using a unique immutable identity.

The writer collects:

- URI;
- object identity or version;
- content hash when available;
- file size;
- record count;
- schema and partition metadata;
- statistics; and
- encryption metadata.

These files remain invisible until a committed snapshot references them.

### 28.3 Read and pin parent

The writer reads `HEAD` and obtains:

- parent table version;
- parent root revision;
- parent semantic state hash;
- parent generation;
- required features; and
- opaque storage CAS token.

### 28.4 Open private writable view

The writer opens the parent generation using:

- a fully materialized private SQLite file;
- a writable copy-on-write VFS;
- Turso MVCC;
- another SQLite-compatible private runtime; or
- an equivalent relational engine that exports the canonical image.

Published objects remain read-only.

### 28.5 Check idempotency

For each intent:

```sql
SELECT intent_sha256, result_json
FROM otmp_idempotency
WHERE idempotency_key = ?;
```

Outcomes:

- absent: continue;
- same hash: return the prior result for that intent;
- different hash: fail with idempotency conflict.

A group commit MAY contain a mix of new and already-committed intents only when the coordinator removes already-committed intents before forming the new semantic commit.

### 28.6 Build candidate semantic commit

The writer assigns:

- stable commit ID;
- next candidate table version;
- parent commit;
- intents;
- requirements;
- operations;
- final operation-assigned IDs and sequence numbers; and
- post-commit feature sets.

Caller-supplied commit metadata and the metadata of each proposed snapshot are
part of logical intent identity. They remain stable when candidate-assigned
versions, IDs, timestamps, parents, sequence numbers, hashes, and artifact URIs
are regenerated during rebase.

It computes the semantic state hash.

### 28.7 Apply locally

The writer:

1. begins one local transaction;
2. evaluates requirements;
3. applies operations in order;
4. inserts commit and idempotency rows;
5. updates current state;
6. commits locally; and
7. validates the resulting image.

### 28.8 Capture physical changes

The writer records the final bytes of every SQLite page changed by the successful local transaction.

Intermediate or rolled-back page versions MUST NOT be published.

A simple implementation MAY compare the parent and child SQLite files page-by-page.

An optimized implementation MAY capture dirty pages through a custom VFS, WAL conversion, or MVCC export.

### 28.9 Build physical artifacts

The writer:

1. groups changed pages into one or more page packs;
2. updates the persistent page map;
3. determines the exact page count;
4. computes `image_root_sha256`;
5. creates the metadata generation descriptor; and
6. creates any required scan projection.

### 28.10 Validate equivalence

Before publication, the writer MUST validate that:

- the semantic commit and relational post-state agree;
- the generation reconstructs the exact candidate SQLite image;
- required scan projections match the snapshot live file set;
- all referenced user files exist with the declared immutable identity; and
- all hashes are correct.

### 28.11 Upload immutable objects

The writer uploads with create-only semantics where possible:

- user files not already uploaded;
- semantic commit;
- page packs;
- page-map nodes;
- scan root and manifests;
- metadata generation; and
- optional auxiliary artifacts.

The writer MUST ensure all required objects are durably readable before publishing `HEAD`.

### 28.12 Publish `HEAD`

The writer constructs a new `HEAD` with:

```text
table_version  = parent table version + 1
root_revision  = parent root revision + 1
```

It conditionally replaces `HEAD` using the pinned CAS token.

### 28.13 Success

The transaction is committed only when the conditional `HEAD` replacement succeeds.

The writer then returns each intent’s stable result.

### 28.14 Failed publication

If the conditional replacement fails:

- no candidate artifact became committed;
- candidate user files and metadata objects are orphans;
- the writer MUST NOT mutate those objects;
- the writer MAY reuse immutable user files whose identity is still valid; and
- semantic retry follows Section 29.

---

## 29. Concurrency, rebasing, and conflicts

### 29.1 Reader concurrency

Readers pin immutable generations and require no locks.

A writer publishing version `N+1` does not affect a reader pinned to `N`.

### 29.2 Writer concurrency

Many writers MAY concurrently:

- write data files;
- read the same parent generation;
- validate operations;
- build candidate relational states;
- build page packs and scan projections; and
- upload immutable candidates.

Only `HEAD` publication is serialized per table root.

### 29.3 CAS conflict

A failed `HEAD` compare-and-swap is a physical publication conflict, not automatically a semantic conflict.

The writer:

1. reads the new current `HEAD`;
2. checks whether each idempotency key is already committed;
3. opens the new current metadata generation;
4. re-evaluates requirements;
5. applies operation-specific rebase rules; and
6. either creates a new candidate or reports a semantic conflict.

### 29.4 Never merge physical pages

A writer MUST NOT merge page packs or SQLite pages produced against different parent generations.

Rebase occurs by reapplying semantic operations to the new parent relational state.

### 29.5 Core rebase guidance

| Operation | Rebase behavior |
|---|---|
| Pure append with unique new files | Usually safe when schema/spec compatibility remains and files are still absent |
| Add independent schema then set current | Safe only when new IDs remain unused and parent assumptions still hold |
| Add independent partition or sort definition | Safe when IDs remain unused |
| Set property | Safe when no `property_is` requirement failed; last-writer behavior is NOT implicit |
| Rewrite files | Safe only when every required input file remains live |
| Delete files | Safe only when target files remain live and delete semantics remain valid |
| Rename represented as property/catalog action | Not a core table-format operation |
| Move branch | Requires expected ref snapshot |
| Create ref | Requires name absence |
| Drop ref | Requires expected ref state when concurrent movement matters |

### 29.6 Append-safe snapshot rebase

`commit_snapshot.rebase_mode = "append-safe"` permits the committer to choose the new current target-ref snapshot as the parent when:

- the operation removes no files;
- every added file identity remains absent;
- the target ref still exists;
- the new parent schema is read-compatible with the files;
- partition and sort metadata remain valid; and
- no explicit requirement forbids rebase.

The retry receives a new table version and sequence number but retains the same logical intent ID and file IDs.

### 29.7 Conflict classes

Core conflict classes are:

- `OTMP_HEAD_CONFLICT` — `HEAD` changed;
- `OTMP_REQUIREMENT_FAILED` — a semantic requirement is false;
- `OTMP_IDEMPOTENCY_CONFLICT` — key reused with a different intent;
- `OTMP_REF_CONFLICT` — branch/tag state conflicts;
- `OTMP_FILE_CONFLICT` — file identity or liveness conflicts;
- `OTMP_SCHEMA_CONFLICT` — schema ID, field ID, or evolution conflict;
- `OTMP_PARTITION_CONFLICT` — partition-spec or partition-field conflict;
- `OTMP_SORT_CONFLICT` — sort-order conflict;
- `OTMP_FEATURE_CONFLICT` — unsupported required feature;
- `OTMP_OBJECT_CONFLICT` — immutable URI already exists with different bytes; and
- `OTMP_CORRUPT_STATE` — hashes or relational invariants do not match.

### 29.8 Fairness

The direct protocol does not guarantee fairness among highly contending writers.

A deployment requiring fairness, admission control, or high-throughput group commit SHOULD use an optional commit coordinator while preserving the same table artifacts and `HEAD` semantics.

---

## 30. Optional commit coordinator and group commit

### 30.1 Non-authoritative role

A catalog, serverless function, actor, broker, or managed service MAY coordinate commits.

The coordinator:

- MAY cache the metadata image;
- MAY authenticate and authorize writers;
- MAY assign sequence numbers;
- MAY combine compatible intents;
- MAY serialize conflicting operations;
- MAY provide fairness and backpressure; and
- MAY use a private fast transactional store.

It MUST still publish conforming immutable semantic commits, metadata generations, and `HEAD`.

### 30.2 Zero-resident-compute coordinator

A coordinator MAY start on demand, process a burst, and terminate after inactivity.

It MUST NOT hold unique durable table state that cannot be reconstructed from the table package.

### 30.3 Group commit

A coordinator MAY combine multiple compatible intents into one semantic commit and one table version.

A group commit:

- contains all included intent keys and hashes;
- applies all operations atomically;
- stores a result for every intent;
- may combine multiple appends into one snapshot;
- publishes one metadata generation and one `HEAD` update; and
- commits either all included new intents or none.

### 30.4 Catalog-managed mode

A catalog MAY deny direct storage writes through access policy and require all writers to use the coordinator.

This does not change the table format. A reader with sufficient storage credentials can still interpret the table by location.

---

## 31. Physical checkpointing and metadata compaction

### 31.1 Purpose

Checkpointing reduces:

- page-map size;
- page-map lookup depth;
- page-pack fragmentation;
- cold-start requests;
- recovery work; and
- historical physical storage.

It does not change semantic table state.

### 31.2 Trigger policy

Implementations SHOULD checkpoint based on measured thresholds such as:

- override page ratio;
- page-map node count;
- page-pack count;
- bytes since checkpoint;
- maximum page-map height;
- cold-open request count;
- time since checkpoint; or
- metadata image growth.

No threshold is normative in alpha.

### 31.3 Checkpoint algorithm

A checkpointer:

1. reads and pins current `HEAD` and CAS token;
2. opens the current generation;
3. materializes the complete SQLite image;
4. validates integrity and semantic identity;
5. optionally canonicalizes and compacts the file;
6. uploads a new immutable checkpoint;
7. creates a new generation for the same table version and semantic state hash, normally with an empty page map;
8. constructs a new `HEAD` with unchanged semantic commit and table version, and `root_revision + 1`; and
9. conditionally replaces `HEAD`.

### 31.4 Checkpoint race

If any writer or other checkpointer changes `HEAD` before step 9, the checkpoint publication fails safely.

The new checkpoint remains an unreferenced artifact and MAY be garbage-collected or reused only after revalidation.

### 31.5 Reader behavior

Readers pinned to the old generation remain valid.

New readers use the newer physical generation while observing identical semantic state.

### 31.6 Semantic history retention

Checkpointing MUST NOT silently delete semantic commits required by retention policy, audit, replication, or deterministic rebuild.

Physical page packs no longer reachable from retained generations MAY become eligible for deletion after the reader grace period.

---

## 32. Conformance profiles

### 32.1 Core table reader

A Core Table Reader MUST:

- read and validate `HEAD`;
- validate required features;
- read generation and semantic identity;
- open `otmp.metadata.sqlite3-cow.v1` by materialization or VFS;
- query the normative relational schema;
- resolve current `main` snapshot and files;
- honor file immutability and sequence semantics; and
- fail closed on unsupported required features.

### 32.2 Catalog-free direct writer

A Direct Writer MUST additionally:

- stage immutable files;
- validate requirements;
- apply core semantic operations;
- maintain idempotency;
- produce a valid relational image;
- create page packs and page-map updates;
- upload immutable objects;
- publish with linearizable `HEAD` CAS; and
- semantically rebase after conflicts.

### 32.3 Full-image writer

A Full-Image Writer MAY publish a complete SQLite checkpoint for every semantic version instead of page packs.

It is conforming but may have high write amplification. Its generation advertises
`otmp.metadata.sqlite3-cow.v1`, because the checkpoint is a complete materialization
of that logical image profile rather than a different metadata codec.

Its generation uses a complete checkpoint and `page_map: null`. It computes
`image_root_sha256` with the normative Section 14.7 formula, including 32 zero
bytes for the absent page-map root. The image-root hash is not the checkpoint hash.

### 32.3.1 Gate 1 Rust local/full-image profile (non-normative)

The implementation in the accompanying Rust proof of concept targets a narrower
qualification slice than the Core Reader or Direct Writer profiles. Gate 1 covers
self-contained genesis, pinned catalog-free reads, byte-verified table-relative
staging, one non-empty atomic Parquet-descriptor append batch to `main`, stable
idempotent retry results, conditional-publication reconciliation, and append-safe
semantic rebase using complete SQLite checkpoints and a null page map.

The Gate 1 implementation accepts exactly one `initialize_table` operation at
genesis and exactly one append-only `commit_snapshot` operation thereafter. It
rejects every other core or extension operation. On pin, it verifies that the
supported semantic operation agrees with the relational snapshot row, target
ref, normalized summary, file-change set, immutable file descriptors,
partition encodings and hashes, live-file projection, and file metrics.
It also verifies the Gate 1 history shape: no genesis snapshot, one contiguous
append snapshot per positive table version, one `main` branch, unbroken parent
ancestry, and a sequence allocator equal to the current snapshot sequence.

Gate 1 does not claim cloud correctness, Parquet semantic validation, page maps,
remote VFS support, delete files, scan projections, garbage collection, managed
coordination, complete Core Reader or Direct Writer conformance, or production
readiness.

### 32.4 Checkpoint writer

A Checkpoint Writer implements Section 31.

### 32.5 Scan projection reader

A Scan Projection Reader implements `otmp.scan.parquet.v1`.

### 32.6 Scan projection writer

A Scan Projection Writer creates and validates scan roots/manifests atomically with snapshots requiring that feature.

### 32.7 Delete-aware reader/writer

A Delete-Aware implementation supports required position and equality delete features.

### 32.8 Catalog adapter

A Catalog Adapter maps names and governance to table roots but MUST preserve self-contained table semantics.

### 32.9 Full implementation

A Full Implementation supports:

- direct read and write;
- incremental page representation;
- checkpointing;
- scan projections;
- refs;
- delete features advertised as supported;
- recovery and validation tooling; and
- the conformance tests in Appendix E.

---

## 33. Feature negotiation

### 33.1 Feature naming

Features use globally unique lowercase names.

Core registered features include:

```text
otmp.core.v2
otmp.metadata.sqlite3-cow.v1
otmp.scan.parquet.v1
otmp.refs.v1
otmp.data.parquet.v1
otmp.data.orc.v1
otmp.delete-files.v1
otmp.delete.position.v1
otmp.delete.equality.v1
otmp.signatures.v1
otmp.encryption-metadata.v1
```

### 33.2 Reader and writer requirements

`required_reader_features` are needed to read current table state correctly.

`required_writer_features` are needed to produce valid future commits while preserving all current semantics.

A reader MUST reject an unknown required reader feature.

A writer MUST reject an unknown required writer feature.

### 33.3 Monotonic upgrade

Required features are monotonic in core OTMP.

A feature MUST NOT be removed while any retained snapshot or file depends on it.

### 33.4 Optional metadata

Unknown fields in extension namespaces MAY be preserved as opaque metadata when the relevant feature is not required.

Unknown core fields MUST follow the version’s forward-compatibility rules. In alpha, implementations SHOULD reject unknown required core semantics.


---

## 34. Failure and recovery semantics

### 34.1 Writer fails before uploading protocol artifacts

No metadata change is visible.

Any newly written user files are unreferenced and may later be classified as orphans.

### 34.2 Writer uploads artifacts but not `HEAD`

All candidate artifacts remain uncommitted because they are unreachable from current `HEAD`.

### 34.3 Writer publishes `HEAD` but loses the response

The commit succeeded.

A retry with the same intent key and hash discovers the committed idempotency row and returns the prior result.

### 34.4 Partial immutable upload

A writer MUST NOT publish `HEAD` until every required object is durably readable and hash-valid.

If a storage system can acknowledge a write before globally readable durability, the implementation MUST use stronger durability confirmation or a coordinator that supplies it.

### 34.5 Corrupt `HEAD`

A reader SHOULD retain or obtain storage object version history when available.

Recovery MAY:

- restore the previous valid `HEAD` object version;
- locate a valid generation and semantic commit through administrative records;
- verify the semantic chain and metadata image; and
- publish a repaired `HEAD` with a new root revision under controlled recovery policy.

Object listing is permitted for recovery.

### 34.6 Missing immutable object

A missing object reachable from `HEAD` is corruption.

A reader MUST NOT silently fall back to a different table version unless explicitly requested by the caller.

Recovery MAY reconstruct a metadata generation from:

- a retained checkpoint;
- semantic commits;
- retained scan projections; and
- immutable data-file descriptors.

### 34.7 Physical-state rebuild

A conforming recovery tool SHOULD be able to:

1. select a trusted checkpoint;
2. replay retained semantic commits in order into the normative relational schema;
3. validate every semantic requirement and operation;
4. produce a complete SQLite image;
5. verify semantic state hash;
6. create a new metadata generation; and
7. publish a repaired `HEAD` under explicit administrative control.

Semantic replay is a recovery path, not the ordinary read path.

### 34.8 Checkpointer failure

A failed checkpointer cannot corrupt the current table because its new checkpoint is invisible until `HEAD` CAS succeeds.

### 34.9 Coordinator failure

A commit coordinator MUST acknowledge success only after conforming `HEAD` publication.

Unique state retained only in coordinator memory is not committed state.

---

## 35. Time travel and historical access

### 35.1 Snapshot time travel

A caller MAY select a snapshot by:

- snapshot ID;
- branch or tag;
- table sequence number; or
- implementation-provided timestamp resolution.

Timestamp selection MUST resolve to an actual committed snapshot before planning.

### 35.2 Historical metadata generation

A retained historical metadata generation MAY be opened directly when its descriptor is known.

The protocol does not require `HEAD` to expose every historical generation.

Implementations MAY discover retained generations through:

- parent-generation references;
- semantic commit metadata;
- scan artifacts;
- a catalog index;
- an administrative history file; or
- object listing outside the normal read path.

### 35.3 Historical snapshot planning

A historical snapshot’s file set is obtained through its scan projection when present.

Otherwise an implementation MAY reconstruct the set from snapshot file changes and ancestry.

### 35.4 Tag stability

A retained tag provides a stable named snapshot reference and therefore SHOULD be used for long-lived reproducible reads.

### 35.5 Retention caveat

Time travel is available only while the required metadata and data objects remain retained.

---

## 36. Catalog integration

### 36.1 Optional manager

A catalog MAY provide:

- namespace and table names;
- table-location resolution;
- search and discovery;
- authentication and authorization;
- credential vending;
- policy enforcement;
- audit and lineage indexes;
- caching;
- commit coordination;
- maintenance scheduling; and
- cross-table transactions above OTMP.

### 36.2 Authority boundary

The OTMP table package remains the authoritative table-format state.

A catalog MUST NOT require hidden metadata unavailable from the table package to interpret:

- schema;
- snapshots;
- refs;
- live files;
- delete applicability;
- statistics;
- required features; or
- current semantic table version.

### 36.3 Managed write policy

A catalog MAY enforce that only its service identity can update `HEAD`.

This is a deployment access-control policy, not a different table format.

### 36.4 Cached catalog state

A catalog cache MUST be invalidated or validated against `HEAD` and semantic state identity.

A stale cache MUST NOT publish a commit without a valid table-head fence.

### 36.5 Table movement

A table SHOULD remain operable after copying its root and relative objects to a new location.

A catalog move updates name-to-location mapping. It does not rewrite stable table identity.

Absolute external file URIs may require separate relocation tooling.

### 36.6 Compatibility APIs

A catalog MAY expose OTMP tables through:

- an OTMP-native API;
- Iceberg REST-compatible projections;
- Unity Catalog-compatible APIs;
- SQL metadata APIs; or
- other adapters.

Adapters MUST preserve OTMP semantics and MUST fail rather than silently discard required features.

### 36.7 Application-defined catalog correlation

A catalog MAY place stable correlation data in a semantic commit's top-level
`metadata` object under a reverse-domain namespace it controls. OTMP assigns no
standard meaning to those values in `0.0.2-alpha`, and defines no coordination
object, core coordination field, or coordination feature flag.

Correlation metadata:

- is optional and opaque to ordinary OTMP readers;
- MUST NOT alter OTMP operation, conflict, snapshot, ref, or file semantics;
- MUST NOT make catalog access necessary to interpret the table;
- is preserved in immutable semantic history;
- is stable across retries and rebases; and
- participates in logical intent identity when supplied by the caller.

A successful OTMP participant commit is durable table state, but it is not
proof that an enclosing catalog transaction committed. A catalog transaction
identifier records correlation only. The catalog's own durable transaction
record and atomic catalog-root publication determine the multi-table outcome.

### 36.8 Catalog snapshot coordinates

A catalog snapshot that resolves an OTMP table SHOULD pin at least:

```text
table ID
table root
table version
semantic state SHA-256
selected snapshot ID, or null for an empty table
```

It MAY additionally retain the semantic commit object SHA-256 for audit
verification.

A catalog snapshot SHOULD NOT treat `root_revision`, generation ID, checkpoint
URI or hash, page-map root, or other physical metadata-generation details as
semantic table identity. OTMP may replace the physical generation at the same
table version and semantic state.

### 36.9 Catalog visibility boundary

Several successful OTMP `HEAD` replacements do not form one atomic cross-table
primitive. A catalog providing multi-table atomic visibility first publishes
and verifies its table-local participant commits, then atomically publishes one
catalog transaction or root that resolves all participant semantic coordinates.

Direct readers opening individual table roots may observe participant commits
before the catalog root is published when those commits move public refs. A
catalog requiring stronger isolation may prepare snapshots on catalog-owned
private refs and resolve them only through its catalog snapshot. Prepared-ref
lifecycle and catalog recovery remain outside the normative OTMP table schema.

An OTMP commit cannot be physically uncommitted after a successful `HEAD`
replacement. Catalog recovery therefore rolls forward, publishes the catalog
snapshot, compensates with later semantic commits, or abandons privately
prepared states. It does not destructively roll back immutable OTMP history.

---

## 37. Garbage collection and retention

### 37.1 Conservative alpha posture

Core OTMP defines reachability and orphan classification. It does not require aggressive deletion.

An implementation MUST prefer retained storage over deleting an object that may still be required by a pinned reader or retained snapshot.

### 37.2 Current reachability

The current `HEAD` reaches:

- current semantic commit chain as retained by policy;
- current metadata generation;
- its checkpoint;
- its page-map nodes and page packs;
- current scan projection;
- current relationally referenced snapshots and artifacts; and
- current live data and delete files.

### 37.3 Protocol orphan

An immutable object is an orphan when:

- it is not reachable from current or retained roots;
- it is not referenced by a retained snapshot, tag, commit, or recovery policy; and
- it is older than the minimum orphan grace period.

Failed candidate artifacts are common orphans.

### 37.4 Reader grace period

Because direct readers do not register leases, a deployment MUST retain replaced generations and their physical objects for at least a configured minimum reader grace period.

The table property:

```text
otmp.retention.min-reader-grace-ms
```

SHOULD define this period.

### 37.5 Semantic history retention

The table property:

```text
otmp.retention.semantic-history-ms
```

MAY define minimum semantic commit retention.

Removing semantic history can reduce audit and deterministic rebuild capability.

### 37.6 Snapshot retention

Refs, retention properties, and explicit snapshot-expiration extensions determine retained snapshots.

Core OTMP does not implicitly expire snapshots merely because they are no longer the `main` head.

### 37.7 Data-file deletion

A data or delete file is eligible for physical deletion only when:

- it is not live in any retained branch;
- it is not required by any retained tag or snapshot;
- it is not referenced by a retained scan projection;
- it is older than the configured grace period; and
- no external retention policy requires it.

### 37.8 Mark-and-sweep

A conforming GC MAY:

1. pin current `HEAD`;
2. mark retained semantic, physical, snapshot, ref, scan, and user-file objects;
3. list candidate objects;
4. exclude objects newer than the grace horizon;
5. re-read `HEAD` before deletion;
6. abort or restart if retention roots changed materially; and
7. delete only unmarked safe candidates.

### 37.9 Object-store versioning

Object-store versioning is RECOMMENDED for `HEAD` and MAY be used as an additional recovery mechanism. It does not replace protocol hashes or CAS.

---

## 38. Integrity and security

### 38.1 Hash verification

Readers and writers MUST verify immutable object hashes.

A page returned from a pack MUST be verified independently before use.

### 38.2 Immutable SQLite mode

A local SQLite reader MAY use immutable read-only mode only after:

- verifying the checkpoint or materialized file hash/identity;
- ensuring the file cannot be modified by another process; and
- pinning one generation.

### 38.3 URI safety

Implementations MUST reject relative paths escaping the table root.

Implementations SHOULD apply allowlists to URI schemes and storage authorities.

### 38.4 Untrusted SQL schema

A reader opening an untrusted checkpoint SHOULD:

- use read-only mode;
- disable extension loading;
- avoid executing triggers or views for writes;
- use trusted-schema restrictions when supported;
- reject custom collations/functions required by normative objects; and
- enforce memory, page-count, recursion, and query limits.

### 38.5 Authorization

OTMP does not define authorization.

Deployments MAY enforce permissions through:

- object-storage IAM;
- a catalog or broker;
- signed commits;
- credential vending; or
- network policy.

### 38.6 Signatures

`otmp.signatures.v1` MAY define signatures over canonical semantic commits, generations, and `HEAD` transitions.

Core alpha hashes provide integrity, not identity or non-repudiation.

### 38.7 Encryption

Storage-layer encryption is compatible with OTMP.

File-level encryption metadata MAY be recorded in file descriptors.

A future checkpoint-encryption codec MAY be added through feature negotiation. The core SQLite image remains plaintext at the logical codec boundary unless that feature is enabled.

### 38.8 Sensitive metadata

Statistics, paths, schemas, and properties may reveal sensitive information. Deployments SHOULD apply the same access controls to OTMP metadata as to table data.

---

## 39. Error model

A protocol error contains:

```json
{
  "code": "OTMP_REF_CONFLICT",
  "message": "main no longer points to the expected snapshot",
  "retryable": true,
  "details": {}
}
```

Core codes:

| Code | Retryable | Meaning |
|---|---:|---|
| `OTMP_NOT_FOUND` | maybe | Required table or object absent |
| `OTMP_ALREADY_EXISTS` | no | Table genesis or immutable object conflicts |
| `OTMP_HEAD_CONFLICT` | yes | Conditional root update failed |
| `OTMP_REQUIREMENT_FAILED` | depends | Semantic precondition failed |
| `OTMP_IDEMPOTENCY_CONFLICT` | no | Key reused with different intent |
| `OTMP_REF_CONFLICT` | depends | Ref state changed or invalid |
| `OTMP_FILE_CONFLICT` | depends | File identity/liveness invalid |
| `OTMP_SCHEMA_CONFLICT` | depends | Invalid schema evolution or ID reuse |
| `OTMP_PARTITION_CONFLICT` | depends | Invalid partition evolution |
| `OTMP_SORT_CONFLICT` | depends | Invalid sort evolution |
| `OTMP_FEATURE_CONFLICT` | no | Required feature unsupported |
| `OTMP_HASH_MISMATCH` | no | Immutable object hash mismatch |
| `OTMP_CORRUPT_STATE` | no | Relational or graph invariant invalid |
| `OTMP_UNSUPPORTED_CODEC` | no | Metadata or data codec unsupported |
| `OTMP_STORAGE_FENCE_UNAVAILABLE` | no | Direct writes lack linearizable CAS |
| `OTMP_OBJECT_CONFLICT` | no | Create-only URI contains different bytes |
| `OTMP_REBASE_REQUIRED` | yes | Candidate must be rebuilt on newer state |
| `OTMP_REBASE_UNSAFE` | no | Operation cannot be semantically rebased |
| `OTMP_VALIDATION_FAILED` | no | Candidate image/projection failed validation |

Implementations MAY add namespaced error codes.

---

## 40. Protocol evolution

### 40.1 Version fields

The protocol uses:

- `protocol_version` for the overall specification;
- `format_version` within individual object codecs;
- feature names for optional capabilities; and
- SQLite `user_version` for the checkpoint schema.

### 40.2 Alpha compatibility

Versions before 0.1.0 may make incompatible changes.

A 0.0.1-alpha catalog-package table is not automatically a 0.0.2-alpha self-contained table.

### 40.3 Reader behavior

A reader MUST reject:

- a higher incompatible core version;
- an unknown required reader feature;
- an unknown required metadata codec; or
- a semantic operation that affects the requested state but cannot be understood.

### 40.4 Writer behavior

A writer MUST preserve every required feature and MUST NOT commit a state it cannot read and validate.

### 40.5 Schema migration

A future checkpoint-schema version MAY require a new metadata generation at the same semantic table version.

When migration changes only physical representation, it increments `root_revision`, not `table_version`.

When migration changes semantic state, it requires a semantic commit.

---

## 41. Performance guidance (non-normative)

### 41.1 No guaranteed universal winner

OTMP is designed to be competitive across serverless table workloads, but no representation is optimal for every operation.

The relational image is strongest for:

- table and snapshot lookup;
- targeted partition/file access;
- metadata joins;
- governance and maintenance queries;
- browser-local metadata exploration; and
- fine-grained indexed access.

The scan projection is strongest for:

- planning broad scans over millions of files;
- parallel metadata processing; and
- reading selected metrics columns sequentially.

### 41.2 Cold-read requirements

A remote VFS should avoid one network request per SQLite page.

Recommended techniques include:

- 256 KiB to 1 MiB range-fetch units;
- range coalescing;
- page-pack locality;
- root/schema page preloading;
- B-tree sibling prefetch;
- content-addressed caches;
- bounded page-map height; and
- proactive checkpointing.

### 41.3 Commit-object count

A production writer should minimize per-commit objects.

Implementations MAY use a range-readable commit bundle containing:

- semantic commit bytes;
- page-pack bytes;
- page-map nodes; and
- projection deltas,

provided every logical object remains independently hash-addressable through an index.

### 41.4 Group commit

Bursty small appends benefit from combining compatible intents into one snapshot, one physical update, and one `HEAD` CAS.

### 41.5 Metadata indexes

Writers SHOULD maintain indexes for common access paths such as:

- ref name;
- snapshot sequence;
- file ID;
- file URI/object identity;
- partition hash;
- file sequence; and
- field metrics.

### 41.6 Checkpoint cadence

Checkpoint too frequently and write amplification rises. Checkpoint too infrequently and cold-read requests and page-map depth rise.

Implementations should tune using measured:

- metadata bytes per commit;
- override-page ratio;
- page-map height;
- range-request count;
- cold-open latency; and
- cache hit rate.

### 41.7 Runtime choice

Upstream SQLite is the interchange baseline.

Turso is a strong candidate for:

- async Rust execution;
- MVCC inside an ephemeral coordinator;
- concurrent private transactions;
- browser/WASM runtimes; and
- custom object I/O.

Turso-specific WAL, MVCC logs, or cloud manifests are private implementation details and must not leak into the canonical generation.

---

## 42. End-to-end examples

### 42.1 Initialize an empty table

1. Choose a new table ID.
2. Set the genesis `root_revision` to `0` and define schema ID `1`.
3. Define partition spec `0` and sort order `0`.
4. Create genesis semantic commit version `0` with `initialize_table`.
5. Create the relational database with `main` branch and null snapshot.
6. Export and validate a complete SQLite checkpoint.
7. Create generation `0` pointing to that checkpoint with no page map.
8. Upload commit, checkpoint, and generation with create-only semantics.
9. Create `_otmp/HEAD` only if absent.

Result:

```text
HEAD root_revision 0 -> table version 0 -> complete empty relational table metadata
```

### 42.2 Append two files

Parent:

```text
HEAD table_version = 7
main -> snapshot S7
```

Writer:

1. writes `data/part-a.parquet` and `data/part-b.parquet`;
2. reads and pins version 7;
3. opens private metadata generation 7;
4. checks idempotency and `main` state;
5. creates snapshot S8 with sequence 8;
6. inserts both file descriptors and metrics;
7. adds both to `main` live membership;
8. advances `main` to S8;
9. creates semantic commit version 8;
10. captures changed SQLite pages;
11. writes page pack and page-map nodes;
12. writes generation 8;
13. optionally writes scan projection S8; and
14. CAS-updates `HEAD` from version 7 to 8.

### 42.3 Two concurrent independent appends

Writers A and B both start from version 8.

A commits first, producing version 9.

B’s `HEAD` CAS fails.

B:

1. opens version 9;
2. confirms B’s files remain absent;
3. confirms schema/spec compatibility;
4. uses append-safe rebase;
5. creates a new snapshot parented by A’s snapshot;
6. produces candidate version 10; and
7. publishes version 10.

No raw pages from B’s failed version-9 candidate are merged.

### 42.4 Rewrite conflict

Writer A rewrites files X and Y.

Writer B also rewrites Y and Z.

A publishes first.

B retries and finds Y is no longer live. Its `file_live` requirement fails. B returns `OTMP_REBASE_UNSAFE` or regenerates its rewrite plan from current files.

### 42.5 Group commit

A temporary coordinator receives three append intents.

It validates all three, combines their file additions into one snapshot, stores three idempotency results, creates one semantic commit and one metadata generation, then performs one `HEAD` CAS.

Every intent commits atomically.

### 42.6 Physical checkpoint replacement

Current semantic version is 500, based on checkpoint 420 plus page overrides.

A checkpointer materializes version 500, validates it, uploads `checkpoint-500.sqlite3`, and creates a new generation with no page map.

It CAS-updates:

```text
table_version: 500 -> 500
root_revision: 731 -> 732
```

Readers observe identical table state with a cheaper physical representation.

### 42.7 Catalog-managed table

A catalog resolves:

```text
prod.analytics.sales -> s3://warehouse/tables/<table-id>/
```

The catalog authorizes a writer and performs the same OTMP direct-write algorithm on its behalf.

A catalog-free reader with the table URI opens the same table artifacts without the catalog.

---

## 43. Design decisions and superseded alternatives

This section records the major iterations that led to 0.0.2-alpha.

### 43.1 Catalog database to self-contained table

**Superseded design:** one versioned relational database per warehouse or catalog.

**Problem:** that design recreated a metastore, introduced a global root and contention domain, reduced table portability, required catalog sharding, and made cross-shard transactions part of the format.

**Decision:** the protocol unit is one self-contained table. A catalog manages tables but is not the table-format authority.

### 43.2 Semantic log replay to ready-to-query generation

**Superseded design:** current state equals SQLite checkpoint plus semantic transactions after the checkpoint.

**Problem:** ordinary readers paid Delta-like replay overhead and could need to rebuild indexes or overlays, weakening the main benefit of relational metadata.

**Decision:** every committed `HEAD` points to a complete logical relational database generation. Semantic commits remain for meaning, audit, conflicts, and recovery, not normal reads.

### 43.3 Full SQLite file per commit to copy-on-write pages

**Superseded design:** upload a complete new SQLite file for every metadata transaction.

**Problem:** one small row change could require rewriting gigabytes of metadata.

**Decision:** expose a complete logical SQLite image while physically reusing a base checkpoint and storing only changed pages through immutable page packs and a persistent page map.

### 43.4 Shared mutable SQLite on object storage rejected

**Rejected design:** place one mutable SQLite file on S3, FUSE, NFS, or another shared filesystem.

**Problem:** SQLite local locking and sidecar assumptions do not provide safe, efficient distributed multiwriter object-store semantics.

**Decision:** every writer uses a private database view. Distribution and publication occur above SQLite through immutable objects and `HEAD` CAS.

### 43.5 Relational logical model plus file physical model

**False choice rejected:** relational database or metadata files.

**Decision:** relational entities, constraints, and SQL are the logical model; immutable files and segments are the physical object-storage representation.

### 43.6 SQLite as codec, Turso as implementation

**Rejected design:** require one SQLite fork or managed database service.

**Decision:** standard SQLite Format 3 is the portable metadata-image codec. Turso may be the preferred Rust/serverless runtime but is not normative.

### 43.7 Semantic commit plus physical page state

**Rejected design:** use raw SQLite pages as the open table protocol.

**Problem:** page changes do not explain file appends, schema evolution, delete applicability, conflict safety, or authorization intent.

**Decision:** semantic operations define meaning; page state makes the database view efficient.

### 43.8 Relational image plus columnar scan projection

**Rejected design:** force every large distributed scan to enumerate a remote SQL B-tree.

**Decision:** use the relational database for targeted metadata and an optional immutable Parquet scan projection for giant scan planning.

### 43.9 Catalog coordination as optional acceleration

**Rejected design:** make a Unity Catalog-like service the irreplaceable source of truth.

**Decision:** a catalog or coordinator may provide governance, caching, and group commit, but it publishes the same open table artifacts and can be removed without making the table uninterpretable.

### 43.10 Generic CRDT merging rejected

**Problem:** converged rows can still violate table invariants such as one branch head, stable IDs, unique file identities, or atomic snapshot publication.

**Decision:** conflicts are resolved using typed semantic requirements and operation-specific rebasing.

### 43.11 Distributed SQLite clusters are deployment options, not protocol

Raft SQLite, FoundationDB-backed SQLite, hosted SQLite, and other distributed engines may implement a hot control plane. Requiring them would violate zero-resident-compute and service independence.

### 43.12 Per-table serial publication is intentional

OTMP does not promise simultaneous final publication of contradictory changes to one linear table state.

Many writers perform data production and candidate construction concurrently; the final table root is ordered, as it is in other transactional table formats and databases.

### 43.13 Incorporate relational metadata lessons into one core model

**Rejected design:** maintain a separate compatibility schema or branded
metadata profile alongside the OTMP relational model.

**Decision:** OTMP has one core relational metadata model. It incorporates
applicable lessons from relational lakehouse prior art—such as stable identity,
versioned schema and file state, explicit statistics, delete metadata, and
ready-to-query current state—while retaining OTMP's own names, invariants,
single-table atomicity domain, and storage protocol. Compatibility with another
format is not implied.

---

## 44. Alpha limitations and open questions

The following remain experimental or incomplete:

1. **Page-map encoding validation.** The persistent B-tree profile needs interoperability fixtures and fuzz testing.
2. **Optimal page and pack sizes.** These require browser, serverless, and object-store benchmarks.
3. **Delete-file physical schemas.** Descriptor semantics are defined, but exact Parquet delete schemas need a dedicated extension.
4. **Snapshot expiration.** Core retention is conservative; destructive expiration requires more proof rules.
5. **Signature profile.** Hashes do not authenticate writers.
6. **Commit bundles.** A packed object format may reduce object count while preserving logical references.
7. **Branch scaling.** Materializing live files for many branches may require a copy-on-write branch index extension.
8. **Projection equivalence proof.** Reference validators and canonical summary hashes are needed.
9. **Cross-table transactions.** These remain a catalog-level concern.
10. **Object-store latency.** Pure direct commits inherit durable object PUT and CAS latency.
11. **High-contention fairness.** A coordinator is recommended but not standardized as a network API.
12. **Metadata database scale.** Very large tables may require attached/partitioned metadata images or additional index projections.
13. **Alternative metadata codecs.** The alpha standardizes SQLite-compatible pages; future codecs require feature negotiation.
14. **Exact deterministic SQLite bytes.** Semantic equivalence is required; byte-identical output from different writers is not.
15. **Protocol name.** OTMP remains provisional.

---

# Appendix A — Normative SQLite schema summary

The companion SQL file is normative for `PRAGMA user_version = 2`.

| Table | Purpose |
|---|---|
| `otmp_meta` | Singleton table identity and current defaults |
| `otmp_commits` | One row per semantic table version |
| `otmp_idempotency` | One row per committed caller intent |
| `otmp_properties` | Current table properties |
| `otmp_features` | Enabled reader/writer features |
| `otmp_schemas` | Immutable schemas |
| `otmp_field_ids` | Never-reused field-ID registry |
| `otmp_fields` | Schema field definitions |
| `otmp_identifier_fields` | Logical identifier fields |
| `otmp_partition_specs` | Immutable partition specs |
| `otmp_partition_field_ids` | Never-reused partition-field registry |
| `otmp_partition_fields` | Partition transforms |
| `otmp_sort_orders` | Immutable sort orders |
| `otmp_sort_fields` | Sort definitions |
| `otmp_snapshots` | Immutable snapshots |
| `otmp_snapshot_summary` | Snapshot summary entries |
| `otmp_refs` | Branches and tags |
| `otmp_files` | Immutable data and delete descriptors |
| `otmp_delete_file_details` | Delete applicability |
| `otmp_file_metrics` | Per-field file statistics |
| `otmp_snapshot_file_changes` | Snapshot file additions/removals |
| `otmp_ref_live_files` | Materialized live files for branches |
| `otmp_artifacts` | Scan indexes and auxiliary objects |
| `otmp_live_files` | Read convenience view |

Normative schema file:

```text
OTMP-0.0.2-alpha-table-schema.sql
```

---

# Appendix B — Deterministic CBOR scalar profile

The logical CBOR value is a two-element array:

```text
[type_code, payload]
```

Core type codes:

| Code | Type | Payload |
|---:|---|---|
| 0 | null | CBOR null |
| 1 | boolean | CBOR boolean |
| 2 | int32 | integer |
| 3 | int64 | integer |
| 4 | float32 | 4 raw IEEE-754 bytes |
| 5 | float64 | 8 raw IEEE-754 bytes |
| 6 | decimal | `[precision, scale, two_complement_unscaled_bytes]` |
| 7 | date | signed days from 1970-01-01 |
| 8 | time_micros | microseconds from midnight |
| 9 | timestamp_micros | signed microseconds from Unix epoch, no zone |
| 10 | timestamptz_micros | signed UTC microseconds from Unix epoch |
| 11 | string | UTF-8 text |
| 12 | binary | byte string |
| 13 | fixed | byte string |
| 14 | uuid | 16 raw bytes |

Negative zero for floats MUST be preserved. All NaNs MUST be canonicalized to one quiet NaN bit pattern per width when encoded as metadata values.

---

# Appendix C — Canonical bucket-transform source bytes

Bucket source bytes are:

- boolean: one byte `00` or `01`;
- int32: 4-byte little-endian two’s complement;
- int64/date/time/timestamp: 8-byte little-endian two’s complement;
- float32: canonical IEEE-754 little-endian bits;
- float64: canonical IEEE-754 little-endian bits;
- decimal: minimal two’s-complement big-endian unscaled integer bytes;
- string: UTF-8 bytes;
- binary/fixed: raw bytes;
- UUID: 16 raw bytes.

Null values do not produce a bucket value.

---

# Appendix D — Reference algorithms

## D.1 Page lookup

```text
function read_page(generation, page_number):
    require 1 <= page_number <= generation.page_count

    ref = page_map_lookup(generation.page_map, page_number)

    if ref exists:
        bytes = range_read(ref.pack.uri, ref.offset, ref.stored_length)
        bytes = decode(ref.codec, bytes)
        require len(bytes) == generation.page_size
        require sha256(bytes) == ref.page_sha256
        return bytes

    offset = (page_number - 1) * generation.page_size
    bytes = range_read(generation.checkpoint.uri, offset, generation.page_size)
    require len(bytes) == generation.page_size
    return bytes
```

## D.2 Direct commit

```text
function commit(table_root, intent):
    loop:
        head_bytes, token = storage.read_with_token(table_root / "_otmp/HEAD")
        head = validate_head(head_bytes)

        db = open_private_writable_generation(head.metadata_generation)

        existing = db.lookup_idempotency(intent.key)
        if existing exists:
            require existing.intent_hash == hash_intent(intent)
            return existing.result

        semantic_commit = build_candidate_commit(db, head, intent)
        apply_and_validate(db, semantic_commit)

        artifacts = build_generation_artifacts(db, semantic_commit)
        upload_create_only(artifacts)

        next_head = make_head(head, semantic_commit, artifacts.generation)

        if storage.compare_and_swap("_otmp/HEAD", token, canonical(next_head)):
            return semantic_commit.intent_result(intent.key)

        if not can_rebase(intent):
            raise OTMP_REBASE_UNSAFE
```

## D.3 Checkpoint

```text
function checkpoint(table_root):
    head_bytes, token = storage.read_with_token("_otmp/HEAD")
    head = validate_head(head_bytes)

    image = materialize(head.metadata_generation)
    validate_sqlite(image)

    checkpoint_ref = upload_immutable(image)
    generation = generation_from_checkpoint(head, checkpoint_ref)
    upload_immutable(generation)

    new_head = head
    new_head.root_revision += 1
    new_head.metadata_generation = ref(generation)

    return storage.compare_and_swap("_otmp/HEAD", token, canonical(new_head))
```

---

# Appendix E — Minimum conformance tests

A conforming implementation SHOULD pass fixtures covering:

1. Canonical JSON stability.
2. Genesis table creation with create-if-absent `HEAD`.
3. Standard SQLite opening of a published checkpoint.
4. Integrity and foreign-key validation.
5. Materialization from checkpoint plus page-map overrides.
6. Remote VFS lookup of checkpoint and packed pages.
7. Hash mismatch rejection.
8. Current `main` snapshot planning.
9. Schema rename preserving field ID.
10. Hidden partition-spec evolution.
11. Append snapshot and branch live-file materialization.
12. Idempotent retry after lost response.
13. Idempotency key reused with a different intent.
14. Two concurrent appends with one CAS failure and safe rebase.
15. Rewrite conflict on a removed input file.
16. Unknown required feature rejection.
17. Scan projection equivalence with relational live files.
18. Physical checkpoint replacement without semantic version change.
19. Reader pinned to an old generation during a new commit.
20. Writer crash before `HEAD` publication.
21. Writer crash after `HEAD` publication.
22. Orphan detection after failed candidate publication.
23. Catalog-free read using only a table URI.
24. Catalog-coordinated write producing identical open artifacts.
25. Upstream SQLite and Turso round-trip compatibility for the normative schema.
26. Commit and snapshot metadata remain distinct in semantic and relational state.
27. Changing either metadata object changes logical intent identity.
28. Semantic snapshot metadata divergence from the relational snapshot row is rejected.
29. A malformed or flat `commit_snapshot` semantic operation is rejected before relational projection.
30. Every `commit_snapshot` operation maps to exactly one relational snapshot row created at the same table version.
31. Genesis rejects an initial snapshot and the first data snapshot is committed at a positive table version.
32. A Gate 1 reader rejects unknown, extension, and otherwise unsupported semantic operations rather than silently projecting them.
33. Mutating any supported snapshot, ref, file-change, immutable file-descriptor, partition, or metric projection causes pin validation to fail.
34. A Gate 1 image rejects genesis/future snapshots, extra or malformed refs, truncated ancestry, and stale or advanced sequence allocator state.

---

# Appendix F — Protocol story in one page

An OTMP table is a relational metadata database that belongs to one table and lives as immutable files when idle.

A catalog may tell a client where the table lives, but once the location is known the client reads `_otmp/HEAD`, pins a metadata generation, opens that generation as a complete SQLite-compatible database, asks SQL which files belong to the requested snapshot, and reads those files.

A writer creates new immutable data files, opens the current metadata generation privately, applies a typed semantic transaction, captures the resulting changed database pages, writes immutable page packs and a new generation, and conditionally advances `HEAD`.

Many readers use immutable generations without locks. Many writers prepare independently. One final ordered root publication defines each table version. A temporary broker may group commits, but it is not required and holds no unique table state.

The semantic commit tells the world what the operation meant. The relational generation gives readers an already-materialized indexed state. The scan projection gives distributed engines a columnar planning path. The standard SQLite checkpoint makes the table inspectable, portable, and recoverable.

When nobody is using the table, no table-specific compute remains running.


## Pre-release local/full-image transaction profile

The `0.0.2-alpha` draft evolves in place before the first official release. No
backward compatibility, fixture migration, or feature-upgrade operation is
promised. [Transactions, history, and refs](../docs/TRANSACTIONS.md) defines the
implemented requirement/operation matrix, validation layers, retry behavior,
selectors, verification scopes, additive-schema rules, and current exclusions.

The profile advances table version once per semantic transaction, root revision
once per successful HEAD replacement, and sequence only when creating a
snapshot. Metadata and ref transactions MUST NOT create snapshots. Requirements
MUST be evaluated against the pinned base before ordered operations execute in
one private SQLite transaction. Durable operations and requirements remain open
canonical protocol values. Known but unimplemented behavior MUST fail closed.

Two additional core requirements allocate immutable schema/field identities:

```json
{"type":"schema_id_absent","schema_id":"2"}
{"type":"field_ids_absent","field_ids":["2","3"]}
```

`schema_id_absent` asserts no schema with that ID exists in the base.
`field_ids_absent` asserts that every listed positive, distinct field ID is
absent from the table-global field registry, including prior schemas.

In this profile, `create_ref` carries `operation_id`, `ref`, `ref_type`, and an
explicit nullable `snapshot_id`; retention policy mutation is unimplemented.
`replace_ref` carries `operation_id`, `ref`, and its new `snapshot_id`; the
expected old target MUST appear in an exact `ref_snapshot_is` requirement.
`add_schema` carries `operation_id` and a full `schema` object. All operation
shapes include `type`. Counter IDs outside schema objects use decimal strings.
New tables advertise `otmp.refs.v1` at genesis; `upgrade_features` is unsupported.

Top-level null properties are rejected by this runtime; nested nulls are valid.
`property_is.value: null` means absence and a missing `value` is malformed.
Every touched property requires exactly one precondition, and keys may not be
touched by multiple operations. Undefined `otmp.*` keys are rejected.

Normal pinning validates the selected HEAD, generation, commit, image, features,
and commit projection. Explicit historical verification validates retained
transitions by replay into private images and comparison of logical tables.
Metadata is selected before ref/snapshot/sequence resolution. Historical images
have no fabricated historical root revision. Missing explicitly referenced
objects and transport failures are not retention boundaries.

For the general protocol `physical_parent` remains optional and informational.
The full-image writer guarantees retention of each publication attempt's parent
and every published full-image generation; garbage collection is out of scope.
