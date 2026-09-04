# Commit and Snapshot Metadata Design

## Decision

OTMP has one core relational metadata model. Lessons from prior relational
lakehouse systems improve that model, while OTMP continues to own the durable
single-table protocol: semantic commits, immutable metadata images, verified
artifacts, pinned reads, semantic rebasing, and atomic `HEAD` publication.

Catalog correlation remains application-defined in `0.0.2-alpha`. Catalogs may
place stable, reverse-domain-namespaced correlation values in semantic commit
metadata. The values are opaque to OTMP, do not imply that an enclosing catalog
transaction committed, and do not change the one-table atomicity domain.

## Semantic ownership

- Commit metadata describes a semantic transaction and its external context.
- Snapshot metadata describes the immutable data state created by one
  `commit_snapshot` operation.
- Implementations must not copy, merge, or inherit one metadata value into the
  other.
- Both values are stable logical inputs and participate independently in intent
  identity.
- Attempt-local telemetry and runtime-assigned values do not belong in either
  value.

For the Gate 1 append convenience API, one request creates one snapshot, so it
accepts one commit metadata object and one snapshot metadata object. A future
general transaction API keeps commit metadata at transaction scope and carries
snapshot metadata on each snapshot operation.

## Catalog boundary

A successful OTMP participant commit is durable table state, but is not proof
that its enclosing catalog transaction committed. An external transaction ID is
correlation evidence only. The catalog's durable transaction record and atomic
catalog-root publication determine the multi-table outcome.

A catalog snapshot pins semantic coordinates: table ID, table root, table
version, semantic state hash, and selected snapshot ID. It does not use root
revision, generation ID, checkpoint identity, or page-map identity as semantic
table identity.

