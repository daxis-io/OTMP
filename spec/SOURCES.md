# Authoritative sources

The tracked OTMP 0.0.2-alpha working draft and normative SQLite schema were copied
from the following local source artifacts before the Gate 1 clarification edits:

| Tracked artifact | Source path | Source SHA-256 |
|---|---|---|
| `OTMP-0.0.2-alpha.md` | `/Users/ethanurbanski/Downloads/OTMP-0.0.2-alpha.md` | `4037a472c8871725f25be2bee63b195652c41e6047dc7b23f83cfe26e31aabb1` |
| `OTMP-0.0.2-alpha-table-schema.sql` | `/Users/ethanurbanski/Downloads/otmp-table-schema.sql` | `290adc6a7c5425e6f63289dc8f9c53633ada94ddb353c49cd61b32a8df460bae` |

The Markdown draft is intentionally amended in this repository without changing
the protocol version. The schema is embedded and used verbatim by the Rust runtime.

## Relational metadata prior art

OTMP's single core relational model incorporates applicable lessons from open
relational lakehouse metadata systems without adopting a compatibility schema or
delegating OTMP transaction and storage semantics to them. The DuckLake 1.0
metadata specification was consulted for snapshot-versioned schemas, files,
delete files, statistics, partitioning, sorting, and catalog transaction prior
art:

- <https://ducklake.select/docs/stable/specification/introduction>
- <https://ducklake.select/docs/stable/specification/tables/overview>

These references are informative. The tracked OTMP specification and companion
SQL schema remain authoritative for OTMP.
