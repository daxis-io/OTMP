# Gate 1 conformance fixtures

The canonical JSON, deterministic CBOR, and hash fixtures are byte-for-byte
normative for this Gate 1 implementation. `tables/genesis` and `tables/append`
are static, self-contained OTMP packages. Future implementations must be able to
read them; independently regenerated SQLite checkpoints are not required to be
byte-identical.

`sources/` records the schema, opaque Parquet-like byte fixture, and append
manifest used to generate the static packages. The file is intentionally only a
byte-identity fixture; Gate 1 does not claim that it is semantically valid
Parquet.
