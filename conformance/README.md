# Conformance fixtures

The canonical JSON, deterministic CBOR, and hash fixtures are byte-for-byte
normative for the current pre-release implementation. `tables/genesis` and `tables/append`
are static, self-contained OTMP packages. The draft may regenerate fixtures before the official release without a
backward-compatibility or migration promise.

`sources/` records the schema, opaque Parquet-like byte fixture, and append
manifest used to generate the static packages. The file is intentionally only a
byte-identity fixture; the runtime does not claim that it is semantically valid
Parquet.

Run `python3 conformance/regenerate.py --check`, the protocol fixture and static
table tests, and `git diff --exit-code`. No external setup is required.
