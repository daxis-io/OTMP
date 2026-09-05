# OTMP 0.0.2-alpha

This repository is an experimental Rust catalog-optional local/full-image implementation of the Open Table Metadata Protocol (OTMP).


## Workspace

The workspace contains three crates:

- `otmp-protocol`: portable protocol values, canonical codecs, validation, and
  domain-separated hashes;
- `otmp`: the catalog-free table runtime, SQLite image implementation, storage
  seam, staging, and publication state machine;
- `otmp-cli`: a local-directory command adapter.

`conformance/` contains language-neutral codec/hash fixtures and self-contained
table packages. `tests/` contains the subprocess crash-evidence harness and is
not a Rust crate.

## CLI

```bash
cargo run -p otmp-cli -- init TABLE --schema schema.json
cargo run -p otmp-cli -- inspect-file data.parquet
cargo run -p otmp-cli -- append TABLE --manifest append.json
cargo run -p otmp-cli -- status TABLE
cargo run -p otmp-cli -- files TABLE --reference main
cargo run -p otmp-cli -- history TABLE
cargo run -p otmp-cli -- verify TABLE
```

Append manifests must supply the expected SHA-256 and length. A local source
path is an execution input and is excluded from logical intent identity. Command
success is JSON on stdout; failures are structured JSON on stderr.

## Project documents

- [Specification](spec/OTMP-0.0.2-alpha.md)
- [Qualification and reproduction](docs/QUALIFICATION.md)
- [Catalog integration](docs/CATALOG-INTEGRATION.md)
- [Conformance fixtures](conformance/README.md)
- [Contributing](CONTRIBUTING.md)

This is an evolving pre-release v1 development line. The `0.0.2-alpha`
identifiers remain in use; no backward compatibility or fixture migration is
promised before the first official release.
