# OTMP 0.0.2-alpha Gate 1

This repository is a Rust proof of concept for a deliberately narrow,
catalog-free, full-image subset of the Open Table Metadata Protocol.

The qualification target is documented in [docs/GATE-1.md](docs/GATE-1.md).

## Workspace

The workspace contains exactly three production crates:

- `otmp-protocol`: portable protocol values, canonical codecs, validation, and
  domain-separated hashes;
- `otmp`: the catalog-free table runtime, SQLite image implementation, storage
  seam, staging, and publication state machine;
- `otmp-cli`: a thin local-directory command adapter.

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
