# Contributing to OTMP

OTMP is experimental and has no supported production release. Changes evolve
the pre-release draft in place, including checked-in fixtures; there is no
backward-compatibility or migration commitment before the official release.

Install Rust with rustup; `rust-toolchain.toml` selects the exact compiler,
components, and Wasm target. Install the verification tools:

```sh
cargo install cargo-nextest --version 0.9.114 --locked
cargo install cargo-deny --version 0.20.2 --locked
cargo install cargo-audit --version 0.22.1 --locked
```

Run every command in [qualification](docs/QUALIFICATION.md) from the repository
root. A native C/C++ build toolchain and CMake are needed for SQLite/TLS. Python 3 and bash are needed for conformance and subprocess checks.
No credentials, cloud accounts, source paths outside this checkout, or local
configuration are required. Data fixtures test byte identity, not Parquet semantics.

Open a bug, protocol-change, or implementation issue with a reproducible case
and acceptance criteria. Keep changes in reviewable capability slices, update
contracts alongside behavior, and include relevant tests and evidence in the PR.
All contributions are licensed under Apache-2.0. Use squash or rebase merging.
