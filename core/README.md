# vsdb_core

[![Crates.io](https://img.shields.io/crates/v/vsdb_core.svg)](https://crates.io/crates/vsdb_core)
[![Docs.rs](https://docs.rs/vsdb_core/badge.svg)](https://docs.rs/vsdb_core)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> `vsdb_core` provides the low-level building blocks for `vsdb`.

This crate contains the foundational components of `vsdb`, including:
- **Raw Data Structures**: Untyped, high-performance data structures like `MapxRaw` that operate on raw bytes.
- **Utilities**: Shared functions for environment management, such as setting the database directory.

Most users should use the `vsdb` crate instead, which provides high-level, typed APIs.

## Installation

```bash
cargo add vsdb_core
```

For detailed API examples, see [API Examples](docs/api.md).

## Read-only access

Call `vsdb_configure(VsdbOptions::read_only(path))` before any other VSDB API,
then restore existing `MapxRaw` handles with `from_meta` or serde. The setting
is one-shot and process-wide, including every namespace. Reads and in-memory
WAL recovery leave the database tree unchanged; creation and mutation are
unavailable. See the [read-only mode guide](docs/read-only.md) for complete
semantics and deployment guidance.

## Storage Engine

The storage backend is MMDB, a pure-Rust LSM-Tree engine.

## License

This project is licensed under the **MIT** license.
