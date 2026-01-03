# vsdb_core

[![Crates.io](https://img.shields.io/crates/v/vsdb_core.svg)](https://crates.io/crates/vsdb_core)
[![Docs.rs](https://docs.rs/vsdb_core/badge.svg)](https://docs.rs/vsdb_core)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](../../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> `vsdb_core` provides the low-level building blocks for `vsdb`.

This crate contains the foundational components of `vsdb`, including:
- **Raw Data Structures**: Untyped, high-performance data structures like `MapxRaw` that operate on raw bytes.
- **Utilities**: Shared functions for environment management, such as setting the database directory.

Most users should use the `vsdb` crate instead, which provides high-level, typed APIs.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vsdb_core = "8.0.0"
```

## Features

For detailed API examples, see [API Examples](docs/api.md).

Uses RocksDB as the storage backend.


## License

This project is licensed under the **GPL-3.0** license.
