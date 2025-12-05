# vsdb_core

[![Crates.io](https://img.shields.io/crates/v/vsdb_core.svg)](https://crates.io/crates/vsdb_core)
[![Docs.rs](https://docs.rs/vsdb_core/badge.svg)](https://docs.rs/vsdb_core)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](../../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> `vsdb_core` provides the low-level building blocks for `vsdb`.

This crate contains the foundational components of `vsdb`, including:
- **Storage Abstractions**: An `Engine` trait that abstracts over key-value storage backends.
- **Raw Data Structures**: Untyped, high-performance data structures like `MapxRaw` that operate on raw bytes.
- **Utilities**: Shared functions for environment management, such as setting the database directory.

Most users should use the `vsdb` crate instead, which provides high-level, typed APIs.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vsdb_core = "7.0.0"
```

## Features

For detailed API examples, see [API Examples](docs/api.md).

- `fjall_backend`: **(Default)** Use `fjall` as the backend database. Pure Rust implementation.
- `rocks_backend`: Use `rocksdb` as the backend database. C++ implementation.
- `compress`: Enable data compression in the backend database.


## License

This project is licensed under the **GPL-3.0** license.
