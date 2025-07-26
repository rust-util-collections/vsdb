# vsdb_core

[![Crates.io](https://img.shields.io/crates/v/vsdb_core.svg)](https://crates.io/crates/vsdb_core)
[![Docs.rs](https://docs.rs/vsdb_core/badge.svg)](https://docs.rs/vsdb_core)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](../../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> A std-collection-like database.

This crate provides the low-level implementations for `vsdb`.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vsdb_core = "4.0.2"
```

## Features

- `parity_backend`: Use `parity-db` as the backend database (default).
- `rocks_backend`: Use `rocksdb` as the backend database.
- `compress`: Enable compression in the backend database.

## Known Issues

- The instance `len` is not absolutely reliable and should be regarded as a hint.

## License

This project is licensed under the **GPL-3.0** license.