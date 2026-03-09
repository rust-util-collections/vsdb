![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/vsdb)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.85+-lightgray.svg)](https://github.com/rust-util-collections/vsdb)

# vsdb

`vsdb` is a high-performance, embedded database designed to feel like using Rust's standard collections. It provides persistent, disk-backed data structures (`Mapx`, `MapxOrd`) with a familiar, in-memory feel, plus Git-model versioned storage (`VerMap`) with branching, commits, merge, and history.

For a detailed guide and API examples, see the [**vsdb crate documentation**](strata/README.md).
For the versioned storage architecture (VerMap internals, merge algorithm, COW B+ tree, etc.), see the [**Versioned Module — Architecture & Internals**](strata/docs/versioned.md).

## Crate Ecosystem

The `vsdb` project is a workspace containing two crates:

| Crate | Version | Documentation | Path | Description |
| :--- | :--- | :--- | :--- | :--- |
| [**vsdb**](strata) | [![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb) | [![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb) | `strata` | High-level, typed data structures (`Mapx`, `MapxOrd`, `VerMap`, `SlotDB`, `MptCalc`, `SmtCalc`). This is the primary crate for most users. |
| [**vsdb_core**](core) | [![Crates.io](https://img.shields.io/crates/v/vsdb_core.svg)](https://crates.io/crates/vsdb_core) | [![Docs.rs](https://docs.rs/vsdb_core/badge.svg)](https://docs.rs/vsdb_core) | `core` | Low-level implementations, including the RocksDB storage layer and raw data structures. |

## Important Changes

- **Performance-focused API**: The `insert()` and `remove()` methods no longer return the old value, eliminating expensive read-before-write operations and significantly improving write performance.
