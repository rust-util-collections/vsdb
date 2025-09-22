![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/vsdb)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.89+-lightgray.svg)](https://github.com/rust-random/rand#rust-version-requirements)

# vsdb

`vsdb` is a high-performance, embedded database designed to feel like using Rust's standard collections (`Vec`, `HashMap`, etc.). It provides persistent, disk-backed data structures with a familiar, in-memory feel.

This repository is a simplified version of the original [**vsdb**](https://crates.io/crates/vsdb/0.70.0), retaining the most practical and stable features while focusing on performance and ease of use.

For a detailed guide and API examples, see the [**vsdb crate documentation**](wrappers/README.md).

### Crate Ecosystem

The `vsdb` project is a workspace containing several related crates:

| Crate | Version | Documentation | Path | Description |
| :--- | :--- | :--- | :--- | :--- |
| [**vsdb**](wrappers) | [![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb) | [![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb) | `wrappers` | High-level, typed data structures (e.g., `Mapx`, `Vecx`). This is the primary crate for most users. |
| [**vsdb_core**](core) | [![Crates.io](https://img.shields.io/crates/v/vsdb_core.svg)](https://crates.io/crates/vsdb_core) | [![Docs.rs](https://docs.rs/vsdb_core/badge.svg)](https://docs.rs/vsdb_core) | `core` | Low-level implementations, including storage backends and raw data structures. |
| [**vsdb_slot_db**](utils/slot_db) | [![Crates.io](https://img.shields.io/crates/v/vsdb_slot_db.svg)](https://crates.io/crates/vsdb_slot_db) | [![Docs.rs](https://docs.rs/vsdb_slot_db/badge.svg)](https://docs.rs/vsdb_slot_db) | `utils/slot_db` | A skip-list-like, timestamp-based index for efficient paged queries. |
| [**vsdb_trie_db**](utils/trie_db) | [![Crates.io](https://img.shields.io/crates/v/vsdb_trie_db.svg)](https://crates.io/crates/vsdb_trie_db) | [![Docs.rs](https://docs.rs/vsdb_trie_db/badge.svg)](https://docs.rs/vsdb_trie_db) | `utils/trie_db` | An out-of-the-box Merkle Patricia Trie (MPT) implementation. |

### Known Issues

- The `len()` of a data structure is not always guaranteed to be absolutely reliable and should be treated as a hint. This is because some operations may not update the length atomically in real-time for performance reasons.

