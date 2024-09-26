![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/vsdb)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.81+-lightgray.svg)](https://github.com/rust-random/rand#rust-version-requirements)

# vsdb

vsdb is a 'std-collection-like' database.

This is a simplified version of the original [**vsdb**](https://crates.io/crates/vsdb/0.70.0), retaining only the most practical and stable parts.

Check [**here**](wrappers/README.md) for a detailed description.

### Crate List

|Name|Version|Doc|Path|Description|
|:-|:-|:-|:-|:-|
|[**vsdb**](wrappers)|[![](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb)|`wrappers`|High-level APIs|
|[**vsdb_core**](core)|[![](https://img.shields.io/crates/v/vsdb_core.svg)](https://crates.io/crates/vsdb_core)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb_core)|`core`|Low-level implementations|
|[**vsdb_slot_db**](utils/slot_db)|[![](https://img.shields.io/crates/v/vsdb_slot_db.svg)](https://crates.io/crates/vsdb_slot_db)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb_slot_db)|`utils/slot_db`|A skip-list like timestamp DB|
|[**vsdb_trie_db**](utils/trie_db)|[![](https://img.shields.io/crates/v/vsdb_trie_db.svg)](https://crates.io/crates/vsdb_trie_db)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb_trie_db)|`utils/trie_db`|MPT(trie) implementations|
