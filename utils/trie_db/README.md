# vsdb_trie_db

[![Crates.io](https://img.shields.io/crates/v/vsdb_trie_db.svg)](https://crates.io/crates/vsdb_trie_db)
[![Docs.rs](https://docs.rs/vsdb_trie_db/badge.svg)](https://docs.rs/vsdb_trie_db)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> An out-of-the-box wrapper for `trie-db` with persistent storage.

This crate provides `MptStore`, a high-level wrapper for [`trie-db`](https://crates.io/crates/trie-db) that uses `vsdb` for its underlying storage. It simplifies the management of multiple Merkle Patricia Tries (MPTs), handling state, commits, and backend storage automatically.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vsdb_trie_db = "5.0.1"
```

## Usage

For more detailed API examples, see [API Examples](docs/api.md).

`vsdb_trie_db` provides an easy-to-use interface for creating and managing persistent MPTs.

```rust
use vsdb_trie_db::MptStore;

// Create a new MptStore
let mut store = MptStore::new();

// Initialize a new trie with a unique backend key
let mut trie = store.trie_init(b"my_app_state").unwrap();

// Insert a key-value pair
trie.insert(b"key1", b"value1").unwrap();

// Commit the changes to persist them and get a new state root
let mut trie = trie.commit().unwrap();
let root = trie.root();

// Retrieve the value using the latest trie instance
let value = trie.get(b"key1").unwrap().unwrap();
assert_eq!(value, b"value1");

// Create a read-only handle to the trie at a specific root
let ro_trie = trie.ro_handle(root).unwrap();
let value = ro_trie.get(b"key1").unwrap().unwrap();
assert_eq!(value, b"value1");
```

## License

This project is licensed under the **MIT** license.
