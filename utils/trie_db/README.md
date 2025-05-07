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
vsdb_trie_db = "6.0.1"
```

## Usage

For more detailed API examples, see [API Examples](docs/api.md).

`vsdb_trie_db` provides an easy-to-use interface for creating and managing persistent MPTs.

```rust
use vsdb_trie_db::MptStore;

// Create a new MptStore
let store = MptStore::new();

// Initialize a new trie (starts with an empty root)
let mut trie = store.trie_init();

// Insert key-value pairs (automatically commits)
trie.insert(b"key1", b"value1").unwrap();
trie.insert(b"key2", b"value2").unwrap();

// Get the current root hash
let root = trie.root();

// Retrieve values
let value = trie.get(b"key1").unwrap().unwrap();
assert_eq!(value, b"value1");

// Load an existing trie from a root hash
let loaded_trie = store.trie_load(&root);
let value = loaded_trie.get(b"key1").unwrap().unwrap();
assert_eq!(value, b"value1");

// Batch update operations
let ops = vec![
    (b"key3".as_ref(), Some(b"value3".as_ref())),
    (b"key1".as_ref(), None), // Remove key1
];
trie.batch_update(&ops).unwrap();
```

## License

This project is licensed under the **MIT** license.
