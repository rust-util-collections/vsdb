# vsdb_trie_db

[![Crates.io](https://img.shields.io/crates/v/vsdb_trie_db.svg)](https://crates.io/crates/vsdb_trie_db)
[![Docs.rs](https://docs.rs/vsdb_trie_db/badge.svg)](https://docs.rs/vsdb_trie_db)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> An out-of-box wrapper of the `trie_db` crate.

This crate provides an out-of-the-box wrapper for [`trie-db`](https://crates.io/crates/trie-db), based on the powerful [`vsdb`](https://crates.io/crates/vsdb) crate.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vsdb_trie_db = "4.0.1"
```

## Usage

`vsdb_trie_db` provides an out-of-the-box wrapper for the `trie-db` crate, backed by `vsdb`.

```rust
use vsdb_trie_db::MptStore;

// Create a new MptStore
let mut store = MptStore::new();

// Initialize a new trie with a backend key
let mut trie = store.trie_init(b"my_trie").unwrap();

// Insert a key-value pair
trie.insert(b"key1", b"value1").unwrap();

// Commit the changes to the trie
let mut trie = trie.commit().unwrap();
let root = trie.root();

// Retrieve the value
let value = trie.get(b"key1").unwrap().unwrap();
assert_eq!(value, b"value1");

// Create a read-only handle to the trie at a specific root
let ro_trie = trie.ro_handle(root).unwrap();
let value = ro_trie.get(b"key1").unwrap().unwrap();
assert_eq!(value, b"value1");
```

## License

This project is licensed under the **MIT** license.