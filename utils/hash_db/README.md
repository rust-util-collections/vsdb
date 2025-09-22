# vsdb_hash_db

[![Crates.io](https://img.shields.io/crates/v/vsdb_hash_db.svg)](https://crates.io/crates/vsdb_hash_db)
[![Docs.rs](https://docs.rs/vsdb_hash_db/badge.svg)](https://docs.rs/vsdb_hash_db)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> An implementation of the `hash_db::HashDB` trait, backed by `vsdb`.

This crate provides `MmBackend`, a concrete implementation of the [`hash_db::HashDB`](https://crates.io/crates/hash-db) trait. It uses `vsdb` as its underlying storage, making it a persistent backend for hash-based data structures like Merkle Patricia Tries.

It is primarily used by `vsdb_trie_db`.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vsdb_hash_db = "5.0.1"
```

## Usage

`vsdb_hash_db` provides a `vsdb`-backed implementation of the `hash_db::HashDB` trait.

```rust
use vsdb_hash_db::{MmBackend, KeccakHasher, sp_hash_db::{HashDB, Hasher}};
use vsdb::{Orphan, DagMapRaw};

// The value type for the trie, typically raw bytes.
type TrieValue = Vec<u8>;

// Create a new orphan parent for the backend.
let mut parent = Orphan::new_free();

// Create a new backend instance.
let mut backend = MmBackend::<KeccakHasher, TrieValue>::new(&mut parent).unwrap();

// Use the HashDB interface to insert a value.
let value = b"my_value";
let hash = backend.insert(Default::default(), value);

// Check if the value exists.
assert!(backend.contains(&hash, Default::default()));

// Get the value.
let retrieved_value = backend.get(&hash, Default::default()).unwrap();
assert_eq!(retrieved_value, value);

// Remove the value.
backend.remove(&hash, Default::default());
assert!(!backend.contains(&hash, Default::default()));
```

## License

For API examples, see [API Examples](docs/api.md).

This project is licensed under the **MIT** license.
