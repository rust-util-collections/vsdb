# vsdb_hash_db

[![Crates.io](https://img.shields.io/crates/v/vsdb_hash_db.svg)](https://crates.io/crates/vsdb_hash_db)
[![Docs.rs](https://docs.rs/vsdb_hash_db/badge.svg)](https://docs.rs/vsdb_hash_db)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> An implementation of the `hash_db::HashDB` trait.

This crate provides an implementation of the [`hash_db::HashDB`](https://crates.io/crates/hash-db) trait, based on the powerful [`vsdb`](https://crates.io/crates/vsdb) crate.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vsdb_hash_db = "4.0"
```

## Usage

`vsdb_hash_db` provides an implementation of the `hash_db::HashDB` trait, backed by `vsdb`.

```rust
use vsdb_hash_db::{MmBackend, KeccakHasher, sp_hash_db::{HashDB, Hasher}};
use vsdb::{Orphan, DagMapRaw};

// Define a type alias for the backend
type TrieBackend = MmBackend<KeccakHasher, Vec<u8>>;

// Create a new Orphan instance
let mut orphan = Orphan::new_dag_map_raw();

// Create a new TrieBackend
let mut db = TrieBackend::new(&mut orphan).unwrap();

// Insert a value and get its hash
let value = b"hello world";
let hash = db.insert(Default::default(), value);

// Retrieve the value using its hash
let retrieved_value = db.get(&hash, Default::default()).unwrap();
assert_eq!(retrieved_value, value);

// Check if a hash exists
assert!(db.contains(&hash, Default::default()));

// Remove the value
db.remove(&hash, Default::default());
assert!(!db.contains(&hash, Default::default()));
```

## License

This project is licensed under the **MIT** license.
