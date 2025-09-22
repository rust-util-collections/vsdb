# vsdb

[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](../../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> `vsdb` is a high-performance, embedded database with an API similar to Rust's standard collections.

This crate provides high-level, typed data structures that are backed by a persistent key-value store. It is the primary crate for end-users.

This is a simplified version of the original [**vsdb**](https://crates.io/crates/vsdb/0.70.0), retaining only the most practical and stable parts.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vsdb = "5.0.1"
```

## Highlights

For more detailed API examples, see [API Examples](docs/api.md).

- **Familiar API**: Most APIs are designed to mirror their counterparts in the standard library.
  - `Vecx` behaves like `std::collections::Vec`.
  - `Mapx` behaves like `std::collections::HashMap`.
  - `MapxOrd` behaves like `std::collections::BTreeMap`.
- **Persistent Storage**: Data is automatically saved to disk and loaded on instantiation.
- **Typed Keys and Values**: Keys and values are strongly typed and automatically serialized/deserialized.

## Features

- `parity_backend`: **(Default)** Use `parity-db` as the backend database. Pure Rust implementation.
- `rocks_backend`: Use `rocksdb` as the backend database. C++ implementation.
- `msgpack_codec`: **(Default)** Use `rmp-serde` as the codec for faster performance.
- `json_codec`: Use `serde_json` as the codec for better compatibility.
- `compress`: **(Default)** Enable data compression in the backend database.

## Usage

### Vecx

`Vecx` is a persistent, vector-like data structure.

```rust
use vsdb::Vecx;

let mut vec = Vecx::new();

// Push some values
vec.push(&10);
vec.push(&20);
vec.push(&30);

// Get a value
assert_eq!(vec.get(1), Some(20));

// Iterate over the values
for value in vec.iter() {
    println!("{}", value);
}

// Pop a value
assert_eq!(vec.pop(), Some(30));
assert_eq!(vec.len(), 2);
```

### Mapx

`Mapx` is a persistent, hash map-like data structure.

```rust
use vsdb::Mapx;

let mut map = Mapx::new();

// Insert some key-value pairs
map.insert(&"key1", &"value1");
map.insert(&"key2", &"value2");

// Get a value
assert_eq!(map.get(&"key1"), Some("value1".to_string()));

// Check if a key exists
assert!(map.contains_key(&"key2"));

// Iterate over the key-value pairs
for (key, value) in map.iter() {
    println!("{}: {}", key, value);
}

// Remove a key-value pair
map.remove(&"key1");
assert_eq!(map.len(), 1);
```

### MapxOrd

`MapxOrd` is a persistent, B-tree map-like data structure that keeps keys in sorted order.

```rust
use vsdb::MapxOrd;

let mut map = MapxOrd::new();

// Insert some key-value pairs
map.insert(&3, &"three");
map.insert(&1, &"one");
map.insert(&2, &"two");

// Get a value
assert_eq!(map.get(&1), Some("one".to_string()));

// Iterate over the key-value pairs in sorted order
for (key, value) in map.iter() {
    println!("{}: {}", key, value);
}

// Get the first and last key-value pairs
assert_eq!(map.first(), Some((1, "one".to_string())));
assert_eq!(map.last(), Some((3, "three".to_string())));
```

## Important Notes

- The serialized result of a `vsdb` instance cannot be used for distributed consensus. The serialized data contains meta-information (like storage paths) that may differ across environments. The correct approach is to read the required data and then process the raw content.
- The `len()` of a data structure is not always guaranteed to be absolutely reliable and should be treated as a hint. This is because some operations may not update the length atomically in real-time for performance reasons.

## License

This project is licensed under the **GPL-3.0** license.
