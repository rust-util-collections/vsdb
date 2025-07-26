# vsdb

[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](../../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> A std-collection-like database.

This is a simplified version of the original [**vsdb**](https://crates.io/crates/vsdb/0.70.0), retaining only the most practical and stable parts. This crate provides high-level APIs.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vsdb = "4.0"
```

## Highlights

- Most APIs are similar to the corresponding data structures in the standard library:
    - Use `Vecx` just like `Vec`.
    - Use `Mapx` just like `HashMap`.
    - Use `MapxOrd` just like `BTreeMap`.

## Features

- `parity_backend`: **(Default)** Use `parity-db` as the backend database. Pure Rust implementation.
- `rocks_backend`: Use `rocksdb` as the backend database. C++ implementation.
- `msgpack_codec`: **(Default)** Use `rmp-serde` as the codec for faster performance.
- `json_codec`: Use `serde_json` as the codec for better compatibility.
- `compress`: **(Default)** Enable compression in the backend database.

## Usage

### Vecx

`Vecx` is a vector-like data structure that stores values in a contiguous sequence.

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

`Mapx` is a hash map-like data structure that stores key-value pairs.

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

`MapxOrd` is a B-tree map-like data structure that stores key-value pairs in a sorted order.

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

- The serialized result of a `vsdb` instance cannot be used as the basis for distributed consensus. The serialized result only contains meta-information (like storage paths) which may differ across environments. The correct approach is to read the required data and then process the actual content.
- The instance `len` is not absolutely reliable and should be regarded as a hint.

## License

This project is licensed under the **GPL-3.0** license.
