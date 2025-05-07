# vsdb_core API Examples

This document provides examples for the public APIs in the `vsdb_core` crate.

## MapxRaw

`MapxRaw` is a raw key-value store.

```rust
use vsdb_core::MapxRaw;
use vsdb_core::common::{vsdb_set_base_dir, vsdb_get_base_dir};

// It's recommended to set a base directory for the database.
// vsdb_set_base_dir("/tmp/vsdb_core_test").unwrap();

let mut map = MapxRaw::new();

// Insert raw bytes
map.insert(b"key1", b"value1");
map.insert(b"key2", b"value2");

// Get raw bytes
assert_eq!(map.get(b"key1").as_deref(), Some(&b"value1"[..]));

// Check for existence
assert!(map.contains_key(b"key2"));

// Remove a key
map.remove(b"key1");
assert!(!map.contains_key(b"key1"));
```

## Utility Functions

Example for getting and setting the base directory.

```rust
use vsdb_core::{vsdb_set_base_dir, vsdb_get_base_dir};

// Set a custom base directory
vsdb_set_base_dir("/tmp/my_vsdb_data").unwrap();

// Get the current base directory
let dir = vsdb_get_base_dir();
assert_eq!(dir.to_str().unwrap(), "/tmp/my_vsdb_data");
```
