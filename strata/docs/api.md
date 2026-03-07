# vsdb API Examples

This document provides examples for the public APIs in the `vsdb` crate.

## Mapx

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
```

## MapxOrd

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

## VerMap

`VerMap` provides Git-style versioned storage with branching, commits, merge, and rollback.

For a detailed architecture guide with diagrams, see [Versioned Module — Architecture & Internals](versioned.md).

```rust
use vsdb::versioned::map::VerMap;
use vsdb::versioned::BranchId;

// 1. Create an empty versioned map (starts with a "main" branch).
let mut m: VerMap<u32, String> = VerMap::new();
let main = m.main_branch();

// 2. Write on the main branch and commit a snapshot.
m.insert(main, &1, &"hello".into()).unwrap();
m.commit(main).unwrap();

// 3. Fork a feature branch — cheap, no data copied.
let feat: BranchId = m.create_branch("feature", main).unwrap();
m.insert(feat, &1, &"updated".into()).unwrap();
m.commit(feat).unwrap();

// 4. Branches are isolated.
assert_eq!(m.get(main, &1).unwrap(), Some("hello".into()));
assert_eq!(m.get(feat, &1).unwrap(), Some("updated".into()));

// 5. Three-way merge: feature → main (source wins on conflict).
m.merge(feat, main).unwrap();
assert_eq!(m.get(main, &1).unwrap(), Some("updated".into()));

// 6. Clean up: delete the branch, then garbage-collect unreachable data.
m.delete_branch(feat).unwrap();
m.gc();
```
