# vsdb_hash_db API Examples

This document provides examples for the public APIs in the `vsdb_hash_db` crate.

## MmBackend

`MmBackend` is a `HashDB` implementation that stores trie nodes in a memory-mapped file.

```rust
use vsdb_hash_db::{MmBackend, KeccakHasher};
use vsdb_hash_db::sp_hash_db::{HashDB, Hasher};
use vsdb::Orphan;

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
