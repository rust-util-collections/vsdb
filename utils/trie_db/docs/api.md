# vsdb_trie_db API Examples

This document provides examples for the public APIs in the `vsdb_trie_db` crate.

## MptStore

`MptStore` provides a high-level wrapper for managing Merkle Patricia Tries with persistent storage.

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

// Remove a key
trie.remove(b"key2").unwrap();
assert!(trie.get(b"key2").unwrap().is_none());

// Batch update operations
let ops = vec![
    (b"key3".as_ref(), Some(b"value3".as_ref())), // Insert
    (b"key1".as_ref(), None),                       // Remove
];
trie.batch_update(&ops).unwrap();

// Verify changes
assert!(trie.get(b"key1").unwrap().is_none());
assert_eq!(trie.get(b"key3").unwrap().unwrap(), b"value3");
```
