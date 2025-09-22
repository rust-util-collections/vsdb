# vsdb_trie_db API Examples

This document provides examples for the public APIs in the `vsdb_trie_db` crate.

## MptStore

`MptStore` provides an out-of-the-box wrapper for the `trie-db` crate, backed by `vsdb`.

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
