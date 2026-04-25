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

## MptCalc / SmtCalc (Merkle Trie)

`MptCalc` and `SmtCalc` are stateless, in-memory Merkle trie implementations.

```rust,ignore
use vsdb::trie::MptCalc;

// Build a trie
let mut mpt = MptCalc::new();
mpt.insert(b"key1", b"value1").unwrap();
mpt.insert(b"key2", b"value2").unwrap();

// Compute the 32-byte Merkle root hash
let root = mpt.root_hash().unwrap();
assert_eq!(root.len(), 32);

// Lookup
assert_eq!(mpt.get(b"key1").unwrap(), Some(b"value1".to_vec()));

// Remove
mpt.remove(b"key1").unwrap();
assert_eq!(mpt.get(b"key1").unwrap(), None);

// Batch update
mpt.batch_update(&[
    (b"k1".as_ref(), Some(b"v1".as_ref())),
    (b"k2".as_ref(), None),  // remove
]).unwrap();

// Disposable cache (low-level API, used internally by VerMapWithProof):
// mpt.save_cache(cache_id, sync_tag).unwrap();
// let (loaded, tag, hash) = MptCalc::load_cache(cache_id).unwrap();
// When using VerMapWithProof, caching is fully automatic.
```

### SmtCalc with Proofs

```rust,ignore
use vsdb::trie::SmtCalc;

let mut smt = SmtCalc::new();
smt.insert(b"alice", b"100").unwrap();
smt.insert(b"bob", b"200").unwrap();

let root = smt.root_hash().unwrap();
let root32: [u8; 32] = root.try_into().unwrap();

// Membership proof
let proof = smt.prove(b"alice").unwrap();
assert_eq!(proof.value, Some(b"100".to_vec()));
assert!(SmtCalc::verify_proof(&root32, b"alice", &proof).unwrap());

// Non-membership proof
let proof = smt.prove(b"charlie").unwrap();
assert_eq!(proof.value, None);
assert!(SmtCalc::verify_proof(&root32, b"charlie", &proof).unwrap());
```

## VerMapWithProof

Integrates `VerMap` with `MptCalc` for versioned Merkle root computation.

```rust,ignore
use vsdb::trie::{MptCalc, VerMapWithProof};

let mut vmp: VerMapWithProof<Vec<u8>, Vec<u8>, MptCalc> = VerMapWithProof::new();
let main = vmp.map().main_branch();

// Write data and commit
vmp.map_mut().insert(main, &b"key1".to_vec(), &b"val1".to_vec()).unwrap();
vmp.map_mut().commit(main).unwrap();

// Compute the Merkle root (incrementally maintained)
let root = vmp.merkle_root(main).unwrap();
assert_eq!(root.len(), 32);

// Cache is auto-saved on Drop and auto-loaded on construction.
// No manual save_cache / load_cache calls needed.
```

## Slotdex

`SlotDex` (in the `slotdex` module) is a skip-list-like index for efficient, timestamp-based paged queries.

```rust,ignore
use vsdb::SlotDex;  // SlotDex64<K> alias — slot type is u64

let mut db = SlotDex::<String>::new(10u64, false);

// Insert entries into slots (e.g., timestamps)
db.insert(100, "entry_a".to_string()).unwrap();
db.insert(100, "entry_b".to_string()).unwrap();
db.insert(200, "entry_c".to_string()).unwrap();
db.insert(300, "entry_d".to_string()).unwrap();

assert_eq!(db.total(), 4);

// Paged queries
let page = db.get_entries_by_page(2, 0, true);  // page_size=2, page_index=0, reverse=true
assert_eq!(page, vec!["entry_d".to_string(), "entry_c".to_string()]);

// Slot-range queries
let entries = db.get_entries_by_page_slot(Some(100), Some(200), 10, 0, false);
assert_eq!(entries.len(), 3);

// Remove
db.remove(100, &"entry_a".to_string());
assert_eq!(db.total(), 3);
```
