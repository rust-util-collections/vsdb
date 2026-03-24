# vsdb

[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> `vsdb` is a high-performance, embedded database with an API similar to Rust's standard collections.

This crate provides high-level, typed data structures that are backed by a persistent key-value store. It is the primary crate for end-users.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vsdb = "12.0.0"
```

## Highlights

For more detailed API examples, see [API Examples](docs/api.md).
For the versioned storage architecture with diagrams, see [Versioned Module — Architecture & Internals](docs/versioned.md).

- **Familiar API**: Most APIs are designed to mirror their counterparts in the standard library.
  - `Mapx` behaves like `std::collections::HashMap`.
  - `MapxOrd` behaves like `std::collections::BTreeMap`.
- **Persistent Storage**: Data is automatically saved to disk and loaded on instantiation.
- **Typed Keys and Values**: Keys and values are strongly typed and automatically serialized/deserialized.
- **Git-Model Versioning**: `VerMap` provides branching, commits, three-way merge, rollback, and history — backed by a persistent B+ tree with copy-on-write structural sharing.
- **Merkle Trie**: Built-in `MptCalc` (Merkle Patricia Trie) and `SmtCalc` (Sparse Merkle Tree) for cryptographic state commitments. `VerMapWithProof` integrates `VerMap` with `MptCalc` for versioned Merkle roots.
- **Slotdex**: A skip-list-like index (`SlotDex`) for efficient, timestamp-based paged queries.

## Usage

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

### VerMap

`VerMap` provides Git-style versioned storage with branching, commits, merge, and rollback.

The typical lifecycle is: **create -> write -> commit -> branch -> merge -> gc**.

#### Merge conflict resolution: source wins on conflicts

`merge(source, target)` uses three-way merge with the common ancestor.
If only one side changed a key relative to the ancestor, that single-sided
change is preserved. If both sides changed the same key differently, **source
wins**. A deletion is treated as "assigning empty", so delete-vs-modify is also
resolved by source priority.

| source | target | result |
|--------|--------|--------|
| unchanged (A) | changed to T | **T** (target-only change preserved) |
| changed to S | unchanged (A) | **S** (source-only change preserved) |
| changed to S | changed to T | **S** (conflict -> source wins) |
| deleted | changed to T | **deleted** (conflict -> source wins -> delete) |
| changed to S | deleted | **S** (conflict -> source wins -> keep) |

The caller controls priority by choosing which branch to pass as `source` vs `target`.

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

// 5. Three-way merge: feature -> main (source wins on conflict).
m.merge(feat, main).unwrap();
assert_eq!(m.get(main, &1).unwrap(), Some("updated".into()));

// 6. Clean up: delete the branch, then garbage-collect unreachable data.
m.delete_branch(feat).unwrap();
m.gc();
```

### MptCalc / SmtCalc (Merkle Trie)

`MptCalc` (Merkle Patricia Trie) and `SmtCalc` (Sparse Merkle Tree) are stateless,
in-memory Merkle trie implementations. They are designed as computation layers
that can be paired with `VerMap` for versioned Merkle root commitments.

```rust,ignore
use vsdb::trie::MptCalc;

let mut mpt = MptCalc::new();
mpt.insert(b"key1", b"value1").unwrap();
mpt.insert(b"key2", b"value2").unwrap();

// Compute the 32-byte Merkle root hash
let root = mpt.root_hash().unwrap();
assert_eq!(root.len(), 32);

// Lookup by key
assert_eq!(mpt.get(b"key1").unwrap(), Some(b"value1".to_vec()));
```

`SmtCalc` additionally supports Merkle proofs:

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
assert!(SmtCalc::verify_proof(&root32, &proof).unwrap());

// Non-membership proof
let proof = smt.prove(b"charlie").unwrap();
assert_eq!(proof.value, None);
assert!(SmtCalc::verify_proof(&root32, &proof).unwrap());
```

### VerMapWithProof

`VerMapWithProof` integrates `VerMap` with `MptCalc` for versioned Merkle root computation:

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
```

### Slotdex

`SlotDex` (in the `slotdex` module) is a skip-list-like data structure for fast, timestamp-based paged queries.

```rust,ignore
use vsdb::SlotDex;  // SlotDex64<K> alias — slot type is u64

let mut db = SlotDex::<String>::new(10u64, false);

db.insert(100, "entry_a".to_string()).unwrap();
db.insert(100, "entry_b".to_string()).unwrap();
db.insert(200, "entry_c".to_string()).unwrap();
db.insert(300, "entry_d".to_string()).unwrap();

assert_eq!(db.total(), 4);

// Get entries by page (page_size=2, page_index=0, reverse=true)
let entries = db.get_entries_by_page(2, 0, true);
assert_eq!(entries, vec!["entry_d".to_string(), "entry_c".to_string()]);
```

## Important Notes

- The serialized result of a `vsdb` instance cannot be used for distributed consensus. The serialized data contains meta-information (like storage paths) that may differ across environments. The correct approach is to read the required data and then process the raw content.

## License

This project is licensed under the **MIT** license.
