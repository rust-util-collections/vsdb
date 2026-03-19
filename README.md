![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/vsdb)
[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.85+-lightgray.svg)](https://github.com/rust-util-collections/vsdb)

# vsdb

A high-performance, embedded key-value database for Rust with an API that feels like standard collections.

## What it does

- **Persistent collections** — `Mapx` (like `HashMap`), `MapxOrd` (like `BTreeMap`), backed by RocksDB
- **Git-model versioning** — `VerMap` provides branching, commits, three-way merge, rollback, and garbage collection over a COW B+ tree with structural sharing
- **Merkle trie** — `MptCalc` (Merkle Patricia Trie) and `SmtCalc` (Sparse Merkle Tree) as stateless computation layers; `VerMapWithProof` integrates `VerMap` with `MptCalc` for versioned 32-byte Merkle root commitments
- **Slot-based index** — `SlotDex` for efficient, timestamp-based paged queries via a skip-list-like tier structure

## Quick start

```toml
[dependencies]
vsdb = "9.1.0"
```

```rust
use vsdb::versioned::map::VerMap;

let mut m: VerMap<u32, String> = VerMap::new();
let main = m.main_branch();

m.insert(main, &1, &"hello".into()).unwrap();
m.commit(main).unwrap();

let feat = m.create_branch("feature", main).unwrap();
m.insert(feat, &1, &"updated".into()).unwrap();
m.commit(feat).unwrap();

// Branches are isolated
assert_eq!(m.get(main, &1).unwrap(), Some("hello".into()));
assert_eq!(m.get(feat, &1).unwrap(), Some("updated".into()));

// Three-way merge: source wins on conflict
m.merge(feat, main).unwrap();
assert_eq!(m.get(main, &1).unwrap(), Some("updated".into()));

m.delete_branch(feat).unwrap();
m.gc();
```

## Architecture

```text
vsdb (workspace)
+-- core/    vsdb_core   RocksDB engine, MapxRaw, PersistentBTree
+-- strata/  vsdb         High-level crate (the one users depend on)
     +-- basic/       Mapx, MapxOrd, MapxOrdRawKey, Orphan
     +-- versioned/   VerMap (branch, commit, merge, diff, gc)
     +-- trie/        MptCalc, SmtCalc, VerMapWithProof
     +-- slotdex/     SlotDex
     +-- dagmap/      DagMapRaw, DagMapRawKey
```

### Module overview

| Module | Key types | Purpose |
|--------|-----------|---------|
| [`basic`](strata/src/basic) | `Mapx`, `MapxOrd`, `Orphan` | Persistent, typed collections backed by RocksDB |
| [`versioned`](strata/src/versioned) | `VerMap`, `BranchId`, `CommitId` | Git-model versioned KV store with COW B+ tree |
| [`trie`](strata/src/trie) | `MptCalc`, `SmtCalc`, `SmtProof`, `VerMapWithProof` | Stateless Merkle tries + VerMap integration |
| [`slotdex`](strata/src/slotdex) | `SlotDex` | Skip-list-like index for timestamp-based paged queries |
| [`dagmap`](strata/src/dagmap) | `DagMapRaw`, `DagMapRawKey` | DAG-based collections |

### Trie + VerMap integration

```text
  VerMap<K,V>          MptCalc / SmtCalc
  (persistence)        (computation)
  +-------------+      +-------------+
  | branch/     |      | in-memory   |
  | commit/     | diff | trie nodes  |  root_hash()
  | merge/      |----->| (ephemeral) |-------------> [u8; 32]
  | rollback    |      |             |
  +-------------+      +-------------+
       |                      |
       |                 save_cache()
       |                 load_cache()
       |                      |
       |                +-----v-----+
       |                | disk cache| (disposable)
       +----------------+-----------+
```

`VerMapWithProof` wraps a `VerMap` and an `MptCalc`. On each `merkle_root()` call it computes an incremental diff from the last sync point and applies it to the trie, avoiding full rebuilds. A disposable on-disk cache makes restarts cheap.

`SmtCalc` additionally supports `prove()` / `verify_proof()` for constant-time (256-hash) membership and non-membership proofs.

## Documentation

- [API Examples](strata/docs/api.md)
- [Engine Comparison: MMDB vs RocksDB](strata/docs/engine-comparison.md)
- [Versioned Module — Architecture & Internals](strata/docs/versioned.md)

## License

MIT
