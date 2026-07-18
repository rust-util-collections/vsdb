![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/vsdb)
[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.85+-lightgray.svg)](https://github.com/rust-util-collections/vsdb)

# vsdb

A high-performance, embedded key-value database for Rust with an API that feels like standard collections.

## What it does

- **Persistent collections** — `Mapx` (like `HashMap`), `MapxOrd` (like `BTreeMap`), backed by MMDB (pure-Rust LSM-Tree)
- **Git-model versioning** — `VerMap` provides branching, commits, three-way merge, and rollback over a COW B+ tree with structural sharing; garbage collection is fully automatic via reference counting and MMDB background compaction
- **Merkle trie** — `MptCalc` (Merkle Patricia Trie) and `SmtCalc` (Sparse Merkle Tree) as stateless computation layers; `VerMapWithProof` pairs `VerMap` with either back-end for versioned 32-byte Merkle root commitments
- **Slot-based index** — `SlotDex` for efficient, timestamp-based paged queries via a skip-list-like tier structure
- **Vector index** — `VecDex` for approximate nearest-neighbor search via a pure-Rust HNSW implementation; supports L2, Cosine, and InnerProduct metrics with filtered search
- **Namespaces** — anonymous placement groups: independently-rooted engine instances in one process (own dir/volume, shards, WALs, memory budget), with O(1) whole-namespace destroy; plain `new()` is untouched, placement is expressed through the object graph

## Quick start

```bash
cargo add vsdb
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
// Dead commits and B+ tree nodes are reclaimed automatically —
// no manual gc() call required.
```

### Namespaces

```rust
use vsdb::{Namespace, basic::mapx::Mapx};

// Everyday tier: zero parameters, no names, no paths.
let cold = Namespace::create().unwrap();

// Place a whole subsystem with one line (creation-time only —
// reads/writes/deserialization always route via the handle itself):
let mut archive: Mapx<u64, String> = cold.scope(|| Mapx::new());

// Co-location: "put this data together with that data".
let mut index = Mapx::<u64, u64>::new_in(&archive.namespace());

// Recovery rides the identifiers you already persist:
let id = archive.save_meta().unwrap();          // InstanceId, e.g. "42@1"
let restored: Mapx<u64, String> = Mapx::from_meta(id).unwrap();

// Advanced tier (opt-in): explicit volume, shard count, memory budget.
// Namespace::create_with(NamespaceOpts { path, shards, mem_budget_mb })
// Admin: vsdb_ns_list() / vsdb_ns_destroy(id) / vsdb_ns_relocate(id, path)
```

`Mapx::new()` still targets the implicit default namespace — existing
code needs zero changes. Cross-namespace atomic transactions do not
exist (separate WALs); a composite structure (`VerMap`, `SlotDex`, …)
always lives wholly inside one namespace.

### Memory sizing

Memory budgets are fixed and predictable: the default namespace uses
2 GiB, every other namespace 512 MB (per-namespace override via
`NamespaceOpts { mem_budget_mb, .. }`). vsdb never sizes itself from
the host's RAM or its cgroup.

Applications that can afford more memory should raise the budget of the
default engine through the `VSDB_MEM_BUDGET_MB` environment variable
(applied verbatim; set before the first database touch). **A larger
budget enlarges the block cache and write buffers, which directly
improves read and write performance** — give vsdb as much memory as the
deployment can spare.

```bash
VSDB_MEM_BUDGET_MB=8192 ./your-app   # 8 GiB for the default engine
```

## Architecture

```text
vsdb (workspace)
+-- core/    vsdb_core   Storage engine (MMDB), MapxRaw, prefix allocation
+-- strata/  vsdb        High-level crate (the one users depend on)
     +-- basic/          Mapx, MapxOrd, MapxOrdRawKey, Orphan, PersistentBTree
     +-- versioned/      VerMap (branch, commit, merge, diff)
     +-- trie/           MptCalc, SmtCalc, VerMapWithProof
     +-- slotdex/        SlotDex
     +-- dagmap/         DagMapRaw, DagMapRawKey
     +-- vecdex/         VecDex (HNSW vector index)
```

### Module overview

| Module | Key types | Purpose |
|--------|-----------|---------|
| [`basic`](strata/src/basic) | `Mapx`, `MapxOrd`, `Orphan`, `PersistentBTree` | Persistent, typed collections + COW B+ tree |
| [`versioned`](strata/src/versioned) | `VerMap`, `BranchId`, `CommitId` | Git-model versioned KV store with COW B+ tree |
| [`trie`](strata/src/trie) | `MptCalc`, `SmtCalc`, `SmtProof`, `VerMapWithProof` | Stateless Merkle tries + VerMap integration |
| [`slotdex`](strata/src/slotdex) | `SlotDex` | Skip-list-like index for timestamp-based paged queries |
| [`dagmap`](strata/src/dagmap) | `DagMapRaw`, `DagMapRawKey` | DAG-based collections |
| [`vecdex`](strata/src/vecdex) | `VecDex`, `VecDexDyn`, `HnswConfig` | Approximate nearest-neighbor vector index (HNSW); metric compile-time or runtime-selected |

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

`VerMapWithProof` wraps a `VerMap` and a trie back-end (`MptCalc` or `SmtCalc`). On each `merkle_root()` call it computes an incremental diff from the last sync point and applies it to the trie, avoiding full rebuilds. A disposable on-disk cache makes restarts cheap.

`SmtCalc` additionally supports `prove()` / `verify_proof()` for compact (O(log N)-hash, Diem/JMT-style) membership and non-membership proofs.

## Documentation

- [API Examples](strata/docs/api.md) — Mapx, MapxOrd, VerMap, MptCalc/SmtCalc, VerMapWithProof, SlotDex
- [Versioned Module — Architecture & Internals](strata/docs/versioned.md)
- [VecDex — HNSW Vector Index](strata/docs/vecdex.md)
- [Changelog](CHANGELOG.md)

## License

MIT
