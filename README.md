![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/vsdb)
[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.89+-lightgray.svg)](https://github.com/rust-util-collections/vsdb)

# vsdb

A high-performance, embedded key-value database for Rust with an API that feels like standard collections.

## What it does

- **Persistent collections** — `Mapx` (like `HashMap`), `MapxOrd` (like `BTreeMap`), backed by MMDB (pure-Rust LSM-Tree)
- **Git-model versioning** — `VerMap` provides branching, commits, three-way merge, and rollback over a COW B+ tree with structural sharing; logical cleanup uses reference counting, with best-effort physical reclamation during MMDB compaction
- **Merkle trie** — `MptCalc` (Merkle Patricia Trie) and `SmtCalc` (Sparse Merkle Tree) as stateless computation layers; `VerMapWithProof` pairs `VerMap` with either back-end for versioned 32-byte Merkle root commitments
- **Slot-based index** — `SlotDex` for efficient, timestamp-based paged queries via a skip-list-like tier structure
- **Vector index** — `VecDex` for approximate nearest-neighbor search via a pure-Rust HNSW implementation; supports L2, Cosine, and InnerProduct metrics with filtered search
- **Namespaces** — anonymous placement groups: independently-rooted engine instances in one process (own dir/volume, shards, WALs, memory budget), with whole-directory destruction without per-key traversal; placement is expressed through handles and creation scopes

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

// Same namespace placement (this does not guarantee the same shard).
let mut index = Mapx::<u64, u64>::new_in(&archive.namespace());

// Recovery rides the identifiers you already persist:
let id = archive.save_meta().unwrap();          // InstanceId, e.g. "42@1"
let restored: Mapx<u64, String> = Mapx::from_meta(id).unwrap();

// Advanced tier (opt-in): explicit volume, shard count, memory budget.
// Namespace::create_with(NamespaceOpts { path, shards, mem_budget_mb })
// Admin: Namespace::list() / Namespace::destroy(id) / Namespace::relocate(id, path)
```

`Mapx::new()` targets the current `Namespace::scope`, or the implicit
default namespace outside a scope. Cross-namespace atomic transactions do not
exist (separate WALs); a composite structure (`VerMap`, `SlotDex`, …)
always lives wholly inside one namespace.

### Read-only mode

Open an existing VSDB universe without changing its directories, including
when the tree is mounted read-only:

```rust,no_run
use vsdb::{InstanceId, Mapx, VsdbOptions, vsdb_configure};

# fn main() -> vsdb::Result<()> {
vsdb_configure(VsdbOptions::read_only("/srv/my-app/vsdb"))?;

// An earlier writable process created the map and persisted this token.
let id: InstanceId = "40960000".parse()?;
let map = Mapx::<String, String>::from_meta(id)?;
for (key, value) in map.iter() {
    println!("{key}: {value}");
}
# Ok(())
# }
```

Configuration is one-shot and process-wide: call it before any other VSDB
access, and every automatically opened namespace inherits read-only mode.
Reads include in-memory WAL recovery; creation and mutation are unavailable,
while maintenance writes such as flushes are skipped. Explicit trie-cache
saves return a read-only error. See the [read-only mode guide](strata/docs/read-only.md) for locking,
snapshots, error/panic behavior, and recovery constraints.

### Memory sizing

Memory sizing uses a fixed 2 GiB default for the default namespace and
512 MiB for each non-default namespace. VSDB does not inspect host RAM or
cgroup limits. `mem_budget_mb` and `VSDB_MEM_BUDGET_MB` use binary MiB.

Configure the default engine before the first VSDB access:

```rust,no_run
use vsdb::{VsdbOptions, vsdb_configure};

vsdb_configure(VsdbOptions::new("/data/vsdb").with_mem_budget_mb(8192)).unwrap();
```

An explicit nonzero budget takes precedence over `VSDB_MEM_BUDGET_MB`; non-default
namespaces use `NamespaceOpts::mem_budget_mb`. These are sizing inputs, not
hard process RSS limits: cache and write-buffer sizes have floors and caps,
and runtime metadata, pinned blocks, and application allocations need extra
headroom. More memory may help a cache- or buffer-limited workload; measure
before changing it. See the [memory design](docs/proposals/shared-mem-pool.md)
for the current formulas and telemetry.

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

`VerMapWithProof` wraps a `VerMap` and a trie back-end (`MptCalc` or `SmtCalc`). A `merkle_root()` call uses an incremental diff when the previous sync point is still usable, and otherwise rebuilds from the selected state. An optional `save_cache(commit)` checkpoint makes restarts cheaper; construction loads it automatically. Root computation and `Drop` never rewrite the full cache.

`SmtCalc` additionally supports `prove()` / `verify_proof()` for compact (O(log N)-hash, Diem/JMT-style) membership and non-membership proofs.

## Documentation

- [API Examples](strata/docs/api.md) — Mapx, MapxOrd, VerMap, MptCalc/SmtCalc, VerMapWithProof, SlotDex
- [Read-only Mode](strata/docs/read-only.md) — configuration, supported operations, locking, and snapshots
- [Versioned Module — Architecture & Internals](strata/docs/versioned.md)
- [VecDex — HNSW Vector Index](strata/docs/vecdex.md)
- [Namespace Design](docs/proposals/namespaces.md) — placement, metadata, recovery, and lifetime
- [Memory Pools](docs/proposals/shared-mem-pool.md) — current sizing, cache sharing, and deferred extensions
- [Changelog](CHANGELOG.md)

## License

MIT
