# VSDB — Claude Code Project Guide

## What is this project?

VSDB is a high-performance embedded key-value database for Rust that provides:
- **Persistent collections** with Rust std-like API (Mapx, MapxOrd, Orphan)
- **Git-model versioning** (VerMap) — branches, commits, three-way merge, rollback, GC
- **Merkle tries** (MPT + SMT) for stateless cryptographic commitments
- **Slot-based indexing** (SlotDex) for timestamp-paged queries
- **DAG-based collections** (DagMap) for graph-like data
- **Vector index** (VecDex) — pure-Rust HNSW for approximate nearest-neighbor search; `VecDexDyn` + `MetricKind` select the distance metric at runtime (enum dispatch, metric persisted in meta)
- **Namespaces** — anonymous placement groups: independently-rooted engine instances in one process (own dir/volume, shards, WALs, memory budget); co-location via `handle.namespace()` + `new_in`/`ns.scope(..)`; cross-ns deep copy via `clone_in(&ns)` (MapxRaw and typed wrappers); in-process `close` (full resource reclaim; consuming `Namespace::close(self)` returns the handle on refusal) and O(1) whole-namespace destroy — the epoch-rotation loop needs no restart

Built exclusively on [mmdb](https://github.com/rust-util-collections/mmdb) (pure-Rust LSM-Tree engine). The default namespace uses 16-shard prefix-based routing (pinned); non-default namespaces persist their own creation-time shard count.

## Workspace Layout

```
vsdb/
├── core/     # vsdb_core — engine integration, MapxRaw, prefix allocation
└── strata/   # vsdb — typed collections, versioning, tries, slotdex, dagmap, vecdex
```

## Build & Test

```bash
make all          # fmt + lint + test
make test         # cargo test --workspace (release + debug, parallel)
make lint         # cargo clippy --workspace + check tests/benches
make bench        # criterion benches (core basic, strata basic, versioned, slotdex)
```

**Important**: Tests run in PARALLEL (v16.0.2+). Test data stays disjoint via globally-unique prefixes; tests must not assert on cross-test global state (exact allocator values, registry sizes) and must serialize any `vsdb_set_base_dir` behind a `Once` (env mutation is unsound to race).

## Architecture

| Subsystem | Key files | Purpose |
|-----------|-----------|---------|
| Engine | `core/src/common/engine/mod.rs`, `mmdb.rs` | Mapx, batch ops, GLOBAL prefix alloc (all namespaces), per-ns shard routing, format marker |
| Namespaces | `core/src/common/namespace.rs` | Namespace handle, registry, InstanceId, ambient scope, lifecycle (create/open/close/destroy/relocate); engines owned by `Arc<NsInner>`, no leak |
| MapxRaw | `core/src/basic/mapx_raw/` | Untyped raw KV, prefix isolation |
| Typed Collections | `strata/src/basic/mapx/`, `mapx_ord/`, `mapx_ord_rawkey/`, `orphan/` | Mapx<K,V>, MapxOrd<K,V>, MapxOrdRawKey<V>, Orphan<T> |
| Persistent B+ Tree | `strata/src/basic/persistent_btree/` | COW B+ tree, structural sharing |
| Versioning | `strata/src/versioned/` | VerMap, Branch/BranchMut handles, commit DAG, merge |
| Error types | `core/src/common/error.rs` (re-exported via `vsdb::common::error`) | VsdbError enum (thiserror-based), unified across both crates |
| Merkle Tries | `strata/src/trie/` | MPT (16-ary) + SMT (binary 256-bit) |
| Slot Index | `strata/src/slotdex/` | Time-slot tier-based indexing (single-handle, crash-atomic) |
| DAG Collections | `strata/src/dagmap/` | DAG-based data structures |
| Vector Index | `strata/src/vecdex/` | VecDex + VecDexDyn (runtime `MetricKind`), HNSW ANN search, distance metrics (single-handle, crash-atomic) |
| Encoding | `strata/src/common/ende.rs` | postcard-based KeyEnDe/ValueEnDe |
| Staged mutation | `strata/src/common/staged.rs` | read-your-writes overlay + one atomic write batch per mutation (SlotDex/VecDex) |

## Commands

- `/x-review` — deep regression analysis (supports: N commits, `all`, hash, range)
- `/x-fix` — fix audit backlog: resolve `docs/audit.md` → self-review → commit
- `/x-commit` — self-reviewing commit: review uncommitted changes → fix → commit
- `/x-overhaul` — full codebase overhaul: review all → fix → commit

Supporting documentation in `.claude/docs/`:
- `technical-patterns.md` — cataloged bug patterns for vsdb + mmdb layers
- `review-core.md` — systematic review methodology
- `false-positive-guide.md` — rules for filtering spurious findings
- `patterns/` — per-subsystem review guides (btree, versioning, trie, slotdex, dagmap, engine, vecdex)

Additional documentation in `docs/`:
- `audit.md` — audit findings registry (tracked by /x-review and /x-fix)

## Conventions

- All clippy warnings are errors (`#![deny(warnings)]` in lib.rs)
- **No `#[allow(...)]`** — fix warnings at the source, never suppress them
- **Prefer imports over inline paths** — avoid `std::foo::Bar::new()` inline in function bodies when the same path appears 3+ times in a file; add `use std::foo;` at file top (or `use std::foo::Bar;`) instead. Function-body `use` statements (scoped imports) are fine and don't count as inline paths. 1-2 inline uses of common `std::` items are acceptable.
- **Grouped imports** — merge common prefixes: `use std::sync::{Arc, Mutex};`
- **Doc-code alignment** — public API changes must update corresponding docs
- `parking_lot` for Mutex (prefix allocator, VSDB_BASE_DIR global, DagMap ID allocation)
- `VsdbError` (thiserror, defined in `vsdb_core`) is the **only** error type in public APIs of both crates; `ruc` is internal-only for error chaining — boundary conversions preserve the complete chain via `stringify_chain`
- `postcard` for serialization (replaced serde_cbor_2 in v12)
- Tests run in parallel; isolation comes from globally-unique prefixes (plus `tempdir`/`/tmp/vsdb_testing` for file-level scratch); global-state assertions must be race-tolerant
- ~22 unsafe blocks in library code (plus ~5 in benches) — all require `// SAFETY:` comments
  - `shadow()`: SWMR contract — caller serializes writes (the ONLY aliasing handle primitive; `Clone` deep-copies storage)
  - `from_bytes()`: caller provides valid serialized bytes
  - Pointer casts in entry API macros
  - `env::set_var` in `vsdb_set_base_dir`: caller must invoke before spawning threads
