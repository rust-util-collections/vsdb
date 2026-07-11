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
make bench        # criterion benches (core: basic + cache_pool; strata: basic, versioned, slotdex, trie_bench, vecdex)
```

**Important**: Tests run in PARALLEL (v16.0.2+). Test data stays disjoint via globally-unique prefixes; tests must not assert on cross-test global state (exact allocator values, registry sizes) and must serialize any `vsdb_set_base_dir` behind a `Once` (env mutation is unsound to race).

`make test` is the manual/CI convenience target and performs global cleanup.
Automation skills use direct `cargo test --workspace --tests` commands instead,
so they never delete `$HOME/.vsdb` or shared `/tmp/vsdb_testing`.

## Architecture

| Subsystem | Key files | Purpose |
|-----------|-----------|---------|
| Engine | `core/src/common/engine/mod.rs`, `mmdb.rs` | Mapx, batch ops, GLOBAL prefix alloc (all namespaces), per-ns shard routing, per-engine block-cache pool (shards share one `BlockCachePool`), format marker |
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

## Skills

Project skills live under `.claude/skills/<name>/SKILL.md` and are
user-invocable only.

- `/x-review` — deep regression analysis (supports: N commits, `all`, hash, range)
- `/x-fix` — fix audit backlog: resolve `docs/audit.md` → self-review → commit
- `/x-commit` — self-reviewing commit: review uncommitted changes → fix → commit
- `/x-overhaul` — full codebase overhaul: review all → fix → commit

Supporting documentation in `.claude/docs/`:
- `workflow-policy.md` — shared-worktree safety and one-issue-one-commit policy
- `commit-protocol.md` — atomic validation/commit and lockstep version + release tag procedure
- `compatibility-policy.md` — public/on-disk breaks and required migration paths
- `technical-patterns.md` — cataloged bug patterns for vsdb + mmdb layers
- `review-core.md` — systematic methodology + canonical subsystem map
- `false-positive-guide.md` — rules for filtering spurious findings
- `patterns/` — per-subsystem review guides (btree, versioning, trie, slotdex, dagmap, engine, vecdex)

Additional documentation in `docs/`:
- `audit.md` — Open, Won't Fix, and Rejected registry (tracked by /x-review and /x-fix)

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
- Unsafe code is concentrated in handle reconstruction/shadowing, entry APIs,
  DagMap aliases, trie internals, and concurrent benches; derive the live
  inventory during review and require accurate `// SAFETY:` contracts
  - `shadow()`: aliasing contract — no concurrent writes to the same storage
    key; disjoint-key raw/typed map writes are allowed, while structural
    multi-key operations require their documented broader serialization
    (`Clone` deep-copies storage)
  - `from_bytes()`: caller provides a valid uniquely-owned prefix for the
    correct type and namespace
  - Pointer casts in entry API macros
  - `env::set_var` in `vsdb_set_base_dir`: caller must invoke before spawning threads
- **No Co-Authored-By in commits** — never add `Co-Authored-By:` or similar trailers to commit messages; project commits are authored only by the human contributor
