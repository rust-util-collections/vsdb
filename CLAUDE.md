# VSDB — Claude Code Project Guide

## What is this project?

VSDB is a high-performance embedded key-value database for Rust that provides:
- **Persistent collections** with Rust std-like API (Mapx, MapxOrd, Orphan)
- **Git-model versioning** (VerMap) — branches, commits, three-way merge, rollback, GC
- **Merkle tries** (MPT + SMT) for stateless cryptographic commitments
- **Slot-based indexing** (SlotDex) for timestamp-paged queries
- **DAG-based collections** (DagMap) for graph-like data

Built exclusively on [mmdb](https://github.com/rust-util-collections/mmdb) (pure-Rust LSM-Tree engine) with 16-shard prefix-based routing.

## Workspace Layout

```
vsdb/
├── core/     # vsdb_core — engine integration, MapxRaw, prefix allocation
└── strata/   # vsdb — typed collections, versioning, tries, slotdex, dagmap
```

## Build & Test

```bash
make all          # fmt + lint + test
make test         # cargo test --workspace (release + debug, single-threaded)
make lint         # cargo clippy --workspace + check tests/benches
make bench        # criterion benches (core, basic, versioned, slotdex, trie_bench)
```

**Important**: Tests MUST run single-threaded (`--test-threads=1`) due to global MMDB state.

## Architecture

| Subsystem | Key files | Purpose |
|-----------|-----------|---------|
| Engine | `core/src/common/engine/mmdb.rs` | MMDB integration, 16-shard routing |
| MapxRaw | `core/src/basic/mapx_raw/` | Untyped raw KV, prefix isolation |
| Typed Collections | `strata/src/basic/mapx/`, `mapx_ord/`, `mapx_ord_rawkey/` | Mapx<K,V>, MapxOrd<K,V>, MapxOrdRawKey<V> |
| Persistent B+ Tree | `strata/src/basic/persistent_btree/` | COW B+ tree, structural sharing |
| Versioning | `strata/src/versioned/` | VerMap, Branch/BranchMut handles, commit DAG, merge |
| Error types | `strata/src/common/error.rs` | VsdbError enum (thiserror-based) |
| Merkle Tries | `strata/src/trie/` | MPT (16-ary) + SMT (binary 256-bit) |
| Slot Index | `strata/src/slotdex/` | Time-slot tier-based indexing |
| DAG Collections | `strata/src/dagmap/` | DAG-based data structures |
| Encoding | `strata/src/common/ende.rs` | postcard-based KeyEnDe/ValueEnDe |

## Code Review Commands

- `/vs-review` — deep regression analysis (supports: N commits, `all`, hash, range)
- `/vs-debug` — crash/corruption root cause investigation
- `/vs-verify` — validate whether a reported finding is true bug or false positive

Supporting documentation in `.claude/docs/`:
- `technical-patterns.md` — cataloged bug patterns for vsdb + mmdb layers
- `review-core.md` — systematic review methodology
- `false-positive-guide.md` — rules for filtering spurious findings
- `patterns/` — per-subsystem review guides (btree, versioning, trie, slotdex, dagmap, engine)

## Conventions

- All clippy warnings are errors (`#![deny(warnings)]` in lib.rs)
- **No `#[allow(...)]`** — fix warnings at the source, never suppress them
- **No inline paths** — use `use` imports at file top; no `std::foo::Bar::new()` in function bodies. **Exception**: a single-use reference in a file is allowed to stay inline. For multi-use, prefer `use std::mem;` + `mem::take(..)` style (import parent module, not leaf item)
- **Grouped imports** — merge common prefixes: `use std::sync::{Arc, Mutex};`
- **Doc-code alignment** — public API changes must update corresponding docs
- `parking_lot` for Mutex (prefix allocator, VSDB_BASE_DIR global, DagMap ID allocation)
- `VsdbError` (thiserror) for public API errors; `ruc` for internal error chaining
- `postcard` for serialization (replaced serde_cbor_2 in v12)
- Tests run single-threaded; use `tempdir` or `/tmp/vsdb_testing` for isolation
- ~23 unsafe blocks — all require `// SAFETY:` comments
  - `shadow()`: SWMR contract — caller serializes writes
  - `from_bytes()`: caller provides valid serialized bytes
  - Pointer casts in entry API macros
