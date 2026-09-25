# VSDB — Claude Code Project Guide

## What is this project?

VSDB is a high-performance embedded key-value database for Rust that provides:
- **Persistent collections** with Rust std-like API (Mapx, MapxOrd, Orphan)
- **Git-model versioning** (VerMap) — branches, commits, three-way merge, rollback, GC
- **Merkle tries** (MPT + SMT) for stateless cryptographic commitments
- **Slot-based indexing** (SlotDex) for timestamp-paged queries
- **DAG-based collections** (DagMap) for graph-like data
- **Vector index** (VecDex) — pure-Rust HNSW for approximate nearest-neighbor search; `VecDexDyn` + `MetricKind` select the distance metric at runtime (enum dispatch, metric persisted in meta)
- **Namespaces** — anonymous placement groups: independently-rooted engine instances in one process (own dir/volume, shards, WALs, memory budget); namespace placement via `handle.namespace()` + `new_in`/`ns.scope(..)` (not a same-shard guarantee); cross-ns deep copy via `clone_in(&ns)` (MapxRaw and typed wrappers); in-process `close` (engine teardown; detached snapshots may retain sources; consuming `Namespace::close(self)` returns the handle on refusal) and whole-directory destroy without per-key traversal — the epoch-rotation loop needs no restart

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

**Important**: Tests run in PARALLEL (v16.0.2+). Test data stays disjoint via globally-unique prefixes; tests must not assert on cross-test global state (exact allocator values, registry sizes) and must configure the base dir once per binary (`vsdb_configure` is one-shot; put it behind a `Once`).

`make test` / `make all` / `make bench` are manual/CI convenience targets and
perform global cleanup. The agent uses bare Cargo unless you explicitly ask for
a make target, so it does not delete `$HOME/.vsdb` or shared `/tmp/vsdb_testing`. For an
isolated run, set `VSDB_BASE_DIR="$(mktemp -d /tmp/vsdb-test.XXXXXX)"` in the
parent shell when launching Cargo; do not mutate environment variables inside
parallel tests.

## Architecture

| Subsystem | Key files | Purpose |
|-----------|-----------|---------|
| Engine | `core/src/common/engine/mod.rs`, `mmdb.rs` | Mapx, single-prefix batches, GLOBAL prefix alloc (all namespaces), shard co-location (`new_colocated`), per-ns shard routing, per-engine block-cache pool (shards share one `BlockCachePool`), format marker |
| Namespaces | `core/src/common/namespace.rs` | Namespace handle, registry, InstanceId, ambient scope, lifecycle (create/open/close/destroy/relocate); engines owned by `Arc<NsInner>`, no leak |
| MapxRaw | `core/src/basic/mapx_raw/` | Untyped raw KV, prefix isolation |
| Typed Collections | `strata/src/basic/mapx/`, `mapx_ord/`, `mapx_ord_rawkey/`, `orphan/` | Mapx<K,V>, MapxOrd<K,V>, MapxOrdRawKey<V>, Orphan<T> |
| Persistent B+ Tree | `strata/src/basic/persistent_btree/` | COW B+ tree, structural sharing, shared-subtree diff + delta-replay three-way merge, deferred reclamation |
| Versioning | `strata/src/versioned/` | VerMap (components co-located on one shard WAL), Snapshot views, typed ids/diffs, commit DAG |
| Error types | `core/src/common/error.rs` (re-exported via `vsdb::common::error`) | VsdbError enum (thiserror-based), unified across both crates |
| Merkle Tries | `strata/src/trie/` | MPT (16-ary) + SMT (binary 256-bit); explicit cache checkpoints, automatic load, in-memory fallback |
| Slot Index | `strata/src/slotdex/` | Time-slot tier-based indexing (single-handle, crash-atomic) |
| DAG Collections | `strata/src/dagmap/` | Single-parent overlay trees (`DagMapRaw` / `DagMapRawKey`): child reads fall back to ancestors; prune folds a mainline |
| Vector Index | `strata/src/vecdex/` | VecDex + VecDexDyn (runtime `MetricKind`), HNSW ANN search, distance metrics (single-handle, crash-atomic) |
| Encoding | `strata/src/common/ende.rs`, `common/mod.rs` | postcard-based KeyEnDe/ValueEnDe, borrowed-key `KeyRef`/`OrderedKeyRef`, typed-handle envelope (`VSTYPE03`, reads `VSTYPE02`) |
| Staged mutation | `strata/src/common/staged.rs` | read-your-writes overlay + one atomic write batch per mutation/chunk (SlotDex/VecDex) |

## Skills

Project skills live under `.claude/skills/<name>/SKILL.md` and are
user-invocable only.

- `/x-review` — regression review. Empty = latest commit. Also: N, `all`, `staged`, `worktree`, hash, range, `--fix`. Code read-only; `docs/audit.md` is the registry exception
- `/x-fix` — clear `docs/audit.md` Open, one finding per commit; resumable patch bump, no tag
- `/x-commit` — review worktree (optional pathspecs) → fix → commit. Unfixed defects go to `docs/audit.md`
- `/x-overhaul` — same scopes as `/x-review` (empty = latest commit; `all` = full repo; `staged`/`worktree` commit that work), then fix and local commits

Supporting docs (`.claude/docs/`):
- `workflow-policy.md` — worktree safety, one-issue-one-commit
- `pragmatic-engineering.md` — root goal, low variance
- `commit-protocol.md` — validate → commit → resumable patch bump (no tag, no autonomous major); CHANGELOG written by the release commit
- `compatibility-policy.md` — public/on-disk breaks + migration
- `technical-patterns.md` / `design-patterns.md` — bug catalog + D-\* design lens
- `review-core.md` — evidence standard + Subsystem Map + audit registry rules/shape (SSOT)
- `false-positive-guide.md` — suppress noise
- `patterns/*` — per-subsystem checklists

Additional docs in `docs/`:
- `audit.md` — Open / Won't Fix; disproven entries are removed (managed by the four skills; registry writes are not code writes)
- `proposals/namespaces.md` / `ns-close.md` — current namespace identity, placement, recovery, and teardown contracts
- `proposals/shared-mem-pool.md` — implemented per-engine cache sharing and explicitly deferred extensions
- `evaluations/hotmint-v17.md` — historical v17.0.3 integration evidence, not a current-HEAD validation report

## Conventions

- All clippy warnings are errors (`#![deny(warnings)]` in lib.rs)
- **No `#[allow(...)]`** — fix warnings at the source, never suppress them
- **Prefer imports over inline paths** — avoid `std::foo::Bar::new()` inline in function bodies when the same path appears 3+ times in a file; add `use std::foo;` at file top (or `use std::foo::Bar;`) instead. Function-body `use` statements (scoped imports) are fine and don't count as inline paths. 1-2 inline uses of common `std::` items are acceptable.
- **Grouped imports** — merge common prefixes: `use std::sync::{Arc, Mutex};`
- **Doc-code alignment** — public API changes must update corresponding docs
- `parking_lot` for Mutex (prefix allocator, VSDB_BASE_DIR global, DagMap ID allocation)
- `VsdbError` (thiserror, defined in `vsdb_core`) is the **only** error type in public APIs of both crates; `ruc` is internal-only for error chaining — boundary conversions preserve the complete chain via `stringify_chain`
- `postcard` for serialization (replaced serde_cbor_2 in v12)
- SlotDex/VecDex recovery creates independent runtime caches: retire the old active handle; route all reads/writes during mutation through one shared instance. Sequential writes through independently restored handles are also unsafe for index consistency.
- Tests run in parallel; isolation comes from globally-unique prefixes (plus `tempdir`/`/tmp/vsdb_testing` for file-level scratch); global-state assertions must be race-tolerant
- Unsafe code is concentrated in handle reconstruction/shadowing, entry APIs,
  DagMap aliases, trie internals, and concurrent benches; derive the live
  inventory during review and require accurate `// SAFETY:` contracts
  - `shadow()`: aliasing contract — no concurrent writes to the same storage
    key; disjoint-key raw/typed map writes are allowed, while structural
    multi-key operations require their documented broader serialization
    (`Clone` deep-copies storage)
  - `MapxRaw::from_bytes_in()` (core raw layer only; typed wrappers have
    no raw-byte constructors): caller provides a valid uniquely-owned prefix
    issued by this universe, in the named namespace
  - Pointer casts in entry API macros
- **No Co-Authored-By in commits** — never add `Co-Authored-By:` or similar trailers to commit messages; project commits are authored only by the human contributor
