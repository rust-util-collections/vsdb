# Working on VSDB

## What is this project?

VSDB is a high-performance embedded key-value database for Rust that provides:
- **Persistent collections** with Rust std-like API (Mapx, MapxOrd, Orphan)
- **Git-model versioning** (VerMap) — branches, commits, three-way merge, rollback, GC
- **Merkle tries** (MPT + SMT) for stateless cryptographic commitments
- **Slot-based indexing** (SlotDex) for timestamp-paged queries
- **DAG-based collections** (DagMap) for graph-like data
- **Vector index** (VecDex) — pure-Rust HNSW for approximate nearest-neighbor search; `VecDexDyn` + `MetricKind` select the distance metric at runtime (enum dispatch, metric persisted in meta)
- **Namespaces** — anonymous placement groups: independently-rooted engine instances in one process (own dir/volume, shards, WALs, memory budget); namespace placement via `handle.namespace()` + `new_in`/`ns.scope(..)` (not a same-shard guarantee); cross-ns deep copy via `clone_in(&ns)` (MapxRaw and typed wrappers); in-process `close` (engine teardown; detached snapshots may retain sources; consuming `Namespace::close(self)` returns the handle on refusal) and whole-directory destroy without per-key traversal — the epoch-rotation loop needs no restart

Built exclusively on [mmdb](https://github.com/rust-util-collections/mmdb) (pure-Rust LSM-Tree engine; its bundled Zstd compression backend requires a C toolchain). The default namespace uses 16-shard prefix-based routing (pinned); non-default namespaces persist their own creation-time shard count.

## Workspace Layout

```
vsdb/
├── core/     # vsdb_core — engine integration, MapxRaw, prefix allocation
├── migration/ # vsdb_migrate — offline format conversion and frozen v17 fixtures
└── strata/   # vsdb — typed collections, versioning, tries, slotdex, dagmap, vecdex
```

## Build & Test

```bash
make all          # fmt + lint + test
make test         # cargo test --workspace (release + debug + doc tests, parallel)
make lint         # cargo clippy --workspace + check tests/benches
make bench        # criterion benches (core: basic + cache_pool; strata: basic, versioned, slotdex, slotdex_batch, dagmap, trie_bench, vecdex)
```

**Important**: Tests run in PARALLEL (v16.0.2+). Test data stays disjoint via globally-unique prefixes; tests must not assert on cross-test global state (exact allocator values, registry sizes) and must configure the base dir once per binary (`vsdb_configure` is one-shot; put it behind a `parking_lot::Once`).

`make test` / `make all` / `make bench` are manual/CI convenience targets. They
run under a per-invocation scratch `VSDB_BASE_DIR` and remove only that scratch
directory; they do not touch `$HOME/.vsdb` or caller-provided data. The agent
still uses bare Cargo unless you explicitly ask for a make target. For an
isolated bare-Cargo run, set
`VSDB_BASE_DIR="$(mktemp -d /tmp/vsdb-test.XXXXXX)"` in the parent shell when
launching Cargo; do not mutate environment variables inside parallel tests.

## Architecture

| Subsystem | Key files | Purpose |
|-----------|-----------|---------|
| Engine & Universe | `core/src/common/universe.rs`, `engine/mod.rs`, `mmdb.rs`, `common/{mod,ids,range}.rs` | `VSUNI018`/`VSENG018` headers, advisory `LOCK`, Mapx, single-prefix batches, GLOBAL prefix alloc, shard co-location (`new_colocated`), per-ns shard routing, per-engine `BlockCachePool` |
| Namespaces | `core/src/common/namespace.rs` | Namespace handle (`OnceLock` default), `VSNREG02` registry, `InstanceId`, ambient scope, lifecycle (create/open/close/destroy/relocate); engines owned by `Arc<NsInner>`, no leak |
| MapxRaw | `core/src/basic/mapx_raw/` | Untyped raw KV (`VSMAPX02`), prefix isolation, safe `Entry`, `MapxRawReader` / `MapxRawReadView` |
| Typed Collections | `strata/src/basic/mapx/`, `mapx_ord/`, `mapx_ord_rawkey/`, `orphan/` | `Mapx<K,V>`, `MapxOrd<K,V>`, `MapxOrdRawKey<V>`, `Orphan<T>`, `MapReader` / `*ReadView`, fallible `update` |
| Persistent B+ Tree | `strata/src/basic/persistent_btree/` (`nodes.rs`, `persist.rs`) | `#![forbid(unsafe_code)]` slotted `NodeView`, `[0x10]` pages + `[0x12]` `> 1 KiB` blobs, shared-subtree diff, range-grouped three-way merge, `[0x07]` `REFS` checkpoint |
| Versioning | `strata/src/versioned/` (`store.rs`, `map.rs`, `read.rs`, `repair.rs`) | `#![forbid(unsafe_code)]` single-prefix `VerMap`, owned `'static` `Snapshot`, `[0x05]` `RETIRED` / `[0x06]` `STAGE` / `[0x07]` `REFS`, commit DAG |
| Error types | `core/src/common/error.rs` (re-exported via `vsdb::common::error`) | `VsdbError` + `StorageErrorKind` (thiserror-based, `ruc`-free), unified across all workspace crates |
| Merkle Tries | `strata/src/trie/` (`shared.rs`, `cache.rs`, `proof.rs`, `transport.rs`) | `Arc`-COW MPT (16-ary POPCNT) + SMT (binary 256-bit); map-bound `MPTC`/`SMTC` streaming checkpoints, `VerMapWithProof` |
| Slot Index | `strata/src/slotdex/` (`mod.rs`, `query.rs`, `slot_type.rs`) | Sealed `[u8; N]` fixed-width `SlotType`, tiered counts, `[total, generation]`-checked writes, forward-chunked descending queries |
| DAG Collections | `strata/src/dagmap/` (`typed.rs`, `raw/`, `rawkey/`) | `#![forbid(unsafe_code)]` single-shard co-located overlay trees (`DagMap<K,V>`, `DagMapRawKey<V>`, `DagMapRaw`) with `dag_id` + local edges |
| Vector Index | `strata/src/vecdex/` (`mod.rs`, `hnsw.rs`, `distance.rs`, `reader.rs`) | `VecDex` + `VecDexDyn` (`MetricKind`), LE vector slices, `[META_KEY]` schema binding, monotonic 34B `GraphState`, snapshot search |
| Encoding & Guards | `strata/src/common/{ende,frame,guard,macros,reader,mod}.rs` | `KeyEnDe`/`ValueEnDe`, borrowed `KeyLookup`/`OrderedKeyLookup`, `DerefMut` `ValueGuard`, `VSTYPE04` frame |
| Staged mutation | `strata/src/common/staged.rs` | Read-your-writes overlay + one atomic write batch per mutation/chunk (`SlotDex`/`VecDex`/`DagMap`) |
| Migration | `migration/src/{lib,universe,slotdex,vecdex,versioned,dagmap}.rs` | Independent `vsdb_migrate` crate: offline `v17 → v18` universe, handle, B+ tree, `VerMap`, `SlotDex`, `VecDex`, and 3-phase `DagMap` converters + frozen v17 fixtures |

## Skills

Project skills live under `.agents/skills/<name>/SKILL.md` and are
explicitly invoked with `$x-review`, `$x-fix`, `$x-commit` or `$x-overhaul`.
Invocation arguments are plain prompt text; run the selected skill's preflight
commands explicitly. Each skill's `agents/openai.yaml` preserves explicit-only
selection with `allow_implicit_invocation: false`.

These are local Rust maintenance workflows: review collection results,
storage durability, concurrency, and public/on-disk compatibility; reproduce
confirmed defects with focused regression tests; and apply authorized fixes.
Describe each check in those concrete project terms. Shared scope and evidence
requirements live in [pragmatic-engineering.md](docs/ai/pragmatic-engineering.md) and
[review-core.md](docs/ai/review-core.md).

- [`$x-review`](.agents/skills/x-review/SKILL.md) — regression review. Empty = latest commit. Also: N, `all`, `staged`, `worktree`, hash, range, `--fix`. Code read-only; `docs/audit.md` is the registry exception
- [`$x-fix`](.agents/skills/x-fix/SKILL.md) — clear `docs/audit.md` Open, one finding per commit
- [`$x-commit`](.agents/skills/x-commit/SKILL.md) — review worktree (optional pathspecs) → fix → commit. Unfixed defects go to `docs/audit.md`
- [`$x-overhaul`](.agents/skills/x-overhaul/SKILL.md) — same scopes as `$x-review` (empty = latest commit; `all` = full repo; `staged`/`worktree` commit that work), then fix and local commits

Supporting docs (`docs/ai/`):

- [workflow-policy.md](docs/ai/workflow-policy.md) — worktree safety, one-issue-one-commit
- [pragmatic-engineering.md](docs/ai/pragmatic-engineering.md) — root goal, low variance
- [commit-protocol.md](docs/ai/commit-protocol.md) — validate → commit → resumable version bump and local annotated tag for `$x-commit`, `$x-fix`, and `$x-overhaul`. The release commit updates CHANGELOG.
- [compatibility-policy.md](docs/ai/compatibility-policy.md) — public/on-disk breaks + migration
- [technical-patterns.md](docs/ai/technical-patterns.md) / [design-patterns.md](docs/ai/design-patterns.md) — bug catalog + D-\* design lens
- [review-core.md](docs/ai/review-core.md) — evidence standard + Subsystem Map + audit registry rules/shape (SSOT)
- [false-positive-guide.md](docs/ai/false-positive-guide.md) — suppress noise
- `patterns/*` — per-subsystem checklists

Additional docs in `docs/`:

- `audit.md` — Open / Won't Fix; disproven entries are removed (managed by the four skills; registry writes are not code writes)
- `proposals/v18-roadmap.md` — v18 structural architecture, post-implementation performance audit (§10), and follow-up `v18.x`/`v19` designs
- `proposals/namespaces.md` / `ns-close.md` — current namespace identity, placement, recovery, and teardown contracts
- `proposals/shared-mem-pool.md` — implemented per-engine cache sharing and explicitly deferred extensions
- `proposals/durable-batches-and-read-views.md` — implemented synchronous commits and borrowed read views; typed read views are available on the basic collections and `VecDex`
- `evaluations/v18-{basic,slotdex-dagmap,trie-vecdex,versioned}.md` — v18 Criterion/RSS validation baselines against `v17.0.21` (`da9a325`)
- `evaluations/acc-migration-primitives.md` / `hotmint-v17.md` / `performance-fixes.md` — historical validation evidence, not current-HEAD validation reports

## Conventions

- **Rust toolchains** — use the official latest stable Rust for edits,
  dependency updates, CI and releases. Update/select stable before validation.
  Define no compatible Rust version range or minimum compiler; do not add
  `rust-version`, numeric toolchain pins or older-compiler compatibility shims.
  [compatibility-policy.md](docs/ai/compatibility-policy.md) §Rust toolchain is the rule's source of truth.
- All clippy warnings are errors (`#![deny(warnings)]` in lib.rs)
- **No `#[allow(...)]`** — fix warnings at the source, never suppress them
- **Prefer imports over inline paths** — avoid `std::foo::Bar::new()` inline in function bodies when the same path appears 3+ times in a file; add `use std::foo;` at file top (or `use std::foo::Bar;`) instead. Function-body `use` statements (scoped imports) are fine and don't count as inline paths. 1-2 inline uses of common `std::` items are acceptable.
- **Grouped imports** — merge common prefixes: `use std::sync::{Arc, Mutex};`
- **Doc-code alignment** — public API changes must update corresponding docs
- `parking_lot` for every `Mutex`, `RwLock`, and `Once` in library and test code (prefix allocator, `VSDB_BASE_DIR` global, one-time test setup); do not mix in `std::sync` lock primitives
- Public storage operations use `VsdbError`. Engine failures retain the source and a stable classification.
- Use postcard for handle and application serialization. Keep old-format parsers and converters in the independent `vsdb_migrate` crate.
- Serialize writes through aliases of one composite index. SlotDex detects stale writer generations. VecDex reads graph state per operation and uses snapshots for graph queries.
- Tests run in parallel; isolation comes from globally-unique prefixes (plus `tempdir`/`/tmp/vsdb_testing` for file-level scratch); global-state assertions must be race-tolerant
- Derive the live unsafe inventory during review. A raw or typed map `shadow()` requires exclusion of concurrent writes to the same key. Structural multi-key edits require the collection's documented writer exclusion. Mutable collection `Clone` performs a deep copy.
- **No Co-Authored-By in commits** — never add `Co-Authored-By:` or similar trailers to commit messages; project commits are authored only by the human contributor
