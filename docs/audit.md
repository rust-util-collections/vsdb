# Audit Findings

> Auto-managed by $x-review, $x-fix, $x-commit, and $x-overhaul.
> Registry edits are not code writes.
>
> **Won't Fix is not permanent.** Re-evaluate an entry when a review touches
> its code, callers, assumptions, or subsystem; a full audit checks every entry.
>
> Disproven findings are removed rather than kept as backlog.
> Resolved history belongs in Git and CHANGELOG.

## Open

### [HIGH] persistent_btree: BLOB_LEAF mutations and diffs re-read and re-allocate all unchanged >1 KiB sibling blobs
- **Where**: `strata/src/basic/persistent_btree/mod.rs` (`PersistentBTree::{alloc, node}`), `nodes.rs` (`NodeView::materialize`, `Node::encode_with`), `insert.rs` (`insert_rec`), `remove.rs` (`remove_rec`, `fix_underflow`), `merge.rs` (`apply_leaf_run`), `diff.rs` (`Cursor::expand`)
- **What**: `self.node(id)` eagerly reads all up to 32 `[0x12]` `BLOB` objects of a `BLOB_LEAF` into `Node::Leaf { values: Vec<Vec<u8>> }`, and `self.alloc` unconditionally allocates fresh `BLOB` IDs for every value `> 1024` bytes instead of preserving unchanged `BLOB` descriptors (`desc & CLASS_MASK == BLOB`). `fix_underflow` also calls `self.node(sibling).key_count()`, reading all 32 sibling blobs just to inspect `raw[1]`, and `BTreeDiffIter` reads all 32 blobs on both sides of a modified `BLOB_LEAF` before comparing entries.
- **Why**: Up to 32× write, read, and multi-version space amplification on hot/warm B+ tree paths whenever values exceed 1,024 bytes: on a 1,024-key `VerMap`, crossing `1,024 B → 1,025 B` (+1 byte) jumps `VerMap::insert` from `66.1 µs` to `101.8 µs` and 1-key `diff` from `19.9 µs` to `51.9 µs`; at `64 KiB` values, 40 single-key working edits write ~80 MiB of duplicate blobs instead of ~2.5 MiB (`2.05 ms` median / `53.3 ms` p99 per unsynced `insert`, `56.0 ms` 40-edit `diff`), and unchanged sibling blobs are duplicated across retained `VerMap` commits instead of shared (violates BT4 structural sharing for `[0x12]` blobs).
- **Suggested fix**: Represent leaf values during COW/diff as `LeafValue::Inline(Vec<u8>) | LeafValue::Blob(NodeId)` (materializing blob bytes only when comparing a single overwritten key or emitting a `Added`/`Removed`/`Modified` diff entry whose `NodeId`s differ), and read `NodeView::open(&self.object(sibling)).len` in `fix_underflow` without materializing blobs. Add a regression test asserting `next_id` advances by `height + 2` (1 new blob + path pages, not 32 blobs) on a single-key `BLOB_LEAF` update and `NODE_READS` stays $O(\text{height})$ on `diff`. On-disk `[0x10]`/`[0x12]` page format is unchanged (no migration needed).

---

### [MEDIUM] versioned/persistent_btree: aborted >16 MiB staged builds strand STAGE anchors and flushed chunks on disk
- **Where**: `strata/src/basic/persistent_btree/persist.rs` (`PersistentBTree::{abandon_pending, flush_stage, publish}`), `strata/src/versioned/map.rs` (`VerMap::build_root`)
- **What**: When `VerMap::bulk_load` or `VerMap::merge` crosses 16 MiB, `flush_stage()` commits `0x10`/`0x12` chunks and `[0x06, root_id]` (`STAGE`) anchors to disk. If the build later errors (e.g. an unsorted key in `bulk_load`) or unwinds, `abandon_pending()` clears `self.stage` in memory without removing or deferring removal of those `[0x06]` anchors or private `0x10`/`0x12` objects.
- **Why**: Flushed 16 MiB chunks leak on disk for the session, and the stranded `[0x06]` `STAGE` rows force `has_anchors = true` on reopen (`repair.rs`), invalidating `[0x07]` `REFS` checkpoints written by `flush()` / `close()` and forcing a full tree recount (violates V5 / FP-14).
- **Suggested fix**: Capture `let start = self.tree.next_id();` before `build` in `VerMap::build_root`; on pre-publication error/unwind, queue `row(STAGE, id)` from `self.stage` and flushed private objects with serial `>= start` into `refs.deferred` (or delete them in a cleanup batch). Add a >16 MiB failing `bulk_load` + `close()` + `from_meta()` regression test in `strata/src/versioned/test.rs`. No migration impact.

---

### [LOW] docs/conventions: stale v17 rustdocs, missing v18 breaking-API notes, ValueMut Debug name, and repeated inline std paths
- **Where**: `strata/src/versioned/map.rs` (`VerMap` rustdoc), `core/src/common/mod.rs` (`vsdb_get_system_dir`), `core/src/basic/mapx_raw/mod.rs` (`MapxRaw::namespace`), `core/src/common/engine/mod.rs` (`ValueMut::fmt`), `CHANGELOG.md`, `migration/src/dagmap.rs`, `core/src/basic/mapx_raw/test.rs`
- **What**: `VerMap` rustdoc still describes v17 multi-prefix layout, refcount dirty markers, and `lazy_delete`; `vsdb_get_system_dir` still claims `{base_dir}/__SYSTEM__/` holds the removed DagMap ID counter; `MapxRaw::namespace` calls `namespace()` "THE co-location primitive" (contradicting `new_colocated` and `AGENTS.md`); `CHANGELOG.md` `[v18.0.0]` omits concrete signature breaks (`VecDexDyn::new` → `new_with_metric`, `VerMap::sync` removal, `TrieCalc` checkpoint args, `MapxOrdRawKey::entry` enum); `engine::ValueMut::fmt` formats `"ValueIterMut"`; `std::any::type_name` (`migration/src/dagmap.rs`) and `std::path::Path::new` (`core/src/basic/mapx_raw/test.rs`) appear 3+ times inline.
- **Why**: Misleads callers on v18 durability/placement contracts and v17→v18 API migration, and violates `AGENTS.md` doc-alignment and import rules.
- **Suggested fix**: Update the rustdoc comments, `CHANGELOG.md` `[v18.0.0]` notes, `ValueMut` `Debug` struct name, and top-level `use` imports.

## Won't Fix

### [LOW] engine: lazy-delete auto-sweep threshold stays disabled
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`), mmdb `DbOptions::lazy_delete_compaction_threshold`
- **What**: keys registered via `lazy_delete` are physically dropped only when organic compaction rewrites their level; registrations resting in cold levels can outlive the process (registrations are memory-only and documented best-effort).
- **Reason**: mmdb documents the auto-sweep as holding the DB mutex for each level's full rewrite, stalling readers and writers; administrative `compact()` additionally holds the write queue for its full call. Enabling it by default trades unbounded stalls for space reclaim. Correctness never depends on physical removal (dead B+ tree nodes are unreachable by id), and administrative `compact()` remains available.

---

### [LOW] engine: 16 GiB write-buffer threshold is a sizing cliff
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`, `legacy_wr`)
- **What**: `legacy_wr` uses `1 GiB / shards` up to and including a 16 GiB budget, then switches to `budget / 4 / shards`; the later `budget / 8 / shards` clamp makes a default 16-shard active buffer jump from 64 MiB to approximately 128 MiB just above the boundary.
- **Reason**: This is a pre-existing tuning discontinuity, not a correctness issue; the low side is conservative. Smoothing it changes sizing for every unconstrained host and requires a dedicated tuning campaign.

---

### [LOW] engine: budget decisions are not logged at startup
- **Where**: `core/src/common/engine/mmdb.rs` (`MEM_BUDGET`)
- **What**: Operators cannot see whether the configured option, environment override or fixed default selected the budget, or the resulting per-shard sizes.
- **Reason**: `vsdb_core` has no logging facade and is a library; unconditional stderr output from a storage engine is worse than silence. Revisit if a workspace-wide logging facade is adopted.

---

### [LOW] benches: fixtures and persisted state are not isolated from measured work
- **Where**: `core/benches/units/batch_write.rs`, `strata/benches/versioned.rs`, `strata/benches/units/concurrent.rs`
- **What**: Several timed closures build fixtures (fresh maps, prepopulation, rollback/discard state) or reuse a map that grows across thread-count runs; `gc_bench` measures an already-cleaned graph after warmup. Fresh collections are also never wiped, so storage grows during a run.
- **Reason**: These affect benchmark interpretation and disk usage, not library correctness or durability. Fixing them is a benchmark-tooling overhaul (Criterion `iter_batched` fixtures, per-sample state reset, and bench-owned directory cleanup) that should be validated separately from this correctness pass; the numbers remain comparable within a single run only with that caveat.
