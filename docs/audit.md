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
