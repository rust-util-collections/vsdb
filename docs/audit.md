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

## Won't Fix

### [MEDIUM] dagmap: an unmarked fold left by earlier releases without children is undetectable
- **Where**: `strata/src/dagmap/raw/mod.rs` (`prune`, `inspect_below_head`)
- **What**: Releases before the single-batch prune publication could stop after folding a line into the root and before writing any clearing marker. If the head had no live children to reparent, nothing on disk records that fold. A later prune entering at an ancestor treats the line as unfolded and folds that ancestor's rows over the root, so keys the head had overridden revert to the ancestor's values.
- **Reason**: The state leaves no evidence to detect it, and new prunes publish their markers together with the fold, so they cannot create it. Retrying the original head, the documented recovery, folds the full line again. Detecting old cases would need a migration that compares every root with its lines, which is disproportionate for a crash window in earlier releases.

---

### [MEDIUM] platform support: memory-sizing constants overflow on 32-bit targets

- **Where**: `core/src/common/engine/mmdb.rs` (`effective_mem_budget_semantics`) and both `benches/units/legacy_budget.rs` helpers in `core` and `strata`.
- **What**: The budget test expects `64 * G` bytes in a `usize`. Both benchmark helpers use `1usize << 60` when interpreting a cgroup limit. These expressions overflow on 32-bit targets.
- **Reason**: Both expressions fit on the supported `x86_64` and `aarch64` targets. See the [platform policy](ai/compatibility-policy.md#supported-architectures).

---

### [LOW] engine: lazy-delete auto-sweep threshold stays disabled
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`), mmdb `DbOptions::lazy_delete_compaction_threshold`
- **What**: The default threshold is zero. `Namespace::flush()` flushes memtables and can drain a full L0, but VSDB never requests a dead-key sweep. Registered keys are reclaimed during organic level compaction while the process remains open; a restart loses registrations for rows that compaction has yet to rewrite.
- **Reason**: Enabling automatic sweeps rewrites populated levels and consumes I/O even on a store with no organic compaction debt. MMDB releases the DB mutex while building SSTs. Unreachable B+ tree pages do not affect reads and are dropped as subsequent writes trigger normal compaction. [MMDB 6.0.1 sweep semantics](https://github.com/rust-util-collections/mmdb/blob/80b635a8926ec75fcfb1c9e734940688de30ad2a/src/db.rs#L2812-L2826)

---

### [LOW] engine: budget decisions are not logged at startup
- **Where**: `core/src/common/engine/mmdb.rs` (`MEM_BUDGET`)
- **What**: Startup does not report which configuration source selected the budget. Public callers can inspect the derived cache capacities and per-shard write-buffer sizes via `Namespace::memory_usage()`.
- **Reason**: `vsdb_core` has no logging facade and is a library; unconditional stderr output from a storage engine is worse than silence. Revisit if a workspace-wide logging facade is adopted.

---

### [LOW] benches: fixtures and persisted state are not isolated from measured work
- **Where**: `core/benches/units/batch_write.rs`, `strata/benches/versioned.rs`, `strata/benches/units/concurrent.rs`, `strata/benches/vecdex.rs`
- **What**: Several timed closures build fixtures (fresh maps, prepopulation, rollback/discard state) or reuse a map that grows across thread-count runs; `gc_bench` measures an already-cleaned graph after warmup. The VecDex insert benches time `clear()`, and `bench_insert` also times vector generation. Fresh collections are also never wiped, so storage grows during a run.
- **Reason**: These affect benchmark interpretation and disk usage, not library correctness or durability. Fixing them is a benchmark-tooling overhaul (Criterion `iter_batched` fixtures, per-sample state reset, and bench-owned directory cleanup) that should be validated separately from this correctness pass; the numbers remain comparable within a single run only with that caveat.
