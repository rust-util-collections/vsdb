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

### [MEDIUM] platform support: memory-sizing constants overflow on 32-bit targets

- **Where**: `core/src/common/engine/mmdb.rs` (`effective_mem_budget_semantics`) and both `benches/units/legacy_budget.rs` helpers in `core` and `strata`.
- **What**: The budget test expects `64 * G` bytes in a `usize`. Both benchmark helpers use `1usize << 60` when interpreting a cgroup limit. These expressions overflow on 32-bit targets.
- **Reason**: Both expressions fit on the supported `x86_64` and `aarch64` targets. See the [platform policy](ai/compatibility-policy.md#supported-architectures).

---

### [LOW] engine: lazy-delete auto-sweep threshold stays disabled
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`), mmdb `DbOptions::lazy_delete_compaction_threshold`
- **What**: keys registered via `lazy_delete` are physically dropped only when organic compaction rewrites their level; registrations resting in cold levels can outlive the process (registrations are memory-only and documented best-effort).
- **Reason**: mmdb documents the auto-sweep as holding the DB mutex for each level's full rewrite, stalling readers and writers; administrative `compact()` additionally holds the write queue for its full call. Enabling it by default trades unbounded stalls for space reclaim. Correctness never depends on physical removal (dead B+ tree nodes are unreachable by id), and administrative `compact()` remains available.

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
