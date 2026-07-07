# Audit Findings

> Auto-managed by /x-review and /x-fix.
>
> **Won't Fix ≠ permanent.** Every entry under `## Won't Fix` must be
> re-evaluated against the current codebase on each audit. Surrounding code
> changes, new callers, or API evolution may make a previously-disproportionate
> fix straightforward — or make the finding obsolete. Never silently carry
> forward a Won't Fix entry without fresh assessment.

## Open

*(empty)*

---

## Resolved

### [LOW] namespace: create_with consumes id on engine-open failure with no automatic rollback
- **Where**: `core/src/common/namespace.rs` (`create_with`)
- **Resolution**: Non-crash engine-open failures now roll the just-persisted
  registry entry back inline (under the already-held `REGISTRY_LOCK`) before
  propagating the error — a failed `create` leaves no registry residue.
  `next_id` deliberately stays advanced (ids are never reused; a burnt id is
  free). A failed rollback write degrades to the crash-equivalent state that
  was already documented safe: entry visible in `vsdb_ns_list()`, re-openable,
  destroyable. Covered by `namespace_lifecycle` (failed-create rollback
  sub-scenario).

### [LOW] engine: missing `// SAFETY:` comment on inner unsafe block in `from_prefix_slice`
- **Where**: `core/src/common/engine/mod.rs` (`from_prefix_slice`)
- **Resolution**: Added the per-block `// SAFETY:` comment (forwards the fn's
  contract verbatim), matching the repo convention that every `unsafe {}`
  block carries its own justification.

---

## Won't Fix

### [REJECTED] dagmap: "public `Orphan::get_mut()` allows cycle creation"
- **Where**: `strata/src/dagmap/raw/mod.rs` (`parent` field), `strata/src/basic/orphan/mod.rs` (`get_mut`)
- **What**: Claimed that because `parent` is an `Orphan<Option<DagMapRaw>>` and `Orphan` has a public `get_mut()`, users can re-parent a node onto its own descendant in safe code.
- **Reason**: False premise — the `parent` field is private to the `dagmap::raw` module and no public API returns it (or any handle to it): `new/get/get_mut(key)/insert/remove/prune*/destroy` never expose the parent `Orphan`. `Orphan::get_mut` being public is irrelevant without access to the field. The only external route is deliberately reinterpreting a serialized payload as a different type, which is the generic serde-aliasing hazard already documented on the deserialization impls, not a dagmap-specific hole. Parent-chain cycle *tolerance* (lookups degrade to `None`, `prune` errors) remains covered by existing cycle guards and tests.

### [REJECTED] engine: "derated cgroup comparison undercuts host when cgroup is not binding"
- **Where**: `core/src/common/engine/mmdb.rs` (`effective_mem_budget`)
- **What**: External review of ca1335f proposed comparing the RAW cgroup limit against the host reading and derating only when the cgroup is the binding constraint.
- **Reason**: The proposed fix is unsafe: in that band it leaves `budget_limited` unset, and the unconstrained write-buffer path can overshoot the cgroup line wholesale. `budget_limited` is not merely "which number won"; it gates conservative budget-scaled memtable sizing. The min-fold is the documented, deliberate semantic and is strictly conservative. Now pinned by `effective_mem_budget_semantics` and the function's doc comment.

### [REJECTED] engine: "derating should not apply to memory.max, only memory.high"
- **Where**: `core/src/common/engine/mmdb.rs` (`cgroup_mem_limit_bytes`)
- **What**: External review claimed `memory.max` (and v1 `limit_in_bytes`) are "safe at 100%" because they have no reclaim-stall phase.
- **Reason**: Sizing engine caches to 100% of the OOM-kill line is worse than sitting at the throttle line: crossing it is SIGKILL → dirty store → derived-state rebuild. Under `memory.max` the kernel *does* reclaim/stall before killing once the cgroup is at its limit. Headroom below a hard kill line is more necessary, not less.

### [LOW] engine: 16 GiB write-buffer threshold is a sizing cliff
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`, `legacy_wr`)
- **What**: `legacy_wr` switches from `budget/4/NUM_SHARDS` to a fixed `1G/NUM_SHARDS` floor at the 16 GiB budget boundary.
- **Reason**: Pre-existing tuning discontinuity, not a correctness issue. The low side of the cliff is the conservative side. Smoothing it changes sizing for every unconstrained host, which is a tuning campaign.

### [LOW] engine: budget decisions are not logged at startup
- **Where**: `core/src/common/engine/mmdb.rs` (`MEM_BUDGET`)
- **What**: Operators cannot see which constraint bound the budget or the resulting per-shard sizes.
- **Reason**: `vsdb_core` has no logging facade and is a library — unconditional `eprintln!` from a storage engine at first use is worse than silence. Revisit if a logging facade is ever adopted workspace-wide.
