# Audit Findings

> Auto-managed by /x-review and /x-fix.
>
> **Won't Fix ≠ permanent.** Every entry under `## Won't Fix` must be
> re-evaluated against the current codebase on each audit. Surrounding code
> changes, new callers, or API evolution may make a previously-disproportionate
> fix straightforward — or make the finding obsolete. Never silently carry
> forward a Won't Fix entry without fresh assessment.

## Open

*(none)*

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

### [MEDIUM] vecdex: `compact()` is not atomic across a hard process crash, losing not-yet-reinserted vectors
- **Where**: `strata/src/vecdex/mod.rs` (`compact`)
- **What**: `compact()` collects and validates all pairs before calling `clear()`, but `clear()` is irreversible and the re-insertion loop spans many individual mutations. A `kill -9` between `clear()` and loop completion permanently loses not-yet-reinserted pairs.
- **Reason**: A true fix requires rebuilding into fresh prefixes and atomically flipping a pointer — but `VecDex` has no indirection layer, and a bare field swap would silently desync any earlier `save_meta`/parent-collection snapshot from the live handle. That failure mode (silent staleness in the common, non-crash case) is worse than the current rare-crash-window data loss. `compact()` is also explicitly documented as a cold, explicit maintenance API.
