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

### [REJECTED] engine: "derated cgroup comparison undercuts host when cgroup is not binding"
- **Where**: `core/src/common/engine/mmdb.rs` (`effective_mem_budget`)
- **What**: External review of ca1335f proposed comparing the RAW cgroup limit against the host reading and derating only when the cgroup is the binding constraint (`limit < budget` → `budget = limit*3/4`), on the grounds that `min(host, limit*3/4)` "incorrectly" lets a derated non-binding limit win when `host < limit < 4/3*host`.
- **Reason**: The proposed fix is unsafe: in that band it leaves `budget_limited` unset, and the unconstrained write-buffer path (budget/4 per shard, ×5 worst-case memtables) can overshoot the cgroup line wholesale — host 24G under a 30G `memory.high` would size 30G of worst-case memtables, re-opening the exact v14.0.10 incident class. `budget_limited` is not merely "which number won"; it gates conservative budget-scaled memtable sizing. The min-fold is the documented, deliberate semantic ("detected limits contribute limit×3/4 to the budget min") and is strictly conservative; the "undercut" is a bounded (≤25%, only within that band) safety margin against a standing throttle/kill line, judged against a soft moment-in-time host reading. Now pinned by `effective_mem_budget_semantics` and the function's doc comment.

### [REJECTED] engine: "derating should not apply to memory.max, only memory.high"
- **Where**: `core/src/common/engine/mmdb.rs` (`cgroup_mem_limit_bytes`)
- **What**: External review claimed `memory.max` (and v1 `limit_in_bytes`) are "safe at 100%" because they have no reclaim-stall phase, so the uniform 3/4 derate wastes headroom; suggested tracking limit provenance and derating only throttle lines.
- **Reason**: Sizing engine caches to 100% of the OOM-**kill** line is worse than sitting at the throttle line: crossing it is SIGKILL → dirty store → derived-state rebuild (the very incident ca1335f documents). Under `memory.max` the kernel *does* reclaim/stall before killing once the cgroup is at its limit. Headroom below a hard kill line is more necessary, not less; a uniform conservative derate is correctness-first. Per-provenance derate factors add complexity for marginal cache gains.

### [LOW] engine: 16 GiB write-buffer threshold is a sizing cliff
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`, `legacy_wr`)
- **What**: `legacy_wr` switches from `budget/4/NUM_SHARDS` to a fixed `1G/NUM_SHARDS` floor at the 16 GiB budget boundary, so a budget of 16.1G sizes ~2× the per-shard write buffer of 15.9G; derating/env budgets can move a deployment across the cliff.
- **Reason**: Pre-existing tuning discontinuity, not a correctness issue: the low side of the cliff is the conservative side (smaller buffers → more flushes, never overshoot). Smoothing it changes sizing for every unconstrained host, which is a tuning campaign (bench-validated), not a safe contained fix. Revisit if write-amp on 8-16G budget deployments becomes a measured problem.

### [LOW] engine: budget decisions are not logged at startup
- **Where**: `core/src/common/engine/mmdb.rs` (`MEM_BUDGET`)
- **What**: Operators cannot see which constraint (host reading, cgroup limit, env override) bound the budget or the resulting per-shard sizes without reading the code.
- **Reason**: `vsdb_core` has no logging facade (no `log`/`tracing` dependency) and is a library — unconditional `eprintln!` from a storage engine at first use is worse than silence. Adding a logging dependency is a project-level design decision, not a review fix. The computation is now a pure function with pinned tests, which covers the verifiability concern at development time. Revisit if a logging facade is ever adopted workspace-wide.

### [MEDIUM] vecdex: `compact()` is not atomic across a hard process crash, losing not-yet-reinserted vectors
- **Where**: `strata/src/vecdex/mod.rs` (`compact`)
- **What**: `compact()` collects and dimension-validates all pairs before calling `clear()` (closing the `Result::Err` partial-state path), but `clear()` is irreversible and the re-insertion loop that follows spans many individual mutations. A `kill -9` between `clear()` and loop completion permanently loses every not-yet-reinserted pair. Metadata stays internally consistent (`recover_after_crash` correctly reconciles a valid-but-incomplete graph), so this is data loss, not corruption.
- **Reason**: A true fix requires rebuilding into a fresh set of prefixes and atomically flipping a pointer once the new graph is durably complete (mirroring `DagMapRaw::prune`'s COW re-parenting design). This is not viable as a contained patch: `VecDex` has no indirection layer between its 5 field collections' prefixes and any external reference to them (`save_meta`/`from_meta`, or the value bytes when `VecDex` is nested inside another collection) — those references are captured at a single point in time. Rebuilding into fresh prefixes and swapping `self`'s fields would silently desync any *earlier* `save_meta`/parent-collection snapshot from the live handle, since only a `ValueMut`-mediated write-back (not a bare field swap) propagates a changed prefix set back to such a reference. That failure mode (silent staleness in the common, non-crash case) is worse than the current rare-crash-window data loss it would be trading away. `compact()` is also explicitly documented as a cold, explicit maintenance API, not a hot/warm path. Revisit if `VecDex` ever gains a version-indirection layer for other reasons.
