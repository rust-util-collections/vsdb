# Audit Findings

> Auto-managed by /x-review, /x-fix, /x-commit, and /x-overhaul.
> Registry edits are not code writes.
>
> **Won't Fix is not permanent.** Re-evaluate an entry when a review touches
> its code, callers, assumptions, or subsystem; a full audit checks every entry.
>
> Disproven findings are removed rather than kept as backlog.
> Resolved history belongs in Git and CHANGELOG.

## Open

None.

## Won't Fix

### [MEDIUM] cached indexes: independently restored handles do not share runtime caches
- **Where**: `strata/src/slotdex/mod.rs`, `strata/src/vecdex/mod.rs`, `strata/src/vecdex/dynamic.rs`
- **What**: serde/from_meta restores the same backing store with separate cached totals, tiers or graph state. Alternating writes through those handles can overwrite counters or node IDs; independent readers can observe stale cached state after another handle mutates.
- **Reason**: sharing or invalidating all generic cached state requires a runtime ownership/cache redesign, or reloading index state on ordinary operations with material cost. Recovery is intended to replace the active handle. Public struct and recovery docs now require all access during mutation to use one live instance (shared behind a lock when needed); multiple independent immutable readers remain supported. This documents the current limitation rather than claiming alias coherence is fixed.

---

### [LOW] engine: lazy-delete auto-sweep threshold stays disabled
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`), mmdb `DbOptions::lazy_delete_compaction_threshold`
- **What**: keys registered via `lazy_delete` are physically dropped only when organic compaction rewrites their level; registrations resting in cold levels can outlive the process (registrations are memory-only and documented best-effort).
- **Reason**: mmdb documents the auto-sweep as holding the DB's write-serializing lock for each level's full rewrite — enabling it by default trades unbounded write stalls for space reclaim. Correctness never depends on physical removal (dead B+ tree nodes are unreachable by id), and administrative `compact()` remains available.

---

### [MEDIUM] dagmap: serde decomposition can expose the private parent slot
- **Where**: `strata/src/dagmap/raw/mod.rs`, `strata/src/basic/orphan/mod.rs`
- **What**: callers can deserialize `DagMapRaw`'s public tuple representation into its public component types, retain an alias to the private parent `Orphan`, and later create a parent cycle through safe APIs. The same deliberate decomposition can also plant a foreign registry entry that `owned_or_residue`'s `None`-parent residue arm treats as reclaimable, so a later destroy/prune walk can wipe a live root (`parent == None`) — value loss under representation abuse, not hang/UB.
- **Reason**: stopping deliberate decomposition requires a handle format that old bytes cannot decode into the public component types. A tombstone that treats `None` as residue only when present would stop the planted-root wipe, but a crash on the current build can already have `parent == None` with data still intact and no tombstone; requiring the tombstone would skip that residue and, if the registry entry is then dropped, make it unreachable. That recovery regression is not an acceptable silent patch. Casual misuse still cannot hang or cause memory unsafety. Keep the debt until a DagMap format redesign.

---

### [LOW] engine: 16 GiB write-buffer threshold is a sizing cliff
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`, `legacy_wr`)
- **What**: `legacy_wr` uses `1 GiB / shards` up to and including a 16 GiB budget, then switches to `budget / 4 / shards`; the later `budget / 8 / shards` clamp makes a default 16-shard active buffer jump from 64 MiB to approximately 128 MiB just above the boundary.
- **Reason**: This is a pre-existing tuning discontinuity, not a correctness issue; the low side is conservative. Smoothing it changes sizing for every unconstrained host and requires a dedicated tuning campaign.

---

### [LOW] engine: budget decisions are not logged at startup
- **Where**: `core/src/common/engine/mmdb.rs` (`MEM_BUDGET`)
- **What**: Operators cannot see which constraint bound the budget or the resulting per-shard sizes.
- **Reason**: `vsdb_core` has no logging facade and is a library; unconditional stderr output from a storage engine is worse than silence. Revisit if a workspace-wide logging facade is adopted.
