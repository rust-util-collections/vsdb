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

### [CRITICAL] engine: the MMDB dependency permits known unsafe group-commit and snapshot failures
- **Where**: root `Cargo.toml` (`mmdb` workspace dependency), `core/src/common/engine/mmdb.rs`
- **What**: the current minimum permits MMDB 4.3.1, whose unlocked group-commit fault paths form a whole-request mutable reference while a woken writer can read `done`. Upstream commit `f7fc6cf` fixes this aliasing violation; releases before 4.3.4 also retain snapshot-filter installation and immediate reopen defects relevant to VSDB's views and namespace rotation.
- **Why**: ordinary writers on separate collections can share the affected shard. VSDB's synchronization and snapshot promises cannot exclude these dependency defects.
- **Suggested fix**: require an MMDB release containing the upstream fixes, check public and persisted compatibility, and validate concurrent batches, views, close/reopen and existing-data restoration.

---

### [CRITICAL] storage: directory creation is not covered by durable publication
- **Where**: `core/src/common/mod.rs`, `core/src/common/engine/mmdb.rs`, `core/src/common/namespace.rs`, raw and typed metadata saves
- **What**: first metadata save and engine/namespace initialization create directories, then synchronize files and their immediate containing directories without synchronizing the ancestors that contain the newly created directory names.
- **Why**: a successful metadata save or established namespace can still lose a directory link on power loss. Syscall traces confirm the missing parent-directory synchronization; no physical power-loss experiment was performed.
- **Suggested fix**: use durable directory creation before publishing dependent files or completed namespace state, including retries and concurrent creation. Verify syscall ordering and propagation of directory-sync failures; preserve read-only access and all wire formats.

---

### [HIGH] versioned: raw diffs traverse roots without retaining reader ownership
- **Where**: `strata/src/versioned/map.rs` (`raw_diff_commits`, `raw_diff_uncommitted`)
- **What**: diffs capture bare tree roots. A restored writer alias can discard working state or delete history, settle retired nodes, and let compaction remove those roots before diff traversal finishes.
- **Why**: immutable alias reads are supported, but the diff paths omit the root leases used by ordinary snapshots and can panic on a missing node.
- **Suggested fix**: capture and retain both roots under the shared reference lock through traversal. Add deterministic alias-reclamation regressions and check balanced references; no API or format change.

---

### [MEDIUM] dagmap: typed mutable guards discard interior edits
- **Where**: `strata/src/dagmap/rawkey/mod.rs` (`get_mut`, `ValueMut::drop`)
- **What**: a `DagMapRawKey<RefCell<u32>>` guard modified through `borrow_mut` never sets the `DerefMut` dirty flag, so dropping it loses the edit.
- **Why**: the same supported value and edit persist through ordinary typed-map and Orphan guards. A standalone public-API regression returns the original value instead of the changed value.
- **Suggested fix**: detect stable encoding changes while retaining explicit edits, untouched unstable encodings, tombstone checks and unwind suppression. Add interior-edit and non-mutating guard coverage; no wire change.

---

### [MEDIUM] vecdex: valid neighbor caps can panic before candidate selection
- **Where**: `strata/src/vecdex/hnsw.rs` (`select_neighbors_heuristic`)
- **What**: both selection buffers reserve the configured cap before bounding it by candidate count. A valid `m_max0 = usize::MAX` configuration panics on the second insertion although only one candidate exists.
- **Why**: accepted configuration values cause capacity overflow or excessive allocation without a corresponding workload.
- **Suggested fix**: reserve at most `min(cap, candidates.len())`, and test extreme accepted caps through insertion and search. Selection semantics and stored bytes stay unchanged.

---

### [LOW] indexes: mutation docs promise unchanged recovered state after any error
- **Where**: `strata/src/slotdex/mod.rs`, `strata/src/vecdex/mod.rs` (mutation error documentation)
- **What**: these docs say a failed batch never changes disk state, while complete WAL records can survive a persistence error and replay after reopening.
- **Why**: callers can mistake an ambiguous persistence failure for a safe retry. Cache publication after success does not establish a recovered-disk no-op.
- **Suggested fix**: distinguish unapplied preflight rejection from ambiguous I/O failure, state cache publication behavior, and discourage automatic retries; align the public guides.

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
- **What**: Operators cannot see whether the configured option, environment override or fixed default selected the budget, or the resulting per-shard sizes.
- **Reason**: `vsdb_core` has no logging facade and is a library; unconditional stderr output from a storage engine is worse than silence. Revisit if a workspace-wide logging facade is adopted.
