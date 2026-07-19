# Audit Findings

> Auto-managed by /x-review and /x-fix.
>
> **Won't Fix is not permanent.** Re-evaluate an entry when a review touches
> its code, callers, assumptions, or subsystem; a full audit checks every entry.
>
> **Rejected is not Won't Fix.** Rejected entries are disproven recurring
> claims, not deferred defects. Re-check them only when cited code/invariants
> change. Resolved history belongs in Git and CHANGELOG.

## Open

*(none)*

---

## Won't Fix

### [MEDIUM] versioned: three-way merge materializes the merged result in memory
- **Where**: `strata/src/versioned/merge.rs` (`three_way_merge`, `three_way_merge_many_bases`), `strata/src/basic/persistent_btree/mod.rs` (`bulk_load`)
- **What**: merge collects every merged `(key, value)` pair into a `Vec` and `bulk_load` re-collects its input, so `VerMap::merge` peaks at memory proportional to the union of live keys across both branches; merging branches whose combined live data exceeds available memory aborts the process.
- **Reason**: a streaming merge requires an incremental bulk-loader driven by a merged iterator — a substantial B+ tree rewrite not justified until larger-than-RAM merges are a real workload. The transient cost is documented on both public APIs.

---

### [LOW] engine: lazy-delete auto-sweep threshold stays disabled
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`), mmdb `DbOptions::lazy_delete_compaction_threshold`
- **What**: keys registered via `lazy_delete` are physically dropped only when organic compaction rewrites their level; registrations resting in cold levels can outlive the process (registrations are memory-only and documented best-effort).
- **Reason**: mmdb documents the auto-sweep as holding the DB's write-serializing lock for each level's full rewrite — enabling it by default trades unbounded write stalls for space reclaim. Correctness never depends on physical removal (dead B+ tree nodes are unreachable by id), and administrative `compact()` remains available.

---

### [MEDIUM] dagmap: serde decomposition can expose the private parent slot
- **Where**: `strata/src/dagmap/raw/mod.rs:70-127`, `strata/src/basic/orphan/mod.rs:199-224`
- **What**: callers can deserialize `DagMapRaw`'s public tuple representation into its public component types, retain an alias to the private parent `Orphan`, and later create a parent cycle through safe APIs.
- **Reason**: preventing deliberate representation decomposition requires a breaking redesign that no longer serializes recoverable component handles. Current cycle guards bound the result to failed lookups/prune errors rather than hangs or memory unsafety; keep this debt visible until a broader DagMap format redesign is justified.

---

### [LOW] engine: 16 GiB write-buffer threshold is a sizing cliff
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`, `legacy_wr`)
- **What**: `legacy_wr` switches from `budget/4/NUM_SHARDS` to a fixed `1G/NUM_SHARDS` floor at the 16 GiB budget boundary.
- **Reason**: This is a pre-existing tuning discontinuity, not a correctness issue; the low side is conservative. Smoothing it changes sizing for every unconstrained host and requires a dedicated tuning campaign.

---

### [LOW] engine: budget decisions are not logged at startup
- **Where**: `core/src/common/engine/mmdb.rs` (`MEM_BUDGET`)
- **What**: Operators cannot see which constraint bound the budget or the resulting per-shard sizes.
- **Reason**: `vsdb_core` has no logging facade and is a library; unconditional stderr output from a storage engine is worse than silence. Revisit if a workspace-wide logging facade is adopted.

---

## Rejected

### collections: "unbounded growth of caller-retained data is a leak"
- **Where**: `strata/src/versioned/map.rs` (`commit`, `create_branch`, `log`, `list_branches`, `gc`), `strata/src/dagmap/raw/mod.rs`, `strata/src/vecdex/mod.rs`, `strata/src/slotdex/mod.rs`, `strata/src/trie/mod.rs` (`MptCalc`/`SmtCalc`)
- **Claim**: commits, branches, DAG children, HNSW nodes, SlotDex tiers, and in-memory trie keys need built-in caps, TTLs, or auto-expiry; `log`/`list_branches`/`gc` collecting proportional-to-history vectors can OOM.
- **Reason**: collections store exactly what callers insert and retain — a cap turns valid writes into artificial failures, and auto-expiry would silently destroy committed history (unreferenced VerMap commits are already hard-deleted immediately by the `delete_branch`/`rollback_to` ref-count cascade). Traversals, listing, and GC are cold paths doing O(retained data) work; SlotDex tier count is bounded by the slot type's bit width (`floor_base_of` saturates); `MptCalc`/`SmtCalc` are documented in-memory calculators.

---

### engine/strata: "atomic batch staging is unbounded"
- **Where**: `core/src/common/engine/mod.rs` (`BatchTrait`), `core/src/common/engine/mmdb.rs` (`MmdbBatch`), `strata/src/slotdex/mod.rs` (`insert_batch`), `strata/src/vecdex/mod.rs` (`insert_batch`)
- **Claim**: batches accumulate every staged operation in memory with no entry cap, so huge batches OOM.
- **Reason**: staging-then-commit is the documented atomicity contract, and memory is proportional to caller-supplied input under the caller's control (chunk the input for bounded memory). Auto-splitting inside the library would silently break the promised whole-batch atomicity. VecDex chunks internally because its documented contract is per-chunk atomicity; its dedup pass holds references plus encoded keys, again O(input).

---

### namespace/meta: "postcard deserialization of registry/meta files is a memory bomb"
- **Where**: `core/src/common/namespace.rs` (`load_registry`), `strata/src/common/mod.rs` (`load_instance_meta`)
- **Claim**: `fs::read` + `postcard::from_bytes` without a file-size cap lets a crafted registry or instance meta allocate unboundedly.
- **Reason**: postcard parses sequences element-by-element from the input, so decode cost and allocation are bounded by the actual file size (serde caps `Vec` preallocation; a small file cannot decode into millions of records), and file size is proportional to namespaces/instances actually created. Files under the base dir sit inside the process's own trust boundary; malformed bytes yield a clean `Decode` error.

---

### namespace: "the default namespace taxes non-default users; namespace count needs a cap"
- **Where**: `core/src/common/namespace.rs` (`DEFAULT_NS`, `create_with`, `open`)
- **Claim**: `DEFAULT_NS` never drops, so every process pays for the default engine; unlimited `create`/`open` multiplies memory budgets until OOM.
- **Reason**: `DEFAULT_NS` is a `LazyLock` — non-default-only workloads never force it on v16 datasets (the allocator reads the ceiling file directly; the default engine is forced only for pre-v16 migration). Each non-default namespace is an explicit admin-tier act with an explicit, persisted per-namespace budget; a process-level cap would break the supported epoch-rotation pattern while defending only against the operator's own deliberate calls.

---

### engine: "mmdb-inherited defaults leak resources"
- **Where**: mmdb `options.rs`, mmdb `manifest/version_set.rs`
- **Claim**: the MANIFEST grows monotonically without compaction; per-level SST limits are soft; the disabled compaction rate limiter starves foreground reads.
- **Reason**: mmdb rotates the MANIFEST via `maybe_compact_manifest()` (full-snapshot rewrite past an edit threshold), so monotonic growth is factually wrong. L0 backpressure exists (`l0_slowdown_trigger`/`l0_stop_trigger` write stalls). The rate-limiter default is a deliberate tuning choice, and vsdb already bounds compaction to one background thread per shard.

---

### engine: "Drop skips the flush that close() performs"
- **Where**: `core/src/common/engine/mmdb.rs` (`MmDB::close` vs engine drop)
- **Claim**: dropping an engine can lose buffered writes because only `close()` flushes.
- **Reason**: every applied write is WAL-durable before its `put` returns, and mmdb's `DB::drop` additionally syncs the WAL best-effort — a drop-without-close loses no committed data; recovery replays the WAL. `close()` exists to surface sync errors, not to add durability.

---

### engine: "no key/value size validation at the vsdb boundary"
- **Where**: `core/src/basic/mapx_raw/mod.rs` (`insert`), `core/src/common/engine/mmdb.rs` (`MmDB::insert`)
- **Claim**: absent vsdb-side checks, oversized values reach the memtable and OOM.
- **Reason**: mmdb validates every write (8 MiB key cap, ~64 MiB entry cap) before WAL/memtable admission and rejects with a descriptive error; nothing oversized is ever buffered. The boundary behavior is documented on `MapxRaw::insert` and `BatchTrait::commit`: direct ops panic under the fatal-write convention, batch commits surface `Err`.

---

### namespace: "`mem_budget_mb: usize::MAX` overflows sizing arithmetic"
- **Where**: `core/src/common/namespace.rs` (`sizing_for`), `core/src/common/engine/mmdb.rs` (`EngineSizing::from_budget_mb`, `effective_mem_budget`)
- **Claim**: a huge budget overflows the megabyte-to-byte conversion or allocates giant structures up front.
- **Reason**: both paths use `checked_mul`/`saturating_mul` (covered by `effective_mem_budget_semantics`); write buffers stay clamped by the 512 MiB legacy cap, and the block-cache figure is an eviction *limit*, never a preallocation. A giant budget is an explicit operator request for an effectively unbounded cache, not an overflow.

---

### vecdex: "renaming a `VecDexDyn` variant breaks persisted metas"
- **Where**: `strata/src/vecdex/dynamic.rs`
- **Claim**: Postcard persists enum variant names, so renaming `L2` would invalidate saved metadata.
- **Reason**: Postcard is non-self-describing and index/tag based; variant names are not written. `VecDexDyn` now uses explicit frozen wire tags, so source renames do not change the mapping.

---

### namespace: "`close(self)` drops the handle inside the table-lock scope unnecessarily"
- **Where**: `core/src/common/namespace.rs` (`ns_close_impl`)
- **Claim**: The consumed handle should be dropped after releasing `OPEN_NAMESPACES`.
- **Reason**: That drop is the exclusivity-accounting decrement that makes the removed entry the sole strong reference. The slow engine teardown already runs after the table lock is released; `REGISTRY_LOCK` intentionally preserves same-id lifecycle exclusion through teardown.

---

### vecdex: "`dispatch!` bindings can shadow same-named caller variables"
- **Where**: `strata/src/vecdex/dynamic.rs` (`dispatch!`)
- **Claim**: A caller binding could silently replace a query/key argument with the inner `VecDex`.
- **Reason**: The proposed misuse does not type-check; the caller explicitly chooses the binding identifier, with ordinary closure-parameter shadowing semantics. No API parameter has the inner handle type.

---

### engine: "`OnceLock::get_or_init` can run `alloc_prefix` twice under concurrent reads"
- **Where**: `core/src/common/engine/mod.rs` (`Mapx::prefix_bytes`)
- **Claim**: Concurrent readers can run both initializer closures and leak a prefix.
- **Reason**: `OnceLock::get_or_init` executes one initializer; competing callers wait for it. Double allocation cannot occur.

---

### namespace: "`DEFAULT_NS_ID` guards should be one shared helper"
- **Where**: `core/src/common/namespace.rs` (`open`, destroy, relocate, close)
- **Claim**: Similar default-namespace guards are harmful duplication.
- **Reason**: The operations intentionally diverge: open succeeds via `default_ns`, while destroy/relocate/close return distinct actionable errors. A helper would require flags/closures and reduce clarity.

---

### engine: "derated cgroup comparison undercuts host when cgroup is not binding"
- **Where**: `core/src/common/engine/mmdb.rs` (`effective_mem_budget`)
- **Claim**: Derating should occur only when the raw cgroup limit is below host memory.
- **Reason**: That change can leave `budget_limited` unset and let unconstrained write-buffer sizing cross the cgroup limit. The current min-fold is deliberately conservative and covered by semantic tests.

---

### engine: "derating should apply to `memory.high`, not `memory.max`"
- **Where**: `core/src/common/engine/mmdb.rs` (`cgroup_mem_limit_bytes`)
- **Claim**: A hard cgroup maximum is safe to budget at 100%.
- **Reason**: `memory.max` is the OOM-kill boundary and still induces reclaim/stall near the limit. Headroom below a kill line is at least as necessary as below a throttle line.
