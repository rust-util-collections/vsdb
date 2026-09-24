# Audit Findings

> Auto-managed by /x-review, /x-fix, /x-commit, and /x-overhaul.
> Registry edits are not code writes.
>
> **Won't Fix is not permanent.** Re-evaluate an entry when a review touches
> its code, callers, assumptions, or subsystem; a full audit checks every entry.
>
> **Rejected is not Won't Fix.** Rejected entries are disproven recurring
> claims, not deferred defects. Re-check them only when cited code/invariants
> change. Resolved history belongs in Git and CHANGELOG.

## Open

### [HIGH] collections: Mapx slice queries against `[u8; N]` keys miss or delete a different key
- **Where**: `strata/src/basic/mapx/mod.rs` (`get`, `get_mut`, `contains_key`, `remove`)
- **What**: `Mapx<[u8; N], V>` stores `K::encode()` (postcard tuple: N raw bytes). Those methods accept `K: Borrow<Q>, Q: KeyRef`, and `[u8; N]: Borrow<[u8]>`, so a slice calls `[u8]::key_bytes` (postcard byte string: varint length plus bytes). An equal-length slice always misses. A shorter slice can name another key: `insert(&[0x01, 0x00], …)` then `get`/`remove` of `&[0x00]` addresses that entry, because both encodings are `01 00`. `get_mut` returns a write-back guard for that other key.
- **Why**: technical-patterns 3.5. The method docs advertise borrowed lookup like `HashMap::get`, and `KeyEn` calls fixed arrays safe keys. `HashMap` compares through `Borrow`; this bound does not prove encoding identity. `KeyRef for [u8]` is correct for `Vec<u8>` and `Box<[u8]>` only. `MapxOrd` is unaffected (both sides are raw bytes). Owned `&[u8; N]` lookups already match. Batch insert/remove take `&K` and use `encode()`. No on-disk layout change.
- **Suggested fix**: Stop treating unconstrained `Borrow + KeyRef` as encoding identity. For `[u8; N]`, an equal-length slice is the raw N bytes; any other length is a miss and must not be length-prefixed (that encoding is what collides). Keep `String`/`str` and `Vec<u8>`/`[u8]`. Regression: `Mapx<[u8; 2], u32>` — `get(&[0x01, 0x00][..])` hits; `get`/`remove` of `&[0x00][..]` does not see or delete `[0x01, 0x00]`. Bugfix only; no migration.

---

### [HIGH] dagmap: prune retry treats a partial clearing-marker set as finished
- **Where**: `strata/src/dagmap/raw/mod.rs` (`mark_consumed_clearing`, `prune`, `finish_interrupted_clear`, `survivor_ids`)
- **What**: `mark_consumed_clearing` writes the head marker, then each intermediate, as separate puts. A default put flushes the WAL to the OS before returning, so `kill -9` after the head `set_aux` returns and before the next `set_aux` is issued recovers only the head marker. Parent slots are still intact (clear has not started; the re-parent flush already returned). `prune` then takes `finish_interrupted_clear` and does not mark unmarked ancestors. `clear_marked_reachable` only walks children that already have the marker, so an unmarked intermediate hides the rest of the chain. `survivor_ids` keeps that intermediate because it has no marker and its parent is genesis. `genesis.prune()` is a no-op (parentless, unmarked). The intermediate stays a live child and serves its pre-fold values (`k1=v1` while genesis has `k1=v1x` in `build_prune_fixture`).
- **Why**: DG6 — once the clearing marker is the retry key, retry must finish the clear. Existing interruption tests cut only `after_clear_step`, which runs after every marker write and the mark flush, so they never enter this window. Writing intermediates first is not enough: `namespace().flush()` syncs shards one at a time, so power loss during that flush can persist the head's shard and drop another. Same-shard program order does not help the `kill -9` window, because the second put was never issued.
- **Suggested fix**: In `finish_interrupted_clear`, before any clear, walk the parent chain from `self` to the marker genesis (cycle-guarded), `set_aux(PRUNE_CLEARING_KEY)` on every non-genesis ancestor, flush, then the existing clear. If a parent slot is already `None`, clear has started, which is only after a completed mark flush, so every intermediate marker is already durable. Test: after merge and re-parent, set the marker on the head only; `head.prune()` must return genesis, the intermediate must be dead, and `i1.get("k1")` must not be `v1`. Same aux key `&[1]`; no migration.

---

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

---

## Rejected

### collections: "Orphan get_mut rewrites unstable encodings the same way as MapxOrdRawKey"
- **Where**: `strata/src/basic/orphan/mod.rs` (`ValueMut::drop`)
- **Claim**: dropping an unchanged `Orphan` guard writes when the value encoding is not a stored-byte round-trip, including `HashMap`.
- **Reason**: `Orphan::get_mut` snapshots `value.encode()` after decode, not the stored bytes. A second encode of that same instance matches, so a `HashMap` value is not rewritten. `MapxOrdRawKey` compared against the stored bytes and did rewrite; that path writes back only a real edit.

---

### collections: "unbounded growth of caller-retained data is a leak"
- **Where**: `strata/src/versioned/map.rs` (`commit`, `create_branch`, `log`, `list_branches`, `gc`), `strata/src/dagmap/raw/mod.rs`, `strata/src/vecdex/mod.rs`, `strata/src/slotdex/mod.rs`, `strata/src/trie/mod.rs` (`MptCalc`/`SmtCalc`)
- **Claim**: commits, branches, DAG children, HNSW nodes, SlotDex tiers, and in-memory trie keys need built-in caps, TTLs, or auto-expiry; `log`/`list_branches`/`gc` collecting proportional-to-history vectors can OOM.
- **Reason**: collections store exactly what callers insert and retain — a cap turns valid writes into artificial failures, and auto-expiry would silently destroy committed history (unreferenced VerMap commits are already hard-deleted immediately by the `delete_branch`/`rollback_to` ref-count cascade). Traversals, listing, and GC are cold paths doing O(retained data) work; SlotDex tier count is bounded by the slot type's bit width (`floor_base_of` saturates); `MptCalc`/`SmtCalc` are documented in-memory calculators.

---

### engine/strata: "atomic batch staging is unbounded"
- **Where**: `core/src/common/engine/mod.rs` (`MapxRawBatch`), `core/src/common/engine/mmdb.rs` (`MmdbBatch`), `strata/src/slotdex/mod.rs` (`insert_batch`), `strata/src/vecdex/mod.rs` (`insert_batch`)
- **Claim**: batches accumulate every staged operation in memory with no entry cap, so huge batches OOM.
- **Reason**: staging-then-commit is the documented atomicity contract, and memory is proportional to caller-supplied input under the caller's control (chunk the input for bounded memory). Auto-splitting inside the library would silently break the promised whole-batch atomicity. VecDex chunks internally because its documented contract is per-chunk atomicity; its dedup pass holds references plus encoded keys, again O(input).

---

### namespace/meta: "postcard deserialization of registry/meta files is a memory bomb"
- **Where**: `core/src/common/namespace.rs` (`load_registry`), `strata/src/common/mod.rs` (`load_instance_meta`)
- **Claim**: `fs::read` + `postcard::from_bytes` without a file-size cap lets a crafted registry or instance meta allocate unboundedly.
- **Reason**: postcard parses sequences element-by-element from the input, so decode cost and allocation are bounded by the actual file size (serde caps `Vec` preallocation; a small file cannot decode into millions of records), and file size is proportional to namespaces/instances actually created. Files under the base dir sit inside the process's own trust boundary; malformed bytes yield a clean decoding/encoding error.

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
- **Reason**: ordinary writes flush WAL bytes to the OS before returning; they are recoverable after a process crash, but are not individually fsynced against power loss. mmdb's `DB::drop` attempts WAL sync best-effort; `close()` additionally surfaces flush/sync errors. This rejects the claim that Drop entirely skips WAL sync; Drop alone does not establish cross-shard durability ordering, which VerMap now fences explicitly.

---

### engine: "no key/value size validation at the vsdb boundary"
- **Where**: `core/src/basic/mapx_raw/mod.rs` (`insert`), `core/src/common/engine/mmdb.rs` (`MmDB::insert`)
- **Claim**: absent vsdb-side checks, oversized values reach the memtable and OOM.
- **Reason**: mmdb validates every write (8 MiB key cap, ~64 MiB entry cap) before WAL/memtable admission and rejects with a descriptive error; oversized entries are never admitted to the WAL or memtable, although a caller-owned WriteBatch may copy them during staging. The boundary behavior is documented on `MapxRaw::insert` and `MapxRawBatch::commit`: direct ops panic under the fatal-write convention, batch commits surface `Err`.

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
- **Where**: `core/src/common/engine/mmdb.rs` (`effective_mem_budget`) — historical; default budget no longer uses cgroup/host
- **Claim**: Derating should occur only when the raw cgroup limit is below host memory.
- **Reason**: Obsolete against current code: default engine budget is fixed 2 GiB / `VSDB_MEM_BUDGET_MB` only (`effective_mem_budget` no longer folds cgroup/host). Cgroup derating remains only in bench `legacy_budget` code. The prior min-fold rationale no longer applies to library open paths.

---

### engine: "derating should apply to `memory.high`, not `memory.max`"
- **Where**: `core/src/common/engine/mmdb.rs` (`cgroup_mem_limit_bytes`) — historical; symbol removed from library
- **Claim**: A hard cgroup maximum is safe to budget at 100%.
- **Reason**: Obsolete against current code: `cgroup_mem_limit_bytes` is not in `core/src`; library default budget no longer reads cgroup limits. Keep as permanent reject of the recurring claim if reintroduced without evidence.

---

### namespace: "destroy must restore the registry when directory removal returns an error"
- **Where**: `core/src/common/namespace.rs` (`Namespace::destroy`)
- **Claim**: Unregistering before `remove_dir_all`, then returning that I/O error, strands an intact tree (`EACCES`) or reports failure after a mount-point wipe (`EBUSY`), and retry cannot finish the destroy.
- **Reason**: The documented order is registry update, then tree removal. A crash in that window leaves an orphaned directory that is manually removable; re-attachment is an explicit non-goal. An I/O error after the registry commit is that same durable state, not a rollback obligation. Restoring the record after a failed `remove_dir_all` can re-register a half-deleted tree (children already unlinked, then `rmdir` fails), which a later open would treat as a live dataset. `cleanup_failed_root` is safe only because create proved the root was empty before it wrote anything.
