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

### [LOW] review-docs: alias, raw-restore, and GC guidance is stale
- **Where**: `CLAUDE.md:90-96`, `.claude/docs/technical-patterns.md:73-77`, `.claude/docs/technical-patterns.md:148-160`, `.claude/docs/false-positive-guide.md:13-18`, `.claude/docs/patterns/versioning.md:38-40`
- **What**: review rules still require global writer serialization, same-version raw bytes, and a dirty marker around idempotent full GC.
- **Why**: current public contracts permit disjoint-key writers, define raw restore by prefix/type/namespace validity, and use `gc_dirty` only around non-idempotent ref-count cascades.
- **Suggested fix**: align the review guides with the current per-key, restore-validity, and dirty-state contracts.

### [LOW] docs: shared-memory-pool proposal contradicts shipped status
- **Where**: `docs/proposals/shared-mem-pool.md:3-20`, `docs/proposals/shared-mem-pool.md:225-232`, `docs/proposals/shared-mem-pool.md:463-471`, `docs/proposals/shared-mem-pool.md:530-560`
- **What**: the status/table say telemetry and the Q1 gate shipped, while later sections still say telemetry is unavailable and Q1 unresolved.
- **Why**: maintainers can repeat completed work or treat a passed rollout gate as open.
- **Suggested fix**: mark those statements historical/resolved and leave only soak, tier (ii), and phase 2 open.

### [LOW] CI: formatting is not enforced
- **Where**: `.github/workflows/rust.yml:18-22`
- **What**: CI runs lint/tests but omits `cargo fmt --all -- --check`, despite the canonical commit gate requiring it.
- **Why**: unformatted Rust can merge while CI passes.
- **Suggested fix**: add a non-mutating formatting step.

### [LOW] docs: crate README license links are broken
- **Where**: `core/README.md:5`, `strata/README.md:5`
- **What**: both use `../../LICENSE`, which resolves outside the repository.
- **Why**: the license badges link to a nonexistent target.
- **Suggested fix**: link to `../LICENSE`.

### [LOW] bench: `hotspot_writes` measures independent cloned maps
- **Where**: `strata/benches/units/concurrent.rs:77-118`
- **What**: each worker deep-clones `shared_db`, allocating a fresh prefix before timing.
- **Why**: the benchmark label implies one contended map but records writes to independent maps.
- **Suggested fix**: either rename/document the independent-clone workload or use verified disjoint-key shadows for a shared-map benchmark.

### [LOW] bench: ordered operations use non-order-preserving postcard bytes
- **Where**: `strata/benches/units/basic_mapx_ord.rs:17-50`, `strata/benches/units/basic_mapx_ord.rs:84-126`
- **What**: inferred `MapxOrd<Vec<u8>, usize>` orders postcard varints lexicographically while labels describe numeric ordering/ranges.
- **Why**: `get_le`, `get_ge`, and the claimed 1,000-key range measure a different ordering and row count.
- **Suggested fix**: benchmark `MapxOrd<usize, usize>` with ordered-key encoding and assert the prepared range size.

### [LOW] bench: read/remove workloads decay into misses and no-ops
- **Where**: `strata/benches/units/basic_mapx.rs:25-29`, `strata/benches/units/basic_mapx.rs:73-78`, `strata/benches/units/basic_mapx_ord.rs:28-50`
- **What**: decrementing counters start one past the last inserted key and eventually underflow or exhaust the finite removal set.
- **Why**: timings increasingly measure absent-key reads/removes rather than the labeled successful operations.
- **Suggested fix**: cycle reads over a fixed populated range and replenish/setup successful removals per iteration.

---

## Won't Fix

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
