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

### [HIGH] vecdex: reciprocal pruning can isolate inserts and violate entry-point invariants
- **Where**: `strata/src/vecdex/mod.rs:661-745`, `strata/src/vecdex/mod.rs:986-1031`, `strata/src/vecdex/hnsw.rs:284-311`
- **What**: every selected neighbor may prune the newly inserted node and detach the reciprocal edge; re-election can then choose a lower-layer linked node while retaining a higher `max_layer`.
- **Why**: a committed node can become unreachable, and a promoted isolated node can hide the existing graph; search may start from a node absent from the recorded top layers.
- **Suggested fix**: preserve at least one reciprocal edge per linked layer, re-elect only a true-max-layer entry point, and reconnect an isolated winner before commit.

### [HIGH] vecdex: replacement is not one atomic mutation
- **Where**: `strata/src/vecdex/mod.rs:543-579`, `strata/src/vecdex/mod.rs:605-658`, `strata/src/vecdex/mod.rs:897-1036`
- **What**: replacing an existing key commits `remove` before staging the new insert; batch replacement removes all old keys before their chunks are attempted.
- **Why**: a crash or second commit failure returns an error after the old value is already gone, and later batch chunks can lose keys they never replaced.
- **Suggested fix**: extract staged removal and combine remove+insert in the same transaction/chunk; retain bounded independent chunk commits.

### [HIGH] dagmap: teardown ordering lacks durable cross-shard barriers
- **Where**: `strata/src/dagmap/raw/mod.rs:602-632`, `strata/src/dagmap/raw/mod.rs:647-679`, `strata/src/dagmap/raw/mod.rs:682-803`
- **What**: destroy/prune clearing relies on call order, but data/parent clears and later registry removals can land in different unsynced shard WALs.
- **Why**: after power loss, unregistering can be durable while earlier clears are lost, permanently hiding live storage from every registry-driven cleanup path.
- **Suggested fix**: flush the namespace after all owned data/parent slots are cleared and before discoverability registries are removed; add restart-oriented phase tests.

### [HIGH] dagmap: deserialization accepts mixed-namespace components
- **Where**: `strata/src/dagmap/raw/mod.rs:82-127`, `strata/src/dagmap/raw/mod.rs:131-152`
- **What**: `DagMapRaw` assembles `data`, `parent`, and `children` handles without verifying that they belong to one namespace.
- **Why**: malformed safe serde input bypasses the constructor invariant, and prune flushes only `data`'s engine while mutating components in other engines.
- **Suggested fix**: reject deserialized composites whose component namespace IDs differ; add mixed-namespace serde tests.

### [HIGH] trie: typed SMT proof APIs accept ambiguous raw key bytes
- **Where**: `strata/src/trie/proof.rs:332-352`, `strata/src/common/ende.rs:333-348`
- **What**: `VerMapWithProof<K, V, SmtCalc>::prove` accepts `&[u8]` even though the committed trie uses `K::to_bytes()`.
- **Why**: for ordered encodings such as negative signed integers, natural bytes differ from stored bytes and yield a valid non-membership proof for a present logical key.
- **Suggested fix**: add typed proof/verification methods that encode `&K`, make raw-byte requirements explicit, and cover a negative signed-key membership proof.

### [HIGH] trie: cache trust-boundary validation is incomplete
- **Where**: `strata/src/trie/cache.rs:71-147`, `strata/src/trie/cache.rs:224-385`, `strata/src/trie/codec_util.rs:24-60`, `strata/src/trie/smt/cache.rs:60-132`, `strata/src/trie/smt/cache.rs:195-325`
- **What**: loaders accept noncanonical tree shapes and self-attested cached hashes, SMT permits cached parents with unhashed children, length arithmetic can overflow, and files are read without a size bound.
- **Why**: checksum-valid malformed caches can return a wrong root, make later proofs fail, panic during parsing, or exhaust memory instead of falling back to authoritative rebuild.
- **Suggested fix**: enforce canonical shape, recompute every cached hash, reject mixed handle state/trailing bytes, harden varints and checked ranges, and cap cache-file size before allocation.

### [MEDIUM] persistent-btree: deserialized deletes flush discarded temporary nodes
- **Where**: `strata/src/basic/persistent_btree/remove.rs:27-84`, `strata/src/basic/persistent_btree/mod.rs:175-180`
- **What**: `discard_node` returns immediately while ref counts are unavailable, even when the node is a current-operation entry in `pending`.
- **Why**: singleton/root contraction and underflow churn after standalone restore persist unreachable nodes instead of dropping them from the write buffer.
- **Suggested fix**: remove current-operation pending nodes before the not-ready return; retain conservative handling for unknown on-disk nodes.

### [MEDIUM] slotdex: `insert_batch` does not preserve serial tier-growth cadence
- **Where**: `strata/src/slotdex/mod.rs:513-623`, `strata/src/slotdex/mod.rs:746-801`
- **What**: bulk insertion groups by slot and checks growth once per group, while serial insertion checks before every unique key.
- **Why**: the first key in a slot can create a new top-level bucket and make the second key require another tier; one-shot bulk then persists fewer levels than serial/chunked insertion.
- **Suggested fix**: simulate unique inserts in original order inside one staged batch, including per-key growth/count overlays; extend serial/bulk/reopen equivalence tests.

### [MEDIUM] tests: staged unit tests race process-environment mutation
- **Where**: `strata/src/common/staged.rs:185-235`
- **What**: two parallel library tests call `vsdb_set_base_dir` after the test harness has spawned threads.
- **Why**: the successful call performs `env::set_var`, violating its documented safety precondition and permitting undefined/flaky test behavior.
- **Suggested fix**: remove the per-test base-dir mutation and rely on globally unique prefixes.

### [MEDIUM] dagmap: selective prune APIs expose no usable child ID
- **Where**: `strata/src/dagmap/raw/mod.rs:195-205`, `strata/src/dagmap/raw/mod.rs:635-660`, `strata/src/dagmap/rawkey/mod.rs:258-270`
- **What**: include/exclude pruning requires hidden 16-byte registry IDs that construction never returns and no public method exposes.
- **Why**: callers cannot reliably retain or remove a chosen child; its public `InstanceId` is a different identifier.
- **Suggested fix**: expose child-registry IDs or add instance-based selection helpers, with non-empty include/exclude tests.

### [MEDIUM] compatibility: historical persisted-data breaks lack an executable migration path
- **Where**: `CHANGELOG.md:744-748`, `CHANGELOG.md:799-803`, `CHANGELOG.md:805-811`, `CHANGELOG.md:825-830`, `strata/src/common/mod.rs:60-66`
- **What**: codec and typed-meta breaks identify incompatibility but omit exact last-readable versions, export/import steps, backup/rollback guidance, and direct-open behavior.
- **Why**: users cannot safely upgrade old datasets from msgpack, CBOR, or pre-`VSTYPE02` metadata by following the current documentation.
- **Suggested fix**: document full old-version logical export into a fresh current namespace/base directory, validation, and rollback; correct the stale v13 re-save claim.

### [LOW] docs: namespace examples violate current identity and lifecycle contracts
- **Where**: `core/docs/api.md:70-117`, `strata/docs/api.md:25-33`
- **What**: one example closes/destroys/relocates namespaces while handles remain live; another claims a non-default `InstanceId` converts into legacy `u64`.
- **Why**: the examples panic/fail to compile or would redirect lookup to the default namespace.
- **Suggested fix**: split lifecycle examples with explicit drops and physical movement, and demonstrate legacy `u64` only for a genuine default-namespace token.

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
