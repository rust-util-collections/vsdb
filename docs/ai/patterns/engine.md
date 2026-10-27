# Engine & Storage Review Patterns

**Files:** `engine/{mod,mmdb}.rs`, `namespace.rs`, `common/mod.rs`, `mapx_raw/`.

**Arch:** One engine per namespace = N MMDB shards (default pinned 16; others
creation-persisted). One process-global prefix allocator
(`{base}/__SYSTEM__/__prefix_ceiling__`). Key = `[prefix_u64][user]`; reserved
1-byte system keys `[0]` (legacy ceiling) / `[1]` (WAL fence) cannot collide.
Handles own `Namespace` Arc — ambient scope is read at create only, never by
ops/serde/`from_meta`/`from_bytes_in`. Engines in `Arc<NsInner>`
(no leak); close under lifecycle locks. Format marker after shards complete.
One `BlockCachePool` per engine. A WriteBatch is bound to one prefix (⇒ one shard).

## Invariants

**E1 Prefix unique** — alloc floors/ceilings/cursors only forward; durable ceiling before issue; recovered reserved; never reuse.
**E2 Shard routing** — same formula on get/put/delete/iter (`self.dbs.len()`).
**E3 Prefix scope** — all ops/iter bounds use structure prefix only.
**E4 Engine uniqueness** — default once; non-default id ≤ one open under REGISTRY_LOCK + OPEN re-check.
**E5 Batch scope** — one batch = one prefix; no multi-prefix batch exists. Cross-prefix atomicity needs single-handle staging or dirty-flag/root-last recovery.
**E6 Iter prefix bound** — stop at prefix end (incl. `u64::MAX` upper-bound care).
**E7 Lifecycle exclusion** — open/create/destroy/relocate/close serialize properly; table lock released before slow teardown.
**E8 Format/shards** — marked root = exact shard set + CURRENT anchors. No marker: 0 shards, exact set + CURRENT, or partial only with the `__SYSTEM__/__initializing__` sentinel; stray `shard_*` refused.
**E9 Identity** — route via owned Namespace; meta ns suffix absent ⇔ default; no ambient redirect of existing handles.
**E10 Cache pool** — all shards of one engine share one pool; engines do not
share pools or identity. Memory inputs size buffers/caches with floors/caps,
not hard RSS limits. `block-cache-usage` repeats the pool-wide entry count
plus each shard's pinned entries; do not sum it as per-shard byte usage.
**E11 clone_in** — fresh unobservable prefix; bounded independent batches; failed chunk best-effort wipe target without masking primary Err.

**E13 Co-location** — `new_colocated` allocates until the prefix hits the anchor's shard
(skipped prefixes are burned, never reissued); `is_colocated_with` = same ns + same shard.
A composite may drop cross-component fences only when **every** component is
co-located, re-checked on restore; otherwise keep the per-shard fences.
**E14 Path-only helpers** — `vsdb_get_{base,custom,system,meta}_dir` / `vsdb_meta_path`
resolve paths without opening an engine; `Namespace::default_ns()` opens the default
engine — never use it just for a path. `vsdb_configure` never mutates the environment;
default budget = nonzero `VsdbOptions::mem_budget_mb` > `VSDB_MEM_BUDGET_MB` > 2 GiB.

**E15 Read-only open** — complete immutable datasets only; in-memory WAL replay,
no persistent repair. A stale initialization sentinel on a complete marked root
is accepted untouched; a Pending namespace lifecycle is refused. One writable
process per universe; multiple readers require the documented locking/snapshot
rules. Explicit trie checkpoints return ReadOnly, never silently succeed.

**E12 Scan errors** — inspect streaming iterator errors before filter/map erases the source; both directions fail fast instead of returning successful partial data.

## Bugs

**Collision** — ceiling/floor regress, unrecovered reserve, cursor outside window.
**Cross-shard read** — endian/route mismatch.
**Iter leak** — past prefix / MAX overflow.
**Double open** — race create two engines.
**Partial root as complete** — marker/layout wrong but adopted silently.

## Checklist

- [ ] Monotonic alloc; durable ceiling before issue
- [ ] Identical routing all paths
- [ ] Prefixed keys + bounded iters (`u64::MAX` OK)
- [ ] Default once; one engine per non-default id
- [ ] Batches single-prefix; cross-prefix atomicity via staging/recovery
- [ ] Lifecycle under documented locks
- [ ] Marker ↔ shards ↔ CURRENT agree
- [ ] Handle ns independent of ambient create
- [ ] One pool per engine
- [ ] clone_in chunk+wipe on error
- [ ] Late SST read failures surface in both scan directions
- [ ] Co-location checked on restore before skipping fences
- [ ] No engine open for path-only needs
