# Engine & Storage Review Patterns

[Engine and metadata](../../../core/src/common) · [Raw map](../../../core/src/basic/mapx_raw)

**Arch:** One universe (`{base}/__SYSTEM__/universe_header` `VSUNI018`, advisory `LOCK`, `VSNREG02` `__namespaces__`, `__prefix_ceiling__`). One engine per namespace = N MMDB shards (default pinned 16 behind lazy `OnceLock`; others creation-persisted), bound by `VSENG018` `engine_header` + `.shards_ready == b"18"`. Key = `[prefix_u64][user]`; 1-byte WAL fence key `[1]` cannot collide. Handles own `Namespace` `Arc` — ambient scope is read at create only, never by ops/serde/`from_meta`. Engines in `Arc<NsInner>` (no leak); close under lifecycle locks. One `BlockCachePool` per engine. A `WriteBatch` is bound to one prefix (⇒ one shard).

## Invariants

**E1 Prefix unique** — alloc floors/ceilings/cursors only forward; durable ceiling before issue; recovered reserved; never reuse.

**E2 Shard routing** — same formula on get/put/delete/iter (`self.dbs.len()`).

**E3 Prefix scope** — all ops/iter bounds use structure prefix only.

**E4 Engine uniqueness** — default once; non-default id ≤ one open under REGISTRY_LOCK + OPEN re-check.

**E5 Batch scope** — one batch = one prefix; no multi-prefix batch exists. Multi-prefix protocols such as DagMap order their writes on the shared shard and retain recovery records until cleanup finishes.

**E6 Iter prefix bound** — stop at prefix end (incl. `u64::MAX` upper-bound care).

**E7 Lifecycle exclusion** — open/create/destroy/relocate/close serialize properly; table lock released before slow teardown.

**E8 Headers/shards** — `VSUNI018` `universe_header` is written last during bootstrap under exclusive `__SYSTEM__/LOCK`; once present, missing `VSNREG02` `__namespaces__` fails closed (`Corrupt`). Ready engine = matching `VSENG018` `(universe_id, ns_id, shards)` + `.shards_ready == b"18"` + exact `shard_00..shard_{N-1}` with `CURRENT` anchors. Unready engine = 0 shards, or an interrupted create with matching `VSENG018` `__initializing__` sentinel; stray `shard_*` refused.

**E9 Identity** — route via owned Namespace; ns ID zero selects default; no ambient redirect of existing handles. Missing-ceiling recovery (`note_meta_bytes` + 23-byte `note_meta_stream` carry) scans keys, values, `__instance_meta__`, and `__CUSTOM__` for `VSMAPX02` (`+8..+16`) and `VSTYPE04` (`+16..+24`).

**E10 Cache pool** — all shards of one engine share one pool; engines do not
share pools or identity. Memory inputs size buffers/caches with floors/caps,
not hard RSS limits. `block-cache-usage` repeats the pool-wide entry count
plus each shard's pinned entries; do not sum it as per-shard byte usage.

**E11 clone_in** — fresh unobservable prefix; bounded independent batches; failed chunk best-effort wipe target without masking primary Err.

**E12 Scan errors** — inspect streaming iterator errors before filter/map erases the source; both directions fail fast instead of returning successful partial data.

**E13 Co-location** — `new_colocated` allocates until the prefix hits the anchor's shard
(skipped prefixes are burned, never reissued); `is_colocated_with` = same ns + same shard.
A composite may drop cross-component fences only when **every** component is
co-located, re-checked on restore; otherwise keep the per-shard fences.

**E14 Path-only helpers & config freeze** — `vsdb_get_{base,custom,system}_dir`
resolve paths without opening an engine, and **every** path-materializing getter must set `BASE_DIR_FROZEN` under `VSDB_BASE_DIR` lock so `vsdb_configure` cannot overwrite an already-returned path. `Namespace::default_ns()` opens the default
engine (`OnceLock`; `vsdb_flush()` skips it when unopened) — never use it just for a path. `vsdb_configure` never mutates the environment;
default budget = nonzero `VsdbOptions::mem_budget_mb` > `VSDB_MEM_BUDGET_MB` > 2 GiB.

**E15 Read-only open** — complete immutable datasets only; in-memory WAL replay,
no persistent repair. Acquire shared `__SYSTEM__/LOCK` (`NotFound` OK on lockless immutable copies) **before** reading `universe_header` so active writer/migrator contention returns `Io(WouldBlock)` rather than `Corrupt`. A stale initialization sentinel on a complete marked root
is accepted untouched; a `Pending` namespace lifecycle is refused. Explicit trie checkpoints return `ReadOnly`, never silently succeed.

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
- [ ] Batches cover one prefix. Multi-prefix operations preserve documented ordering and retry behavior.
- [ ] Lifecycle under documented locks
- [ ] Marker ↔ shards ↔ CURRENT agree
- [ ] Handle ns independent of ambient create
- [ ] One pool per engine
- [ ] clone_in chunk+wipe on error
- [ ] Late SST read failures surface in both scan directions
- [ ] Co-location checked on restore before skipping fences
- [ ] No engine open for path-only needs
