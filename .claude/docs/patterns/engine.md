# Engine & Storage Review Patterns

**Files:** `engine/{mod,mmdb}.rs`, `namespace.rs`, `common/mod.rs`, `mapx_raw/`.

**Arch:** One engine per namespace = N MMDB shards (default pinned 16; others
creation-persisted). One process-global prefix allocator
(`{base}/__SYSTEM__/__prefix_ceiling__`). Key = `[prefix_u64][user]`; reserved
1-byte system keys `[0]` (legacy ceiling) / `[1]` (WAL fence) cannot collide.
Handles own `Namespace` Arc — ambient scope is read at create and by unsafe
non-`_in` `from_bytes`, never by ops/serde/`from_meta`. Engines in `Arc<NsInner>`
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
**E9 Identity** — route via owned Namespace; meta ns suffix absent ⇔ default; no ambient redirect of existing handles (only unsafe non-`_in` `from_bytes` reads ambient).
**E10 Cache pool** — all shards of one engine share one pool; engines don’t share identity.
**E11 clone_in** — fresh unobservable prefix; bounded independent batches; failed chunk best-effort wipe target without masking primary Err.

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
