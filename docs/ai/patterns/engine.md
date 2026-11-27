# Engine & Storage Review Patterns

[Engine and metadata](../../../core/src/common) · [Raw map](../../../core/src/basic/mapx_raw)

**Arch:** One universe (`{base}/__SYSTEM__/universe_header` `VSUNI018`, advisory `LOCK`, `VSNREG02` `__namespaces__`, `__prefix_ceiling__`). One engine per namespace = N MMDB shards (default pinned 16 behind lazy `OnceLock`; others creation-persisted), bound by `VSENG018` `engine_header` + `.shards_ready == b"18"`. Key = `[prefix_u64][user]`; a native shard WAL fence writes no data key or sequence. Handles own `Namespace` `Arc` — ambient scope is read at create only, never by ops/serde/`from_meta`. Engines in `Arc<NsInner>` (no leak); close under lifecycle locks. One `BlockCachePool` per engine. A raw batch binds one prefix. A checked co-located insert binds two prefixes on one shard.

## Invariants

**E1 Prefix unique** — alloc floors/ceilings/cursors only forward; durable ceiling before issue; recovered reserved; never reuse.

**E2 Shard routing** — same formula on get/put/delete/iter (`self.dbs.len()`).

**E3 Prefix scope** — all ops/iter bounds use structure prefix only.

**E4 Engine uniqueness** — default once; non-default id ≤ one open under REGISTRY_LOCK + OPEN re-check.

**E5 Batch scope** — Ordinary batches bind one prefix. `insert_colocated`
checks engine identity and shard before either map's rows are appended. If
both handles name the same map, its `here` puts precede its `there` puts.
`write_colocated` applies the same checks to any number of maps before
appending puts and removals in order. DagMap child creation uses one write
for all three rows. A prune publishes its fold, reparented children and
clearing markers in one write and retains its recovery records across the
later phases. [Co-location tests](../../../core/tests/colocated.rs)

**E6 Iter prefix bound** — stop at prefix end (incl. `u64::MAX` upper-bound care).

**E7 Lifecycle exclusion** — open/create/destroy/relocate/close serialize properly; table lock released before slow teardown.

**E8 Headers/shards** — Bootstrap publishes `VSUNI018` after its registry and
allocator under `__SYSTEM__/LOCK`. A ready engine has a matching `VSENG018`
identity, a ready marker and the registered set of shards. Trace the
`CURRENT`-selected MANIFEST in every anchored shard before writable recovery
can touch the first one. The v5 header checksum, seven levels and eight-byte
prefix must agree in both open modes. Exercise a bad final shard with an
earlier recoverable WAL. For a partial initializer, preflight the anchors
already present before backfilling metadata. A missing *required* lifecycle
record returns `Corrupt`. Other filesystem read errors retain their I/O cause.
[Geometry preflight](../../../core/src/common/engine/mmdb/geometry.rs) · [Lifecycle fixtures](../../../core/tests/universe_header.rs) · [Adapter fixtures](../../../core/tests/v5_adapter_test.rs)

**E9 Identity** — route via owned Namespace; ns ID zero selects default; no ambient redirect of existing handles. Missing-ceiling recovery (`note_meta_bytes` + 23-byte `note_meta_stream` carry) scans keys, values, `__instance_meta__`, and `__CUSTOM__` for `VSMAPX02` (`+8..+16`) and `VSTYPE04` (`+16..+24`).

**E10 Cache pool** — `Namespace::memory_usage()` counts one pool with
separate segmented and overflow capacities per engine. The shard entries
track current memtables and L0 pins. Compare caller-held pins, per-tree
caches, RSS and compaction debt separately. The string property
`block-cache-usage` measures entries and repeats the shared component on
each shard. [Sizing and units](../../storage.md#memory-sizing)

**E11 Clone** — A fallible clone can hit a source error after writing
destination chunks. Use checked cursor steps, wipe the new target on failure
and preserve the original error even if cleanup also fails.
[Late-corruption fixture](../../../core/src/common/engine/clone_tests.rs)

**E12 Pinned reads and cursors** — `try_step_next` and `try_step_prev` lend
a row until the next cursor operation and surface late errors. The ordinary
iterator returns owned rows and keeps its documented panic behavior. Check
`try_get_with` on hits, empty values and misses. A snapshot's `try_visit_many`
keeps backing pins alive across the visit, including duplicate slots. Its
32-key cap bounds the slot count. Retained bytes depend on the backing blocks or arenas. [Raw API](../../../core/docs/api.md)

**E13 Co-location** — `new_colocated` allocates until the prefix hits the anchor's shard
(skipped prefixes are burned, never reissued); `is_colocated_with` = same ns + same shard.
A composite may drop cross-component fences only when **every** component is
co-located, re-checked on restore; otherwise keep the per-shard fences.

**E14 Path-only helpers & config freeze** — `vsdb_get_{base,custom,system}_dir`
resolve paths without opening an engine, and **every** path-materializing getter must set `BASE_DIR_FROZEN` under `VSDB_BASE_DIR` lock so `vsdb_configure` cannot overwrite an already-returned path. `Namespace::default_ns()` opens the default
engine (`OnceLock`; `vsdb_flush()` skips it when unopened) — never use it just for a path. `vsdb_configure` never mutates the environment;
default budget = nonzero `VsdbOptions::mem_budget_mb` > `VSDB_MEM_BUDGET_MB` > 2 GiB.

**E15 Read-only open** — complete immutable datasets only; in-memory WAL replay,
no persistent repair. Acquire shared `__SYSTEM__/LOCK` before reading
`universe_header`. If the first lock probe finds no file, probe again before
interpreting the header result. The reader reports `Io(WouldBlock)` if a
writer holds the lock, even during bootstrap. Reread the header under a newly acquired shared lock.
An immutable copy may omit lock files. A stale initialization sentinel on a complete marked root
is accepted untouched; a `Pending` namespace lifecycle is refused. Explicit trie checkpoints return `ReadOnly`, never silently succeed.

**E16 Write admission and fences** — A nonempty `commit_no_slowdown` or
`commit_sync_no_slowdown` can return `StorageErrorKind::Busy` under L0
pressure or a full immutable queue. Compare WAL bytes, sequence and rows
before and after rejection, then rebuild the consumed batch and retry. A
malformed batch keeps its argument error. An empty sync batch does not
fence earlier writes. The explicit shard fence covers sealed and current WAL
generations and adds no mutation. [Public batch contract](../../../core/docs/api.md) · [Pressure and fence tests](../../../core/src/common/engine/mmdb/v5_tests.rs)

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
- [ ] Ordinary batches stay within their prefix. Co-located writes publish every row set together and reject mismatched placement before writing.
- [ ] Lifecycle under documented locks
- [ ] Marker ↔ shards ↔ CURRENT agree
- [ ] Handle ns independent of ambient create
- [ ] One pool per engine
- [ ] clone_in chunk+wipe on error
- [ ] Late SST read failures surface in both scan directions
- [ ] Co-location checked on restore before skipping fences
- [ ] No engine open for path-only needs
