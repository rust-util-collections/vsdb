# RFC: Shared Memory Pools — Injectable Cache & Write-Buffer Accounting

- **Status**: draft — **not scheduled**; revisit when the trigger
  conditions in §8 are met
- **Prerequisites**: [namespaces.md](./namespaces.md) (implemented,
  v16.0.0+), [ns-close.md](./ns-close.md) (implemented, v16.1.0 —
  engine lifetimes are now dynamic, which this design must respect)
- **Layers involved**: mmdb (mechanism), vsdb (policy)

---

## 1. Motivation

v16 namespaces deliberately split memory budgeting into two paths:

- **Default namespace**: the process-wide `MEM_BUDGET` pipeline —
  `VSDB_MEM_BUDGET_MB` verbatim override / detected cgroup limit
  min-folded at ¾ / host reading (`EngineSizing::from_process_budget`,
  `core/src/common/engine/mmdb.rs`). v15-parity.
- **Non-default namespaces**: an explicit creation-time budget
  persisted in the registry, defaulting to a fixed conservative
  `DEFAULT_NS_BUDGET_MB = 512`, always treated as binding
  (`EngineSizing::from_budget_mb`; rationale: namespaces.md C6/§4.4 —
  auto-detection per namespace would multiply the process footprint by
  the number of open namespaces).

The split is principled and stays (evaluated and reaffirmed: policy
unification inside vsdb is rejected — every variant either re-opens
the C6 multiplication hazard, requires runtime resizing mmdb does not
support, or breaks v15 sizing compatibility for the default
namespace). But the *static division* underneath it has real costs:

1. **Within one engine, shards cannot borrow from each other.** Every
   engine statically splits its budget across its shards: block cache
   `budget/8/shards` each, write buffer likewise (`mmdb_open`). A hot
   shard on a skewed workload evicts while its 15 siblings idle below
   capacity. This cost exists **today, inside the default namespace**,
   independent of any namespace question.
2. **Across namespaces, budgets cannot flow.** Epoch rotation — the
   headline namespace workload — has exactly one namespace hot (the
   one being filled) while the rest idle. The filling namespace is
   pinned to its creation-time 512 MB even when the process ceiling
   has tens of GB of headroom.
3. **No single number controls the process.** Total footprint ≈
   default-engine budget + Σ per-namespace budgets; `VSDB_MEM_BUDGET_MB`
   governs only the first term.

The correct end-state — *if* these costs are ever observed in
production rather than in theory — is to solve this in **mmdb**, at
the mechanism layer. This RFC records that design so the evaluation
does not have to be redone.

## 2. Form: injectable shared handles, NOT a library-global pool

The question "should mmdb switch to a global memory pool?" decomposes
into form and mechanism, and the form matters more:

**Rejected: a process-global pool inside mmdb.** A library-internal
singleton is the wrong shape regardless of the accounting design:

- mmdb is a general-purpose library; unrelated DBs in one process must
  not be silently coupled through hidden shared state.
- "How much total memory, and who shares with whom" is *policy* — it
  belongs to the caller (vsdb), not the engine.
- Parallel test suites opening many DBs would contend on the
  singleton — exactly the cross-test global coupling vsdb v16.0.2
  spent effort eliminating.

**Adopted direction: RocksDB's shape.** RocksDB solved this same
problem two decades of LSM tuning ago and never made the pool
implicit: the caller constructs a `Cache` and/or `WriteBufferManager`
and passes them into each DB's options; DBs given the same handle
share, DBs given none stay independent. Translated to mmdb:

```rust
pub struct DbOptions {
    // ...existing fields...
    /// `None` (default) ⇒ a private cache sized by
    /// `block_cache_capacity` — today's behavior, unchanged.
    pub block_cache: Option<Arc<BlockCache>>,
    /// `None` (default) ⇒ per-DB `write_buffer_size` trigger only —
    /// today's behavior, unchanged.
    pub write_buffer_manager: Option<Arc<WriteBufferManager>>,
}
```

vsdb then assembles the topology: one pool per process (or per
namespace tier — policy is free to choose), sized by the existing
`MEM_BUDGET` pipeline, passed to every shard it opens. At that point
`VSDB_MEM_BUDGET_MB` genuinely bounds the whole process, and the
unification the two-path split could not deliver happens at the layer
that can.

## 3. Current mechanics (verified against source)

Facts the design must build on, as of mmdb master / vsdb v16.1.x:

- **BlockCache is per-DB and internally constructed**
  (`src/db.rs:340`): `Arc::new(BlockCache::new(options.block_cache_capacity))`.
  moka LRU keyed by `(sst_file_number, block_offset)`
  (`src/cache/block_cache.rs`). File numbers are **per-DB manifest
  counters** — two DBs both own an SST numbered 5, so sharing one
  cache today would collide keys and *return wrong data*. Key
  namespacing is a hard correctness prerequisite, not tuning.
- **Pinned entries never evict**: `insert_pinned` (first data block of
  each L0 file, `pin_l0_filter_and_index_blocks_in_cache: true` by
  default). N DBs pinning into one pool can squeeze the evictable
  region without per-DB pin accounting.
- **A `file_offsets` reverse index** supports bulk invalidation per
  file; it would need the same per-DB key namespacing.
- **TableCache is per-DB and path-bound**
  (`TableCache::new_with_stats(&path, ...)`) — it is NOT shareable and
  is out of scope; note that internal `Option<Arc<TableCache>>`
  injection plumbing already exists (`VersionSet::open_with_cache`) as
  a pattern precedent.
- **Flush trigger is a per-DB size check under the DB's own mutex**
  (`src/db.rs:2717`):
  `active_memtable.approximate_size() >= options.write_buffer_size`,
  with `max_immutable_memtables = 4` bounding frozen tables — worst
  case ≈ 5 × write_buffer_size per shard. Memtables already track
  `approximate_size: AtomicUsize`, so global accounting can hook the
  existing deltas cheaply.
- **Write throttle is L0-count based, not memory based**
  (`maybe_throttle_writes`, slowdown/stop triggers). A memory-based
  stall would be a *new* throttle dimension with its own error-policy
  interactions (`drain_l0` fail-stop, bg_error, manifest poison — the
  most heavily audited code in mmdb).
- **DB lifetimes are dynamic** since ns-close (v16.1.0): any cross-DB
  registry inside a pool must tolerate members closing at arbitrary
  times without dangling into them.

## 4. Design sketch

### 4.1 Phase 1 — shared BlockCache (read-side elasticity)

The 20%-effort / 80%-value piece: no write-path risk, immediately
addresses cost #1 (intra-engine skew) and the read half of #2.

1. **Cache-key namespacing** (prerequisite): extend `CacheKey` with a
   per-DB unique id — a `u64` issued from a process-global atomic at
   `DB::open` is sufficient (RocksDB uses a session-id prefix; an
   atomic counter gives the same uniqueness for in-process sharing).
   Touches `get`/`insert`/`insert_pinned`, the pinned map, and
   `file_offsets` (which becomes keyed by `(db_id, file_number)`).
2. **Pin accounting**: track pinned bytes per `db_id`; either cap pins
   per DB or simply expose the number and let the pool sizing policy
   absorb it (L0 first-blocks are small; a cap is likely
   over-engineering — decide with data).
3. **Injection**: `DbOptions.block_cache: Option<Arc<BlockCache>>`;
   `None` constructs the private cache exactly as today.
   `block_cache_capacity` is ignored when a shared handle is supplied
   (document loudly).
4. **Close/Drop**: a closing DB bulk-invalidates its own `db_id`
   entries (the `file_offsets` index already supports per-file
   invalidation; per-DB is a loop over it). Cheap, and prevents a
   rotated-out namespace's dead blocks from occupying the pool until
   LRU pressure evicts them.

### 4.2 Phase 2 — WriteBufferManager (write-side elasticity)

The high-risk piece; only justified by demonstrated memory pressure.

1. **Accounting**: a shared `AtomicUsize` summing all member DBs'
   active + immutable memtable bytes, updated from the existing
   `approximate_size` delta points.
2. **Trigger**: when the pool crosses its ceiling, the writing DB
   first freezes/flushes *itself* if it holds the largest memtable;
   otherwise it signals the largest holder's flush (members register a
   flush-signal handle on join, deregister on close — must tolerate
   the ns-close lifecycle).
3. **Stall**: if accounting stays above a hard line while flushes are
   in flight, writers on ALL member DBs block (bounded, with the same
   fail-stop-on-bg-error discipline as the L0 stop trigger). This is
   the semantic cost center — see §5.
4. **Per-DB floors stay**: each DB keeps a minimal private
   `write_buffer_size` floor so a pool squeeze cannot starve any
   single member into zero-progress flushing.

### 4.3 vsdb-side policy assembly

- One `BlockCache` (+ later one WBM) per process, sized by the
  existing pipeline (`MEM_BUDGET`), passed to every shard of every
  engine, default namespace included.
- Per-namespace `mem_budget_mb` becomes a *guaranteed floor / weight*
  rather than a hard ceiling — or is simply superseded by the pool for
  cache purposes while remaining the WBM floor. Exact semantics decided
  at implementation time; the registry field stays (it is creation-time
  metadata either way, and upgrade compat requires reading it).
- Rollout is opt-in behind explicit assembly; `None` everywhere
  reproduces v16 behavior bit-for-bit.

## 5. Trade-offs — what a pool costs

**Isolation → elasticity is a real trade, not a free upgrade.** Static
budgets give per-namespace performance isolation: ns A's ingest burst
cannot evict ns B's cache or stall ns B's writes. A shared pool
deliberately spends that isolation to buy utilization. For epoch
rotation (idle old namespaces) the trade is nearly free; for
concurrently-hot multi-tenant namespaces it is not — a noisy neighbor
becomes possible *by design*. This is why the pool must remain
caller-assembled and opt-in, never the engine's silent default.

Secondary costs: one more atomic on the write path (accounting), LRU
contention concentrated in one moka instance instead of N (moka is
sharded internally; measure, don't assume), and a wider blast radius
for any cache bug (one poisoned pool = every namespace).

## 6. Non-goals

- **A library-internal global singleton pool** (§2 — rejected on
  form).
- **Runtime rebalancing of the existing static budgets.** Write
  buffers are sized at open; namespaces.md already records
  "VSDB never auto-rebalances budgets at runtime". The pool *replaces*
  static division where injected; it does not retrofit elasticity onto
  the static path.
- **Sharing the TableCache** (path-bound, per-DB fd bookkeeping).
- **Cross-process budgeting.** Everything here is one-process scope;
  cgroup limits remain the cross-process authority.
- **Downgrade support** — n/a; this feature is runtime-only and
  touches no on-disk format.

## 7. Compatibility

- **mmdb**: `Option` fields defaulting to `None` — existing callers
  compile and behave identically. No on-disk format change of any
  kind (cache and memtables are volatile state).
- **vsdb**: no API change required for phase 1 (assembly is internal
  to engine open); per-ns `mem_budget_mb` semantics may soften from
  ceiling to floor/weight in phase 2 — that is an observable
  behavioral change and takes a minor version + CHANGELOG contract
  note when it happens.
- Registry format: untouched (the `mem_budget_mb` field keeps its
  slot and its creation-time meaning).

## 8. Trigger conditions — when to pick this up

Do **not** implement on speculation. Start when any of:

1. **Measured intra-engine cache skew**: per-shard
   `stats.block_cache_hits/misses` (already exposed via
   `DB::property`) show sustained hit-rate divergence across shards of
   one engine under a real workload.
2. **Measured rotation pressure**: an epoch-rotation deployment where
   the filling namespace's flush/stall behavior is budget-bound
   (memtable churn at 512 MB-scale budgets) while process memory sits
   idle.
3. **An operator actually asking** for "one number that bounds the
   whole process" and being unable to express it with
   `VSDB_MEM_BUDGET_MB` + per-ns budgets.

Evidence first, phase 1 only, phase 2 only on renewed evidence.

## 9. Implementation plan (when triggered)

| Step | Layer | Content | Risk |
|------|-------|---------|------|
| 1 | mmdb | Cache-key namespacing (`db_id` in `CacheKey` + `file_offsets`), per-DB bulk invalidation on close | correctness-critical, mechanical |
| 2 | mmdb | `DbOptions.block_cache: Option<Arc<BlockCache>>` injection; capacity-field precedence docs | low |
| 3 | vsdb | Assemble one process pool in engine open; wire default + ns engines through it | low |
| 4 | — | Soak: hit-rate/eviction telemetry comparison vs static split | — |
| 5 | mmdb | WriteBufferManager: accounting, victim flush signaling, stall + fail-stop policy, close/deregister lifecycle | high — full crash-safety & error-policy re-audit of the write path |
| 6 | vsdb | Budget-semantics change (ceiling → floor/weight), CHANGELOG contract note | medium |

Each mmdb step lands with `None`-default compatibility so vsdb can
pin/roll back independently.

## 10. Open questions

1. Should the pool be one-per-process or one-per-tier (e.g. default
   namespace keeps a private pool; only rotation namespaces share)?
   Per-tier preserves default-ns isolation and confines the noisy
   -neighbor trade to the tier that wants it.
2. Pin cap per DB: needed, or is L0-first-block pinning small enough
   to ignore? (Measure in step 4.)
3. WBM victim selection: largest-memtable is the obvious policy;
   does epoch rotation want "oldest namespace first" instead?
4. Does moka's internal sharding hold up under 16–64 DBs × N threads
   hitting one instance, or does the pool need explicit external
   sharding? (Benchmark before step 2 finalizes.)
