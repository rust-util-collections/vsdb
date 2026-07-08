# RFC: Shared Memory Pools — Injectable Cache & Write-Buffer Accounting

- **Status**: draft, rev 2 — vsdb side **not scheduled**; revisit when
  the trigger conditions in §8 are met. **mmdb-side mechanism (steps
  1–3) is implemented and shipped in mmdb v4.1.0** (`BlockCachePool` +
  per-member `BlockCache` views, detach lifecycle,
  `DbOptions::block_cache` injection — all `None`-default, zero
  behavior change for non-sharing callers; one design delta from this
  doc: pinned entries stayed *member-local* rather than pool-level, so
  one member's pins add zero lock traffic to other members' `get` fast
  paths). Rev 2 is a source-verified deep dive: every mechanism claim
  in §3 was checked against mmdb 4.0.10 / vsdb v16.2.0, the pool
  topology is decomposed (§4.0 — the per-engine tier turns out to need
  **no** isolation trade at all), and the mmdb-side change surface is
  specified to implementation granularity (§4.1, §4.2).
  **Pragmatic sequencing in §9.0**: step 0 + the Q1 benchmark are
  recommended *now* (pre-commitment); vsdb wiring (steps 4–5) is the
  recommended first batch; tier (ii) and phase 2 are deferred.
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
   `budget/8/shards` each, write buffer likewise (`mmdb_open`). And the
   skew is **structural, not exotic**: routing is `prefix % shards`
   (`Engine::shard`, `core/src/common/engine/mmdb.rs`) — **one
   collection lives entirely inside one shard**. The most ordinary
   workload shape there is — one big hot map — therefore runs on 1/16
   of the engine's cache while 15 sibling caches idle. This cost exists
   **today, inside the default namespace**, independent of any
   namespace question.
2. **Across namespaces, budgets cannot flow.** Epoch rotation — the
   headline namespace workload — has exactly one namespace hot (the
   one being filled) while the rest idle. The filling namespace is
   pinned to its creation-time 512 MB even when the process ceiling
   has tens of GB of headroom.
3. **No single number controls the process.** Total footprint ≈
   default-engine budget + Σ per-namespace budgets; `VSDB_MEM_BUDGET_MB`
   governs only the first term. (Documented as an ops rule in
   `strata/docs/api.md` since v16.2.0; a rule is not a mechanism.)

The correct end-state — *if* these costs are ever observed in
production rather than in theory — is to solve this in **mmdb**, at
the mechanism layer. This RFC records that design so the evaluation
does not have to be redone.

### 1.1 How big is the prize? (sizing arithmetic)

Under a binding budget B, `mmdb_open` sizes per engine: block cache
`B/8` total, write buffers `B/8` total with a worst case of
`5 × B/8` (1 active + 4 immutable memtables per shard). So:

- **Phase 1 (cache pooling) governs exactly the `B/8` slice.** Its win
  is *allocation* of that slice, not total memory: a fully skewed read
  set gains up to **shards× effective cache** (16× for the default
  engine) for the same footprint.
- **Phase 2 (write-buffer accounting) governs up to the worst-case
  `5B/8` slice** — the dominant share, which is why it carries the
  risk budget (§4.2) and the higher evidence bar (§8).

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
    pub block_cache: Option<Arc<BlockCachePool>>,
    /// `None` (default) ⇒ per-DB `write_buffer_size` trigger only —
    /// today's behavior, unchanged.
    pub write_buffer_manager: Option<Arc<WriteBufferManager>>,
}
```

vsdb then assembles the topology (§4.0/§4.3), sized by the existing
`MEM_BUDGET` pipeline, passed to every shard it opens. At that point
`VSDB_MEM_BUDGET_MB` genuinely bounds the whole process, and the
unification the two-path split could not deliver happens at the layer
that can.

## 3. Current mechanics (verified against source, mmdb 4.0.10)

Facts the design must build on. Each was re-verified for rev 2; the
ones marked **(new)** were discovered in the rev-2 deep dive and
materially shape the design.

### Cache side

- **BlockCache is per-DB and internally constructed**
  (`src/db.rs:340`): `Arc::new(BlockCache::new(options.block_cache_capacity))`.
  moka 0.12 (`sync` feature) LRU keyed by `(sst_file_number,
  block_offset)` (`src/cache/block_cache.rs:11`). File numbers are
  **per-DB manifest counters** — two DBs both own an SST numbered 5,
  so sharing one cache today would collide keys and *return wrong
  data*. Key namespacing is a hard correctness prerequisite, not
  tuning.
- **(new) Every consumer holds `Arc<BlockCache>` and calls the same
  five methods** — `get` / `insert` / `insert_pinned` / `unpin_file` /
  `invalidate_file` (+ `entry_count` for the stats property). Key
  construction is concentrated: the read path collapses into
  `TableReader::read_block_cached_opt` (`src/sst/table_reader/mod.rs:552`
  — all six iterator call sites route through it), pinning into
  `pin_metadata_in_cache` (one site), invalidation into ~8 sites
  (`db.rs` ×6, `compaction/leveled.rs` ×2). This makes a **per-DB
  façade** (§4.1) a zero-blast-radius refactor: consumers keep their
  field type and calls.
- **(new) `file_offsets` is a single `Mutex<HashMap>` per cache**
  (`block_cache.rs:19,44`), taken on **every insert** (read-miss path)
  and by the moka **eviction listener**. moka 0.12 sync runs eviction
  listeners on *caller* threads during cache maintenance (piggybacked
  on inserts/gets, plus explicit `run_pending_tasks`). Today that
  mutex is per-DB scope; naively pooling N DBs concentrates N DBs ×
  all their reader/writer threads onto **one** mutex on the read-miss
  path. The pool must shard this index (§4.1 step 2) — this is a
  rev-2 design delta beyond key namespacing.
- **(new) `invalidate_file` calls `inner.run_pending_tasks()`**
  (`block_cache.rs:170`) — on a pool, that drains the *whole pool's*
  pending maintenance queue on the calling thread. Bounded by making
  per-member bulk invalidation (detach) batch its invalidations and
  run maintenance once.
- **Pinned entries never evict**: `insert_pinned` — in practice only
  the first data block of each L0 file
  (`TableReader::pin_metadata_in_cache`). Sizing: ≤ one `block_size`
  (16 KB) block per L0 file, and L0 is bounded by
  `l0_stop_trigger = 12` — worst case ≈ **200 KB per shard**. Pinned
  volume is structurally trivial; a per-member pin *cap* is
  over-engineering, a per-member pinned-bytes *counter* suffices
  (resolves former open question 2).
- **Hit/miss stats are per-DB and counted in the TableReader**
  (`read_block_cached_opt` → `DbStats.block_cache_hits/misses`,
  exposed via `DB::property("stats.block_cache_*")`). Pooling does
  **not** merge stats: per-shard hit rates stay measurable after
  pooling — the soak comparison in §9 step 5 depends on this.
- **TableCache is per-DB and path-bound**
  (`TableCache::new_with_stats(&path, ...)`, load-coalescing via moka
  `try_get_with`) — NOT shareable, out of scope; it merely *holds*
  the cache handle it is given, so the façade flows through it
  untouched.

### Write side

- **Flush trigger is a per-DB size check under the DB's own mutex**
  (`src/db.rs:2717`):
  `active_memtable.approximate_size() >= options.write_buffer_size`,
  with `max_immutable_memtables = 4` bounding frozen tables — worst
  case ≈ 5 × write_buffer_size per shard. Memtables already track
  `approximate_size: AtomicUsize`, so global accounting can hook the
  existing deltas cheaply.
- **(new) There is NO background flush thread.** mmdb's only
  background threads are compaction workers
  (`mmdb-compaction-{i}`, `db.rs:586`; vsdb configures 1 per shard).
  Memtable flushes execute **inline**: on the write-group leader when
  the size trigger fires (`db.rs:2717` → freeze, drop lock, build
  SSTs, reacquire, install), or client-driven (`DB::flush`, `close`,
  `compact_range`). Consequence: a WriteBufferManager **cannot
  "signal the largest holder to flush"** — an idle member has no
  thread to run its flush. The victim policy must be *self-flush*
  (§4.2), or mmdb must grow a shared flush executor (explicitly out
  of the baseline, §6).
- **Write throttle is L0-count based, not memory based**
  (`maybe_throttle_writes`: slowdown at 8, inline `drain_l0` at stop
  trigger 12, **fail-stop** if that drain fails). A memory-based
  stall is a *new* throttle dimension that must compose with the
  existing error policy (`bg_error`, MANIFEST poison — the most
  heavily audited code in mmdb).
- **DB lifetimes are dynamic** since ns-close (v16.1.0): `DB::close`
  (idempotent via `closed` CAS) freezes/flushes and syncs the WAL; it
  does not touch the cache today. `Drop` best-effort syncs the WAL
  and joins compaction threads. Any pool membership must be detached
  from **both** paths, idempotently, and must tolerate members
  closing at arbitrary times without dangling references.
- **Detached snapshot iterators may outlive `close`**
  (`MapxRaw::range_detached`, vsdb): they hold `Arc<TableReader>`s,
  which hold the cache handle. A detached member's view must degrade
  to cache-bypass (serve reads from disk, insert nothing) rather than
  repopulate dead entries under a retired member id (§4.1 step 4).

### vsdb side

- **(new) Trigger condition 1 (§8) is not currently measurable.**
  mmdb exposes per-shard `stats.block_cache_hits/misses` via
  `DB::property`, but vsdb's engine layer (`core/src/common/engine/`)
  exposes **no property passthrough** — an operator cannot read
  per-shard hit rates through vsdb at all. Measurement infrastructure
  is pre-work (§9 step 0), independent of any pooling decision.

## 4. Design

### 4.0 Topology decomposition — three tiers, one mechanism

Rev 1 asked "one pool per process, or per tier?" (former open
question 1). The deep dive resolves it by decomposing the topology.
One mechanism (the injectable pool) serves three distinct sharing
scopes with **different trade profiles**:

| Topology | Who shares | Solves | Isolation trade |
|----------|-----------|--------|-----------------|
| (i) **Per-engine pool** — the N shards of ONE engine share one pool | one namespace's own shards | cost #1 (intra-engine skew, incl. the default namespace) | **None.** The shards are one tenant: same dataset, same failure domain, same budget. Nothing is newly coupled. |
| (ii) **Namespace-tier pool** — all non-default namespaces share one pool | rotation/secondary namespaces | cost #2 (cache side), cost #3 for the ns tier ("one number bounds all namespaces") | Real but confined: noisy neighbor *among non-default namespaces*, opted into explicitly. Default namespace untouched. |
| (iii) **Whole-process pool** — everything shares | all engines | cost #3 maximally | Maximal: rotation churn can evict the default namespace's hot set. |

Recommended rollout: **(i) is the sleeper win** — it addresses the
structural single-hot-collection skew for every existing deployment
with zero semantic change (same `B/8` total, one pool instead of N
slices) and zero cross-tenant coupling; only the contention question
(§10 Q1) gates making it the eventual default. **(ii) is the
namespace story**, opt-in. **(iii) stays available** to a policy that
wants it (the mechanism cannot tell the difference) but is not a
recommended vsdb default — it spends the default namespace's
isolation for tail utilization.

This also settles the interaction with the ¾-derating question
(evaluated 2026-07, kept at ¾): under (ii), "how much may all
namespaces collectively use for cache" becomes one explicit number,
which is the principled answer to "many namespaces squeeze the
headroom" — instead of re-purposing the default engine's safety
margin.

### 4.1 Phase 1 — shared BlockCache (read-side elasticity)

The 20%-effort / 80%-value piece: no write-path risk, immediately
addresses cost #1 and the read half of #2.

**Shape: pool + per-member façade.** The public type `BlockCache`
*becomes the per-member view*; the pool is a new type. Consumers
(`TableReader`, `TableCache`, compaction, `db.rs` teardown paths)
keep their `Arc<BlockCache>` fields and five-method call surface
unchanged — the entire external blast radius is `DB::open`
construction, `DbOptions`, and close/drop detach.

```rust
pub struct BlockCachePool {
    inner: moka::sync::Cache<(u64, u64, u64), CacheValue>, // (member, file, offset)
    pinned: Mutex<HashMap<(u64, u64, u64), CacheValue>>,
    pinned_count: AtomicUsize,
    /// Rev-2 delta: sharded to keep the read-miss path scalable
    /// (single-mutex version verified as a pool-global serial point).
    file_offsets: [Mutex<HashMap<(u64, u64), HashSet<u64>>>; FO_SHARDS],
    next_member: AtomicU64,
    disabled: bool, // capacity 0 — inherited by every view
}

pub struct BlockCache {            // the per-member view (name kept!)
    pool: Arc<BlockCachePool>,
    member: u64,
    detached: AtomicBool,
    pinned_bytes: AtomicU64,       // observability; no cap (see §3)
}
```

1. **Key namespacing**: pool keys are `(member, file_number,
   block_offset)`; `file_offsets` keyed by `(member, file_number)`,
   sharded `FO_SHARDS` ways (16 or 32; benchmark) by
   `hash(member, file_number)` so inserts and the eviction listener
   lock 1/FO_SHARDS of the index. The moka weigher still counts value
   bytes; +8 B of key per entry is noise against 16 KB blocks.
2. **Constructors**:
   `BlockCache::new(capacity)` — allocates a fresh single-member pool
   internally: today's behavior and signature, bit-for-bit, so every
   existing mmdb caller (and vsdb with `block_cache: None`) is
   untouched. `BlockCachePool::new(capacity)` +
   `pool.attach() -> BlockCache` — the sharing path; member ids from
   the pool's own counter (no global state).
3. **Injection**: `DbOptions.block_cache: Option<Arc<BlockCachePool>>`;
   `DB::open` does `pool.attach()` when `Some`, else
   `BlockCache::new(block_cache_capacity)`. Document loudly:
   `block_cache_capacity` is **ignored** when a pool is supplied
   (capacity is the pool's).
4. **Detach lifecycle**: `view.detach()` — sets the `detached` flag,
   removes the member's pinned entries, sweeps its `file_offsets`
   shards, batch-invalidates its blocks, runs maintenance **once**.
   Idempotent. Called from `DB::close` *and* `Drop` (either may come
   first; ns-close calls close explicitly, leak paths hit Drop).
   After detach the view is a cache-bypass: `get` misses, `insert`
   returns the block without caching (the detached-iterator rule from
   §3). This also prevents a rotated-out namespace's dead blocks from
   squatting in the pool until LRU pressure notices.
5. **Observability**: `entry_count`/pinned-bytes per member (derived
   from the member's `file_offsets` shards — property queries are
   cold paths), pool-level totals for capacity planning.

### 4.2 Phase 2 — WriteBufferManager (write-side elasticity)

The high-risk piece; only justified by demonstrated memory pressure.
Rev 2 reshapes it around the verified **no-background-flush** reality
(§3): the original "signal the largest holder" plan is not
implementable — an idle member has no thread to flush on.

1. **Accounting**: a shared `AtomicUsize` of all members' active +
   immutable memtable bytes. Hooks at the three existing transition
   points, not inside `MemTable`: write-apply (leader knows the
   batch's byte delta), `install_flush` (subtract the flushed frozen
   size), close/detach (subtract everything remaining).
2. **Trigger — self-flush policy**: when the pool is over its soft
   ceiling, the **writing** DB freezes/flushes *itself* before
   admitting the write (it is the one with a thread on the scene, and
   under rotation the hot writer is almost always the largest holder
   anyway — the two victim policies coincide exactly where the
   feature matters). Known degraded mode, documented: a member that
   wrote a fat memtable and went permanently idle keeps its bytes
   pinned until *its* next write, `flush`, or `close`; the pool
   ceiling honestly accounts them, shrinking headroom for active
   members. vsdb-side mitigations already exist (`vsdb_flush`,
   `flush_all_open`, close-on-rotation flushes the retiring
   namespace).
3. **Stall — bounded, poison-aware, soft**: if accounting stays above
   the hard line after self-flush (only possible via idle-member
   pinning), writers wait on the WBM condvar with a bounded timeout,
   re-checking `check_usable()` per wake so a member's `bg_error` /
   MANIFEST poison fails ITS writers fast instead of wedging the
   pool; on timeout the write is admitted with a warning (soft
   enforcement). Hard enforcement without a flush executor would
   convert one idle member into a pool-wide livelock — rejected.
4. **Per-DB floors stay**: each member keeps a minimal private
   `write_buffer_size` floor so a pool squeeze cannot starve any
   single member into zero-progress flushing.
5. **Escalation path (out of baseline)**: if multi-tenant fairness
   ever demands true victim selection, the clean design is a shared
   flush-executor thread owned by the WBM (members register/deregister
   flushable handles on attach/detach) — RocksDB's shape. That is a
   new concurrency component with its own audit cost; it needs its
   own evidence, on top of phase 2's (§8).

### 4.3 vsdb-side policy assembly

- **Step (i)** — per-engine pool: `Engine::open_at` builds ONE
  `BlockCachePool` of `budget/8` per engine and attaches every shard.
  No env vars, no semantic change, no registry change; totals match
  the static split bit-for-bit. Gated only on the §10 Q1 benchmark +
  §9 step-5 soak.
- **Step (ii)** — namespace-tier pool, opt-in: a single explicit knob
  (e.g. `VSDB_NS_CACHE_POOL_MB`; exact spelling decided at
  implementation) assembles one pool shared by every **non-default**
  namespace engine. When active, a member namespace's `mem_budget_mb`
  keeps sizing its write buffers (unchanged) while its cache slice is
  superseded by the pool — the budget softens from full ceiling to
  "write-side ceiling + pooled cache", an observable change that
  takes a minor version + CHANGELOG contract note. Unset ⇒ v16
  behavior exactly. The registry format is untouched either way.
- **Phase 2 wiring** (when it exists) follows the same tier shape;
  budget semantics change (ceiling → floor/weight) is phase-2-gated
  and carries its own contract note.
- Tests: vsdb's parallel suite already shares one default engine per
  process; a per-engine pool changes no cross-test coupling class.
  Capacity-sensitive assertions must target pool totals, not
  per-shard slices.

## 5. Trade-offs — what a pool costs

**Isolation → elasticity is a real trade at tier (ii)/(iii), and NOT
a trade at tier (i).** Static budgets give per-namespace performance
isolation: ns A's ingest burst cannot evict ns B's cache or stall
ns B's writes. A cross-namespace pool deliberately spends that
isolation to buy utilization. For epoch rotation (idle old
namespaces) the trade is nearly free; for concurrently-hot
multi-tenant namespaces it is not — a noisy neighbor becomes possible
*by design*. This is why cross-namespace pooling must remain
caller-assembled and opt-in, never the engine's silent default. The
per-engine tier is exempt: shards of one engine were never isolated
from each other in any way that mattered (one tenant, one budget) —
pooling them only removes an arbitrary internal fence.

Quantified benefit ceiling (from §1.1): phase 1 reallocates the
`B/8` cache slice (up to shards× effective cache under full skew);
phase 2 governs up to `5B/8` of worst-case memtable footprint.

Secondary costs, updated by the deep dive:

- One more atomic on the write path (phase-2 accounting).
- LRU contention concentrated in one moka instance instead of N —
  **plus** the `file_offsets` index and eviction-listener work, which
  rev 2 identified as the sharper concentration point and §4.1
  designs around (sharding); both still need the §10 Q1 benchmark.
- Detach cost on close: a member sweep of the pool (rare, batched,
  single maintenance pass) replaces dropping a private cache (free).
- A wider blast radius for any cache bug: one poisoned pool = every
  member namespace. The façade keeps the correctness-critical surface
  (key namespacing) small and mechanically auditable.

## 6. Non-goals

- **A library-internal global singleton pool** (§2 — rejected on
  form).
- **A background flush executor in the phase-2 baseline** (§4.2.5 —
  recorded as the escalation path, needs its own evidence; the
  baseline is strictly self-flush + soft stall).
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

- **mmdb**: `BlockCache::new(capacity)` keeps its exact signature and
  single-member semantics; `DbOptions` grows `Option` fields
  defaulting to `None` — existing callers compile and behave
  identically. No on-disk format change of any kind (cache and
  memtables are volatile state).
- **vsdb**: tier (i) has no observable API or semantic change (same
  totals, better allocation). Tier (ii) and phase 2 soften per-ns
  budget semantics — each is an observable behavioral change taking a
  minor version + CHANGELOG contract note when (and only when) it
  ships.
- Registry format: untouched (the `mem_budget_mb` field keeps its
  slot and its creation-time meaning).
- Rollout/rollback: every step lands `None`-default, so vsdb can pin
  or revert each tier independently, and mmdb remains fully usable
  by non-vsdb callers throughout.

## 8. Trigger conditions — when to pick this up

Do **not** implement on speculation. Start when any of:

1. **Measured intra-engine cache skew**: per-shard
   `stats.block_cache_hits/misses` show sustained hit-rate divergence
   across shards of one engine under a real workload. ⚠ Requires §9
   step 0 first — vsdb currently exposes no property passthrough, so
   this evidence **cannot be collected today**.
2. **Measured rotation pressure**: an epoch-rotation deployment where
   the filling namespace's flush/stall behavior is budget-bound
   (memtable churn at 512 MB-scale budgets) while process memory sits
   idle.
3. **An operator actually asking** for "one number that bounds the
   whole process" (or the namespace tier) and being unable to express
   it with `VSDB_MEM_BUDGET_MB` + per-ns budgets.

Evidence first. Tier (i) on trigger 1; tier (ii) on trigger 2/3;
phase 2 only on renewed, write-side evidence.

## 9. Implementation plan (when triggered)

### 9.0 Pragmatic cut — recommended first vs. deferred

The net-effect analysis (2026-07) splits the plan into three buckets
by benefit sign and risk shape:

**Do now — pre-commitment work, shippable anytime, no trigger
needed:**

- **Step 0** (vsdb property passthrough). Trivial, zero risk,
  independently useful for ops, and §8 trigger 1 is *unevaluable*
  without it. Implementing measurement is not implementing the
  feature.
- **The §10 Q1 contention microbenchmark.** Analysis, not commitment:
  it decides whether tier (i) can ever be default-on, and picks
  FO_SHARDS. Cheap to build against a prototype pool outside the
  engine.

**First implementation batch — steps 1–5 (phase 1 + tier (i)), once
trigger 1 fires** *or* the Q1 benchmark shows no uniform-load
regression on a representative skewed workload:

- This is the only bucket whose net effect is
  **positive-or-neutral on every axis**: no isolation trade (one
  tenant), no write-path change, no crash-semantics change, no
  on-disk change, identical memory totals; the sole residual risks
  are implementation-time key-isolation correctness (small, audited,
  volatile state) and the uniform-load contention question that the
  Q1 benchmark exists to retire. Cost #1 is structural
  (`prefix % shards`), so the payoff does not depend on an exotic
  workload materializing.

**Deferred — explicitly not in the first batch:**

- **Step 6, tier (ii)** (ns-tier pool): wait for §8 trigger 2/3. It
  buys elasticity with a real (if confined and opt-in) noisy-neighbor
  trade, and its stability upside (hard cache cap for the ns tier)
  only matters to deployments that actually run many namespaces.
- **Steps 7–8, phase 2 WBM**: hard-deferred until renewed
  *write-side* evidence. Highest variance in the whole design —
  touches the most heavily audited code (write path, bg_error /
  MANIFEST-poison interplay), and its soft stall makes the ceiling
  best-effort rather than guaranteed. Expected-positive for rotation
  ingest, but the expectation is not worth the audit cost until the
  pressure is observed.

| Step | When | Layer | Content | Risk |
|------|------|-------|---------|------|
| 0 | **now** | vsdb | **Measurement pre-work** (may ship any time, independently): property passthrough `Engine → DB::property` for per-shard cache hit/miss; optional aggregate helper | trivial |
| 1 | **✅ done (mmdb v4.1.0)** | mmdb | `BlockCachePool` + façade `BlockCache` view: key namespacing `(member, file, offset)`, **sharded** `file_offsets`, per-member pinned-bytes counter; *delta vs. plan: pinned entries kept member-local (zero cross-member lock traffic on the pinned fast path)* | correctness-critical, mechanical; call-site blast radius ≈ 0 (§4.1) |
| 2 | **✅ done (mmdb v4.1.0)** | mmdb | Detach lifecycle: idempotent `detach()` from `close` + `Drop`, batched invalidation + single maintenance pass, cache-bypass mode for detached views | low |
| 3 | **✅ done (mmdb v4.1.0)** | mmdb | `DbOptions.block_cache: Option<Arc<BlockCachePool>>` injection; capacity-precedence docs; contention benchmark (§10 Q1) gating defaults | low |
| 4 | first batch | vsdb | Tier (i): per-engine pool in `Engine::open_at` (all namespaces, default included) | low |
| 5 | first batch | — | Soak: per-shard hit-rate/eviction telemetry vs static split (uses step 0) | — |
| 6 | **deferred** (trigger 2/3) | vsdb | Tier (ii): opt-in ns-tier pool + budget-semantics contract note | medium |
| 7 | **deferred hard** (write-side evidence) | mmdb | Phase 2 WBM baseline: accounting hooks, self-flush trigger, bounded poison-aware soft stall, attach/detach lifecycle | high — full crash-safety & error-policy re-audit of the write path |
| 8 | **deferred hard** (with 7) | vsdb | Phase 2 wiring + budget semantics change (ceiling → floor/weight), CHANGELOG contract note | medium |

Each mmdb step lands with `None`-default compatibility so vsdb can
pin/roll back independently.

## 10. Open questions

Rev 2 resolved three of the original four (former Q1 → §4.0 topology
decomposition; former Q2 → pinning is bounded at ~200 KB/shard by
`l0_stop_trigger`, counter without cap; former Q3 → self-flush, victim
selection needs a flush executor that is out of baseline). Remaining:

1. **Pool contention under fan-in** (sharpened from former Q4): does
   one moka instance + FO_SHARDS-way `file_offsets` hold up under
   16–64 members × N threads, measured on the read-miss path and
   eviction-listener path specifically? Benchmark before step 3
   finalizes; also picks FO_SHARDS (16 vs 32) and decides whether
   tier (i) can become the default rather than opt-in.
2. **Per-member `entry_count` derivation cost**: scanning the
   member's `file_offsets` shards on a property query is O(member's
   files); confirm this is acceptable for monitoring cadence or add a
   per-member counter.
3. **Tier-(ii) knob spelling**: env var vs `NamespaceOpts`-level pool
   handle vs a vsdb-level `PoolConfig`. Decide at implementation with
   the constraint-zero rule: zero ceremony on the everyday path,
   explicit at the admin tier.
