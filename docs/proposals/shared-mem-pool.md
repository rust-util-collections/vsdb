# Shared memory pools — implemented design and deferred extensions

Per-engine block-cache sharing shipped in VSDB v16.3.0, using MMDB's
`BlockCachePool` introduced in v4.1.0. It is enabled for every engine,
including the default namespace. Cross-namespace pooling and a shared
write-buffer manager remain proposals, not available VSDB options.

The current integration is in
[core/src/common/engine/mmdb.rs](../../core/src/common/engine/mmdb.rs).
The MMDB internals described here match the workspace's 4.3.1 resolution;
the manifest's compatible dependency range can resolve newer versions.
Earlier RFC mechanics based on MMDB 4.0.10 are historical.

## Why share within an engine?

Prefix routing puts all entries of one collection on one shard. Under the old
private-cache split, one hot collection could fill its shard's cache while
other shards' cache capacity sat idle. All shards of an engine now share one
evictable block-cache pool. A hot shard can use the engine's cache capacity,
subject to the cache's admission and eviction policy.

Namespaces still have separate engines and pools. Opening many namespaces
therefore adds memory and worker costs. Close unused engines when rotating
datasets; there is no process-wide memory governor.

## Current sizing policy

VSDB uses fixed defaults rather than inspecting host RAM or cgroup limits:

| Engine | Shards | Memory sizing input |
|--------|--------|---------------------|
| Default namespace | 16, pinned for routing compatibility | 2 GiB; explicit nonzero `VsdbOptions::mem_budget_mb`, then `VSDB_MEM_BUDGET_MB`, overrides the default |
| Non-default namespace | 4 by default; creation clamps to 1–64 | 512 MiB; optional persisted `NamespaceOpts::mem_budget_mb` |

MB-named inputs use binary MiB. An explicit default-engine zero means unset,
so environment fallback still applies. A zero, invalid, or unrepresentable
environment value, or an unrepresentable nonzero explicit value, falls back
to 2 GiB. For a non-default engine,
conversion saturates and the effective input is floored at 8 MiB.

With effective input `B` bytes and `S` shards, integer division applies:

```text
per_shard_cache = max(B / 8 / S, 4 MiB)
engine_pool    = S * per_shard_cache

legacy_write   = min(if B > 16 GiB { B / 4 / S } else { 1 GiB / S }, 512 MiB)
write_buffer   = max(min(legacy_write, B / 8 / S), 4 MiB)
```

Each shard retains an active write buffer and may have up to four immutable
memtables awaiting flush. Before floors/caps, that allows roughly `5B/8` for
memtables and `B/8` for the evictable block cache. Floors can exceed that split
for small budgets; large budgets encounter the write-buffer cap.

These inputs do not impose a hard RSS limit. Pinned blocks, table metadata,
reverse indexes, in-flight reads, staged mutations, and application allocations
need additional headroom. Moka capacity is weighted cached value size, not total
allocator overhead. Increasing a budget can help a constrained workload, but
does not guarantee higher throughput or lower latency.

Pooling preserves the cache capacity derived by the former static split. The
per-shard `block_cache_capacity` option remains a fallback; MMDB ignores it
when an explicit shared pool is attached. Write buffers are still private to
each shard and cannot borrow a sibling's unused allowance.

## MMDB pool mechanics

The implemented injection point is `DbOptions::block_cache:
Option<Arc<BlockCachePool>>`. There is no current `write_buffer_manager` field.
VSDB constructs one pool in `MmDB::open_at` and passes it to all shard opens.

- A member view assigns each DB a pool-unique member ID. Cache keys are
  `(member_id, sst_file_number, block_offset)`, so identical SST numbers in
  different shards cannot alias.
- The evictable store is `moka::sync::SegmentedCache`, with up to 64 segments.
  Smaller capacities use fewer segments to retain useful admission capacity;
  a block still has to fit its hashed segment. It is not a global exact LRU.
- Pinned entries remain member-local, outside the evictable capacity. Their
  byte counters and locks are also local.
- A 16-way reverse index maps `(member, file)` to cached offsets. Eviction
  callbacks prune it; SST invalidation removes the relevant member's entries.
  Callbacks and invalidation must preserve the index/cache lock order.
- `detach()` invalidates the member's cached entries and turns that view into
  permanent cache-bypass mode. It is idempotent and participates in close/drop
  cleanup, including when snapshot sources outlive engine close.
- Per-DB table caches and read-site statistics remain per DB. Sharing block
  capacity does not share WALs, memtables, data identities, or transactions.

Ordinary API calls do not select pools. Namespace placement chooses the engine;
that engine owns the sharing policy. This avoids a library-global MMDB pool
that would silently couple otherwise independent users of the dependency.

## Telemetry and measurement

`Namespace::shard_properties(name)` returns one reading per shard, in shard
order. Useful MMDB property names include:

| Property | Meaning under a shared pool |
|----------|-----------------------------|
| `stats.block_cache_hits` | That shard's read-site hit counter |
| `stats.block_cache_misses` | That shard's read-site miss counter |
| `stats.cache_hit_rate` | Hit rate at that shard's read sites |
| `block-cache-usage` | Approximate entry count for the whole pool plus that shard's pinned entries; not bytes and not per-shard occupancy |

Do not sum `block-cache-usage` across shards: its pool component is repeated.
Per-member unpinned occupancy is not exposed by this property.

The original v16.3.0 A/B gate in
[cache_pool.rs](../../core/benches/cache_pool.rs) reported skewed-load latency
reductions of 72% / 81% at 1 / 8 threads, with uniform-load parity within noise
(`p > 0.6`). These are historical results from that benchmark setup, not new
measurements or promises for current deployments. The benchmark documents
the private-versus-shared protocol. Production tuning still needs hit rates,
latency, RSS, and workload-specific pressure measurements.

## Deferred topology extensions

| Tier | Sharing boundary | Status and tradeoff |
|------|------------------|---------------------|
| (i) Per engine | One namespace's shards | Implemented; shared eviction replaces private shard capacities |
| (ii) Namespace tier | Several non-default engines | Deferred; could reduce idle cache reservations during rotation, but couples tenants' eviction behavior |
| (iii) Whole process | Default and non-default engines | Deferred; rotating data could evict the default namespace's working set |

Broader sharing would need an explicit opt-in, capacity/ownership rules,
creation and close semantics, and telemetry that separates tenants. No such
VSDB option is currently wired. Static per-engine sizing does not itself
guarantee performance isolation when engines share CPUs, disks, or RAM.

Revisit tier (ii)/(iii) only with evidence that idle engine caches or rotation
churn impose a material cost. Measure the current per-engine pool first and
compare representative hot and uniform workloads, including contention and
detach cycles. No cross-namespace on-disk migration is needed merely to share
cache capacity, but public budget semantics still require compatibility review.

## Deferred write-buffer manager

A shared write-buffer manager would govern memtable accounting separately
from block-cache eviction. The earlier RFC proposed an injected shared handle;
it is a design direction, not an existing MMDB or VSDB API.

Unlike a cache pool, write-side sharing affects latency, flush scheduling,
backpressure, error handling, and crash recovery. A viable design must:

1. Account for active and immutable memtables, including transitions during
   flush, failed flush, replay, close, and member detach without double release.
2. Trigger flushes through the owning DB's normal path; accounting alone
   cannot safely flush another member while holding its write-path locks.
3. Define bounded, failure-aware backpressure. A poisoned/stopped flush worker
   must not leave other writers stalled forever waiting for memory release.
4. Keep WAL durability and write ordering unchanged, including across error
   paths and namespace close. Prove lock ordering and cancellation behavior.
5. Specify whether per-engine inputs become floors, weights, or ceilings and
   how the total interacts with cache capacity and pinned/runtime memory.

Only pursue this with measured write-side pressure that static sizing cannot
address. It requires an independent write-path/recovery audit and fault tests;
the success of read-cache pooling is not evidence for write-side safety.
Any observable contract change must follow the compatibility policy, rather
than assuming a minor release is automatically sufficient.

## Non-goals

Current pooling does not auto-detect process memory availability, resize pools
at runtime, merge namespaces, provide cross-engine atomicity, or govern all
process allocations. The implementation plan for tier (i) is complete; future
topologies and write sharing remain conditional on concrete workload evidence.
