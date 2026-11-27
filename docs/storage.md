# Writes, read views and memory

Each map's prefix selects one shard in its namespace. The shard owns the
write-ahead log (WAL) and read sequence used for the map. A successful
ordinary batch becomes visible to new reads immediately. Use a synchronous
commit or a subsequent WAL fence when the application needs those writes
to survive power loss.
[Namespace layout](namespaces.md) · [Raw map API](../core/docs/api.md)

## Batch visibility and durability

`MapxRaw`, `Mapx`, `MapxOrd` and `MapxOrdRawKey` expose `batch()`.
Edits accumulate in memory until the caller commits. Dropping the batch
discards those edits. `MapxRaw::batch_wiped()` places a prefix-wide range
deletion before the replacements, so the wipe and replacements become
visible together.

| Operation | Effect |
| --- | --- |
| `commit()` | A nonempty batch publishes all its edits together. Acknowledgement can precede WAL synchronization. |
| `commit_sync()` | A nonempty batch synchronizes the shard WAL before publication. |
| `MapxRawBatch::commit_no_slowdown()`, `commit_sync_no_slowdown()` | A full immutable queue or L0 pressure can yield `StorageErrorKind::Busy` before a WAL record or sequence is allocated for these edits. Rebuild the consumed batch to retry. |
| `try_sync_wal()` on a map | Synchronizes preceding WAL-backed writes on its shard, including across rotation. It adds no data row or sequence. An untouched map has no assigned shard to fence. |
| `Namespace::try_sync_wal()` | Visits each shard in the namespace. Each shard has its own durability boundary. A failure can leave earlier shards synchronized. Read-only fences perform no writes. |

An empty ordinary batch checks writability and performs no write or fence,
even with `commit_sync()`. A wiped batch stages a prefix deletion, so a
wipe with zero replacements still has an effect.

An I/O error after WAL append begins can leave an ambiguous recovery
outcome. Establish the outcome before retrying an operation with
non-idempotent effects. [Batch and fence tests](../core/src/common/engine/mmdb/v5_tests.rs) · [Exported error types](../core/src/common/error.rs)

### Edits across map prefixes

`MapxRaw::insert_colocated` accepts two mutable raw handles. It verifies
that both map to the same engine and shard, then submits a single
ordinary WAL batch using each handle's own prefix. If the handles name the
same map, the rows in `here` precede the rows in `there`. DagMap uses this
operation to publish a child's owner, its edge and the parent's next edge
ID together.

`MapxRaw::write_colocated` applies puts and removals to any number of such
handles in one batch. Edits apply in order. DagMap uses it to publish a
prune's folded rows, reparented children and clearing markers together.
[Co-location tests](../core/tests/colocated.rs) · [DagMap](../strata/src/dagmap/raw/mod.rs)

## Captured reads

A map or VecDex `read_view()` borrows its handle and pins one committed
state. To continue writing through the original handle, obtain an
independent `reader()` first, then capture a view through the reader.
A reader shares the map identity and retains its namespace. Separate
views can observe different sequences. An `OrphanReader` reads its
current value on each `get()`. A VerMap snapshot owns a root lease and
can outlive the handle from which it was obtained. [Typed readers](../strata/src/common/reader.rs) · [Versioned snapshots](../strata/docs/versioned.md#branches-commits-and-snapshots)

A raw `range_detached()` can retain its captured files and memory after the
originating view drops. Raw callbacks and checked cursors lend bytes for
shorter lifetimes. [Borrowed values and fallible cursors](../core/docs/api.md#borrowed-values-and-fallible-cursors) · [Detached ranges](../core/docs/api.md#detached-ranges)

## Memory sizing

VSDB uses the budget to size caches and write buffers. Process RSS also
includes memory outside those allocations. All `*_mb` inputs use MiB
(1,048,576 bytes) and are independent of host RAM and cgroup limits.

| Namespace | Default | Override |
| --- | --- | --- |
| Default (ID zero) | 16 shards, 2 GiB | A nonzero `VsdbOptions::mem_budget_mb` takes precedence over `VSDB_MEM_BUDGET_MB`. A positive budget representable in bytes is used as supplied. |
| Additional | 4 shards, 512 MiB | Set `NamespaceOpts::shards` and `mem_budget_mb` at creation. The registry persists both settings. The effective budget has an 8 MiB floor. |

For an 8 GiB default namespace, configure the process before first use.

```rust,no_run
use vsdb_core::{VsdbOptions, vsdb_configure};

vsdb_configure(VsdbOptions::new("/data/vsdb").with_mem_budget_mb(8192)).unwrap();
```

For a namespace budget of `B` bytes and `S` shards, the formulas use
integer division.

| Resource | Configured size and owner |
| --- | --- |
| Shared block cache | One pool per namespace, with independent segmented and overflow capacities of `B / 8` each. The pool keys include the shard's member identity. |
| Write buffer | `clamp(B / 8 / S, 64 KiB, 512 MiB)` per shard. One active table and up to four immutable generations can retain memory. A large admitted batch can overshoot its buffer threshold. |
| B+ tree page cache | `clamp(B / 64, 64 KiB, 16 MiB)` per active tree runtime, split across eight cache shards. Aliases of a tree share its runtime. |

`Namespace::memory_usage()` reports both stores' configured capacities and
weighted charges once for the namespace, plus per-shard active/immutable
bytes, immutable counts and L0 pin payload. `shard_properties(name)` exposes
per-shard counters. For `block-cache-usage` each returned entry count
includes the shared evictable entries and that shard's pins. Sum per-shard
hit/miss counts when computing a namespace-wide hit ratio. Additional
namespaces, retired views, caller-held pins, worker allocations and
application data contribute to process RSS. [Adapter](../core/src/common/engine/mmdb.rs) · [Namespace telemetry](../core/src/common/namespace.rs)

The following reports the shared cache charge once for the default namespace.

```rust
use vsdb_core::Namespace;

let ns = Namespace::default_ns();
let pool = ns.memory_usage().pool;
let bytes = pool.segmented_weighted_bytes + pool.overflow_weighted_bytes;
println!("Charged cache bytes: {bytes}");
```

`ns.shard_properties("stats.block_cache_hits")` and
`ns.shard_properties("stats.block_cache_misses")` return one count per shard.
On small budgets the 64 KiB write-buffer minimum can dominate sizing.

For example, at 2 GiB across 16 shards, each write buffer is **16 MiB**.
With five potentially resident generations per shard, the nominal thresholds
total **1,280 MiB** before caches or overshoot. Historical v18 measurements
compare this sizing with a 6 MiB buffer and report measured RSS, throughput
and L0 file counts for the tested workloads. [Adapter evaluation](evaluations/v18-adapter/README.md)
