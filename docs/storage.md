# Writes, read views and memory

The runtime uses MMDB v5. A map's eight-byte prefix selects one shard in
its namespace. That shard owns the WAL and read sequence used for the map.
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
| `commit_sync()` | A nonempty batch synchronizes the shard WAL before publication. An empty ordinary batch checks writability and performs no write or fence. |
| `MapxRawBatch::commit_no_slowdown()`, `commit_sync_no_slowdown()` | A full immutable queue or L0 pressure can yield `StorageErrorKind::Busy` before a WAL record or sequence is allocated for these edits. Rebuild the consumed batch to retry. |
| `try_sync_wal()` on a map | Synchronizes preceding WAL-backed writes on its shard, including across rotation. It adds no data row or sequence. An untouched map has no assigned shard to fence. |
| `Namespace::try_sync_wal()` | Visits each shard in the namespace. Each shard has its own durability boundary. A failure can leave earlier shards synchronized. Read-only fences perform no writes. |

An I/O error after WAL append begins can leave an ambiguous recovery
outcome. Establish the outcome before retrying an operation with
non-idempotent effects. [Batch and fence tests](../core/src/common/engine/mmdb/v5_tests.rs) · [Exported error types](../core/src/common/error.rs)

`MapxRaw::insert_colocated` accepts two mutable raw handles. It verifies
that both map to the same engine and shard, then submits a single
asynchronous WAL batch using each handle's own prefix. If the handles
name the same map, the rows in `here` precede the rows in `there`. DagMap uses this operation to publish a child's owner,
its edge and the parent's next edge ID together. `MapxRaw::write_colocated`
applies puts and removals to any number of such handles with the same
checks and one batch; edits apply in order. DagMap uses it to publish a
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

Use `try_get_with` to consume a value while the backing bytes are pinned.
A missing key skips the callback. An empty value invokes it with an empty
slice. A raw view's `try_visit_many` resolves up to 32 slots at one sequence
before visiting the results in input order. Distinct keys can retain entire
blocks or memtable arenas until the visit returns. The checked iterator
steps `try_step_next` and `try_step_prev` lend a row until the next cursor
operation and propagate late I/O errors. `range_detached()` retains its
captured sources even after the originating view drops. [Raw reads and ranges](../core/docs/api.md#immutable-iteration-alongside-writes) · [Adapter fixtures](../core/tests/v5_adapter_test.rs)

## Memory sizing

The default namespace has 16 shards and a 2 GiB sizing input. A nonzero
`VsdbOptions::mem_budget_mb` overrides `VSDB_MEM_BUDGET_MB`. A named
namespace defaults to 4 shards and 512 MiB. Its creation options persist
in the registry. MB inputs use binary MiB. Default-engine zero means
unset, while a named engine's effective input is floored at 8 MiB.
The default engine uses a positive representable budget as given. VSDB
uses the configured sizes independently of detected host RAM or cgroup limits.
For an 8 GiB default namespace, configure the process before first use.

```rust,no_run
use vsdb_core::{VsdbOptions, vsdb_configure};

vsdb_configure(VsdbOptions::new("/data/vsdb").with_mem_budget_mb(8192)).unwrap();
```

For a namespace budget `B` bytes and `S` shards, integer division applies:

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

For example, `ns.shard_properties("stats.block_cache_hits")` and
`ns.shard_properties("stats.block_cache_misses")` return one count per shard.
On small budgets the 64 KiB write-buffer minimum can dominate sizing.

At 2 GiB with 16 shards, the selected write buffer is **16 MiB**. The
6 MiB alternative had 27.60% lower hot-shard saturation throughput.
With five potentially resident generations per shard the nominal
thresholds total **1,280 MiB** before caches or overshoot. In a separate
all-active read/write fixture about 248 MiB of segmented cache remained
resident and process peak RSS reached 588.08 MiB. That run ended with
48 L0 files from 32 at the start. [Measured sizing, RSS and remaining work](evaluations/v18-adapter/README.md)
