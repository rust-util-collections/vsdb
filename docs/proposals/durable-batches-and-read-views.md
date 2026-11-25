# Synchronous batches and collection read views

A batch publishes several edits to one collection together. A read view lets a caller query that collection repeatedly at one committed state. The two APIs can be used independently. The raw primitives and typed synchronous batches originated in v17. The v18 implementation adds typed map views, an Orphan reader and vector index views. [Raw API](../../core/docs/api.md) · [Collection API](../../strata/docs/api.md)

## Batch publication and WAL durability

`MapxRaw`, `Mapx`, `MapxOrd` and `MapxOrdRawKey` expose `batch()`. The raw map also exposes `batch_wiped()` to replace the contents of a prefix in one batch. The caller stages edits and consumes the batch using one of two commit methods. Dropping an uncommitted batch discards its edits.

| Operation | When readers can see the changes | Durability boundary |
| --- | --- | --- |
| `commit()` | All edits become visible together | WAL synchronization may follow publication |
| `commit_sync()` | All edits become visible together after the shard WAL sync succeeds | A successful nonempty commit acknowledges synchronized WAL data |
| `try_sync_wal()` on the map | Previously published edits can already be observed | Fences prior writes on the map's shard |

The engine submits one MMDB write batch with the requested sync option. Colocated maps share the shard WAL, but a VSDB batch addresses one map prefix. An empty ordinary batch performs capability checks and is a no-op. A wiped batch includes a prefix deletion even when the caller adds no entries. [Raw batch implementation](../../core/src/basic/mapx_raw/mod.rs) · [MMDB integration](../../core/src/common/engine/mmdb.rs)

A capability or preflight size rejection leaves the batch unapplied. An I/O error after the WAL write starts has an ambiguous recovery outcome. The caller must establish the outcome before repeating an operation that would have a second effect. The collection error retains the storage cause. [Error types](../../core/src/common/error.rs)

## Read handles and captured state

| Collection | Independent handle from `reader()` | What can be captured |
| --- | --- | --- |
| `MapxRaw` | `MapxRawReader` | `MapxRawReadView` for point and byte-range queries |
| `Mapx`, `MapxOrd`, `MapxOrdRawKey` | `MapReader<Self>` | A typed view for point queries and iteration. Ordered maps also expose ranges |
| `Orphan<T>` | `OrphanReader<T>` | `get()` reads the current value on each call |
| `VecDex`, including `VecDexDyn` | `MapReader<Self>` | `VecDexReadView` for repeated graph queries at one sequence |
| `VerMap` | `VerMapReader<K, V>` | An owned `Snapshot<K, V>` of a working branch or a commit |

For a raw map, typed map or vector index, `map.read_view()` borrows the handle directly. To keep writing through that handle, obtain an independent reader first, then call `reader.read_view()`. The reader retains the namespace. Its view holds an MMDB snapshot guard that pins the captured sequence while other writers advance. Separate calls to `read_view()` capture independent sequences. The normal mutation and alias contracts still apply. [Raw reader](../../core/src/basic/mapx_raw/mod.rs) · [Typed readers](../../strata/src/common/reader.rs) · [Vector views](../../strata/src/vecdex/reader.rs)

A view's ordinary iterators borrow it. A raw view also offers `range_detached()`, which retains that range's captured sources independently. Dropping views and iterators releases their pins and allows old data to be reclaimed. Owned VerMap snapshots pin a B+ tree root and can outlive their originating handle. [Raw view and iterator lifetimes](../../core/src/common/engine/mod.rs) · [Versioned snapshots](../../strata/src/versioned/read.rs) · [Namespace close](ns-close.md)

## Design evidence

The original consumer needed to acknowledge a durable update to several related keys, and to join related records at one consistent instant. Separate application engines, service acknowledgements and cross-WAL dependencies stay under the application's control. The September 2026 ACC report compares engine topologies and records a paused-fsync experiment that distinguished synchronous publication from an ordinary commit followed by a fence. Its timings and dependency versions belong to that assessment. [ACC validation report](../evaluations/acc-migration-primitives.md)

Workspace tests cover batch visibility, process reopen, read view consistency and namespace ownership. The engine API includes compile-fail lifetime examples. [Synchronous batch tests](../../core/tests/sync_batch_test.rs) · [Read view tests](../../core/tests/read_view_test.rs) · [Iterator snapshot tests](../../core/tests/iterator_snapshot_test.rs)
