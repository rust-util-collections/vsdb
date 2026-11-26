# VSDB v18 documentation

The workspace contains two runtime crates and an offline converter. Applications
use [vsdb](../strata/README.md), which builds its collections on
[vsdb_core](../core/README.md). The converter depends on both runtime crates and
runs in a separate upgrade program. [Migrating from v17/v4 or v18/v4](../migration/README.md)

## Using the database

| Task | Guide |
| --- | --- |
| Create your first map or branch | [Quick start](../README.md#quick-start) |
| Pick a collection and use its public API | [Typed collection examples](../strata/docs/api.md) |
| Configure a universe, place data, or work with raw keys and batches | [Core API examples](../core/docs/api.md) |
| Open an existing dataset read-only | [Application operations](../strata/docs/read-only.md) · [Engine locking and recovery](../core/docs/read-only.md) |
| Use branches, snapshots, merges and reference checkpoints | [Versioned storage](../strata/docs/versioned.md) |
| Build an HNSW index or search a consistent view | [VecDex](../strata/docs/vecdex.md) |
| Upgrade v17 or a v18 development universe built on MMDB v4 | [Public API changes](../CHANGELOG.md) · [Offline conversion procedure and supported handles](../migration/README.md) |

## Storage design

| Topic | Guide |
| --- | --- |
| v18 collection formats, backend integration and validation | [v18 on MMDB v5](v18.md) |
| Placement, universe and engine identity, prefix allocation, close and relocation | [Namespaces](namespaces.md) |
| Shard-local publication, fences, captured reads and per-namespace memory | [Storage operations and sizing](storage.md) |

## Performance evidence

The v18/v4 and v18/v5 measurements use identical logical workloads on
x86_64 Linux. Each report identifies the source binaries, fixture, units
and control variation. [Upgrade results by operation](v18.md#measured-performance)

- [Adapter allocations, write throughput and 16-shard memory](evaluations/v18-adapter/README.md)
- [DagMap child publication, VerMap working edits and foreground writes](evaluations/v18-publication/README.md)
- [Stock B+ tree point and range reads](evaluations/v18-tree-v5/stock/README.md)
- [Complete VecDex queries, storage reads and pin ownership](evaluations/v18-vecdex-v5/README.md)
- [v17 to v18 collection-layout comparisons with MMDB v4 held constant](evaluations/v18-collection-layouts.md)

## Maintaining the workspace

Start with the [contributor guide and project skills](../AGENTS.md). The
[subsystem map](ai/review-core.md#subsystem-map) connects implementation paths
to the relevant review guides. Confirmed findings are tracked in the
[audit registry](audit.md).
