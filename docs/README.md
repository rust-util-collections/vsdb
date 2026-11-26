# VSDB v18 documentation

The workspace contains two runtime crates and an offline converter. Applications
use [vsdb](../strata/README.md), which builds its collections on
[vsdb_core](../core/README.md). The converter depends on both runtime crates and
runs in a separate upgrade program. [Migrating from v17/v4 or v18/v4](../migration/README.md)

## Using the database

| Task | Guide |
| --- | --- |
| Create your first map or branch | [Quick start](../README.md#quick-start) · [VerMap example](../strata/docs/api.md#vermap) |
| Pick a collection and use its public API | [Collection chooser](../strata/README.md#choosing-a-collection) · [Typed collection examples](../strata/docs/api.md) |
| Configure a universe or work with raw keys and batches | [Core API examples](../core/docs/api.md) |
| Open an existing dataset read-only | [Reader setup, collection behavior and locking](read-only.md) |
| Use branches, snapshots, merges and reference checkpoints | [Versioned storage](../strata/docs/versioned.md) |
| Build an HNSW index or search a consistent view | [VecDex](../strata/docs/vecdex.md) |
| Upgrade v17 or a v18 development universe built on MMDB v4 | [Public API changes](../CHANGELOG.md) · [Offline conversion procedure and supported handles](../migration/README.md) |

## Storage design

| Topic | Guide |
| --- | --- |
| Follow a collection operation from handle to shard, trace a VerMap commit or plan an upgrade | [v18 architecture and core flow diagrams](v18.md) |
| Placement, universe and engine identity, prefix allocation, close and relocation | [Namespaces](namespaces.md) |
| Shard-local publication, fences, captured reads and per-namespace memory | [Storage operations and sizing](storage.md) |

## Performance evidence

The [evaluation index](evaluations/README.md) groups measured results by
the change under test. It links each operation to its workload, controls,
source revisions, and reproduction instructions.

## Maintaining the workspace

Start with the [contributor guide and project skills](../AGENTS.md). The
[subsystem map](ai/review-core.md#subsystem-map) connects implementation paths
to the relevant review guides. Confirmed findings are tracked in the
[audit registry](audit.md).
