# VSDB documentation

The [vsdb](../strata/README.md) crate builds its typed collections on
[vsdb_core](../core/README.md).

## Upgrade an existing universe

VSDB v19 opens a native v18 universe (MMDB format 5) in place. Stop the v18
processes and keep the base directory, all externally placed namespaces and
application files containing IDs or serialized handles together. Update the
application to v19, configure the same base and restore the saved handles.
The universe identity, shard count, collection prefixes and typed-handle
encodings stay valid. Check application queries before resuming writes.

The data upgrade path for older stores is **v17 → v18 → v19**. Complete
the conversion with the [v18 maintenance branch's upgrade guide](https://github.com/rust-util-collections/vsdb/blob/v18/migration/README.md).
The same guide covers early v18 development stores built on the previous
MMDB shard format.

## Using the database

| Task | Guide |
| --- | --- |
| Create your first map or branch | [Quick start](../README.md#quick-start) · [VerMap example](../strata/docs/api.md#vermap) |
| Pick a collection and use its public API | [Collection chooser](../strata/README.md#choosing-a-collection) · [Typed collection examples](../strata/docs/api.md) |
| Configure a universe or work with raw keys and batches | [Core API examples](../core/docs/api.md) |
| Open an existing dataset read-only | [Reader setup, collection behavior and locking](read-only.md) |
| Use branches, snapshots, merges and reference checkpoints | [Versioned storage](../strata/docs/versioned.md) |
| Build an HNSW index or search a consistent view | [VecDex](../strata/docs/vecdex.md) |

## Storage design

| Topic | Guide |
| --- | --- |
| Follow a collection operation from handle to shard or trace a VerMap commit | [Storage architecture and core flow diagrams](v18.md) |
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
