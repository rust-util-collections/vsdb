# VSDB v18 documentation

The workspace contains two runtime crates and an offline converter. Applications
use [vsdb](../strata/README.md), which builds its collections on
[vsdb_core](../core/README.md). The converter depends on both runtime crates and
runs in a separate upgrade program. [Migrating from v17](../migration/README.md)

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

| Topic | What the document covers |
| --- | --- |
| [v18 roadmap](proposals/v18-roadmap.md) | Storage design by subsystem, the initial performance evaluation, and the status of follow-up work |
| [Joint MMDB v5 and VSDB v18 proposal](https://github.com/rust-util-collections/mmdb/blob/master/docs/proposals/mmdb-v5-vsdb-v18-improvements.md) | Shard-local publication, graph reads, memory budgets and the upgrade sequence. The [MMDB execution record](https://github.com/rust-util-collections/mmdb/blob/master/docs/proposals/mmdb-v5-execution-plan.md) records the engine decisions. The [VSDB port and verification](proposals/v18-mmdb-v5-execution.md) covers the actual adapter, collections and offline migration. Both documents are in `../mmdb/docs/proposals/` in sibling checkouts. |
| [Namespaces](proposals/namespaces.md) | Placement, universe and engine identities, prefix allocation and recovery |
| [Namespace close](proposals/ns-close.md) | Engine ownership, close refusal and in-process teardown |
| [Memory sizing](proposals/shared-mem-pool.md) | Per-engine block cache, per-shard buffers, telemetry and proposed extensions |
| [Synchronous batches and read views](proposals/durable-batches-and-read-views.md) | Single-prefix durability, snapshot lifetimes and reader handles |

## Performance evidence

The initial v18 evaluations compare against v17 code at `da9a325`. Each report
records the workload and build used for its measurements. Later implementation
work is tracked in the [roadmap](proposals/v18-roadmap.md).

- [Basic map operations](evaluations/v18-basic.md)
- [SlotDex and DagMap](evaluations/v18-slotdex-dagmap.md)
- [Merkle tries and VecDex](evaluations/v18-trie-vecdex.md)
- [VerMap and reference-count persistence](evaluations/v18-versioned.md)

Earlier records cover [ACC migration primitives](evaluations/acc-migration-primitives.md)
and [Hotmint integration on v17](evaluations/hotmint-v17.md).

## Maintaining the workspace

Start with the [contributor guide and project skills](../AGENTS.md). The
[subsystem map](ai/review-core.md#subsystem-map) connects implementation paths
to the relevant review guides. Confirmed findings are tracked in the
[audit registry](audit.md).
