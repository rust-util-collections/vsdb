# VSDB documentation

[Write and reopen a map](../README.md#quick-start)

## Build an application

- [Choose a typed collection](../strata/README.md#choosing-a-collection)
- [Store raw bytes, batch writes and read snapshots](../core/docs/api.md#mapxraw)

## Operate a database

| Task | Guide |
| --- | --- |
| Select the base directory and open mode | [Process configuration](../core/docs/api.md#configure-the-universe) |
| Place collections on another volume, or close or relocate a namespace | [Namespace placement and lifecycle](namespaces.md) |
| Query a database from a separate read-only process | [Read-only deployment](read-only.md) |
| Decide when edits become visible and when they survive power loss | [Batch visibility and durability](storage.md#batch-visibility-and-durability) |
| Choose cache and write-buffer sizes or interpret memory telemetry | [Memory sizing](storage.md#memory-sizing) |
| <a id="upgrade-an-existing-universe"></a>Reopen or convert an older store | [Upgrade paths and backup steps](upgrade.md) |

## Understand the storage model

Follow a collection handle to its shard, a batch to its write-ahead log, or
a VerMap edit to a history commit in the [architecture diagrams](architecture.md).
For on-disk records, see the [namespace directory layout](namespaces.md#directory-layout-and-recovery)
and [VerMap page and metadata layout](../strata/docs/versioned.md#storage-layout).

## Project records and maintenance

| Looking for | Start here |
| --- | --- |
| Changes by release | [Changelog](../CHANGELOG.md) |
| Performance results for a particular revision and workload | [Evaluation index](evaluations/README.md) |
| Recorded workspace checks for v18 revision `1b51384` | [Linux/x86_64 validation receipt](validation/v18-linux-x86_64.json) |
| Build commands, coding conventions and project workflows | [Contributor guide](../AGENTS.md) |
| Implementation paths, review guidance and known findings | [Subsystem map](ai/review-core.md#subsystem-map) · [Audit registry](audit.md) |
| Supported platforms, toolchain and persisted-format policy | [Compatibility policy](ai/compatibility-policy.md) |
