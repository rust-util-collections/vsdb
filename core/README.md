# vsdb_core

[![Crates.io](https://img.shields.io/crates/v/vsdb_core.svg)](https://crates.io/crates/vsdb_core)
[![Docs.rs](https://docs.rs/vsdb_core/badge.svg)](https://docs.rs/vsdb_core)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

`vsdb_core` provides the byte-oriented storage API for VSDB. Use it directly
for `MapxRaw`, process configuration and namespace administration. For typed
collections, indexes, tries and version history, see the [vsdb crate](../strata/README.md).

## Installation

From an application using a local checkout:

```bash
cargo add vsdb_core --path /path/to/vsdb/core
```

VSDB stores data in MMDB, a Rust LSM-tree engine. Building its bundled Zstd
compression library requires a C toolchain.

## Storage model

One process selects one **universe** (base directory and open mode). Its default
namespace uses 16 shards. Each additional namespace has its own root, MMDB
shards, WALs and memory sizing. The universe owns the namespace registry
and allocates collection prefixes across all its namespaces.

A namespace uses MMDB v5 with seven levels and an eight-byte prefix.
Each raw map stores byte keys and values under one prefix on one shard.
A native shard fence makes earlier WAL-backed writes durable. Checked
co-located inserts can publish rows under two map prefixes in one batch.
`Namespace::memory_usage()` reports its shared pool and individual shards.
`InstanceId` identifies the map within the universe. Persist it with
`save_meta` and use `from_meta` to reopen the same rows.

## Guides

- [Raw map examples](docs/api.md) cover configuration, handle restoration, batch writes and snapshot reads. See [namespace administration](../docs/namespaces.md) for placement and relocation.
- [Read-only mode](../docs/read-only.md) covers immutable snapshots, process locking and recovery.
- [Upgrade an existing universe](../docs/README.md#upgrade-an-existing-universe) covers reopening native v18 data.

## License

[MIT](../LICENSE)
