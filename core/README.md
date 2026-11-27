# vsdb_core

[![Crates.io](https://img.shields.io/crates/v/vsdb_core.svg)](https://crates.io/crates/vsdb_core)
[![Docs.rs](https://docs.rs/vsdb_core/badge.svg)](https://docs.rs/vsdb_core)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

`vsdb_core` provides the byte-oriented storage API for VSDB. Use it directly
for `MapxRaw`, process configuration and namespace administration. For typed
collections, indexes, tries and version history, see the [vsdb crate](../strata/README.md).

## Installation

To use this checkout from a Rust application, add the crate by path.

```bash
cargo add vsdb_core --path /path/to/vsdb/core
```

VSDB stores data in MMDB, a Rust LSM-tree engine. Building its bundled Zstd
compression library requires a C toolchain.

## Storage model

One process selects one **universe** (base directory and open mode).
Its registry tracks additional namespaces, each with its own storage root.
Each raw map has an eight-byte prefix and stores all of its rows on one
MMDB shard in its namespace. The shard is the unit of atomic write
publication and WAL synchronization. [Storage architecture](../docs/architecture.md)

## Guides

| Task | Guide |
| --- | --- |
| Configure the universe, save a raw map or restore its `InstanceId` | [Core API examples](docs/api.md) |
| Write an atomic batch or query a captured state | [Batch and reader examples](docs/api.md#atomic-batches-and-durability) · [Visibility and durability](../docs/storage.md) |
| Create, close or relocate a namespace | [Namespace administration](../docs/namespaces.md) |
| Set a memory budget or inspect cache and shard usage | [Sizing and telemetry](../docs/storage.md#memory-sizing) |
| Open an existing database | [Read-only deployment](../docs/read-only.md) · [Upgrade paths](../docs/upgrade.md) |

## License

[MIT](../LICENSE)
