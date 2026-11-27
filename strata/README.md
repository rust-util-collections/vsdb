# vsdb

[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

`vsdb` provides persistent maps, version history and specialized indexes for
Rust applications. Its storage layer is MMDB through `vsdb_core`. Building
MMDB's bundled Zstd compression dependency requires a C toolchain.

## Getting started

To use this checkout from a Rust application, add the crate by path.

```bash
cargo add vsdb --path /path/to/vsdb/strata
```

The [quick start](../README.md#quick-start) writes a map, makes its rows
durable and reopens it using a saved `InstanceId`.

## Choosing a collection

| When you need | Use |
|---|---|
| Exact key lookups | [`Mapx<K, V>`](docs/maps.md#mapx) |
| Sorted iteration and range queries | [`MapxOrd<K, V>`](docs/maps.md#mapxord), or [`MapxOrdRawKey<V>`](docs/maps.md#single-values-and-byte-keys) for byte keys |
| One persistent value | [`Orphan<T>`](docs/maps.md#single-values-and-byte-keys) |
| A copy-on-write B+ tree with application-managed root IDs | [`PersistentBTree`](docs/versioned.md#standalone-tree-roots) |
| Branches, commits, snapshots and three-way merges | [`VerMap<K, V>`](docs/versioned.md) |
| Merkle commitments and membership or absence proofs | In-memory [`MptCalc` or `SmtCalc`](docs/proofs.md#standalone-tries), or [`VerMapWithProof<K, V, T>`](docs/proofs.md#versioned-proofs) for versioned state |
| Offset-based pagination and counts over ordered slots | [`SlotDex<S, K>`](docs/slotdex.md) |
| Parent overlays with local overrides, deletions and pruning | [`DagMap<K, V>`, `DagMapRawKey<V>` or `DagMapRaw`](docs/dagmap.md) |
| Approximate nearest-neighbor search | [`VecDex<K, D, S>`](docs/vecdex.md), or `VecDexDyn<K, S>` for runtime metric selection |

For placement, read-only deployment, upgrades and memory sizing, see the
[database operations guides](../docs/README.md#operate-a-database).

## License

[MIT](../LICENSE)
