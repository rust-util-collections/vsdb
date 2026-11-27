# vsdb

[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/rust-util-collections/vsdb/blob/master/LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

`vsdb` provides persistent maps, version history and specialized indexes for
Rust applications. Its storage layer is MMDB through `vsdb_core`. Building
MMDB's bundled Zstd compression dependency requires a C toolchain.

## Getting started

To use this checkout from a Rust application, add the crate by path.

```bash
cargo add vsdb --path /path/to/vsdb/strata
```

The [quick start](https://github.com/rust-util-collections/vsdb/blob/master/README.md#quick-start) writes a map, makes its rows
durable and reopens it using a saved `InstanceId`.

## Choosing a collection

| When you need | Use |
|---|---|
| Exact key lookups | [`Mapx<K, V>`](https://github.com/rust-util-collections/vsdb/blob/master/strata/docs/maps.md#mapx) |
| Sorted iteration and range queries | [`MapxOrd<K, V>`](https://github.com/rust-util-collections/vsdb/blob/master/strata/docs/maps.md#mapxord), or [`MapxOrdRawKey<V>`](https://github.com/rust-util-collections/vsdb/blob/master/strata/docs/maps.md#single-values-and-byte-keys) for byte keys |
| One persistent value | [`Orphan<T>`](https://github.com/rust-util-collections/vsdb/blob/master/strata/docs/maps.md#single-values-and-byte-keys) |
| A copy-on-write B+ tree with application-managed root IDs | [`PersistentBTree`](https://github.com/rust-util-collections/vsdb/blob/master/strata/docs/versioned.md#standalone-tree-roots) |
| Branches, commits, snapshots and three-way merges | [`VerMap<K, V>`](https://github.com/rust-util-collections/vsdb/blob/master/strata/docs/versioned.md) |
| Merkle commitments and membership or absence proofs | In-memory [`MptCalc` or `SmtCalc`](https://github.com/rust-util-collections/vsdb/blob/master/strata/docs/proofs.md#standalone-tries), or [`VerMapWithProof<K, V, T>`](https://github.com/rust-util-collections/vsdb/blob/master/strata/docs/proofs.md#versioned-proofs) for versioned state |
| Offset-based pagination and counts over ordered slots | [`SlotDex<S, K>`](https://github.com/rust-util-collections/vsdb/blob/master/strata/docs/slotdex.md) |
| Parent overlays with local overrides, deletions and pruning | [`DagMap<K, V>`, `DagMapRawKey<V>` or `DagMapRaw`](https://github.com/rust-util-collections/vsdb/blob/master/strata/docs/dagmap.md) |
| Approximate nearest-neighbor search | [`VecDex<K, D, S>`](https://github.com/rust-util-collections/vsdb/blob/master/strata/docs/vecdex.md), or `VecDexDyn<K, S>` for runtime metric selection |

For placement, read-only deployment, upgrades and memory sizing, see the
[database operations guides](https://github.com/rust-util-collections/vsdb/blob/master/docs/README.md#operate-a-database).

## License

[MIT](https://github.com/rust-util-collections/vsdb/blob/master/LICENSE)
