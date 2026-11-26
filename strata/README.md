# vsdb

[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

`vsdb` provides typed, persistent collections for Rust. VSDB uses the MMDB
storage engine through `vsdb_core`. Building its bundled Zstd compression
dependency requires a C toolchain.

## Getting started

From an application using a local checkout:

```bash
cargo add vsdb --path /path/to/vsdb/strata
```

```rust
use vsdb::Mapx;

let mut map = Mapx::<String, String>::new();
map.insert(&"key".to_owned(), &"value".to_owned());
assert_eq!(map.get("key").as_deref(), Some("value"));

let id = map.save_meta().unwrap();
let restored = Mapx::<String, String>::from_meta(id).unwrap();
assert_eq!(restored.get("key"), map.get("key"));
```

Keep the returned `InstanceId` as an application entry point. A saved handle
identifies a collection in its database universe. A backup must include the
data in the owning namespace as well as the metadata.

## Choosing a collection

| When you need | Use |
|---|---|
| Exact key lookups | [`Mapx<K, V>`](docs/api.md#mapx) |
| Sorted iteration and range queries | [`MapxOrd<K, V>`](docs/api.md#mapxord), or `MapxOrdRawKey<V>` for byte keys |
| One persistent value | [`Orphan<T>`](docs/api.md#single-values-and-byte-keys) |
| Branches, commits, snapshots and three-way merges | [`VerMap<K, V>`](docs/versioned.md) |
| Merkle commitments and membership or absence proofs | In-memory [`MptCalc` or `SmtCalc`](docs/api.md#mptcalc--smtcalc-merkle-trie), or `VerMapWithProof<K, V, T>` for versioned state |
| Offset-based pagination and counts over ordered slots | [`SlotDex<S, K>`](docs/api.md#slotdex) |
| Parent overlays with local overrides, deletions and pruning | [`DagMap<K, V>`](docs/api.md#dagmap), `DagMapRawKey<V>` or `DagMapRaw` |
| Approximate nearest-neighbor search | [`VecDex<K, D, S>`](docs/vecdex.md), or `VecDexDyn<K, S>` for runtime metric selection |

## Guides

- [Collection examples](docs/api.md) cover common operations, reader handles and WAL durability. See [namespace placement](../docs/namespaces.md) to select a storage root.
- [Read-only mode](../docs/read-only.md) explains how to restore and query typed collections in a read-only process.
- [Versioned storage](docs/versioned.md) describes branch operations, merge rules, the B+ tree layout and recovery.
- [Vector search](docs/vecdex.md) covers HNSW configuration, filtering and persisted graph state.
- [Upgrade an existing universe](../docs/README.md#upgrade-an-existing-universe) covers reopening existing data and saved handles.

## License

MIT
