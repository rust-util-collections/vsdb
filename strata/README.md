# vsdb

[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> High-performance, embedded database with an API similar to Rust's standard collections.

`vsdb` provides typed, persistent collections backed by [mmdb](https://github.com/rust-util-collections/mmdb), a pure-Rust LSM-Tree engine. No C/C++ toolchain required.

## Features

| Collection | Description |
|-----------|-------------|
| `Mapx<K, V>` | Persistent `HashMap`-like KV store |
| `MapxOrd<K, V>` | Persistent `BTreeMap`-like KV store with sorted iteration |
| `VerMap<K, V>` | Git-model versioning: branches, commits, three-way merge, rollback |
| `MptCalc` / `SmtCalc` | Merkle Patricia Trie and Sparse Merkle Tree for cryptographic state commitments |
| `VerMapWithProof<K, V, T>` | VerMap + trie integration for versioned Merkle roots |
| `SlotDex<S, K>` | Timestamp-based paged index with skip-list-like tier acceleration |
| `VecDex<K, D>` | HNSW approximate nearest-neighbor vector search (L2, Cosine, InnerProduct; `VecDexDyn` for runtime metric selection) |
| `DagMapRaw` / `DagMapRawKey<V>` | DAG-based collections |

## Installation

```bash
cargo add vsdb
```

## Quick example

```rust
use vsdb::Mapx;

let mut map: Mapx<String, String> = Mapx::new();
map.insert(&"key".to_string(), &"value".to_string());
assert_eq!(map.get(&"key".to_string()), Some("value".to_string()));
```

## Read-only access

Use `vsdb_configure(VsdbOptions::read_only(path))` at process startup, then
restore handles saved by a writer with `from_meta` or serde. The mode covers
the default namespace and every non-default namespace opened in that process;
it performs reads and WAL recovery without modifying the database tree.

Collection creation and mutation are unavailable. Fallible write APIs return
`VsdbError::ReadOnly`; legacy infallible mutation APIs panic; maintenance-only
flush, GC, deferred-delete, and automatic trie-cache writes are skipped;
explicit trie-cache saves return the capability error. See
the [read-only mode guide](docs/read-only.md) for a complete example,
locking rules, immutable-snapshot requirements, and the exact API behavior.

For detailed examples covering all collection types (VerMap, Merkle tries, VecDex, SlotDex, and more), see the **[API Examples](docs/api.md)**.

## Documentation

- [API Examples](docs/api.md) — code examples for all collection types
- [Read-only Mode](docs/read-only.md) — configuration, supported operations, locking, and snapshots
- [Versioned Module — Architecture & Internals](docs/versioned.md) — COW B+ tree, commit DAG, three-way merge, automatic GC
- [VecDex — HNSW Vector Index](docs/vecdex.md) — configuration, distance metrics, filtered search, storage architecture

## Important Notes

- The serialized output of a `vsdb` instance cannot be used for distributed consensus. The serialized data contains meta-information (like storage paths) that may differ across environments. Read the required data and process the raw content instead.

## License

MIT
