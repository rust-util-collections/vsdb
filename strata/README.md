# vsdb

[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> High-performance, embedded database with an API similar to Rust's standard collections.

`vsdb` provides typed, persistent collections backed by [mmdb](https://github.com/rust-util-collections/mmdb), a pure-Rust LSM-Tree engine. The engine and HNSW implementation are Rust; the current compression dependency includes native Zstd (`zstd-sys`), whose bundled build requires a C toolchain.

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
assert_eq!(map.get("key"), Some("value".to_string()));
```

## Read-only access

Use `vsdb_configure(VsdbOptions::read_only(path))` at process startup, then
restore handles saved by a writer with `from_meta` or serde. The mode covers
the default namespace and every non-default namespace opened in that process;
it performs reads and WAL recovery without modifying the database tree.

Collection creation and mutation are unavailable. Fallible write APIs return
`VsdbError::ReadOnly`; legacy infallible mutation APIs panic; maintenance-only
flush, GC, and deferred-delete writes are skipped. Trie caches are saved only
by explicit calls, which return the capability error in read-only mode. See
the [read-only mode guide](docs/read-only.md) for a complete example,
locking rules, immutable-snapshot requirements, and the exact API behavior.

See the **[API Examples](docs/api.md)** for typed maps, VerMap, tries, and
SlotDex, and the **[VecDex guide](docs/vecdex.md)** for vector search.

## Documentation

- [API Examples](docs/api.md) — code examples for selected collection types
- [Read-only Mode](docs/read-only.md) — configuration, supported operations, locking, and snapshots
- [Versioned Module — Architecture & Internals](docs/versioned.md) — COW B+ tree, commit DAG, three-way merge, automatic GC
- [VecDex — HNSW Vector Index](docs/vecdex.md) — configuration, distance metrics, filtered search, storage architecture

## Important Notes

- Serialized collection handles identify storage (prefix and namespace, plus type metadata); they do not contain the collection contents. Paths are resolved through the local namespace registry. Use logical data or a defined Merkle commitment for distributed consensus, and preserve the database universe when restoring handles.

## License

MIT
