![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/vsdb)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)
[![Latest Version](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Rust Documentation](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.78+-lightgray.svg)](https://github.com/rust-random/rand#rust-version-requirements)

# vsdb

vsdb is a 'std-collection-like' database.

This is a simplified version of the original [**vsdb**](https://crates.io/crates/vsdb/0.70.0), retaining only the most practical and stable parts.

[**To view the change log check here**](https://github.com/rust-util-collections/vsdb/blob/master/CHANGELOG.md).

### Highlights

- Most APIs is similar as the coresponding data structures in the standard library
    - Use `Vecx` just like `Vec`
    - Use `Mapx` just like `HashMap`
    - Use `MapxOrd` just like `BTreeMap`
- ...

### Compilation features

- [ **DEFAULT** ] `rocks_backend`, use `rocksdb` as the backend database
  - Stable
  - C++ implementation, difficult to be compiled into a static binary
- `parity_backend`, use `parity-db` as the backend database
  - Experimental
  - Pure rust implementation, can be easily compiled into a static binary
- `msgpack_codec`, use `rmp-serde` as the codec
    - Faster running speed than json
- `json_codec`, use `serde_json` as the codec
    - Better generality and compatibility
- `compress`, enable compression in the backend database

### NOTE

- The serialized result of a vsdb instance can not be used as the basis for distributed consensus
  - The serialized result only contains some meta-information(storage paths, etc.)
  - These meta-information are likely to be different in different environments
  - The correct way is to read what you need from it, and then process the real content
