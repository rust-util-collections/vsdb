[![Latest Version](https://img.shields.io/crates/v/vsdb_core.svg)](https://crates.io/crates/vsdb_core)
[![Rust Documentation](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb_core)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.60+-lightgray.svg)](https://github.com/rust-random/rand#rust-version-requirements)

# vsdb_core

The core implementations of [**vsdb**](https://crates.io/crates/vsdb).

### Design Principles

Based on the underlying one-dimensional linear storage structure (native kv-database, such as sled/rocksdb, etc.), multiple different namespaces are divided, and then abstract each dimension in the multi-dimensional logical structure based on these divided namespaces.

In the category of kv-database, namespaces can be expressed as different key ranges, or different key prefix.

This is the same as expressing complex data structures in computer memory(the memory itself is just a one-dimensional linear structure).

User data will be divided into two dimensions: 'branch' and 'version', the functions of the 'basic' category are stateless, and the functions of the 'versioned' category are stateful. In the internal implementation, each stateful function is implemented based on its corresponding stateless function,
all stateful data has two additional identification dimensions ('branch' and 'version'), somewhat like the logic in Git. Stateless functions do not have the feature of 'version' management, but they have higher performance.

### LICENSE

- [**MIT**](https://choosealicense.com/licenses/mit) for v0.40 and earlier
- [**GPL-3.0**](../LICENSE) for v0.41 and later
