//! # vsdb_core
//!
//! `vsdb_core` provides the low-level building blocks for `vsdb`, including the
//! storage layer, raw data structures, and common utilities. It is not
//! typically used directly by end-users, but forms the foundation of the `vsdb`
//! ecosystem.
//!
//! ## Storage backend
//!
//! The storage engine is selected at compile time via feature flags:
//! - `backend_mmdb` (default): uses MMDB (pure-Rust LSM-Tree engine)
//! - `backend_rocksdb`: uses RocksDB (C++ FFI)

#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

#[cfg(all(feature = "backend_rocksdb", feature = "backend_mmdb"))]
compile_error!(
    "features `backend_rocksdb` and `backend_mmdb` are mutually exclusive; \
     use `--no-default-features --features backend_rocksdb` to switch backend"
);

#[cfg(not(any(feature = "backend_rocksdb", feature = "backend_mmdb")))]
compile_error!(
    "exactly one storage backend must be enabled: `backend_rocksdb` or `backend_mmdb`"
);

/// Manages the storage layer, raw data types, and shared utilities.
///
/// This module provides the storage engine along with fundamental
/// types such as `RawKey`, `RawValue`, and environment management functions.
pub mod common;

/// Contains raw, untyped data structures.
///
/// This module provides `MapxRaw`, a basic, high-performance key-value map that
/// operates on raw bytes. It serves as the foundation for the typed, user-facing
/// collections in the `vsdb` crate.
pub mod basic;

/// A raw, high-performance, disk-backed key-value map.
pub use basic::mapx_raw::MapxRaw;

/// A persistent B+ tree with copy-on-write structural sharing.
pub use basic::persistent_btree::PersistentBTree;

/// Commonly used items, re-exported for convenience.
///
/// This includes data size constants (KB, MB, GB), a null terminator constant (`NULL`),
/// raw data types (`RawBytes`, `RawKey`, `RawValue`), and functions for managing
/// the database environment (e.g., `vsdb_flush`, `vsdb_set_base_dir`).
pub use common::{
    BatchTrait, GB, KB, MB, NULL, RawBytes, RawKey, RawValue, vsdb_flush,
    vsdb_get_base_dir, vsdb_get_custom_dir, vsdb_set_base_dir,
};
