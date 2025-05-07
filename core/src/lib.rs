//! # vsdb_core
//!
//! `vsdb_core` provides the low-level building blocks for `vsdb`, including storage
//! engine abstractions, raw data structures, and common utilities. It is not
//! typically used directly by end-users, but forms the foundation of the `vsdb`
//! ecosystem.

#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

/// Manages storage backends, raw data types, and shared utilities.
///
/// This module provides the `Engine` trait for abstracting over different
/// key-value stores (like RocksDB and ParityDB), along with fundamental
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

/// Commonly used items, re-exported for convenience.
///
/// This includes data size constants (KB, MB, GB), a null terminator constant (`NULL`),
/// raw data types (`RawBytes`, `RawKey`, `RawValue`), and functions for managing
/// the database environment (e.g., `vsdb_flush`, `vsdb_set_base_dir`).
pub use common::{
    GB, KB, MB, NULL, RawBytes, RawKey, RawValue, vsdb_flush, vsdb_get_base_dir,
    vsdb_get_custom_dir, vsdb_set_base_dir,
};
