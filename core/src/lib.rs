//! # vsdb_core
//!
//! `vsdb_core` provides the low-level building blocks for `vsdb`, including the
//! storage layer, raw data structures, and common utilities. It is not
//! typically used directly by end-users, but forms the foundation of the `vsdb`
//! ecosystem.
//!
//! ## Storage backend
//!
//! The storage engine is MMDB, a pure-Rust LSM-Tree engine.
//!
//! ## Read-only mode
//!
//! Configure read-only access before any other VSDB API, then restore handles
//! saved by a writable process:
//!
//! ```no_run
//! use vsdb_core::{InstanceId, MapxRaw, VsdbOptions, vsdb_configure};
//!
//! # fn main() -> vsdb_core::Result<()> {
//! vsdb_configure(VsdbOptions::read_only("/srv/my-app/vsdb"))?;
//! let id: InstanceId = "40960000".parse()?;
//! let map = MapxRaw::from_meta(id)?;
//! assert!(map.namespace().is_read_only());
//! # Ok(())
//! # }
//! ```
//!
//! The setting is one-shot and process-wide, including every namespace.
//! Queries and in-memory WAL recovery do not change the database tree;
//! creation and mutation are unavailable.

#![deny(warnings)]
#![recursion_limit = "512"]

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

/// Commonly used items, re-exported for convenience.
///
/// This includes data size constants (KB, MB, GB),
/// raw data types (`RawBytes`, `RawKey`, `RawValue`), and functions for managing
/// the database environment (e.g., `vsdb_flush`, `vsdb_set_base_dir`).
pub use common::{
    BatchTrait, DEFAULT_NS_ID, GB, InstanceId, KB, MB, Namespace, NamespaceOpts, NsId,
    NsInfo, OpenMode, RawBytes, RawKey, RawValue, VsdbOptions, vsdb_configure,
    vsdb_flush, vsdb_get_base_dir, vsdb_get_custom_dir, vsdb_get_meta_dir,
    vsdb_get_system_dir, vsdb_meta_path, vsdb_ns_close, vsdb_ns_destroy, vsdb_ns_list,
    vsdb_ns_relocate, vsdb_open_mode, vsdb_set_base_dir,
};

/// The unified, structured error type of the VSDB ecosystem.
pub use common::error::{Result, VsdbError};
