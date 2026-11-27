//! # vsdb_core
//!
//! Use [`MapxRaw`] for persistent byte keys and values, [`VsdbOptions`] to
//! configure the process, and [`Namespace`] to place and administer maps.
//! The `vsdb` crate builds typed collections on these APIs.
//!
//! ## Storage backend
//!
//! Each map prefix routes to one MMDB shard in its owning namespace. MMDB is
//! a Rust LSM-tree engine. Building its bundled Zstd backend requires a C
//! toolchain.
//!
//! ## Read-only mode
//!
//! Configure the base directory before opening collections or requesting
//! database paths. Pass a token previously saved by a writable process.
//!
//! ```no_run
//! use vsdb_core::{InstanceId, MapxRaw, VsdbOptions, vsdb_configure};
//!
//! fn open_reader(id: InstanceId) -> vsdb_core::Result<MapxRaw> {
//!     vsdb_configure(VsdbOptions::read_only("/srv/my-app/vsdb"))?;
//!     MapxRaw::from_meta(id)
//! }
//! ```
//!
//! The open mode applies to every namespace in the process. Read-only queries
//! and in-memory WAL recovery leave the stored files untouched.
//!
//! ## Handling write pressure
//!
//! A raw batch can request immediate refusal when admission would stall on
//! the immutable queue or L0 pressure. A [`StorageErrorKind::Busy`] refusal
//! consumes the batch before it writes any rows or allocates a sequence.
//! The caller can rebuild it from the original edits and retry.
//!
//! ```
//! use vsdb_core::{MapxRaw, StorageErrorKind, VsdbError};
//!
//! fn try_enqueue(map: &mut MapxRaw) -> vsdb_core::Result<bool> {
//!     let mut batch = map.batch();
//!     batch.insert(b"job", b"ready");
//!     match batch.commit_no_slowdown() {
//!         Err(VsdbError::Storage { kind: StorageErrorKind::Busy, .. }) => Ok(false),
//!         outcome => outcome.map(|()| true),
//!     }
//! }
//! ```

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
pub use basic::mapx_raw::{MapxRaw, MapxRawReadView, MapxRawReader};

/// Commonly used items, re-exported for convenience.
///
/// This includes data size constants (KB, MB, GB),
/// raw data types (`RawBytes`, `RawKey`, `RawValue`), and functions for managing
/// the database environment (e.g., `vsdb_flush`, `vsdb_configure`).
pub use common::{
    ColocatedEdits, DEFAULT_NS_ID, GB, InstanceId, KB, MB, Namespace, NamespaceOpts,
    NsId, NsInfo, OpenMode, RawBytes, RawKey, RawRangeBounds, RawValue, VsdbOptions,
    vsdb_configure, vsdb_flush, vsdb_get_base_dir, vsdb_get_custom_dir,
    vsdb_get_system_dir, vsdb_open_mode,
};

/// The unified, structured error type of the VSDB ecosystem.
pub use common::error::{Result, StorageErrorKind, VsdbError};
pub use common::namespace::{
    BlockCachePoolUsage, NamespaceMemoryUsage, ShardMemoryUsage,
};

/// Maximum number of input slots in a single snapshot-bound grouped lookup.
pub const MAX_GROUPED_READ_KEYS: usize = mmdb::MAX_GROUPED_READ_KEYS;
