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
//! let token = std::env::args().nth(1).expect("pass the saved map ID");
//! let id: InstanceId = token.parse()?;
//! vsdb_configure(VsdbOptions::read_only("data"))?;
//! let settings = MapxRaw::from_meta(id)?;
//! # Ok::<(), vsdb_core::VsdbError>(())
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
//!         Ok(()) => Ok(true),
//!         Err(VsdbError::Storage { kind: StorageErrorKind::Busy, .. }) => Ok(false),
//!         Err(error) => Err(error),
//!     }
//! }
//! ```

#![deny(warnings)]
#![recursion_limit = "512"]

pub mod common;

pub mod basic;

pub use basic::mapx_raw::{MapxRaw, MapxRawReadView, MapxRawReader};

pub use common::{
    ColocatedEdits, DEFAULT_NS_ID, GB, InstanceId, KB, MB, Namespace, NamespaceOpts,
    NsId, NsInfo, OpenMode, RawBytes, RawKey, RawRangeBounds, RawValue, VsdbOptions,
    vsdb_configure, vsdb_flush, vsdb_get_base_dir, vsdb_get_custom_dir,
    vsdb_get_system_dir, vsdb_open_mode,
};

pub use common::error::{Result, StorageErrorKind, VsdbError};
pub use common::namespace::{
    BlockCachePoolUsage, NamespaceMemoryUsage, ShardMemoryUsage,
};

/// Maximum number of input slots in a single snapshot-bound grouped lookup.
pub const MAX_GROUPED_READ_KEYS: usize = mmdb::MAX_GROUPED_READ_KEYS;
