//! # vsdb
//!
//! `vsdb` provides persistent collections for embedded Rust applications.
//!
//! - Use [`Mapx`] for exact lookups, [`MapxOrd`] for ordered ranges,
//!   [`MapxOrdRawKey`] for byte keys and [`Orphan`] for a single value.
//! - Use [`VerMap`] for branch history and snapshots. [`PersistentBTree`]
//!   exposes a copy-on-write tree with application-managed roots.
//! - [`MptCalc`] and [`SmtCalc`] compute Merkle roots and proofs in memory.
//!   [`VerMapWithProof`] connects either calculator to a versioned map.
//! - [`SlotDex`] pages and counts keys by slot. [`DagMap`] overlays local
//!   changes on a parent. [`VecDex`] searches vectors by distance.
//!
//! ## Read-only mode
//!
//! Set the process-wide open mode before opening a collection or requesting a
//! database directory. Restore a handle saved by a writable process.
//!
//! ```no_run
//! use vsdb::{InstanceId, Mapx, VsdbOptions, vsdb_configure};
//!
//! fn open_counts(id: InstanceId) -> vsdb::Result<Mapx<String, u64>> {
//!     vsdb_configure(VsdbOptions::read_only("/srv/my-app/vsdb"))?;
//!     Mapx::from_meta(id)
//! }
//! ```
//!
//! ## Counting entries
//!
//! The basic maps expose `iter().count()`, which scans their entries.
//! [`SlotDex::total`] and [`VecDex::len`] read stored counts. Each index
//! updates its count and data rows in the same shard write batch. For raw
//! batches, write pressure and namespace telemetry, see [`vsdb_core`].

#![deny(warnings)]
#![recursion_limit = "512"]

pub mod common;

/// User-facing, typed data structures (e.g., `Mapx`, `MapxOrd`).
pub mod basic;
/// Data structures for representing directed acyclic graphs (DAGs).
pub mod dagmap;
/// Git-model versioned storage: branches, commits, merge, and history.
pub mod versioned;

/// Fixed-width slot index with tiered counts and paged queries.
pub mod slotdex;

/// In-memory Merkle trie implementations (MPT and SMT) and versioned proofs.
pub mod trie;

/// Approximate nearest-neighbor vector index (HNSW algorithm).
pub mod vecdex;

// --- Re-exports ---

// Basic data structures
pub use basic::{
    mapx::Mapx, mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey, orphan::Orphan,
};

// Key codecs, borrowed lookups and typed read views.
pub use common::ende::{
    KeyEnDe, KeyEnDeOrdered, KeyLookup, OrderedBoundEndpoint, OrderedKeyLookup,
    OrderedRangeBounds, ValueEnDe,
};
pub use common::{
    MapReader, MapxOrdRawKeyReadView, MapxOrdReadView, MapxReadView, OrphanReader,
    RawRangeBounds,
};

// The unified, structured error type of the whole VSDB ecosystem
// (defined in `vsdb_core`, shared by both crates).
pub use common::error::{Result, VsdbError};

// Versioned storage core types (previously not re-exported)
pub use versioned::{
    BranchId, Commit, CommitId, DiffEntry, Snapshot,
    map::{VerMap, WorkingEdit},
};

// DAG-related structures
pub use dagmap::{DagMap, DagMapId, raw::DagMapRaw, rawkey::DagMapRawKey};

// Trie
pub use trie::{
    MptCalc, MptProof, ProofWithRoot, SmtCalc, SmtProof, TrieCalc, VerMapWithProof,
};

// Slotdex — `SlotDex` is the generic struct (`SlotDex<S, K>`); the
// width-specific aliases pin the slot type.
pub use slotdex::{SlotDex, SlotDex32, SlotDex64, SlotDex128, SlotType};

// VecDex — approximate nearest-neighbor vector index.
pub use vecdex::{
    HnswConfig, VecDex, VecDexCosine, VecDexCosineF64, VecDexDyn, VecDexL2, VecDexL2F64,
    distance::{Cosine, DistanceMetric, InnerProduct, L2, MetricKind, Scalar},
};

// Re-export vsdb_core crate for advanced users, plus the user-facing
// environment management functions.
pub use vsdb_core::{
    self, BlockCachePoolUsage, DEFAULT_NS_ID, InstanceId, MAX_GROUPED_READ_KEYS,
    MapxRaw, MapxRawReadView, MapxRawReader, Namespace, NamespaceMemoryUsage,
    NamespaceOpts, NsId, NsInfo, OpenMode, ShardMemoryUsage, StorageErrorKind,
    VsdbOptions, vsdb_configure, vsdb_flush, vsdb_get_base_dir, vsdb_get_custom_dir,
    vsdb_open_mode,
};

// Persistent B+ tree (moved from vsdb_core).
pub use basic::persistent_btree::PersistentBTree;
