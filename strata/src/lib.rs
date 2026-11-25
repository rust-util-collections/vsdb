//! # vsdb
//!
//! `vsdb` is a high-performance, embedded database designed to feel like using
//! Rust's standard collections. It provides persistent, disk-backed data
//! structures including [`Mapx`] for exact lookups, [`MapxOrd`] for ordered
//! ranges, [`MapxOrdRawKey`] for byte keys and [`Orphan`] for single values.
//! [`VerMap`] adds branch history and snapshots, while [`DagMap`] provides
//! single-parent overlays. [`MptCalc`] and [`SmtCalc`] compute in-memory
//! Merkle commitments and proofs. [`VerMapWithProof`] connects either trie to
//! versioned storage. [`SlotDex`] indexes keys by slot for pagination and
//! counts, and [`VecDex`] provides nearest-neighbor vector search.
//!
//! This crate is the primary entry point for most users.
//!
//! # Read-only mode
//!
//! Configure read-only access before any other VSDB API, then restore handles
//! saved by a writable process:
//!
//! ```no_run
//! use vsdb::{InstanceId, Mapx, VsdbOptions, vsdb_configure};
//!
//! # fn main() -> vsdb::Result<()> {
//! vsdb_configure(VsdbOptions::read_only("/srv/my-app/vsdb"))?;
//! let id: InstanceId = "40960000".parse()?;
//! let map = Mapx::<String, String>::from_meta(id)?;
//! assert!(map.namespace().is_read_only());
//! # Ok(())
//! # }
//! ```
//!
//! The setting is one-shot and process-wide, including every namespace.
//! Query APIs and in-memory WAL recovery leave the database tree unchanged;
//! creation and mutation are unavailable.
//!
//! # Counting entries
//!
//! The basic maps expose `iter().count()`, which scans their entries.
//! [`SlotDex::total`] and [`VecDex::len`] read stored counts. Each index
//! updates its count and data rows in the same shard write batch.

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
    self, DEFAULT_NS_ID, InstanceId, MapxRaw, MapxRawReadView, MapxRawReader, Namespace,
    NamespaceOpts, NsId, NsInfo, OpenMode, VsdbOptions, vsdb_configure, vsdb_flush,
    vsdb_get_base_dir, vsdb_get_custom_dir, vsdb_open_mode,
};

// Persistent B+ tree (moved from vsdb_core).
pub use basic::persistent_btree::PersistentBTree;
