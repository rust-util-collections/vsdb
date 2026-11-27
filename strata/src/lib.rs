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
//! let token = std::env::args().nth(1).expect("pass the saved map ID");
//! let id: InstanceId = token.parse()?;
//! vsdb_configure(VsdbOptions::read_only("data"))?;
//! let stock = Mapx::<String, u64>::from_meta(id)?;
//! # Ok::<(), vsdb::VsdbError>(())
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

pub mod basic;
pub mod dagmap;
pub mod versioned;

pub mod slotdex;
pub mod trie;
pub mod vecdex;

pub use basic::{
    mapx::Mapx, mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey, orphan::Orphan,
};

pub use common::ende::{
    KeyEnDe, KeyEnDeOrdered, KeyLookup, OrderedBoundEndpoint, OrderedKeyLookup,
    OrderedRangeBounds, ValueEnDe,
};
pub use common::{
    MapReader, MapxOrdRawKeyReadView, MapxOrdReadView, MapxReadView, OrphanReader,
    RawRangeBounds,
};

pub use common::error::{Result, VsdbError};

pub use versioned::{
    BranchId, Commit, CommitId, DiffEntry, Snapshot,
    map::{VerMap, WorkingEdit},
};

pub use dagmap::{DagMap, DagMapId, raw::DagMapRaw, rawkey::DagMapRawKey};

pub use trie::{
    MptCalc, MptProof, ProofWithRoot, SmtCalc, SmtProof, TrieCalc, VerMapWithProof,
};

pub use slotdex::{SlotDex, SlotDex32, SlotDex64, SlotDex128, SlotType};

pub use vecdex::{
    HnswConfig, VecDex, VecDexCosine, VecDexCosineF64, VecDexDyn, VecDexL2, VecDexL2F64,
    distance::{Cosine, DistanceMetric, InnerProduct, L2, MetricKind, Scalar},
};

pub use vsdb_core::{
    self, BlockCachePoolUsage, DEFAULT_NS_ID, InstanceId, MAX_GROUPED_READ_KEYS,
    MapxRaw, MapxRawReadView, MapxRawReader, Namespace, NamespaceMemoryUsage,
    NamespaceOpts, NsId, NsInfo, OpenMode, ShardMemoryUsage, StorageErrorKind,
    VsdbOptions, vsdb_configure, vsdb_flush, vsdb_get_base_dir, vsdb_get_custom_dir,
    vsdb_open_mode,
};

pub use basic::persistent_btree::PersistentBTree;
