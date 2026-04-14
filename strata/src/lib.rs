//! # vsdb
//!
//! `vsdb` is a high-performance, embedded database designed to feel like using
//! Rust's standard collections. It provides persistent, disk-backed data
//! structures — [`Mapx`] (a `HashMap`-like map), [`MapxOrd`] (a `BTreeMap`-like
//! ordered map), [`VerMap`](versioned::map::VerMap)
//! (Git-model versioned storage with branching, commits, and merge),
//! [`MptCalc`] / [`SmtCalc`] (stateless Merkle trie implementations),
//! [`VerMapWithProof`] (versioned storage with Merkle root computation),
//! [`SlotDex`] (skip-list-like index for paged queries),
//! and [`VecDex`] (approximate nearest-neighbor vector index via HNSW).
//!
//! This crate is the primary entry point for most users.
//!
//! # Why core collections don't have `len()`
//!
//! The underlying LSM-Tree engine (mmdb) does not support atomic
//! "write data + update count" across different keys.  A process crash
//! between the two leaves them inconsistent — downstream code that
//! trusts the count for index arithmetic will panic.  For this reason
//! [`Mapx`], [`MapxOrd`], and other core primitives intentionally omit
//! `len()`.
//!
//! Higher-level structures ([`VecDex`], [`SlotDex`]) **do** maintain a
//! count because they fully control their own insert/remove paths.
//! These counts are accurate during normal operation but may drift after
//! an unclean shutdown — see their respective docs.
//!
//! ## Application-layer counting
//!
//! If you need a count over a core collection, maintain it yourself:
//!
//! ```rust,ignore
//! use vsdb::MapxOrd;
//! use vsdb::{KeyEnDe, KeyEnDeOrdered, ValueEnDe};
//!
//! struct CountedMap<K: KeyEnDe + KeyEnDeOrdered, V: ValueEnDe> {
//!     map: MapxOrd<K, V>,
//!     count: usize,  // in-memory; rebuild on restart
//! }
//!
//! impl<K: KeyEnDe + KeyEnDeOrdered + Ord, V: ValueEnDe> CountedMap<K, V> {
//!     fn insert(&mut self, key: &K, value: &V) {
//!         if !self.map.contains_key(key) {
//!             self.count += 1;
//!         }
//!         self.map.insert(key, value);
//!     }
//!
//!     fn remove(&mut self, key: &K) {
//!         if self.map.contains_key(key) {
//!             self.count -= 1;
//!         }
//!         self.map.remove(key);
//!     }
//!
//!     fn len(&self) -> usize { self.count }
//!
//!     /// Rebuild from disk after an unclean shutdown.
//!     fn rebuild_count(&mut self) {
//!         self.count = self.map.iter().count();
//!     }
//! }
//! ```

#![deny(warnings)]
#![recursion_limit = "512"]

#[macro_use]
pub mod common;

/// User-facing, typed data structures (e.g., `Mapx`, `MapxOrd`).
pub mod basic;
/// Data structures for representing directed acyclic graphs (DAGs).
pub mod dagmap;
/// Git-model versioned storage: branches, commits, merge, and history.
pub mod versioned;

/// Skip-list-like index for efficient, timestamp-based paged queries.
pub mod slotdex;

/// Lightweight, stateless Merkle trie implementations (MPT + SMT).
pub mod trie;

/// Approximate nearest-neighbor vector index (HNSW algorithm).
pub mod vecdex;

// --- Re-exports ---

// Basic data structures
pub use basic::{
    mapx::Mapx, mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey, orphan::Orphan,
};

// Common traits and types — only the three user-facing encoding traits
// are re-exported.  `KeyEn`/`KeyDe`/`ValueEn`/`ValueDe` remain accessible
// via `vsdb::common::ende::*` for advanced use cases.
pub use common::{
    NULL,
    ende::{KeyEnDe, KeyEnDeOrdered, ValueEnDe},
};

// Structured error type
pub use common::error::{Result as VsdbResult, VsdbError};

// Versioned storage core types (previously not re-exported)
pub use versioned::diff::DiffEntry;
pub use versioned::handle::{Branch, BranchMut};
pub use versioned::map::VerMap;
pub use versioned::{BranchId, Commit, CommitId, NO_COMMIT};

// DAG-related structures
pub use dagmap::{DagMapId, raw::DagMapRaw, rawkey::DagMapRawKey};

// Trie
pub use trie::{MptCalc, MptProof, SmtCalc, SmtProof, TrieCalc, VerMapWithProof};

// Slotdex — re-export SlotDex64 as SlotDex for backward compatibility;
// the generic struct is still accessible as `vsdb::slotdex::SlotDex<S, K>`.
pub use slotdex::{SlotDex32, SlotDex64 as SlotDex, SlotDex64, SlotDex128, SlotType};

// VecDex — approximate nearest-neighbor vector index.
pub use vecdex::distance::{Cosine, DistanceMetric, InnerProduct, L2, Scalar};
pub use vecdex::{
    HnswConfig, VecDex, VecDexCosine, VecDexCosineF64, VecDexL2, VecDexL2F64,
};

// Re-export vsdb_core crate for advanced users, plus the user-facing
// environment management functions.
pub use vsdb_core;
pub use vsdb_core::{vsdb_flush, vsdb_get_base_dir, vsdb_set_base_dir};

// Persistent B+ tree (moved from vsdb_core).
pub use basic::persistent_btree::PersistentBTree;
