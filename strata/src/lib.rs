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

#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
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
