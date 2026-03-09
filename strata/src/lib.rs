//! # vsdb
//!
//! `vsdb` is a high-performance, embedded database designed to feel like using
//! Rust's standard collections. It provides persistent, disk-backed data
//! structures — [`Mapx`] (a `HashMap`-like map), [`MapxOrd`] (a `BTreeMap`-like
//! ordered map), [`VerMap`](versioned::map::VerMap)
//! (Git-model versioned storage with branching, commits, and merge),
//! [`MptCalc`] / [`SmtCalc`] (stateless Merkle trie implementations),
//! [`VerMapWithProof`] (versioned storage with Merkle root computation),
//! and [`SlotDex`] (skip-list-like index for paged queries).
//!
//! This crate is the primary entry point for most users.

#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

#[cfg(all(feature = "msgpack_codec", feature = "cbor_codec"))]
compile_error!(
    "features `msgpack_codec` and `cbor_codec` are mutually exclusive; use `--no-default-features --features cbor_codec` to switch codec"
);

#[cfg(all(
    feature = "serde_ende",
    not(any(feature = "msgpack_codec", feature = "cbor_codec"))
))]
compile_error!(
    "feature `serde_ende` requires exactly one codec feature: `msgpack_codec` or `cbor_codec`"
);

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

// --- Re-exports ---

// Basic data structures
pub use basic::{
    mapx::Mapx, mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey, orphan::Orphan,
};

// Common traits and types
pub use common::{
    NULL,
    ende::{KeyDe, KeyEn, KeyEnDe, KeyEnDeOrdered, ValueDe, ValueEn, ValueEnDe},
};

// DAG-related structures
pub use dagmap::{DagMapId, raw::DagMapRaw, rawkey::DagMapRawKey};

// Trie
pub use trie::{MptCalc, SmtCalc, SmtProof, VerMapWithProof};

// Slotdex
pub use slotdex::SlotDex;

// Re-export all of vsdb_core for convenience
pub use vsdb_core::{self, *};
