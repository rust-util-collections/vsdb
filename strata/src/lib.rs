//! # vsdb
//!
//! `vsdb` is a high-performance, embedded database designed to feel like using
//! Rust's standard collections. It provides persistent, disk-backed data
//! structures — `Mapx` (a `HashMap`-like map), `MapxOrd` (a `BTreeMap`-like
//! ordered map), and [`VersionedMap`](versioned::map::VersionedMap)
//! (Git-model versioned storage with branching, commits, and merge).
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

// Re-export all of vsdb_core for convenience
pub use vsdb_core::{self, *};
