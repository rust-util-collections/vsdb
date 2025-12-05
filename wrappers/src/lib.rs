//! # vsdb
//!
//! `vsdb` is a high-performance, embedded database designed to feel like using
//! Rust's standard collections. It provides a suite of familiar data structures
//! like `Vecx` (a `Vec`-like vector) and `Mapx` (a `HashMap`-like map), all
//! backed by a persistent, on-disk key-value store.
//!
//! This crate is the primary entry point for most users.

#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

#[macro_use]
pub mod common;

/// User-facing, typed data structures (e.g., `Mapx`, `Vecx`).
pub mod basic;
/// Data structures that use multiple keys for indexing.
pub mod basic_multi_key;
/// Data structures for representing directed acyclic graphs (DAGs).
pub mod dagmap;

// --- Re-exports ---

// Basic data structures
pub use basic::{
    mapx::Mapx,
    mapx_ord::MapxOrd,
    mapx_ord_rawkey::MapxOrdRawKey,
    mapx_ord_rawvalue::MapxOrdRawValue,
    orphan::Orphan,
    // vecx::Vecx, vecx_raw::VecxRaw, // Removed - relied on unreliable len() tracking
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
