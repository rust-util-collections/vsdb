#![doc = include_str!("../README.md")]
#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

/// Contains basic data structures with typed keys and values.
pub mod basic;
/// Contains basic data structures with multiple keys.
pub mod basic_multi_key;
/// Contains common utilities and traits for encoding and decoding.
pub mod common;
/// Contains data structures for directed acyclic graphs (DAGs).
pub mod dagmap;

pub use basic::{
    mapx::Mapx, mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey,
    mapx_ord_rawvalue::MapxOrdRawValue, orphan::Orphan, vecx::Vecx, vecx_raw::VecxRaw,
};

pub use common::{
    NULL,
    ende::{KeyDe, KeyEn, KeyEnDe, KeyEnDeOrdered, ValueDe, ValueEn, ValueEnDe},
};

pub use dagmap::{DagMapId, raw::DagMapRaw, rawkey::DagMapRawKey};

pub use vsdb_core::{self, *};
