#![doc = include_str!("../README.md")]
#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

pub mod basic;
pub mod basic_multi_key;
pub mod common;
pub mod dagmap;

#[cfg(feature = "vs")]
pub mod versioned;

#[cfg(feature = "vs")]
pub mod versioned_multi_key;

pub use basic::{
    mapx::Mapx, mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey,
    mapx_ord_rawvalue::MapxOrdRawValue, orphan::Orphan, vecx::Vecx, vecx_raw::VecxRaw,
};

pub use dagmap::{raw::DagMapRaw, rawkey::DagMapRawKey, DagMapId};

pub use vsdb_core::{self, *};

pub use common::{
    ende::{KeyDe, KeyEn, KeyEnDe, KeyEnDeOrdered, ValueDe, ValueEn, ValueEnDe},
    NULL,
};

#[cfg(feature = "vs")]
pub use versioned::{mapx::MapxVs, mapx_ord::MapxOrdVs, orphan::OrphanVs, vecx::VecxVs};

#[cfg(feature = "vs")]
pub use versioned_multi_key::{
    mapx_double_key::MapxDkVs, mapx_raw::MapxRawMkVs, mapx_triple_key::MapxTkVs,
};
