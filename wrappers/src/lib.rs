#![doc = include_str!("../README.md")]
#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

pub mod basic;
pub mod basic_multi_key;
pub mod common;

#[cfg(feature = "vs")]
pub mod versioned;

#[cfg(feature = "vs")]
pub mod versioned_multi_key;

pub use basic::{mapx::Mapx, mapx_ord::MapxOrd, vecx::Vecx};
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

pub use vsdb_core::{self, *};
