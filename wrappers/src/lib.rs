#![doc = include_str!("../README.md")]
#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

pub mod basic;
pub mod basic_multi_key;
pub mod common;
pub mod dagmap;

pub use basic::{
    mapx::Mapx, mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey,
    mapx_ord_rawvalue::MapxOrdRawValue, orphan::Orphan, vecx::Vecx, vecx_raw::VecxRaw,
};

pub use dagmap::{raw::DagMapRaw, rawkey::DagMapRawKey, DagMapId};

pub use common::{
    ende::{KeyDe, KeyEn, KeyEnDe, KeyEnDeOrdered, ValueDe, ValueEn, ValueEnDe},
    NULL,
};

pub use vsdb_core::{self, *};
