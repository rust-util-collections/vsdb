//!
//! VSDB is a 'Git' in the form of KV-database.
//!
//! Based on the powerful version control function of VSDB,
//! you can easily give your data structure the ability to version management.
//!

#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

pub mod basic;
pub mod basic_multi_key;
pub mod common;
pub mod versioned;
pub mod versioned_multi_key;

pub use basic::{mapx::Mapx, mapx_ord::MapxOrd, vecx::Vecx};
pub use common::{
    ende::{KeyDe, KeyEn, KeyEnDe, KeyEnDeOrdered, ValueDe, ValueEn, ValueEnDe},
    NULL,
};
pub use versioned::{mapx::MapxVs, mapx_ord::MapxOrdVs, orphan::OrphanVs, vecx::VecxVs};
pub use versioned_multi_key::{
    mapx_double_key::MapxDkVs, mapx_raw::MapxRawMkVs, mapx_triple_key::MapxTkVs,
};
pub use vsdb_core::{self, *};
