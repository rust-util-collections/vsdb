//!
//! # vsdb
//!

#![deny(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "512"]

pub mod basic;
mod common;
pub mod versioned;

pub use basic::mapx::Mapx;
pub use basic::mapx_oc::{MapxOC, OrderConsistKey};
pub use basic::mapx_raw::MapxRaw;
pub use basic::vecx::Vecx;

pub use common::{vsdb_flush, vsdb_set_base_dir};
