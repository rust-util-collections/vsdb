//!
//! # vsdb_core
//!
//! The core implementations of [VSDB](https://crates.io/crates/vsdb).
//!

#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

pub mod basic;
pub mod common;
pub mod versioned;

pub use basic::mapx_raw::MapxRaw;
pub use versioned::mapx_raw::MapxRawVs;

pub use versioned::VsMgmt;

#[cfg(feature = "derive")]
pub use vsdb_derive::Vs;

pub use common::{
    vsdb_flush, vsdb_get_base_dir, vsdb_get_custom_dir, vsdb_set_base_dir, BranchName,
    BranchNameOwned, ParentBranchName, ParentBranchNameOwned, RawBytes, RawKey,
    RawValue, VersionName, VersionNameOwned, GB, KB, MB, NULL,
};

#[cfg(feature = "extra_types")]
pub use {primitive_types_0_10, primitive_types_0_11, primitive_types_0_12};
