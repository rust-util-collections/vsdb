#![doc = include_str!("../README.md")]

#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

pub mod common;

pub mod basic;

#[cfg(feature = "vs")]
pub mod versioned;

pub use basic::mapx_raw::MapxRaw;

#[cfg(feature = "vs")]
pub use versioned::mapx_raw::MapxRawVs;

#[cfg(feature = "vs")]
pub use versioned::VsMgmt;

#[cfg(feature = "vs")]
pub use vsdb_derive::Vs;

pub use common::{
    vsdb_flush, vsdb_get_base_dir, vsdb_get_custom_dir, vsdb_set_base_dir, RawBytes,
    RawKey, RawValue, GB, KB, MB, NULL,
};

#[cfg(feature = "vs")]
pub use common::{
    BranchName, BranchNameOwned, ParentBranchName, ParentBranchNameOwned, VersionName,
    VersionNameOwned,
};

#[cfg(feature = "extra_types")]
pub use {primitive_types_0_10, primitive_types_0_11, primitive_types_0_12};
