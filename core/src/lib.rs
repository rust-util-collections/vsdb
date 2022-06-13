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

#[cfg(feature = "hash")]
pub use common::utils::hash::{hash, Hash, HASH_SIZ};

pub use common::{
    vsdb_flush, vsdb_get_base_dir, vsdb_get_custom_dir, vsdb_set_base_dir, BranchName,
    BranchNameOwned, ParentBranchName, ParentBranchNameOwned, VersionName,
    VersionNameOwned, GB, KB, MB,
};
