#![doc = include_str!("../README.md")]
#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

pub mod common;

pub mod basic;

pub use basic::mapx_raw::MapxRaw;

pub use common::{
    GB, KB, MB, NULL, RawBytes, RawKey, RawValue, vsdb_flush, vsdb_get_base_dir,
    vsdb_get_custom_dir, vsdb_set_base_dir,
};
