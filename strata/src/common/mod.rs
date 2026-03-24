//!
//! # Common Components
//!
//! This module provides common components and utilities used throughout the `vsdb` crate.
//! It re-exports items from `vsdb_core::common` and includes the `ende` module for
//! encoding and decoding traits.
//!

/// A module for encoding and decoding traits.
pub mod ende;
pub mod macros;

pub use vsdb_core::common::*;

use ruc::*;
use serde::{Serialize, de::DeserializeOwned};
use std::fs;

/// Serializes `value` with `postcard` and writes it to the instance-meta
/// directory under the given `instance_id`.
pub fn save_instance_meta(instance_id: u64, value: &impl Serialize) -> Result<()> {
    let path = vsdb_meta_path(instance_id);
    let bytes = postcard::to_allocvec(value).c(d!())?;
    fs::write(&path, bytes).c(d!())
}

/// Reads the meta file for `instance_id` and deserializes it back.
pub fn load_instance_meta<T: DeserializeOwned>(instance_id: u64) -> Result<T> {
    let path = vsdb_meta_path(instance_id);
    let bytes = fs::read(&path).c(d!())?;
    postcard::from_bytes(&bytes).c(d!())
}
