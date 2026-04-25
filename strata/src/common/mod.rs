//!
//! # Common Components
//!
//! This module provides common components and utilities used throughout the `vsdb` crate.
//! It re-exports items from `vsdb_core::common` and includes the `ende` module for
//! encoding and decoding traits.
//!

/// A module for encoding and decoding traits.
pub mod ende;
/// Structured error types for the public API.
pub mod error;
pub mod macros;

pub use vsdb_core::common::*;

pub mod dirty_count;

use serde::{Serialize, de::DeserializeOwned};
use std::fs;

/// Serializes `value` with `postcard` and writes it to the instance-meta
/// directory under the given `instance_id`.
pub fn save_instance_meta(
    instance_id: u64,
    value: &impl Serialize,
) -> error::Result<()> {
    let path = vsdb_meta_path(instance_id);
    let bytes = postcard::to_allocvec(value)?;
    fs::write(&path, bytes)?;
    Ok(())
}

/// Reads the meta file for `instance_id` and deserializes it back.
pub fn load_instance_meta<T: DeserializeOwned>(instance_id: u64) -> error::Result<T> {
    let path = vsdb_meta_path(instance_id);
    let bytes = fs::read(&path)?;
    Ok(with_legacy_mapx_meta_decode(|| {
        postcard::from_bytes(&bytes)
    })?)
}
