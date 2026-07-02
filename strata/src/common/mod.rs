//!
//! # Common Components
//!
//! This module provides common components and utilities used throughout the `vsdb` crate.
//! It re-exports items from `vsdb_core::common` (including the [`error`]
//! module — the unified error vocabulary of the whole ecosystem) and
//! includes the `ende` module for encoding and decoding traits.
//!

/// A module for encoding and decoding traits.
pub mod ende;
pub(crate) mod macros;

pub use vsdb_core::common::*;

pub mod dirty_count;

use error::Result;
use serde::{Serialize, de::DeserializeOwned};
use std::fs;

/// Serializes `value` with `postcard` and writes it to the instance-meta
/// directory under the given `instance_id`.
pub fn save_instance_meta(instance_id: u64, value: &impl Serialize) -> Result<()> {
    let path = vsdb_meta_path(instance_id);
    let bytes = postcard::to_allocvec(value)?;
    fs::write(&path, bytes)?;
    Ok(())
}

/// Reads the meta file for `instance_id` and deserializes it back.
///
/// Only the current (magic-tagged) meta format is accepted; metas written
/// by pre-v13.4 code must be re-saved under a v13 release first.
pub fn load_instance_meta<T: DeserializeOwned>(instance_id: u64) -> Result<T> {
    let path = vsdb_meta_path(instance_id);
    let bytes = fs::read(&path)?;
    Ok(postcard::from_bytes(&bytes)?)
}
