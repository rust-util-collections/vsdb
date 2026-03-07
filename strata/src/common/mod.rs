//!
//! # Common Components
//!
//! This module provides common components and utilities used throughout the `vsdb` wrappers.
//! It re-exports items from `vsdb_core::common` and includes the `ende` module for
//! encoding and decoding traits.
//!

/// A module for encoding and decoding traits.
pub mod ende;
pub mod macros;

pub use vsdb_core::common::*;
