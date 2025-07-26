#![doc = include_str!("../README.md")]
#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

/// Contains common modules and utilities for the VSDB framework.
///
/// This module provides access to essential components such as database backends,
/// traits for data manipulation, and various utility functions. It serves as a
/// central hub for shared functionality used throughout the VSDB ecosystem.
pub mod common;

/// Defines basic data structures for the VSDB framework.
///
/// This module contains fundamental data structures like maps and vectors
/// that are essential for building more complex storage solutions. These
/// structures are designed to be efficient and flexible, providing the
/// building blocks for various data management tasks.
pub mod basic;

/// A re-export of the `MapxRaw` data structure for convenient access.
///
/// `MapxRaw` is a raw, high-performance map implementation that forms the
/// foundation of many other data structures in the VSDB framework.
pub use basic::mapx_raw::MapxRaw;

/// A re-export of commonly used items for convenient access.
///
/// This includes constants for data sizes (KB, MB, GB), a null terminator constant (`NULL`),
/// and various raw data types (`RawBytes`, `RawKey`, `RawValue`). It also includes
/// functions for managing the VSDB environment, such as flushing data and
/// getting or setting directory paths.
pub use common::{
    GB, KB, MB, NULL, RawBytes, RawKey, RawValue, vsdb_flush, vsdb_get_base_dir,
    vsdb_get_custom_dir, vsdb_set_base_dir,
};
