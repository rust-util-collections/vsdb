//!
//! # Basic Data Structures
//!
//! This module provides a collection of fundamental, unversioned data structures
//! that wrap the raw storage implementations from `vsdb_core`. These wrappers
//! offer typed APIs for keys and values, leveraging the encoding and decoding
//! capabilities defined in the `common::ende` module.
//!
//! The structures available include various map and vector implementations,
//! each tailored for specific use cases.
//!

/// A typed, unversioned map with ordered keys and values.
pub mod mapx;
/// A typed, unversioned map with ordered keys and values, optimized for ordered iteration.
pub mod mapx_ord;
/// A typed, unversioned map with ordered raw keys and typed values.
pub mod mapx_ord_rawkey;
/// A typed, unversioned map with ordered typed keys and raw values.
pub mod mapx_ord_rawvalue;
/// A container for holding a value that may not have a direct owner.
pub mod orphan;
/// A typed, unversioned vector.
pub mod vecx;
/// A raw, unversioned vector.
pub mod vecx_raw;
