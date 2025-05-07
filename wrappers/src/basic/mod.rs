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

//!
//! Contains basic data structures with typed keys and values.
//!

pub mod mapx;
pub mod mapx_ord;
pub mod mapx_ord_rawkey;
pub mod mapx_ord_rawvalue;
pub mod orphan;
pub mod vecx;
pub mod vecx_raw;
