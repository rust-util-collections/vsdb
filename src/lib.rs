//!
//! # An mixed(mem&disk) cache implementation
//!

#![deny(warnings)]
#![deny(missing_docs)]

pub mod helper;
pub mod mapx;
mod serde;
pub mod vecx;

pub use mapx::Mapx;
pub use vecx::Vecx;
