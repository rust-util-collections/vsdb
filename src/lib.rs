//!
//! VSDB, **V**ersioned **S**tateful **D**ata**B**ase, mainly used in blockchain scene.
//!
//! Support some GIT-like operations, such as:
//!
//! - Support rolling back a 'branch' to a specified historical 'version'
//! - Support querying the historical value of a key on the specified 'branch'
//!
//! All data is divided into two dimensions: 'branch' and 'version', the functions of the 'basic' category are stateless, and the functions of the 'versioned' category are stateful.
//!
//! In the internal implementation, each stateful function is implemented based on its corresponding stateless function,
//! all stateful data has two additional identification dimensions ('branch' and 'version'), somewhat like the logic in GIT.
//!
//! Stateless functions do not have the feature of 'version' management, but they have higher performance.
//!

#![deny(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "512"]

pub mod basic;
mod common;
pub mod versioned;

pub use basic::mapx::Mapx;
pub use basic::mapx_oc::{MapxOC, OrderConsistKey};
pub use basic::mapx_raw::MapxRaw;
pub use basic::vecx::Vecx;

pub use common::{vsdb_flush, vsdb_set_base_dir};
