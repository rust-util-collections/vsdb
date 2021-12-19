//!
//! # VSDB
//!
//! VSDB, **V**ersioned **S**tateful **D**ata**B**ase, mainly used in blockchain scene.
//!
//! ## Highlights(Features)
//!
//! - Support GIT-like verison operations, such as:
//!     - Rolling back a 'branch' to a specified historical 'version'
//!     - Querying the historical value of a key on the specified 'branch'
//!     - Merge branches(different data versions) just like 'git merge BRANCH'
//!     - ...
//! - The definition of most APIs is same as the coresponding data structures of the standard library
//!     - Use `Vecx` just like `Vec`, but data will be automatically stored in disk instead of memory
//!     - Use `Mapx` just like `BTreeMap`, but data will be automatically stored in disk instead of memory
//!     - ...
//!
//! ## Implementation ideas
//!
//! Based on the underlying one-dimensional linear storage structure (native kv-database, such as sled/rocksdb, etc.), multiple different namespaces are divided, and then abstract each dimension in the multi-dimensional logical structure based on these divided namespaces.
//!
//! In the category of kv-database, namespaces can be expressed as different key ranges, or different key prefix.
//!
//! This is the same as expressing complex data structures in computer memory, you know, the memory itself is just a one-dimensional linear structure.
//!
//! User data will be divided into two dimensions: 'branch' and 'version', the functions of the 'basic' category are stateless, and the functions of the 'versioned' category are stateful.
//!
//! In the internal implementation, each stateful function is implemented based on its corresponding stateless function,
//! all stateful data has two additional identification dimensions ('branch' and 'version'), somewhat like the logic in GIT.
//!
//! Stateless functions do not have the feature of 'version' management, but they have higher performance.

#![deny(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "512"]

pub mod basic;
mod common;
pub mod versioned;

pub use basic::mapx::Mapx;
pub use basic::mapx_ord::MapxOrd;
pub use basic::orphan::Orphan;
pub use basic::vecx::Vecx;

// pub use versioned::mapx::MapxVersioned;
// pub use versioned::mapx_ord::MapxOrdVersioned;
// pub use versioned::orphan::OrphanVersioned;
// pub use versioned::vecx::VecxVersioned;

pub use common::{
    ende::{KeyEnDe, KeyEnDeOrdered, ValueEnDe},
    vsdb_flush, vsdb_set_base_dir,
};
