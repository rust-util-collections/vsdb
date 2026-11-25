//!
//! Git-model versioned storage built on the persistent B+ tree.
//!
//! # Concepts
//!
//! | Git         | vsdb                                |
//! |-------------|-------------------------------------|
//! | tree object | `NodeId` (root of a B+ tree)      |
//! | commit      | [`Commit`]                          |
//! | ref/branch  | [`BranchId`] → mutable pointer      |
//! | working dir | uncommitted writes on a branch      |
//! | `git gc`    | `VerMap::gc`                |
//!
//! A commit identifies a complete map state through its B+ tree root.
//! Branches are lightweight pointers. Structural sharing keeps storage
//! costs proportional to the number of *changes*, not the dataset size.
//!
//! # Workflow
//!
//! ```text
//! new()  ──►  insert / remove  ──►  commit()
//!                  ▲                    │
//!                  │                    ▼
//!              discard()         create_branch()
//!                  ▲                    │
//!                  │                    ▼
//!            rollback_to()    insert / remove / commit
//!                                       │
//!                                       ▼
//!                                   merge()  ──►  delete_branch()
//!                                                    (release roots)
//! ```
//!
//! # Merkle proofs
//!
//! `VerMapWithProof` wraps a `VerMap<K, V>`
//! with an `MptCalc` (or
//! `SmtCalc`) to provide a 32-byte Merkle root
//! commitment over the versioned state.  See the `trie`
//! module for details.
//!

#![forbid(unsafe_code)]

pub(crate) mod diff;
pub mod map;

mod read;
mod repair;
pub(crate) mod store;
pub use repair::GcStats;

pub use diff::DiffEntry;
pub use read::{Snapshot, VerMapReader};

#[cfg(test)]
pub(crate) mod test;

use crate::{
    basic::persistent_btree::NodeId,
    common::{ende::KeyEnDeOrdered, error::Result},
};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

// =========================================================================
// ID types
// =========================================================================

pub use vsdb_core::common::{BranchId, CommitId};

macro_rules! ordered_id {
    ($id:ty) => {
        impl KeyEnDeOrdered for $id {
            fn with_ordered_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
                self.raw().with_ordered_bytes(f)
            }
            fn from_slice(bytes: &[u8]) -> Result<Self> {
                u64::from_slice(bytes).map(Self::from_raw)
            }
        }
    };
}
ordered_id!(BranchId);
ordered_id!(CommitId);
/// Sentinel: no commit yet.
pub(crate) const NO_COMMIT: CommitId = CommitId::from_raw(0);

// =========================================================================
// Commit
// =========================================================================

/// An immutable snapshot in the version history.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Commit {
    pub(crate) id: CommitId,
    /// The B+ tree root that holds the **complete** map state at this point.
    pub(crate) root: NodeId,
    #[serde(deserialize_with = "deserialize_parents")]
    pub(crate) parents: SmallVec<[CommitId; 2]>,
    pub(crate) timestamp_us: u64,
    /// Number of references: branch HEADs pointing at this commit
    /// plus child commits listing it in their `parents` array.
    /// When this reaches zero the commit is automatically deleted.
    #[serde(default)]
    pub(crate) ref_count: u32,
}

// Public serde values retain Vec's bounded preallocation for sequence hints.
// The storage row decoder reads the at-most-two durable parents directly.
fn deserialize_parents<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<SmallVec<[CommitId; 2]>, D::Error> {
    Vec::<CommitId>::deserialize(d).map(SmallVec::from_vec)
}

impl Commit {
    /// This commit's id.
    pub fn id(&self) -> CommitId {
        self.id
    }

    /// Parent commit(s): empty for a branch's first commit, two for a merge.
    pub fn parents(&self) -> &[CommitId] {
        &self.parents
    }

    /// Wall-clock microseconds since the Unix epoch at creation
    /// (informational only; never used for ordering).
    pub fn timestamp_us(&self) -> u64 {
        self.timestamp_us
    }
}
