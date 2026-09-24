//!
//! Git-model versioned storage built on the persistent B+ tree.
//!
//! # Concepts
//!
//! | Git         | vsdb                                |
//! |-------------|-------------------------------------|
//! | tree object | [`NodeId`](crate::basic::persistent_btree::NodeId) (root of a B+ tree) |
//! | commit      | [`Commit`]                          |
//! | ref/branch  | [`BranchId`] → mutable pointer      |
//! | working dir | uncommitted writes on a branch      |
//! | `git gc`    | [`VerMap::gc`]                |
//!
//! A *version* is a complete, self-contained snapshot (a B+ tree root).
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
//!                                                    (ref-count GC)
//!                                                       │
//!                                                   gc() (B+ tree nodes)
//! ```
//!
//! # Merkle proofs
//!
//! [`VerMapWithProof`](crate::trie::VerMapWithProof) wraps a `VerMap<K, V>`
//! with an [`MptCalc`](crate::trie::MptCalc) (or
//! [`SmtCalc`](crate::trie::SmtCalc)) to provide a 32-byte Merkle root
//! commitment over the versioned state.  See the [`trie`](crate::trie)
//! module for details.
//!

pub(crate) mod diff;
pub mod map;
pub(crate) mod merge;

mod read;
mod repair;

pub use diff::DiffEntry;
pub use read::Snapshot;

#[cfg(test)]
mod test;

use crate::basic::persistent_btree::NodeId;
use serde::{Deserialize, Serialize};
use std::fmt;

// =========================================================================
// ID types
// =========================================================================

macro_rules! define_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            /// Rebuilds an id from [`raw`](Self::raw) (e.g. one persisted
            /// by the application). Lookups reject ids that do not exist.
            pub const fn from_raw(raw: u64) -> Self {
                Self(raw)
            }

            /// The numeric value, stable across restarts.
            pub const fn raw(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

define_id! {
    /// Identifies a commit in the history DAG.
    ///
    /// A distinct type from [`BranchId`], so the two cannot be swapped;
    /// persisted exactly like the `u64` it wraps.
    CommitId
}

define_id! {
    /// Identifies a branch.
    ///
    /// A distinct type from [`CommitId`], so the two cannot be swapped;
    /// persisted exactly like the `u64` it wraps.
    BranchId
}

/// Sentinel: no commit yet.
pub(crate) const NO_COMMIT: CommitId = CommitId(0);

// =========================================================================
// Commit
// =========================================================================

/// An immutable snapshot in the version history.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Commit {
    pub(crate) id: CommitId,
    /// The B+ tree root that holds the **complete** map state at this point.
    pub(crate) root: NodeId,
    pub(crate) parents: Vec<CommitId>,
    pub(crate) timestamp_us: u64,
    /// Number of references: branch HEADs pointing at this commit
    /// plus child commits listing it in their `parents` array.
    /// When this reaches zero the commit is automatically deleted.
    #[serde(default)]
    pub(crate) ref_count: u32,
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
