//!
//! Git-model versioned storage built on the persistent B+ tree.
//!
//! # Concepts
//!
//! | Git         | vsdb                                |
//! |-------------|-------------------------------------|
//! | tree object | [`NodeId`] (root of a B+ tree)      |
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

pub mod diff;
pub mod map;
pub mod merge;

#[cfg(test)]
mod test;

use serde::{Deserialize, Serialize};
use vsdb_core::basic::persistent_btree::NodeId;

// =========================================================================
// ID types
// =========================================================================

/// Identifies a commit in the history DAG.
pub type CommitId = u64;

/// Identifies a branch.
pub type BranchId = u64;

/// Sentinel: no commit yet.
pub const NO_COMMIT: CommitId = 0;

// =========================================================================
// Commit
// =========================================================================

/// An immutable snapshot in the version history.
///
/// Each commit records the complete state of the map (as a B+ tree root)
/// plus parent linkage and a reference count for automatic lifecycle
/// management.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Commit {
    /// Unique identifier.
    pub id: CommitId,
    /// The B+ tree root that holds the **complete** map state at this point.
    pub root: NodeId,
    /// Parent commit(s).  Empty for the initial commit, two entries for a merge.
    pub parents: Vec<CommitId>,
    /// Wall-clock microseconds since epoch (informational only).
    pub timestamp_us: u64,
    /// Number of references: branch HEADs pointing at this commit
    /// plus child commits listing it in their `parents` array.
    /// When this reaches zero the commit is automatically deleted.
    #[serde(default)]
    pub ref_count: u32,
}
