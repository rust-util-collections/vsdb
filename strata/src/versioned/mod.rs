//! Branches, commits and snapshots over a copy-on-write B+ tree.
//!
//! A branch has a working tree and a head commit. Writing changes the working
//! tree. [`VerMap::commit`](map::VerMap::commit) records its root in history.
//! Branches can share a root and diverge on later writes.
//! [`VerMap`](map::VerMap) shows how to fork a branch and merge it back.
//!
//! [`crate::VerMapWithProof`] adds a 32-byte Merkle root and proofs for
//! working or committed state using [`crate::MptCalc`] or [`crate::SmtCalc`].

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

    /// Parent commit(s): the branch head the commit was created on, or the
    /// target and source heads for a merge.
    ///
    /// The list is empty only for a commit created on a branch with no head.
    /// A branch forked from a branch with commits starts at the source head,
    /// so its first commit names that head as its parent.
    pub fn parents(&self) -> &[CommitId] {
        &self.parents
    }

    /// Wall-clock microseconds since the Unix epoch at creation
    /// (informational only; never used for ordering).
    pub fn timestamp_us(&self) -> u64 {
        self.timestamp_us
    }
}
