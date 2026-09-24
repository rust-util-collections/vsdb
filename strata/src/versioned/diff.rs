//!
//! Typed diff entries for [`VerMap`](super::map::VerMap), decoded from the
//! tree-level [`TreeDiff`] (which skips shared subtrees, so a diff costs
//! O(changed keys × depth × fanout) node reads).
//!

use crate::{
    basic::persistent_btree::TreeDiff,
    common::ende::{KeyEnDeOrdered, ValueEnDe},
};

/// A single difference between two map states, decoded.
///
/// Produced by [`VerMap::diff_commits`](super::map::VerMap::diff_commits)
/// and [`VerMap::diff_uncommitted`](super::map::VerMap::diff_uncommitted)
/// in ascending key order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiffEntry<K, V> {
    /// Present in the newer state only.
    Added { key: K, value: V },
    /// Present in the older state only.
    Removed { key: K, value: V },
    /// Present in both with different values.
    Modified { key: K, old_value: V, new_value: V },
}

impl<K: KeyEnDeOrdered, V: ValueEnDe> DiffEntry<K, V> {
    /// Decodes a raw entry; panics on undecodable bytes like every other
    /// typed read (see the [encode/decode trust model](crate::common::ende)).
    pub(crate) fn decode(raw: TreeDiff) -> Self {
        let k = |b: Vec<u8>| K::from_bytes(b).unwrap();
        let v = |b: Vec<u8>| V::decode(&b).unwrap();
        match raw {
            TreeDiff::Added { key, value } => Self::Added {
                key: k(key),
                value: v(value),
            },
            TreeDiff::Removed { key, value } => Self::Removed {
                key: k(key),
                value: v(value),
            },
            TreeDiff::Modified {
                key,
                old_value,
                new_value,
            } => Self::Modified {
                key: k(key),
                old_value: v(old_value),
                new_value: v(new_value),
            },
        }
    }
}
