//!
//! Ergonomic branch handles for [`VerMap`].
//!
//! [`Branch`] provides read-only access to a specific branch, while
//! [`BranchMut`] adds write access.  Both eliminate the need to pass
//! a [`BranchId`] on every operation.
//!
//! ```ignore
//! let mut m: VerMap<u32, String> = VerMap::new();
//! let mut main = m.main_mut();
//! main.insert(&1, &"hello".into())?;
//! main.commit()?;
//! ```

use crate::common::ende::{KeyEnDeOrdered, ValueEnDe};
use crate::common::error::Result;
use crate::versioned::diff::DiffEntry;
use crate::versioned::map::VerMap;
use crate::versioned::{BranchId, CommitId};
/// Read-only handle bound to a specific branch.
///
/// Obtained via [`VerMap::branch`] or [`VerMap::main`].
#[derive(Debug)]
pub struct Branch<'a, K, V> {
    pub(super) map: &'a VerMap<K, V>,
    pub(super) id: BranchId,
}

impl<'a, K, V> Branch<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Returns the branch ID.
    pub fn id(&self) -> BranchId {
        self.id
    }

    /// Returns the branch name, if it exists.
    pub fn name(&self) -> Option<String> {
        self.map.branch_name(self.id)
    }

    /// Reads a value from the working state.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        self.map.get(self.id, key)
    }

    /// Checks if `key` exists in the working state.
    pub fn contains_key(&self, key: &K) -> Result<bool> {
        self.map.contains_key(self.id, key)
    }

    /// Iterates all entries in ascending key order.
    pub fn iter(&self) -> Result<impl Iterator<Item = (K, V)> + '_> {
        self.map.iter(self.id)
    }

    /// Iterates entries within `bounds` in ascending key order.
    pub fn range(
        &self,
        lo: std::ops::Bound<&K>,
        hi: std::ops::Bound<&K>,
    ) -> Result<impl Iterator<Item = (K, V)> + '_> {
        self.map.range(self.id, lo, hi)
    }

    /// Returns `true` if the branch has uncommitted changes.
    pub fn has_uncommitted(&self) -> Result<bool> {
        self.map.has_uncommitted(self.id)
    }

    /// Returns the head commit on this branch, if any.
    pub fn head_commit(&self) -> Result<Option<crate::versioned::Commit>> {
        self.map.head_commit(self.id)
    }

    /// Walks the first-parent commit history from head to root.
    pub fn log(&self) -> Result<Vec<crate::versioned::Commit>> {
        self.map.log(self.id)
    }

    /// Computes the diff of uncommitted changes relative to HEAD.
    pub fn diff_uncommitted(&self) -> Result<Vec<DiffEntry>> {
        self.map.diff_uncommitted(self.id)
    }
}

/// Mutable handle bound to a specific branch.
///
/// Obtained via [`VerMap::branch_mut`] or [`VerMap::main_mut`].
/// Provides all [`Branch`] read methods plus write operations.
#[derive(Debug)]
pub struct BranchMut<'a, K, V> {
    pub(super) map: &'a mut VerMap<K, V>,
    pub(super) id: BranchId,
}

impl<'a, K, V> BranchMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    // ---- Read methods (delegate to immutable view) ----

    /// Returns the branch ID.
    pub fn id(&self) -> BranchId {
        self.id
    }

    /// Returns the branch name, if it exists.
    pub fn name(&self) -> Option<String> {
        self.map.branch_name(self.id)
    }

    /// Reads a value from the working state.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        self.map.get(self.id, key)
    }

    /// Checks if `key` exists in the working state.
    pub fn contains_key(&self, key: &K) -> Result<bool> {
        self.map.contains_key(self.id, key)
    }

    /// Iterates all entries in ascending key order.
    pub fn iter(&self) -> Result<impl Iterator<Item = (K, V)> + '_> {
        self.map.iter(self.id)
    }

    /// Iterates entries within `bounds` in ascending key order.
    pub fn range(
        &self,
        lo: std::ops::Bound<&K>,
        hi: std::ops::Bound<&K>,
    ) -> Result<impl Iterator<Item = (K, V)> + '_> {
        self.map.range(self.id, lo, hi)
    }

    /// Returns `true` if the branch has uncommitted changes.
    pub fn has_uncommitted(&self) -> Result<bool> {
        self.map.has_uncommitted(self.id)
    }

    // ---- Write methods ----

    /// Inserts a key-value pair into the working state.
    pub fn insert(&mut self, key: &K, value: &V) -> Result<()> {
        self.map.insert(self.id, key, value)
    }

    /// Removes a key from the working state.
    pub fn remove(&mut self, key: &K) -> Result<()> {
        self.map.remove(self.id, key)
    }

    /// Commits the current working state, returning the commit ID.
    pub fn commit(&mut self) -> Result<CommitId> {
        self.map.commit(self.id)
    }

    /// Discards uncommitted changes, resetting to HEAD.
    pub fn discard(&mut self) -> Result<()> {
        self.map.discard(self.id)
    }

    /// Rolls back to a previous commit.
    pub fn rollback_to(&mut self, target: CommitId) -> Result<()> {
        self.map.rollback_to(self.id, target)
    }
}
