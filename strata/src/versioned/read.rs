//! Read operations for VerMap: get, contains_key, iter, range.
//!
//! Pure read path — none of these methods mutate state.

use std::ops::Bound;

use ruc::{RucResult, pnk};

use crate::{
    common::ende::{KeyEnDeOrdered, ValueEnDe},
    common::error::Result,
};

use super::map::VerMap;
use super::{BranchId, CommitId};

impl<K, V> VerMap<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Reads a value from the working state of `branch`.
    ///
    /// # Panics
    ///
    /// Panics if the stored bytes cannot be decoded back into `V`.
    /// This can only happen due to data corruption or a type mismatch
    /// between the writing and reading code — see the
    /// [encode/decode trust model](crate::common::ende).
    pub fn get(&self, branch: BranchId, key: &K) -> Result<Option<V>> {
        let state = self.get_branch(branch)?;
        let raw = self.tree.get(state.dirty_root, &key.to_bytes());
        match raw {
            Some(v) => Ok(Some(pnk!(V::decode(&v)))),
            None => Ok(None),
        }
    }

    /// Reads a value at a specific historical commit.
    ///
    /// # Panics
    ///
    /// Panics if the stored bytes cannot be decoded — see
    /// [`get`](Self::get) for details.
    pub fn get_at_commit(&self, commit_id: CommitId, key: &K) -> Result<Option<V>> {
        let commit = self.get_commit_inner(commit_id)?;
        let raw = self.tree.get(commit.root, &key.to_bytes());
        match raw {
            Some(v) => Ok(Some(pnk!(V::decode(&v)))),
            None => Ok(None),
        }
    }

    /// Checks if `key` exists in the working state of `branch`.
    pub fn contains_key(&self, branch: BranchId, key: &K) -> Result<bool> {
        let state = self.get_branch(branch)?;
        Ok(self.tree.contains_key(state.dirty_root, &key.to_bytes()))
    }

    /// Iterates all entries on `branch` in ascending key order.
    ///
    /// # Panics
    ///
    /// The returned iterator panics if any stored entry cannot be
    /// decoded — see [`get`](Self::get) for details.
    pub fn iter(&self, branch: BranchId) -> Result<impl Iterator<Item = (K, V)> + '_> {
        let state = self.get_branch(branch)?;
        Ok(self
            .tree
            .iter(state.dirty_root)
            .map(|(k, v)| (pnk!(K::from_slice(&k)), pnk!(V::decode(&v)))))
    }

    /// Iterates entries in `[lo, hi)` on `branch` in ascending key order.
    ///
    /// # Panics
    ///
    /// The returned iterator panics on decode failure — see
    /// [`get`](Self::get).
    pub fn range(
        &self,
        branch: BranchId,
        lo: Bound<&K>,
        hi: Bound<&K>,
    ) -> Result<impl Iterator<Item = (K, V)> + '_> {
        let state = self.get_branch(branch)?;
        let lo_raw = match lo {
            Bound::Included(k) => Bound::Included(k.to_bytes()),
            Bound::Excluded(k) => Bound::Excluded(k.to_bytes()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let hi_raw = match hi {
            Bound::Included(k) => Bound::Included(k.to_bytes()),
            Bound::Excluded(k) => Bound::Excluded(k.to_bytes()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(self
            .tree
            .range(
                state.dirty_root,
                lo_raw.as_ref().map(|v| v.as_slice()),
                hi_raw.as_ref().map(|v| v.as_slice()),
            )
            .map(|(k, v)| (pnk!(K::from_slice(&k)), pnk!(V::decode(&v)))))
    }

    /// Iterates all entries at a specific historical commit.
    ///
    /// # Panics
    ///
    /// The returned iterator panics on decode failure — see
    /// [`get`](Self::get).
    pub fn iter_at_commit(
        &self,
        commit_id: CommitId,
    ) -> Result<impl Iterator<Item = (K, V)> + '_> {
        let commit = self.get_commit_inner(commit_id)?;
        Ok(self
            .tree
            .iter(commit.root)
            .map(|(k, v)| (pnk!(K::from_slice(&k)), pnk!(V::decode(&v)))))
    }

    /// Iterates entries in `[lo, hi)` at a specific historical commit
    /// in ascending key order.
    ///
    /// # Panics
    ///
    /// The returned iterator panics on decode failure — see
    /// [`get`](Self::get).
    pub fn range_at_commit(
        &self,
        commit_id: CommitId,
        lo: Bound<&K>,
        hi: Bound<&K>,
    ) -> Result<impl Iterator<Item = (K, V)> + '_> {
        let commit = self.get_commit_inner(commit_id)?;
        let lo_raw = match lo {
            Bound::Included(k) => Bound::Included(k.to_bytes()),
            Bound::Excluded(k) => Bound::Excluded(k.to_bytes()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let hi_raw = match hi {
            Bound::Included(k) => Bound::Included(k.to_bytes()),
            Bound::Excluded(k) => Bound::Excluded(k.to_bytes()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(self
            .tree
            .range(
                commit.root,
                lo_raw.as_ref().map(|v| v.as_slice()),
                hi_raw.as_ref().map(|v| v.as_slice()),
            )
            .map(|(k, v)| (pnk!(K::from_slice(&k)), pnk!(V::decode(&v)))))
    }

    /// Iterates all raw (untyped) key-value pairs on a branch.
    ///
    /// Returns `(Vec<u8>, Vec<u8>)` without decoding, useful for
    /// feeding into external consumers (e.g. MPT hash computation).
    pub fn raw_iter(
        &self,
        branch: BranchId,
    ) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_> {
        let state = self.get_branch(branch)?;
        Ok(self.tree.iter(state.dirty_root))
    }

    /// Iterates all raw (untyped) key-value pairs at a historical commit.
    pub fn raw_iter_at_commit(
        &self,
        commit_id: CommitId,
    ) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_> {
        let commit = self.get_commit_inner(commit_id)?;
        Ok(self.tree.iter(commit.root))
    }

    /// Checks if `key` exists at a specific historical commit.
    pub fn contains_key_at_commit(&self, commit_id: CommitId, key: &K) -> Result<bool> {
        let commit = self.get_commit_inner(commit_id)?;
        Ok(self.tree.contains_key(commit.root, &key.to_bytes()))
    }
}
