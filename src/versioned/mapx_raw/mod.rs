//!
//! # Versioned functions
//!

mod backend;

use crate::common::BranchID;
use ruc::*;
use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;

use backend::{
    MapxRawVersionedIter, RawKey, RawValue, ValueMut, VerSig, INITIAL_BRANCH_NAME, NULL,
};

/// Advanced `MapxRaw`, with versioned feature.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapxRawVersioned {
    inner: backend::MapxRawVersioned,
}

impl Default for MapxRawVersioned {
    fn default() -> Self {
        Self::new()
    }
}

impl MapxRawVersioned {
    #[inline(always)]
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {
            inner: backend::MapxRawVersioned::new(),
        }
    }

    /// Insert a KV to the head version of the default branch.
    #[inline(always)]
    pub fn insert(&mut self, key: RawKey, value: RawValue) -> Result<Option<RawValue>> {
        self.inner.insert(key, value).c(d!())
    }

    /// Insert a KV to the head version of a specified branch.
    #[inline(always)]
    pub fn insert_by_branch(
        &mut self,
        key: RawKey,
        value: RawValue,
        branch_id: BranchID,
    ) -> Result<Option<RawValue>> {
        self.inner.insert_by_branch(key, value, branch_id).c(d!())
    }

    /// Remove a KV from the head version of the default branch.
    #[inline(always)]
    pub fn remove(&mut self, key: &[u8]) -> Result<Option<RawValue>> {
        self.inner.remove(key).c(d!())
    }

    /// Remove a KV from the head version of a specified branch.
    #[inline(always)]
    pub fn remove_by_branch(
        &mut self,
        key: &[u8],
        branch_id: BranchID,
    ) -> Result<Option<RawValue>> {
        self.inner.remove_by_branch(key, branch_id).c(d!())
    }

    /// Get the value of a key from the default branch.
    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<RawValue> {
        self.inner.get(key)
    }

    /// Get the value of a key from the head of a specified branch.
    #[inline(always)]
    pub fn get_by_branch(&self, key: &[u8], branch_name: &[u8]) -> Option<RawValue> {
        let branch_id = self.inner.branch_name_to_branch_id._get(branch_name)?;
        self.inner.get_by_branch(key, branch_id)
    }

    /// Get the value of a key from a specified version of a specified branch.
    #[inline(always)]
    pub fn get_by_branch_version(
        &self,
        key: &[u8],
        branch_name: &[u8],
        version_name: &[u8],
    ) -> Option<RawValue> {
        let branch_id = self.inner.branch_name_to_branch_id._get(branch_name)?;
        let version_id = self.inner.version_name_to_version_id._get(version_name)?;
        self.inner.get_by_branch_version(key, branch_id, version_id)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<ValueMut<'_>> {
        self.inner.get_mut(key)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn get_mut_by_branch(
        &mut self,
        key: &[u8],
        branch_name: &[u8],
    ) -> Option<ValueMut<'_>> {
        let branch_id = self.inner.branch_name_to_branch_id._get(branch_name)?;
        self.inner.get_mut_by_branch(key, branch_id)
    }

    /// Get the value of a key from the default branch,
    /// if the target key does not exist, will try to
    /// search a closest value bigger than the target key.
    #[inline(always)]
    pub fn get_ge(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.inner.get_ge(key)
    }

    /// Get the value of a key from the head of a specified branch,
    /// if the target key does not exist, will try to
    /// search a closest value bigger than the target key.
    #[inline(always)]
    pub fn get_ge_by_branch(
        &self,
        key: &[u8],
        branch_name: &[u8],
    ) -> Option<(RawKey, RawValue)> {
        let branch_id = self.inner.branch_name_to_branch_id._get(branch_name)?;
        self.inner.get_ge_by_branch(key, branch_id)
    }

    /// Get the value of a key from a specified version of a specified branch,
    /// if the target key does not exist, will try to
    /// search a closest value bigger than the target key.
    #[inline(always)]
    pub fn get_ge_by_branch_version(
        &self,
        key: &[u8],
        branch_name: &[u8],
        version_name: &[u8],
    ) -> Option<(RawKey, RawValue)> {
        let branch_id = self.inner.branch_name_to_branch_id._get(branch_name)?;
        let version_id = self.inner.version_name_to_version_id._get(version_name)?;
        self.inner
            .get_ge_by_branch_version(key, branch_id, version_id)
    }

    /// Get the value of a key from the default branch,
    /// if the target key does not exist, will try to
    /// search a closest value less than the target key.
    #[inline(always)]
    pub fn get_le(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.inner.get_le(key)
    }

    /// Get the value of a key from the head of a specified branch,
    /// if the target key does not exist, will try to
    /// search a closest value bigger less the target key.
    #[inline(always)]
    pub fn get_le_by_branch(
        &self,
        key: &[u8],
        branch_name: &[u8],
    ) -> Option<(RawKey, RawValue)> {
        let branch_id = self.inner.branch_name_to_branch_id._get(branch_name)?;
        self.inner.get_le_by_branch(key, branch_id)
    }

    /// Get the value of a key from a specified version of a specified branch,
    /// if the target key does not exist, will try to
    /// search a closest value bigger than the target key.
    #[inline(always)]
    pub fn get_le_by_branch_version(
        &self,
        key: &[u8],
        branch_name: &[u8],
        version_name: &[u8],
    ) -> Option<(RawKey, RawValue)> {
        let branch_id = self.inner.branch_name_to_branch_id._get(branch_name)?;
        let version_id = self.inner.version_name_to_version_id._get(version_name)?;
        self.inner
            .get_le_by_branch_version(key, branch_id, version_id)
    }

    /// Create an iterator over the default branch.
    #[inline(always)]
    pub fn iter(&self) -> MapxRawVersionedIter {
        self.inner.iter()
    }

    /// Create an iterator over a specified branch.
    #[inline(always)]
    pub fn iter_by_branch(&self, branch_name: &[u8]) -> MapxRawVersionedIter {
        let branch_id = self
            .inner
            .branch_name_to_branch_id
            ._get(branch_name)
            .unwrap_or(NULL);
        self.inner.iter_by_branch(branch_id)
    }

    /// Create an iterator over a specified version of a specified branch.
    #[inline(always)]
    pub fn iter_by_branch_version(
        &self,
        branch_name: &[u8],
        version_name: &[u8],
    ) -> MapxRawVersionedIter {
        let branch_id = self
            .inner
            .branch_name_to_branch_id
            ._get(branch_name)
            .unwrap_or(NULL);

        let version_id = self
            .inner
            .version_name_to_version_id
            ._get(version_name)
            .unwrap_or(NULL);

        self.inner.iter_by_branch_version(branch_id, version_id)
    }

    /// Create a range iterator over the default branch.
    #[inline(always)]
    pub fn range<R: RangeBounds<RawKey>>(&self, bounds: R) -> MapxRawVersionedIter {
        self.inner.range(bounds)
    }

    /// Create a range iterator over a specified branch.
    #[inline(always)]
    pub fn range_by_branch<R: RangeBounds<RawKey>>(
        &self,
        branch_name: &[u8],
        bounds: R,
    ) -> MapxRawVersionedIter {
        let branch_id = self
            .inner
            .branch_name_to_branch_id
            ._get(branch_name)
            .unwrap_or(NULL);

        self.inner.range_by_branch(branch_id, bounds)
    }

    /// Create a range iterator over a specified version of a specified branch.
    #[inline(always)]
    pub fn range_by_branch_version<R: RangeBounds<RawKey>>(
        &self,
        branch_name: &[u8],
        version_name: &[u8],
        bounds: R,
    ) -> MapxRawVersionedIter {
        let branch_id = self
            .inner
            .branch_name_to_branch_id
            ._get(branch_name)
            .unwrap_or(NULL);

        let version_id = self
            .inner
            .version_name_to_version_id
            ._get(version_name)
            .unwrap_or(NULL);

        self.inner
            .range_by_branch_version(branch_id, version_id, bounds)
    }

    /// Check if a key exist on the default branch.
    #[inline(always)]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Check if a key exist on a specified branch.
    #[inline(always)]
    pub fn contains_key_by_branch(&self, key: &[u8], branch_name: &[u8]) -> bool {
        self.get_by_branch(key, branch_name).is_some()
    }

    /// Check if a key exist on a specified version of a specified branch.
    #[inline(always)]
    pub fn contains_key_by_branch_version(
        &self,
        key: &[u8],
        branch_name: &[u8],
        version_name: &[u8],
    ) -> bool {
        self.get_by_branch_version(key, branch_name, version_name)
            .is_some()
    }

    /// Get the total number of items of the default branch.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Get the total number of items of the head of a specified branch.
    #[inline(always)]
    pub fn len_by_branch(&self, branch_name: &[u8]) -> usize {
        let branch_id =
            if let Some(id) = self.inner.branch_name_to_branch_id._get(branch_name) {
                id
            } else {
                return 0;
            };
        self.inner.len_by_branch(branch_id)
    }

    /// Get the total number of items of a specified version of a specified branch.
    #[inline(always)]
    pub fn len_by_branch_version(
        &self,
        branch_name: &[u8],
        version_name: &[u8],
    ) -> usize {
        let branch_id =
            if let Some(id) = self.inner.branch_name_to_branch_id._get(branch_name) {
                id
            } else {
                return 0;
            };
        let version_id =
            if let Some(id) = self.inner.version_name_to_version_id._get(version_name) {
                id
            } else {
                return 0;
            };
        self.inner.len_by_branch_version(branch_id, version_id)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn is_empty(&self) -> bool {
        0 == self.len()
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn is_empty_by_branch(&self, branch_name: &[u8]) -> bool {
        0 == self.len_by_branch(branch_name)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn is_empty_by_branch_version(
        &self,
        branch_name: &[u8],
        version_name: &[u8],
    ) -> bool {
        0 == self.len_by_branch_version(branch_name, version_name)
    }

    /// Clear all data, mainly for testing purpose.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Create a new version on the default branch.
    #[inline(always)]
    pub fn version_create(&mut self, version_name: &[u8]) -> Result<()> {
        self.inner.version_create(version_name).c(d!())
    }

    /// Create a new version on a specified branch,
    /// NOTE: the branch must has been created.
    #[inline(always)]
    pub fn version_create_by_branch(
        &mut self,
        version_name: &[u8],
        branch_name: &[u8],
    ) -> Result<()> {
        let branch_id = self
            .inner
            .branch_name_to_branch_id
            ._get(branch_name)
            .c(d!("branch not found"))?;

        self.inner
            .version_create_by_branch(version_name, branch_id)
            .c(d!())
    }

    /// Check if a verison exists on default branch.
    #[inline(always)]
    pub fn version_exists(&self, version_name: &[u8]) -> bool {
        let version_id =
            if let Some(id) = self.inner.version_name_to_version_id._get(version_name) {
                id
            } else {
                return false;
            };

        self.inner.version_exists(version_id)
    }

    /// Check if a version exists on a specified branch(include its parents).
    #[inline(always)]
    pub fn version_exists_on_branch(
        &self,
        version_name: &[u8],
        branch_name: &[u8],
    ) -> bool {
        let version_id =
            if let Some(id) = self.inner.version_name_to_version_id._get(version_name) {
                id
            } else {
                return false;
            };

        let branch_id =
            if let Some(id) = self.inner.branch_name_to_branch_id._get(branch_name) {
                id
            } else {
                return false;
            };

        self.inner.version_exists_on_branch(version_id, branch_id)
    }

    /// Check if a version is directly created on the default branch.
    #[inline(always)]
    pub fn version_created(&self, version_name: &[u8]) -> bool {
        self.version_created_on_branch(version_name, INITIAL_BRANCH_NAME)
    }

    /// Check if a version is directly created on a specified branch(exclude its parents).
    #[inline(always)]
    pub fn version_created_on_branch(
        &self,
        version_name: &[u8],
        branch_name: &[u8],
    ) -> bool {
        let version_id =
            if let Some(id) = self.inner.version_name_to_version_id._get(version_name) {
                id
            } else {
                return false;
            };

        let branch_id =
            if let Some(id) = self.inner.branch_name_to_branch_id._get(branch_name) {
                id
            } else {
                return false;
            };

        self.inner.version_created_on_branch(version_id, branch_id)
    }

    /// Remove the newest version on the default branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    #[inline(always)]
    pub fn version_pop(&mut self) -> Result<()> {
        self.inner.version_pop().c(d!())
    }

    /// Remove the newest version on a specified branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    #[inline(always)]
    pub fn version_pop_by_branch(&mut self, branch_name: &[u8]) -> Result<()> {
        let branch_id = self
            .inner
            .branch_name_to_branch_id
            ._get(branch_name)
            .c(d!("branch not found"))?;

        self.inner.version_pop_by_branch(branch_id).c(d!())
    }

    /// Create a new branch based on the head of the default branch.
    #[inline(always)]
    pub fn branch_create(&mut self, branch_name: &[u8]) -> Result<()> {
        self.inner.branch_create(branch_name).c(d!())
    }

    /// Create a new branch based on the head of a specified branch.
    #[inline(always)]
    pub fn branch_create_by_base_branch(
        &mut self,
        branch_name: &[u8],
        base_branch_name: &[u8],
    ) -> Result<()> {
        let base_branch_id = self
            .inner
            .branch_name_to_branch_id
            ._get(base_branch_name)
            .c(d!("base branch not found"))?;

        self.inner
            .branch_create_by_base_branch(branch_name, base_branch_id)
            .c(d!())
    }

    /// Check if a branch exists or not.
    #[inline(always)]
    pub fn branch_exists(&self, branch_name: &[u8]) -> bool {
        if let Some(branch_id) = self.inner.branch_name_to_branch_id._get(branch_name) {
            self.inner.branch_exists(branch_id)
        } else {
            false
        }
    }

    /// Remove a branch, remove all changes directly made by this branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    #[inline(always)]
    pub fn branch_remove(&mut self, branch_name: &[u8]) -> Result<()> {
        if let Some(branch_id) = self.inner.branch_name_to_branch_id._get(branch_name) {
            self.inner.branch_remove(branch_id).c(d!())
        } else {
            Err(eg!("branch not found"))
        }
    }

    /// Remove all changes directly made by versions(bigger than `last_version_id`) of this branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    #[inline(always)]
    pub fn branch_truncate(&mut self, branch_name: &[u8]) -> Result<()> {
        let branch_id = self
            .inner
            .branch_name_to_branch_id
            ._get(branch_name)
            .c(d!("branch not found"))?;
        self.inner.branch_truncate(branch_id).c(d!())
    }

    /// Remove all changes directly made by versions(bigger than `last_version_id`) of this branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    #[inline(always)]
    pub fn branch_truncate_to(
        &mut self,
        branch_name: &[u8],
        last_version_name: &[u8],
    ) -> Result<()> {
        let branch_id = self
            .inner
            .branch_name_to_branch_id
            ._get(branch_name)
            .c(d!("branch not found"))?;
        let last_version_id = self
            .inner
            .version_name_to_version_id
            ._get(last_version_name)
            .c(d!("version not found"))?;
        self.inner
            .branch_truncate_to(branch_id, last_version_id)
            .c(d!())
    }

    /// Remove the newest version on a specified branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    #[inline(always)]
    pub fn branch_pop_version(&mut self, branch_name: &[u8]) -> Result<()> {
        let branch_id = self
            .inner
            .branch_name_to_branch_id
            ._get(branch_name)
            .c(d!("branch not found"))?;
        self.inner.branch_pop_version(branch_id).c(d!())
    }

    /// Merge a branch to its parent branch.
    #[inline(always)]
    pub fn branch_merge_to_parent(&mut self, branch_name: &[u8]) -> Result<()> {
        let branch_id = self
            .inner
            .branch_name_to_branch_id
            ._get(branch_name)
            .c(d!("branch not found"))?;
        self.inner.branch_merge_to_parent(branch_id).c(d!())
    }

    /// Check if a branch has children branches.
    #[inline(always)]
    pub fn branch_has_children(&self, branch_name: &[u8]) -> bool {
        if let Some(id) = self.inner.branch_name_to_branch_id._get(branch_name) {
            self.inner.branch_has_children(id)
        } else {
            false
        }
    }

    /// Get the signature of the head of the default branch.
    #[inline(always)]
    pub fn sig_get(&self) -> Option<VerSig> {
        self.inner.sig_get()
    }

    /// Get the signature of the head of a specified branch.
    #[inline(always)]
    pub fn sig_get_by_branch(&self, branch_name: &[u8]) -> Option<VerSig> {
        let branch_id = self.inner.branch_name_to_branch_id._get(branch_name)?;
        self.inner.sig_get_by_branch(branch_id)
    }

    /// Get the signature of a specified version of a specified branch.
    #[inline(always)]
    pub fn sig_get_by_branch_version(
        &self,
        branch_name: &[u8],
        version_name: &[u8],
    ) -> Option<VerSig> {
        let branch_id = self.inner.branch_name_to_branch_id._get(branch_name)?;
        let version_id = self.inner.version_name_to_version_id._get(version_name)?;
        self.inner
            .sig_get_by_branch_version(branch_id, Some(version_id))
    }

    /// Clean outdated versions out of the default reserved number.
    #[inline(always)]
    pub fn prune(&mut self) -> Result<()> {
        self.inner.prune().c(d!())
    }

    /// Clean outdated versions out of a specified reserved number.
    #[inline(always)]
    pub fn prune_by_branch(
        &mut self,
        branch_name: &[u8],
        reserved_ver_num: usize,
    ) -> Result<()> {
        let branch_id = self
            .inner
            .branch_name_to_branch_id
            ._get(branch_name)
            .c(d!())?;
        self.inner
            .prune_by_branch(branch_id, reserved_ver_num)
            .c(d!())
    }
}
