//!
//! # Vs functions
//!
//! # Examples
//!
//! Used as version-ful:
//!
//! ```
//! use vsdb::versioned::mapx_raw::MapxRawVs;
//!
//! // TODO
//! let _l = MapxRawVs::new();
//! ```
//!
//! Used as version-less(not recommand, use `MapxRaw` instead):
//!
//! ```
//! use vsdb::{VersionName, versioned::mapx_raw::MapxRawVs};
//!
//! let mut l = MapxRawVs::new();
//! l.version_create(VersionName(b"test")).unwrap();
//!
//! l.insert(&[1], &[0]);
//! l.insert(&[1], &[0]);
//! l.insert(&[2], &[0]);
//!
//! l.iter().for_each(|(_, v)| {
//!     assert_eq!(&v[..], &[0]);
//! });
//!
//! l.remove(&[2]);
//! assert_eq!(l.len(), 1);
//!
//! l.clear();
//! assert_eq!(l.len(), 0);
//! ```
//!

mod backend;

#[cfg(test)]
mod test;

use crate::{
    common::{
        BranchName, ParentBranchName, RawKey, RawValue, VersionName,
        INITIAL_BRANCH_NAME, NULL,
    },
    VsMgmt,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;

pub(crate) use backend::MapxRawVsIter;

/// Advanced `MapxRaw`, with versioned feature.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapxRawVs {
    inner: backend::MapxRawVs,
}

impl Default for MapxRawVs {
    fn default() -> Self {
        Self::new()
    }
}

impl MapxRawVs {
    #[inline(always)]
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {
            inner: backend::MapxRawVs::new(),
        }
    }

    /// Insert a KV to the head version of the default branch.
    #[inline(always)]
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<Option<RawValue>> {
        self.inner.insert(key, value).c(d!())
    }

    /// Insert a KV to the head version of a specified branch.
    #[inline(always)]
    pub fn insert_by_branch(
        &self,
        key: &[u8],
        value: &[u8],
        branch_name: BranchName,
    ) -> Result<Option<RawValue>> {
        let branch_id = self.inner.get_branch_id(branch_name).c(d!())?;
        self.inner.insert_by_branch(key, value, branch_id).c(d!())
    }

    /// Remove a KV from the head version of the default branch.
    #[inline(always)]
    pub fn remove(&self, key: &[u8]) -> Result<Option<RawValue>> {
        self.inner.remove(key).c(d!())
    }

    /// Remove a KV from the head version of a specified branch.
    #[inline(always)]
    pub fn remove_by_branch(
        &self,
        key: &[u8],
        branch_name: BranchName,
    ) -> Result<Option<RawValue>> {
        let branch_id = self.inner.get_branch_id(branch_name).c(d!())?;
        self.inner.remove_by_branch(key, branch_id).c(d!())
    }

    /// Get the value of a key from the default branch.
    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<RawValue> {
        self.inner.get(key)
    }

    /// Get the value of a key from the head of a specified branch.
    #[inline(always)]
    pub fn get_by_branch(
        &self,
        key: &[u8],
        branch_name: BranchName,
    ) -> Option<RawValue> {
        let branch_id = self.inner.get_branch_id(branch_name)?;
        self.inner.get_by_branch(key, branch_id)
    }

    /// Get the value of a key from a specified version of a specified branch.
    #[inline(always)]
    pub fn get_by_branch_version(
        &self,
        key: &[u8],
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<RawValue> {
        let branch_id = self.inner.get_branch_id(branch_name)?;
        let version_id = self.inner.get_version_id(branch_name, version_name)?;
        self.inner.get_by_branch_version(key, branch_id, version_id)
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
        branch_name: BranchName,
    ) -> Option<(RawKey, RawValue)> {
        let branch_id = self.inner.get_branch_id(branch_name)?;
        self.inner.get_ge_by_branch(key, branch_id)
    }

    /// Get the value of a key from a specified version of a specified branch,
    /// if the target key does not exist, will try to
    /// search a closest value bigger than the target key.
    #[inline(always)]
    pub fn get_ge_by_branch_version(
        &self,
        key: &[u8],
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<(RawKey, RawValue)> {
        let branch_id = self.inner.get_branch_id(branch_name)?;
        let version_id = self.inner.get_version_id(branch_name, version_name)?;
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
        branch_name: BranchName,
    ) -> Option<(RawKey, RawValue)> {
        let branch_id = self.inner.get_branch_id(branch_name)?;
        self.inner.get_le_by_branch(key, branch_id)
    }

    /// Get the value of a key from a specified version of a specified branch,
    /// if the target key does not exist, will try to
    /// search a closest value bigger than the target key.
    #[inline(always)]
    pub fn get_le_by_branch_version(
        &self,
        key: &[u8],
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<(RawKey, RawValue)> {
        let branch_id = self.inner.get_branch_id(branch_name)?;
        let version_id = self.inner.get_version_id(branch_name, version_name)?;
        self.inner
            .get_le_by_branch_version(key, branch_id, version_id)
    }

    /// Create an iterator over the default branch.
    #[inline(always)]
    pub fn iter(&self) -> MapxRawVsIter {
        self.inner.iter()
    }

    /// Create an iterator over a specified branch.
    #[inline(always)]
    pub fn iter_by_branch(&self, branch_name: BranchName) -> MapxRawVsIter {
        let branch_id = self.inner.get_branch_id(branch_name).unwrap_or(NULL);
        self.inner.iter_by_branch(branch_id)
    }

    /// Create an iterator over a specified version of a specified branch.
    #[inline(always)]
    pub fn iter_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> MapxRawVsIter {
        let branch_id = self.inner.get_branch_id(branch_name).unwrap_or(NULL);
        let version_id = self
            .inner
            .get_version_id(branch_name, version_name)
            .unwrap_or(NULL);
        self.inner.iter_by_branch_version(branch_id, version_id)
    }

    /// Create a range iterator over the default branch.
    #[inline(always)]
    pub fn range<'a, R: 'a + RangeBounds<RawKey>>(
        &'a self,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        self.inner.range(bounds)
    }

    /// Create a range iterator over a specified branch.
    #[inline(always)]
    pub fn range_by_branch<'a, R: 'a + RangeBounds<RawKey>>(
        &'a self,
        branch_name: BranchName,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        let branch_id = self.inner.get_branch_id(branch_name).unwrap_or(NULL);
        self.inner.range_by_branch(branch_id, bounds)
    }

    /// Create a range iterator over a specified version of a specified branch.
    #[inline(always)]
    pub fn range_by_branch_version<'a, R: 'a + RangeBounds<RawKey>>(
        &'a self,
        branch_name: BranchName,
        version_name: VersionName,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        let branch_id = self.inner.get_branch_id(branch_name).unwrap_or(NULL);
        let version_id = self
            .inner
            .get_version_id(branch_name, version_name)
            .unwrap_or(NULL);
        self.inner
            .range_by_branch_version(branch_id, version_id, bounds)
    }

    /// Create a range iterator over the default branch.
    #[inline(always)]
    pub fn range_ref<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        self.inner.range_ref(bounds)
    }

    /// Create a range iterator over a specified branch.
    #[inline(always)]
    pub fn range_ref_by_branch<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        branch_name: BranchName,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        let branch_id = self.inner.get_branch_id(branch_name).unwrap_or(NULL);
        self.inner.range_ref_by_branch(branch_id, bounds)
    }

    /// Create a range iterator over a specified version of a specified branch.
    #[inline(always)]
    pub fn range_ref_by_branch_version<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        branch_name: BranchName,
        version_name: VersionName,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        let branch_id = self.inner.get_branch_id(branch_name).unwrap_or(NULL);
        let version_id = self
            .inner
            .get_version_id(branch_name, version_name)
            .unwrap_or(NULL);
        self.inner
            .range_ref_by_branch_version(branch_id, version_id, bounds)
    }

    /// Check if a key exist on the default branch.
    #[inline(always)]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Check if a key exist on a specified branch.
    #[inline(always)]
    pub fn contains_key_by_branch(&self, key: &[u8], branch_name: BranchName) -> bool {
        self.get_by_branch(key, branch_name).is_some()
    }

    /// Check if a key exist on a specified version of a specified branch.
    #[inline(always)]
    pub fn contains_key_by_branch_version(
        &self,
        key: &[u8],
        branch_name: BranchName,
        version_name: VersionName,
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
    pub fn len_by_branch(&self, branch_name: BranchName) -> usize {
        self.inner
            .get_branch_id(branch_name)
            .map(|id| self.inner.len_by_branch(id))
            .unwrap_or(0)
    }

    /// Get the total number of items of a specified version of a specified branch.
    #[inline(always)]
    pub fn len_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> usize {
        self.inner
            .get_branch_id(branch_name)
            .and_then(|br_id| {
                self.inner
                    .get_version_id(branch_name, version_name)
                    .map(|ver_id| self.inner.len_by_branch_version(br_id, ver_id))
            })
            .unwrap_or(0)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn is_empty_by_branch(&self, branch_name: BranchName) -> bool {
        self.iter_by_branch(branch_name).next().is_none()
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn is_empty_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> bool {
        self.iter_by_branch_version(branch_name, version_name)
            .next()
            .is_none()
    }

    /// Clear all data, mainly for testing purpose.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl VsMgmt for MapxRawVs {
    /// Create a new version on the default branch.
    #[inline(always)]
    fn version_create(&self, version_name: VersionName) -> Result<()> {
        self.inner.version_create(version_name.0).c(d!())
    }

    /// Create a new version on a specified branch,
    /// NOTE: the branch must has been created.
    #[inline(always)]
    fn version_create_by_branch(
        &self,
        version_name: VersionName,
        branch_name: BranchName,
    ) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|br_id| {
                self.inner
                    .version_create_by_branch(version_name.0, br_id)
                    .c(d!())
            })
    }

    /// Check if a verison exists on default branch.
    #[inline(always)]
    fn version_exists(&self, version_name: VersionName) -> bool {
        self.inner
            .get_version_id(BranchName(INITIAL_BRANCH_NAME), version_name)
            .map(|id| self.inner.version_exists(id))
            .unwrap_or(false)
    }

    /// Check if a version exists on a specified branch(include its parents).
    #[inline(always)]
    fn version_exists_on_branch(
        &self,
        version_name: VersionName,
        branch_name: BranchName,
    ) -> bool {
        self.inner
            .get_branch_id(branch_name)
            .and_then(|br_id| {
                self.inner
                    .get_version_id(branch_name, version_name)
                    .map(|ver_id| self.inner.version_exists_on_branch(ver_id, br_id).0)
            })
            .unwrap_or(false)
    }

    /// Check if a version is directly created on the default branch.
    #[inline(always)]
    fn version_created(&self, version_name: VersionName) -> bool {
        self.version_created_on_branch(version_name, BranchName(INITIAL_BRANCH_NAME))
    }

    /// Check if a version is directly created on a specified branch(exclude its parents).
    #[inline(always)]
    fn version_created_on_branch(
        &self,
        version_name: VersionName,
        branch_name: BranchName,
    ) -> bool {
        self.inner
            .get_branch_id(branch_name)
            .and_then(|br_id| {
                self.inner
                    .get_version_id(branch_name, version_name)
                    .map(|ver_id| self.inner.version_created_on_branch(ver_id, br_id))
            })
            .unwrap_or(false)
    }

    /// Remove the newest version on the default branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    #[inline(always)]
    fn version_pop(&self) -> Result<()> {
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
    fn version_pop_by_branch(&self, branch_name: BranchName) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|br_id| self.inner.version_pop_by_branch(br_id).c(d!()))
    }

    /// Create a new branch based on the head of the default branch.
    #[inline(always)]
    fn branch_create(&self, branch_name: BranchName) -> Result<()> {
        self.inner.branch_create(branch_name.0).c(d!())
    }

    /// Create a new branch based on the head of a specified branch.
    #[inline(always)]
    fn branch_create_by_base_branch(
        &self,
        branch_name: BranchName,
        base_branch_name: ParentBranchName,
    ) -> Result<()> {
        self.inner
            .get_branch_id(BranchName(base_branch_name.0))
            .c(d!("base branch not found"))
            .and_then(|base_br_id| {
                self.inner
                    .branch_create_by_base_branch(branch_name.0, base_br_id)
                    .c(d!())
            })
    }

    /// Create a new branch based on a specified version of a specified branch.
    #[inline(always)]
    fn branch_create_by_base_branch_version(
        &self,
        branch_name: BranchName,
        base_branch_name: ParentBranchName,
        base_version_name: VersionName,
    ) -> Result<()> {
        let base_br_id = self
            .inner
            .get_branch_id(BranchName(base_branch_name.0))
            .c(d!("base branch not found"))?;
        let base_ver_id = self
            .inner
            .get_version_id(BranchName(base_branch_name.0), base_version_name)
            .c(d!("base vesion not found"))?;
        self.inner
            .branch_create_by_base_branch_version(branch_name.0, base_br_id, base_ver_id)
            .c(d!())
    }

    /// Check if a branch exists or not.
    #[inline(always)]
    fn branch_exists(&self, branch_name: BranchName) -> bool {
        self.inner
            .get_branch_id(branch_name)
            .map(|id| self.inner.branch_exists(id))
            .unwrap_or(false)
    }

    /// Remove a branch, remove all changes directly made by this branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    #[inline(always)]
    fn branch_remove(&self, branch_name: BranchName) -> Result<()> {
        if let Some(branch_id) = self.inner.get_branch_id(branch_name) {
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
    fn branch_truncate(&self, branch_name: BranchName) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|br_id| self.inner.branch_truncate(br_id).c(d!()))
    }

    /// Remove all changes directly made by versions(bigger than `last_version_id`) of this branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    #[inline(always)]
    fn branch_truncate_to(
        &self,
        branch_name: BranchName,
        last_version_name: VersionName,
    ) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|br_id| {
                self.inner
                    .get_version_id(branch_name, last_version_name)
                    .c(d!("version not found"))
                    .and_then(|last_ver_id| {
                        self.inner.branch_truncate_to(br_id, last_ver_id).c(d!())
                    })
            })
    }

    /// Remove the newest version on a specified branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    #[inline(always)]
    fn branch_pop_version(&self, branch_name: BranchName) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|id| self.inner.branch_pop_version(id).c(d!()))
    }

    /// Merge a branch to its parent branch.
    #[inline(always)]
    fn branch_merge_to_parent(&self, branch_name: BranchName) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|id| self.inner.branch_merge_to_parent(id).c(d!()))
    }

    /// Check if a branch has children branches.
    #[inline(always)]
    fn branch_has_children(&self, branch_name: BranchName) -> bool {
        self.inner
            .get_branch_id(branch_name)
            .map(|id| self.inner.branch_has_children(id))
            .unwrap_or(false)
    }

    /// Make a branch to be default,
    /// all default operations will be applied to it.
    #[inline(always)]
    fn branch_set_default(&mut self, branch_name: BranchName) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|brid| self.inner.branch_set_default(brid).c(d!()))
    }

    /// Clean outdated versions out of the default reserved number.
    #[inline(always)]
    fn prune(&self, reserved_ver_num: Option<usize>) -> Result<()> {
        self.inner.prune(reserved_ver_num).c(d!())
    }

    /// Clean outdated versions out of a specified reserved number.
    #[inline(always)]
    fn prune_by_branch(
        &self,
        branch_name: BranchName,
        reserved_ver_num: Option<usize>,
    ) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!())
            .and_then(|br_id| {
                self.inner.prune_by_branch(br_id, reserved_ver_num).c(d!())
            })
    }
}
