//!
//! # Multi-key VS functions
//!

mod backend;

#[cfg(test)]
mod test;

use crate::{
    common::{BranchName, ParentBranchName, RawValue, VersionName},
    BranchNameOwned, VersionNameOwned, VsMgmt,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    ops::{Deref, DerefMut},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapxRawMkVs {
    inner: backend::MapxRawMkVs,
}

impl MapxRawMkVs {
    /// # Safety
    ///
    /// This API breaks the semantic safety guarantees,
    /// but it is safe to use in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        Self {
            inner: self.inner.shadow(),
        }
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn new(key_size: usize) -> Self {
        Self {
            inner: backend::MapxRawMkVs::new(key_size),
        }
    }

    #[inline(always)]
    pub fn insert(&mut self, key: &[&[u8]], value: &[u8]) -> Result<Option<RawValue>> {
        self.inner.insert(key, value).c(d!())
    }

    #[inline(always)]
    pub fn insert_by_branch(
        &mut self,
        key: &[&[u8]],
        value: &[u8],
        br_name: BranchName,
    ) -> Result<Option<RawValue>> {
        let br_id = self.inner.branch_get_id_by_name(br_name).c(d!())?;
        self.inner.insert_by_branch(key, value, br_id).c(d!())
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &[&[u8]]) -> Result<Option<RawValue>> {
        self.inner.remove(key).c(d!())
    }

    #[inline(always)]
    pub fn remove_by_branch(
        &mut self,
        key: &[&[u8]],
        br_name: BranchName,
    ) -> Result<Option<RawValue>> {
        let br_id = self.inner.branch_get_id_by_name(br_name).c(d!())?;
        self.inner.remove_by_branch(key, br_id).c(d!())
    }

    #[inline(always)]
    pub fn get(&self, key: &[&[u8]]) -> Option<RawValue> {
        self.inner.get(key)
    }

    #[inline(always)]
    pub fn get_mut<'a>(&'a mut self, key: &'a [&'a [u8]]) -> Option<ValueMut<'a>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    #[inline(always)]
    pub fn entry<'a>(&'a mut self, key: &'a [&'a [u8]]) -> Entry<'a> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn get_by_branch(&self, key: &[&[u8]], br_name: BranchName) -> Option<RawValue> {
        let br_id = self.inner.branch_get_id_by_name(br_name)?;
        self.inner.get_by_branch(key, br_id)
    }

    #[inline(always)]
    pub fn get_by_branch_version(
        &self,
        key: &[&[u8]],
        br_name: BranchName,
        ver_name: VersionName,
    ) -> Option<RawValue> {
        let br_id = self.inner.branch_get_id_by_name(br_name)?;
        let ver_id = self.inner.version_get_id_by_name(ver_name)?;
        self.inner.get_by_branch_version(key, br_id, ver_id)
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &[&[u8]]) -> bool {
        self.get(key).is_some()
    }

    #[inline(always)]
    pub fn contains_key_by_branch(&self, key: &[&[u8]], br_name: BranchName) -> bool {
        self.get_by_branch(key, br_name).is_some()
    }

    #[inline(always)]
    pub fn contains_key_by_branch_version(
        &self,
        key: &[&[u8]],
        br_name: BranchName,
        ver_name: VersionName,
    ) -> bool {
        self.get_by_branch_version(key, br_name, ver_name).is_some()
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline(always)]
    pub fn iter_op<F>(&self, op: &mut F) -> Result<()>
    where
        F: FnMut(&[&[u8]], RawValue) -> Result<()>,
    {
        self.inner.iter_op(op).c(d!())
    }

    #[inline(always)]
    pub fn iter_op_with_key_prefix<F>(
        &self,
        op: &mut F,
        key_prefix: &[&[u8]],
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], RawValue) -> Result<()>,
    {
        self.inner.iter_op_with_key_prefix(op, key_prefix).c(d!())
    }

    #[inline(always)]
    pub fn iter_op_by_branch<F>(&self, br_name: BranchName, op: &mut F) -> Result<()>
    where
        F: FnMut(&[&[u8]], RawValue) -> Result<()>,
    {
        let br_id = self.inner.branch_get_id_by_name(br_name).c(d!())?;
        self.inner.iter_op_by_branch(br_id, op).c(d!())
    }

    #[inline(always)]
    pub fn iter_op_with_key_prefix_by_branch<F>(
        &self,
        br_name: BranchName,
        op: &mut F,
        key_prefix: &[&[u8]],
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], RawValue) -> Result<()>,
    {
        let br_id = self.inner.branch_get_id_by_name(br_name).c(d!())?;
        self.inner
            .iter_op_with_key_prefix_by_branch(br_id, op, key_prefix)
            .c(d!())
    }

    #[inline(always)]
    pub fn iter_op_by_branch_version<F>(
        &self,
        br_name: BranchName,
        ver_name: VersionName,
        op: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], RawValue) -> Result<()>,
    {
        let br_id = self.inner.branch_get_id_by_name(br_name).c(d!())?;
        let ver_id = self.inner.version_get_id_by_name(ver_name).c(d!())?;
        self.inner
            .iter_op_by_branch_version(br_id, ver_id, op)
            .c(d!())
    }

    #[inline(always)]
    pub fn iter_op_with_key_prefix_by_branch_version<F>(
        &self,
        br_name: BranchName,
        ver_name: VersionName,
        op: &mut F,
        key_prefix: &[&[u8]],
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], RawValue) -> Result<()>,
    {
        let br_id = self.inner.branch_get_id_by_name(br_name).c(d!())?;
        let ver_id = self.inner.version_get_id_by_name(ver_name).c(d!())?;
        self.inner
            .iter_op_with_key_prefix_by_branch_version(br_id, ver_id, op, key_prefix)
            .c(d!())
    }
}

impl VsMgmt for MapxRawMkVs {
    #[inline(always)]
    fn version_create(&mut self, ver_name: VersionName) -> Result<()> {
        self.inner.version_create(ver_name.0).c(d!())
    }

    #[inline(always)]
    fn version_create_by_branch(
        &mut self,
        ver_name: VersionName,
        br_name: BranchName,
    ) -> Result<()> {
        self.inner
            .branch_get_id_by_name(br_name)
            .c(d!("branch not found"))
            .and_then(|br_id| {
                self.inner
                    .version_create_by_branch(ver_name.0, br_id)
                    .c(d!())
            })
    }

    #[inline(always)]
    fn version_exists(&self, ver_name: VersionName) -> bool {
        self.inner
            .version_get_id_by_name(ver_name)
            .map(|id| self.inner.version_exists(id))
            .unwrap_or(false)
    }

    #[inline(always)]
    fn version_exists_on_branch(
        &self,
        ver_name: VersionName,
        br_name: BranchName,
    ) -> bool {
        self.inner
            .branch_get_id_by_name(br_name)
            .and_then(|br_id| {
                self.inner
                    .version_get_id_by_name(ver_name)
                    .map(|ver_id| self.inner.version_exists_on_branch(ver_id, br_id))
            })
            .unwrap_or(false)
    }

    #[inline(always)]
    fn version_pop(&mut self) -> Result<()> {
        self.inner.version_pop().c(d!())
    }

    #[inline(always)]
    fn version_pop_by_branch(&mut self, br_name: BranchName) -> Result<()> {
        self.inner
            .branch_get_id_by_name(br_name)
            .c(d!("branch not found"))
            .and_then(|br_id| self.inner.version_pop_by_branch(br_id).c(d!()))
    }

    #[inline(always)]
    unsafe fn version_rebase(&mut self, base_version: VersionName) -> Result<()> {
        self.inner
            .version_get_id_by_name(base_version)
            .c(d!())
            .and_then(|bv| self.inner.version_rebase(bv).c(d!()))
    }

    #[inline(always)]
    unsafe fn version_rebase_by_branch(
        &mut self,
        base_version: VersionName,
        br_name: BranchName,
    ) -> Result<()> {
        let bv = self.inner.version_get_id_by_name(base_version).c(d!())?;
        let brid = self.inner.branch_get_id_by_name(br_name).c(d!())?;
        self.inner.version_rebase_by_branch(bv, brid).c(d!())
    }

    #[inline(always)]
    fn version_exists_globally(&self, ver_name: VersionName) -> bool {
        self.inner
            .version_get_id_by_name(ver_name)
            .map(|verid| self.inner.version_exists_globally(verid))
            .unwrap_or(false)
    }

    #[inline(always)]
    fn version_list(&self) -> Result<Vec<VersionNameOwned>> {
        self.inner.version_list().c(d!())
    }

    #[inline(always)]
    fn version_list_by_branch(
        &self,
        br_name: BranchName,
    ) -> Result<Vec<VersionNameOwned>> {
        self.inner
            .branch_get_id_by_name(br_name)
            .c(d!("branch not found"))
            .and_then(|brid| self.inner.version_list_by_branch(brid).c(d!()))
    }

    #[inline(always)]
    fn version_list_globally(&self) -> Vec<VersionNameOwned> {
        self.inner.version_list_globally()
    }

    #[inline(always)]
    fn version_has_change_set(&self, ver_name: VersionName) -> Result<bool> {
        self.inner
            .version_get_id_by_name(ver_name)
            .c(d!("version not found"))
            .and_then(|verid| self.inner.version_has_change_set(verid).c(d!()))
    }

    #[inline(always)]
    fn version_clean_up_globally(&mut self) -> Result<()> {
        self.inner.version_clean_up_globally().c(d!())
    }

    #[inline(always)]
    unsafe fn version_revert_globally(&mut self, ver_name: VersionName) -> Result<()> {
        self.inner
            .version_get_id_by_name(ver_name)
            .c(d!("version not found"))
            .and_then(|verid| self.inner.version_revert_globally(verid).c(d!()))
    }

    #[inline(always)]
    fn version_chgset_trie_root(
        &self,
        br_name: Option<BranchName>,
        ver_name: Option<VersionName>,
    ) -> Result<Vec<u8>> {
        let brid = if let Some(bn) = br_name {
            Some(
                self.inner
                    .branch_get_id_by_name(bn)
                    .c(d!("version not found"))?,
            )
        } else {
            None
        };

        let verid = if let Some(vn) = ver_name {
            Some(
                self.inner
                    .version_get_id_by_name(vn)
                    .c(d!("version not found"))?,
            )
        } else {
            None
        };

        self.inner.version_chgset_trie_root(brid, verid).c(d!())
    }

    #[inline(always)]
    fn branch_create(
        &mut self,
        br_name: BranchName,
        ver_name: VersionName,
        force: bool,
    ) -> Result<()> {
        self.inner
            .branch_create(br_name.0, ver_name.0, force)
            .c(d!())
    }

    #[inline(always)]
    fn branch_create_by_base_branch(
        &mut self,
        br_name: BranchName,
        ver_name: VersionName,
        base_br_name: ParentBranchName,
        force: bool,
    ) -> Result<()> {
        self.inner
            .branch_get_id_by_name(BranchName(base_br_name.0))
            .c(d!("base branch not found"))
            .and_then(|base_br_id| {
                self.inner
                    .branch_create_by_base_branch(
                        br_name.0, ver_name.0, base_br_id, force,
                    )
                    .c(d!())
            })
    }

    #[inline(always)]
    fn branch_create_by_base_branch_version(
        &mut self,
        br_name: BranchName,
        ver_name: VersionName,
        base_br_name: ParentBranchName,
        base_ver_name: VersionName,
        force: bool,
    ) -> Result<()> {
        let base_br_id = self
            .inner
            .branch_get_id_by_name(BranchName(base_br_name.0))
            .c(d!("base branch not found"))?;
        let base_ver_id = self
            .inner
            .version_get_id_by_name(base_ver_name)
            .c(d!("base vesion not found"))?;
        self.inner
            .branch_create_by_base_branch_version(
                br_name.0,
                ver_name.0,
                base_br_id,
                base_ver_id,
                force,
            )
            .c(d!())
    }

    #[inline(always)]
    unsafe fn branch_create_without_new_version(
        &mut self,
        br_name: BranchName,
        force: bool,
    ) -> Result<()> {
        self.inner
            .branch_create_without_new_version(br_name.0, force)
            .c(d!())
    }

    #[inline(always)]
    unsafe fn branch_create_by_base_branch_without_new_version(
        &mut self,
        br_name: BranchName,
        base_br_name: ParentBranchName,
        force: bool,
    ) -> Result<()> {
        self.inner
            .branch_get_id_by_name(BranchName(base_br_name.0))
            .c(d!("base branch not found"))
            .and_then(|base_br_id| {
                self.inner
                    .branch_create_by_base_branch_without_new_version(
                        br_name.0, base_br_id, force,
                    )
                    .c(d!())
            })
    }

    #[inline(always)]
    unsafe fn branch_create_by_base_branch_version_without_new_version(
        &mut self,
        br_name: BranchName,
        base_br_name: ParentBranchName,
        base_ver_name: VersionName,
        force: bool,
    ) -> Result<()> {
        let base_br_id = self
            .inner
            .branch_get_id_by_name(BranchName(base_br_name.0))
            .c(d!("base branch not found"))?;
        let base_ver_id = self
            .inner
            .version_get_id_by_name(base_ver_name)
            .c(d!("base vesion not found"))?;
        self.inner
            .branch_create_by_base_branch_version_without_new_version(
                br_name.0,
                base_br_id,
                base_ver_id,
                force,
            )
            .c(d!())
    }

    #[inline(always)]
    fn branch_exists(&self, br_name: BranchName) -> bool {
        self.inner
            .branch_get_id_by_name(br_name)
            .map(|id| self.inner.branch_exists(id))
            .unwrap_or(false)
    }

    #[inline(always)]
    fn branch_has_versions(&self, br_name: BranchName) -> bool {
        self.inner
            .branch_get_id_by_name(br_name)
            .map(|id| self.inner.branch_has_versions(id))
            .unwrap_or(false)
    }

    #[inline(always)]
    fn branch_remove(&mut self, br_name: BranchName) -> Result<()> {
        if let Some(br_id) = self.inner.branch_get_id_by_name(br_name) {
            self.inner.branch_remove(br_id).c(d!())
        } else {
            Err(eg!("branch not found"))
        }
    }

    /// Clean up all other branches not in the list.
    #[inline(always)]
    fn branch_keep_only(&mut self, br_names: &[BranchName]) -> Result<()> {
        let br_ids = br_names
            .iter()
            .copied()
            .map(|brname| {
                self.inner
                    .branch_get_id_by_name(brname)
                    .c(d!("version not found"))
            })
            .collect::<Result<BTreeSet<_>>>()?
            .into_iter()
            .collect::<Vec<_>>();
        self.inner.branch_keep_only(&br_ids).c(d!())
    }

    #[inline(always)]
    fn branch_truncate(&mut self, br_name: BranchName) -> Result<()> {
        self.inner
            .branch_get_id_by_name(br_name)
            .c(d!("branch not found"))
            .and_then(|br_id| self.inner.branch_truncate(br_id).c(d!()))
    }

    #[inline(always)]
    fn branch_truncate_to(
        &mut self,
        br_name: BranchName,
        last_ver_name: VersionName,
    ) -> Result<()> {
        self.inner
            .branch_get_id_by_name(br_name)
            .c(d!("branch not found"))
            .and_then(|br_id| {
                self.inner
                    .version_get_id_by_name(last_ver_name)
                    .c(d!("version not found"))
                    .and_then(|last_ver_id| {
                        self.inner.branch_truncate_to(br_id, last_ver_id).c(d!())
                    })
            })
    }

    #[inline(always)]
    fn branch_pop_version(&mut self, br_name: BranchName) -> Result<()> {
        self.inner
            .branch_get_id_by_name(br_name)
            .c(d!("branch not found"))
            .and_then(|id| self.inner.branch_pop_version(id).c(d!()))
    }

    #[inline(always)]
    fn branch_merge_to(
        &mut self,
        br_name: BranchName,
        target_br_name: BranchName,
    ) -> Result<()> {
        self.inner
            .branch_get_id_by_name(br_name)
            .c(d!("branch not found"))
            .and_then(|brid| {
                let target_brid = self
                    .inner
                    .branch_get_id_by_name(target_br_name)
                    .c(d!("target branch not found"))?;
                self.inner.branch_merge_to(brid, target_brid).c(d!())
            })
    }

    #[inline(always)]
    unsafe fn branch_merge_to_force(
        &mut self,
        br_name: BranchName,
        target_br_name: BranchName,
    ) -> Result<()> {
        self.inner
            .branch_get_id_by_name(br_name)
            .c(d!("branch not found"))
            .and_then(|brid| {
                let target_brid = self
                    .inner
                    .branch_get_id_by_name(target_br_name)
                    .c(d!("target branch not found"))?;
                self.inner.branch_merge_to_force(brid, target_brid).c(d!())
            })
    }

    #[inline(always)]
    fn branch_set_default(&mut self, br_name: BranchName) -> Result<()> {
        self.inner
            .branch_get_id_by_name(br_name)
            .c(d!("branch not found"))
            .and_then(|brid| self.inner.branch_set_default(brid).c(d!()))
    }

    #[inline(always)]
    fn branch_is_empty(&self, br_name: BranchName) -> Result<bool> {
        self.inner
            .branch_get_id_by_name(br_name)
            .c(d!("branch not found"))
            .and_then(|brid| self.inner.branch_is_empty(brid).c(d!()))
    }

    #[inline(always)]
    fn branch_list(&self) -> Vec<BranchNameOwned> {
        self.inner.branch_list()
    }

    #[inline(always)]
    fn branch_get_default(&self) -> BranchNameOwned {
        self.inner.branch_get_default_name()
    }

    #[inline(always)]
    unsafe fn branch_swap(
        &mut self,
        branch_1: BranchName,
        branch_2: BranchName,
    ) -> Result<()> {
        self.inner.branch_swap(branch_1.0, branch_2.0).c(d!())
    }

    #[inline(always)]
    fn prune(&mut self, reserved_ver_num: Option<usize>) -> Result<()> {
        self.inner.prune(reserved_ver_num).c(d!())
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a> {
    hdr: &'a mut MapxRawMkVs,
    key: &'a [&'a [u8]],
    value: RawValue,
}

impl<'a> ValueMut<'a> {
    fn new(hdr: &'a mut MapxRawMkVs, key: &'a [&'a [u8]], value: RawValue) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<'a> Drop for ValueMut<'a> {
    fn drop(&mut self) {
        pnk!(self.hdr.insert(self.key, &self.value));
    }
}

impl<'a> Deref for ValueMut<'a> {
    type Target = RawValue;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a> DerefMut for ValueMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub struct Entry<'a> {
    hdr: &'a mut MapxRawMkVs,
    key: &'a [&'a [u8]],
}

impl<'a> Entry<'a> {
    pub fn or_insert(self, default: &'a [u8]) -> ValueMut<'a> {
        if !self.hdr.contains_key(self.key) {
            pnk!(self.hdr.insert(self.key, default));
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}
