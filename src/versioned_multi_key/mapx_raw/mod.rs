//!
//! # Multi-key VS functions
//!

mod backend;

#[cfg(test)]
mod test;

use crate::{
    common::{BranchName, ParentBranchName, RawValue, VersionName},
    VsMgmt,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapxRawMkVs {
    inner: backend::MapxRawMkVs,
}

impl MapxRawMkVs {
    #[inline(always)]
    #[allow(missing_docs)]
    pub fn new(key_size: usize) -> Self {
        Self {
            inner: backend::MapxRawMkVs::new(key_size),
        }
    }

    #[inline(always)]
    pub fn insert(&self, key: &[&[u8]], value: &[u8]) -> Result<Option<RawValue>> {
        self.inner.insert(key, value).c(d!())
    }

    #[inline(always)]
    pub fn insert_by_branch(
        &self,
        key: &[&[u8]],
        value: &[u8],
        branch_name: BranchName,
    ) -> Result<Option<RawValue>> {
        let branch_id = self.inner.get_branch_id(branch_name).c(d!())?;
        self.inner.insert_by_branch(key, value, branch_id).c(d!())
    }

    #[inline(always)]
    pub fn remove(&self, key: &[&[u8]]) -> Result<Option<RawValue>> {
        self.inner.remove(key).c(d!())
    }

    #[inline(always)]
    pub fn remove_by_branch(
        &self,
        key: &[&[u8]],
        branch_name: BranchName,
    ) -> Result<Option<RawValue>> {
        let branch_id = self.inner.get_branch_id(branch_name).c(d!())?;
        self.inner.remove_by_branch(key, branch_id).c(d!())
    }

    #[inline(always)]
    pub fn get(&self, key: &[&[u8]]) -> Option<RawValue> {
        self.inner.get(key)
    }

    #[inline(always)]
    pub fn get_mut<'a>(&'a self, key: &'a [&'a [u8]]) -> Option<ValueMut<'a>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    #[inline(always)]
    pub fn entry_ref<'a>(&'a self, key: &'a [&'a [u8]]) -> Entry<'a> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn get_by_branch(
        &self,
        key: &[&[u8]],
        branch_name: BranchName,
    ) -> Option<RawValue> {
        let branch_id = self.inner.get_branch_id(branch_name)?;
        self.inner.get_by_branch(key, branch_id)
    }

    #[inline(always)]
    pub fn get_by_branch_version(
        &self,
        key: &[&[u8]],
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<RawValue> {
        let branch_id = self.inner.get_branch_id(branch_name)?;
        let version_id = self.inner.get_version_id(version_name)?;
        self.inner.get_by_branch_version(key, branch_id, version_id)
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &[&[u8]]) -> bool {
        self.get(key).is_some()
    }

    #[inline(always)]
    pub fn contains_key_by_branch(
        &self,
        key: &[&[u8]],
        branch_name: BranchName,
    ) -> bool {
        self.get_by_branch(key, branch_name).is_some()
    }

    #[inline(always)]
    pub fn contains_key_by_branch_version(
        &self,
        key: &[&[u8]],
        branch_name: BranchName,
        version_name: VersionName,
    ) -> bool {
        self.get_by_branch_version(key, branch_name, version_name)
            .is_some()
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl VsMgmt for MapxRawMkVs {
    #[inline(always)]
    fn version_create(&self, version_name: VersionName) -> Result<()> {
        self.inner.version_create(version_name.0).c(d!())
    }

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

    #[inline(always)]
    fn version_exists(&self, version_name: VersionName) -> bool {
        self.inner
            .get_version_id(version_name)
            .map(|id| self.inner.version_exists(id))
            .unwrap_or(false)
    }

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
                    .get_version_id(version_name)
                    .map(|ver_id| self.inner.version_exists_on_branch(ver_id, br_id))
            })
            .unwrap_or(false)
    }

    #[inline(always)]
    fn version_pop(&self) -> Result<()> {
        self.inner.version_pop().c(d!())
    }

    #[inline(always)]
    fn version_pop_by_branch(&self, branch_name: BranchName) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|br_id| self.inner.version_pop_by_branch(br_id).c(d!()))
    }

    #[inline(always)]
    unsafe fn version_rebase(&self, base_version: VersionName) -> Result<()> {
        self.inner
            .get_version_id(base_version)
            .c(d!())
            .and_then(|bv| self.inner.version_rebase(bv).c(d!()))
    }

    #[inline(always)]
    unsafe fn version_rebase_by_branch(
        &self,
        base_version: VersionName,
        branch_name: BranchName,
    ) -> Result<()> {
        let bv = self.inner.get_version_id(base_version).c(d!())?;
        let brid = self.inner.get_branch_id(branch_name).c(d!())?;
        self.inner.version_rebase_by_branch(bv, brid).c(d!())
    }

    #[inline(always)]
    fn branch_create(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Result<()> {
        self.inner
            .branch_create(branch_name.0, version_name.0)
            .c(d!())
    }

    #[inline(always)]
    fn branch_create_by_base_branch(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
        base_branch_name: ParentBranchName,
    ) -> Result<()> {
        self.inner
            .get_branch_id(BranchName(base_branch_name.0))
            .c(d!("base branch not found"))
            .and_then(|base_br_id| {
                self.inner
                    .branch_create_by_base_branch(
                        branch_name.0,
                        version_name.0,
                        base_br_id,
                    )
                    .c(d!())
            })
    }

    #[inline(always)]
    fn branch_create_by_base_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
        base_branch_name: ParentBranchName,
        base_version_name: VersionName,
    ) -> Result<()> {
        let base_br_id = self
            .inner
            .get_branch_id(BranchName(base_branch_name.0))
            .c(d!("base branch not found"))?;
        let base_ver_id = self
            .inner
            .get_version_id(base_version_name)
            .c(d!("base vesion not found"))?;
        self.inner
            .branch_create_by_base_branch_version(
                branch_name.0,
                version_name.0,
                base_br_id,
                base_ver_id,
            )
            .c(d!())
    }

    #[inline(always)]
    unsafe fn branch_create_without_new_version(
        &self,
        branch_name: BranchName,
    ) -> Result<()> {
        self.inner
            .branch_create_without_new_version(branch_name.0)
            .c(d!())
    }

    #[inline(always)]
    unsafe fn branch_create_by_base_branch_without_new_version(
        &self,
        branch_name: BranchName,
        base_branch_name: ParentBranchName,
    ) -> Result<()> {
        self.inner
            .get_branch_id(BranchName(base_branch_name.0))
            .c(d!("base branch not found"))
            .and_then(|base_br_id| {
                self.inner
                    .branch_create_by_base_branch_without_new_version(
                        branch_name.0,
                        base_br_id,
                    )
                    .c(d!())
            })
    }

    #[inline(always)]
    unsafe fn branch_create_by_base_branch_version_without_new_version(
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
            .get_version_id(base_version_name)
            .c(d!("base vesion not found"))?;
        self.inner
            .branch_create_by_base_branch_version_without_new_version(
                branch_name.0,
                base_br_id,
                base_ver_id,
            )
            .c(d!())
    }

    #[inline(always)]
    fn branch_exists(&self, branch_name: BranchName) -> bool {
        self.inner
            .get_branch_id(branch_name)
            .map(|id| self.inner.branch_exists(id))
            .unwrap_or(false)
    }

    #[inline(always)]
    fn branch_has_versions(&self, branch_name: BranchName) -> bool {
        self.inner
            .get_branch_id(branch_name)
            .map(|id| self.inner.branch_has_versions(id))
            .unwrap_or(false)
    }

    #[inline(always)]
    fn branch_remove(&self, branch_name: BranchName) -> Result<()> {
        if let Some(branch_id) = self.inner.get_branch_id(branch_name) {
            self.inner.branch_remove(branch_id).c(d!())
        } else {
            Err(eg!("branch not found"))
        }
    }

    #[inline(always)]
    fn branch_truncate(&self, branch_name: BranchName) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|br_id| self.inner.branch_truncate(br_id).c(d!()))
    }

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
                    .get_version_id(last_version_name)
                    .c(d!("version not found"))
                    .and_then(|last_ver_id| {
                        self.inner.branch_truncate_to(br_id, last_ver_id).c(d!())
                    })
            })
    }

    #[inline(always)]
    fn branch_pop_version(&self, branch_name: BranchName) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|id| self.inner.branch_pop_version(id).c(d!()))
    }

    #[inline(always)]
    fn branch_merge_to(
        &self,
        branch_name: BranchName,
        target_branch_name: BranchName,
    ) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|brid| {
                let target_brid = self
                    .inner
                    .get_branch_id(target_branch_name)
                    .c(d!("target branch not found"))?;
                self.inner.branch_merge_to(brid, target_brid).c(d!())
            })
    }

    #[inline(always)]
    unsafe fn branch_merge_to_force(
        &self,
        branch_name: BranchName,
        target_branch_name: BranchName,
    ) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|brid| {
                let target_brid = self
                    .inner
                    .get_branch_id(target_branch_name)
                    .c(d!("target branch not found"))?;
                self.inner.branch_merge_to_force(brid, target_brid).c(d!())
            })
    }

    #[inline(always)]
    fn branch_set_default(&mut self, branch_name: BranchName) -> Result<()> {
        self.inner
            .get_branch_id(branch_name)
            .c(d!("branch not found"))
            .and_then(|brid| self.inner.branch_set_default(brid).c(d!()))
    }

    #[inline(always)]
    fn prune(&self, reserved_ver_num: Option<usize>) -> Result<()> {
        self.inner.prune(reserved_ver_num).c(d!())
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a> {
    hdr: &'a MapxRawMkVs,
    key: &'a [&'a [u8]],
    value: RawValue,
}

impl<'a> ValueMut<'a> {
    fn new(hdr: &'a MapxRawMkVs, key: &'a [&'a [u8]], value: RawValue) -> Self {
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
    key: &'a [&'a [u8]],
    hdr: &'a MapxRawMkVs,
}

impl<'a> Entry<'a> {
    pub fn or_insert_ref(self, default: &'a [u8]) -> ValueMut<'a> {
        if !self.hdr.contains_key(self.key) {
            pnk!(self.hdr.insert(self.key, default));
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}
