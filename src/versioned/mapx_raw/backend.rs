//!
//! Core logic of the version management.
//!

use crate::{
    basic::{
        mapx_ord::MapxOrd,
        mapx_ord_rawkey::{MapxOrdRawKey, MapxOrdRawKeyIter},
        mapx_raw::MapxRaw,
    },
    common::{
        compute_checksum,
        ende::{encode_optioned_bytes, KeyEnDeOrdered},
        BranchID, BranchName, RawKey, RawValue, VerChecksum, VersionID, VersionName,
        BRANCH_CNT_LIMIT, INITIAL_BRANCH_ID, INITIAL_BRANCH_NAME, NULL, VSDB,
    },
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut, RangeBounds},
};

type BranchPath = BTreeMap<BranchID, VersionID>;

// default value for reserved number when pruning branches
pub(super) const RESERVED_VERSION_NUM_DEFAULT: usize = 10;

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct MapxRawVersioned {
    default_branch: BranchID,

    branch_name_to_branch_id: MapxOrdRawKey<BranchID>,
    version_name_to_version_id: MapxOrdRawKey<VersionID>,

    // which version the branch is forked from
    branch_to_parent: MapxOrd<BranchID, Option<BasePoint>>,

    // versions directly created by this branch
    branch_to_created_versions: MapxOrd<BranchID, MapxOrd<VersionID, VerChecksum>>,

    // globally ever changed keys within each version
    version_to_change_set: MapxOrd<VersionID, MapxRaw>,

    // key -> multi-branch -> multi-version -> multi-value
    layered_kv: MapxOrdRawKey<MapxOrd<BranchID, MapxOrd<VersionID, Option<RawValue>>>>,
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

impl MapxRawVersioned {
    #[inline(always)]
    pub(super) fn new() -> Self {
        let mut ret = Self::default();
        ret.init();
        ret
    }

    #[inline(always)]
    fn init(&mut self) {
        self.default_branch = INITIAL_BRANCH_ID;
        self.branch_name_to_branch_id
            .insert_ref(&INITIAL_BRANCH_NAME.to_vec(), &INITIAL_BRANCH_ID);
        self.branch_to_parent.insert(INITIAL_BRANCH_ID, None);
        self.branch_to_created_versions
            .insert(INITIAL_BRANCH_ID, MapxOrd::new());
    }

    #[inline(always)]
    pub(super) fn insert(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<RawValue>> {
        self.insert_by_branch(key, value, self.branch_get_default())
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn insert_by_branch(
        &mut self,
        key: &[u8],
        value: &[u8],
        branch_id: BranchID,
    ) -> Result<Option<RawValue>> {
        self.branch_to_created_versions
            .get(&branch_id)
            .c(d!("branch not found"))?
            .last()
            .c(d!("no version on this branch, create a version first"))
            .and_then(|(version_id, _)| {
                self.insert_by_branch_version(key, value, branch_id, version_id)
                    .c(d!())
            })
    }

    // This function should NOT be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    #[inline(always)]
    fn insert_by_branch_version(
        &mut self,
        key: &[u8],
        value: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_branch_version(key, Some(value), branch_id, version_id)
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn remove(&mut self, key: &[u8]) -> Result<Option<RawValue>> {
        self.remove_by_branch(key, self.branch_get_default())
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn remove_by_branch(
        &mut self,
        key: &[u8],
        branch_id: BranchID,
    ) -> Result<Option<RawValue>> {
        self.branch_to_created_versions
            .get(&branch_id)
            .c(d!("branch not found"))?
            .last()
            .c(d!("no version on this branch, create a version first"))
            .and_then(|(version_id, _)| {
                self.remove_by_branch_version(key, branch_id, version_id)
                    .c(d!())
            })
    }

    // This function should NOT be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    //
    // The `remove` is essentially aschecksumning a `None` value to the key.
    fn remove_by_branch_version(
        &mut self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_branch_version(key, None, branch_id, version_id)
            .c(d!())
    }

    // This function should NOT be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    fn write_by_branch_version(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        let ret = self.get_by_branch_version(key, branch_id, version_id);

        // remove a non-existing value
        if value.is_none() && ret.is_none() {
            return Ok(None);
        }

        self.version_to_change_set
            .get_mut(&version_id)
            .c(d!("BUG: version not found"))?
            .insert(key, &[]);

        self.layered_kv
            .entry_ref(key)
            .or_insert_ref(&MapxOrd::new())
            .entry(branch_id)
            .or_insert(MapxOrd::new())
            .insert_ref_encoded_value(&version_id, &encode_optioned_bytes(&value)[..]);

        // value changed, then re-calculate checksum
        if ret.as_deref() != value {
            let mut vers = self
                .branch_to_created_versions
                .get(&branch_id)
                .c(d!("BUG: branch not found"))?;
            let mut checksum =
                vers.get_mut(&version_id).c(d!("BUG: version not found"))?;
            *checksum =
                compute_checksum(&[&checksum[..], key, value.unwrap_or_default()]);
        }

        Ok(ret)
    }

    #[inline(always)]
    pub(super) fn get(&self, key: &[u8]) -> Option<RawValue> {
        self.get_by_branch(key, self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn get_by_branch(
        &self,
        key: &[u8],
        branch_id: BranchID,
    ) -> Option<RawValue> {
        if let Some(vers) = self.branch_to_created_versions.get(&branch_id) {
            if let Some(version_id) = vers.last().map(|(id, _)| id).or_else(|| {
                self.branch_to_parent
                    .get(&branch_id)
                    .unwrap()
                    .map(|bi| bi.version_id)
            }) {
                return self.get_by_branch_version(key, branch_id, version_id);
            }
        }
        None
    }

    pub(super) fn get_by_branch_version(
        &self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Option<RawValue> {
        let fp = self.branch_get_full_path(branch_id);

        if !Self::version_id_is_in_bounds(&fp, version_id) {
            return None;
        }

        if let Some(brs) = self.layered_kv.get(key) {
            // they are all monotonically increasing
            for (br, ver) in fp.iter().rev() {
                if let Some(vers) = brs.get(br) {
                    if let Some((_, v)) = vers.get_le(&min!(*ver, version_id)) {
                        return v;
                    };
                }
            }
        }

        None
    }

    #[inline(always)]
    pub(super) fn get_mut(&mut self, key: &[u8]) -> Option<ValueMut<'_>> {
        self.get_mut_by_branch(key, self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn get_mut_by_branch(
        &mut self,
        key: &[u8],
        branch_id: BranchID,
    ) -> Option<ValueMut<'_>> {
        self.branch_to_created_versions
            .get(&branch_id)?
            .last()
            .and_then(|(version_id, _)| {
                self.get_mut_by_branch_version(key, branch_id, version_id)
            })
    }

    // This function should NOT be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    fn get_mut_by_branch_version(
        &mut self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Option<ValueMut<'_>> {
        self.get_by_branch_version(key, branch_id, version_id)
            .map(|v| ValueMut::new(self, RawKey::from_slice(key).unwrap(), v, branch_id))
    }

    #[inline(always)]
    pub(super) fn get_ge(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range_ref(key..).next()
    }

    #[inline(always)]
    pub(super) fn get_ge_by_branch(
        &self,
        key: &[u8],
        branch_id: BranchID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_ref_by_branch(branch_id, key..).next()
    }

    #[inline(always)]
    pub(super) fn get_ge_by_branch_version(
        &self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_ref_by_branch_version(branch_id, version_id, key..)
            .next()
    }

    #[inline(always)]
    pub(super) fn get_le(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range_ref(..=key).next_back()
    }

    #[inline(always)]
    pub(super) fn get_le_by_branch(
        &self,
        key: &[u8],
        branch_id: BranchID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_ref_by_branch(branch_id, ..=key).next_back()
    }

    #[inline(always)]
    pub(super) fn get_le_by_branch_version(
        &self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_ref_by_branch_version(branch_id, version_id, ..=key)
            .next_back()
    }

    #[inline(always)]
    pub(super) fn iter(&self) -> MapxRawVersionedIter {
        self.iter_by_branch(self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn iter_by_branch(&self, branch_id: BranchID) -> MapxRawVersionedIter {
        if let Some(vers) = self.branch_to_created_versions.get(&branch_id) {
            if let Some((version_id, _)) = vers.last() {
                return self.iter_by_branch_version(branch_id, version_id);
            }
        }

        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            branch_id: NULL,
            version_id: NULL,
        }
    }

    #[inline(always)]
    pub(super) fn iter_by_branch_version(
        &self,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> MapxRawVersionedIter {
        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            branch_id,
            version_id,
        }
    }

    #[inline(always)]
    pub(super) fn range<'a, R: 'a + RangeBounds<RawKey>>(
        &'a self,
        bounds: R,
    ) -> MapxRawVersionedIter<'a> {
        self.range_by_branch(self.branch_get_default(), bounds)
    }

    #[inline(always)]
    pub(super) fn range_by_branch<'a, R: 'a + RangeBounds<RawKey>>(
        &'a self,
        branch_id: BranchID,
        bounds: R,
    ) -> MapxRawVersionedIter<'a> {
        if let Some(vers) = self.branch_to_created_versions.get(&branch_id) {
            if let Some((version_id, _)) = vers.last() {
                return self.range_by_branch_version(branch_id, version_id, bounds);
            }
        }

        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            branch_id: NULL,
            version_id: NULL,
        }
    }

    #[inline(always)]
    pub(super) fn range_by_branch_version<'a, R: 'a + RangeBounds<RawKey>>(
        &'a self,
        branch_id: BranchID,
        version_id: VersionID,
        bounds: R,
    ) -> MapxRawVersionedIter<'a> {
        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.range(bounds),
            branch_id,
            version_id,
        }
    }

    #[inline(always)]
    pub(super) fn range_ref<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        bounds: R,
    ) -> MapxRawVersionedIter<'a> {
        self.range_ref_by_branch(self.branch_get_default(), bounds)
    }

    #[inline(always)]
    pub(super) fn range_ref_by_branch<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        branch_id: BranchID,
        bounds: R,
    ) -> MapxRawVersionedIter<'a> {
        if let Some(vers) = self.branch_to_created_versions.get(&branch_id) {
            if let Some((version_id, _)) = vers.last() {
                return self.range_ref_by_branch_version(branch_id, version_id, bounds);
            }
        }

        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            branch_id: NULL,
            version_id: NULL,
        }
    }

    #[inline(always)]
    pub(super) fn range_ref_by_branch_version<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        branch_id: BranchID,
        version_id: VersionID,
        bounds: R,
    ) -> MapxRawVersionedIter<'a> {
        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.range_ref(bounds),
            branch_id,
            version_id,
        }
    }

    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        self.iter().count()
    }

    #[inline(always)]
    pub(super) fn len_by_branch(&self, branch_id: BranchID) -> usize {
        self.iter_by_branch(branch_id).count()
    }

    #[inline(always)]
    pub(super) fn len_by_branch_version(
        &self,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> usize {
        self.iter_by_branch_version(branch_id, version_id).count()
    }

    // Clear all data, mainly for testing purpose.
    #[inline(always)]
    pub(super) fn clear(&mut self) {
        self.branch_name_to_branch_id.clear();
        self.version_name_to_version_id.clear();
        self.branch_to_parent.clear();
        self.branch_to_created_versions.clear();
        self.version_to_change_set.clear();
        self.layered_kv.clear();

        self.init();
    }

    #[inline(always)]
    pub(super) fn version_create(&mut self, version_name: &[u8]) -> Result<()> {
        self.version_create_by_branch(version_name, self.branch_get_default())
            .c(d!())
    }

    pub(super) fn version_create_by_branch(
        &mut self,
        version_name: &[u8],
        branch_id: BranchID,
    ) -> Result<()> {
        let mut vername = branch_id.to_be_bytes().to_vec();
        vername.extend_from_slice(version_name);

        if self.version_name_to_version_id.get(&vername).is_some() {
            return Err(eg!("version already exists"));
        }

        let mut vers = self
            .branch_to_created_versions
            .get_mut(&branch_id)
            .c(d!("branch not found"))?;

        // hash(<previous checksum> + <version name> + <every kv writes>)
        let new_checksum = compute_checksum(&[
            &vers.last().map(|(_, s)| s).unwrap_or_default(),
            &vername,
        ]);

        let version_id = VSDB.alloc_version_id();
        vers.insert(version_id, new_checksum);

        self.version_name_to_version_id
            .insert(vername.into_boxed_slice(), version_id);
        self.version_to_change_set
            .insert(version_id, MapxRaw::new());

        Ok(())
    }

    // Check if a verison exists on the initial branch
    #[inline(always)]
    pub(super) fn version_exists(&self, version_id: BranchID) -> bool {
        self.version_exists_on_branch(version_id, self.branch_get_default())
    }

    // Check if a version exists on a specified branch(include its parents)
    #[inline(always)]
    pub(super) fn version_exists_on_branch(
        &self,
        version_id: VersionID,
        branch_id: BranchID,
    ) -> bool {
        let fp = self.branch_get_full_path(branch_id);

        if !Self::version_id_is_in_bounds(&fp, version_id) {
            return false;
        }

        for (br, ver) in fp.iter().rev() {
            if self
                .branch_to_created_versions
                .get(br)
                .unwrap()
                .get_le(&min!(*ver, version_id))
                .is_some()
            {
                return true;
            }
        }

        false
    }

    // Check if a version is directly created on a specified branch(exclude its parents)
    #[inline(always)]
    pub(super) fn version_created_on_branch(
        &self,
        version_id: VersionID,
        branch_id: BranchID,
    ) -> bool {
        self.branch_to_created_versions
            .get(&branch_id)
            .map(|vers| vers.get(&version_id))
            .flatten()
            .is_some()
    }

    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn version_pop(&mut self) -> Result<()> {
        self.version_pop_by_branch(self.branch_get_default())
            .c(d!())
    }

    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn version_pop_by_branch(&mut self, branch_id: BranchID) -> Result<()> {
        if let Some((version_id, _)) = self
            .branch_to_created_versions
            .get(&branch_id)
            .c(d!("branch not found"))?
            .iter()
            .last()
        {
            self.version_remove_by_branch(version_id, branch_id).c(d!())
        } else {
            Ok(())
        }
    }

    // This function should NOT be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    fn version_remove_by_branch(
        &mut self,
        version_id: VersionID,
        branch_id: BranchID,
    ) -> Result<()> {
        if self
            .branch_to_created_versions
            .get(&branch_id)
            .c(d!("branch not found"))?
            .remove(&version_id)
            .is_none()
        {
            return Err(eg!("version is not created by this branch"));
        }

        for (key, _) in self
            .version_to_change_set
            .get(&version_id)
            .c(d!("BUG: change set not found"))?
            .iter()
        {
            let mut local_brs = self.layered_kv.get(&key).unwrap();
            let mut local_vers = local_brs.get(&branch_id).unwrap();
            local_vers.remove(&version_id);
            if local_vers.is_empty() {
                local_brs.remove(&branch_id);
            }
        }
        self.version_to_change_set.remove(&version_id);

        let version_name = self
            .version_name_to_version_id
            .iter()
            .find(|(_, id)| *id == version_id)
            .map(|(name, _)| name)
            .unwrap();
        self.version_name_to_version_id
            .remove(&version_name)
            .unwrap();

        Ok(())
    }

    // Check if the given version is bigger than the biggest existing version
    fn version_id_is_in_bounds(fp: &BranchPath, version_id: VersionID) -> bool {
        if let Some(max_version_id) = fp.values().last() {
            // querying future versions
            if *max_version_id < version_id {
                return false;
            }
        } else {
            // branch does not exist
            return false;
        }
        true
    }

    #[inline(always)]
    pub(super) fn branch_create(&mut self, branch_name: &[u8]) -> Result<()> {
        self.branch_create_by_base_branch(branch_name, self.branch_get_default())
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn branch_create_by_base_branch(
        &mut self,
        branch_name: &[u8],
        base_branch_id: BranchID,
    ) -> Result<()> {
        let base_version_id = self
            .branch_to_created_versions
            .get(&base_branch_id)
            .c(d!("base branch not found"))?
            .last()
            .map(|(version_id, _)| version_id)
            .c(d!("base version not found"))?;

        self.branch_create_by_base_branch_version(
            branch_name,
            base_branch_id,
            base_version_id,
        )
        .c(d!())
    }

    fn branch_create_by_base_branch_version(
        &mut self,
        branch_name: &[u8],
        base_branch_id: BranchID,
        base_version_id: VersionID,
    ) -> Result<()> {
        if (BRANCH_CNT_LIMIT - 1) < self.branch_to_parent.len() {
            return Err(eg!("too many branches"));
        }

        if self.branch_name_to_branch_id.contains_key(branch_name) {
            return Err(eg!("branch already exists"));
        }

        if !self.version_exists_on_branch(base_version_id, base_branch_id) {
            return Err(eg!("BUG: version is not on branch"));
        }

        let branch_id = VSDB.alloc_branch_id();

        self.branch_name_to_branch_id
            .insert(branch_name.to_owned().into_boxed_slice(), branch_id);

        // All new branches will have a base point,
        // the only exception is the initial branch created by system
        self.branch_to_parent.insert(
            branch_id,
            Some(BasePoint {
                branch_id: base_branch_id,
                version_id: base_version_id,
            }),
        );

        self.branch_to_created_versions
            .insert(branch_id, MapxOrd::new());

        Ok(())
    }

    // Check if a branch exists or not
    #[inline(always)]
    pub(super) fn branch_exists(&self, branch_id: BranchID) -> bool {
        self.branch_to_parent.contains_key(&branch_id)
    }

    // Remove all changes directly made by this branch, and delete the branch itself.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn branch_remove(&mut self, branch_id: BranchID) -> Result<()> {
        if self.branch_has_children(branch_id) {
            return Err(eg!("can not remove branches with children"));
        }

        if self.branch_get_default() == branch_id {
            return Err(eg!("the default branch can NOT be removed"));
        }

        self.branch_truncate(branch_id).c(d!())?;

        self.branch_to_parent.remove(&branch_id);

        let branch_name = self
            .branch_name_to_branch_id
            .iter()
            .find(|(_, id)| *id == branch_id)
            .map(|(name, _)| name)
            .c(d!("BUG: branch name not found"))?;
        self.branch_name_to_branch_id.remove(&branch_name);

        let mut created_vers = self
            .branch_to_created_versions
            .remove(&branch_id)
            .c(d!("BUG: created versions missing"))?;
        for (ver, _) in created_vers.iter() {
            created_vers.remove(&ver);
        }

        Ok(())
    }

    // Remove all changes directly made by this branch, but keep its meta infomation.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn branch_truncate(&mut self, branch_id: BranchID) -> Result<()> {
        self.branch_truncate_to(branch_id, VersionID::MIN).c(d!())
    }

    // Remove all changes directly made by versions(bigger than `last_version_id`) of this branch.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    pub(super) fn branch_truncate_to(
        &mut self,
        branch_id: BranchID,
        last_version_id: VersionID,
    ) -> Result<()> {
        if let Some(vers) = self.branch_to_created_versions.get(&branch_id) {
            // version id must be in descending order
            for (version_id, _) in vers.range((1 + last_version_id)..).rev() {
                self.version_remove_by_branch(version_id, branch_id)
                    .c(d!())?;
            }
            Ok(())
        } else {
            Err(eg!("branch not found"))
        }
    }

    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn branch_pop_version(&mut self, branch_id: BranchID) -> Result<()> {
        self.version_pop_by_branch(branch_id).c(d!())
    }

    // Merge a branch back to its parent branch
    pub(super) fn branch_merge_to_parent(&mut self, branch_id: BranchID) -> Result<()> {
        if self.branch_has_children(branch_id) {
            return Err(eg!("can not merge branches with children"));
        }

        let fp = self.branch_get_recurive_path(branch_id, 2);

        if fp.is_empty() {
            return Err(eg!("branch not found"));
        } else if branch_id != *fp.keys().rev().next().unwrap() || 1 == fp.len() {
            // no new versions yet, nothing to merge
            return Ok(());
        }

        let parent_branch_id = fp.keys().rev().find(|&id| *id != branch_id).unwrap();

        let mut vers_created =
            self.branch_to_created_versions.remove(&branch_id).unwrap();

        let mut vers_created_parent = self
            .branch_to_created_versions
            .get_mut(parent_branch_id)
            .unwrap();

        // // merge an empty branch,
        // // this check is not necessary because this scene has been checked
        // if vers_created.is_empty() {
        //     return Ok(());
        // }

        // used to calculate new checksum
        let (last_ver, last_checksum) = vers_created.last().unwrap();

        // used to calculate new checksum
        let (last_ver_parent, last_checksum_parent) =
            vers_created_parent.last().unwrap();

        for (ver, checksum) in vers_created.iter() {
            vers_created_parent.insert(ver, checksum);

            // `unwrap`s here should be safe
            for k in self
                .version_to_change_set
                .get(&ver)
                .unwrap()
                .iter()
                .map(|(k, _)| k)
            {
                let mut key_hdr = self.layered_kv.get_mut(&k).unwrap();

                let (value, empty) = {
                    let mut br_hdr = key_hdr.get_mut(&branch_id).unwrap();
                    let v = br_hdr.remove(&ver).unwrap();
                    (v, br_hdr.is_empty())
                };

                if empty {
                    key_hdr.remove(&branch_id);
                }

                key_hdr
                    .entry(*parent_branch_id)
                    .or_insert(MapxOrd::new())
                    .insert(ver, value);
            }
        }

        // re-calcute checksum, the old parent checksum should be set in the first place
        let new_checksum = compute_checksum(&[&last_checksum_parent, &last_checksum]);
        vers_created_parent
            .insert(max!(last_ver_parent, last_ver), new_checksum)
            .unwrap();

        // remove outdated values
        vers_created.iter().for_each(|(k, _)| {
            vers_created.remove(&k);
        });

        // remove user-registered infomation
        let (br_name, _) = self
            .branch_name_to_branch_id
            .iter()
            .find(|(_, br)| *br == branch_id)
            .unwrap();
        self.branch_name_to_branch_id.remove(&br_name);

        self.branch_to_parent.remove(&branch_id);

        // change the prefix of version names to the id of parent branch
        let brbytes = branch_id.to_be_bytes();
        let parent_brbytes = parent_branch_id.to_be_bytes();
        self.version_name_to_version_id
            .iter()
            .for_each(|(vername, verid)| {
                if vername.starts_with(&brbytes) {
                    let mut new_vername = vername.to_vec();
                    new_vername[..brbytes.len()].copy_from_slice(&parent_brbytes);
                    self.version_name_to_version_id.remove(&vername).unwrap();
                    self.version_name_to_version_id
                        .insert(new_vername.into_boxed_slice(), verid);
                }
            });

        Ok(())
    }

    pub(super) fn branch_has_children(&self, branch_id: BranchID) -> bool {
        self.branch_to_parent
            .iter()
            .filter_map(|(_, p)| p)
            .any(|p| p.branch_id == branch_id)
    }

    // Get itself and all its ancestral branches with the base point it born on.
    #[inline(always)]
    fn branch_get_full_path(&self, branch_id: BranchID) -> BranchPath {
        self.branch_get_recurive_path(branch_id, BRANCH_CNT_LIMIT)
    }

    fn branch_get_recurive_path(
        &self,
        mut branch_id: BranchID,
        mut depth_limit: usize,
    ) -> BranchPath {
        let mut ret = BTreeMap::new();

        // Both 'branch id' and 'version id'
        // are globally monotonically increasing.
        if let Some(vers) = self.branch_to_created_versions.get(&branch_id) {
            if let Some(version_id) = vers.last().map(|(id, _)| id) {
                ret.insert(branch_id, version_id);
                depth_limit = depth_limit.saturating_sub(1);
            }
            loop {
                if 0 == depth_limit {
                    break;
                }
                if let Some(Some(bp)) = self.branch_to_parent.get(&branch_id) {
                    depth_limit -= 1;
                    ret.insert(bp.branch_id, bp.version_id);
                    branch_id = bp.branch_id;
                } else {
                    break;
                }
            }
        }
        ret
    }

    #[inline(always)]
    pub(super) fn branch_set_default(&mut self, branch_id: BranchID) -> Result<()> {
        if !self.branch_to_parent.contains_key(&branch_id) {
            return Err(eg!("branch not found"));
        }
        self.default_branch = branch_id;
        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_get_default(&self) -> BranchID {
        self.default_branch
    }

    #[inline(always)]
    pub(super) fn checksum_get(&self) -> Option<VerChecksum> {
        self.checksum_get_by_branch(self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn checksum_get_by_branch(
        &self,
        branch_id: BranchID,
    ) -> Option<VerChecksum> {
        self.checksum_get_by_branch_version(branch_id, None)
    }

    pub(super) fn checksum_get_by_branch_version(
        &self,
        branch_id: BranchID,
        version_id: Option<VersionID>,
    ) -> Option<VerChecksum> {
        let fp = self.branch_get_full_path(branch_id);

        let version_id = if let Some(id) = version_id {
            if !Self::version_id_is_in_bounds(&fp, id) {
                return None;
            }
            id
        } else {
            VersionID::MAX
        };

        for (br, ver) in fp.iter().rev() {
            if let Some((_, checksum)) = self
                .branch_to_created_versions
                .get(br)
                .unwrap()
                .get_le(&min!(*ver, version_id))
            {
                return Some(checksum);
            }
        }

        None
    }

    #[inline(always)]
    pub(super) fn prune(&mut self, reserved_ver_num: Option<usize>) -> Result<()> {
        self.prune_by_branch(self.branch_get_default(), reserved_ver_num)
    }

    pub(super) fn prune_by_branch(
        &mut self,
        branch_id: BranchID,
        reserved_ver_num: Option<usize>,
    ) -> Result<()> {
        let reserved_ver_num = reserved_ver_num.unwrap_or(RESERVED_VERSION_NUM_DEFAULT);
        if 0 == reserved_ver_num {
            return Err(eg!("reserved version number should NOT be zero"));
        }

        let mut created_vers = self
            .branch_to_created_versions
            .get_mut(&branch_id)
            .c(d!("branch not found"))?;

        // need not to prune
        if created_vers.len() <= reserved_ver_num {
            return Ok(());
        }

        let guard_ver_id = created_vers
            .iter()
            .rev()
            .nth(reserved_ver_num - 1)
            .map(|(ver, _)| ver)
            .unwrap();

        for (key, _) in self
            .layered_kv
            .iter()
            .filter(|(_, brs)| brs.contains_key(&branch_id))
        {
            let mut key_hdr = self.layered_kv.get_mut(&key).unwrap();
            let mut br_hdr = key_hdr.get_mut(&branch_id).unwrap();

            // keep one version at least
            for (ver, _) in br_hdr
                .iter()
                .rev()
                .skip(1)
                .filter(|(ver, _)| *ver < guard_ver_id)
            {
                br_hdr.remove(&ver);
            }
        }

        for (ver, _) in created_vers.iter().rev().skip(reserved_ver_num) {
            created_vers.remove(&ver);
            self.version_to_change_set.remove(&ver);

            // one version belong(directly) to one branch only,
            // so we can remove these created versions safely.
            let (vername, _) = self
                .version_name_to_version_id
                .iter()
                .find(|(_, v)| *v == ver)
                .unwrap();
            self.version_name_to_version_id.remove(&vername);
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn get_branch_id(&self, branch_name: BranchName) -> Option<BranchID> {
        self.branch_name_to_branch_id.get(branch_name.0)
    }

    #[inline(always)]
    pub(super) fn get_version_id(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<VersionID> {
        let mut vername = self.get_branch_id(branch_name)?.to_be_bytes().to_vec();
        vername.extend_from_slice(version_name.0);
        self.version_name_to_version_id.get(&vername)
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// used mark where a new branch are forked from
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct BasePoint {
    // parent branch ID of this branch
    branch_id: BranchID,
    // which verion of its parent branch is this branch forked from
    version_id: VersionID,
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

pub struct MapxRawVersionedIter<'a> {
    hdr: &'a MapxRawVersioned,
    iter: MapxOrdRawKeyIter<MapxOrd<BranchID, MapxOrd<VersionID, Option<RawValue>>>>,
    branch_id: BranchID,
    version_id: VersionID,
}

impl<'a> Iterator for MapxRawVersionedIter<'a> {
    type Item = (RawKey, RawValue);

    #[allow(clippy::while_let_on_iterator)]
    fn next(&mut self) -> Option<Self::Item> {
        if NULL == self.branch_id || NULL == self.version_id {
            return None;
        }

        while let Some((k, _)) = self.iter.next() {
            if let Some(v) =
                self.hdr
                    .get_by_branch_version(&k, self.branch_id, self.version_id)
            {
                return Some((k, v));
            }
        }

        None
    }
}

impl DoubleEndedIterator for MapxRawVersionedIter<'_> {
    #[allow(clippy::while_let_on_iterator)]
    fn next_back(&mut self) -> Option<Self::Item> {
        if NULL == self.branch_id || NULL == self.version_id {
            return None;
        }

        while let Some((k, _)) = self.iter.next_back() {
            if let Some(v) =
                self.hdr
                    .get_by_branch_version(&k, self.branch_id, self.version_id)
            {
                return Some((k, v));
            }
        }

        None
    }
}

impl ExactSizeIterator for MapxRawVersionedIter<'_> {}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[allow(missing_docs)]
#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a> {
    hdr: &'a mut MapxRawVersioned,
    key: RawKey,
    value: RawValue,
    branch_id: BranchID,
}

impl<'a> ValueMut<'a> {
    fn new(
        hdr: &'a mut MapxRawVersioned,
        key: RawKey,
        value: RawValue,
        branch_id: BranchID,
    ) -> Self {
        ValueMut {
            hdr,
            key,
            value,
            branch_id,
        }
    }
}

// NOTE: Very Important !!!
impl<'a> Drop for ValueMut<'a> {
    fn drop(&mut self) {
        pnk!(
            self.hdr
                .insert_by_branch(&self.key, &self.value, self.branch_id)
        );
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

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
