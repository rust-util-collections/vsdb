//!
//! Core logic of the multi-key version managements.
//!

use crate::{
    basic::{mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey},
    basic_multi_key::{mapx_raw::MapxRawMk, mapx_rawkey::MapxMkRawKey},
    common::{
        ende::encode_optioned_bytes, BranchID, BranchName, RawValue, VersionID,
        VersionName, BRANCH_ANCESTORS_LIMIT, INITIAL_BRANCH_ID, INITIAL_BRANCH_NAME,
        INITIAL_VERSION, RESERVED_VERSION_NUM_DEFAULT, VSDB,
    },
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

type BranchPath = BTreeMap<BranchID, VersionID>;

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct MapxRawMkVs {
    default_branch: BranchID,
    key_size: usize,

    branch_name_to_branch_id: MapxOrdRawKey<BranchID>,
    version_name_to_version_id: MapxOrdRawKey<VersionID>,

    // which version the branch is forked from
    branch_to_parent: MapxOrd<BranchID, Option<BasePoint>>,

    // versions directly created by this branch
    branch_to_created_versions: MapxOrd<BranchID, MapxOrd<VersionID, ()>>,

    // globally ever changed keys within each version
    version_to_change_set: MapxOrd<VersionID, MapxRawMk>,

    // key -> multi-branch -> multi-version -> multi-value
    layered_kv: MapxMkRawKey<MapxOrd<BranchID, MapxOrd<VersionID, Option<RawValue>>>>,
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

impl MapxRawMkVs {
    #[inline(always)]
    pub(super) fn new(key_size: usize) -> Self {
        let mut ret = Self {
            default_branch: BranchID::default(),
            key_size,
            branch_name_to_branch_id: MapxOrdRawKey::new(),
            version_name_to_version_id: MapxOrdRawKey::new(),
            branch_to_parent: MapxOrd::new(),
            branch_to_created_versions: MapxOrd::new(),
            version_to_change_set: MapxOrd::new(),
            layered_kv: MapxMkRawKey::new(key_size),
        };
        ret.init();
        ret
    }

    #[inline(always)]
    fn init(&mut self) {
        self.default_branch = INITIAL_BRANCH_ID;
        self.branch_name_to_branch_id
            .insert_ref(INITIAL_BRANCH_NAME.0, &INITIAL_BRANCH_ID);
        self.branch_to_parent.insert(INITIAL_BRANCH_ID, None);
        self.branch_to_created_versions
            .insert(INITIAL_BRANCH_ID, MapxOrd::new());
        self.version_create(INITIAL_VERSION.0).unwrap();
    }

    #[inline(always)]
    pub(super) fn insert(
        &self,
        key: &[&[u8]],
        value: &[u8],
    ) -> Result<Option<RawValue>> {
        self.insert_by_branch(key, value, self.branch_get_default())
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn insert_by_branch(
        &self,
        key: &[&[u8]],
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
        &self,
        key: &[&[u8]],
        value: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_branch_version(key, Some(value), branch_id, version_id)
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn remove(&self, key: &[&[u8]]) -> Result<Option<RawValue>> {
        self.remove_by_branch(key, self.branch_get_default())
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn remove_by_branch(
        &self,
        key: &[&[u8]],
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
    // The `remove` is essentially assign a `None` value to the key.
    fn remove_by_branch_version(
        &self,
        key: &[&[u8]],
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
        &self,
        key: &[&[u8]],
        value: Option<&[u8]>,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        if key.len() < self.key_size {
            return self
                .batch_remove_by_branch_version(key, value, branch_id, version_id)
                .c(d!());
        };

        let ret = self.get_by_branch_version(key, branch_id, version_id);

        // remove a non-existing value
        if value.is_none() && ret.is_none() {
            return Ok(None);
        }

        self.version_to_change_set
            .get_mut(&version_id)
            .c(d!("BUG: version not found"))?
            .insert(key, &[])
            .c(d!("BUG: fatal !!"))?;

        self.layered_kv
            .entry_ref(key)
            .or_insert_ref(&MapxOrd::new())
            .c(d!())?
            .entry(branch_id)
            .or_insert(MapxOrd::new())
            .insert_ref_encoded_value(&version_id, &encode_optioned_bytes(&value)[..]);

        Ok(ret)
    }

    fn batch_remove_by_branch_version(
        &self,
        key: &[&[u8]],
        value: Option<&[u8]>,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        let hdr = self
            .version_to_change_set
            .get_mut(&version_id)
            .c(d!("BUG: version not found"))?;
        let mut op = |k: &[&[u8]], _: &[u8]| hdr.insert(k, &[]).c(d!()).map(|_| ());
        hdr.iter_op_with_key_prefix(&mut op, key)
            .c(d!("BUG: fatal !!"))?;

        let mut op =
            |k: &[&[u8]],
             _: &MapxOrd<BranchID, MapxOrd<VersionID, Option<RawValue>>>| {
                self.layered_kv
                    .entry_ref(k)
                    .or_insert_ref(&MapxOrd::new())
                    .c(d!())?
                    .entry(branch_id)
                    .or_insert(MapxOrd::new())
                    .insert_ref_encoded_value(
                        &version_id,
                        &encode_optioned_bytes(&value)[..],
                    );
                Ok(())
            };
        self.layered_kv
            .iter_op_with_key_prefix(&mut op, key)
            .c(d!())?;

        Ok(None)
    }

    #[inline(always)]
    pub(super) fn get(&self, key: &[&[u8]]) -> Option<RawValue> {
        self.get_by_branch(key, self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn get_by_branch(
        &self,
        key: &[&[u8]],
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
        key: &[&[u8]],
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
    pub(super) fn version_create(&self, version_name: &[u8]) -> Result<()> {
        self.version_create_by_branch(version_name, self.branch_get_default())
            .c(d!())
    }

    pub(super) fn version_create_by_branch(
        &self,
        version_name: &[u8],
        branch_id: BranchID,
    ) -> Result<()> {
        let mut vername = branch_id.to_be_bytes().to_vec();
        vername.extend_from_slice(version_name);

        if self.version_name_to_version_id.get(&vername).is_some() {
            return Err(eg!("version already exists"));
        }

        let vers = self
            .branch_to_created_versions
            .get_mut(&branch_id)
            .c(d!("branch not found"))?;

        let version_id = VSDB.alloc_version_id();
        vers.insert(version_id, ());

        self.version_name_to_version_id
            .insert(vername.into_boxed_slice(), version_id);
        self.version_to_change_set
            .insert(version_id, MapxRawMk::new(self.key_size));

        Ok(())
    }

    // Check if a verison exists on the initial branch
    #[inline(always)]
    pub(super) fn version_exists(&self, version_id: BranchID) -> bool {
        self.version_exists_on_branch(version_id, self.branch_get_default())
            .0
    }

    // Check if a version exists on a specified branch(include its parents)
    #[inline(always)]
    pub(super) fn version_exists_on_branch(
        &self,
        version_id: VersionID,
        branch_id: BranchID,
    ) -> (bool, BranchPath) {
        let fp = self.branch_get_full_path(branch_id);

        if !Self::version_id_is_in_bounds(&fp, version_id) {
            return (false, fp);
        }

        for (br, ver) in fp.iter().rev() {
            if self
                .branch_to_created_versions
                .get(br)
                .unwrap()
                .get_le(&min!(*ver, version_id))
                .is_some()
            {
                return (true, fp);
            }
        }

        (false, fp)
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
            .and_then(|vers| vers.get(&version_id))
            .is_some()
    }

    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn version_pop(&self) -> Result<()> {
        self.version_pop_by_branch(self.branch_get_default())
            .c(d!())
    }

    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn version_pop_by_branch(&self, branch_id: BranchID) -> Result<()> {
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
        &self,
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

        let mut change_set_ops = |key: &[&[u8]], _: &[u8]| {
            let local_brs = self.layered_kv.get(key).unwrap();
            let local_vers = local_brs.get(&branch_id).unwrap();
            local_vers.remove(&version_id);
            if local_vers.is_empty() {
                local_brs.remove(&branch_id);
            }
            Ok(())
        };

        self.version_to_change_set
            .get(&version_id)
            .c(d!("BUG: change set not found"))?
            .iter_op(&mut change_set_ops)
            .c(d!())?;

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
    pub(super) fn branch_create(&self, branch_name: &[u8]) -> Result<()> {
        self.branch_create_by_base_branch(branch_name, self.branch_get_default())
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn branch_create_by_base_branch(
        &self,
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

    pub(super) fn branch_create_by_base_branch_version(
        &self,
        branch_name: &[u8],
        base_branch_id: BranchID,
        base_version_id: VersionID,
    ) -> Result<()> {
        if self.branch_name_to_branch_id.contains_key(branch_name) {
            return Err(eg!("branch already exists"));
        }

        let (exist, fp) = self.version_exists_on_branch(base_version_id, base_branch_id);
        if !exist {
            return Err(eg!("version is not on the base branch"));
        }
        if BRANCH_ANCESTORS_LIMIT < fp.len() {
            return Err(eg!("the base branch has too many ancestors"));
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

    // Check if a branch exists or not.
    #[inline(always)]
    pub(super) fn branch_exists(&self, branch_id: BranchID) -> bool {
        self.branch_to_parent.contains_key(&branch_id)
    }

    // Check if a branch exists and has versions on it.
    #[inline(always)]
    pub(super) fn branch_has_versions(&self, branch_id: BranchID) -> bool {
        self.branch_exists(branch_id) || !self.version_name_to_version_id.is_empty()
    }

    // Remove all changes directly made by this branch, and delete the branch itself.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn branch_remove(&self, branch_id: BranchID) -> Result<()> {
        // if self.branch_get_default() == branch_id {
        //     return Err(eg!("the default branch can NOT be removed"));
        // }

        if self.branch_has_children(branch_id) {
            return Err(eg!("can not remove branches with children"));
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

        let created_vers = self
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
    pub(super) fn branch_truncate(&self, branch_id: BranchID) -> Result<()> {
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
        &self,
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
    pub(super) fn branch_pop_version(&self, branch_id: BranchID) -> Result<()> {
        self.version_pop_by_branch(branch_id).c(d!())
    }

    // Merge a branch back to its parent branch
    pub(super) fn branch_merge_to_parent(&self, branch_id: BranchID) -> Result<()> {
        // if self.branch_get_default() == branch_id {
        //     return Err(eg!("the default branch can NOT be merged"));
        // }

        if self.branch_has_children(branch_id) {
            return Err(eg!("can not merge branches with children"));
        }

        let fp = self.branch_get_recurive_path(branch_id, 2);

        if fp.is_empty() {
            return Err(eg!("branch not found"));
        } else if 1 == fp.len() {
            // no ancestors, aka try to merge the root branch to sky!
            return Err(eg!("parent branch not found"));
        } else if branch_id != *fp.keys().rev().next().unwrap() {
            // no new versions on the target branch, delete it
            return self.branch_remove(branch_id).c(d!());
        }

        let parent_branch_id = fp.keys().rev().find(|&id| *id != branch_id).unwrap();

        let vers_created = self.branch_to_created_versions.remove(&branch_id).unwrap();

        let vers_created_parent = self
            .branch_to_created_versions
            .get_mut(parent_branch_id)
            .unwrap();

        // // merge an empty branch,
        // // this check is not necessary because this scene has been checked
        // if vers_created.is_empty() {
        //     return Ok(());
        // }

        for (ver, _) in vers_created.iter() {
            vers_created_parent.insert(ver, ());

            let mut change_set_ops = |key: &[&[u8]], _: &[u8]| {
                let key_hdr = self.layered_kv.get_mut(key).unwrap();
                let (value, empty) = {
                    let br_hdr = key_hdr.get_mut(&branch_id).unwrap();
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
                Ok(())
            };

            // `unwrap`s here should be safe
            self.version_to_change_set
                .get(&ver)
                .unwrap()
                .iter_op(&mut change_set_ops)
                .c(d!())?;
        }

        // remove data on the original branch
        vers_created.clear();

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

        // remove user-registered infomation
        let (br_name, _) = self
            .branch_name_to_branch_id
            .iter()
            .find(|(_, br)| *br == branch_id)
            .unwrap();
        self.branch_name_to_branch_id.remove(&br_name);

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
        self.branch_get_recurive_path(branch_id, BRANCH_ANCESTORS_LIMIT)
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
    pub(super) fn prune(&self, reserved_ver_num: Option<usize>) -> Result<()> {
        self.prune_by_branch(self.branch_get_default(), reserved_ver_num)
    }

    pub(super) fn prune_by_branch(
        &self,
        branch_id: BranchID,
        reserved_ver_num: Option<usize>,
    ) -> Result<()> {
        let reserved_ver_num = reserved_ver_num.unwrap_or(RESERVED_VERSION_NUM_DEFAULT);
        if 0 == reserved_ver_num {
            return Err(eg!("reserved version number should NOT be zero"));
        }

        let created_vers = self
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

        let mut layered_kv_ops =
            |key: &[&[u8]],
             brs: &MapxOrd<BranchID, MapxOrd<VersionID, Option<RawValue>>>| {
                if brs.contains_key(&branch_id) {
                    let key_hdr = self.layered_kv.get_mut(key).unwrap();
                    let br_hdr = key_hdr.get_mut(&branch_id).unwrap();
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
                Ok(())
            };

        self.layered_kv.iter_op(&mut layered_kv_ops).c(d!())?;

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
