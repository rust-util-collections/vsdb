//!
//! # Versioned functions
//!

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(missing_docs)]

use crate::{
    basic::mapx_oc::{MapxOC, MapxOCIter},
    common::{compute_sig, BranchID, VersionID, BIGGEST_RESERVED_ID, VSDB},
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut, RangeBounds},
};

// hash of a version
type VerSig = Vec<u8>;

type RawKey = Vec<u8>;
type RawValue = Vec<u8>;

type BranchPath = BTreeMap<BranchID, VersionID>;

const INITIAL_BRANCH_ID: BranchID = 0;
const INITIAL_BRANCH_NAME: &str = "main";

const ERROR_BRANCH_ID: BranchID = BIGGEST_RESERVED_ID;
const ERROR_VERSION_ID: VersionID = BIGGEST_RESERVED_ID;

const BRANCH_CNT_LIMIT: usize = 1024;

// default value for reserved number when pruning branches
const RESERVED_VERSION_NUM_DEFAULT: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct VerPoint {
    // parent branch ID of this branch
    branch_id: BranchID,
    // which verion of its parent branch is this branch forked from
    version_id: VersionID,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapxRawVersioned {
    branch_name_to_branch_id: MapxOC<RawKey, BranchID>,
    version_name_to_version_id: MapxOC<RawKey, VersionID>,

    branch_to_parent: MapxOC<BranchID, Option<VerPoint>>,

    // versions directly created on this branch
    branch_to_created_versions: MapxOC<BranchID, MapxOC<VersionID, VerSig>>,

    // ever changed keys within each version
    version_to_change_set: MapxOC<VersionID, MapxOC<RawKey, bool>>,

    layered_kv: MapxOC<RawKey, MapxOC<BranchID, MapxOC<VersionID, Option<RawValue>>>>,
}

impl Default for MapxRawVersioned {
    fn default() -> Self {
        Self::new()
    }
}

impl MapxRawVersioned {
    #[inline(always)]
    pub fn new() -> Self {
        let mut ret = Self::default();

        ret.branch_name_to_branch_id.insert(
            INITIAL_BRANCH_NAME.to_owned().into_bytes(),
            INITIAL_BRANCH_ID,
        );
        ret.branch_to_parent.insert(INITIAL_BRANCH_ID, None);
        ret.branch_to_created_versions
            .insert(INITIAL_BRANCH_ID, MapxOC::new());

        ret
    }

    #[inline(always)]
    pub fn insert(&mut self, key: RawKey, value: RawValue) -> Result<Option<RawValue>> {
        self.insert_by_branch(key, value, INITIAL_BRANCH_ID).c(d!())
    }

    #[inline(always)]
    pub fn insert_by_branch(
        &mut self,
        key: RawKey,
        value: RawValue,
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
        key: RawKey,
        value: RawValue,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_branch_version(key, Some(value), branch_id, version_id)
            .c(d!())
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &[u8]) -> Result<Option<RawValue>> {
        self.remove_by_branch(key, INITIAL_BRANCH_ID).c(d!())
    }

    #[inline(always)]
    pub fn remove_by_branch(
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
    // The `remove` is essentially assigning a `None` value to the key.
    fn remove_by_branch_version(
        &mut self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_branch_version(key.to_owned(), None, branch_id, version_id)
            .c(d!())
    }

    // This function should NOT be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    fn write_by_branch_version(
        &mut self,
        key: RawKey,
        value: Option<RawValue>,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        if let Some(mut changeset) = self.version_to_change_set.get_mut(&version_id) {
            changeset.insert(key.clone(), true);
        } else {
            return Err(eg!("BUG: version not found"));
        }

        if let Some(mut vers) = self.branch_to_created_versions.get(&branch_id) {
            if let Some(mut sig) = vers.get_mut(&version_id) {
                *sig = compute_sig(&[
                    sig.as_slice(),
                    key.as_slice(),
                    value.as_deref().unwrap_or_default(),
                ]);
            } else {
                return Err(eg!("BUG: version not found"));
            }
        } else {
            return Err(eg!("BUG: branch not found"));
        }

        let res = self
            .layered_kv
            .entry(key)
            .or_insert(MapxOC::new())
            .entry(branch_id)
            .or_insert(MapxOC::new())
            .insert(version_id, value)
            .flatten();

        Ok(res)
    }

    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<RawValue> {
        self.get_by_branch(key, INITIAL_BRANCH_ID)
    }

    #[inline(always)]
    pub fn get_by_branch(&self, key: &[u8], branch_id: BranchID) -> Option<RawValue> {
        if let Some(vers) = self.branch_to_created_versions.get(&branch_id) {
            if let Some((version_id, _)) = vers.last() {
                return self.get_by_branch_version(key, branch_id, version_id);
            }
        }
        None
    }

    pub fn get_by_branch_version(
        &self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Option<RawValue> {
        let branch_fp = self.branch_get_full_path(branch_id);

        if !Self::version_id_is_in_bounds(&branch_fp, version_id) {
            return None;
        }

        if let Some(brs) = self.layered_kv._get(key) {
            // they are all monotonically increasing
            for (br, ver) in branch_fp.iter().rev() {
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
    pub fn get_mut(&mut self, key: &[u8]) -> Option<ValueMut<'_>> {
        self.get_mut_by_branch(key, INITIAL_BRANCH_ID)
    }

    #[inline(always)]
    pub fn get_mut_by_branch(
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
            .map(|v| ValueMut::new(self, key.to_owned(), v, branch_id))
    }

    #[inline(always)]
    pub fn get_ge(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range(key.to_owned()..).next()
    }

    #[inline(always)]
    pub fn get_ge_by_branch(
        &self,
        key: &[u8],
        branch_id: BranchID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_by_branch(branch_id, key.to_owned()..).next()
    }

    #[inline(always)]
    pub fn get_ge_by_branch_version(
        &self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_by_branch_version(branch_id, version_id, key.to_owned()..)
            .next()
    }

    #[inline(always)]
    pub fn get_le(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range(..=key.to_owned()).next_back()
    }

    #[inline(always)]
    pub fn get_le_by_branch(
        &self,
        key: &[u8],
        branch_id: BranchID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_by_branch(branch_id, ..=key.to_owned())
            .next_back()
    }

    #[inline(always)]
    pub fn get_le_by_branch_version(
        &self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_by_branch_version(branch_id, version_id, ..=key.to_owned())
            .next_back()
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxRawVersionedIter {
        self.iter_by_branch(INITIAL_BRANCH_ID)
    }

    #[inline(always)]
    pub fn iter_by_branch(&self, branch_id: BranchID) -> MapxRawVersionedIter {
        if let Some(vers) = self.branch_to_created_versions.get(&branch_id) {
            if let Some((version_id, _)) = vers.last() {
                return self.iter_by_branch_version(branch_id, version_id);
            }
        }
        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            branch_id: ERROR_BRANCH_ID,
            version_id: ERROR_VERSION_ID,
        }
    }

    #[inline(always)]
    pub fn iter_by_branch_version(
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
    pub fn range<R: RangeBounds<RawKey>>(&self, bounds: R) -> MapxRawVersionedIter {
        self.range_by_branch(INITIAL_BRANCH_ID, bounds)
    }

    #[inline(always)]
    pub fn range_by_branch<R: RangeBounds<RawKey>>(
        &self,
        branch_id: BranchID,
        bounds: R,
    ) -> MapxRawVersionedIter {
        if let Some(vers) = self.branch_to_created_versions.get(&branch_id) {
            if let Some((version_id, _)) = vers.last() {
                return self.range_by_branch_version(branch_id, version_id, bounds);
            }
        }
        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            branch_id: ERROR_BRANCH_ID,
            version_id: ERROR_VERSION_ID,
        }
    }

    #[inline(always)]
    pub fn range_by_branch_version<R: RangeBounds<RawKey>>(
        &self,
        branch_id: BranchID,
        version_id: VersionID,
        bounds: R,
    ) -> MapxRawVersionedIter {
        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.range(bounds),
            branch_id,
            version_id,
        }
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    #[inline(always)]
    pub fn contains_key_by_branch(&self, key: &[u8], branch_id: BranchID) -> bool {
        self.get_by_branch(key, branch_id).is_some()
    }

    #[inline(always)]
    pub fn contains_key_by_branch_version(
        &self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> bool {
        self.get_by_branch_version(key, branch_id, version_id)
            .is_some()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    #[inline(always)]
    pub fn len_by_branch(&self, branch_id: BranchID) -> usize {
        self.iter_by_branch(branch_id).count()
    }

    #[inline(always)]
    pub fn len_by_branch_version(
        &self,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> usize {
        self.iter_by_branch_version(branch_id, version_id).count()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        0 == self.len()
    }

    #[inline(always)]
    pub fn is_empty_by_branch(&self, branch_id: BranchID) -> bool {
        0 == self.len_by_branch(branch_id)
    }

    #[inline(always)]
    pub fn is_empty_by_branch_version(
        &self,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> bool {
        0 == self.len_by_branch_version(branch_id, version_id)
    }

    /// Clear all data, mainly for testing purpose.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.branch_name_to_branch_id.clear();
        self.version_name_to_version_id.clear();
        self.branch_to_parent.clear();
        self.branch_to_created_versions.clear();
        self.version_to_change_set.clear();
        self.layered_kv.clear();
    }

    #[inline(always)]
    pub fn version_create(&mut self, version_name: &[u8]) -> Result<()> {
        self.version_create_by_branch(version_name, INITIAL_BRANCH_ID)
            .c(d!())
    }

    pub fn version_create_by_branch(
        &mut self,
        version_name: &[u8],
        branch_id: BranchID,
    ) -> Result<()> {
        if self.version_name_to_version_id._get(version_name).is_some() {
            return Err(eg!("version already exists"));
        }

        let version_id = VSDB.alloc_version_id();
        let version_id_bytes = version_id.to_be_bytes();

        if let Some(mut vers) = self.branch_to_created_versions.get_mut(&branch_id) {
            // hash(<version id> + <previous sig> + <every kv writes>)
            let new_sig = if let Some((_, sig)) = vers.last() {
                compute_sig(&[version_id_bytes.as_slice(), sig.as_slice()])
            } else {
                compute_sig(&[version_id_bytes.as_slice()])
            };
            vers.insert(version_id, new_sig);
        } else {
            return Err(eg!("branch not found"));
        }

        self.version_name_to_version_id
            .insert(version_name.to_owned(), version_id);
        self.version_to_change_set.insert(version_id, MapxOC::new());

        Ok(())
    }

    /// Check if a verison exists on default branch
    #[inline(always)]
    pub fn version_exists(&self, version_id: BranchID) -> bool {
        self.version_exists_on_branch(version_id, INITIAL_BRANCH_ID)
    }

    /// Check if a version exists on a specified branch(include its parents)
    #[inline(always)]
    pub fn version_exists_on_branch(
        &self,
        version_id: VersionID,
        branch_id: BranchID,
    ) -> bool {
        let branch_fp = self.branch_get_full_path(branch_id);

        if !Self::version_id_is_in_bounds(&branch_fp, version_id) {
            return false;
        }

        for (br, ver) in branch_fp.iter().rev() {
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

    /// Check if a version is directly created on a specified branch(exclude its parents)
    #[inline(always)]
    pub fn version_created_on_branch(
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
    pub fn version_pop(&mut self) -> Result<Option<VersionID>> {
        self.version_pop_by_branch(INITIAL_BRANCH_ID).c(d!())
    }

    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub fn version_pop_by_branch(
        &mut self,
        branch_id: BranchID,
    ) -> Result<Option<VersionID>> {
        if let Some((version_id, _)) = self.branch_to_created_versions.iter().last() {
            self.version_remove_by_branch(version_id, branch_id)
                .c(d!())
                .map(|_| Some(version_id))
        } else {
            Ok(None)
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

        for (key, _) in self.version_to_change_set.get(&version_id).c(d!())?.iter() {
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
            .find(|(name, id)| *id == version_id)
            .map(|(name, _)| name)
            .unwrap();
        self.version_name_to_version_id.remove(&version_name);

        Ok(())
    }

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
    pub fn branch_create(
        &mut self,
        branch_name: &[u8],
        base_branch_id: BranchID,
    ) -> Result<()> {
        self.branch_create_by_base_branch(branch_name, INITIAL_BRANCH_ID)
            .c(d!())
    }

    #[inline(always)]
    pub fn branch_create_by_base_branch(
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

    pub fn branch_create_by_base_branch_version(
        &mut self,
        branch_name: &[u8],
        base_branch_id: BranchID,
        base_version_id: VersionID,
    ) -> Result<()> {
        if (BRANCH_CNT_LIMIT - 1) < self.branch_to_parent.len() {
            return Err(eg!("too many branches"));
        }

        if self.branch_name_to_branch_id._contains_key(branch_name) {
            return Err(eg!("branch already exists"));
        }

        if !self.version_exists_on_branch(base_version_id, base_branch_id) {
            return Err(eg!("invalid base branch or version"));
        }

        let branch_id = VSDB.alloc_branch_id();

        self.branch_name_to_branch_id
            .insert(branch_name.to_owned(), branch_id);
        self.branch_to_parent.insert(
            branch_id,
            Some(VerPoint {
                branch_id: base_branch_id,
                version_id: base_version_id,
            }),
        );
        self.branch_to_created_versions
            .insert(branch_id, MapxOC::new());

        Ok(())
    }

    /// Check if a branch exists or not
    #[inline(always)]
    pub fn branch_exists(&self, branch_id: BranchID) -> bool {
        self.branch_to_parent.contains_key(&branch_id)
    }

    // Remove all changes directly made by this branch.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub fn branch_remove(&mut self, branch_id: BranchID) -> Result<()> {
        if self.branch_has_children(branch_id) {
            return Err(eg!("can not remove branches with children"));
        }

        if INITIAL_BRANCH_ID == branch_id {
            return Err(eg!("default branch can NOT be removed"));
        }

        self.branch_truncate_to(branch_id, None).c(d!())?;

        let branch_name = self
            .branch_name_to_branch_id
            .iter()
            .find(|(name, id)| *id == branch_id)
            .map(|(name, _)| name)
            .unwrap();
        self.branch_name_to_branch_id.remove(&branch_name);

        self.branch_to_parent.remove(&branch_id);
        self.branch_to_created_versions.remove(&branch_id);

        Ok(())
    }

    // Remove all changes directly made by versions(bigger than `last_version_id`) of this branch.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    pub fn branch_truncate_to(
        &mut self,
        branch_id: BranchID,
        last_version_id: Option<VersionID>,
    ) -> Result<()> {
        let last_version_id = last_version_id.unwrap_or(VersionID::MIN);

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
    pub fn branch_pop_version(
        &mut self,
        branch_id: BranchID,
    ) -> Result<Option<VersionID>> {
        self.version_pop_by_branch(branch_id).c(d!())
    }

    pub fn branch_merge_to_parent(&mut self, from: BranchID) -> Result<()> {
        let fp = self.branch_get_recurive_path(from, 2);

        if fp.is_empty() {
            return Err(eg!("branch not found"));
        } else if 1 == fp.len() {
            // no parents, means 'merge itself to itself'
            return Ok(());
        }

        let (parent_id, _) = fp.iter().nth(1).unwrap();

        let vers_created = self.branch_to_created_versions.remove(&from).unwrap();
        let mut vers_created_parent =
            self.branch_to_created_versions.get_mut(parent_id).unwrap();
        for (ver, sig) in vers_created.iter() {
            vers_created_parent.insert(ver, sig);

            for k in self
                .version_to_change_set
                .get(&ver)
                .unwrap()
                .iter()
                .map(|(k, _)| k)
            {
                let mut key_hdr = self.layered_kv.get_mut(&k).unwrap();

                let (value, empty) = {
                    let mut from_hdr = key_hdr.get_mut(&from).unwrap();
                    let v = from_hdr.remove(&ver).unwrap();
                    (v, from_hdr.is_empty())
                };

                if empty {
                    key_hdr.remove(&from);
                }

                key_hdr
                    .entry(*parent_id)
                    .or_insert(MapxOC::new())
                    .insert(ver, value);
            }
        }

        self.branch_to_parent.remove(&from);

        Ok(())
    }

    pub fn branch_has_children(&self, branch_id: BranchID) -> bool {
        self.branch_to_parent
            .iter()
            .filter_map(|(_, p)| p)
            .any(|p| p.branch_id == branch_id)
    }

    /// Get itself and all its ancestral branches with the base point it born on.
    #[inline(always)]
    pub fn branch_get_full_path(&self, branch_id: BranchID) -> BranchPath {
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
        if let Some(version_id) = self
            .branch_to_created_versions
            .get(&branch_id)
            .and_then(|vers| vers.last().map(|(id, _)| id))
        {
            ret.insert(branch_id, version_id);
            depth_limit = depth_limit.saturating_sub(1);
            loop {
                if 0 == depth_limit {
                    break;
                }
                if let Some(Some(vp)) = self.branch_to_parent.get(&branch_id) {
                    depth_limit -= 1;
                    ret.insert(vp.branch_id, vp.version_id);
                    branch_id = vp.branch_id;
                }
            }
        }
        ret
    }

    #[inline(always)]
    pub fn sig_get(&self) -> Option<VerSig> {
        self.sig_get_by_branch(INITIAL_BRANCH_ID)
    }

    #[inline(always)]
    pub fn sig_get_by_branch(&self, branch_id: BranchID) -> Option<VerSig> {
        self.branch_to_created_versions
            .get(&branch_id)?
            .last()
            .map(|(_, sig)| sig)
    }

    pub fn sig_get_by_branch_version(
        &self,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Option<VerSig> {
        let branch_fp = self.branch_get_full_path(branch_id);

        if !Self::version_id_is_in_bounds(&branch_fp, version_id) {
            return None;
        }

        for (br, ver) in branch_fp.iter().rev() {
            if let Some((_, sig)) = self
                .branch_to_created_versions
                .get(br)
                .unwrap()
                .get_le(&min!(*ver, version_id))
            {
                return Some(sig);
            }
        }

        None
    }

    #[inline(always)]
    pub fn prune(&mut self) -> Result<()> {
        self.prune_by_branch(INITIAL_BRANCH_ID, RESERVED_VERSION_NUM_DEFAULT)
    }

    pub fn prune_by_branch(
        &mut self,
        branch_id: BranchID,
        reserved_ver_num: usize,
    ) -> Result<()> {
        if self.branch_has_children(branch_id) {
            return Err(eg!("can not prune branches with children"));
        }

        let mut created_vers = self
            .branch_to_created_versions
            .get_mut(&branch_id)
            .c(d!("branch not found"))?;

        if created_vers.len() <= reserved_ver_num {
            return Ok(());
        }

        let guard_ver_id = created_vers
            .iter()
            .rev()
            .nth(reserved_ver_num)
            .map(|(ver, _)| ver)
            .unwrap();

        for (key, _) in self
            .layered_kv
            .iter()
            .filter(|(_, brs)| brs.contains_key(&branch_id))
        {
            let mut key_hdr = self.layered_kv.get_mut(&key).unwrap();
            let mut br_hdr = key_hdr.get_mut(&branch_id).unwrap();

            // at least keep one version
            for (ver, _) in br_hdr
                .iter()
                .rev()
                .skip(1)
                .filter(|(ver, _)| *ver <= guard_ver_id)
            {
                br_hdr.remove(&ver);
            }
        }

        for (ver, _) in created_vers.iter().rev().skip(reserved_ver_num) {
            created_vers.remove(&ver);
            self.version_to_change_set.remove(&ver);
            let (ver_name, _) = self
                .version_name_to_version_id
                .iter()
                .find(|(_, v)| *v == ver)
                .unwrap();
            self.version_name_to_version_id.remove(&ver_name);
        }

        Ok(())
    }
}

pub struct MapxRawVersionedIter<'a> {
    hdr: &'a MapxRawVersioned,
    iter: MapxOCIter<RawKey, MapxOC<BranchID, MapxOC<VersionID, Option<RawValue>>>>,
    branch_id: BranchID,
    version_id: VersionID,
}

impl<'a> Iterator for MapxRawVersionedIter<'a> {
    type Item = (RawKey, RawValue);

    #[allow(clippy::while_let_on_iterator)]
    fn next(&mut self) -> Option<Self::Item> {
        if ERROR_BRANCH_ID == self.branch_id || ERROR_VERSION_ID == self.version_id {
            return None;
        }

        while let Some((k, _)) = self.iter.next() {
            if let Some(v) =
                self.hdr
                    .get_by_branch_version(&k, self.branch_id, self.version_id)
            {
                return Some((k.to_owned(), v));
            }
        }

        None
    }
}

impl DoubleEndedIterator for MapxRawVersionedIter<'_> {
    #[allow(clippy::while_let_on_iterator)]
    fn next_back(&mut self) -> Option<Self::Item> {
        if ERROR_BRANCH_ID == self.branch_id || ERROR_VERSION_ID == self.version_id {
            return None;
        }

        while let Some((k, _)) = self.iter.next() {
            if let Some(v) =
                self.hdr
                    .get_by_branch_version(&k, self.branch_id, self.version_id)
            {
                return Some((k.to_owned(), v));
            }
        }

        None
    }
}

impl ExactSizeIterator for MapxRawVersionedIter<'_> {}

#[allow(missing_docs)]
#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a> {
    hdr: &'a mut MapxRawVersioned,
    key: ManuallyDrop<RawKey>,
    value: ManuallyDrop<RawValue>,
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
            key: ManuallyDrop::new(key),
            value: ManuallyDrop::new(value),
            branch_id,
        }
    }
}

/// NOTE: Very Important !!!
impl<'a> Drop for ValueMut<'a> {
    fn drop(&mut self) {
        // This operation is safe within a `drop()`.
        // SEE: [**ManuallyDrop::take**](std::mem::ManuallyDrop::take)
        unsafe {
            pnk!(self.hdr.insert_by_branch(
                ManuallyDrop::take(&mut self.key),
                ManuallyDrop::take(&mut self.value),
                self.branch_id,
            ));
        };
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
