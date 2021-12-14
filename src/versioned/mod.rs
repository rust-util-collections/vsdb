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

type BranchFullPath = BTreeMap<BranchID, VersionID>;

const INITIAL_BRANCH_ID: BranchID = 0;
const INITIAL_BRANCH_NAME: &str = "main";

const ERROR_BRANCH_ID: BranchID = BIGGEST_RESERVED_ID;
const ERROR_VERSION_ID: VersionID = BIGGEST_RESERVED_ID;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct VerPoint {
    // parent branch ID of this branch
    br_id: BranchID,
    // which verion of its parent branch is this branch forked from
    ver_id: VersionID,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapxRawVersioned {
    br_name_to_br_id: MapxOC<RawKey, BranchID>,
    ver_name_to_ver_id: MapxOC<RawKey, VersionID>,

    br_to_parent: MapxOC<BranchID, Option<VerPoint>>,

    // versions directly created on this branch
    br_to_created_vers: MapxOC<BranchID, MapxOC<VersionID, VerSig>>,

    // ever changed keys within each version
    ver_to_chg_set: MapxOC<VersionID, MapxOC<RawKey, bool>>,

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

        ret.br_name_to_br_id.insert(
            INITIAL_BRANCH_NAME.to_owned().into_bytes(),
            INITIAL_BRANCH_ID,
        );
        ret.br_to_parent.insert(INITIAL_BRANCH_ID, None);
        ret.br_to_created_vers
            .insert(INITIAL_BRANCH_ID, MapxOC::new());

        ret
    }

    #[inline(always)]
    pub fn insert(&mut self, key: RawKey, value: RawValue) -> Result<Option<RawValue>> {
        self.insert_by_br(key, value, INITIAL_BRANCH_ID).c(d!())
    }

    #[inline(always)]
    pub fn insert_by_br(
        &mut self,
        key: RawKey,
        value: RawValue,
        br_id: BranchID,
    ) -> Result<Option<RawValue>> {
        self.br_to_created_vers
            .get(&br_id)
            .c(d!("branch not found"))?
            .last()
            .c(d!("no version on this branch, create a version first"))
            .and_then(|(ver_id, _)| {
                self.insert_by_br_ver(key, value, br_id, ver_id).c(d!())
            })
    }

    // This function should NOT be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    #[inline(always)]
    fn insert_by_br_ver(
        &mut self,
        key: RawKey,
        value: RawValue,
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_br_ver(key, Some(value), br_id, ver_id)
            .c(d!())
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &[u8]) -> Result<Option<RawValue>> {
        self.remove_by_br(key, INITIAL_BRANCH_ID).c(d!())
    }

    #[inline(always)]
    pub fn remove_by_br(
        &mut self,
        key: &[u8],
        br_id: BranchID,
    ) -> Result<Option<RawValue>> {
        self.br_to_created_vers
            .get(&br_id)
            .c(d!("branch not found"))?
            .last()
            .c(d!("no version on this branch, create a version first"))
            .and_then(|(ver_id, _)| self.remove_by_br_ver(key, br_id, ver_id).c(d!()))
    }

    // This function should NOT be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    //
    // The `remove` is essentially assigning a `None` value to the key.
    fn remove_by_br_ver(
        &mut self,
        key: &[u8],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_br_ver(key.to_owned(), None, br_id, ver_id)
            .c(d!())
    }

    // This function should NOT be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    fn write_by_br_ver(
        &mut self,
        key: RawKey,
        value: Option<RawValue>,
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Result<Option<RawValue>> {
        if let Some(mut chgset) = self.ver_to_chg_set.get_mut(&ver_id) {
            chgset.insert(key.clone(), true);
        } else {
            return Err(eg!("BUG: version not found"));
        }

        if let Some(mut vers) = self.br_to_created_vers.get(&br_id) {
            if let Some(mut sig) = vers.get_mut(&ver_id) {
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

        if let Some(mut brs) = self.layered_kv.get_mut(&key) {
            if let Some(mut vers) = brs.get_mut(&br_id) {
                return Ok(vers.insert(ver_id, value).flatten());
            }

            let mut vers = MapxOC::new();
            vers.insert(ver_id, value);
            brs.insert(br_id, vers);

            return Ok(None);
        }

        let mut brs = MapxOC::new();
        let mut vers = MapxOC::new();
        vers.insert(ver_id, value);
        brs.insert(br_id, vers);
        self.layered_kv.insert(key, brs);

        Ok(None)
    }

    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<RawValue> {
        self.get_by_br(key, INITIAL_BRANCH_ID)
    }

    #[inline(always)]
    pub fn get_by_br(&self, key: &[u8], br_id: BranchID) -> Option<RawValue> {
        if let Some(vers) = self.br_to_created_vers.get(&br_id) {
            if let Some((ver_id, _)) = vers.last() {
                return self.get_by_br_ver(key, br_id, ver_id);
            }
        }
        None
    }

    pub fn get_by_br_ver(
        &self,
        key: &[u8],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Option<RawValue> {
        let br_fp = self.br_get_full_path(br_id);

        if !Self::ver_id_is_in_bounds(&br_fp, ver_id) {
            return None;
        }

        if let Some(brs) = self.layered_kv._get(key) {
            // they are all monotonically increasing
            for (br, ver) in br_fp.iter().rev() {
                if let Some(vers) = brs.get(br) {
                    if let Some((_, v)) = vers.get_le(&min!(*ver, ver_id)) {
                        return v;
                    };
                }
            }
        }

        None
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<ValueMut<'_>> {
        self.get_mut_by_br(key, INITIAL_BRANCH_ID)
    }

    #[inline(always)]
    pub fn get_mut_by_br(
        &mut self,
        key: &[u8],
        br_id: BranchID,
    ) -> Option<ValueMut<'_>> {
        self.br_to_created_vers
            .get(&br_id)?
            .last()
            .and_then(|(ver_id, _)| self.get_mut_by_br_ver(key, br_id, ver_id))
    }

    // This function should NOT be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    fn get_mut_by_br_ver(
        &mut self,
        key: &[u8],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Option<ValueMut<'_>> {
        self.get_by_br_ver(key, br_id, ver_id)
            .map(|v| ValueMut::new(self, key.to_owned(), v, br_id))
    }

    #[inline(always)]
    pub fn get_ge(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range(key.to_owned()..).next()
    }

    #[inline(always)]
    pub fn get_ge_by_br(
        &self,
        key: &[u8],
        br_id: BranchID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_by_br(br_id, key.to_owned()..).next()
    }

    #[inline(always)]
    pub fn get_ge_by_br_ver(
        &self,
        key: &[u8],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_by_br_ver(br_id, ver_id, key.to_owned()..).next()
    }

    #[inline(always)]
    pub fn get_le(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range(..=key.to_owned()).next_back()
    }

    #[inline(always)]
    pub fn get_le_by_br(
        &self,
        key: &[u8],
        br_id: BranchID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_by_br(br_id, ..=key.to_owned()).next_back()
    }

    #[inline(always)]
    pub fn get_le_by_br_ver(
        &self,
        key: &[u8],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_by_br_ver(br_id, ver_id, ..=key.to_owned())
            .next_back()
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxRawVersionedIter {
        self.iter_by_br(INITIAL_BRANCH_ID)
    }

    #[inline(always)]
    pub fn iter_by_br(&self, br_id: BranchID) -> MapxRawVersionedIter {
        if let Some(vers) = self.br_to_created_vers.get(&br_id) {
            if let Some((ver_id, _)) = vers.last() {
                return self.iter_by_br_ver(br_id, ver_id);
            }
        }
        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            br_id: ERROR_BRANCH_ID,
            ver_id: ERROR_VERSION_ID,
        }
    }

    #[inline(always)]
    pub fn iter_by_br_ver(
        &self,
        br_id: BranchID,
        ver_id: VersionID,
    ) -> MapxRawVersionedIter {
        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            br_id,
            ver_id,
        }
    }

    #[inline(always)]
    pub fn range<R: RangeBounds<RawKey>>(&self, bounds: R) -> MapxRawVersionedIter {
        self.range_by_br(INITIAL_BRANCH_ID, bounds)
    }

    #[inline(always)]
    pub fn range_by_br<R: RangeBounds<RawKey>>(
        &self,
        br_id: BranchID,
        bounds: R,
    ) -> MapxRawVersionedIter {
        if let Some(vers) = self.br_to_created_vers.get(&br_id) {
            if let Some((ver_id, _)) = vers.last() {
                return self.range_by_br_ver(br_id, ver_id, bounds);
            }
        }
        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            br_id: ERROR_BRANCH_ID,
            ver_id: ERROR_VERSION_ID,
        }
    }

    #[inline(always)]
    pub fn range_by_br_ver<R: RangeBounds<RawKey>>(
        &self,
        br_id: BranchID,
        ver_id: VersionID,
        bounds: R,
    ) -> MapxRawVersionedIter {
        MapxRawVersionedIter {
            hdr: self,
            iter: self.layered_kv.range(bounds),
            br_id,
            ver_id,
        }
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    #[inline(always)]
    pub fn contains_key_by_br(&self, key: &[u8], br_id: BranchID) -> bool {
        self.get_by_br(key, br_id).is_some()
    }

    #[inline(always)]
    pub fn contains_key_by_br_ver(
        &self,
        key: &[u8],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> bool {
        self.get_by_br_ver(key, br_id, ver_id).is_some()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    #[inline(always)]
    pub fn len_by_br(&self, br_id: BranchID) -> usize {
        self.iter_by_br(br_id).count()
    }

    #[inline(always)]
    pub fn len_by_br_ver(&self, br_id: BranchID, ver_id: VersionID) -> usize {
        self.iter_by_br_ver(br_id, ver_id).count()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        0 == self.len()
    }

    #[inline(always)]
    pub fn is_empty_by_br(&self, br_id: BranchID) -> bool {
        0 == self.len_by_br(br_id)
    }

    #[inline(always)]
    pub fn is_empty_by_br_ver(&self, br_id: BranchID, ver_id: VersionID) -> bool {
        0 == self.len_by_br_ver(br_id, ver_id)
    }

    /// Clear all data, mainly for testing purpose.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.br_name_to_br_id.clear();
        self.ver_name_to_ver_id.clear();
        self.br_to_parent.clear();
        self.br_to_created_vers.clear();
        self.ver_to_chg_set.clear();
        self.layered_kv.clear();
    }

    #[inline(always)]
    pub fn ver_create(&mut self, ver_name: &[u8]) -> Result<()> {
        self.ver_create_by_br(ver_name, INITIAL_BRANCH_ID).c(d!())
    }

    pub fn ver_create_by_br(&mut self, ver_name: &[u8], br_id: BranchID) -> Result<()> {
        if self.ver_name_to_ver_id._get(ver_name).is_some() {
            return Err(eg!("version already exists"));
        }

        let ver_id = VSDB.alloc_version_id();
        let ver_id_bytes = ver_id.to_be_bytes();

        if let Some(mut vers) = self.br_to_created_vers.get_mut(&br_id) {
            // hash(<version id> + <previous sig> + <every kv writes>)
            let new_sig = if let Some((_, sig)) = vers.last() {
                compute_sig(&[ver_id_bytes.as_slice(), sig.as_slice()])
            } else {
                compute_sig(&[ver_id_bytes.as_slice()])
            };
            vers.insert(ver_id, new_sig);
        } else {
            return Err(eg!("branch not found"));
        }

        self.ver_name_to_ver_id.insert(ver_name.to_owned(), ver_id);
        self.ver_to_chg_set.insert(ver_id, MapxOC::new());

        Ok(())
    }

    /// Check if a verison exists on default branch
    #[inline(always)]
    pub fn ver_exists(&self, ver_id: BranchID) -> bool {
        self.ver_exists_on_br(ver_id, INITIAL_BRANCH_ID)
    }

    /// Check if a version exists on a specified branch(include its parents)
    #[inline(always)]
    pub fn ver_exists_on_br(&self, ver_id: VersionID, br_id: BranchID) -> bool {
        let br_fp = self.br_get_full_path(br_id);

        if !Self::ver_id_is_in_bounds(&br_fp, ver_id) {
            return false;
        }

        for (br, ver) in br_fp.iter().rev() {
            if self
                .br_to_created_vers
                .get(br)
                .unwrap()
                .get_le(&min!(*ver, ver_id))
                .is_some()
            {
                return true;
            }
        }

        false
    }

    /// Check if a version is directly created on a specified branch(exclude its parents)
    #[inline(always)]
    pub fn ver_created_on_br(&self, ver_id: VersionID, br_id: BranchID) -> bool {
        self.br_to_created_vers
            .get(&br_id)
            .map(|vers| vers.get(&ver_id))
            .flatten()
            .is_some()
    }

    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub fn ver_pop(&mut self) -> Result<Option<VersionID>> {
        self.ver_pop_by_br(INITIAL_BRANCH_ID).c(d!())
    }

    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub fn ver_pop_by_br(&mut self, br_id: BranchID) -> Result<Option<VersionID>> {
        if let Some((ver_id, _)) = self.br_to_created_vers.iter().last() {
            self.ver_remove_by_br(ver_id, br_id)
                .c(d!())
                .map(|_| Some(ver_id))
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
    fn ver_remove_by_br(&mut self, ver_id: VersionID, br_id: BranchID) -> Result<()> {
        if self
            .br_to_created_vers
            .get(&br_id)
            .c(d!("branch not found"))?
            .remove(&ver_id)
            .is_none()
        {
            return Err(eg!("version is not created by this branch"));
        }

        for (key, _) in self.ver_to_chg_set.get(&ver_id).c(d!())?.iter() {
            let mut local_brs = self.layered_kv.get(&key).unwrap();
            let mut local_vers = local_brs.get(&br_id).unwrap();
            local_vers.remove(&ver_id);
            if local_vers.is_empty() {
                local_brs.remove(&br_id);
            }
        }
        self.ver_to_chg_set.remove(&ver_id);

        let ver_name = self
            .ver_name_to_ver_id
            .iter()
            .find(|(name, id)| *id == ver_id)
            .map(|(name, _)| name)
            .unwrap();
        self.ver_name_to_ver_id.remove(&ver_name);

        Ok(())
    }

    fn ver_id_is_in_bounds(fp: &BranchFullPath, ver_id: VersionID) -> bool {
        if let Some(max_ver_id) = fp.values().last() {
            // querying future versions
            if *max_ver_id < ver_id {
                return false;
            }
        } else {
            // branch does not exist
            return false;
        }
        true
    }

    #[inline(always)]
    pub fn br_create(&mut self, br_name: &[u8], base_br_id: BranchID) -> Result<()> {
        self.br_create_by_base_br(br_name, INITIAL_BRANCH_ID)
            .c(d!())
    }

    #[inline(always)]
    pub fn br_create_by_base_br(
        &mut self,
        br_name: &[u8],
        base_br_id: BranchID,
    ) -> Result<()> {
        let base_ver_id = self
            .br_to_created_vers
            .get(&base_br_id)
            .c(d!("base branch not found"))?
            .last()
            .map(|(ver_id, _)| ver_id)
            .c(d!("base version not found"))?;
        self.br_create_by_base_br_ver(br_name, base_br_id, base_ver_id)
            .c(d!())
    }

    pub fn br_create_by_base_br_ver(
        &mut self,
        br_name: &[u8],
        base_br_id: BranchID,
        base_ver_id: VersionID,
    ) -> Result<()> {
        if self.br_name_to_br_id._contains_key(br_name) {
            return Err(eg!("branch already exists"));
        }

        if !self.ver_exists_on_br(base_ver_id, base_br_id) {
            return Err(eg!("invalid base branch or version"));
        }

        let br_id = VSDB.alloc_branch_id();

        self.br_name_to_br_id.insert(br_name.to_owned(), br_id);
        self.br_to_parent.insert(
            br_id,
            Some(VerPoint {
                br_id: base_br_id,
                ver_id: base_ver_id,
            }),
        );
        self.br_to_created_vers.insert(br_id, MapxOC::new());

        Ok(())
    }

    /// Check if a branch exists or not
    #[inline(always)]
    pub fn br_exists(&self, br_id: BranchID) -> bool {
        self.br_to_parent.contains_key(&br_id)
    }

    // Remove all changes directly made by this branch.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub fn br_remove(&mut self, br_id: BranchID) -> Result<()> {
        if INITIAL_BRANCH_ID == br_id {
            return Err(eg!("default branch can NOT be removed"));
        }

        self.br_truncate_to(br_id, None).c(d!())?;

        let br_name = self
            .br_name_to_br_id
            .iter()
            .find(|(name, id)| *id == br_id)
            .map(|(name, _)| name)
            .unwrap();
        self.br_name_to_br_id.remove(&br_name);

        self.br_to_parent.remove(&br_id);
        self.br_to_created_vers.remove(&br_id);

        Ok(())
    }

    // Remove all changes directly made by versions(bigger than `last_ver_id`) of this branch.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    pub fn br_truncate_to(
        &mut self,
        br_id: BranchID,
        last_ver_id: Option<VersionID>,
    ) -> Result<()> {
        let last_ver_id = last_ver_id.unwrap_or(VersionID::MIN);

        if let Some(vers) = self.br_to_created_vers.get(&br_id) {
            // version id must be in descending order
            for (ver_id, _) in vers.range((1 + last_ver_id)..).rev() {
                self.ver_remove_by_br(ver_id, br_id).c(d!())?;
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
    pub fn br_pop_ver(&mut self, br_id: BranchID) -> Result<Option<VersionID>> {
        self.ver_pop_by_br(br_id).c(d!())
    }

    /// Both 'branch id' and 'version id' are globally monotonically increasing.
    pub fn br_get_full_path(&self, mut br_id: BranchID) -> BranchFullPath {
        let mut ret = BTreeMap::new();
        if let Some(ver_id) = self
            .br_to_created_vers
            .get(&br_id)
            .and_then(|vers| vers.last().map(|(id, _)| id))
        {
            ret.insert(br_id, ver_id);
            while let Some(Some(vp)) = self.br_to_parent.get(&br_id) {
                ret.insert(vp.br_id, vp.ver_id);
                br_id = vp.br_id;
            }
        }
        ret
    }

    #[inline(always)]
    pub fn sig_get(&self) -> Option<VerSig> {
        self.sig_get_by_br(INITIAL_BRANCH_ID)
    }

    #[inline(always)]
    pub fn sig_get_by_br(&self, br_id: BranchID) -> Option<VerSig> {
        self.br_to_created_vers
            .get(&br_id)?
            .last()
            .map(|(_, sig)| sig)
    }

    pub fn sig_get_by_br_ver(
        &self,
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Option<VerSig> {
        let br_fp = self.br_get_full_path(br_id);

        if !Self::ver_id_is_in_bounds(&br_fp, ver_id) {
            return None;
        }

        for (br, ver) in br_fp.iter().rev() {
            if let Some((_, sig)) = self
                .br_to_created_vers
                .get(br)
                .unwrap()
                .get_le(&min!(*ver, ver_id))
            {
                return Some(sig);
            }
        }

        None
    }
}

pub struct MapxRawVersionedIter<'a> {
    hdr: &'a MapxRawVersioned,
    iter: MapxOCIter<RawKey, MapxOC<BranchID, MapxOC<VersionID, Option<RawValue>>>>,
    br_id: BranchID,
    ver_id: VersionID,
}

impl<'a> Iterator for MapxRawVersionedIter<'a> {
    type Item = (RawKey, RawValue);

    #[allow(clippy::while_let_on_iterator)]
    fn next(&mut self) -> Option<Self::Item> {
        if ERROR_BRANCH_ID == self.br_id || ERROR_VERSION_ID == self.ver_id {
            return None;
        }

        while let Some((k, _)) = self.iter.next() {
            if let Some(v) = self.hdr.get_by_br_ver(&k, self.br_id, self.ver_id) {
                return Some((k.to_owned(), v));
            }
        }

        None
    }
}

impl DoubleEndedIterator for MapxRawVersionedIter<'_> {
    #[allow(clippy::while_let_on_iterator)]
    fn next_back(&mut self) -> Option<Self::Item> {
        if ERROR_BRANCH_ID == self.br_id || ERROR_VERSION_ID == self.ver_id {
            return None;
        }

        while let Some((k, _)) = self.iter.next() {
            if let Some(v) = self.hdr.get_by_br_ver(&k, self.br_id, self.ver_id) {
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
    br_id: BranchID,
}

impl<'a> ValueMut<'a> {
    fn new(
        hdr: &'a mut MapxRawVersioned,
        key: RawKey,
        value: RawValue,
        br_id: BranchID,
    ) -> Self {
        ValueMut {
            hdr,
            key: ManuallyDrop::new(key),
            value: ManuallyDrop::new(value),
            br_id,
        }
    }
}

/// NOTE: Very Important !!!
impl<'a> Drop for ValueMut<'a> {
    fn drop(&mut self) {
        // This operation is safe within a `drop()`.
        // SEE: [**ManuallyDrop::take**](std::mem::ManuallyDrop::take)
        unsafe {
            pnk!(self.hdr.insert_by_br(
                ManuallyDrop::take(&mut self.key),
                ManuallyDrop::take(&mut self.value),
                self.br_id,
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
