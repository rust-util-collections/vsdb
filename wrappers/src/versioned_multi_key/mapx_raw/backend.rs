use crate::{
    basic::{
        mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey,
        mapx_ord_rawvalue::MapxOrdRawValue,
    },
    basic_multi_key::{mapx_raw::MapxRawMk, mapx_rawkey::MapxRawKeyMk},
    common::{
        ende::encode_optioned_bytes, BranchID, BranchIDBase, BranchName,
        BranchNameOwned, RawValue, VersionID, VersionIDBase, VersionName,
        VersionNameOwned, INITIAL_BRANCH_ID, INITIAL_BRANCH_NAME,
        RESERVED_VERSION_NUM_DEFAULT, VSDB,
    },
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};
use vsdb_core::common::{utils::hash::trie_root, TRASH_CLEANER};

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct MapxRawMkVs {
    default_branch: BranchID,
    key_size: usize,

    br_name_to_br_id: MapxOrdRawKey<BranchID>,
    ver_name_to_ver_id: MapxOrdRawKey<VersionID>,

    br_id_to_br_name: MapxOrdRawValue<BranchID>,
    ver_id_to_ver_name: MapxOrdRawValue<VersionID>,

    br_to_its_vers: MapxOrd<BranchID, MapxOrd<VersionID, ()>>,

    ver_to_change_set: MapxOrd<VersionID, MapxRawMk>,

    layered_kv: MapxRawKeyMk<MapxOrd<VersionID, Option<RawValue>>>,
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

impl MapxRawMkVs {
    #[inline(always)]
    pub(super) unsafe fn shadow(&self) -> Self {
        Self {
            default_branch: self.default_branch,
            key_size: self.key_size,
            br_name_to_br_id: self.br_name_to_br_id.shadow(),
            ver_name_to_ver_id: self.ver_name_to_ver_id.shadow(),
            br_id_to_br_name: self.br_id_to_br_name.shadow(),
            ver_id_to_ver_name: self.ver_id_to_ver_name.shadow(),
            br_to_its_vers: self.br_to_its_vers.shadow(),
            ver_to_change_set: self.ver_to_change_set.shadow(),
            layered_kv: self.layered_kv.shadow(),
        }
    }

    #[inline(always)]
    pub(super) fn new(key_size: usize) -> Self {
        let mut ret = Self {
            default_branch: BranchID::default(),
            key_size,
            br_name_to_br_id: MapxOrdRawKey::new(),
            ver_name_to_ver_id: MapxOrdRawKey::new(),
            br_id_to_br_name: MapxOrdRawValue::new(),
            ver_id_to_ver_name: MapxOrdRawValue::new(),
            br_to_its_vers: MapxOrd::new(),
            ver_to_change_set: MapxOrd::new(),
            layered_kv: MapxRawKeyMk::new(key_size),
        };
        ret.init();
        ret
    }

    #[inline(always)]
    fn init(&mut self) {
        let initial_brid = INITIAL_BRANCH_ID.to_be_bytes();
        self.default_branch = initial_brid;
        self.br_name_to_br_id
            .insert(INITIAL_BRANCH_NAME.0, &initial_brid);
        self.br_id_to_br_name
            .insert(&initial_brid, INITIAL_BRANCH_NAME.0);
        self.br_to_its_vers.insert(&initial_brid, &MapxOrd::new());
    }

    #[inline(always)]
    pub(super) fn insert(
        &mut self,
        key: &[&[u8]],
        value: &[u8],
    ) -> Result<Option<RawValue>> {
        self.insert_by_branch(key, value, self.branch_get_default())
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn insert_by_branch(
        &mut self,
        key: &[&[u8]],
        value: &[u8],
        br_id: BranchID,
    ) -> Result<Option<RawValue>> {
        self.br_to_its_vers
            .get(&br_id)
            .c(d!("branch not found"))?
            .last()
            .c(d!("no version on this branch, create a version first"))
            .and_then(|(ver_id, _)| {
                self.insert_by_branch_version(key, value, br_id, ver_id)
                    .c(d!())
            })
    }

    #[inline(always)]
    fn insert_by_branch_version(
        &mut self,
        key: &[&[u8]],
        value: &[u8],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_branch_version(key, Some(value), br_id, ver_id)
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn remove(&mut self, key: &[&[u8]]) -> Result<Option<RawValue>> {
        self.remove_by_branch(key, self.branch_get_default())
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn remove_by_branch(
        &mut self,
        key: &[&[u8]],
        br_id: BranchID,
    ) -> Result<Option<RawValue>> {
        self.br_to_its_vers
            .get(&br_id)
            .c(d!("branch not found"))?
            .last()
            .c(d!("no version on this branch, create a version first"))
            .and_then(|(ver_id, _)| {
                self.remove_by_branch_version(key, br_id, ver_id).c(d!())
            })
    }

    fn remove_by_branch_version(
        &mut self,
        key: &[&[u8]],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_branch_version(key, None, br_id, ver_id)
            .c(d!())
    }

    fn write_by_branch_version(
        &mut self,
        key: &[&[u8]],
        value: Option<&[u8]>,
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Result<Option<RawValue>> {
        if key.len() < self.key_size {
            return self
                .batch_remove_by_branch_version(key, value, ver_id)
                .c(d!());
        };

        let ret = self.get_by_branch_version(key, br_id, ver_id);

        if value.is_none() && ret.is_none() {
            return Ok(None);
        }

        self.ver_to_change_set
            .get_mut(&ver_id)
            .c(d!())?
            .insert(key, &[])
            .c(d!())?;

        unsafe {
            self.layered_kv
                .entry(key)
                .or_insert(&MapxOrd::new())
                .c(d!())?
                .insert_encoded_value(&ver_id, &encode_optioned_bytes(&value)[..]);
        }

        Ok(ret)
    }

    fn batch_remove_by_branch_version(
        &mut self,
        key: &[&[u8]],
        value: Option<&[u8]>,
        ver_id: VersionID,
    ) -> Result<Option<RawValue>> {
        let mut hdr = self.ver_to_change_set.get(&ver_id).c(d!())?;
        let hdr_shadow = unsafe { hdr.shadow() };
        let mut op = |k: &[&[u8]], _: &[u8]| hdr.insert(k, &[]).c(d!()).map(|_| ());
        hdr_shadow.iter_op_with_key_prefix(&mut op, key).c(d!())?;

        unsafe {
            let layered_kv_shadow = self.layered_kv.shadow();

            let mut op = |k: &[&[u8]], _: &MapxOrd<VersionID, Option<RawValue>>| {
                self.layered_kv
                    .entry(k)
                    .or_insert(&MapxOrd::new())
                    .c(d!())?
                    .insert_encoded_value(&ver_id, &encode_optioned_bytes(&value)[..]);
                Ok(())
            };

            layered_kv_shadow
                .iter_op_with_key_prefix(&mut op, key)
                .c(d!())?;
        }

        Ok(None)
    }

    #[inline(always)]
    pub(super) fn get(&self, key: &[&[u8]]) -> Option<RawValue> {
        self.get_by_branch(key, self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn iter_op<F>(&self, op: &mut F) -> Result<()>
    where
        F: FnMut(&[&[u8]], RawValue) -> Result<()>,
    {
        self.iter_op_by_branch(self.branch_get_default(), op)
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn iter_op_with_key_prefix<F>(
        &self,
        op: &mut F,
        key_prefix: &[&[u8]],
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], RawValue) -> Result<()>,
    {
        self.iter_op_with_key_prefix_by_branch(self.branch_get_default(), op, key_prefix)
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn get_by_branch(
        &self,
        key: &[&[u8]],
        br_id: BranchID,
    ) -> Option<RawValue> {
        if let Some(vers) = self.br_to_its_vers.get(&br_id) {
            if let Some(ver_id) = vers.last().map(|(id, _)| id) {
                return self.get_by_branch_version(key, br_id, ver_id);
            }
        }
        None
    }

    #[inline(always)]
    pub(super) fn iter_op_by_branch<F>(&self, br_id: BranchID, op: &mut F) -> Result<()>
    where
        F: FnMut(&[&[u8]], RawValue) -> Result<()>,
    {
        self.iter_op_with_key_prefix_by_branch(br_id, op, &[])
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn iter_op_with_key_prefix_by_branch<F>(
        &self,
        br_id: BranchID,
        op: &mut F,
        key_prefix: &[&[u8]],
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], RawValue) -> Result<()>,
    {
        self.br_to_its_vers
            .get(&br_id)
            .and_then(|vers| vers.last().map(|(id, _)| id))
            .c(d!("no versions found"))
            .and_then(|ver_id| {
                self.iter_op_with_key_prefix_by_branch_version(
                    br_id, ver_id, op, key_prefix,
                )
                .c(d!())
            })
    }

    pub(super) fn get_by_branch_version(
        &self,
        key: &[&[u8]],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Option<RawValue> {
        let vers = self.br_to_its_vers.get(&br_id)?;
        self.layered_kv
            .get(key)?
            .range(..=ver_id)
            .rev()
            .find(|(ver, _)| vers.contains_key(ver))
            .and_then(|(_, value)| value)
    }

    #[inline(always)]
    pub(super) fn iter_op_by_branch_version<F>(
        &self,
        br_id: BranchID,
        ver_id: VersionID,
        op: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], RawValue) -> Result<()>,
    {
        self.iter_op_with_key_prefix_by_branch_version(br_id, ver_id, op, &[])
    }

    #[inline(always)]
    pub(super) fn iter_op_with_key_prefix_by_branch_version<F>(
        &self,
        br_id: BranchID,
        ver_id: VersionID,
        op: &mut F,
        key_prefix: &[&[u8]],
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], RawValue) -> Result<()>,
    {
        let vers = self.br_to_its_vers.get(&br_id).c(d!())?;
        let mut cb =
            |k: &[&[u8]], v: &MapxOrd<VersionID, Option<RawValue>>| -> Result<()> {
                if let Some(value) = v
                    .range(..=ver_id)
                    .rev()
                    .find(|(ver, _)| vers.contains_key(ver))
                    .and_then(|(_, value)| value)
                {
                    op(k, value).c(d!())?;
                }
                Ok(())
            };

        self.layered_kv
            .iter_op_with_key_prefix(&mut cb, key_prefix)
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn clear(&mut self) {
        self.br_name_to_br_id.clear();
        self.ver_name_to_ver_id.clear();
        self.br_id_to_br_name.clear();
        self.ver_id_to_ver_name.clear();
        self.br_to_its_vers.clear();
        self.ver_to_change_set.clear();
        self.layered_kv.clear();

        self.init();
    }

    #[inline(always)]
    pub(super) fn version_create(&mut self, ver_name: &[u8]) -> Result<()> {
        self.version_create_by_branch(ver_name, self.branch_get_default())
            .c(d!())
    }

    pub(super) fn version_create_by_branch(
        &mut self,
        ver_name: &[u8],
        br_id: BranchID,
    ) -> Result<()> {
        if self.ver_name_to_ver_id.get(ver_name).is_some() {
            return Err(eg!("version already exists"));
        }

        let mut vers = self
            .br_to_its_vers
            .get_mut(&br_id)
            .c(d!("branch not found"))?;

        let ver_id = VSDB.alloc_ver_id().to_be_bytes();
        vers.insert(&ver_id, &());

        self.ver_name_to_ver_id.insert(ver_name, &ver_id);
        self.ver_id_to_ver_name.insert(&ver_id, ver_name);
        self.ver_to_change_set
            .insert(&ver_id, &MapxRawMk::new(self.key_size));

        Ok(())
    }

    // Check if a verison exists in the global scope
    #[inline(always)]
    pub(super) fn version_exists_globally(&self, ver_id: BranchID) -> bool {
        self.ver_to_change_set.contains_key(&ver_id)
    }

    #[inline(always)]
    pub(super) fn version_exists(&self, ver_id: BranchID) -> bool {
        self.version_exists_on_branch(ver_id, self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn version_exists_on_branch(
        &self,
        ver_id: VersionID,
        br_id: BranchID,
    ) -> bool {
        self.br_to_its_vers
            .get(&br_id)
            .map(|vers| vers.contains_key(&ver_id))
            .unwrap_or(false)
    }

    #[inline(always)]
    pub(super) fn version_pop(&mut self) -> Result<()> {
        self.version_pop_by_branch(self.branch_get_default())
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn version_pop_by_branch(&mut self, br_id: BranchID) -> Result<()> {
        let mut vers = self.br_to_its_vers.get(&br_id).c(d!("branch not found"))?;
        let vers_shadow = unsafe { vers.shadow() };
        if let Some((ver_id, _)) = vers_shadow.last() {
            vers.remove(&ver_id).c(d!("version is not on this branch"))
        } else {
            Ok(())
        }
    }

    #[inline(always)]
    pub(super) unsafe fn version_rebase(
        &mut self,
        base_version: VersionID,
    ) -> Result<()> {
        self.version_rebase_by_branch(base_version, self.branch_get_default())
            .c(d!())
    }
    pub(super) unsafe fn version_rebase_by_branch(
        &mut self,
        base_version: VersionID,
        br_id: BranchID,
    ) -> Result<()> {
        let mut vers_hdr = self.br_to_its_vers.get(&br_id).c(d!("branch not found"))?;
        let mut vers = vers_hdr.range(base_version..).map(|(ver, _)| ver);

        if let Some(ver) = vers.next() {
            if base_version != ver {
                return Err(eg!("base version is not on this branch"));
            }
        } else {
            return Err(eg!("base version is not on this branch"));
        };

        let mut base_ver_chgset = self.ver_to_change_set.get(&base_version).c(d!())?;
        let vers_to_be_merged = vers.collect::<Vec<_>>();

        let mut chgsets = vec![];
        let mut new_kvchgset_for_base_ver = HashMap::new();
        for verid in vers_to_be_merged.iter() {
            let mut chgset_ops = |k: &[&[u8]], _: &[u8]| {
                let v = self.layered_kv.get(k).c(d!())?.remove(verid).c(d!())?;
                let k = k.iter().map(|k| k.to_vec()).collect::<Vec<_>>();
                new_kvchgset_for_base_ver.insert(k, v);
                Ok(())
            };

            let chgset = self.ver_to_change_set.remove(verid).c(d!())?;
            chgset.iter_op(&mut chgset_ops).c(d!())?;
            chgsets.push(chgset);

            self.ver_id_to_ver_name
                .remove(verid)
                .c(d!())
                .and_then(|vername| self.ver_name_to_ver_id.remove(&vername).c(d!()))
                .and_then(|_| vers_hdr.remove(verid).c(d!()))?;
        }

        // avoid dup-middle 'insert's
        new_kvchgset_for_base_ver.iter().for_each(|(k, v)| {
            let k = k.iter().map(|k| k.as_slice()).collect::<Vec<_>>();
            pnk!(base_ver_chgset.insert(&k, &[]));
            pnk!(self.layered_kv.get(&k)).insert(&base_version, v);
        });

        TRASH_CLEANER.lock().execute(move || {
            chgsets.into_iter().for_each(|mut cs| {
                cs.clear();
            });
        });

        Ok(())
    }

    #[inline(always)]
    pub(super) fn version_get_id_by_name(
        &self,
        ver_name: VersionName,
    ) -> Option<VersionID> {
        self.ver_name_to_ver_id.get(ver_name.0)
    }

    #[inline(always)]
    pub(super) fn version_list(&self) -> Result<Vec<VersionNameOwned>> {
        self.version_list_by_branch(self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn version_list_by_branch(
        &self,
        br_id: BranchID,
    ) -> Result<Vec<VersionNameOwned>> {
        self.br_to_its_vers.get(&br_id).c(d!()).map(|vers| {
            vers.iter()
                .map(|(ver, _)| self.ver_id_to_ver_name.get(&ver).unwrap().to_vec())
                .map(VersionNameOwned)
                .collect()
        })
    }

    #[inline(always)]
    pub(super) fn version_list_globally(&self) -> Vec<VersionNameOwned> {
        self.ver_to_change_set
            .iter()
            .map(|(ver, _)| self.ver_id_to_ver_name.get(&ver).unwrap().to_vec())
            .map(VersionNameOwned)
            .collect()
    }

    #[inline(always)]
    pub(super) fn version_has_change_set(&self, ver_id: VersionID) -> Result<bool> {
        self.ver_to_change_set
            .get(&ver_id)
            .c(d!())
            .map(|chgset| !chgset.is_empty())
    }

    // clean up all orphaned versions in the global scope
    #[inline(always)]
    pub(super) fn version_clean_up_globally(&mut self) -> Result<()> {
        let mut valid_vers = HashSet::new();
        self.br_to_its_vers.values().for_each(|vers| {
            vers.iter().for_each(|(ver, _)| {
                valid_vers.insert(ver);
            })
        });

        let mut chgsets = vec![];
        for (ver, chgset) in unsafe { self.ver_to_change_set.shadow() }
            .iter()
            .filter(|(ver, _)| !valid_vers.contains(ver))
        {
            let mut chgset_ops = |k: &[&[u8]], _: &[u8]| {
                let mut lkv = self.layered_kv.get(k).c(d!())?;
                lkv.remove(&ver).c(d!())?;
                if lkv.is_empty() {
                    self.layered_kv.remove(k).c(d!())?;
                }
                Ok(())
            };
            chgset.iter_op(&mut chgset_ops).c(d!())?;

            self.ver_id_to_ver_name
                .remove(&ver)
                .c(d!())
                .and_then(|vername| self.ver_name_to_ver_id.remove(&vername).c(d!()))
                .and_then(|_| self.ver_to_change_set.remove(&ver).c(d!()))
                .map(|chgset| chgsets.push(chgset))?;
        }

        TRASH_CLEANER.lock().execute(move || {
            chgsets.into_iter().for_each(|mut cs| {
                cs.clear();
            });
        });

        Ok(())
    }

    // # Safety
    //
    // Version itself and its corresponding changes will be completely purged from all branches
    pub(super) unsafe fn version_revert_globally(
        &mut self,
        ver_id: VersionID,
    ) -> Result<()> {
        let mut chgset_ops = |key: &[&[u8]], _: &[u8]| {
            self.layered_kv
                .get(key)
                .c(d!())?
                .remove(&ver_id)
                .c(d!())
                .map(|_| ())
        };
        let chgset = self.ver_to_change_set.remove(&ver_id).c(d!())?;
        chgset.iter_op(&mut chgset_ops).c(d!())?;

        TRASH_CLEANER.lock().execute(move || {
            let mut cs = chgset;
            cs.clear();
        });

        self.br_to_its_vers.values().for_each(|mut vers| {
            vers.remove(&ver_id);
        });

        self.ver_id_to_ver_name
            .remove(&ver_id)
            .c(d!())
            .and_then(|vername| self.ver_name_to_ver_id.remove(&vername).c(d!()))
            .map(|_| ())
    }

    pub(super) fn version_chgset_trie_root(
        &self,
        br_id: Option<BranchID>,
        ver_id: Option<VersionID>,
    ) -> Result<Vec<u8>> {
        let ver = if let Some(v) = ver_id {
            v
        } else {
            let br = br_id.unwrap_or_else(|| self.branch_get_default());
            let v = self
                .br_to_its_vers
                .get(&br)
                .c(d!("branch not found"))?
                .last()
                .map(|(verid, _)| verid)
                .c(d!("version not found"))?;
            let mut ver = VersionID::default();
            ver.copy_from_slice(&v);
            ver
        };

        let chgset = self.ver_to_change_set.get(&ver).c(d!())?;
        let mut entries = vec![];
        let mut ops = |k: &[&[u8]], _: &[u8]| {
            let mut key = vec![];
            k.iter().for_each(|k| {
                key.extend_from_slice(k);
            });
            let v = pnk!(pnk!(self.layered_kv.get(k)).get(&ver)).unwrap_or_default();
            entries.push((key, v));
            Ok(())
        };
        chgset.iter_op(&mut ops).c(d!())?;

        Ok(trie_root(entries).to_vec())
    }

    #[inline(always)]
    pub(super) fn branch_create(
        &mut self,
        br_name: &[u8],
        ver_name: &[u8],
        force: bool,
    ) -> Result<()> {
        self.branch_create_by_base_branch(
            br_name,
            ver_name,
            self.branch_get_default(),
            force,
        )
        .c(d!())
    }

    #[inline(always)]
    pub(super) fn branch_create_by_base_branch(
        &mut self,
        br_name: &[u8],
        ver_name: &[u8],
        base_br_id: BranchID,
        force: bool,
    ) -> Result<()> {
        if self.ver_name_to_ver_id.contains_key(ver_name) {
            return Err(eg!("this version already exists"));
        }

        let base_ver_id = self
            .br_to_its_vers
            .get(&base_br_id)
            .c(d!("base branch not found"))?
            .last()
            .map(|(ver_id, _)| ver_id);

        unsafe {
            self.do_branch_create_by_base_branch_version(
                br_name,
                Some(ver_name),
                base_br_id,
                base_ver_id,
                force,
            )
            .c(d!())
        }
    }

    #[inline(always)]
    pub(super) fn branch_create_by_base_branch_version(
        &mut self,
        br_name: &[u8],
        ver_name: &[u8],
        base_br_id: BranchID,
        base_ver_id: VersionID,
        force: bool,
    ) -> Result<()> {
        if self.ver_name_to_ver_id.contains_key(ver_name) {
            return Err(eg!("this version already exists"));
        }

        unsafe {
            self.do_branch_create_by_base_branch_version(
                br_name,
                Some(ver_name),
                base_br_id,
                Some(base_ver_id),
                force,
            )
            .c(d!())
        }
    }

    #[inline(always)]
    pub(super) unsafe fn branch_create_without_new_version(
        &mut self,
        br_name: &[u8],
        force: bool,
    ) -> Result<()> {
        self.branch_create_by_base_branch_without_new_version(
            br_name,
            self.branch_get_default(),
            force,
        )
        .c(d!())
    }

    #[inline(always)]
    pub(super) unsafe fn branch_create_by_base_branch_without_new_version(
        &mut self,
        br_name: &[u8],
        base_br_id: BranchID,
        force: bool,
    ) -> Result<()> {
        let base_ver_id = self
            .br_to_its_vers
            .get(&base_br_id)
            .c(d!("base branch not found"))?
            .last()
            .map(|(ver_id, _)| ver_id);

        self.do_branch_create_by_base_branch_version(
            br_name,
            None,
            base_br_id,
            base_ver_id,
            force,
        )
        .c(d!())
    }

    #[inline(always)]
    pub(super) unsafe fn branch_create_by_base_branch_version_without_new_version(
        &mut self,
        br_name: &[u8],
        base_br_id: BranchID,
        base_ver_id: VersionID,
        force: bool,
    ) -> Result<()> {
        self.do_branch_create_by_base_branch_version(
            br_name,
            None,
            base_br_id,
            Some(base_ver_id),
            force,
        )
        .c(d!())
    }

    unsafe fn do_branch_create_by_base_branch_version(
        &mut self,
        br_name: &[u8],
        ver_name: Option<&[u8]>,
        base_br_id: BranchID,
        base_ver_id: Option<VersionID>,
        force: bool,
    ) -> Result<()> {
        if force {
            if let Some(brid) = self.br_name_to_br_id.get(br_name) {
                self.branch_remove(brid).c(d!())?;
            }
        }

        if self.br_name_to_br_id.contains_key(br_name) {
            return Err(eg!("branch already exists"));
        }

        let vers = self
            .br_to_its_vers
            .get(&base_br_id)
            .c(d!("base branch not exist"))?;

        let vers_copied = if let Some(bv) = base_ver_id {
            if !vers.contains_key(&bv) {
                return Err(eg!("version is not on the base branch"));
            }
            vers.range(..=bv).fold(MapxOrd::new(), |mut acc, (k, v)| {
                acc.insert(&k, &v);
                acc
            })
        } else {
            MapxOrd::new()
        };

        let br_id = VSDB.alloc_br_id().to_be_bytes();

        self.br_name_to_br_id.insert(br_name, &br_id);
        self.br_id_to_br_name.insert(&br_id, br_name);
        self.br_to_its_vers.insert(&br_id, &vers_copied);

        if let Some(vername) = ver_name {
            self.version_create_by_branch(vername, br_id).c(d!())?;
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_exists(&self, br_id: BranchID) -> bool {
        let condition_1 = self.br_id_to_br_name.contains_key(&br_id);
        let condition_2 = self.br_to_its_vers.contains_key(&br_id);
        assert_eq!(condition_1, condition_2);
        condition_1
    }

    #[inline(always)]
    pub(super) fn branch_has_versions(&self, br_id: BranchID) -> bool {
        self.branch_exists(br_id)
            && self
                .br_to_its_vers
                .get(&br_id)
                .map(|vers| !vers.is_empty())
                .unwrap_or(false)
    }

    #[inline(always)]
    pub(super) fn branch_remove(&mut self, br_id: BranchID) -> Result<()> {
        self.branch_truncate(br_id).c(d!())?;

        self.br_id_to_br_name
            .remove(&br_id)
            .c(d!())
            .and_then(|brname| self.br_name_to_br_id.remove(&brname).c(d!()))?;

        let mut vers = self.br_to_its_vers.remove(&br_id).c(d!())?;

        TRASH_CLEANER.lock().execute(move || {
            vers.clear();
        });

        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_keep_only(&mut self, br_ids: &[BranchID]) -> Result<()> {
        for brid in unsafe { self.br_id_to_br_name.shadow() }
            .iter()
            .map(|(brid, _)| brid)
            .filter(|brid| !br_ids.contains(brid))
        {
            self.branch_remove(brid).c(d!())?;
        }
        self.version_clean_up_globally().c(d!())
    }

    #[inline(always)]
    pub(super) fn branch_truncate(&mut self, br_id: BranchID) -> Result<()> {
        if let Some(mut vers) = self.br_to_its_vers.get(&br_id) {
            vers.clear();
            Ok(())
        } else {
            Err(eg!(
                "branch not found: {}",
                BranchIDBase::from_be_bytes(br_id)
            ))
        }
    }

    pub(super) fn branch_truncate_to(
        &mut self,
        br_id: BranchID,
        last_ver_id: VersionID,
    ) -> Result<()> {
        if let Some(mut vers) = self.br_to_its_vers.get(&br_id) {
            let vers_shadow = unsafe { vers.shadow() };
            for (ver_id, _) in vers_shadow.range(ver_add_1(last_ver_id)..).rev() {
                vers.remove(&ver_id)
                    .c(d!("version is not on this branch"))?;
            }
            Ok(())
        } else {
            Err(eg!(
                "branch not found: {}",
                BranchIDBase::from_be_bytes(br_id)
            ))
        }
    }

    #[inline(always)]
    pub(super) fn branch_pop_version(&mut self, br_id: BranchID) -> Result<()> {
        self.version_pop_by_branch(br_id).c(d!())
    }

    #[inline(always)]
    pub(super) fn branch_merge_to(
        &mut self,
        br_id: BranchID,
        target_br_id: BranchID,
    ) -> Result<()> {
        unsafe { self.do_branch_merge_to(br_id, target_br_id, false) }
    }

    #[inline(always)]
    pub(super) unsafe fn branch_merge_to_force(
        &mut self,
        br_id: BranchID,
        target_br_id: BranchID,
    ) -> Result<()> {
        self.do_branch_merge_to(br_id, target_br_id, true)
    }

    unsafe fn do_branch_merge_to(
        &mut self,
        br_id: BranchID,
        target_br_id: BranchID,
        force: bool,
    ) -> Result<()> {
        let vers = self.br_to_its_vers.get(&br_id).c(d!("branch not found"))?;
        let mut target_vers = self
            .br_to_its_vers
            .get(&target_br_id)
            .c(d!("target branch not found"))?;

        if !force {
            if let Some((ver, _)) = target_vers.last() {
                if !vers.contains_key(&ver) {
                    return Err(eg!("unable to merge safely"));
                }
            }
        }

        if let Some(fork_point) = vers
            .iter()
            .zip(target_vers.iter())
            .find(|(a, b)| a.0 != b.0)
        {
            vers.range(fork_point.0.0..).for_each(|(ver, _)| {
                target_vers.insert(&ver, &());
            });
        } else if let Some((latest_ver, _)) = vers.last() {
            if let Some((target_latest_ver, _)) = target_vers.last() {
                match latest_ver.cmp(&target_latest_ver) {
                    Ordering::Equal => {
                        return Ok(());
                    }
                    Ordering::Greater => {
                        vers.range(ver_add_1(target_latest_ver)..)
                            .map(|(ver, _)| ver)
                            .for_each(|ver| {
                                target_vers.insert(&ver, &());
                            });
                    }
                    _ => {}
                }
            } else {
                vers.iter().for_each(|(ver, _)| {
                    target_vers.insert(&ver, &());
                });
            }
        } else {
            return Ok(());
        };

        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_set_default(&mut self, br_id: BranchID) -> Result<()> {
        if !self.branch_exists(br_id) {
            return Err(eg!("branch not found"));
        }
        self.default_branch = br_id;
        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_get_default(&self) -> BranchID {
        self.default_branch
    }

    #[inline(always)]
    pub(super) fn branch_get_default_name(&self) -> BranchNameOwned {
        self.br_id_to_br_name
            .get(&self.default_branch)
            .map(|br| BranchNameOwned(br.to_vec()))
            .unwrap()
    }

    #[inline(always)]
    pub(super) fn branch_is_empty(&self, br_id: BranchID) -> Result<bool> {
        self.br_to_its_vers.get(&br_id).c(d!()).map(|vers| {
            vers.iter()
                .all(|(ver, _)| !self.version_has_change_set(ver).unwrap())
        })
    }

    #[inline(always)]
    pub(super) fn branch_list(&self) -> Vec<BranchNameOwned> {
        self.br_name_to_br_id
            .iter()
            .map(|(brname, _)| brname.to_vec())
            .map(BranchNameOwned)
            .collect()
    }

    // Logically similar to `std::ptr::swap`
    //
    // For example: If you have a master branch and a test branch, the data is always trial-run on the test branch, and then periodically merged back into the master branch. Rather than merging the test branch into the master branch, and then recreating the new test branch, it is more efficient to just swap the two branches, and then recreating the new test branch.
    //
    // # Safety
    //
    // - Non-'thread safe'
    // - Must ensure that there are no reads and writes to these two branches during the execution
    pub(super) unsafe fn branch_swap(
        &mut self,
        branch_1: &[u8],
        branch_2: &[u8],
    ) -> Result<()> {
        let brid_1 = self.br_name_to_br_id.get(branch_1).c(d!())?;
        let brid_2 = self.br_name_to_br_id.get(branch_2).c(d!())?;

        self.br_name_to_br_id.insert(branch_1, &brid_2).c(d!())?;
        self.br_name_to_br_id.insert(branch_2, &brid_1).c(d!())?;

        self.br_id_to_br_name.insert(&brid_1, branch_2).c(d!())?;
        self.br_id_to_br_name.insert(&brid_2, branch_1).c(d!())?;

        if self.default_branch == brid_1 {
            self.default_branch = brid_2;
        } else if self.default_branch == brid_2 {
            self.default_branch = brid_1;
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_get_id_by_name(&self, br_name: BranchName) -> Option<BranchID> {
        self.br_name_to_br_id.get(br_name.0)
    }

    #[inline(always)]
    pub(super) fn prune(&mut self, reserved_ver_num: Option<usize>) -> Result<()> {
        // the '1' of this 'add 1' means the never-deleted initial version.
        let reserved_ver_num =
            1 + reserved_ver_num.unwrap_or(RESERVED_VERSION_NUM_DEFAULT);
        if 0 == reserved_ver_num {
            return Err(eg!("reserved version number should NOT be zero"));
        }

        let mut br_vers_non_empty = self
            .br_to_its_vers
            .values()
            .filter(|vers| !vers.is_empty())
            .collect::<Vec<_>>();
        alt!(br_vers_non_empty.is_empty(), return Ok(()));
        let mut br_vers = (0..br_vers_non_empty.len())
            .map(|i| (&br_vers_non_empty[i]).iter())
            .collect::<Vec<_>>();

        let mut guard = Default::default();
        let mut vers_to_be_merged: Vec<VersionID> = vec![];
        'x: loop {
            for (idx, vers) in br_vers.iter_mut().enumerate() {
                if let Some((ver, _)) = vers.next() {
                    alt!(0 == idx, guard = ver);
                    alt!(guard != ver, break 'x);
                } else {
                    break 'x;
                }
            }
            vers_to_be_merged.push(guard);
        }

        let l = vers_to_be_merged.len();
        if l <= reserved_ver_num {
            return Ok(());
        }

        let (vers_to_be_merged, rewrite_ver) = {
            let guard_idx = l - reserved_ver_num + 1;
            (&vers_to_be_merged[1..guard_idx], &vers_to_be_merged[0])
        };

        let mut rewrite_ver_chgset = self.ver_to_change_set.get(rewrite_ver).c(d!())?;

        for vers in br_vers_non_empty.iter_mut() {
            for ver in vers_to_be_merged.iter() {
                vers.remove(ver).c(d!())?;
            }
        }

        let mut chgkeys = HashSet::new();
        let mut new_kvchgset_for_base_ver = HashMap::new();
        for ver in vers_to_be_merged.iter() {
            let chgset = self.ver_to_change_set.get(ver).c(d!())?;
            let mut chgset_ops = |k: &[&[u8]], _: &[u8]| {
                let k_vers = self.layered_kv.get(k).c(d!())?;
                let value = k_vers.get(ver).c(d!())?;
                let k = k.iter().map(|k| k.to_vec()).collect::<Vec<_>>();
                new_kvchgset_for_base_ver.insert(k.clone(), (k_vers, value));
                chgkeys.insert(k);
                Ok(())
            };
            chgset.iter_op(&mut chgset_ops).c(d!())?;
        }

        // avoid dup-middle 'insert's
        new_kvchgset_for_base_ver
            .into_iter()
            .for_each(|(k, (mut k_vers, v))| {
                let k = k.iter().map(|k| k.as_slice()).collect::<Vec<_>>();
                pnk!(rewrite_ver_chgset.insert(&k, &[]));
                k_vers.insert(rewrite_ver, &v);
            });

        // Make all merged version to be orphan,
        // so they will be cleaned up in the `version_clean_up_globally`.
        //
        // NOTE: do this after all data has been copied to new places!
        for ver in vers_to_be_merged.iter() {
            self.br_to_its_vers.iter().for_each(|(_, mut vers)| {
                vers.remove(ver);
            });
        }

        // lowest-level KVs with 'deleted' states should be cleaned up.
        for k in chgkeys.iter() {
            let k = k.iter().map(|k| k.as_slice()).collect::<Vec<_>>();
            if let Some(mut vers) = self.layered_kv.get(&k) {
                // A 'NULL' value means 'not exist'.
                if vers.get(rewrite_ver).c(d!())?.is_none() {
                    vers.remove(rewrite_ver).c(d!())?;
                    rewrite_ver_chgset.remove(&k).c(d!())?;
                }
                if vers.is_empty() {
                    self.layered_kv.remove(&k).c(d!())?;
                }
            }
        }

        self.version_clean_up_globally().c(d!())?;

        #[cfg(test)]
        {
            let mut ops = |_: &[&[u8]], vers: &MapxOrd<VersionID, Option<RawValue>>| {
                if let Some(v) = vers.get(rewrite_ver) {
                    assert!(!v.is_none());
                }
                Ok(())
            };
            pnk!(self.layered_kv.iter_op(&mut ops));

            for ver in vers_to_be_merged.iter() {
                assert!(!self.ver_to_change_set.contains_key(ver));
                let mut ops =
                    |_: &[&[u8]], vers: &MapxOrd<VersionID, Option<RawValue>>| {
                        assert!(!vers.contains_key(ver));
                        Ok(())
                    };
                pnk!(self.layered_kv.iter_op(&mut ops));
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[inline(always)]
fn ver_add_1(ver: VersionID) -> VersionID {
    (VersionIDBase::from_be_bytes(ver) + 1).to_be_bytes()
}
