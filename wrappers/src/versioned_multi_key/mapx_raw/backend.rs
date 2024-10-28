use crate::{
    basic::{mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey},
    basic_multi_key::mapx_raw::MapxRawMk,
    common::{
        trie_root, BranchID, BranchIDBase, BranchName, BranchNameOwned, RawKey,
        RawValue, VersionID, VersionIDBase, VersionName, VersionNameOwned,
        INITIAL_BRANCH_ID, INITIAL_BRANCH_NAME, NULL, RESERVED_VERSION_NUM_DEFAULT,
        TRASH_CLEANER, VER_ID_MAX, VSDB,
    },
};
use parking_lot::RwLock;
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
    result::Result as StdResult,
    sync::Arc,
};

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub(super) struct MapxRawMkVs {
    key_size: u32,
    default_branch: BranchID,

    br_name_to_br_id: MapxOrdRawKey<BranchID>,
    ver_name_to_ver_id: MapxOrdRawKey<VersionID>,
    br_to_its_vers: MapxOrd<BranchID, MapxOrd<VersionID, ()>>,
    ver_to_change_set: MapxOrd<VersionID, MapxRawMk>,

    br_id_to_br_name: Arc<RwLock<HashMap<BranchID, RawValue>>>,
    ver_id_to_ver_name: Arc<RwLock<HashMap<VersionID, RawValue>>>,
    layered_kv: Arc<RwLock<BTreeMap<Vec<RawKey>, BTreeMap<VersionID, RawValue>>>>,
}

impl Clone for MapxRawMkVs {
    fn clone(&self) -> Self {
        Self {
            key_size: self.key_size,
            default_branch: self.default_branch,

            br_name_to_br_id: self.br_name_to_br_id.clone(),
            ver_name_to_ver_id: self.ver_name_to_ver_id.clone(),
            br_to_its_vers: self.br_to_its_vers.clone(),
            ver_to_change_set: self.ver_to_change_set.clone(),

            br_id_to_br_name: self.br_id_to_br_name.clone(),
            ver_id_to_ver_name: self.ver_id_to_ver_name.clone(),
            layered_kv: Arc::new(RwLock::new(self.layered_kv.read().clone())),
        }
    }
}

impl Serialize for MapxRawMkVs {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        MapxRawMkVsWithoutDerivedFields::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MapxRawMkVs {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <MapxRawMkVsWithoutDerivedFields as Deserialize>::deserialize(deserializer)
            .map(Self::from)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct MapxRawMkVsWithoutDerivedFields {
    key_size: u32,
    default_branch: BranchID,

    br_name_to_br_id: MapxOrdRawKey<BranchID>,
    ver_name_to_ver_id: MapxOrdRawKey<VersionID>,
    br_to_its_vers: MapxOrd<BranchID, MapxOrd<VersionID, ()>>,
    ver_to_change_set: MapxOrd<VersionID, MapxRawMk>,
}

impl From<MapxRawMkVsWithoutDerivedFields> for MapxRawMkVs {
    fn from(m: MapxRawMkVsWithoutDerivedFields) -> Self {
        let br_id_to_br_name = m
            .br_name_to_br_id
            .iter()
            .map(|(n, id)| (id, n))
            .collect::<HashMap<_, _>>();
        let ver_id_to_ver_name = m
            .ver_name_to_ver_id
            .iter()
            .map(|(n, id)| (id, n))
            .collect::<HashMap<_, _>>();
        let layered_kv = m.ver_to_change_set.iter().fold(
            BTreeMap::new(),
            |mut acc, (ver, chgset)| {
                let mut op = |k: &[&[u8]], v: &[u8]| {
                    let k = to_owned_key(k);
                    let v = v.to_vec();
                    #[allow(clippy::unwrap_or_default)]
                    acc.entry(k).or_insert_with(BTreeMap::new).insert(ver, v);
                    Ok(())
                };
                pnk!(chgset.iter_op(&mut op));
                acc
            },
        );

        Self {
            key_size: m.key_size,
            default_branch: m.default_branch,

            br_name_to_br_id: m.br_name_to_br_id,
            ver_name_to_ver_id: m.ver_name_to_ver_id,
            br_to_its_vers: m.br_to_its_vers,
            ver_to_change_set: m.ver_to_change_set,

            br_id_to_br_name: Arc::new(RwLock::new(br_id_to_br_name)),
            ver_id_to_ver_name: Arc::new(RwLock::new(ver_id_to_ver_name)),
            layered_kv: Arc::new(RwLock::new(layered_kv)),
        }
    }
}

impl From<&MapxRawMkVs> for MapxRawMkVsWithoutDerivedFields {
    fn from(m: &MapxRawMkVs) -> Self {
        unsafe {
            Self {
                key_size: m.key_size,
                default_branch: m.default_branch,

                br_name_to_br_id: m.br_name_to_br_id.shadow(),
                ver_name_to_ver_id: m.ver_name_to_ver_id.shadow(),
                br_to_its_vers: m.br_to_its_vers.shadow(),
                ver_to_change_set: m.ver_to_change_set.shadow(),
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

impl MapxRawMkVs {
    #[inline(always)]
    pub(super) unsafe fn shadow(&self) -> Self {
        Self {
            key_size: self.key_size,
            default_branch: self.default_branch,

            br_name_to_br_id: self.br_name_to_br_id.shadow(),
            ver_name_to_ver_id: self.ver_name_to_ver_id.shadow(),
            br_to_its_vers: self.br_to_its_vers.shadow(),
            ver_to_change_set: self.ver_to_change_set.shadow(),

            br_id_to_br_name: Arc::clone(&self.br_id_to_br_name),
            ver_id_to_ver_name: Arc::clone(&self.ver_id_to_ver_name),
            layered_kv: Arc::clone(&self.layered_kv),
        }
    }

    #[inline(always)]
    pub(super) fn new(key_size: u32) -> Self {
        let mut ret = Self {
            key_size,
            default_branch: BranchID::default(),

            br_name_to_br_id: MapxOrdRawKey::new(),
            ver_name_to_ver_id: MapxOrdRawKey::new(),
            br_to_its_vers: MapxOrd::new(),
            ver_to_change_set: MapxOrd::new(),

            br_id_to_br_name: Arc::new(RwLock::new(Default::default())),
            ver_id_to_ver_name: Arc::new(RwLock::new(Default::default())),
            layered_kv: Arc::new(RwLock::new(Default::default())),
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

        self.br_to_its_vers.insert(&initial_brid, &MapxOrd::new());

        self.br_id_to_br_name
            .write()
            .insert(initial_brid, INITIAL_BRANCH_NAME.0.to_vec());
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
        if key.len() < self.key_size as usize {
            return self.batch_remove_by_branch_version(key, ver_id).c(d!());
        };

        let ret = self.get_by_branch_version(key, br_id, ver_id);

        if value.is_none() && ret.is_none() {
            return Ok(None);
        }

        let value = value.unwrap_or(NULL);

        self.ver_to_change_set
            .get_mut(&ver_id)
            .c(d!())?
            .insert(key, value)
            .c(d!())?;

        self.layered_kv
            .write()
            .entry(to_owned_key(key))
            .or_default()
            .insert(ver_id, value.to_vec());

        Ok(ret)
    }

    fn batch_remove_by_branch_version(
        &mut self,
        key: &[&[u8]],
        ver_id: VersionID,
    ) -> Result<Option<RawValue>> {
        let mut hdr = self.ver_to_change_set.get(&ver_id).c(d!())?;
        let hdr_shadow = unsafe { hdr.shadow() };
        let mut op = |k: &[&[u8]], _: &[u8]| hdr.insert(k, &[]).c(d!()).map(|_| ());
        hdr_shadow.iter_op_with_key_prefix(&mut op, key).c(d!())?;

        let key = to_owned_key(key);
        self.layered_kv
            .write()
            .range_mut(key.clone()..)
            .filter(|(k, _)| k.starts_with(&key))
            .for_each(|(_, vers)| {
                vers.insert(ver_id, Vec::new());
            });

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
            .read()
            .get(&to_owned_key(key))?
            .range(..=ver_id)
            .rev()
            .find(|(ver, _)| vers.contains_key(ver))
            .and_then(|(_, value)| alt!(value.is_empty(), None, Some(value.clone())))
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
        let key_prefix = to_owned_key(key_prefix);
        for (k, v) in self
            .layered_kv
            .read()
            .range(key_prefix.clone()..)
            .filter(|(k, _)| k.starts_with(&key_prefix))
        {
            if let Some((_, v)) = v
                .range(..=ver_id)
                .rev()
                .find(|(ver, v)| !v.is_empty() && vers.contains_key(ver))
            {
                op(&k.iter().map(|k| &k[..]).collect::<Vec<_>>(), v.to_vec()).c(d!())?;
            }
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn clear(&mut self) {
        self.br_name_to_br_id.clear();
        self.ver_name_to_ver_id.clear();
        self.br_to_its_vers.clear();
        self.ver_to_change_set.clear();

        self.br_id_to_br_name.write().clear();
        self.ver_id_to_ver_name.write().clear();
        self.layered_kv.write().clear();

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
        self.ver_id_to_ver_name
            .write()
            .insert(ver_id, ver_name.to_vec());
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
        let mut brvers_hdr =
            self.br_to_its_vers.get(&br_id).c(d!("branch not found"))?;
        let mut brvers = brvers_hdr.range(base_version..).map(|(ver, _)| ver);

        if let Some(ver) = brvers.next() {
            if base_version != ver {
                return Err(eg!("base version is not on this branch"));
            }
        } else {
            return Err(eg!("base version is not on this branch"));
        };

        let mut base_ver_chgset = self.ver_to_change_set.get(&base_version).c(d!())?;
        let vers_to_be_merged = brvers.collect::<Vec<_>>();

        let mut trash = vec![];

        {
            let mut lkv_hdr = self.layered_kv.write();
            let mut ver_hdr = self.ver_id_to_ver_name.write();
            for verid in vers_to_be_merged.iter() {
                let chgset = self.ver_to_change_set.remove(verid).c(d!())?;
                let mut chgset_ops = |k: &[&[u8]], v: &[u8]| {
                    let key = to_owned_key(k);
                    let kvers = lkv_hdr.get_mut(&key).c(d!())?;
                    let vv = kvers.remove(verid).c(d!())?;
                    assert_eq!(&vv, v);
                    kvers.insert(base_version, vv);

                    base_ver_chgset.insert(k, v).c(d!())?;
                    Ok(())
                };
                chgset.iter_op(&mut chgset_ops).c(d!())?;
                trash.push(chgset);

                brvers_hdr
                    .remove(verid)
                    .c(d!())
                    .and_then(|_| ver_hdr.remove(verid).c(d!()))
                    .and_then(|vername| {
                        self.ver_name_to_ver_id.remove(&vername).c(d!())
                    })?;
            }
        }

        TRASH_CLEANER.lock().execute(move || {
            trash.into_iter().for_each(|mut cs| {
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
                .map(|(ver, _)| {
                    self.ver_id_to_ver_name.read().get(&ver).unwrap().to_vec()
                })
                .map(VersionNameOwned)
                .collect()
        })
    }

    #[inline(always)]
    pub(super) fn version_list_globally(&self) -> Vec<VersionNameOwned> {
        self.ver_to_change_set
            .iter()
            .map(|(ver, _)| self.ver_id_to_ver_name.read().get(&ver).unwrap().to_vec())
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

        let mut trash = vec![];

        {
            let mut lkv_hdr = self.layered_kv.write();
            let mut ver_hdr = self.ver_id_to_ver_name.write();
            for (ver, chgset) in unsafe { self.ver_to_change_set.shadow() }
                .iter()
                .filter(|(ver, _)| !valid_vers.contains(ver))
            {
                let mut chgset_ops = |k: &[&[u8]], _: &[u8]| {
                    let k = to_owned_key(k);
                    let lkv = lkv_hdr.get_mut(&k).c(d!())?;
                    lkv.remove(&ver).c(d!())?;
                    if lkv.is_empty() {
                        lkv_hdr.remove(&k).c(d!())?;
                    }
                    Ok(())
                };
                chgset.iter_op(&mut chgset_ops).c(d!())?;
                trash.push(chgset);

                self.ver_to_change_set
                    .remove(&ver)
                    .c(d!())
                    .and_then(|_| ver_hdr.remove(&ver).c(d!()))
                    .and_then(|vername| {
                        self.ver_name_to_ver_id.remove(&vername).c(d!())
                    })?;
            }
        }

        TRASH_CLEANER.lock().execute(move || {
            trash.into_iter().for_each(|mut cs| {
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
        let chgset = self.ver_to_change_set.remove(&ver_id).c(d!())?;
        let mut lkv_hdr = self.layered_kv.write();
        let mut chgset_ops = |key: &[&[u8]], _: &[u8]| {
            lkv_hdr
                .get_mut(&to_owned_key(key))
                .c(d!())?
                .remove(&ver_id)
                .c(d!())
                .map(|_| ())
        };
        chgset.iter_op(&mut chgset_ops).c(d!())?;
        drop(lkv_hdr);

        TRASH_CLEANER.lock().execute(move || {
            let mut cs = chgset;
            cs.clear();
        });

        self.br_to_its_vers.values().for_each(|mut vers| {
            vers.remove(&ver_id);
        });

        self.ver_id_to_ver_name
            .write()
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
        let mut ops = |k: &[&[u8]], v: &[u8]| {
            let k = k.iter().flat_map(|k| k.iter()).copied().collect::<Vec<_>>();
            let v = v.to_vec();
            entries.push((k, v));
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
        self.br_id_to_br_name
            .write()
            .insert(br_id, br_name.to_vec());
        self.br_to_its_vers.insert(&br_id, &vers_copied);

        if let Some(vername) = ver_name {
            self.version_create_by_branch(vername, br_id).c(d!())?;
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_exists(&self, br_id: BranchID) -> bool {
        let condition_1 = self.br_id_to_br_name.read().contains_key(&br_id);
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
            .write()
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
        let brs = self
            .br_id_to_br_name
            .read()
            .keys()
            .filter(|brid| !br_ids.contains(brid))
            .copied()
            .collect::<Vec<_>>();
        for brid in brs.into_iter() {
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
            .read()
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

        {
            let mut hdr = self.br_id_to_br_name.write();
            hdr.insert(brid_1, branch_2.to_vec()).c(d!())?;
            hdr.insert(brid_2, branch_1.to_vec()).c(d!())?;
        }

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
        self.version_clean_up_globally()
            .c(d!())
            .and_then(|_| self.do_prune(reserved_ver_num).c(d!()))
    }

    fn do_prune(&mut self, reserved_ver_num: Option<usize>) -> Result<()> {
        // the '1' of this 'add 1' means the never-deleted initial version.
        let reserved_ver_num =
            1 + reserved_ver_num.unwrap_or(RESERVED_VERSION_NUM_DEFAULT);
        if 0 == reserved_ver_num {
            return Err(eg!("reserved version number should NOT be zero"));
        }

        let mut brvers_non_empty = self
            .br_to_its_vers
            .values()
            .filter(|vers| !vers.is_empty())
            .collect::<Vec<_>>();
        alt!(brvers_non_empty.is_empty(), return Ok(()));
        let mut brvers = (0..brvers_non_empty.len())
            .map(|i| brvers_non_empty[i].iter())
            .collect::<Vec<_>>();

        let mut guard = VER_ID_MAX;
        let mut vers_to_be_merged: Vec<VersionID> = vec![];
        'x: loop {
            for (idx, vers) in brvers.iter_mut().enumerate() {
                if let Some((ver, _)) = vers.next() {
                    alt!(0 == idx, guard = ver);
                    alt!(guard != ver, break 'x);
                } else {
                    break 'x;
                }
            }
            assert_ne!(guard, VER_ID_MAX);
            vers_to_be_merged.push(guard);
        }

        let l = vers_to_be_merged.len();
        if l <= reserved_ver_num {
            return Ok(());
        }

        let (vers_to_be_merged, rewrite_ver) = {
            let guard_idx = l - reserved_ver_num;
            (&vers_to_be_merged[1..=guard_idx], &vers_to_be_merged[0])
        };

        let mut rewrite_chgset = self.ver_to_change_set.get(rewrite_ver).c(d!())?;

        for vers in brvers_non_empty.iter_mut() {
            for ver in vers_to_be_merged.iter() {
                vers.remove(ver).c(d!())?;
            }
        }

        {
            let mut chgkeys = HashSet::new();
            let mut lkv_hdr = self.layered_kv.write();
            for ver in vers_to_be_merged.iter() {
                let chgset = self.ver_to_change_set.remove(ver).c(d!())?;
                let mut chgset_ops = |k: &[&[u8]], v: &[u8]| {
                    let key = to_owned_key(k);
                    let kvers = lkv_hdr.get_mut(&key).c(d!())?;
                    let vv = kvers.get(ver).c(d!())?.clone();
                    assert_eq!(&vv, v);
                    kvers.insert(*rewrite_ver, vv);

                    rewrite_chgset.insert(k, v).c(d!())?;
                    chgkeys.insert(key);
                    Ok(())
                };
                chgset.iter_op(&mut chgset_ops).c(d!())?;
            }

            // lowest-level KVs with 'deleted' states should be cleaned up.
            for k in chgkeys.iter() {
                if let Some(vers) = lkv_hdr.get_mut(k.as_slice()) {
                    // A 'NULL' value means 'not exist'.
                    if vers.get(rewrite_ver).c(d!())?.is_empty() {
                        vers.remove(rewrite_ver).c(d!())?;
                        rewrite_chgset
                            .remove(&k.iter().map(|k| &k[..]).collect::<Vec<_>>())
                            .c(d!())?;
                    }
                    if vers.is_empty() {
                        lkv_hdr.remove(k).c(d!())?;
                    }
                }
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

#[inline(always)]
fn to_owned_key(k: &[&[u8]]) -> Vec<RawKey> {
    k.iter().map(|k| k.to_vec()).collect()
}
