//!
//! Core logic of the version managements.
//!

#![allow(unused_variables)]

use crate::{
    basic::mapx_raw::{MapxRaw, MapxRawIter},
    common::{
        BranchID, BranchIDBase, BranchName, BranchNameOwned, RawKey, RawValue,
        VersionID, VersionIDBase, VersionName, VersionNameOwned, INITIAL_BRANCH_ID,
        INITIAL_BRANCH_NAME, NULL, NULL_ID, RESERVED_VERSION_NUM_DEFAULT, TRASH_CLEANER,
        VER_ID_MAX, VSDB,
    },
};
use parking_lot::RwLock;
use ruc::{crypto::trie_root, *};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    mem::size_of,
    ops::{Bound, RangeBounds},
    result::Result as StdResult,
    sync::Arc,
};

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(super) struct MapxRawVs {
    default_branch: BranchID,

    br_name_to_br_id: MapxRaw,   // MapxOrdRawKey<BranchID>,
    ver_name_to_ver_id: MapxRaw, // MapxOrdRawKey<VersionID>,

    // versions on this branch,
    // created dirctly by it or inherited from its ancestors
    br_to_its_vers: MapxRaw, // MapxOrd<BranchID, MapxOrd<VersionID, ()>>,

    // - 'empty value'(&[] ...) means 'not exist'
    // - 'key -> multi-version(globally unique) -> multi-value'
    layered_kv: MapxRaw, // <RawKey, MapxRaw<VersionID, RawValue>>,

    // derived from `br_name_to_br_id` during starting
    br_id_to_br_name: Arc<RwLock<HashMap<BranchID, RawValue>>>,

    // derived from `ver_name_to_ver_id` during starting
    ver_id_to_ver_name: Arc<RwLock<HashMap<VersionID, RawValue>>>,

    // Globally ever changed keys within each version
    // derived from `layered_kv` during the starting process.
    ver_to_change_set: Arc<RwLock<BTreeMap<VersionID, BTreeSet<RawKey>>>>,
}

// !^~^! 撸猫 !^~^!
unsafe impl Send for MapxRawVs {}
unsafe impl Sync for MapxRawVs {}

impl Clone for MapxRawVs {
    fn clone(&self) -> Self {
        Self {
            default_branch: self.default_branch,
            br_name_to_br_id: self.br_name_to_br_id.clone(),
            ver_name_to_ver_id: self.ver_name_to_ver_id.clone(),
            br_to_its_vers: self.br_to_its_vers.clone(),
            layered_kv: self.layered_kv.clone(),
            br_id_to_br_name: Arc::new(RwLock::new(
                self.br_id_to_br_name.read().clone(),
            )),
            ver_id_to_ver_name: Arc::new(RwLock::new(
                self.ver_id_to_ver_name.read().clone(),
            )),
            ver_to_change_set: Arc::new(RwLock::new(
                self.ver_to_change_set.read().clone(),
            )),
        }
    }
}

impl Serialize for MapxRawVs {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        MapxRawVsWithoutDerivedFields::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MapxRawVs {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <MapxRawVsWithoutDerivedFields as Deserialize>::deserialize(deserializer)
            .map(Self::from)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct MapxRawVsWithoutDerivedFields {
    default_branch: BranchID,

    br_name_to_br_id: MapxRaw,   // MapxOrdRawKey<BranchID>,
    ver_name_to_ver_id: MapxRaw, // MapxOrdRawKey<VersionID>,

    // versions on this branch,
    // created dirctly by it or inherited from its ancestors
    br_to_its_vers: MapxRaw, // MapxOrd<BranchID, MapxOrd<VersionID, ()>>,

    layered_kv: MapxRaw, // <RawKey, MapxRaw<VersionID, RawValue>>
}

impl From<MapxRawVsWithoutDerivedFields> for MapxRawVs {
    fn from(m: MapxRawVsWithoutDerivedFields) -> Self {
        let br_id_to_br_name = m
            .br_name_to_br_id
            .iter()
            .map(|(n, id)| (to_brid(&id), n))
            .collect::<HashMap<_, _>>();
        let ver_id_to_ver_name = m
            .ver_name_to_ver_id
            .iter()
            .map(|(n, id)| (to_verid(&id), n))
            .collect::<HashMap<_, _>>();
        let existing_vers =
            m.br_to_its_vers
                .iter()
                .fold(BTreeMap::new(), |mut acc, (_br, vers)| {
                    for (ver, _) in decode_map(vers).iter() {
                        acc.insert(to_verid(&ver), BTreeSet::new());
                    }
                    acc
                });
        let ver_to_change_set =
            m.layered_kv
                .iter()
                .fold(existing_vers, |mut acc, (k, vers)| {
                    for (ver, _) in decode_map(vers).iter() {
                        acc.entry(to_verid(&ver))
                            .or_insert_with(BTreeSet::new)
                            .insert(k.clone());
                    }
                    acc
                });
        Self {
            default_branch: m.default_branch,
            br_name_to_br_id: m.br_name_to_br_id,
            ver_name_to_ver_id: m.ver_name_to_ver_id,
            br_to_its_vers: m.br_to_its_vers,
            layered_kv: m.layered_kv,
            br_id_to_br_name: Arc::new(RwLock::new(br_id_to_br_name)),
            ver_id_to_ver_name: Arc::new(RwLock::new(ver_id_to_ver_name)),
            ver_to_change_set: Arc::new(RwLock::new(ver_to_change_set)),
        }
    }
}

impl From<&MapxRawVs> for MapxRawVsWithoutDerivedFields {
    fn from(m: &MapxRawVs) -> Self {
        unsafe {
            Self {
                default_branch: m.default_branch,
                br_name_to_br_id: m.br_name_to_br_id.shadow(),
                ver_name_to_ver_id: m.ver_name_to_ver_id.shadow(),
                br_to_its_vers: m.br_to_its_vers.shadow(),
                layered_kv: m.layered_kv.shadow(),
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

impl MapxRawVs {
    #[inline(always)]
    pub(super) unsafe fn shadow(&self) -> Self {
        Self {
            default_branch: self.default_branch,
            br_name_to_br_id: self.br_name_to_br_id.shadow(),
            ver_name_to_ver_id: self.ver_name_to_ver_id.shadow(),
            br_to_its_vers: self.br_to_its_vers.shadow(),
            layered_kv: self.layered_kv.shadow(),
            br_id_to_br_name: Arc::clone(&self.br_id_to_br_name),
            ver_id_to_ver_name: Arc::clone(&self.ver_id_to_ver_name),
            ver_to_change_set: Arc::clone(&self.ver_to_change_set),
        }
    }

    #[inline(always)]
    pub(super) fn new() -> Self {
        let mut ret = Self {
            default_branch: BranchID::default(),

            br_name_to_br_id: MapxRaw::new(),
            ver_name_to_ver_id: MapxRaw::new(),
            br_to_its_vers: MapxRaw::new(),
            layered_kv: MapxRaw::new(),

            br_id_to_br_name: Arc::new(RwLock::new(Default::default())),
            ver_id_to_ver_name: Arc::new(RwLock::new(Default::default())),
            ver_to_change_set: Arc::new(RwLock::new(Default::default())),
        };

        ret.init();
        ret
    }

    #[inline(always)]
    fn init(&mut self) {
        let initial_brid = INITIAL_BRANCH_ID.to_be_bytes();

        self.default_branch = initial_brid;
        self.br_name_to_br_id
            .insert(INITIAL_BRANCH_NAME.0, &initial_brid[..]);
        self.br_to_its_vers
            .insert(&initial_brid[..], encode_map(&MapxRaw::new()));
        self.br_id_to_br_name
            .write()
            .insert(initial_brid, INITIAL_BRANCH_NAME.0.to_vec());
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
        br_id: BranchID,
    ) -> Result<Option<RawValue>> {
        decode_map(
            self.br_to_its_vers
                .get(&br_id[..])
                .c(d!("branch not found"))?,
        )
        .last()
        .c(d!("no version on this branch, create a version first"))
        .and_then(|(ver_id, _)| {
            self.insert_by_branch_version(key, value, br_id, to_verid(&ver_id))
                .c(d!())
        })
    }

    // This function should **NOT** be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    #[inline(always)]
    fn insert_by_branch_version(
        &mut self,
        key: &[u8],
        value: &[u8],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_branch_version(key, Some(value), br_id, ver_id)
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
        br_id: BranchID,
    ) -> Result<Option<RawValue>> {
        decode_map(self.br_to_its_vers.get(br_id).c(d!("branch not found"))?)
            .last()
            .c(d!("no version on this branch, create a version first"))
            .and_then(|(ver_id, _)| {
                self.remove_by_branch_version(key, br_id, to_verid(&ver_id))
                    .c(d!())
            })
    }

    // This function should **NOT** be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    //
    // The `remove` is essentially assign a `None` value to the key.
    fn remove_by_branch_version(
        &mut self,
        key: &[u8],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_branch_version(key, None, br_id, ver_id)
            .c(d!())
    }

    // This function should **NOT** be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    fn write_by_branch_version(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Result<Option<RawValue>> {
        // clone it, keep a copy of the original unchanged value.
        let ret = self.get_by_branch_version(key, br_id, ver_id);

        // remove a non-existing value
        if value.is_none() && ret.is_none() {
            return Ok(None);
        }

        let value = value.unwrap_or(NULL);

        decode_map(
            &*self
                .layered_kv
                .entry(key)
                .or_insert(encode_map(&MapxRaw::new())),
        )
        .insert(ver_id, value);

        self.ver_to_change_set
            .write()
            .get_mut(&ver_id)
            .c(d!())?
            .insert(key.to_vec());

        Ok(ret)
    }

    #[inline(always)]
    pub(super) fn get(&self, key: &[u8]) -> Option<RawValue> {
        self.get_by_branch(key, self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn get_by_branch(&self, key: &[u8], br_id: BranchID) -> Option<RawValue> {
        if let Some(vers) = self.br_to_its_vers.get(br_id) {
            if let Some(ver_id) = decode_map(vers).last().map(|(id, _)| id) {
                return self.get_by_branch_version(key, br_id, to_verid(&ver_id));
            }
        }
        None
    }

    #[inline(always)]
    pub(super) fn get_by_branch_version(
        &self,
        key: &[u8],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Option<RawValue> {
        let vers = decode_map(self.br_to_its_vers.get(br_id)?);

        decode_map(self.layered_kv.get(key)?)
            .range(..=Cow::Borrowed(&ver_id[..]))
            .rev()
            .find(|(ver, _)| vers.contains_key(ver))
            .and_then(|(_, value)| alt!(value.is_empty(), None, Some(value)))
    }

    #[inline(always)]
    pub(super) fn get_ge(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range(Cow::Borrowed(key)..).next()
    }

    #[inline(always)]
    pub(super) fn get_ge_by_branch(
        &self,
        key: &[u8],
        br_id: BranchID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_by_branch(br_id, Cow::Borrowed(key)..).next()
    }

    #[inline(always)]
    pub(super) fn get_ge_by_branch_version(
        &self,
        key: &[u8],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_by_branch_version(br_id, ver_id, Cow::Borrowed(key)..)
            .next()
    }

    #[inline(always)]
    pub(super) fn get_le(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range(..=Cow::Borrowed(key)).next_back()
    }

    #[inline(always)]
    pub(super) fn get_le_by_branch(
        &self,
        key: &[u8],
        br_id: BranchID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_by_branch(br_id, ..=Cow::Borrowed(key))
            .next_back()
    }

    #[inline(always)]
    pub(super) fn get_le_by_branch_version(
        &self,
        key: &[u8],
        br_id: BranchID,
        ver_id: VersionID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_by_branch_version(br_id, ver_id, ..=Cow::Borrowed(key))
            .next_back()
    }

    #[inline(always)]
    pub(super) fn iter(&self) -> MapxRawVsIter {
        self.iter_by_branch(self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn iter_by_branch(&self, br_id: BranchID) -> MapxRawVsIter {
        if let Some(vers) = self.br_to_its_vers.get(br_id) {
            if let Some((ver_id, _)) = decode_map(vers).last() {
                return self.iter_by_branch_version(to_brid(&br_id), to_verid(&ver_id));
            }
        }

        MapxRawVsIter {
            hdr: self,
            iter: self.layered_kv.range(..),
            br_id: NULL_ID,
            ver_id: NULL_ID,
        }
    }

    #[inline(always)]
    pub(super) fn iter_by_branch_version(
        &self,
        br_id: BranchID,
        ver_id: VersionID,
    ) -> MapxRawVsIter {
        MapxRawVsIter {
            hdr: self,
            iter: self.layered_kv.range(..),
            br_id,
            ver_id,
        }
    }

    #[inline(always)]
    pub(super) fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        self.range_by_branch(self.branch_get_default(), bounds)
    }

    #[inline(always)]
    pub(super) fn range_by_branch<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        br_id: BranchID,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        if let Some(vers) = self.br_to_its_vers.get(br_id) {
            if let Some((ver_id, _)) = decode_map(vers).last() {
                return self.range_by_branch_version(br_id, to_verid(&ver_id), bounds);
            }
        }

        // An empty `Iter`
        MapxRawVsIter {
            hdr: self,
            iter: self.layered_kv.range(..),
            br_id,
            ver_id: NULL_ID,
        }
    }

    #[inline(always)]
    pub(super) fn range_by_branch_version<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        br_id: BranchID,
        ver_id: VersionID,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        let l = match bounds.start_bound() {
            Bound::Included(v) => Bound::Included(Cow::Owned(v.to_vec())),
            Bound::Excluded(v) => Bound::Excluded(Cow::Owned(v.to_vec())),
            Bound::Unbounded => Bound::Unbounded,
        };

        let h = match bounds.end_bound() {
            Bound::Included(v) => Bound::Included(Cow::Owned(v.to_vec())),
            Bound::Excluded(v) => Bound::Excluded(Cow::Owned(v.to_vec())),
            Bound::Unbounded => Bound::Unbounded,
        };

        MapxRawVsIter {
            hdr: self,
            iter: self.layered_kv.range((l, h)),
            br_id,
            ver_id,
        }
    }

    // NOTE: just a stupid O(n) counter, very slow!
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        self.iter().count()
    }

    // NOTE: just a stupid O(n) counter, very slow!
    #[inline(always)]
    pub(super) fn len_by_branch(&self, br_id: BranchID) -> usize {
        self.iter_by_branch(br_id).count()
    }

    // NOTE: just a stupid O(n) counter, very slow!
    #[inline(always)]
    pub(super) fn len_by_branch_version(
        &self,
        br_id: BranchID,
        ver_id: VersionID,
    ) -> usize {
        self.iter_by_branch_version(br_id, ver_id).count()
    }

    // Clear all data, for testing purpose.
    #[inline(always)]
    pub(super) fn clear(&mut self) {
        self.br_name_to_br_id.clear();
        self.ver_name_to_ver_id.clear();
        self.br_to_its_vers.clear();
        self.layered_kv.clear();

        self.br_id_to_br_name.write().clear();
        self.ver_id_to_ver_name.write().clear();
        self.ver_to_change_set.write().clear();

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

        let mut vers = decode_map(
            &*self
                .br_to_its_vers
                .get_mut(br_id)
                .c(d!("branch not found"))?,
        );

        let ver_id = VSDB.alloc_ver_id().to_be_bytes();
        vers.insert(ver_id, []);

        self.ver_name_to_ver_id.insert(ver_name, ver_id);
        self.ver_id_to_ver_name
            .write()
            .insert(ver_id, ver_name.to_vec());
        self.ver_to_change_set
            .write()
            .insert(ver_id, BTreeSet::new());

        Ok(())
    }

    // Check if a verison exists on the default branch
    #[inline(always)]
    pub(super) fn version_exists(&self, ver_id: BranchID) -> bool {
        self.version_exists_on_branch(ver_id, self.branch_get_default())
    }

    // Check if a verison exists in the global scope
    #[inline(always)]
    pub(super) fn version_exists_globally(&self, ver_id: BranchID) -> bool {
        self.ver_to_change_set.read().contains_key(&ver_id)
    }

    // Check if a version exists on a specified branch
    #[inline(always)]
    pub(super) fn version_exists_on_branch(
        &self,
        ver_id: VersionID,
        br_id: BranchID,
    ) -> bool {
        self.br_to_its_vers
            .get(br_id)
            .map(|vers| decode_map(vers).contains_key(ver_id))
            .unwrap_or(false)
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
    pub(super) fn version_pop_by_branch(&mut self, br_id: BranchID) -> Result<()> {
        let mut vers =
            decode_map(self.br_to_its_vers.get(br_id).c(d!("branch not found"))?);

        if let Some((ver_id, _)) = vers.last() {
            vers.remove(&ver_id)
                .c(d!("BUG: version is not on this branch"))?;
        }

        Ok(())
    }

    // # Safety
    //
    // It's the caller's duty to ensure that
    // the `base_version` was created directly by the `br_id`,
    // and versions newer than the `base_version` are not used by any other branches,
    // or the data records of other branches may be corrupted.
    #[inline(always)]
    pub(super) unsafe fn version_rebase(
        &mut self,
        base_version: VersionID,
    ) -> Result<()> {
        self.version_rebase_by_branch(base_version, self.branch_get_default())
            .c(d!())
    }

    // # Safety
    //
    // It's the caller's duty to ensure that
    // the `base_version` was created directly by the `br_id`,
    // and versions newer than the `base_version` are not used by any other branches,
    // or the data records of other branches may be corrupted.
    pub(super) unsafe fn version_rebase_by_branch(
        &mut self,
        base_version: VersionID,
        br_id: BranchID,
    ) -> Result<()> {
        let mut brvers_hdr =
            decode_map(self.br_to_its_vers.get(br_id).c(d!("branch not found"))?);
        let mut brvers = brvers_hdr
            .range(Cow::Borrowed(&base_version[..])..)
            .map(|(ver, _)| to_verid(&ver[..]));

        if let Some(ver) = brvers.next() {
            if base_version != ver {
                return Err(eg!("base version is not on this branch"));
            }
        } else {
            return Err(eg!("base version is not on this branch"));
        };

        let vers_to_be_merged = brvers.collect::<Vec<_>>();

        let mut ver_hdr = self.ver_id_to_ver_name.write();
        let mut chgset_hdr = self.ver_to_change_set.write();

        for verid in vers_to_be_merged.iter() {
            let chgset = chgset_hdr.remove(verid).c(d!())?;
            for k in chgset.iter() {
                chgset_hdr.get_mut(&base_version).c(d!())?.insert(k.clone());
                let mut kvers = decode_map(&*self.layered_kv.get_mut(k).c(d!())?);
                let v = kvers.remove(verid).c(d!())?;
                kvers.insert(base_version, v);
            }
            brvers_hdr
                .remove(verid)
                .c(d!())
                .and_then(|_| ver_hdr.remove(verid).c(d!()))
                .and_then(|vername| self.ver_name_to_ver_id.remove(&vername).c(d!()))?;
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn version_get_id_by_name(
        &self,
        ver_name: VersionName,
    ) -> Option<VersionID> {
        self.ver_name_to_ver_id
            .get(ver_name.0)
            .map(|bytes| to_verid(&bytes))
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
        self.br_to_its_vers.get(br_id).c(d!()).map(|vers| {
            let ver_hdr = self.ver_id_to_ver_name.read();
            decode_map(vers)
                .iter()
                .map(|(ver, _)| ver_hdr.get(&to_verid(&ver)).unwrap().to_vec())
                .map(VersionNameOwned)
                .collect()
        })
    }

    #[inline(always)]
    pub(super) fn version_list_globally(&self) -> Vec<VersionNameOwned> {
        let ver_hdr = self.ver_id_to_ver_name.read();
        self.ver_to_change_set
            .read()
            .iter()
            .map(|(ver, _)| ver_hdr.get(ver).unwrap().to_vec())
            .map(VersionNameOwned)
            .collect()
    }

    #[inline(always)]
    pub(super) fn version_has_change_set(&self, ver_id: VersionID) -> Result<bool> {
        self.ver_to_change_set
            .read()
            .get(&ver_id)
            .c(d!())
            .map(|chgset| !chgset.is_empty())
    }

    /***
     * Clean up orphan instances globally.
     */
    #[inline(always)]
    pub(super) fn version_clean_up_globally(&mut self) -> Result<()> {
        self.do_prune(None, true).c(d!())
    }

    // # Safety
    //
    // Version itself and its corresponding changes will be completely purged from all branches
    pub(super) unsafe fn version_revert_globally(
        &mut self,
        ver_id: VersionID,
    ) -> Result<()> {
        let mut ver_hdr = self.ver_id_to_ver_name.write();

        for key in self
            .ver_to_change_set
            .write()
            .remove(&ver_id)
            .c(d!())?
            .iter()
        {
            decode_map(&*self.layered_kv.get_mut(key).c(d!())?)
                .remove(ver_id)
                .c(d!())?;
        }

        self.br_to_its_vers.iter().for_each(|(_, vers)| {
            decode_map(vers).remove(ver_id);
        });

        ver_hdr
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
            let v = decode_map(self.br_to_its_vers.get(br).c(d!("branch not found"))?)
                .last()
                .map(|(verid, _)| verid)
                .c(d!("version not found"))?;
            let mut ver = VersionID::default();
            ver.copy_from_slice(&v);
            ver
        };

        let chgset_hdr = self.ver_to_change_set.read();
        let chgset = chgset_hdr.get(&ver).c(d!())?;
        let entries = chgset
            .iter()
            .map(|k| {
                let kvers = decode_map(pnk!(self.layered_kv.get(k)));
                let v = pnk!(kvers.get(ver));
                (k.clone(), v)
            })
            .collect::<Vec<_>>();

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

        let base_ver_id = decode_map(
            self.br_to_its_vers
                .get(base_br_id)
                .c(d!("base branch not found"))?,
        )
        .last()
        .map(|(ver_id, _)| ver_id);

        unsafe {
            self.do_branch_create_by_base_branch_version(
                br_name,
                Some(ver_name),
                base_br_id,
                base_ver_id.map(|bytes| to_verid(&bytes)),
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
        let base_ver_id = decode_map(
            self.br_to_its_vers
                .get(base_br_id)
                .c(d!("base branch not found"))?,
        )
        .last()
        .map(|(ver_id, _)| ver_id);

        self.do_branch_create_by_base_branch_version(
            br_name,
            None,
            base_br_id,
            base_ver_id.map(|bytes| to_verid(&bytes)),
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

    // param 'force':
    // remove the target new branch if it exists
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
                self.branch_remove(to_brid(&brid)).c(d!())?;
            }
        }

        if self.br_name_to_br_id.contains_key(br_name) {
            return Err(eg!("branch already exists"));
        }

        let vers = decode_map(
            self.br_to_its_vers
                .get(base_br_id)
                .c(d!("base branch not exist"))?,
        );

        let vers_copied = if let Some(bv) = base_ver_id {
            if !vers.contains_key(bv) {
                return Err(eg!("version is not on the base branch"));
            }
            vers.range(..=Cow::Borrowed(&bv[..])).fold(
                MapxRaw::new(),
                |mut acc, (k, v)| {
                    acc.insert(&k, &v);
                    acc
                },
            )
        } else {
            MapxRaw::new()
        };

        let br_id = VSDB.alloc_br_id().to_be_bytes();

        self.br_name_to_br_id.insert(br_name, br_id);
        self.br_id_to_br_name
            .write()
            .insert(br_id, br_name.to_vec());
        self.br_to_its_vers.insert(br_id, encode_map(&vers_copied));

        if let Some(vername) = ver_name {
            // create the first version of the new branch
            self.version_create_by_branch(vername, br_id).c(d!())?;
        }

        Ok(())
    }

    // Check if a branch exists or not.
    #[inline(always)]
    pub(super) fn branch_exists(&self, br_id: BranchID) -> bool {
        self.br_id_to_br_name.read().contains_key(&br_id)
    }

    // Check if a branch exists and has versions on it.
    #[inline(always)]
    pub(super) fn branch_has_versions(&self, br_id: BranchID) -> bool {
        self.branch_exists(br_id)
            && self
                .br_to_its_vers
                .get(br_id)
                .map(|vers| !decode_map(vers).is_empty())
                .unwrap_or(false)
    }

    // Remove all changes directly made by this branch, and delete the branch itself.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn branch_remove(&mut self, br_id: BranchID) -> Result<()> {
        self.branch_truncate(br_id).c(d!())?;

        self.br_id_to_br_name
            .write()
            .remove(&br_id)
            .c(d!())
            .and_then(|brname| self.br_name_to_br_id.remove(&brname).c(d!()))?;

        let vers = self.br_to_its_vers.remove(br_id).c(d!())?;

        TRASH_CLEANER.lock().execute(move || {
            decode_map(vers).clear();
        });

        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_keep_only(&mut self, br_ids: &[BranchID]) -> Result<()> {
        let brs = self
            .br_id_to_br_name
            .read()
            .iter()
            .map(|(brid, _)| brid)
            .filter(|brid| !br_ids.contains(brid))
            .copied()
            .collect::<Vec<_>>();
        let brs = brs;
        for brid in brs.into_iter() {
            self.branch_remove(brid).c(d!())?;
        }

        self.version_clean_up_globally().c(d!())
    }

    // Remove all changes directly made by this branch, but keep its meta infomation.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn branch_truncate(&mut self, br_id: BranchID) -> Result<()> {
        if let Some(vers) = self.br_to_its_vers.get(br_id) {
            decode_map(vers).clear();
            Ok(())
        } else {
            Err(eg!(
                "branch not found: {}",
                BranchIDBase::from_be_bytes(br_id)
            ))
        }
    }

    // Remove all changes directly made by versions(bigger than `last_ver_id`) of this branch.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    pub(super) fn branch_truncate_to(
        &mut self,
        br_id: BranchID,
        last_ver_id: VersionID,
    ) -> Result<()> {
        if let Some(vers) = self.br_to_its_vers.get(br_id) {
            // version id must be in descending order
            let mut vers = decode_map(vers);
            let vers_shadow = unsafe { vers.shadow() };
            for (ver_id, _) in
                vers_shadow
                    .range(
                        Cow::Borrowed(
                            &(VersionIDBase::from_be_bytes(last_ver_id) + 1)
                                .to_be_bytes()[..],
                        )..,
                    )
                    .rev()
            {
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

    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
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

    // # Safety
    //
    // If new different versions have been created on the target branch,
    // the data records referenced by other branches may be corrupted.
    #[inline(always)]
    pub(super) unsafe fn branch_merge_to_force(
        &mut self,
        br_id: BranchID,
        target_br_id: BranchID,
    ) -> Result<()> {
        self.do_branch_merge_to(br_id, target_br_id, true)
    }

    // Merge a branch into another,
    // even if new different versions have been created on the target branch.
    //
    // # Safety
    //
    // If new different versions have been created on the target branch,
    // the data records referenced by other branches may be corrupted.
    unsafe fn do_branch_merge_to(
        &mut self,
        br_id: BranchID,
        target_br_id: BranchID,
        force: bool,
    ) -> Result<()> {
        let vers = decode_map(self.br_to_its_vers.get(br_id).c(d!("branch not found"))?);
        let mut target_vers = decode_map(
            self.br_to_its_vers
                .get(target_br_id)
                .c(d!("target branch not found"))?,
        );

        if !force {
            if let Some((ver, _)) = target_vers.last() {
                if !vers.contains_key(ver) {
                    // Some new versions have been generated on the target branch
                    return Err(eg!("unable to merge safely"));
                }
            }
        }

        if let Some(fork_point) = vers
            .iter()
            .zip(target_vers.iter())
            .find(|(a, b)| a.0 != b.0)
        {
            vers.range(Cow::Borrowed(&fork_point.0.0[..])..)
                .for_each(|(ver, _)| {
                    target_vers.insert(&ver, []);
                });
        } else if let Some((latest_ver, _)) = vers.last() {
            if let Some((target_latest_ver, _)) = target_vers.last() {
                match latest_ver.cmp(&target_latest_ver) {
                    Ordering::Equal => {
                        // no differences between the two branches
                        return Ok(());
                    }
                    Ordering::Greater => {
                        vers.range(
                            Cow::Borrowed(
                                &(VersionIDBase::from_be_bytes(to_verid(
                                    &target_latest_ver,
                                )) + 1)
                                    .to_be_bytes()[..],
                            )..,
                        )
                        .map(|(ver, _)| ver)
                        .for_each(|ver| {
                            target_vers.insert(&ver, []);
                        });
                    }
                    _ => {}
                }
            } else {
                // target branch is empty, copy all versions to it
                vers.iter().for_each(|(ver, _)| {
                    target_vers.insert(&ver, []);
                });
            }
        } else {
            // nothing to be merges
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
        self.br_to_its_vers.get(br_id).c(d!()).map(|vers| {
            decode_map(vers)
                .iter()
                .all(|(ver, _)| !self.version_has_change_set(to_verid(&ver)).unwrap())
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
        let mut br_hdr = self.br_id_to_br_name.write();

        let brid_1 = to_brid(&self.br_name_to_br_id.get(branch_1).c(d!())?);
        let brid_2 = to_brid(&self.br_name_to_br_id.get(branch_2).c(d!())?);

        self.br_name_to_br_id.insert(branch_1, brid_2).c(d!())?;
        self.br_name_to_br_id.insert(branch_2, brid_1).c(d!())?;

        br_hdr.insert(brid_1, branch_2.to_vec()).c(d!())?;
        br_hdr.insert(brid_2, branch_1.to_vec()).c(d!())?;

        if self.default_branch == brid_1 {
            self.default_branch = brid_2;
        } else if self.default_branch == brid_2 {
            self.default_branch = brid_1;
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_get_id_by_name(&self, br_name: BranchName) -> Option<BranchID> {
        self.br_name_to_br_id
            .get(br_name.0)
            .map(|bytes| to_brid(&bytes))
    }

    #[inline(always)]
    pub(super) fn prune(&mut self, reserved_ver_num: Option<usize>) -> Result<()> {
        self.do_prune(reserved_ver_num, false).c(d!())
    }

    // The oldest version will be kept as the final data container.
    //
    // NOTE: As it will become bigger and bigger,
    // if we migrate the its data to other vesions when pruning,
    // the 'prune' process will be slower and slower,
    // so we should not do that.
    fn do_prune(
        &mut self,
        reserved_ver_num: Option<usize>,
        clean_only: bool,
    ) -> Result<()> {
        /////////////////////////////////////////////////////////////////////
        let (mut ver_hdr, mut chgset_hdr) = if 0 == rand::random::<u32>() % 16 {
            (
                self.ver_id_to_ver_name.write(),
                self.ver_to_change_set.write(),
            )
        } else {
            (
                self.ver_id_to_ver_name.try_write().c(d!("lock busy"))?,
                self.ver_to_change_set.try_write().c(d!("lock busy"))?,
            )
        };
        /////////////////////////////////////////////////////////////////////

        let mut valid_vers = HashSet::new();
        self.br_to_its_vers.iter().for_each(|(_, vers)| {
            decode_map(vers).iter().for_each(|(ver, _)| {
                valid_vers.insert(to_verid(&ver[..]));
            })
        });

        let mut orphanvers = vec![];
        for (ver, chgset) in chgset_hdr
            .iter()
            .filter(|(ver, _)| !valid_vers.contains(&ver[..]))
        {
            for k in chgset.iter() {
                let mut lkv = decode_map(self.layered_kv.get(k).c(d!())?);
                lkv.remove(ver).c(d!())?;
                if lkv.is_empty() {
                    self.layered_kv.remove(k).c(d!())?;
                }
            }
            orphanvers.push(*ver);
        }

        for ver in orphanvers.iter() {
            chgset_hdr
                .remove(ver)
                .c(d!())
                .and_then(|_| ver_hdr.remove(ver).c(d!()))
                .and_then(|vername| self.ver_name_to_ver_id.remove(&vername).c(d!()))?;
        }

        if clean_only {
            return Ok(());
        }

        // the '1' of this 'add 1' means the never-deleted initial version.
        let reserved_ver_num =
            1 + reserved_ver_num.unwrap_or(RESERVED_VERSION_NUM_DEFAULT);
        if 0 == reserved_ver_num {
            return Err(eg!("reserved version number should NOT be zero"));
        }

        let mut brvers_non_empty = self
            .br_to_its_vers
            .iter()
            .map(|(_, vers)| decode_map(vers))
            .filter(|vers| !vers.is_empty())
            .collect::<Vec<_>>();
        alt!(brvers_non_empty.is_empty(), return Ok(()));
        let mut brvers = (0..brvers_non_empty.len())
            .map(|i| brvers_non_empty[i].iter())
            .collect::<Vec<_>>();

        // filter out the longest common prefix
        let mut guard = VER_ID_MAX;
        let mut vers_to_be_merged: Vec<VersionID> = vec![];
        'x: loop {
            for (idx, vers) in brvers.iter_mut().enumerate() {
                if let Some((ver, _)) = vers.next() {
                    alt!(0 == idx, guard = to_verid(&ver));
                    alt!(guard[..] != ver[..], break 'x);
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

        for vers in brvers_non_empty.iter_mut() {
            for ver in vers_to_be_merged.iter() {
                vers.remove(ver).c(d!())?;
            }
        }

        let mut kvchgs = HashSet::new();
        let mut rewrite_chgset = HashSet::new();

        for ver in vers_to_be_merged.iter() {
            let chgset = chgset_hdr.remove(ver).c(d!())?;
            for k in chgset.iter() {
                let mut kvers = decode_map(self.layered_kv.get(k).c(d!())?);
                let v = kvers.remove(ver).c(d!())?;
                kvers.insert(rewrite_ver, v);

                rewrite_chgset.insert(k.clone());
                kvchgs.insert(k.clone());
            }
        }

        let rewrite_chgset_hdr = chgset_hdr.get_mut(&to_verid(rewrite_ver)).c(d!())?;

        rewrite_chgset.into_iter().for_each(|k| {
            rewrite_chgset_hdr.insert(k);
        });

        // lowest-level KVs with 'deleted' states should be cleaned up.
        let mut empty_keys = vec![];
        for k in kvchgs.iter() {
            if let Some(vers) = self.layered_kv.get(k) {
                let mut vers = decode_map(vers);
                // A 'NULL' value means 'not exist'.
                if vers.get(rewrite_ver).c(d!())?.is_empty() {
                    vers.remove(rewrite_ver).c(d!())?;
                    rewrite_chgset_hdr.remove(k);
                }
                if vers.is_empty() {
                    empty_keys.push(k);
                }
            }
        }

        for k in empty_keys.iter() {
            self.layered_kv.remove(k).c(d!())?;
        }

        Ok(())
    }
}

impl Default for MapxRawVs {
    fn default() -> Self {
        Self::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
#[allow(dead_code)]
pub struct MapxRawVsIter<'a> {
    hdr: &'a MapxRawVs,
    iter: MapxRawIter<'a>,
    br_id: BranchID,
    ver_id: VersionID,
}

impl<'a> Iterator for MapxRawVsIter<'a> {
    type Item = (RawKey, RawValue);

    fn next(&mut self) -> Option<Self::Item> {
        if NULL_ID == self.br_id || NULL_ID == self.ver_id {
            return None;
        }

        loop {
            if let Some((k, _)) = self.iter.next() {
                if let Some(v) =
                    self.hdr.get_by_branch_version(&k, self.br_id, self.ver_id)
                {
                    return Some((k.clone(), v));
                }
                // else { continue }
            } else {
                return None;
            }
        }
    }
}

impl DoubleEndedIterator for MapxRawVsIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if NULL_ID == self.br_id || NULL_ID == self.ver_id {
            return None;
        }

        loop {
            if let Some((k, _)) = self.iter.next_back() {
                if let Some(v) =
                    self.hdr.get_by_branch_version(&k, self.br_id, self.ver_id)
                {
                    return Some((k.clone(), v));
                }
                // else { continue }
            } else {
                return None;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[inline(always)]
fn encode_map(m: &MapxRaw) -> &[u8] {
    m.as_prefix_slice()
}

#[inline(always)]
fn decode_map(v: impl AsRef<[u8]>) -> MapxRaw {
    unsafe { MapxRaw::from_slice(v.as_ref()) }
}

#[inline(always)]
fn to_brid(bytes: &[u8]) -> BranchID {
    <[u8; size_of::<BranchID>()]>::try_from(bytes).unwrap()
}

#[inline(always)]
fn to_verid(bytes: &[u8]) -> VersionID {
    <[u8; size_of::<VersionID>()]>::try_from(bytes).unwrap()
}
