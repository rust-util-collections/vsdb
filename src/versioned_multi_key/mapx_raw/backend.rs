use crate::{
    basic::{
        mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey,
        mapx_ord_rawvalue::MapxOrdRawValue,
    },
    basic_multi_key::{mapx_raw::MapxRawMk, mapx_rawkey::MapxRawKeyMk},
    common::{
        ende::encode_optioned_bytes, BranchID, BranchName, RawValue, VersionID,
        VersionName, INITIAL_BRANCH_ID, INITIAL_BRANCH_NAME, INITIAL_VERSION,
        RESERVED_VERSION_NUM_DEFAULT, VSDB,
    },
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashSet};

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct MapxRawMkVs {
    default_branch: BranchID,
    key_size: usize,

    branch_name_to_branch_id: MapxOrdRawKey<BranchID>,
    version_name_to_version_id: MapxOrdRawKey<VersionID>,

    branch_id_to_branch_name: MapxOrdRawValue<BranchID>,
    version_id_to_version_name: MapxOrdRawValue<VersionID>,

    branch_to_its_versions: MapxOrd<BranchID, MapxOrd<VersionID, ()>>,

    version_to_change_set: MapxOrd<VersionID, MapxRawMk>,

    layered_kv: MapxRawKeyMk<MapxOrd<VersionID, Option<RawValue>>>,
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
            branch_id_to_branch_name: MapxOrdRawValue::new(),
            version_id_to_version_name: MapxOrdRawValue::new(),
            branch_to_its_versions: MapxOrd::new(),
            version_to_change_set: MapxOrd::new(),
            layered_kv: MapxRawKeyMk::new(key_size),
        };
        ret.init();
        ret
    }

    #[inline(always)]
    fn init(&mut self) {
        self.default_branch = INITIAL_BRANCH_ID;
        self.branch_name_to_branch_id
            .insert_ref(INITIAL_BRANCH_NAME.0, &INITIAL_BRANCH_ID);
        self.branch_id_to_branch_name
            .insert_ref(&INITIAL_BRANCH_ID, INITIAL_BRANCH_NAME.0);
        self.branch_to_its_versions
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
        self.branch_to_its_versions
            .get(&branch_id)
            .c(d!("branch not found"))?
            .last()
            .c(d!("no version on this branch, create a version first"))
            .and_then(|(version_id, _)| {
                self.insert_by_branch_version(key, value, branch_id, version_id)
                    .c(d!())
            })
    }

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
        self.branch_to_its_versions
            .get(&branch_id)
            .c(d!("branch not found"))?
            .last()
            .c(d!("no version on this branch, create a version first"))
            .and_then(|(version_id, _)| {
                self.remove_by_branch_version(key, branch_id, version_id)
                    .c(d!())
            })
    }

    fn remove_by_branch_version(
        &self,
        key: &[&[u8]],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_branch_version(key, None, branch_id, version_id)
            .c(d!())
    }

    fn write_by_branch_version(
        &self,
        key: &[&[u8]],
        value: Option<&[u8]>,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        if key.len() < self.key_size {
            return self
                .batch_remove_by_branch_version(key, value, version_id)
                .c(d!());
        };

        let ret = self.get_by_branch_version(key, branch_id, version_id);

        if value.is_none() && ret.is_none() {
            return Ok(None);
        }

        self.version_to_change_set
            .get_mut(&version_id)
            .c(d!())?
            .insert(key, &[])
            .c(d!())?;

        self.layered_kv
            .entry_ref(key)
            .or_insert_ref(&MapxOrd::new())
            .c(d!())?
            .insert_ref_encoded_value(&version_id, &encode_optioned_bytes(&value)[..]);

        Ok(ret)
    }

    fn batch_remove_by_branch_version(
        &self,
        key: &[&[u8]],
        value: Option<&[u8]>,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        let hdr = self.version_to_change_set.get_mut(&version_id).c(d!())?;
        let mut op = |k: &[&[u8]], _: &[u8]| hdr.insert(k, &[]).c(d!()).map(|_| ());
        hdr.iter_op_with_key_prefix(&mut op, key).c(d!())?;

        let mut op = |k: &[&[u8]], _: &MapxOrd<VersionID, Option<RawValue>>| {
            self.layered_kv
                .entry_ref(k)
                .or_insert_ref(&MapxOrd::new())
                .c(d!())?
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
        if let Some(vers) = self.branch_to_its_versions.get(&branch_id) {
            if let Some(version_id) = vers.last().map(|(id, _)| id) {
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
        let vers = self.branch_to_its_versions.get(&branch_id)?;
        self.layered_kv
            .get(key)?
            .range(..=version_id)
            .rev()
            .find(|(ver, _)| vers.contains_key(ver))
            .and_then(|(_, value)| value)
    }

    #[inline(always)]
    pub(super) fn clear(&mut self) {
        self.branch_name_to_branch_id.clear();
        self.version_name_to_version_id.clear();
        self.branch_id_to_branch_name.clear();
        self.version_id_to_version_name.clear();
        self.branch_to_its_versions.clear();
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
        if self.version_name_to_version_id.get(version_name).is_some() {
            return Err(eg!("version already exists"));
        }

        let vers = self
            .branch_to_its_versions
            .get_mut(&branch_id)
            .c(d!("branch not found"))?;

        let version_id = VSDB.alloc_version_id();
        vers.insert(version_id, ());

        self.version_name_to_version_id
            .insert_ref(version_name, &version_id);
        self.version_id_to_version_name
            .insert_ref(&version_id, version_name);
        self.version_to_change_set
            .insert(version_id, MapxRawMk::new(self.key_size));

        Ok(())
    }

    #[inline(always)]
    pub(super) fn version_exists(&self, version_id: BranchID) -> bool {
        self.version_exists_on_branch(version_id, self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn version_exists_on_branch(
        &self,
        version_id: VersionID,
        branch_id: BranchID,
    ) -> bool {
        self.branch_to_its_versions
            .get(&branch_id)
            .map(|vers| vers.contains_key(&version_id))
            .unwrap_or(false)
    }

    #[inline(always)]
    pub(super) fn version_pop(&self) -> Result<()> {
        self.version_pop_by_branch(self.branch_get_default())
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn version_pop_by_branch(&self, branch_id: BranchID) -> Result<()> {
        if let Some((version_id, _)) = self
            .branch_to_its_versions
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

    fn version_remove_by_branch(
        &self,
        version_id: VersionID,
        branch_id: BranchID,
    ) -> Result<()> {
        if self
            .branch_to_its_versions
            .get(&branch_id)
            .c(d!("branch not found"))?
            .remove(&version_id)
            .is_none()
        {
            return Err(eg!("version is not on this branch"));
        }
        Ok(())
    }

    #[inline(always)]
    pub(super) unsafe fn version_rebase(&self, base_version: VersionID) -> Result<()> {
        self.version_rebase_by_branch(base_version, self.branch_get_default())
            .c(d!())
    }
    pub(super) unsafe fn version_rebase_by_branch(
        &self,
        base_version: VersionID,
        branch_id: BranchID,
    ) -> Result<()> {
        let vers_hdr = self
            .branch_to_its_versions
            .get(&branch_id)
            .c(d!("branch not found"))?;
        let mut vers = vers_hdr.range(base_version..).map(|(ver, _)| ver);

        if let Some(ver) = vers.next() {
            if base_version != ver {
                return Err(eg!("base version is not on this branch"));
            }
        } else {
            return Err(eg!("base version is not on this branch"));
        };

        let base_ver_chg_set = self.version_to_change_set.get(&base_version).c(d!())?;
        let vers_to_be_merged = vers.collect::<Vec<_>>();

        for verid in vers_to_be_merged.iter() {
            let mut chgset_ops = |k: &[&[u8]], _: &[u8]| {
                base_ver_chg_set.insert(k, &[]).c(d!())?;
                self.layered_kv
                    .get(k)
                    .c(d!())
                    .and_then(|hdr| {
                        hdr.remove(verid)
                            .c(d!())
                            .map(|v| hdr.insert(base_version, v))
                    })
                    .map(|_| ())
            };

            self.version_to_change_set
                .remove(verid)
                .c(d!())?
                .iter_op(&mut chgset_ops)
                .c(d!())?;

            self.version_id_to_version_name
                .remove(verid)
                .c(d!())
                .and_then(|vername| {
                    self.version_name_to_version_id.remove(&vername).c(d!())
                })
                .and_then(|_| vers_hdr.remove(verid).c(d!()))?;
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_create(
        &self,
        branch_name: &[u8],
        version_name: &[u8],
    ) -> Result<()> {
        self.branch_create_by_base_branch(
            branch_name,
            version_name,
            self.branch_get_default(),
        )
        .c(d!())
    }

    #[inline(always)]
    pub(super) fn branch_create_by_base_branch(
        &self,
        branch_name: &[u8],
        version_name: &[u8],
        base_branch_id: BranchID,
    ) -> Result<()> {
        let base_version_id = self
            .branch_to_its_versions
            .get(&base_branch_id)
            .c(d!("base branch not found"))?
            .last()
            .map(|(version_id, _)| version_id)
            .c(d!("base version not found"))?;

        self.branch_create_by_base_branch_version(
            branch_name,
            version_name,
            base_branch_id,
            base_version_id,
        )
        .c(d!())
    }

    #[inline(always)]
    pub(super) fn branch_create_by_base_branch_version(
        &self,
        branch_name: &[u8],
        version_name: &[u8],
        base_branch_id: BranchID,
        base_version_id: VersionID,
    ) -> Result<()> {
        if self.version_name_to_version_id.contains_key(version_name) {
            return Err(eg!("this version already exists"));
        }

        unsafe {
            self.do_branch_create_by_base_branch_version(
                branch_name,
                Some(version_name),
                base_branch_id,
                base_version_id,
            )
        }
    }

    #[inline(always)]
    pub(super) unsafe fn branch_create_without_new_version(
        &self,
        branch_name: &[u8],
    ) -> Result<()> {
        self.branch_create_by_base_branch_without_new_version(
            branch_name,
            self.branch_get_default(),
        )
        .c(d!())
    }

    #[inline(always)]
    pub(super) unsafe fn branch_create_by_base_branch_without_new_version(
        &self,
        branch_name: &[u8],
        base_branch_id: BranchID,
    ) -> Result<()> {
        let base_version_id = self
            .branch_to_its_versions
            .get(&base_branch_id)
            .c(d!("base branch not found"))?
            .last()
            .map(|(version_id, _)| version_id)
            .c(d!("base version not found"))?;

        self.branch_create_by_base_branch_version_without_new_version(
            branch_name,
            base_branch_id,
            base_version_id,
        )
        .c(d!())
    }

    #[inline(always)]
    pub(super) unsafe fn branch_create_by_base_branch_version_without_new_version(
        &self,
        branch_name: &[u8],
        base_branch_id: BranchID,
        base_version_id: VersionID,
    ) -> Result<()> {
        self.do_branch_create_by_base_branch_version(
            branch_name,
            None,
            base_branch_id,
            base_version_id,
        )
    }

    unsafe fn do_branch_create_by_base_branch_version(
        &self,
        branch_name: &[u8],
        version_name: Option<&[u8]>,
        base_branch_id: BranchID,
        base_version_id: VersionID,
    ) -> Result<()> {
        if self.branch_name_to_branch_id.contains_key(branch_name) {
            return Err(eg!("branch already exists"));
        }

        let vers = self
            .branch_to_its_versions
            .get(&base_branch_id)
            .c(d!("base branch not exist"))?;
        if !vers.contains_key(&base_version_id) {
            return Err(eg!("version is not on the base branch"));
        }

        let branch_id = VSDB.alloc_branch_id();

        self.branch_name_to_branch_id
            .insert_ref(branch_name, &branch_id);
        self.branch_id_to_branch_name
            .insert_ref(&branch_id, branch_name);

        let vers_copied =
            vers.range(..=base_version_id)
                .fold(MapxOrd::new(), |acc, (k, v)| {
                    acc.insert(k, v);
                    acc
                });
        self.branch_to_its_versions.insert(branch_id, vers_copied);

        if let Some(vername) = version_name {
            self.version_create_by_branch(vername, branch_id).c(d!())?;
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_exists(&self, branch_id: BranchID) -> bool {
        let condition_1 = self.branch_id_to_branch_name.contains_key(&branch_id);
        let condition_2 = self.branch_to_its_versions.contains_key(&branch_id);
        assert_eq!(condition_1, condition_2);
        condition_1
    }

    #[inline(always)]
    pub(super) fn branch_has_versions(&self, branch_id: BranchID) -> bool {
        self.branch_exists(branch_id) && !self.branch_to_its_versions.is_empty()
    }

    #[inline(always)]
    pub(super) fn branch_remove(&self, branch_id: BranchID) -> Result<()> {
        self.branch_truncate(branch_id).c(d!())?;

        self.branch_id_to_branch_name
            .get(&branch_id)
            .c(d!())
            .and_then(|brname| self.branch_name_to_branch_id.remove(&brname).c(d!()))?;

        self.branch_to_its_versions
            .remove(&branch_id)
            .c(d!())
            .map(|_| ())
    }

    #[inline(always)]
    pub(super) fn branch_truncate(&self, branch_id: BranchID) -> Result<()> {
        self.branch_truncate_to(branch_id, VersionID::MIN).c(d!())
    }

    pub(super) fn branch_truncate_to(
        &self,
        branch_id: BranchID,
        last_version_id: VersionID,
    ) -> Result<()> {
        if let Some(vers) = self.branch_to_its_versions.get(&branch_id) {
            for (version_id, _) in vers.range((1 + last_version_id)..).rev() {
                self.version_remove_by_branch(version_id, branch_id)
                    .c(d!())?;
            }
            Ok(())
        } else {
            Err(eg!("branch not found"))
        }
    }

    #[inline(always)]
    pub(super) fn branch_pop_version(&self, branch_id: BranchID) -> Result<()> {
        self.version_pop_by_branch(branch_id).c(d!())
    }

    #[inline(always)]
    pub(super) fn branch_merge_to(
        &self,
        branch_id: BranchID,
        target_branch_id: BranchID,
    ) -> Result<()> {
        unsafe { self.do_branch_merge_to(branch_id, target_branch_id, false) }
    }

    #[inline(always)]
    pub(super) unsafe fn branch_merge_to_force(
        &self,
        branch_id: BranchID,
        target_branch_id: BranchID,
    ) -> Result<()> {
        self.do_branch_merge_to(branch_id, target_branch_id, true)
    }

    unsafe fn do_branch_merge_to(
        &self,
        branch_id: BranchID,
        target_branch_id: BranchID,
        force: bool,
    ) -> Result<()> {
        let vers = self
            .branch_to_its_versions
            .get(&branch_id)
            .c(d!("branch not found"))?;
        let target_vers = self
            .branch_to_its_versions
            .get(&target_branch_id)
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
                target_vers.insert(ver, ());
            });
        } else if let Some((latest_ver, _)) = vers.last() {
            if let Some((target_latest_ver, _)) = target_vers.last() {
                match latest_ver.cmp(&target_latest_ver) {
                    Ordering::Equal => {
                        return Ok(());
                    }
                    Ordering::Greater => {
                        vers.range((1 + target_latest_ver)..)
                            .map(|(ver, _)| ver)
                            .for_each(|ver| {
                                target_vers.insert(ver, ());
                            });
                    }
                    _ => {}
                }
            } else {
                vers.iter().for_each(|(ver, _)| {
                    target_vers.insert(ver, ());
                });
            }
        } else {
            return Ok(());
        };

        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_set_default(&mut self, branch_id: BranchID) -> Result<()> {
        if !self.branch_exists(branch_id) {
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
        self.version_clean_up_global().c(d!())?;

        let reserved_ver_num = reserved_ver_num.unwrap_or(RESERVED_VERSION_NUM_DEFAULT);
        if 0 == reserved_ver_num {
            return Err(eg!("reserved version number should NOT be zero"));
        }

        let mut br_vers = self
            .branch_to_its_versions
            .iter()
            .map(|(_, vers)| vers.iter())
            .collect::<Vec<_>>();

        let last_idx = br_vers.len().saturating_sub(1);

        let mut guard = 0;
        let mut vers_to_be_merged = vec![];
        'x: loop {
            for (idx, vers) in br_vers.iter_mut().enumerate() {
                if let Some((ver, _)) = vers.next() {
                    alt!(0 == idx, guard = ver);
                    alt!(guard != ver, break 'x);
                    alt!(last_idx == idx, vers_to_be_merged.push(guard));
                } else {
                    break 'x;
                }
            }
        }

        let (vers_to_be_merged, rewrite_ver) = {
            let l = vers_to_be_merged.len();
            if l > reserved_ver_num {
                let guard_idx = l - reserved_ver_num;
                (
                    &vers_to_be_merged[..guard_idx],
                    &vers_to_be_merged[guard_idx],
                )
            } else {
                return Ok(());
            }
        };

        let rewrite_ver_chgset = self.version_to_change_set.get(rewrite_ver).c(d!())?;

        for (_, vers) in self.branch_to_its_versions.iter() {
            for ver in vers_to_be_merged.iter() {
                vers.remove(ver).c(d!())?;
            }
        }

        for ver in vers_to_be_merged.iter() {
            self.version_id_to_version_name
                .remove(ver)
                .c(d!())
                .and_then(|vername| {
                    self.version_name_to_version_id.remove(&vername).c(d!())
                })?;
            let mut chgset_ops = |k: &[&[u8]], _: &[u8]| {
                let k_vers = self.layered_kv.get(k).c(d!())?;
                let value = k_vers.remove(ver).c(d!())?;
                if k_vers.range(..=rewrite_ver).next().is_none() {
                    assert!(rewrite_ver_chgset.insert(k, &[]).c(d!())?.is_none());
                    assert!(k_vers.insert_ref(rewrite_ver, &value).is_none());
                }
                Ok(())
            };
            self.version_to_change_set
                .remove(ver)
                .c(d!())?
                .iter_op(&mut chgset_ops)
                .c(d!())?;
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn get_branch_id(&self, branch_name: BranchName) -> Option<BranchID> {
        self.branch_name_to_branch_id.get(branch_name.0)
    }

    #[inline(always)]
    pub(super) fn get_version_id(&self, version_name: VersionName) -> Option<VersionID> {
        self.version_name_to_version_id.get(version_name.0)
    }

    // clean up all orphaned versions in the global scope
    #[inline(always)]
    fn version_clean_up_global(&self) -> Result<()> {
        let valid_vers = self
            .branch_to_its_versions
            .iter()
            .flat_map(|(_, vers)| vers.iter().map(|(ver, _)| ver))
            .collect::<HashSet<_>>();

        for (ver, chgset) in self
            .version_to_change_set
            .iter()
            .filter(|(ver, _)| !valid_vers.contains(ver))
        {
            let mut chgset_ops = |key: &[&[u8]], _: &[u8]| {
                self.layered_kv
                    .get(key)
                    .c(d!())?
                    .remove(&ver)
                    .c(d!())
                    .map(|_| ())
            };
            chgset.iter_op(&mut chgset_ops).c(d!())?;

            self.version_id_to_version_name
                .remove(&ver)
                .c(d!())
                .and_then(|vername| {
                    self.version_name_to_version_id.remove(&vername).c(d!())
                })
                .and_then(|_| self.version_to_change_set.remove(&ver).c(d!()))?;
        }

        Ok(())
    }
}
