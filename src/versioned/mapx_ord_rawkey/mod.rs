//!
//! NOTE: Documents => [MapxRawVs](crate::versioned::mapx_raw)
//!

// TODO

use crate::{
    common::{
        ende::ValueEnDe, BranchName, ParentBranchName, RawKey, VerChecksum, VersionName,
    },
    versioned::mapx_raw::{MapxRawVs, MapxRawVsIter},
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut, RangeBounds},
};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxOrdRawKeyVs<V>
where
    V: ValueEnDe,
{
    inner: MapxRawVs,
    p: PhantomData<V>,
}

impl<V> Default for MapxOrdRawKeyVs<V>
where
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<V> MapxOrdRawKeyVs<V>
where
    V: ValueEnDe,
{
    #[inline(always)]
    pub fn new() -> Self {
        MapxOrdRawKeyVs {
            inner: MapxRawVs::new(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<V> {
        self.inner
            .get(key)
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn get_le(&self, key: &[u8]) -> Option<(RawKey, V)> {
        self.inner
            .get_le(key)
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn get_ge(&self, key: &[u8]) -> Option<(RawKey, V)> {
        self.inner
            .get_ge(key)
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<ValueMut<'_, V>> {
        self.inner.get(key).map(|v| {
            ValueMut::new(
                self,
                key.to_vec().into_boxed_slice(),
                <V as ValueEnDe>::decode(&v).unwrap(),
            )
        })
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    pub fn insert(&mut self, key: RawKey, value: V) -> Result<Option<V>> {
        self.insert_ref(&key, &value)
    }

    #[inline(always)]
    pub fn insert_ref(&mut self, key: &[u8], value: &V) -> Result<Option<V>> {
        self.inner
            .insert(key, &value.encode())
            .map(|v| v.map(|v| <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn entry(&mut self, key: RawKey) -> Entry<'_, V> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn entry_ref<'a>(&'a mut self, key: &'a [u8]) -> EntryRef<'a, V> {
        EntryRef { key, hdr: self }
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxOrdRawKeyVsIter<'_, V> {
        MapxOrdRawKeyVsIter {
            iter: self.inner.iter(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range<'a, R: 'a + RangeBounds<RawKey>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdRawKeyVsIter<'a, V> {
        MapxOrdRawKeyVsIter {
            iter: self.inner.range(bounds),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_ref<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdRawKeyVsIter<'a, V> {
        MapxOrdRawKeyVsIter {
            iter: self.inner.range_ref(bounds),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first(&self) -> Option<(RawKey, V)> {
        self.iter().next()
    }

    #[inline(always)]
    pub fn last(&self) -> Option<(RawKey, V)> {
        self.iter().next_back()
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.inner.contains_key(key)
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &[u8]) -> Result<Option<V>> {
        self.inner
            .remove(key)
            .map(|v| v.map(|v| <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    crate::impl_vcs_methods!();
}

#[derive(Debug)]
pub struct ValueMut<'a, V>
where
    V: ValueEnDe,
{
    hdr: &'a mut MapxOrdRawKeyVs<V>,
    key: RawKey,
    value: V,
}

impl<'a, V> ValueMut<'a, V>
where
    V: ValueEnDe,
{
    pub(crate) fn new(hdr: &'a mut MapxOrdRawKeyVs<V>, key: RawKey, value: V) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<'a, V> Drop for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        pnk!(self.hdr.insert_ref(&self.key, &self.value));
    }
}

impl<'a, V> Deref for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, V> DerefMut for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub struct Entry<'a, V>
where
    V: 'a + ValueEnDe,
{
    pub(crate) key: RawKey,
    pub(crate) hdr: &'a mut MapxOrdRawKeyVs<V>,
}

impl<'a, V> Entry<'a, V>
where
    V: ValueEnDe,
{
    pub fn or_insert(self, default: V) -> ValueMut<'a, V> {
        if !self.hdr.contains_key(&self.key) {
            pnk!(self.hdr.insert_ref(&self.key, &default));
        }
        pnk!(self.hdr.get_mut(&self.key))
    }
}

pub struct EntryRef<'a, V>
where
    V: ValueEnDe,
{
    key: &'a [u8],
    hdr: &'a mut MapxOrdRawKeyVs<V>,
}

impl<'a, V> EntryRef<'a, V>
where
    V: ValueEnDe,
{
    pub fn or_insert_ref(self, default: &V) -> ValueMut<'a, V> {
        if !self.hdr.contains_key(self.key) {
            pnk!(self.hdr.insert_ref(self.key, default));
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}

pub struct MapxOrdRawKeyVsIter<'a, V>
where
    V: ValueEnDe,
{
    iter: MapxRawVsIter<'a>,
    p: PhantomData<V>,
}

impl<'a, V> Iterator for MapxOrdRawKeyVsIter<'a, V>
where
    V: ValueEnDe,
{
    type Item = (RawKey, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }
}

impl<'a, V> DoubleEndedIterator for MapxOrdRawKeyVsIter<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }
}

impl<'a, V> ExactSizeIterator for MapxOrdRawKeyVsIter<'a, V> where V: ValueEnDe {}

#[macro_export(crate)]
macro_rules! impl_vcs_methods {
    () => {
        #[inline(always)]
        pub fn version_create(&mut self, version_name: VersionName) -> Result<()> {
            self.inner.version_create(version_name).c(d!())
        }

        #[inline(always)]
        pub fn version_create_by_branch(
            &mut self,
            version_name: VersionName,
            branch_name: BranchName,
        ) -> Result<()> {
            self.inner
                .version_create_by_branch(version_name, branch_name)
                .c(d!())
        }

        #[inline(always)]
        pub fn version_exists(&self, version_name: VersionName) -> bool {
            self.inner.version_exists(version_name)
        }

        #[inline(always)]
        pub fn version_exists_on_branch(
            &self,
            version_name: VersionName,
            branch_name: BranchName,
        ) -> bool {
            self.inner
                .version_exists_on_branch(version_name, branch_name)
        }

        #[inline(always)]
        pub fn version_created(&self, version_name: VersionName) -> bool {
            self.inner.version_created(version_name)
        }

        #[inline(always)]
        pub fn version_created_on_branch(
            &self,
            version_name: VersionName,
            branch_name: BranchName,
        ) -> bool {
            self.inner
                .version_created_on_branch(version_name, branch_name)
        }

        #[inline(always)]
        pub fn version_pop(&mut self) -> Result<()> {
            self.inner.version_pop().c(d!())
        }

        #[inline(always)]
        pub fn version_pop_by_branch(&mut self, branch_name: BranchName) -> Result<()> {
            self.inner.version_pop_by_branch(branch_name).c(d!())
        }

        #[inline(always)]
        pub fn branch_create(&mut self, branch_name: BranchName) -> Result<()> {
            self.inner.branch_create(branch_name).c(d!())
        }

        #[inline(always)]
        pub fn branch_create_by_base_branch(
            &mut self,
            branch_name: BranchName,
            base_branch_name: ParentBranchName,
        ) -> Result<()> {
            self.inner
                .branch_create_by_base_branch(branch_name, base_branch_name)
                .c(d!())
        }

        #[inline(always)]
        pub fn branch_exists(&self, branch_name: BranchName) -> bool {
            self.inner.branch_exists(branch_name)
        }

        #[inline(always)]
        pub fn branch_remove(&mut self, branch_name: BranchName) -> Result<()> {
            self.inner.branch_remove(branch_name).c(d!())
        }

        #[inline(always)]
        pub fn branch_truncate(&mut self, branch_name: BranchName) -> Result<()> {
            self.inner.branch_truncate(branch_name).c(d!())
        }

        #[inline(always)]
        pub fn branch_truncate_to(
            &mut self,
            branch_name: BranchName,
            last_version_name: VersionName,
        ) -> Result<()> {
            self.inner
                .branch_truncate_to(branch_name, last_version_name)
                .c(d!())
        }

        #[inline(always)]
        pub fn branch_pop_version(&mut self, branch_name: BranchName) -> Result<()> {
            self.inner.branch_pop_version(branch_name).c(d!())
        }

        #[inline(always)]
        pub fn branch_merge_to_parent(&mut self, branch_name: BranchName) -> Result<()> {
            self.inner.branch_merge_to_parent(branch_name).c(d!())
        }

        #[inline(always)]
        pub fn branch_has_children(&self, branch_name: BranchName) -> bool {
            self.inner.branch_has_children(branch_name)
        }

        #[inline(always)]
        pub fn checksum_get(&self) -> Option<VerChecksum> {
            self.inner.checksum_get()
        }

        #[inline(always)]
        pub fn checksum_get_by_branch(
            &self,
            branch_name: BranchName,
        ) -> Option<VerChecksum> {
            self.inner.checksum_get_by_branch(branch_name)
        }

        #[inline(always)]
        pub fn checksum_get_by_branch_version(
            &self,
            branch_name: BranchName,
            version_name: VersionName,
        ) -> Option<VerChecksum> {
            self.inner
                .checksum_get_by_branch_version(branch_name, version_name)
        }

        #[inline(always)]
        pub fn prune(&mut self, reserved_ver_num: Option<usize>) -> Result<()> {
            self.inner.prune(reserved_ver_num).c(d!())
        }

        #[inline(always)]
        pub fn prune_by_branch(
            &mut self,
            branch_name: BranchName,
            reserved_ver_num: Option<usize>,
        ) -> Result<()> {
            self.inner
                .prune_by_branch(branch_name, reserved_ver_num)
                .c(d!())
        }
    };
}
