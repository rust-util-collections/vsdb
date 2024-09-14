//!
//! Documents => [MapxRawVs](crate::versioned::mapx_raw)
//!
#[cfg(test)]
mod test;

use crate::{
    common::ende::{KeyEnDeOrdered, ValueEnDe},
    versioned::mapx_ord_rawkey::{
        MapxOrdRawKeyVs, MapxOrdRawKeyVsIter, MapxOrdRawKeyVsIterMut, ValueIterMut,
    },
    BranchName, VersionName, VsMgmt,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    marker::PhantomData,
    ops::{Bound, Deref, DerefMut, RangeBounds},
};

/// Documents => [MapxRawVs](crate::versioned::mapx_raw::MapxRawVs)
#[derive(Serialize, Deserialize, Debug)]
#[serde(bound = "")]
pub struct MapxOrdVs<K, V> {
    inner: MapxOrdRawKeyVs<V>,
    _p: PhantomData<K>,
}

impl<K, V> MapxOrdVs<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// # Safety
    ///
    /// This API breaks the semantic safety guarantees,
    /// but it is safe to use in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        Self {
            inner: self.inner.shadow(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn new() -> Self {
        MapxOrdVs {
            inner: MapxOrdRawKeyVs::new(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&key.to_bytes())
    }

    #[inline(always)]
    pub fn get_mut<'a>(&'a mut self, key: &'a K) -> Option<ValueMut<'a, K, V>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    #[inline(always)]
    fn gen_mut<'a>(&'a mut self, key: &'a K, v: V) -> ValueMut<'a, K, V> {
        ValueMut::new(self, key, v)
    }

    #[inline(always)]
    pub fn entry<'a>(&'a mut self, key: &'a K) -> Entry<'a, K, V> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_le(&key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_ge(&key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
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
    pub fn insert(&mut self, key: &K, value: &V) -> Result<Option<V>> {
        self.inner.insert(&key.to_bytes(), value).c(d!())
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxOrdVsIter<K, V> {
        MapxOrdVsIter {
            inner: self.inner.iter(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxOrdVsIterMut<K, V> {
        MapxOrdVsIterMut {
            inner: self.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn values(&self) -> MapxOrdVsValues<V> {
        MapxOrdVsValues {
            inner: self.inner.iter(),
        }
    }

    #[inline(always)]
    pub fn values_mut(&mut self) -> MapxOrdVsValuesMut<V> {
        MapxOrdVsValuesMut {
            inner: self.inner.iter_mut(),
        }
    }

    #[inline(always)]
    pub fn range<'a, R: 'a + RangeBounds<K>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdVsIter<'a, K, V> {
        let l = match bounds.start_bound() {
            Bound::Included(i) => Bound::Included(Cow::Owned(i.to_bytes())),
            Bound::Excluded(i) => Bound::Excluded(Cow::Owned(i.to_bytes())),
            _ => Bound::Unbounded,
        };
        let h = match bounds.end_bound() {
            Bound::Included(i) => Bound::Included(Cow::Owned(i.to_bytes())),
            Bound::Excluded(i) => Bound::Excluded(Cow::Owned(i.to_bytes())),
            _ => Bound::Unbounded,
        };

        MapxOrdVsIter {
            inner: self.inner.range((l, h)),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_mut<'a, R: 'a + RangeBounds<K>>(
        &'a mut self,
        bounds: R,
    ) -> MapxOrdVsIterMut<'a, K, V> {
        let l = match bounds.start_bound() {
            Bound::Included(i) => Bound::Included(Cow::Owned(i.to_bytes())),
            Bound::Excluded(i) => Bound::Excluded(Cow::Owned(i.to_bytes())),
            _ => Bound::Unbounded,
        };
        let h = match bounds.end_bound() {
            Bound::Included(i) => Bound::Included(Cow::Owned(i.to_bytes())),
            Bound::Excluded(i) => Bound::Excluded(Cow::Owned(i.to_bytes())),
            _ => Bound::Unbounded,
        };

        MapxOrdVsIterMut {
            inner: self.inner.range_mut((l, h)),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first(&self) -> Option<(K, V)> {
        self.iter().next()
    }

    #[inline(always)]
    pub fn last(&self) -> Option<(K, V)> {
        self.iter().next_back()
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(&key.to_bytes())
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Result<Option<V>> {
        self.inner.remove(&key.to_bytes()).c(d!())
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline(always)]
    pub fn get_by_branch(&self, key: &K, br_name: BranchName) -> Option<V> {
        self.inner.get_by_branch(&key.to_bytes(), br_name)
    }

    #[inline(always)]
    pub fn get_le_by_branch(&self, key: &K, br_name: BranchName) -> Option<(K, V)> {
        self.inner
            .get_le_by_branch(&key.to_bytes(), br_name)
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    #[inline(always)]
    pub fn get_ge_by_branch(&self, key: &K, br_name: BranchName) -> Option<(K, V)> {
        self.inner
            .get_ge_by_branch(&key.to_bytes(), br_name)
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    #[inline(always)]
    pub fn len_by_branch(&self, br_name: BranchName) -> usize {
        self.inner.len_by_branch(br_name)
    }

    #[inline(always)]
    pub fn is_empty_by_branch(&self, br_name: BranchName) -> bool {
        self.inner.is_empty_by_branch(br_name)
    }

    #[inline(always)]
    pub fn insert_by_branch(
        &mut self,
        key: &K,
        value: &V,
        br_name: BranchName,
    ) -> Result<Option<V>> {
        self.inner
            .insert_by_branch(&key.to_bytes(), value, br_name)
            .c(d!())
    }

    #[inline(always)]
    pub fn iter_by_branch(&self, br_name: BranchName) -> MapxOrdVsIter<K, V> {
        MapxOrdVsIter {
            inner: self.inner.iter_by_branch(br_name),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_by_branch<'a, R: 'a + RangeBounds<K>>(
        &'a self,
        br_name: BranchName,
        bounds: R,
    ) -> MapxOrdVsIter<'a, K, V> {
        let l = match bounds.start_bound() {
            Bound::Included(i) => Bound::Included(Cow::Owned(i.to_bytes())),
            Bound::Excluded(i) => Bound::Excluded(Cow::Owned(i.to_bytes())),
            _ => Bound::Unbounded,
        };
        let h = match bounds.end_bound() {
            Bound::Included(i) => Bound::Included(Cow::Owned(i.to_bytes())),
            Bound::Excluded(i) => Bound::Excluded(Cow::Owned(i.to_bytes())),
            _ => Bound::Unbounded,
        };

        MapxOrdVsIter {
            inner: self.inner.range_by_branch(br_name, (l, h)),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first_by_branch(&self, br_name: BranchName) -> Option<(K, V)> {
        self.iter_by_branch(br_name).next()
    }

    #[inline(always)]
    pub fn last_by_branch(&self, br_name: BranchName) -> Option<(K, V)> {
        self.iter_by_branch(br_name).next_back()
    }

    #[inline(always)]
    pub fn contains_key_by_branch(&self, key: &K, br_name: BranchName) -> bool {
        self.inner.contains_key_by_branch(&key.to_bytes(), br_name)
    }

    #[inline(always)]
    pub fn remove_by_branch(
        &mut self,
        key: &K,
        br_name: BranchName,
    ) -> Result<Option<V>> {
        self.inner
            .remove_by_branch(&key.to_bytes(), br_name)
            .c(d!())
    }

    #[inline(always)]
    pub fn get_by_branch_version(
        &self,
        key: &K,
        br_name: BranchName,
        ver_name: VersionName,
    ) -> Option<V> {
        self.inner
            .get_by_branch_version(&key.to_bytes(), br_name, ver_name)
    }

    #[inline(always)]
    pub fn get_le_by_branch_version(
        &self,
        key: &K,
        br_name: BranchName,
        ver_name: VersionName,
    ) -> Option<(K, V)> {
        self.inner
            .get_le_by_branch_version(&key.to_bytes(), br_name, ver_name)
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    #[inline(always)]
    pub fn get_ge_by_branch_version(
        &self,
        key: &K,
        br_name: BranchName,
        ver_name: VersionName,
    ) -> Option<(K, V)> {
        self.inner
            .get_ge_by_branch_version(&key.to_bytes(), br_name, ver_name)
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    #[inline(always)]
    pub fn len_by_branch_version(
        &self,
        br_name: BranchName,
        ver_name: VersionName,
    ) -> usize {
        self.inner.len_by_branch_version(br_name, ver_name)
    }

    #[inline(always)]
    pub fn is_empty_by_branch_version(
        &self,
        br_name: BranchName,
        ver_name: VersionName,
    ) -> bool {
        self.inner.is_empty_by_branch_version(br_name, ver_name)
    }

    #[inline(always)]
    pub fn iter_by_branch_version(
        &self,
        br_name: BranchName,
        ver_name: VersionName,
    ) -> MapxOrdVsIter<K, V> {
        MapxOrdVsIter {
            inner: self.inner.iter_by_branch_version(br_name, ver_name),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_by_branch_version<'a, R: 'a + RangeBounds<K>>(
        &'a self,
        br_name: BranchName,
        ver_name: VersionName,
        bounds: R,
    ) -> MapxOrdVsIter<'a, K, V> {
        let l = match bounds.start_bound() {
            Bound::Included(i) => Bound::Included(Cow::Owned(i.to_bytes())),
            Bound::Excluded(i) => Bound::Excluded(Cow::Owned(i.to_bytes())),
            _ => Bound::Unbounded,
        };
        let h = match bounds.end_bound() {
            Bound::Included(i) => Bound::Included(Cow::Owned(i.to_bytes())),
            Bound::Excluded(i) => Bound::Excluded(Cow::Owned(i.to_bytes())),
            _ => Bound::Unbounded,
        };

        MapxOrdVsIter {
            inner: self
                .inner
                .range_by_branch_version(br_name, ver_name, (l, h)),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first_by_branch_version(
        &self,
        br_name: BranchName,
        ver_name: VersionName,
    ) -> Option<(K, V)> {
        self.iter_by_branch_version(br_name, ver_name).next()
    }

    #[inline(always)]
    pub fn last_by_branch_version(
        &self,
        br_name: BranchName,
        ver_name: VersionName,
    ) -> Option<(K, V)> {
        self.iter_by_branch_version(br_name, ver_name).next_back()
    }

    #[inline(always)]
    pub fn contains_key_by_branch_version(
        &self,
        key: &K,
        br_name: BranchName,
        ver_name: VersionName,
    ) -> bool {
        self.inner
            .contains_key_by_branch_version(&key.to_bytes(), br_name, ver_name)
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

impl<K, V> Clone for MapxOrdVs<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _p: PhantomData,
        }
    }
}

impl<K, V> Default for MapxOrdVs<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

impl<K, V> VsMgmt for MapxOrdVs<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    crate::impl_vs_methods!();
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    hdr: &'a mut MapxOrdVs<K, V>,
    key: &'a K,
    value: V,
}

impl<'a, K, V> ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn new(hdr: &'a mut MapxOrdVs<K, V>, key: &'a K, value: V) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<'a, K, V> Drop for ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn drop(&mut self) {
        pnk!(self.hdr.insert(self.key, &self.value));
    }
}

impl<'a, K, V> Deref for ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K, V> DerefMut for ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

pub struct Entry<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    hdr: &'a mut MapxOrdVs<K, V>,
    key: &'a K,
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    pub fn or_insert(self, default: V) -> ValueMut<'a, K, V> {
        let hdr = self.hdr as *mut MapxOrdVs<K, V>;
        if let Some(v) = unsafe { &mut *hdr }.get_mut(self.key) {
            v
        } else {
            unsafe { &mut *hdr }.gen_mut(self.key, default)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdVsIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyVsIter<'a, V>,
    _p: PhantomData<K>,
}

impl<'a, K, V> Iterator for MapxOrdVsIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

impl<'a, K, V> DoubleEndedIterator for MapxOrdVsIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdVsValues<'a, V>
where
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyVsIter<'a, V>,
}

impl<'a, V> Iterator for MapxOrdVsValues<'a, V>
where
    V: ValueEnDe,
{
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<'a, V> DoubleEndedIterator for MapxOrdVsValues<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| v)
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdVsIterMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyVsIterMut<'a, V>,
    _p: PhantomData<K>,
}

impl<'a, K, V> Iterator for MapxOrdVsIterMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, ValueIterMut<'a, V>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(k, v)| (pnk!(<K as KeyEnDeOrdered>::from_bytes(k)), v))
    }
}

impl<'a, K, V> DoubleEndedIterator for MapxOrdVsIterMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (pnk!(<K as KeyEnDeOrdered>::from_bytes(k)), v))
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdVsValuesMut<'a, V>
where
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyVsIterMut<'a, V>,
}

impl<'a, V> Iterator for MapxOrdVsValuesMut<'a, V>
where
    V: ValueEnDe,
{
    type Item = ValueIterMut<'a, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<'a, V> DoubleEndedIterator for MapxOrdVsValuesMut<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| v)
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
