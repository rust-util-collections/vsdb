//!
//! Documents => [MapxRawVs](crate::versioned::mapx_raw)
//!

#[cfg(test)]
mod test;

use crate::{
    common::{ende::ValueEnDe, BranchName, RawKey, VersionName},
    VsMgmt,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    marker::PhantomData,
    ops::{Deref, DerefMut, RangeBounds},
};
use vsdb_core::versioned::mapx_raw::{self, MapxRawVs, MapxRawVsIter, MapxRawVsIterMut};

/// Documents => [MapxRawVs](crate::versioned::mapx_raw::MapxRawVs)
#[derive(Serialize, Deserialize, Debug)]
#[serde(bound = "")]
pub struct MapxOrdRawKeyVs<V> {
    inner: MapxRawVs,
    p: PhantomData<V>,
}

impl<V> MapxOrdRawKeyVs<V>
where
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
            p: PhantomData,
        }
    }

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
    pub fn get_mut<'a>(&'a mut self, key: &'a [u8]) -> Option<ValueMut<'_, V>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    #[inline(always)]
    fn gen_mut<'a>(&'a mut self, key: &'a [u8], v: V) -> ValueMut<'_, V> {
        ValueMut::new(self, key, v)
    }

    #[inline(always)]
    pub fn entry<'a>(&'a mut self, key: &'a [u8]) -> Entry<'a, V> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn get_by_branch(&self, key: &[u8], br_name: BranchName) -> Option<V> {
        self.inner
            .get_by_branch(key, br_name)
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn get_by_branch_version(
        &self,
        key: &[u8],
        br_name: BranchName,
        ver_name: VersionName,
    ) -> Option<V> {
        self.inner
            .get_by_branch_version(key, br_name, ver_name)
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn get_le(&self, key: &[u8]) -> Option<(RawKey, V)> {
        self.inner
            .get_le(key)
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn get_le_by_branch(
        &self,
        key: &[u8],
        br_name: BranchName,
    ) -> Option<(RawKey, V)> {
        self.inner
            .get_le_by_branch(key, br_name)
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn get_le_by_branch_version(
        &self,
        key: &[u8],
        br_name: BranchName,
        ver_name: VersionName,
    ) -> Option<(RawKey, V)> {
        self.inner
            .get_le_by_branch_version(key, br_name, ver_name)
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn get_ge(&self, key: &[u8]) -> Option<(RawKey, V)> {
        self.inner
            .get_ge(key)
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn get_ge_by_branch(
        &self,
        key: &[u8],
        br_name: BranchName,
    ) -> Option<(RawKey, V)> {
        self.inner
            .get_ge_by_branch(key, br_name)
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn get_ge_by_branch_version(
        &self,
        key: &[u8],
        br_name: BranchName,
        ver_name: VersionName,
    ) -> Option<(RawKey, V)> {
        self.inner
            .get_ge_by_branch_version(key, br_name, ver_name)
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub fn len_by_branch(&self, br_name: BranchName) -> usize {
        self.inner.len_by_branch(br_name)
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
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    pub fn is_empty_by_branch(&self, br_name: BranchName) -> bool {
        self.inner.is_empty_by_branch(br_name)
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
    pub fn insert(&mut self, key: &[u8], value: &V) -> Result<Option<V>> {
        self.inner
            .insert(key, &value.encode())
            .c(d!())
            .map(|v| v.map(|v| <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn insert_by_branch(
        &mut self,
        key: &[u8],
        value: &V,
        br_name: BranchName,
    ) -> Result<Option<V>> {
        self.inner
            .insert_by_branch(key, &value.encode(), br_name)
            .c(d!())
            .map(|v| v.map(|v| <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxOrdRawKeyVsIter<'_, V> {
        MapxOrdRawKeyVsIter {
            inner: self.inner.iter(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxOrdRawKeyVsIterMut<'_, V> {
        MapxOrdRawKeyVsIterMut {
            inner: self.inner.iter_mut(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn iter_by_branch(&self, br_name: BranchName) -> MapxOrdRawKeyVsIter<'_, V> {
        MapxOrdRawKeyVsIter {
            inner: self.inner.iter_by_branch(br_name),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn iter_by_branch_version(
        &self,
        br_name: BranchName,
        ver_name: VersionName,
    ) -> MapxOrdRawKeyVsIter<'_, V> {
        MapxOrdRawKeyVsIter {
            inner: self.inner.iter_by_branch_version(br_name, ver_name),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdRawKeyVsIter<'a, V> {
        MapxOrdRawKeyVsIter {
            inner: self.inner.range(bounds),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_mut<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a mut self,
        bounds: R,
    ) -> MapxOrdRawKeyVsIterMut<'a, V> {
        MapxOrdRawKeyVsIterMut {
            inner: self.inner.range_mut(bounds),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_by_branch<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        br_name: BranchName,
        bounds: R,
    ) -> MapxOrdRawKeyVsIter<'a, V> {
        MapxOrdRawKeyVsIter {
            inner: self.inner.range_by_branch(br_name, bounds),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_by_branch_version<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        br_name: BranchName,
        ver_name: VersionName,
        bounds: R,
    ) -> MapxOrdRawKeyVsIter<'a, V> {
        MapxOrdRawKeyVsIter {
            inner: self
                .inner
                .range_by_branch_version(br_name, ver_name, bounds),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first(&self) -> Option<(RawKey, V)> {
        self.iter().next()
    }

    #[inline(always)]
    pub fn first_by_branch(&self, br_name: BranchName) -> Option<(RawKey, V)> {
        self.iter_by_branch(br_name).next()
    }

    #[inline(always)]
    pub fn first_by_branch_version(
        &self,
        br_name: BranchName,
        ver_name: VersionName,
    ) -> Option<(RawKey, V)> {
        self.iter_by_branch_version(br_name, ver_name).next()
    }

    #[inline(always)]
    pub fn last(&self) -> Option<(RawKey, V)> {
        self.iter().next_back()
    }

    #[inline(always)]
    pub fn last_by_branch(&self, br_name: BranchName) -> Option<(RawKey, V)> {
        self.iter_by_branch(br_name).next_back()
    }

    #[inline(always)]
    pub fn last_by_branch_version(
        &self,
        br_name: BranchName,
        ver_name: VersionName,
    ) -> Option<(RawKey, V)> {
        self.iter_by_branch_version(br_name, ver_name).next_back()
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.inner.contains_key(key)
    }

    #[inline(always)]
    pub fn contains_key_by_branch(&self, key: &[u8], br_name: BranchName) -> bool {
        self.inner.contains_key_by_branch(key, br_name)
    }

    #[inline(always)]
    pub fn contains_key_by_branch_version(
        &self,
        key: &[u8],
        br_name: BranchName,
        ver_name: VersionName,
    ) -> bool {
        self.inner
            .contains_key_by_branch_version(key, br_name, ver_name)
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &[u8]) -> Result<Option<V>> {
        self.inner
            .remove(key)
            .c(d!())
            .map(|v| v.map(|v| <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn remove_by_branch(
        &mut self,
        key: &[u8],
        br_name: BranchName,
    ) -> Result<Option<V>> {
        self.inner
            .remove_by_branch(key, br_name)
            .c(d!())
            .map(|v| v.map(|v| <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

impl<V> Clone for MapxOrdRawKeyVs<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            p: PhantomData,
        }
    }
}

impl<V> Default for MapxOrdRawKeyVs<V>
where
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

impl<V> VsMgmt for MapxOrdRawKeyVs<V>
where
    V: ValueEnDe,
{
    crate::impl_vs_methods!();
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdRawKeyVsIter<'a, V>
where
    V: ValueEnDe,
{
    inner: MapxRawVsIter<'a>,
    p: PhantomData<V>,
}

impl<'a, V> Iterator for MapxOrdRawKeyVsIter<'a, V>
where
    V: ValueEnDe,
{
    type Item = (RawKey, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }
}

impl<'a, V> DoubleEndedIterator for MapxOrdRawKeyVsIter<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ValueMut<'a, V: ValueEnDe> {
    hdr: &'a mut MapxOrdRawKeyVs<V>,
    key: &'a [u8],
    value: V,
}

impl<'a, V> ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn new(hdr: &'a mut MapxOrdRawKeyVs<V>, key: &'a [u8], value: V) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<'a, V> Drop for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        pnk!(self.hdr.insert(self.key, &self.value));
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

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

pub struct Entry<'a, V: ValueEnDe> {
    hdr: &'a mut MapxOrdRawKeyVs<V>,
    key: &'a [u8],
}

impl<'a, V> Entry<'a, V>
where
    V: ValueEnDe,
{
    pub fn or_insert(self, default: V) -> ValueMut<'a, V> {
        let hdr = self.hdr as *mut MapxOrdRawKeyVs<V>;
        if let Some(v) = unsafe { &mut *hdr }.get_mut(self.key) {
            v
        } else {
            unsafe { &mut *hdr }.gen_mut(self.key, default)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

pub struct ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    value: V,
    inner: mapx_raw::ValueIterMut<'a>,
}

impl<'a, V> Drop for ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        *self.inner = self.value.encode();
    }
}

impl<'a, V> Deref for ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, V> DerefMut for ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdRawKeyVsIterMut<'a, V>
where
    V: ValueEnDe,
{
    inner: MapxRawVsIterMut<'a>,
    p: PhantomData<V>,
}

impl<'a, V> Iterator for MapxOrdRawKeyVsIterMut<'a, V>
where
    V: ValueEnDe,
{
    type Item = (RawKey, ValueIterMut<'a, V>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| {
            (
                k,
                ValueIterMut {
                    value: pnk!(<V as ValueEnDe>::decode(&v)),
                    inner: v,
                },
            )
        })
    }
}

impl<'a, V> DoubleEndedIterator for MapxOrdRawKeyVsIterMut<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(k, v)| {
            (
                k,
                ValueIterMut {
                    value: pnk!(<V as ValueEnDe>::decode(&v)),
                    inner: v,
                },
            )
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
