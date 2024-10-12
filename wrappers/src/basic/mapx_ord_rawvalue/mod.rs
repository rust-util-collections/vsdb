//!
//! A `BTreeMap`-like structure but storing data in disk.
//!
//! NOTE:
//! - Values will **NOT** be encoded in this structure, but keys will be
//!     - Keys will be encoded by `KeyEnDeOrdered`
//! - It's your duty to ensure that the encoded key keeps a same order with the original key
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_ord_rawvalue::MapxOrdRawValue;
//!
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb::vsdb_set_base_dir(&dir);
//!
//! let mut l = MapxOrdRawValue::new();
//!
//! l.insert(&1, &[0]);
//! l.insert(1, Box::new([0]));
//! l.insert(&2, &[0]);
//!
//! l.iter().for_each(|(k, v)| {
//!     assert!(k >= 1);
//!     assert_eq!(&v[..], &[0]);
//! });
//!
//! l.remove(&2);
//! assert_eq!(l.len(), 1);
//!
//! l.clear();
//! assert_eq!(l.len(), 0);
//! ```
//!

#[cfg(test)]
mod test;

use crate::common::{ende::KeyEnDeOrdered, RawValue};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    marker::PhantomData,
    ops::{Bound, Deref, DerefMut, RangeBounds},
};
use vsdb_core::basic::mapx_raw::{MapxRaw, MapxRawIter, MapxRawIterMut, ValueIterMut};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxOrdRawValue<K> {
    pub(crate) inner: MapxRaw,
    _p: PhantomData<K>,
}

impl<K> MapxOrdRawValue<K>
where
    K: KeyEnDeOrdered,
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

    /// # Safety
    ///
    /// Do not use this API unless you know the internal details extremely well.
    #[inline(always)]
    pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
        Self {
            inner: MapxRaw::from_bytes(s),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    #[inline(always)]
    pub fn new() -> Self {
        MapxOrdRawValue {
            inner: MapxRaw::new(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<RawValue> {
        self.inner.get(key.to_bytes())
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, K>> {
        self.inner
            .get(key.to_bytes())
            .map(|v| ValueMut::new(self, key.clone(), v))
    }

    #[inline(always)]
    pub(crate) fn mock_value_mut(&mut self, key: K, value: RawValue) -> ValueMut<'_, K> {
        ValueMut {
            hdr: self,
            key,
            value,
        }
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key.to_bytes())
    }

    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, RawValue)> {
        self.inner
            .get_le(key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, RawValue)> {
        self.inner
            .get_ge(key.to_bytes())
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
    pub fn insert(&mut self, key: &K, value: impl AsRef<[u8]>) -> Option<RawValue> {
        self.inner.insert(key.to_bytes(), value.as_ref())
    }

    #[inline(always)]
    pub fn set_value(&mut self, key: &K, value: impl AsRef<[u8]>) {
        self.inner.insert(key.to_bytes(), value.as_ref());
    }

    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, K> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxOrdRawValueIter<K> {
        MapxOrdRawValueIter {
            inner: self.inner.iter(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxOrdRawValueIterMut<K> {
        MapxOrdRawValueIterMut {
            inner: self.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn values(&self) -> MapxOrdRawValueValues<K> {
        MapxOrdRawValueValues {
            inner: self.inner.iter(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn values_mut(&mut self) -> MapxOrdRawValueValuesMut {
        MapxOrdRawValueValuesMut {
            inner: self.inner.iter_mut(),
        }
    }

    #[inline(always)]
    pub fn range<'a, R: RangeBounds<&'a K>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdRawValueIter<K> {
        let l = match bounds.start_bound() {
            Bound::Included(lo) => Bound::Included(Cow::Owned(lo.to_bytes())),
            Bound::Excluded(lo) => Bound::Excluded(Cow::Owned(lo.to_bytes())),
            Bound::Unbounded => Bound::Unbounded,
        };

        let h = match bounds.end_bound() {
            Bound::Included(hi) => Bound::Included(Cow::Owned(hi.to_bytes())),
            Bound::Excluded(hi) => Bound::Excluded(Cow::Owned(hi.to_bytes())),
            Bound::Unbounded => Bound::Unbounded,
        };

        MapxOrdRawValueIter {
            inner: self.inner.range((l, h)),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_mut<'a, R: RangeBounds<&'a K>>(
        &'a mut self,
        bounds: R,
    ) -> MapxOrdRawValueIterMut<K> {
        let l = match bounds.start_bound() {
            Bound::Included(lo) => Bound::Included(Cow::Owned(lo.to_bytes())),
            Bound::Excluded(lo) => Bound::Excluded(Cow::Owned(lo.to_bytes())),
            Bound::Unbounded => Bound::Unbounded,
        };

        let h = match bounds.end_bound() {
            Bound::Included(hi) => Bound::Included(Cow::Owned(hi.to_bytes())),
            Bound::Excluded(hi) => Bound::Excluded(Cow::Owned(hi.to_bytes())),
            Bound::Unbounded => Bound::Unbounded,
        };

        MapxOrdRawValueIterMut {
            inner: self.inner.range_mut((l, h)),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first(&self) -> Option<(K, RawValue)> {
        self.iter().next()
    }

    #[inline(always)]
    pub fn last(&self) -> Option<(K, RawValue)> {
        self.iter().next_back()
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<RawValue> {
        self.inner.remove(key.to_bytes())
    }

    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.remove(key.to_bytes());
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }
}

impl<K> Clone for MapxOrdRawValue<K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _p: PhantomData,
        }
    }
}

impl<K> Default for MapxOrdRawValue<K>
where
    K: KeyEnDeOrdered,
{
    fn default() -> Self {
        Self::new()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ValueMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    hdr: &'a mut MapxOrdRawValue<K>,
    key: K,
    value: RawValue,
}

impl<'a, K> ValueMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    pub(crate) fn new(hdr: &'a mut MapxOrdRawValue<K>, key: K, value: RawValue) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<'a, K> Drop for ValueMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    fn drop(&mut self) {
        self.hdr.set_value(&self.key, &self.value);
    }
}

impl<'a, K> Deref for ValueMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    type Target = RawValue;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K> DerefMut for ValueMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct Entry<'a, K>
where
    K: KeyEnDeOrdered,
{
    key: K,
    hdr: &'a mut MapxOrdRawValue<K>,
}

impl<'a, K> Entry<'a, K>
where
    K: KeyEnDeOrdered,
{
    pub fn or_insert(self, default: impl AsRef<[u8]>) -> ValueMut<'a, K> {
        let hdr = self.hdr as *mut MapxOrdRawValue<K>;
        if let Some(v) = unsafe { &mut *hdr }.get_mut(&self.key) {
            v
        } else {
            unsafe { &mut *hdr }.mock_value_mut(self.key, default.as_ref().to_vec())
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdRawValueIter<'a, K>
where
    K: KeyEnDeOrdered,
{
    inner: MapxRawIter<'a>,
    _p: PhantomData<K>,
}

impl<'a, K> Iterator for MapxOrdRawValueIter<'a, K>
where
    K: KeyEnDeOrdered,
{
    type Item = (K, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

impl<'a, K> DoubleEndedIterator for MapxOrdRawValueIter<'a, K>
where
    K: KeyEnDeOrdered,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdRawValueValues<'a, K>
where
    K: KeyEnDeOrdered,
{
    inner: MapxRawIter<'a>,
    _p: PhantomData<K>,
}

impl<'a, K> Iterator for MapxOrdRawValueValues<'a, K>
where
    K: KeyEnDeOrdered,
{
    type Item = RawValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<'a, K> DoubleEndedIterator for MapxOrdRawValueValues<'a, K>
where
    K: KeyEnDeOrdered,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| v)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdRawValueIterMut<'a, K> {
    pub(crate) inner: MapxRawIterMut<'a>,
    pub(crate) _p: PhantomData<K>,
}

impl<'a, K> Iterator for MapxOrdRawValueIterMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    type Item = (K, ValueIterMut<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(k, v)| (pnk!(<K as KeyEnDeOrdered>::from_bytes(k)), v))
    }
}

impl<'a, K> DoubleEndedIterator for MapxOrdRawValueIterMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (pnk!(<K as KeyEnDeOrdered>::from_bytes(k)), v))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdRawValueValuesMut<'a> {
    inner: MapxRawIterMut<'a>,
}

impl<'a> Iterator for MapxOrdRawValueValuesMut<'a> {
    type Item = ValueIterMut<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<'a> DoubleEndedIterator for MapxOrdRawValueValuesMut<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| v)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
