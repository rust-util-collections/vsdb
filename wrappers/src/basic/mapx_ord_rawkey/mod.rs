//!
//! A `BTreeMap`-like structure but storing data in disk.
//!
//! NOTE:
//! - Keys will **NOT** be encoded in this structure, but values will be
//!     - Values will be encoded by some `serde`-like methods
//! - It's your duty to ensure that the encoded key keeps a same order with the original key
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_ord_rawkey::MapxOrdRawKey;
//!
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb::vsdb_set_base_dir(&dir);
//!
//! let mut l = MapxOrdRawKey::new();
//!
//! l.insert(&[1], &0);
//! l.insert(vec![1], 0);
//! l.insert(&[2], &0);
//!
//! l.iter().for_each(|(_, v)| {
//!     assert_eq!(v, 0);
//! });
//!
//! l.remove(&[2]);
//! assert_eq!(l.len(), 1);
//!
//! l.clear();
//! assert_eq!(l.len(), 0);
//! ```
//!

#[cfg(test)]
mod test;

use crate::common::{ende::ValueEnDe, RawKey};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    marker::PhantomData,
    ops::{Deref, DerefMut, RangeBounds},
};
use vsdb_core::basic::mapx_raw::{self, MapxRaw, MapxRawIter};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxOrdRawKey<V> {
    pub(crate) inner: MapxRaw,
    _p: PhantomData<V>,
}

impl<V> MapxOrdRawKey<V>
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
        MapxOrdRawKey {
            inner: MapxRaw::new(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<V> {
        self.inner
            .get(key.as_ref())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_, V>> {
        self.inner.get_mut(key.as_ref()).map(|inner| ValueMut {
            value: <V as ValueEnDe>::decode(&inner).unwrap(),
            inner,
        })
    }

    #[inline(always)]
    pub(crate) fn mock_value_mut(&mut self, key: RawKey, value: V) -> ValueMut<'_, V> {
        let v = value.encode();
        ValueMut {
            value,
            inner: self.inner.mock_value_mut(key, v),
        }
    }

    #[inline(always)]
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> bool {
        self.inner.contains_key(key.as_ref())
    }

    #[inline(always)]
    pub fn get_le(&self, key: impl AsRef<[u8]>) -> Option<(RawKey, V)> {
        self.inner
            .get_le(key.as_ref())
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn get_ge(&self, key: impl AsRef<[u8]>) -> Option<(RawKey, V)> {
        self.inner
            .get_ge(key.as_ref())
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
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
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: &V) -> Option<V> {
        self.inner
            .insert(key.as_ref(), value.encode())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    /// # Safety
    ///
    /// Do NOT use this API.
    // used to support efficient versioned-implementations
    #[inline(always)]
    pub unsafe fn insert_encoded_value(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Option<V> {
        self.inner
            .insert(key.as_ref(), value.as_ref())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn set_value(&mut self, key: impl AsRef<[u8]>, value: &V) {
        self.inner.insert(key.as_ref(), value.encode());
    }

    #[inline(always)]
    pub fn entry<'a>(&'a mut self, key: &'a [u8]) -> Entry<'a, V> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxOrdRawKeyIter<V> {
        MapxOrdRawKeyIter {
            inner: self.inner.iter(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxOrdRawKeyIterMut<V> {
        MapxOrdRawKeyIterMut {
            inner: self.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdRawKeyIter<'a, V> {
        MapxOrdRawKeyIter {
            inner: self.inner.range(bounds),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_mut<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a mut self,
        bounds: R,
    ) -> MapxOrdRawKeyIterMut<'a, V> {
        MapxOrdRawKeyIterMut {
            inner: self.inner.range_mut(bounds),
            _p: PhantomData,
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
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Option<V> {
        self.inner
            .remove(key.as_ref())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn unset_value(&mut self, key: impl AsRef<[u8]>) {
        self.inner.remove(key.as_ref());
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

impl<V> Clone for MapxOrdRawKey<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _p: PhantomData,
        }
    }
}

impl<V> Default for MapxOrdRawKey<V>
where
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ValueMut<'a, V>
where
    V: ValueEnDe,
{
    value: V,
    inner: mapx_raw::ValueMut<'a>,
}

impl<'a, V> Drop for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        *self.inner = self.value.encode();
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

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct Entry<'a, V>
where
    V: ValueEnDe,
{
    key: &'a [u8],
    hdr: &'a mut MapxOrdRawKey<V>,
}

impl<'a, V> Entry<'a, V>
where
    V: ValueEnDe,
{
    pub fn or_insert(self, default: V) -> ValueMut<'a, V> {
        let hdr = self.hdr as *mut MapxOrdRawKey<V>;
        if let Some(v) = unsafe { &mut *hdr }.get_mut(self.key) {
            v
        } else {
            unsafe { &mut *hdr }.mock_value_mut(self.key.to_vec(), default)
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdRawKeyIter<'a, V> {
    inner: MapxRawIter<'a>,
    _p: PhantomData<V>,
}

impl<'a, V> Iterator for MapxOrdRawKeyIter<'a, V>
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

impl<'a, V> DoubleEndedIterator for MapxOrdRawKeyIter<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdRawKeyIterMut<'a, V> {
    inner: mapx_raw::MapxRawIterMut<'a>,
    _p: PhantomData<V>,
}

impl<'a, V> Iterator for MapxOrdRawKeyIterMut<'a, V>
where
    V: ValueEnDe,
{
    type Item = (RawKey, ValueIterMut<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| {
            (
                k,
                ValueIterMut {
                    value: <V as ValueEnDe>::decode(&v).unwrap(),
                    inner: v,
                },
            )
        })
    }
}

impl<'a, V> DoubleEndedIterator for MapxOrdRawKeyIterMut<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(k, v)| {
            (
                k,
                ValueIterMut {
                    value: <V as ValueEnDe>::decode(&v).unwrap(),
                    inner: v,
                },
            )
        })
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    pub(crate) value: V,
    pub(crate) inner: mapx_raw::ValueIterMut<'a>,
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

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
