//!
//! A `BTreeMap`-like structure but storing data in disk.
//!
//! NOTE:
//!
//! - Both keys and values will be encoded in this structure
//!     - Keys will be encoded by `KeyEnDeOrdered`
//!     - Values will be encoded by some `serde`-like methods
//! - It's your duty to ensure that the encoded key keeps a same order with the original key
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_ord::MapxOrd;
//!
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb::vsdb_set_base_dir(&dir);
//!
//! let mut l = MapxOrd::new();
//!
//! l.insert(1, 0);
//! l.insert(&1, &0);
//! l.insert(2, 0);
//!
//! l.iter().for_each(|(k, v)| {
//!     assert!(k >= 1);
//!     assert_eq!(v, 0);
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

use crate::{
    basic::mapx_ord_rawkey::{MapxOrdRawKey, MapxOrdRawKeyIter, ValueIterMut, ValueMut},
    common::{
        ende::{KeyEnDeOrdered, ValueEnDe},
        RawKey,
    },
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};
use vsdb_core::basic::mapx_raw;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxOrd<K, V> {
    inner: MapxOrdRawKey<V>,
    _m_pd: PhantomData<K>,
}

impl<K, V> MapxOrd<K, V>
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
            _m_pd: PhantomData,
        }
    }

    #[inline(always)]
    pub fn new() -> Self {
        MapxOrd {
            inner: MapxOrdRawKey::new(),
            _m_pd: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&key.to_bytes())
    }

    #[inline(always)]
    pub fn get_mut<'a>(&'a mut self, key: &K) -> Option<ValueMut<'a, V>> {
        self.inner.get_mut(&key.to_bytes())
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(&key.to_bytes())
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
    pub fn insert(&mut self, key: &K, value: &V) -> Option<V> {
        self.inner.insert(&key.to_bytes(), value)
    }

    /// # Safety
    ///
    /// Used to support efficient versioned-implementations,
    /// Do NOT use this API for any common purpose.
    #[inline(always)]
    pub unsafe fn insert_encoded_value(
        &mut self,
        key: &K,
        value: impl AsRef<[u8]>,
    ) -> Option<V> {
        self.inner.insert_encoded_value(&key.to_bytes(), value)
    }

    #[inline(always)]
    pub fn set_value(&mut self, key: &K, value: &V) {
        self.inner.insert(&key.to_bytes(), value);
    }

    #[inline(always)]
    pub fn entry(&mut self, key: &K) -> Entry<'_, V> {
        Entry {
            key: key.to_bytes(),
            hdr: &mut self.inner,
        }
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxOrdIter<K, V> {
        MapxOrdIter {
            inner: self.inner.iter(),
            _m_pd: PhantomData,
        }
    }

    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxOrdIterMut<K, V> {
        MapxOrdIterMut {
            inner: self.inner.inner.iter_mut(),
            _m_pd: PhantomData,
        }
    }

    #[inline(always)]
    pub fn values(&self) -> MapxOrdValues<V> {
        MapxOrdValues {
            inner: self.inner.iter(),
        }
    }

    #[inline(always)]
    pub fn values_mut(&mut self) -> MapxOrdValuesMut<V> {
        MapxOrdValuesMut {
            inner: self.inner.inner.iter_mut(),
            _m_pd: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> MapxOrdIter<'_, K, V> {
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

        MapxOrdIter {
            inner: self.inner.range((l, h)),
            _m_pd: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_mut<R: RangeBounds<K>>(
        &mut self,
        bounds: R,
    ) -> MapxOrdIterMut<'_, K, V> {
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

        MapxOrdIterMut {
            inner: self.inner.inner.range_mut((l, h)),
            _m_pd: PhantomData,
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
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(&key.to_bytes())
    }

    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.remove(&key.to_bytes());
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<K, V> Clone for MapxOrd<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _m_pd: PhantomData,
        }
    }
}

impl<K, V> Default for MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyIter<'a, V>,
    _m_pd: PhantomData<K>,
}

impl<'a, K, V> Iterator for MapxOrdIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

impl<'a, K, V> DoubleEndedIterator for MapxOrdIter<'a, K, V>
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

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdValues<'a, V>
where
    V: ValueEnDe,
{
    pub(crate) inner: MapxOrdRawKeyIter<'a, V>,
}

impl<'a, V> Iterator for MapxOrdValues<'a, V>
where
    V: ValueEnDe,
{
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<'a, V> DoubleEndedIterator for MapxOrdValues<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| v)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdValuesMut<'a, V>
where
    V: ValueEnDe,
{
    pub(crate) inner: mapx_raw::MapxRawIterMut<'a>,
    pub(crate) _m_pd: PhantomData<V>,
}

impl<'a, V> Iterator for MapxOrdValuesMut<'a, V>
where
    V: ValueEnDe,
{
    type Item = ValueIterMut<'a, V>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| ValueIterMut {
            value: pnk!(<V as ValueEnDe>::decode(&*v)),
            inner: v,
        })
    }
}

impl<'a, V> DoubleEndedIterator for MapxOrdValuesMut<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| ValueIterMut {
            value: pnk!(<V as ValueEnDe>::decode(&*v)),
            inner: v,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdIterMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: mapx_raw::MapxRawIterMut<'a>,
    _m_pd: PhantomData<(K, V)>,
}

impl<'a, K, V> Iterator for MapxOrdIterMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, ValueIterMut<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| {
            (
                pnk!(<K as KeyEnDeOrdered>::from_bytes(k)),
                ValueIterMut {
                    value: <V as ValueEnDe>::decode(&*v).unwrap(),
                    inner: v,
                },
            )
        })
    }
}

impl<'a, K, V> DoubleEndedIterator for MapxOrdIterMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(k, v)| {
            (
                pnk!(<K as KeyEnDeOrdered>::from_bytes(k)),
                ValueIterMut {
                    value: <V as ValueEnDe>::decode(&*v).unwrap(),
                    inner: v,
                },
            )
        })
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct Entry<'a, V>
where
    V: ValueEnDe,
{
    pub(crate) key: RawKey,
    pub(crate) hdr: &'a mut MapxOrdRawKey<V>,
}

impl<'a, V> Entry<'a, V>
where
    V: ValueEnDe,
{
    pub fn or_insert(self, default: &V) -> ValueMut<'a, V> {
        if !self.hdr.contains_key(&self.key) {
            self.hdr.set_value(&self.key, default);
        }
        pnk!(self.hdr.get_mut(&self.key))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

impl<K, V> vsdb_core::VsMgmt for MapxOrd<K, V> {
    vsdb_core::impl_vs_methods_nope! {}
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
