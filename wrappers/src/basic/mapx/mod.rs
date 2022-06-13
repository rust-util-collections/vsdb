//!
//! A `HashMap`-like structure but storing data in disk.
//!
//! NOTE:
//!
//! - Both keys and values will be encoded(serde) in this structure
//!     - Both of them will be encoded by some `serde`-like methods
//!
//! # Examples
//!
//! ```
//! use vsdb::Mapx;
//!
//! let dir = format!("/tmp/__vsdb__{}", rand::random::<u128>());
//! vsdb::vsdb_set_base_dir(&dir);
//!
//! let mut l = Mapx::new();
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
    basic::{
        mapx_ord::{MapxOrdValues, MapxOrdValuesMut},
        mapx_ord_rawkey::{
            self, Entry, MapxOrdRawKey, MapxOrdRawKeyIter, MapxOrdRawKeyIterMut,
            ValueMut,
        },
    },
    common::ende::{KeyEnDe, ValueEnDe},
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct Mapx<K, V> {
    inner: MapxOrdRawKey<V>,
    _m_pd: PhantomData<K>,
}

impl<K, V> Mapx<K, V>
where
    K: KeyEnDe,
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
        Self {
            inner: MapxOrdRawKey::new(),
            _m_pd: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&key.encode())
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, V>> {
        self.inner.get_mut(&key.encode())
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(&key.encode())
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
        self.inner.insert(&key.encode(), value)
    }

    #[inline(always)]
    pub fn set_value(&mut self, key: &K, value: &V) {
        self.inner.set_value(&key.encode(), value);
    }

    #[inline(always)]
    pub fn entry(&mut self, key: &K) -> Entry<'_, V> {
        self.inner.entry(key.encode())
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxIter<K, V> {
        MapxIter {
            iter: self.inner.iter(),
            _m_pd: PhantomData,
        }
    }

    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxIterMut<K, V> {
        MapxIterMut {
            inner: self.inner.iter_mut(),
            _m_pd: PhantomData,
        }
    }

    #[inline(always)]
    pub fn values(&self) -> MapxValues<V> {
        MapxValues {
            inner: self.inner.iter(),
        }
    }

    #[inline(always)]
    pub fn values_mut(&mut self) -> MapxValuesMut<V> {
        MapxValuesMut {
            inner: self.inner.inner.iter_mut(),
            _m_pd: PhantomData,
        }
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(&key.encode())
    }

    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.unset_value(&key.encode());
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<K, V> Clone for Mapx<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _m_pd: PhantomData,
        }
    }
}

impl<K, V> Default for Mapx<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxIter<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    iter: MapxOrdRawKeyIter<'a, V>,
    _m_pd: PhantomData<K>,
}

impl<'a, K, V> Iterator for MapxIter<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(k, v)| (<K as KeyEnDe>::decode(&k).unwrap(), v))
    }
}

impl<'a, K, V> DoubleEndedIterator for MapxIter<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|(k, v)| (<K as KeyEnDe>::decode(&k).unwrap(), v))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxIterMut<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyIterMut<'a, V>,
    _m_pd: PhantomData<K>,
}

impl<'a, K, V> Iterator for MapxIterMut<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    type Item = (K, ValueIterMut<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), ValueIterMut { inner: v }))
    }
}

impl<'a, K, V> DoubleEndedIterator for MapxIterMut<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), ValueIterMut { inner: v }))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

type MapxValues<'a, V> = MapxOrdValues<'a, V>;
type MapxValuesMut<'a, V> = MapxOrdValuesMut<'a, V>;

#[derive(Debug)]
pub struct ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    pub(crate) inner: mapx_ord_rawkey::ValueIterMut<'a, V>,
}

impl<'a, V> Deref for ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

impl<'a, V> DerefMut for ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.inner
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

impl<K, V> vsdb_core::VsMgmt for Mapx<K, V> {
    vsdb_core::impl_vs_methods_nope! {}
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
