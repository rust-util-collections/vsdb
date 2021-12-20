//!
//! NOTE: Documents => [MapxVersionedRaw](crate::versioned::mapx_raw)
//!

// TODO

use crate::{
    common::ende::{KeyEnDe, ValueEnDe},
    versioned::mapx_ord_rawkey::{
        Entry, MapxOrdRawKeyVersioned, MapxOrdRawKeyVersionedIter, ValueMut,
    },
    BranchName, ParentBranchName, VerChecksum, VersionName,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxVersioned<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyVersioned<V>,
    pk: PhantomData<K>,
}

impl<K, V> Default for MapxVersioned<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> MapxVersioned<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    #[inline(always)]
    pub fn new() -> Self {
        MapxVersioned {
            inner: MapxOrdRawKeyVersioned::new(),
            pk: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&key.encode())
    }

    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_le(&key.encode())
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), v))
    }

    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_ge(&key.encode())
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), v))
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, V>> {
        let k = key.encode();
        self.inner
            .get(&k)
            .map(|v| ValueMut::new(&mut self.inner, k, v))
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
    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>> {
        self.insert_ref(&key, &value).c(d!())
    }

    #[inline(always)]
    pub fn insert_ref(&mut self, key: &K, value: &V) -> Result<Option<V>> {
        self.inner.insert_ref(&key.encode(), value).c(d!())
    }

    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, V> {
        Entry {
            key: key.encode(),
            hdr: &mut self.inner,
        }
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxVersionedIter<K, V> {
        MapxVersionedIter {
            iter: self.inner.iter(),
            pk: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range<'a, R: 'a + RangeBounds<K>>(
        &'a self,
        bounds: R,
    ) -> MapxVersionedIter<'a, K, V> {
        let l = match bounds.start_bound() {
            Bound::Included(i) => Bound::Included(i.encode()),
            Bound::Excluded(i) => Bound::Excluded(i.encode()),
            _ => Bound::Unbounded,
        };
        let h = match bounds.end_bound() {
            Bound::Included(i) => Bound::Included(i.encode()),
            Bound::Excluded(i) => Bound::Excluded(i.encode()),
            _ => Bound::Unbounded,
        };

        MapxVersionedIter {
            iter: self.inner.range((l, h)),
            pk: PhantomData,
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
        self.inner.contains_key(&key.encode())
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Result<Option<V>> {
        self.inner.remove(&key.encode()).c(d!())
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    crate::impl_vcs_methods!();
}

pub struct MapxVersionedIter<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    iter: MapxOrdRawKeyVersionedIter<'a, V>,
    pk: PhantomData<K>,
}

impl<'a, K, V> Iterator for MapxVersionedIter<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), v))
    }
}

impl<'a, K, V> DoubleEndedIterator for MapxVersionedIter<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), v))
    }
}

impl<'a, K, V> ExactSizeIterator for MapxVersionedIter<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
}
