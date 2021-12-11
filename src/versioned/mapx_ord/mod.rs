//!
//! NOTE: Documents => [MapxRaw](crate::versioned::mapx_raw)
//!

// TODO

use crate::{
    common::ende::{KeyEnDeOrdered, ValueEnDe},
    versioned::mapx_ord_rawkey::{
        Entry, MapxOrdRawKeyVs, MapxOrdRawKeyVsIter, ValueMut,
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
pub struct MapxOrdVs<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyVs<V>,
    pk: PhantomData<K>,
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

impl<K, V> MapxOrdVs<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    #[inline(always)]
    pub fn new() -> Self {
        MapxOrdVs {
            inner: MapxOrdRawKeyVs::new(),
            pk: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&key.to_bytes())
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
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, V>> {
        let k = key.to_bytes();
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
        self.inner.insert_ref(&key.to_bytes(), value).c(d!())
    }

    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, V> {
        Entry {
            key: key.to_bytes(),
            hdr: &mut self.inner,
        }
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxOrdVsIter<K, V> {
        MapxOrdVsIter {
            iter: self.inner.iter(),
            pk: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range<'a, R: 'a + RangeBounds<K>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdVsIter<'a, K, V> {
        let l = match bounds.start_bound() {
            Bound::Included(i) => Bound::Included(i.to_bytes()),
            Bound::Excluded(i) => Bound::Excluded(i.to_bytes()),
            _ => Bound::Unbounded,
        };
        let h = match bounds.end_bound() {
            Bound::Included(i) => Bound::Included(i.to_bytes()),
            Bound::Excluded(i) => Bound::Excluded(i.to_bytes()),
            _ => Bound::Unbounded,
        };

        MapxOrdVsIter {
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

    crate::impl_vcs_methods!();
}

pub struct MapxOrdVsIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    iter: MapxOrdRawKeyVsIter<'a, V>,
    pk: PhantomData<K>,
}

impl<'a, K, V> Iterator for MapxOrdVsIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

impl<'a, K, V> DoubleEndedIterator for MapxOrdVsIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

impl<'a, K, V> ExactSizeIterator for MapxOrdVsIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
}
