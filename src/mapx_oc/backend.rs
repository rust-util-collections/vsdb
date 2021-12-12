//!
//! # Disk Storage Implementation
//!

use crate::{MetaInfo, OrderConsistKey, VSDB};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use sled::{IVec, Iter};
use std::{
    fmt,
    iter::{DoubleEndedIterator, Iterator},
    marker::PhantomData,
    mem::size_of,
    ops::{Bound, RangeBounds},
};

// To solve the problem of unlimited memory usage,
// use this to replace the original in-memory `HashMap<_, _>`.
#[derive(Debug, Clone)]
pub(super) struct MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    cnter: u64,
    id: u64,
    idx: usize,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

impl<K, V> From<MetaInfo> for MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(mi: MetaInfo) -> Self {
        Self {
            cnter: mi.item_cnt,
            id: mi.obj_id,
            idx: mi.tree_idx,
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }
}

impl<K, V> From<&MapxOC<K, V>> for MetaInfo
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(x: &MapxOC<K, V>) -> Self {
        Self {
            item_cnt: x.cnter,
            obj_id: x.id,
            tree_idx: x.idx,
        }
    }
}

///////////////////////////////////////////////////////
// Begin of the self-implementation of backend::MapxOC //
/*****************************************************/

impl<K, V> MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    // create a new instance
    #[inline(always)]
    pub(super) fn must_new(id: u64) -> Self {
        let idx = id as usize % VSDB.trees.len();

        assert!(VSDB.trees[idx]
            .scan_prefix(id.to_be_bytes())
            .next()
            .is_none());

        MapxOC {
            cnter: 0,
            id,
            idx,
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    // Get the storage path
    pub(super) fn get_meta(&self) -> MetaInfo {
        MetaInfo::from(self)
    }

    // Imitate the behavior of 'HashMap<_>.get(...)'
    #[inline(always)]
    pub(super) fn get(&self, key: &K) -> Option<V> {
        let mut k = self.id.to_be_bytes().to_vec();
        k.append(&mut key.to_bytes());
        VSDB.trees[self.idx]
            .get(k)
            .ok()
            .flatten()
            .map(|bytes| pnk!(bincode::deserialize(&bytes)))
    }

    #[inline(always)]
    pub(super) fn get_closest_smaller(&self, key: &K) -> Option<(K, V)> {
        let mut k = self.id.to_be_bytes().to_vec();
        k.append(&mut key.to_bytes());

        VSDB.trees[self.idx]
            .range(..=k)
            .next_back()
            .map(|i| i.unwrap())
            .map(|(k, v)| {
                (
                    pnk!(K::from_bytes(&k[size_of::<u64>()..])),
                    pnk!(bincode::deserialize(&v)),
                )
            })
    }

    #[inline(always)]
    pub(super) fn get_closest_larger(&self, key: &K) -> Option<(K, V)> {
        let mut k = self.id.to_be_bytes().to_vec();
        k.append(&mut key.to_bytes());

        VSDB.trees[self.idx]
            .range(k..)
            .next()
            .map(|i| i.unwrap())
            .map(|(k, v)| {
                (
                    pnk!(K::from_bytes(&k[size_of::<u64>()..])),
                    pnk!(bincode::deserialize(&v)),
                )
            })
    }

    // Imitate the behavior of 'HashMap<_>.len()'.
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        debug_assert_eq!(
            VSDB.trees[self.idx]
                .scan_prefix(self.id.to_be_bytes())
                .count(),
            self.cnter as usize
        );
        self.cnter as usize
    }

    // A helper func
    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        VSDB.trees[self.idx]
            .scan_prefix(self.id.to_be_bytes())
            .next()
            .is_none()
    }

    // Imitate the behavior of 'HashMap<_>.insert(...)'.
    #[inline(always)]
    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.set_value(key, value)
            .map(|v| pnk!(bincode::deserialize(&v)))
    }

    // Similar with `insert`, but ignore if the old value is exist.
    #[inline(always)]
    pub(super) fn set_value(&mut self, key: K, value: V) -> Option<IVec> {
        let mut k = self.id.to_be_bytes().to_vec();
        k.append(&mut key.to_bytes());
        let v = pnk!(bincode::serialize(&value));
        let old_v = pnk!(VSDB.trees[self.idx].get(&k));

        pnk!(VSDB.trees[self.idx].insert(k, v));

        if old_v.is_none() {
            self.cnter += 1;
        }

        old_v
    }

    // Imitate the behavior of '.iter()'
    #[inline(always)]
    pub(super) fn iter(&self) -> MapxOCIter<K, V> {
        let i = VSDB.trees[self.idx].scan_prefix(self.id.to_be_bytes());
        MapxOCIter {
            iter: i,
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> MapxOCIter<K, V> {
        let mut b_lo = self.id.to_be_bytes().to_vec();
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                b_lo.append(&mut lo.to_bytes());
                Bound::Included(b_lo)
            }
            Bound::Excluded(lo) => {
                b_lo.append(&mut lo.to_bytes());
                Bound::Excluded(b_lo)
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let mut b_hi = self.id.to_be_bytes().to_vec();
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.append(&mut hi.to_bytes());
                Bound::Included(b_hi)
            }
            Bound::Excluded(hi) => {
                b_hi.append(&mut hi.to_bytes());
                Bound::Excluded(b_hi)
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        MapxOCIter {
            iter: VSDB.trees[self.idx].range((l, h)),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    pub(super) fn contains_key(&self, key: &K) -> bool {
        let mut k = self.id.to_be_bytes().to_vec();
        k.append(&mut key.to_bytes());
        pnk!(VSDB.trees[self.idx].contains_key(k))
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        self.unset_value(key)
            .map(|v| pnk!(bincode::deserialize(&v)))
    }

    pub(super) fn unset_value(&mut self, key: &K) -> Option<IVec> {
        let mut k = self.id.to_be_bytes().to_vec();
        k.append(&mut key.to_bytes());
        let old_v = pnk!(VSDB.trees[self.idx].get(&k));

        pnk!(VSDB.trees[self.idx].remove(k));

        if old_v.is_some() {
            self.cnter -= 1;
        }

        old_v
    }
}

/***************************************************/
// End of the self-implementation of backend::MapxOC //
/////////////////////////////////////////////////////

///////////////////////////////////////////////////////////
// Begin of the implementation of Iter for backend::MapxOC //
/*********************************************************/

// Iter over [MapxOC](self::Mapxnk).
pub(super) struct MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    pub(super) iter: Iter,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

impl<K, V> Iterator for MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|i| i.unwrap()).map(|(k, v)| {
            (
                pnk!(K::from_bytes(&k[size_of::<u64>()..])),
                pnk!(bincode::deserialize(&v)),
            )
        })
    }
}

impl<K, V> DoubleEndedIterator for MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|i| i.unwrap()).map(|(k, v)| {
            (
                pnk!(K::from_bytes(&k[size_of::<u64>()..])),
                pnk!(bincode::deserialize(&v)),
            )
        })
    }
}

impl<K, V> ExactSizeIterator for MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
}

/*******************************************************/
// End of the implementation of Iter for backend::MapxOC //
/////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////
// Begin of the implementation of Eq for backend::MapxOC //
/*******************************************************/

impl<K, V> PartialEq for MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &MapxOC<K, V>) -> bool {
        !self.iter().zip(other.iter()).any(|(i, j)| i != j)
    }
}

impl<K, V> Eq for MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
}

/*****************************************************/
// End of the implementation of Eq for backend::MapxOC //
///////////////////////////////////////////////////////
