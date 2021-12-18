use crate::{
    basic::mapx_raw::{MapxRaw, MapxRawIter},
    common::InstanceCfg,
    OrderConsistKey,
};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

// To solve the problem of unlimited memory usage,
// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(PartialEq, Eq, Debug)]
pub(super) struct MapxOrd<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    inner: MapxRaw,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

impl<K, V> From<InstanceCfg> for MapxOrd<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            inner: MapxRaw::from(cfg),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }
}

impl<K, V> From<&MapxOrd<K, V>> for InstanceCfg
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(x: &MapxOrd<K, V>) -> Self {
        let cfg = x.inner.get_instance_cfg();
        Self {
            prefix: cfg.prefix,
            item_cnt: cfg.item_cnt,
            area_idx: cfg.area_idx,
        }
    }
}

///////////////////////////////////////////////////////
// Begin of the self-implementation of backend::MapxOrd //
/*****************************************************/

impl<K, V> MapxOrd<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    // create a new instance
    #[inline(always)]
    pub(super) fn must_new() -> Self {
        MapxOrd {
            inner: MapxRaw::new(),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    // Get the storage path
    pub(super) fn get_instance_cfg(&self) -> InstanceCfg {
        InstanceCfg::from(self)
    }

    #[inline(always)]
    pub(super) fn get(&self, key: &K) -> Option<V> {
        self.inner
            .get(&key.to_bytes())
            .map(|bytes| pnk!(bcs::from_bytes(&bytes)))
    }

    #[inline(always)]
    pub(super) fn get_ref_bytes_k(&self, key: &[u8]) -> Option<V> {
        self.inner
            .get(key)
            .map(|bytes| pnk!(bcs::from_bytes(&bytes)))
    }

    #[inline(always)]
    pub(super) fn get_le(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_le(&key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), pnk!(bcs::from_bytes(&v))))
    }

    #[inline(always)]
    pub(super) fn get_le_ref_bytes_k(&self, key: &[u8]) -> Option<(K, V)> {
        self.inner
            .get_le(key)
            .map(|(k, v)| (pnk!(K::from_bytes(k)), pnk!(bcs::from_bytes(&v))))
    }

    #[inline(always)]
    pub(super) fn get_ge(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_ge(&key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), pnk!(bcs::from_bytes(&v))))
    }

    #[inline(always)]
    pub(super) fn get_ge_ref_bytes_k(&self, key: &[u8]) -> Option<(K, V)> {
        self.inner
            .get_ge(key)
            .map(|(k, v)| (pnk!(K::from_bytes(k)), pnk!(bcs::from_bytes(&v))))
    }

    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    pub(super) fn insert_ref(&mut self, key: &K, value: &V) -> Option<V> {
        self.set_value_ref(key, value)
            .map(|v| pnk!(bcs::from_bytes(&v)))
    }

    #[inline(always)]
    pub(super) fn insert_ref_bytes_k(&mut self, key: &[u8], value: &V) -> Option<V> {
        self.set_value_ref_bytes_k(key, value)
            .map(|v| pnk!(bcs::from_bytes(&v)))
    }

    #[inline(always)]
    pub(super) fn insert_ref_bytes_kv(&mut self, key: &[u8], value: &[u8]) -> Option<V> {
        self.set_value_ref_bytes_kv(key, value)
            .map(|v| pnk!(bcs::from_bytes(&v)))
    }

    #[inline(always)]
    pub(super) fn set_value_ref(&mut self, key: &K, value: &V) -> Option<Vec<u8>> {
        self.inner
            .insert(&key.to_bytes(), &pnk!(bcs::to_bytes(value)))
    }

    #[inline(always)]
    pub(super) fn set_value_ref_bytes_k(
        &mut self,
        key: &[u8],
        value: &V,
    ) -> Option<Vec<u8>> {
        self.inner.insert(key, &pnk!(bcs::to_bytes(value)))
    }

    #[inline(always)]
    pub(super) fn set_value_ref_bytes_kv(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Option<Vec<u8>> {
        self.inner.insert(key, value)
    }

    #[inline(always)]
    pub(super) fn iter(&self) -> MapxOrdIter<K, V> {
        MapxOrdIter {
            iter: self.inner.iter(),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> MapxOrdIter<K, V> {
        let ll;
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                ll = lo.to_bytes();
                Bound::Included(ll.as_slice())
            }
            Bound::Excluded(lo) => {
                ll = lo.to_bytes();
                Bound::Excluded(ll.as_slice())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let hh;
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                hh = hi.to_bytes();
                Bound::Included(hh.as_slice())
            }
            Bound::Excluded(hi) => {
                hh = hi.to_bytes();
                Bound::Excluded(hh.as_slice())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        MapxOrdIter {
            iter: self.inner.range((l, h)),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    #[inline(always)]
    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(&key.to_bytes())
    }

    #[inline(always)]
    pub(super) fn contains_key_ref_bytes_k(&self, key: &[u8]) -> bool {
        self.inner.contains_key(key)
    }

    #[inline(always)]
    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        self.unset_value(key).map(|v| pnk!(bcs::from_bytes(&v)))
    }

    #[inline(always)]
    pub(super) fn remove_ref_bytes_k(&mut self, key: &[u8]) -> Option<V> {
        self.unset_value_ref_bytes_k(key)
            .map(|v| pnk!(bcs::from_bytes(&v)))
    }

    #[inline(always)]
    pub(super) fn unset_value(&mut self, key: &K) -> Option<Vec<u8>> {
        self.inner.remove(&key.to_bytes())
    }

    #[inline(always)]
    pub(super) fn unset_value_ref_bytes_k(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.remove(key)
    }

    #[inline(always)]
    pub(super) fn clear(&mut self) {
        self.inner.clear();
    }
}

/***************************************************/
// End of the self-implementation of backend::MapxOrd //
/////////////////////////////////////////////////////

///////////////////////////////////////////////////////////
// Begin of the implementation of Iter for backend::MapxOrd //
/*********************************************************/

// Iter over [MapxOrd](self::Mapxnk).
pub(super) struct MapxOrdIter<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    pub(super) iter: MapxRawIter,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

impl<K, V> Iterator for MapxOrdIter<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(k, v)| (pnk!(K::from_bytes(k)), pnk!(bcs::from_bytes(&v))))
    }
}

impl<K, V> DoubleEndedIterator for MapxOrdIter<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|(k, v)| (pnk!(K::from_bytes(k)), pnk!(bcs::from_bytes(&v))))
    }
}

impl<K, V> ExactSizeIterator for MapxOrdIter<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
}

/*******************************************************/
// End of the implementation of Iter for backend::MapxOrd //
/////////////////////////////////////////////////////////
