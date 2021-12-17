//!
//! A disk-storage replacement for the pure in-memory BTreeMap.
//!

#[cfg(test)]
mod test;

use crate::{
    basic::mapx_oc::{Entry, MapxOC, MapxOCIter, ValueMut},
    common::{InstanceCfg, SimpleVisitor},
};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt, marker::PhantomData};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(PartialEq, Eq, Debug)]
pub struct Mapx<K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    inner: MapxOC<Vec<u8>, V>,
    _pd: PhantomData<K>,
}

impl<K, V> Default for Mapx<K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! convert {
    ($as_param: expr) => {{ bcs::to_bytes::<K>($as_param).unwrap() }};
    (@$as_ret: expr) => {{ bcs::from_bytes::<K>(&$as_ret).unwrap() }};
}

///////////////////////////////////////////////
// Begin of the self-implementation for Mapx //
/*********************************************/

impl<K, V> Mapx<K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        Mapx {
            inner: MapxOC::new(),
            _pd: PhantomData,
        }
    }

    // Get the database storage path
    fn get_instance_cfg(&self) -> InstanceCfg {
        self.inner.get_instance_cfg()
    }

    /// Imitate the behavior of 'BTreeMap<_>.get(...)'
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&convert!(key))
    }

    /// Imitate the behavior of 'BTreeMap<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, Vec<u8>, V>> {
        let k = convert!(key);
        self.inner
            .get(&k)
            .map(move |v| ValueMut::new(&mut self.inner, k, v))
    }

    /// Imitate the behavior of 'BTreeMap<_>.len()'.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// A helper func
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Imitate the behavior of 'BTreeMap<_>.insert(...)'.
    #[inline(always)]
    pub fn insert(&mut self, key: K, value: &V) -> Option<V> {
        let key = convert!(&key);
        self.inner.insert(key, value)
    }

    /// Similar with `insert`, but ignore the old value.
    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: &V) {
        let key = convert!(&key);
        self.inner.set_value(key, value);
    }

    /// Imitate the behavior of '.entry(...).or_insert(...)'
    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, Vec<u8>, V> {
        let key = convert!(&key);
        self.inner.entry(key)
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> MapxIter<K, V> {
        MapxIter {
            iter: self.inner.iter(),
            _pd: PhantomData,
        }
    }

    /// Check if a key is exists.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        let key = convert!(key);
        self.inner.contains_key(&key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let key = convert!(key);
        self.inner.remove(&key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        let key = convert!(key);
        self.inner.unset_value(&key);
    }

    /// Clear all data.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

/*******************************************/
// End of the self-implementation for Mapx //
/////////////////////////////////////////////

//////////////////////////////////////////////////
// Begin of the implementation of Iter for Mapx //
/************************************************/

/// Iter over [Mapx](self::Mapx).
pub struct MapxIter<K, V>
where
    K: PartialEq + Eq + Serialize + DeserializeOwned + fmt::Debug,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    iter: MapxOCIter<Vec<u8>, V>,
    _pd: PhantomData<K>,
}

impl<K, V> Iterator for MapxIter<K, V>
where
    K: PartialEq + Eq + Serialize + DeserializeOwned + fmt::Debug,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (convert!(@k), v))
    }
}

impl<K, V> DoubleEndedIterator for MapxIter<K, V>
where
    K: PartialEq + Eq + Serialize + DeserializeOwned + fmt::Debug,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|(k, v)| (convert!(@k), v))
    }
}

/**********************************************/
// End of the implementation of Iter for Mapx //
////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////
// Begin of the implementation of Serialize/Deserialize for Mapx //
/*****************************************************************/

impl<K, V> serde::Serialize for Mapx<K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let v = pnk!(bcs::to_bytes(&self.get_instance_cfg()));
        serializer.serialize_bytes(&v)
    }
}

impl<'de, K, V> serde::Deserialize<'de> for Mapx<K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SimpleVisitor).map(|meta| {
            let meta = pnk!(bcs::from_bytes::<InstanceCfg>(&meta));
            Mapx {
                inner: MapxOC::from(meta),
                _pd: PhantomData,
            }
        })
    }
}

/***************************************************************/
// End of the implementation of Serialize/Deserialize for Mapx //
/////////////////////////////////////////////////////////////////
