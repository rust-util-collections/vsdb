//!
//! # A disk-storage replacement for the pure in-memory BTreeMap
//!
//! This module is non-invasive to external code except the `new` method.
//!

#[cfg(test)]
mod test;

use crate::{
    mapx_oc::{self, MapxOC, MapxOCIter},
    MetaInfo, SimpleVisitor,
};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cmp::Ordering,
    fmt,
    hash::Hash,
    iter::{DoubleEndedIterator, Iterator},
    marker::PhantomData,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct Mapx<K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
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
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! convert {
    ($as_param: expr) => {{
        bincode::serialize::<K>($as_param).unwrap()
    }};
    (@$as_ret: expr) => {{
        bincode::deserialize::<K>(&$as_ret).unwrap()
    }};
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
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
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
    fn get_meta(&self) -> MetaInfo {
        self.inner.get_meta()
    }

    /// Imitate the behavior of 'BTreeMap<_>.get(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&convert!(key))
    }

    /// Imitate the behavior of 'BTreeMap<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, K, V>> {
        let k = convert!(key);
        self.inner
            .get(&k)
            .map(move |v| ValueMut::new(self, key.clone(), v))
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

    ///
    /// # Safety
    ///
    /// Only make sense after a 'DataBase clear',
    /// do NOT use this function except testing.
    ///
    #[inline(always)]
    pub unsafe fn set_len(&mut self, len: u64) {
        self.inner.set_len(len);
    }

    /// Imitate the behavior of 'BTreeMap<_>.insert(...)'.
    #[inline(always)]
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let key = convert!(&key);
        self.inner.insert(key, value)
    }

    /// Similar with `insert`, but ignore the old value.
    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        let key = convert!(&key);
        self.inner.set_value(key, value);
    }

    /// Imitate the behavior of '.entry(...).or_insert(...)'
    #[inline(always)]
    pub fn entry(&mut self, key: K) -> mapx_oc::Entry<'_, Vec<u8>, V> {
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
}

/*******************************************/
// End of the self-implementation for Mapx //
/////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Begin of the implementation of ValueMut(returned by `self.get_mut`) for Mapx //
/********************************************************************************/

/// Returned by `<Mapx>.get_mut(...)`
#[derive(Debug)]
pub struct ValueMut<'a, K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    mapx: &'a mut Mapx<K, V>,
    key: ManuallyDrop<K>,
    value: ManuallyDrop<V>,
}

impl<'a, K, V> ValueMut<'a, K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn new(mapx: &'a mut Mapx<K, V>, key: K, value: V) -> Self {
        ValueMut {
            mapx,
            key: ManuallyDrop::new(key),
            value: ManuallyDrop::new(value),
        }
    }

    /// Clone the inner value.
    pub fn clone_inner(self) -> V {
        ManuallyDrop::into_inner(self.value.clone())
    }
}

///
/// **NOTE**: VERY IMPORTANT !!!
///
impl<'a, K, V> Drop for ValueMut<'a, K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn drop(&mut self) {
        // This operation is safe within a `drop()`.
        // SEE: [**ManuallyDrop::take**](std::mem::ManuallyDrop::take)
        unsafe {
            self.mapx.set_value(
                ManuallyDrop::take(&mut self.key),
                ManuallyDrop::take(&mut self.value),
            );
        };
    }
}

impl<'a, K, V> Deref for ValueMut<'a, K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K, V> DerefMut for ValueMut<'a, K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'a, K, V> PartialEq for ValueMut<'a, K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &ValueMut<'a, K, V>) -> bool {
        self.value == other.value
    }
}

impl<'a, K, V> PartialEq<V> for ValueMut<'a, K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &V) -> bool {
        self.value.deref() == other
    }
}

impl<'a, K, V> PartialOrd<V> for ValueMut<'a, K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Ord + PartialOrd + Serialize + DeserializeOwned + fmt::Debug,
{
    fn partial_cmp(&self, other: &V) -> Option<Ordering> {
        self.value.deref().partial_cmp(other)
    }
}

/******************************************************************************/
// End of the implementation of ValueMut(returned by `self.get_mut`) for Mapx //
////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////
// Begin of the implementation of Iter for Mapx //
/************************************************/

/// Iter over [Mapx](self::Mapx).
pub struct MapxIter<K, V>
where
    K: Clone + PartialEq + Eq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    iter: MapxOCIter<Vec<u8>, V>,
    _pd: PhantomData<K>,
}

impl<K, V> Iterator for MapxIter<K, V>
where
    K: Clone + PartialEq + Eq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (convert!(@k), v))
    }
}

impl<K, V> DoubleEndedIterator for MapxIter<K, V>
where
    K: Clone + PartialEq + Eq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
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
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let v = pnk!(bincode::serialize(&self.get_meta()));
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
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SimpleVisitor).map(|meta| {
            let meta = pnk!(bincode::deserialize::<MetaInfo>(&meta));
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
