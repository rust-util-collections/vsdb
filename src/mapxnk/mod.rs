//!
//! # A disk-storage replacement for the pure in-memory BTreeMap
//!
//! This module is non-invasive to external code except the `new` method.
//!

mod backend;
#[cfg(test)]
mod test;

use crate::{
    serde::{CacheMeta, CacheVisitor},
    NumKey,
};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cmp::Ordering,
    fmt,
    iter::Iterator,
    mem::ManuallyDrop,
    ops::{Bound, RangeBounds},
    ops::{Deref, DerefMut},
};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(PartialEq, Debug, Clone)]
pub struct Mapxnk<K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    in_disk: backend::Mapxnk<K, V>,
}

///////////////////////////////////////////////
// Begin of the self-implementation for Mapxnk //
/*********************************************/

impl<K, V> Mapxnk<K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new(path: &str) -> Result<Self> {
        let in_disk = backend::Mapxnk::load_or_create(path).c(d!())?;
        Ok(Mapxnk { in_disk })
    }

    /// Get the database storage path
    pub fn get_path(&self) -> &str {
        self.in_disk.get_path()
    }

    /// Imitate the behavior of 'BTreeMap<_>.get(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.in_disk.get(key)
    }

    /// Get the closest smaller value
    #[inline(always)]
    pub fn get_closest_smaller(&self, key: &K) -> Option<(K, V)> {
        self.in_disk.get_closest_smaller(key)
    }

    /// Get the closest larger value
    #[inline(always)]
    pub fn get_closest_larger(&self, key: &K) -> Option<(K, V)> {
        self.in_disk.get_closest_larger(key)
    }

    /// Imitate the behavior of 'BTreeMap<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, K, V>> {
        self.in_disk
            .get(key)
            .map(move |v| ValueMut::new(self, *key, v))
    }

    /// Imitate the behavior of 'BTreeMap<_>.len()'.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.in_disk.len()
    }

    /// A helper func
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.in_disk.is_empty()
    }

    /// Imitate the behavior of 'BTreeMap<_>.insert(...)'.
    #[inline(always)]
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.in_disk.insert(key, value)
    }

    /// Similar with `insert`, but ignore the old value.
    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        self.in_disk.set_value(key, value);
    }

    /// Imitate the behavior of '.entry(...).or_insert(...)'
    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        Entry { key, db: self }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> MapxnkIter<'_, K, V> {
        MapxnkIter {
            hi: Bound::Unbounded,
            lo: Bound::Unbounded,
            iter: self.in_disk.iter(),
        }
    }

    /// range(start..end)
    pub fn range<R: RangeBounds<K>>(&self, range: R) -> MapxnkIter<'_, K, V> {
        let hi = range.end_bound().cloned();
        let lo = range.start_bound().cloned();

        let iter = match lo {
            Bound::Unbounded => self.in_disk.iter(),
            Bound::Included(k) => self.in_disk.iter_from(&k),
            Bound::Excluded(k) => self.in_disk.iter_from(&k),
        };

        MapxnkIter { hi, lo, iter }
    }

    /// Check if a key is exists.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.in_disk.contains_key(key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.in_disk.remove(key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.in_disk.unset_value(key);
    }
}

/*******************************************/
// End of the self-implementation for Mapxnk //
/////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Begin of the implementation of ValueMut(returned by `self.get_mut`) for Mapxnk //
/********************************************************************************/

/// Returned by `<Mapxnk>.get_mut(...)`
#[derive(Debug)]
pub struct ValueMut<'a, K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    mapxnk: &'a mut Mapxnk<K, V>,
    key: ManuallyDrop<K>,
    value: ManuallyDrop<V>,
}

impl<'a, K, V> ValueMut<'a, K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn new(mapxnk: &'a mut Mapxnk<K, V>, key: K, value: V) -> Self {
        ValueMut {
            mapxnk,
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
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn drop(&mut self) {
        // This operation is safe within a `drop()`.
        // SEE: [**ManuallyDrop::take**](std::mem::ManuallyDrop::take)
        unsafe {
            self.mapxnk.set_value(
                ManuallyDrop::take(&mut self.key),
                ManuallyDrop::take(&mut self.value),
            );
        };
    }
}

impl<'a, K, V> Deref for ValueMut<'a, K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K, V> DerefMut for ValueMut<'a, K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'a, K, V> PartialEq for ValueMut<'a, K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &ValueMut<'a, K, V>) -> bool {
        self.value == other.value
    }
}

impl<'a, K, V> PartialEq<V> for ValueMut<'a, K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &V) -> bool {
        self.value.deref() == other
    }
}

impl<'a, K, V> PartialOrd<V> for ValueMut<'a, K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Ord + PartialOrd + Serialize + DeserializeOwned + fmt::Debug,
{
    fn partial_cmp(&self, other: &V) -> Option<Ordering> {
        self.value.deref().partial_cmp(other)
    }
}

/******************************************************************************/
// End of the implementation of ValueMut(returned by `self.get_mut`) for Mapxnk //
////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////
// Begin of the implementation of Entry for Mapxnk //
/*************************************************/

/// Imitate the `btree_map/btree_map::Entry`.
pub struct Entry<'a, K, V>
where
    K: NumKey,
    V: 'a + fmt::Debug + Clone + PartialEq + Serialize + DeserializeOwned,
{
    key: K,
    db: &'a mut Mapxnk<K, V>,
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: NumKey,
    V: 'a + fmt::Debug + Clone + PartialEq + Serialize + DeserializeOwned,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert(self, default: V) -> ValueMut<'a, K, V> {
        if !self.db.contains_key(&self.key) {
            self.db.set_value(self.key, default);
        }
        pnk!(self.db.get_mut(&self.key))
    }

    /// Imitate the `btree_map/btree_map::Entry.or_insert_with(...)`.
    pub fn or_insert_with<F>(self, default: F) -> ValueMut<'a, K, V>
    where
        F: FnOnce() -> V,
    {
        if !self.db.contains_key(&self.key) {
            self.db.set_value(self.key, default());
        }
        pnk!(self.db.get_mut(&self.key))
    }
}

/***********************************************/
// End of the implementation of Entry for Mapxnk //
/////////////////////////////////////////////////

//////////////////////////////////////////////////
// Begin of the implementation of Iter for Mapxnk //
/************************************************/

/// Iter over [Mapxnk](self::Mapxnk).
pub struct MapxnkIter<'a, K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    hi: Bound<K>,
    lo: Bound<K>,
    iter: backend::MapxnkIter<'a, K, V>,
}

impl<'a, K, V> Iterator for MapxnkIter<'a, K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|(k, v)| {
            if let Bound::Excluded(lo) = self.lo {
                alt!(k == lo, return self.next());
            }
            match self.hi {
                Bound::Unbounded => {}
                Bound::Included(hi) => alt!(hi < k, return None),
                Bound::Excluded(hi) => alt!(hi <= k, return None),
            }
            Some((k, v))
        })
    }
}

/**********************************************/
// End of the implementation of Iter for Mapxnk //
////////////////////////////////////////////////

/////////////////////////////////////////////////////////
// Begin of the implementation of Eq for Mapxnk //
/*******************************************************/

impl<K, V> Eq for Mapxnk<K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
}

/*****************************************************/
// End of the implementation of Eq for Mapxnk //
///////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////
// Begin of the implementation of Serialize/Deserialize for Mapxnk //
/*****************************************************************/

impl<K, V> serde::Serialize for Mapxnk<K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let v = pnk!(serde_json::to_string(&CacheMeta {
            path: self.get_path(),
        }));

        serializer.serialize_str(&v)
    }
}

impl<'de, K, V> serde::Deserialize<'de> for Mapxnk<K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(CacheVisitor).map(|meta| {
            let meta = pnk!(serde_json::from_str::<CacheMeta>(&meta));
            pnk!(Mapxnk::new(meta.path))
        })
    }
}

/***************************************************************/
// End of the implementation of Serialize/Deserialize for Mapxnk //
/////////////////////////////////////////////////////////////////
