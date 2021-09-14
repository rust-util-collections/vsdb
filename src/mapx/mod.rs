//!
//! # A disk-storage replacement for the pure in-memory BTreeMap
//!
//! This module is non-invasive to external code except the `new` method.
//!

mod backend;
#[cfg(test)]
mod test;

use crate::serde::{CacheMeta, CacheVisitor};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt,
    hash::Hash,
    iter::{DoubleEndedIterator, Iterator},
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    ptr,
    sync::Arc,
};

/// In-memory cache size in the number of items
pub const IN_MEM_CNT: usize = 2;

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(PartialEq, Debug, Clone)]
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
    memref: Arc<HashMap<K, V>>,
    in_disk: backend::Mapx<K, V>,
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
    pub fn new(path: &str) -> Result<Self> {
        let in_disk = backend::Mapx::load_or_create(path).c(d!())?;
        Ok(Mapx {
            memref: Arc::new(map! {}),
            in_disk,
        })
    }

    /// Get the database storage path
    pub fn get_root_path(&self) -> &str {
        self.in_disk.get_root_path()
    }

    /// Imitate the behavior of 'BTreeMap<_>.get(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<&V> {
        let hdr = Arc::as_ptr(&self.memref) as *mut HashMap<K, V>;
        unsafe {
            let r = (*hdr).get(key);
            if r.is_some() {
                return r;
            }
        }

        self.in_disk.get(key).map(|v| unsafe {
            (*hdr).insert(key.clone(), v);
            (*hdr).get(key).unwrap()
        })
    }

    /// Imitate the behavior of 'BTreeMap<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, K, V>> {
        self.in_disk
            .get(key)
            .map(move |v| ValueMut::new(self, key.clone(), v))
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
        self.clean_cache();
        self.in_disk.insert(key, value)
    }

    /// Similar with `insert`, but ignore if the old value is exist.
    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        self.clean_cache();
        self.in_disk.set_value(key, value);
    }

    /// Trigger the cache clean process
    /// **NOTE**: use `mut self` to make sure an unique access
    #[inline(always)]
    pub fn clean_cache(&self) {
        let hdr = Arc::as_ptr(&self.memref) as *mut HashMap<K, V>;
        unsafe {
            if !(*hdr).is_empty() {
                ptr::replace(hdr, map! {});
            }
        }
    }

    /// Imitate the behavior of '.entry(...).or_insert(...)'
    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        Entry { key, db: self }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_> {
        Box::new(MapxIter {
            iter: self.in_disk.iter(),
        })
    }

    /// Check if a key is exists.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.in_disk.contains_key(key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        unsafe {
            (*(Arc::as_ptr(&self.memref) as *mut HashMap<K, V>)).remove(key);
        }

        self.in_disk.remove(key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.in_disk.unset_value(key);
    }

    /// Flush data to disk
    #[inline(always)]
    pub fn flush_data(&mut self) {
        self.in_disk.flush();
        self.clean_cache();
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

///////////////////////////////////////////////////
// Begin of the implementation of Entry for Mapx //
/*************************************************/

/// Imitate the `btree_map/btree_map::Entry`.
pub struct Entry<'a, K, V>
where
    K: 'a
        + fmt::Debug
        + Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + Serialize
        + DeserializeOwned,
    V: 'a + fmt::Debug + Clone + PartialEq + Serialize + DeserializeOwned,
{
    key: K,
    db: &'a mut Mapx<K, V>,
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: 'a
        + fmt::Debug
        + Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + Serialize
        + DeserializeOwned,
    V: 'a + fmt::Debug + Clone + PartialEq + Serialize + DeserializeOwned,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert(self, default: V) -> ValueMut<'a, K, V> {
        if !self.db.contains_key(&self.key) {
            self.db.set_value(self.key.clone(), default);
        }
        pnk!(self.db.get_mut(&self.key))
    }

    /// Imitate the `btree_map/btree_map::Entry.or_insert_with(...)`.
    pub fn or_insert_with<F>(self, default: F) -> ValueMut<'a, K, V>
    where
        F: FnOnce() -> V,
    {
        if !self.db.contains_key(&self.key) {
            self.db.set_value(self.key.clone(), default());
        }
        pnk!(self.db.get_mut(&self.key))
    }
}

/***********************************************/
// End of the implementation of Entry for Mapx //
/////////////////////////////////////////////////

//////////////////////////////////////////////////
// Begin of the implementation of Iter for Mapx //
/************************************************/

/// Iter over [Mapx](self::Mapx).
pub struct MapxIter<'a, K, V>
where
    K: Clone + PartialEq + Eq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    iter: backend::MapxIter<'a, K, V>,
}

impl<'a, K, V> Iterator for MapxIter<'a, K, V>
where
    K: Clone + PartialEq + Eq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a, K, V> DoubleEndedIterator for MapxIter<'a, K, V>
where
    K: Clone + PartialEq + Eq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

/**********************************************/
// End of the implementation of Iter for Mapx //
////////////////////////////////////////////////

/////////////////////////////////////////////////////////
// Begin of the implementation of Eq for Mapx //
/*******************************************************/

impl<K, V> Eq for Mapx<K, V>
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
}

/*****************************************************/
// End of the implementation of Eq for Mapx //
///////////////////////////////////////////////////////

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
        let v = pnk!(serde_json::to_string(&CacheMeta {
            root_path: self.get_root_path(),
        }));

        self.in_disk.flush();
        serializer.serialize_str(&v)
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
        deserializer.deserialize_str(CacheVisitor).map(|meta| {
            let meta = pnk!(serde_json::from_str::<CacheMeta>(&meta));
            pnk!(Mapx::new(meta.root_path))
        })
    }
}

/***************************************************************/
// End of the implementation of Serialize/Deserialize for Mapx //
/////////////////////////////////////////////////////////////////
