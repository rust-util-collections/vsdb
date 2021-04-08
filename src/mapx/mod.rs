//!
//! # A mem+disk replacement for the pure in-memory HashMap
//!
//! This module is non-invasive to external code except the `new` method.
//!

mod backend;
#[cfg(test)]
mod test;

use crate::{
    helper::*,
    serde::{CacheMeta, CacheVisitor},
};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{hash_map, HashMap},
    fmt,
    hash::Hash,
    iter::Iterator,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
};

/// Max number of entries stored in memory.
#[cfg(not(feature = "debug_env"))]
pub const IN_MEM_CNT: usize = 2_0000;

/// To make the 'mix storage' to be triggered during tests,
/// set it to 1 with the debug_env feature.
#[cfg(feature = "debug_env")]
pub const IN_MEM_CNT: usize = 1;

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `HashMap<_, _>`.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct Mapx<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    in_mem: HashMap<K, V>,
    in_mem_cnt: usize,
    in_disk: backend::Mapx<K, V>,
}

///////////////////////////////////////////////
// Begin of the self-implementation for Mapx //
/*********************************************/

impl<K, V> Mapx<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new(path: String, imc: Option<usize>, is_tmp: bool) -> Result<Self> {
        let in_disk = backend::Mapx::load_or_create(path, is_tmp).c(d!())?;

        let mut in_mem = HashMap::with_capacity(IN_MEM_CNT);
        let mut cnter = IN_MEM_CNT;
        let mut data = in_disk.iter().rev();
        while cnter > 0 {
            if let Some((k, v)) = data.next() {
                in_mem.insert(k, v);
            } else {
                break;
            }
            cnter -= 1;
        }

        Ok(Mapx {
            in_mem,
            in_mem_cnt: imc.unwrap_or(IN_MEM_CNT),
            in_disk,
        })
    }

    /// Get the database storage path
    pub fn get_data_path(&self) -> &str {
        self.in_disk.get_data_path()
    }

    /// Imitate the behavior of 'HashMap<_>.get(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<Value<V>> {
        self.in_mem
            .get(key)
            .map(Cow::Borrowed)
            .or_else(|| self.in_disk.get(key).map(Cow::Owned))
            .map(Value::new)
    }

    /// Imitate the behavior of 'HashMap<_>.get_mut(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<K, V>> {
        self.in_mem
            .get(key)
            .cloned()
            .or_else(|| self.in_disk.get(key))
            .map(move |v| ValueMut::new(self, key.clone(), v))
    }

    /// Imitate the behavior of 'HashMap<_>.len()'.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.in_disk.len()
    }

    /// A helper func
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.in_disk.is_empty()
    }

    /// Imitate the behavior of 'HashMap<_>.insert(...)'.
    #[inline(always)]
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.mgmt_memory();
        if let Some(v) = self.in_mem.insert(key.clone(), value.clone()) {
            self.in_disk.set_value(key, value);
            Some(v)
        } else {
            self.in_disk.insert(key, value)
        }
    }

    /// Similar with `insert`, but ignore if the old value is exist.
    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        self.mgmt_memory();
        self.in_disk.set_value(key.clone(), value.clone());
        self.in_mem.insert(key, value);
    }

    // Will get a random key since we use HashMap
    fn mgmt_memory(&mut self) {
        if self.in_mem.len() > IN_MEM_CNT {
            pnk!(self
                .in_mem
                .keys()
                .next()
                .cloned()
                .and_then(|k| self.in_mem.remove(&k)));
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
        debug_assert!(self.in_mem.len() <= self.in_disk.len());
        if self.in_mem.len() == self.in_disk.len() {
            Box::new(MapxIterMem {
                iter: self.in_mem.iter(),
            })
        } else {
            Box::new(MapxIter {
                iter: self.in_disk.iter(),
            })
        }
    }

    /// Check if a key is exists.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        let at_mem = self.in_mem.contains_key(key);
        if at_mem {
            at_mem
        } else {
            self.in_disk.contains_key(key)
        }
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(v) = self.in_mem.remove(key) {
            self.in_disk.unset_value(key);
            Some(v)
        } else {
            self.in_disk.remove(key)
        }
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.in_mem.remove(key);
        self.in_disk.unset_value(key);
    }

    /// Flush data to disk
    #[inline(always)]
    pub fn flush_data(&self) {
        self.in_disk.flush();
    }
}

/*******************************************/
// End of the self-implementation for Mapx //
/////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Begin of the implementation of ValueMut(returned by `self.get_mut`) for Mapx //
/********************************************************************************/

/// Returned by `<Mapx>.get_mut(...)`
#[derive(Eq, Debug)]
pub struct ValueMut<'a, K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    mapx: &'a mut Mapx<K, V>,
    key: ManuallyDrop<K>,
    value: ManuallyDrop<V>,
}

impl<'a, K, V> ValueMut<'a, K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
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
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn drop(&mut self) {
        // This operation is safe within a `drop()`.
        // SEE: [**ManuallyDrop::take**](std::mem::ManuallyDrop::take)
        let (k, v) = unsafe {
            (
                ManuallyDrop::take(&mut self.key),
                ManuallyDrop::take(&mut self.value),
            )
        };
        self.mapx.set_value(k, v);
    }
}

impl<'a, K, V> Deref for ValueMut<'a, K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K, V> DerefMut for ValueMut<'a, K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'a, K, V> PartialEq for ValueMut<'a, K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &ValueMut<'a, K, V>) -> bool {
        self.value == other.value
    }
}

impl<'a, K, V> PartialEq<V> for ValueMut<'a, K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &V) -> bool {
        self.value.deref() == other
    }
}

impl<'a, K, V> PartialOrd<V> for ValueMut<'a, K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone
        + Eq
        + PartialEq
        + Ord
        + PartialOrd
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
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

/// Imitate the `btree_map/hash_map::Entry`.
pub struct Entry<'a, K, V>
where
    K: fmt::Debug + Clone + 'a + Eq + PartialEq + Hash + Serialize + DeserializeOwned,
    V: fmt::Debug + Clone + 'a + Eq + PartialEq + Serialize + DeserializeOwned,
{
    key: K,
    db: &'a mut Mapx<K, V>,
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: fmt::Debug + Clone + 'a + Eq + PartialEq + Hash + Serialize + DeserializeOwned,
    V: fmt::Debug + Clone + 'a + Eq + PartialEq + Serialize + DeserializeOwned,
{
    /// Imitate the `btree_map/hash_map::Entry.or_insert(...)`.
    pub fn or_insert(self, default: V) -> ValueMut<'a, K, V> {
        if !self.db.contains_key(&self.key) {
            self.db.set_value(self.key.clone(), default);
        }
        pnk!(self.db.get_mut(&self.key))
    }

    /// Imitate the `btree_map/hash_map::Entry.or_insert_with(...)`.
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
pub struct MapxIter<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    iter: backend::MapxIter<K, V>,
}

impl<K, V> Iterator for MapxIter<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// Iter over [Mapx](self::Mapx).
pub struct MapxIterMem<'a, K, V>
where
    K: 'a + Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: 'a + Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    iter: hash_map::Iter<'a, K, V>,
}

impl<'a, K, V> Iterator for MapxIterMem<'a, K, V>
where
    K: 'a + Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: 'a + Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (k.clone(), v.clone()))
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
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let v = pnk!(serde_json::to_string(&CacheMeta {
            in_mem_cnt: self.in_mem_cnt,
            data_path: self.get_data_path(),
        }));

        self.flush_data();
        serializer.serialize_str(&v)
    }
}

impl<'de, K, V> serde::Deserialize<'de> for Mapx<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + Eq + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(CacheVisitor).map(|meta| {
            let meta = pnk!(serde_json::from_str::<CacheMeta>(&meta));
            pnk!(Mapx::new(
                meta.data_path.to_owned(),
                Some(meta.in_mem_cnt),
                false
            ))
        })
    }
}

/***************************************************************/
// End of the implementation of Serialize/Deserialize for Mapx //
/////////////////////////////////////////////////////////////////
