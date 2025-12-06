//!
//! A `HashMap`-like structure that stores data on disk.
//!
//! `Mapx` provides a key-value store where both keys and values are encoded
//! using `serde`-like methods before being persisted. This allows for storing
//! complex data types while maintaining a familiar `HashMap` interface.
//!
//! # Examples
//!
//! ```
//! use vsdb::{Mapx, vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut m: Mapx<i32, String> = Mapx::new();
//!
//! // Insert key-value pairs
//! m.insert(&1, &"hello".to_string());
//! m.insert(&2, &"world".to_string());
//!
//! // Retrieve a value
//! assert_eq!(m.get(&1), Some("hello".to_string()));
//!
//! // Iterate over the map
//! for (k, v) in m.iter() {
//!     println!("key: {}, val: {}", k, v);
//! }
//!
//! // Remove a key-value pair
//! m.remove(&2);
//!
//! // Clear the entire map
//! m.clear();
//!
//! // Clean up the directory
//! fs::remove_dir_all(vsdb_get_base_dir()).unwrap();
//! ```
//!

#[cfg(test)]
mod test;

use crate::{
    basic::{
        mapx_ord::{Entry, MapxOrdValues, MapxOrdValuesMut},
        mapx_ord_rawkey::{
            self, MapxOrdRawKey, MapxOrdRawKeyBatchEntry, MapxOrdRawKeyIter,
            MapxOrdRawKeyIterMut, ValueMut,
        },
    },
    common::ende::{KeyEnDe, ValueEnDe},
    define_map_wrapper,
};
use ruc::*;
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

define_map_wrapper! {
    #[doc = "A disk-based, `HashMap`-like data structure with typed keys and values."]
    #[doc = ""]
    #[doc = "`Mapx` stores key-value pairs on disk, encoding both keys and values"]
    #[doc = "for type safety and persistence."]
    pub struct Mapx<K, V> {
        inner: MapxOrdRawKey<V>,
        _p: PhantomData<K>,
    }
    where K: KeyEnDe, V: ValueEnDe
}

impl<K, V> Mapx<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    /// Retrieves a value from the map for a given key.
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(key.encode())
    }

    /// Retrieves a mutable reference to a value in the map.
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, V>> {
        self.inner.get_mut(key.encode())
    }

    /// Checks if the map contains a value for the specified key.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key.encode())
    }

    /// Inserts a key-value pair into the map.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn insert(&mut self, key: &K, value: &V) {
        self.inner.insert(key.encode(), value)
    }

    /// Sets the value for a key, overwriting any existing value.
    #[inline(always)]
    pub fn set_value(&mut self, key: &K, value: &V) {
        self.inner.set_value(key.encode(), value);
    }

    /// Gets an entry for a given key, allowing for in-place modification.
    #[inline(always)]
    pub fn entry(&mut self, key: &K) -> Entry<'_, V> {
        Entry {
            key: key.encode(),
            hdr: &mut self.inner,
        }
    }

    /// Returns an iterator over the map's entries.
    #[inline(always)]
    pub fn iter(&self) -> MapxIter<'_, K, V> {
        MapxIter {
            iter: self.inner.iter(),
            _p: PhantomData,
        }
    }

    /// Returns a mutable iterator over the map's entries.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxIterMut<'_, K, V> {
        MapxIterMut {
            inner: self.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    /// Returns an iterator over the map's values.
    #[inline(always)]
    pub fn values(&self) -> MapxValues<'_, V> {
        MapxValues {
            inner: self.inner.iter(),
        }
    }

    /// Returns a mutable iterator over the map's values.
    #[inline(always)]
    pub fn values_mut(&mut self) -> MapxValuesMut<'_, V> {
        MapxValuesMut {
            inner: self.inner.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    /// Removes a key from the map.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) {
        self.inner.remove(key.encode())
    }

    /// Start a batch operation.
    ///
    /// This method allows you to perform multiple insert/remove operations
    /// and commit them atomically.
    ///
    /// # Examples
    ///
    /// ```
    /// use vsdb::{Mapx, vsdb_set_base_dir};
    ///
    /// vsdb_set_base_dir("/tmp/vsdb_mapx_batch_entry").unwrap();
    /// let mut map = Mapx::new();
    ///
    /// let mut batch = map.batch_entry();
    /// batch.insert(&1, &"one".to_string());
    /// batch.insert(&2, &"two".to_string());
    /// batch.commit().unwrap();
    ///
    /// assert_eq!(map.get(&1), Some("one".to_string()));
    /// assert_eq!(map.get(&2), Some("two".to_string()));
    /// ```
    #[inline(always)]
    pub fn batch_entry(&mut self) -> MapxBatchEntry<'_, K, V> {
        MapxBatchEntry {
            inner: self.inner.batch_entry(),
            _marker: PhantomData,
        }
    }
}

/// A batch writer for `Mapx`.
pub struct MapxBatch<'a, 'b, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    inner: &'b mut mapx_ord_rawkey::MapxOrdRawKeyBatch<'a, V>,
    _marker: PhantomData<K>,
}

impl<'a, 'b, K, V> MapxBatch<'a, 'b, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    /// Insert a key-value pair into the batch.
    pub fn insert(&mut self, key: &K, value: &V) {
        self.inner.insert(key.encode(), value);
    }

    /// Remove a key in the batch.
    pub fn remove(&mut self, key: &K) {
        self.inner.remove(key.encode());
    }
}

/// A batch entry for `Mapx`.
pub struct MapxBatchEntry<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyBatchEntry<'a, V>,
    _marker: PhantomData<K>,
}

impl<'a, K, V> MapxBatchEntry<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    /// Insert a key-value pair into the batch.
    pub fn insert(&mut self, key: &K, value: &V) {
        self.inner.insert(key.encode(), value);
    }

    /// Remove a key in the batch.
    pub fn remove(&mut self, key: &K) {
        self.inner.remove(key.encode());
    }

    /// Commit the batch.
    pub fn commit(self) -> Result<()> {
        self.inner.commit()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// An iterator over the entries of a `Mapx`.
pub struct MapxIter<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    iter: MapxOrdRawKeyIter<'a, V>,
    _p: PhantomData<K>,
}

impl<K, V> Iterator for MapxIter<'_, K, V>
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

impl<K, V> DoubleEndedIterator for MapxIter<'_, K, V>
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

/// A mutable iterator over the entries of a `Mapx`.
pub struct MapxIterMut<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyIterMut<'a, V>,
    _p: PhantomData<K>,
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

impl<K, V> DoubleEndedIterator for MapxIterMut<'_, K, V>
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

/// A mutable reference to a value in a `Mapx` iterator.
#[derive(Debug)]
pub struct ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    /// The inner mutable reference to the value.
    pub(crate) inner: mapx_ord_rawkey::ValueIterMut<'a, V>,
}

impl<V> Deref for ValueIterMut<'_, V>
where
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<V> DerefMut for ValueIterMut<'_, V>
where
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
