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
//! // Check the length of the map
//! assert_eq!(m.len(), 2);
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
//! assert_eq!(m.len(), 1);
//!
//! // Clear the entire map
//! m.clear();
//! assert_eq!(m.len(), 0);
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
            self, MapxOrdRawKey, MapxOrdRawKeyIter, MapxOrdRawKeyIterMut, ValueMut,
        },
    },
    common::ende::{KeyEnDe, ValueEnDe},
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

/// A disk-based, `HashMap`-like data structure with typed keys and values.
///
/// `Mapx` stores key-value pairs on disk, encoding both keys and values
/// for type safety and persistence.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct Mapx<K, V> {
    inner: MapxOrdRawKey<V>,
    _p: PhantomData<K>,
}

impl<K, V> Mapx<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    /// Creates a "shadow" copy of the `Mapx` instance.
    ///
    /// This method creates a new `Mapx` that shares the same underlying data source.
    ///
    /// # Safety
    ///
    /// This API breaks Rust's semantic safety guarantees. It is safe to use only in a
    /// race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        unsafe {
            Self {
                inner: self.inner.shadow(),
                _p: PhantomData,
            }
        }
    }

    /// Creates a `Mapx` from a byte slice.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it assumes the byte slice is a valid
    /// representation of a `Mapx`. Do not use this API unless you are certain
    /// about the internal data structure.
    #[inline(always)]
    pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
        unsafe {
            Self {
                inner: MapxOrdRawKey::from_bytes(s),
                _p: PhantomData,
            }
        }
    }

    /// Returns the byte representation of the `Mapx`.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    /// Creates a new, empty `Mapx`.
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            inner: MapxOrdRawKey::new(),
            _p: PhantomData,
        }
    }

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

    /// Returns the number of entries in the map.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Checks if the map is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the key already exists, the old value is returned.
    #[inline(always)]
    pub fn insert(&mut self, key: &K, value: &V) -> Option<V> {
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
    pub fn iter(&self) -> MapxIter<K, V> {
        MapxIter {
            iter: self.inner.iter(),
            _p: PhantomData,
        }
    }

    /// Returns a mutable iterator over the map's entries.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxIterMut<K, V> {
        MapxIterMut {
            inner: self.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    /// Returns an iterator over the map's values.
    #[inline(always)]
    pub fn values(&self) -> MapxValues<V> {
        MapxValues {
            inner: self.inner.iter(),
        }
    }

    /// Returns a mutable iterator over the map's values.
    #[inline(always)]
    pub fn values_mut(&mut self) -> MapxValuesMut<V> {
        MapxValuesMut {
            inner: self.inner.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    /// Removes a key from the map, returning the value at the key if it existed.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key.encode())
    }

    /// Removes a key from the map without returning the value.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.unset_value(key.encode());
    }

    /// Clears the map, removing all key-value pairs.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Checks if this `Mapx` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }
}

impl<K, V> Clone for Mapx<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _p: PhantomData,
        }
    }
}

impl<K, V> Default for Mapx<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
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

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
