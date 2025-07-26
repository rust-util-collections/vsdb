//!
//! A `BTreeMap`-like structure that stores data on disk with raw keys.
//!
//! `MapxOrdRawKey` is an ordered map where keys are stored as raw bytes,
//! while values are encoded using `serde`-like methods. This is useful when
//! you need to work with keys that are already in a byte format and want to
// avoid the overhead of encoding and decoding them.
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_ord_rawkey::MapxOrdRawKey;
//! use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut m: MapxOrdRawKey<String> = MapxOrdRawKey::new();
//!
//! // Insert key-value pairs
//! m.insert(&[1], &"hello".to_string());
//! m.insert(&[2], &"world".to_string());
//!
//! // Check the length of the map
//! assert_eq!(m.len(), 2);
//!
//! // Retrieve a value
//! assert_eq!(m.get(&[1]), Some("hello".to_string()));
//!
//! // Iterate over the map
//! for (k, v) in m.iter() {
//!     println!("key: {:?}, val: {}", k, v);
//! }
//!
//! // Remove a key-value pair
//! m.remove(&[2]);
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

use crate::common::{RawKey, ende::ValueEnDe};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    marker::PhantomData,
    ops::{Deref, DerefMut, RangeBounds},
};
use vsdb_core::basic::mapx_raw::{self, MapxRaw, MapxRawIter};

/// A disk-based, `BTreeMap`-like data structure with raw keys and typed values.
///
/// `MapxOrdRawKey` stores keys as raw bytes and values as encoded data.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxOrdRawKey<V> {
    /// The inner raw map.
    pub(crate) inner: MapxRaw,
    _p: PhantomData<V>,
}

impl<V> MapxOrdRawKey<V>
where
    V: ValueEnDe,
{
    /// Creates a "shadow" copy of the `MapxOrdRawKey` instance.
    ///
    /// # Safety
    ///
    /// This API breaks Rust's semantic safety guarantees. Use only in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        unsafe {
            Self {
                inner: self.inner.shadow(),
                _p: PhantomData,
            }
        }
    }

    /// Creates a `MapxOrdRawKey` from a byte slice.
    ///
    /// # Safety
    ///
    /// This function is unsafe and assumes the byte slice is a valid representation.
    #[inline(always)]
    pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
        unsafe {
            Self {
                inner: MapxRaw::from_bytes(s),
                _p: PhantomData,
            }
        }
    }

    /// Returns the byte representation of the `MapxOrdRawKey`.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    /// Creates a new, empty `MapxOrdRawKey`.
    #[inline(always)]
    pub fn new() -> Self {
        MapxOrdRawKey {
            inner: MapxRaw::new(),
            _p: PhantomData,
        }
    }

    /// Retrieves a value from the map for a given key.
    #[inline(always)]
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<V> {
        self.inner
            .get(key.as_ref())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    /// Retrieves a mutable reference to a value in the map.
    #[inline(always)]
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_, V>> {
        self.inner.get_mut(key.as_ref()).map(|inner| ValueMut {
            value: <V as ValueEnDe>::decode(&inner).unwrap(),
            inner,
        })
    }

    /// Mocks a mutable value for a given key.
    #[inline(always)]
    pub(crate) fn mock_value_mut(&mut self, key: RawKey, value: V) -> ValueMut<'_, V> {
        let v = value.encode();
        ValueMut {
            value,
            inner: self.inner.mock_value_mut(key, v),
        }
    }

    /// Checks if the map contains a value for the specified key.
    #[inline(always)]
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> bool {
        self.inner.contains_key(key.as_ref())
    }

    /// Retrieves the last entry with a key less than or equal to the given key.
    #[inline(always)]
    pub fn get_le(&self, key: impl AsRef<[u8]>) -> Option<(RawKey, V)> {
        self.inner
            .get_le(key.as_ref())
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    /// Retrieves the first entry with a key greater than or equal to the given key.
    #[inline(always)]
    pub fn get_ge(&self, key: impl AsRef<[u8]>) -> Option<(RawKey, V)> {
        self.inner
            .get_ge(key.as_ref())
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
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
    #[inline(always)]
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: &V) -> Option<V> {
        self.inner
            .insert(key.as_ref(), value.encode())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    /// Inserts a key with an already encoded value.
    ///
    /// # Safety
    ///
    /// This is a low-level API for performance-critical scenarios. Do not use for common purposes.
    #[inline(always)]
    pub unsafe fn insert_encoded_value(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Option<V> {
        self.inner
            .insert(key.as_ref(), value.as_ref())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    /// Sets the value for a key, overwriting any existing value.
    #[inline(always)]
    pub fn set_value(&mut self, key: impl AsRef<[u8]>, value: &V) {
        self.inner.insert(key.as_ref(), value.encode());
    }

    /// Gets an entry for a given key, allowing for in-place modification.
    #[inline(always)]
    pub fn entry<'a>(&'a mut self, key: &'a [u8]) -> Entry<'a, V> {
        Entry { key, hdr: self }
    }

    /// Returns an iterator over the map's entries.
    #[inline(always)]
    pub fn iter(&self) -> MapxOrdRawKeyIter<V> {
        MapxOrdRawKeyIter {
            inner: self.inner.iter(),
            _p: PhantomData,
        }
    }

    /// Returns a mutable iterator over the map's entries.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxOrdRawKeyIterMut<V> {
        MapxOrdRawKeyIterMut {
            inner: self.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    /// Returns an iterator over a range of entries in the map.
    #[inline(always)]
    pub fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdRawKeyIter<'a, V> {
        MapxOrdRawKeyIter {
            inner: self.inner.range(bounds),
            _p: PhantomData,
        }
    }

    /// Returns a mutable iterator over a range of entries in the map.
    #[inline(always)]
    pub fn range_mut<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a mut self,
        bounds: R,
    ) -> MapxOrdRawKeyIterMut<'a, V> {
        MapxOrdRawKeyIterMut {
            inner: self.inner.range_mut(bounds),
            _p: PhantomData,
        }
    }

    /// Retrieves the first entry in the map.
    #[inline(always)]
    pub fn first(&self) -> Option<(RawKey, V)> {
        self.iter().next()
    }

    /// Retrieves the last entry in the map.
    #[inline(always)]
    pub fn last(&self) -> Option<(RawKey, V)> {
        self.iter().next_back()
    }

    /// Removes a key from the map, returning the value if it existed.
    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Option<V> {
        self.inner
            .remove(key.as_ref())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    /// Removes a key from the map without returning the value.
    #[inline(always)]
    pub fn unset_value(&mut self, key: impl AsRef<[u8]>) {
        self.inner.remove(key.as_ref());
    }

    /// Clears the map, removing all key-value pairs.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Checks if this `MapxOrdRawKey` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }
}

impl<V> Clone for MapxOrdRawKey<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _p: PhantomData,
        }
    }
}

impl<V> Default for MapxOrdRawKey<V>
where
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable reference to a value in a `MapxOrdRawKey`.
#[derive(Debug)]
pub struct ValueMut<'a, V>
where
    V: ValueEnDe,
{
    value: V,
    inner: mapx_raw::ValueMut<'a>,
}

impl<V> Drop for ValueMut<'_, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        *self.inner = self.value.encode();
    }
}

impl<V> Deref for ValueMut<'_, V>
where
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<V> DerefMut for ValueMut<'_, V>
where
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A view into a single entry in a map, which may either be vacant or occupied.
pub struct Entry<'a, V>
where
    V: ValueEnDe,
{
    key: &'a [u8],
    hdr: &'a mut MapxOrdRawKey<V>,
}

impl<'a, V> Entry<'a, V>
where
    V: ValueEnDe,
{
    /// Ensures a value is in the entry by inserting the default if empty,
    /// and returns a mutable reference to the value.
    pub fn or_insert(self, default: V) -> ValueMut<'a, V> {
        let hdr = self.hdr as *mut MapxOrdRawKey<V>;
        match unsafe { &mut *hdr }.get_mut(self.key) {
            Some(v) => v,
            _ => unsafe { &mut *hdr }.mock_value_mut(self.key.to_vec(), default),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// An iterator over the entries of a `MapxOrdRawKey`.
pub struct MapxOrdRawKeyIter<'a, V> {
    inner: MapxRawIter<'a>,
    _p: PhantomData<V>,
}

impl<V> Iterator for MapxOrdRawKeyIter<'_, V>
where
    V: ValueEnDe,
{
    type Item = (RawKey, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }
}

impl<V> DoubleEndedIterator for MapxOrdRawKeyIter<'_, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable iterator over the entries of a `MapxOrdRawKey`.
pub struct MapxOrdRawKeyIterMut<'a, V> {
    inner: mapx_raw::MapxRawIterMut<'a>,
    _p: PhantomData<V>,
}

impl<'a, V> Iterator for MapxOrdRawKeyIterMut<'a, V>
where
    V: ValueEnDe,
{
    type Item = (RawKey, ValueIterMut<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| {
            (
                k,
                ValueIterMut {
                    value: <V as ValueEnDe>::decode(&v).unwrap(),
                    inner: v,
                },
            )
        })
    }
}

impl<V> DoubleEndedIterator for MapxOrdRawKeyIterMut<'_, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(k, v)| {
            (
                k,
                ValueIterMut {
                    value: <V as ValueEnDe>::decode(&v).unwrap(),
                    inner: v,
                },
            )
        })
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable reference to a value in a `MapxOrdRawKey` iterator.
#[derive(Debug)]
pub struct ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    /// The decoded value.
    pub(crate) value: V,
    /// The inner mutable reference to the raw value.
    pub(crate) inner: mapx_raw::ValueIterMut<'a>,
}

impl<V> Drop for ValueIterMut<'_, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        *self.inner = self.value.encode();
    }
}

impl<V> Deref for ValueIterMut<'_, V>
where
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<V> DerefMut for ValueIterMut<'_, V>
where
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
