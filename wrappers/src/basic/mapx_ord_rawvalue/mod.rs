//!
//! A `BTreeMap`-like structure that stores data on disk with raw values.
//!
//! `MapxOrdRawValue` is an ordered map where keys are encoded, but values are
//! stored as raw bytes. This is useful when you need to work with values that
//! are already in a byte format or when you want to handle value serialization
//! manually.
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_ord_rawvalue::MapxOrdRawValue;
//! use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut m: MapxOrdRawValue<u32> = MapxOrdRawValue::new();
//!
//! // Insert key-value pairs
//! m.insert(&1, &[10]);
//! m.insert(&2, &[20]);
//!
//! // Check the length of the map
//! assert_eq!(m.len(), 2);
//!
//! // Retrieve a value
//! assert_eq!(m.get(&1), Some(vec![10]));
//!
//! // Iterate over the map
//! for (k, v) in m.iter() {
//!     println!("key: {}, val: {:?}", k, v);
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

use crate::common::{RawValue, ende::KeyEnDeOrdered};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    marker::PhantomData,
    ops::{Bound, Deref, DerefMut, RangeBounds},
};
use vsdb_core::basic::mapx_raw::{MapxRaw, MapxRawIter, MapxRawIterMut, ValueIterMut};

/// A disk-based, `BTreeMap`-like data structure with typed keys and raw values.
///
/// `MapxOrdRawValue` stores keys as encoded data and values as raw bytes.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxOrdRawValue<K> {
    /// The inner raw map.
    pub(crate) inner: MapxRaw,
    _p: PhantomData<K>,
}

impl<K> MapxOrdRawValue<K>
where
    K: KeyEnDeOrdered,
{
    /// Creates a "shadow" copy of the `MapxOrdRawValue` instance.
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

    /// Creates a `MapxOrdRawValue` from a byte slice.
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

    /// Returns the byte representation of the `MapxOrdRawValue`.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    /// Creates a new, empty `MapxOrdRawValue`.
    #[inline(always)]
    pub fn new() -> Self {
        MapxOrdRawValue {
            inner: MapxRaw::new(),
            _p: PhantomData,
        }
    }

    /// Retrieves a value from the map for a given key.
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<RawValue> {
        self.inner.get(key.to_bytes())
    }

    /// Retrieves a mutable reference to a value in the map.
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, K>> {
        self.inner
            .get(key.to_bytes())
            .map(|v| ValueMut::new(self, key.clone(), v))
    }

    /// Mocks a mutable value for a given key.
    #[inline(always)]
    pub(crate) fn mock_value_mut(&mut self, key: K, value: RawValue) -> ValueMut<'_, K> {
        ValueMut {
            hdr: self,
            key,
            value,
        }
    }

    /// Checks if the map contains a value for the specified key.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key.to_bytes())
    }

    /// Retrieves the last entry with a key less than or equal to the given key.
    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, RawValue)> {
        self.inner
            .get_le(key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    /// Retrieves the first entry with a key greater than or equal to the given key.
    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, RawValue)> {
        self.inner
            .get_ge(key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
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
    pub fn insert(&mut self, key: &K, value: impl AsRef<[u8]>) -> Option<RawValue> {
        self.inner.insert(key.to_bytes(), value.as_ref())
    }

    /// Sets the value for a key, overwriting any existing value.
    #[inline(always)]
    pub fn set_value(&mut self, key: &K, value: impl AsRef<[u8]>) {
        self.inner.insert(key.to_bytes(), value.as_ref());
    }

    /// Gets an entry for a given key, allowing for in-place modification.
    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, K> {
        Entry { key, hdr: self }
    }

    /// Returns an iterator over the map's entries.
    #[inline(always)]
    pub fn iter(&self) -> MapxOrdRawValueIter<K> {
        MapxOrdRawValueIter {
            inner: self.inner.iter(),
            _p: PhantomData,
        }
    }

    /// Returns a mutable iterator over the map's entries.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxOrdRawValueIterMut<K> {
        MapxOrdRawValueIterMut {
            inner: self.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    /// Returns an iterator over the map's values.
    #[inline(always)]
    pub fn values(&self) -> MapxOrdRawValueValues<K> {
        MapxOrdRawValueValues {
            inner: self.inner.iter(),
            _p: PhantomData,
        }
    }

    /// Returns a mutable iterator over the map's values.
    #[inline(always)]
    pub fn values_mut(&mut self) -> MapxOrdRawValueValuesMut {
        MapxOrdRawValueValuesMut {
            inner: self.inner.iter_mut(),
        }
    }

    /// Returns an iterator over a range of entries in the map.
    #[inline(always)]
    pub fn range<'a, R: RangeBounds<&'a K>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdRawValueIter<'a, K> {
        let l = match bounds.start_bound() {
            Bound::Included(lo) => Bound::Included(Cow::Owned(lo.to_bytes())),
            Bound::Excluded(lo) => Bound::Excluded(Cow::Owned(lo.to_bytes())),
            Bound::Unbounded => Bound::Unbounded,
        };

        let h = match bounds.end_bound() {
            Bound::Included(hi) => Bound::Included(Cow::Owned(hi.to_bytes())),
            Bound::Excluded(hi) => Bound::Excluded(Cow::Owned(hi.to_bytes())),
            Bound::Unbounded => Bound::Unbounded,
        };

        MapxOrdRawValueIter {
            inner: self.inner.range((l, h)),
            _p: PhantomData,
        }
    }

    /// Returns a mutable iterator over a range of entries in the map.
    #[inline(always)]
    pub fn range_mut<'a, R: RangeBounds<&'a K>>(
        &'a mut self,
        bounds: R,
    ) -> MapxOrdRawValueIterMut<'a, K> {
        let l = match bounds.start_bound() {
            Bound::Included(lo) => Bound::Included(Cow::Owned(lo.to_bytes())),
            Bound::Excluded(lo) => Bound::Excluded(Cow::Owned(lo.to_bytes())),
            Bound::Unbounded => Bound::Unbounded,
        };

        let h = match bounds.end_bound() {
            Bound::Included(hi) => Bound::Included(Cow::Owned(hi.to_bytes())),
            Bound::Excluded(hi) => Bound::Excluded(Cow::Owned(hi.to_bytes())),
            Bound::Unbounded => Bound::Unbounded,
        };

        MapxOrdRawValueIterMut {
            inner: self.inner.range_mut((l, h)),
            _p: PhantomData,
        }
    }

    /// Retrieves the first entry in the map.
    #[inline(always)]
    pub fn first(&self) -> Option<(K, RawValue)> {
        self.iter().next()
    }

    /// Retrieves the last entry in the map.
    #[inline(always)]
    pub fn last(&self) -> Option<(K, RawValue)> {
        self.iter().next_back()
    }

    /// Removes a key from the map, returning the value if it existed.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<RawValue> {
        self.inner.remove(key.to_bytes())
    }

    /// Removes a key from the map without returning the value.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.remove(key.to_bytes());
    }

    /// Clears the map, removing all key-value pairs.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Checks if this `MapxOrdRawValue` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }
}

impl<K> Clone for MapxOrdRawValue<K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _p: PhantomData,
        }
    }
}

impl<K> Default for MapxOrdRawValue<K>
where
    K: KeyEnDeOrdered,
{
    fn default() -> Self {
        Self::new()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable reference to a value in a `MapxOrdRawValue`.
#[derive(Debug)]
pub struct ValueMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    hdr: &'a mut MapxOrdRawValue<K>,
    key: K,
    value: RawValue,
}

impl<'a, K> ValueMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    /// Creates a new `ValueMut`.
    pub(crate) fn new(hdr: &'a mut MapxOrdRawValue<K>, key: K, value: RawValue) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<K> Drop for ValueMut<'_, K>
where
    K: KeyEnDeOrdered,
{
    fn drop(&mut self) {
        self.hdr.set_value(&self.key, &self.value);
    }
}

impl<K> Deref for ValueMut<'_, K>
where
    K: KeyEnDeOrdered,
{
    type Target = RawValue;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<K> DerefMut for ValueMut<'_, K>
where
    K: KeyEnDeOrdered,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A view into a single entry in a map, which may either be vacant or occupied.
pub struct Entry<'a, K>
where
    K: KeyEnDeOrdered,
{
    key: K,
    hdr: &'a mut MapxOrdRawValue<K>,
}

impl<'a, K> Entry<'a, K>
where
    K: KeyEnDeOrdered,
{
    /// Ensures a value is in the entry by inserting the default if empty,
    /// and returns a mutable reference to the value.
    pub fn or_insert(self, default: impl AsRef<[u8]>) -> ValueMut<'a, K> {
        let hdr = self.hdr as *mut MapxOrdRawValue<K>;
        match unsafe { &mut *hdr }.get_mut(&self.key) {
            Some(v) => v,
            _ => {
                unsafe { &mut *hdr }.mock_value_mut(self.key, default.as_ref().to_vec())
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// An iterator over the entries of a `MapxOrdRawValue`.
pub struct MapxOrdRawValueIter<'a, K>
where
    K: KeyEnDeOrdered,
{
    inner: MapxRawIter<'a>,
    _p: PhantomData<K>,
}

impl<K> Iterator for MapxOrdRawValueIter<'_, K>
where
    K: KeyEnDeOrdered,
{
    type Item = (K, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

impl<K> DoubleEndedIterator for MapxOrdRawValueIter<'_, K>
where
    K: KeyEnDeOrdered,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// An iterator over the values of a `MapxOrdRawValue`.
pub struct MapxOrdRawValueValues<'a, K>
where
    K: KeyEnDeOrdered,
{
    inner: MapxRawIter<'a>,
    _p: PhantomData<K>,
}

impl<K> Iterator for MapxOrdRawValueValues<'_, K>
where
    K: KeyEnDeOrdered,
{
    type Item = RawValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<K> DoubleEndedIterator for MapxOrdRawValueValues<'_, K>
where
    K: KeyEnDeOrdered,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| v)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable iterator over the entries of a `MapxOrdRawValue`.
pub struct MapxOrdRawValueIterMut<'a, K> {
    /// The inner mutable iterator.
    pub(crate) inner: MapxRawIterMut<'a>,
    /// A phantom data field to hold the key type.
    pub(crate) _p: PhantomData<K>,
}

impl<'a, K> Iterator for MapxOrdRawValueIterMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    type Item = (K, ValueIterMut<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(k, v)| (pnk!(<K as KeyEnDeOrdered>::from_bytes(k)), v))
    }
}

impl<K> DoubleEndedIterator for MapxOrdRawValueIterMut<'_, K>
where
    K: KeyEnDeOrdered,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (pnk!(<K as KeyEnDeOrdered>::from_bytes(k)), v))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable iterator over the values of a `MapxOrdRawValue`.
pub struct MapxOrdRawValueValuesMut<'a> {
    inner: MapxRawIterMut<'a>,
}

impl<'a> Iterator for MapxOrdRawValueValuesMut<'a> {
    type Item = ValueIterMut<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl DoubleEndedIterator for MapxOrdRawValueValuesMut<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| v)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
