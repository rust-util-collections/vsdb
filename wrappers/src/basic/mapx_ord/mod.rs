//!
//! A `BTreeMap`-like structure that stores data on disk.
//!
//! `MapxOrd` provides an ordered map where keys and values are encoded before
//! being persisted. Keys are encoded using `KeyEnDeOrdered` to ensure that
// a lexicographical ordering of the encoded bytes maintains the original order of the keys.
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_ord::MapxOrd;
//! use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut m: MapxOrd<u32, String> = MapxOrd::new();
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
    basic::mapx_ord_rawkey::{
        MapxOrdRawKey, MapxOrdRawKeyBatchEntry, MapxOrdRawKeyIter, ValueIterMut,
        ValueMut,
    },
    common::{
        RawKey,
        ende::{KeyEnDeOrdered, ValueEnDe},
    },
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};
use vsdb_core::basic::mapx_raw;

/// A disk-based, `BTreeMap`-like data structure with typed, ordered keys and values.
///
/// `MapxOrd` stores key-value pairs on disk, ensuring that the keys are ordered.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxOrd<K, V> {
    inner: MapxOrdRawKey<V>,
    _p: PhantomData<K>,
}

impl<K, V> MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Creates a "shadow" copy of the `MapxOrd` instance.
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

    /// Creates a `MapxOrd` from a byte slice.
    ///
    /// # Safety
    ///
    /// This function is unsafe and assumes the byte slice is a valid representation.
    #[inline(always)]
    pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
        unsafe {
            Self {
                inner: MapxOrdRawKey::from_bytes(s),
                _p: PhantomData,
            }
        }
    }

    /// Returns the byte representation of the `MapxOrd`.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    /// Creates a new, empty `MapxOrd`.
    #[inline(always)]
    pub fn new() -> Self {
        MapxOrd {
            inner: MapxOrdRawKey::new(),
            _p: PhantomData,
        }
    }

    /// Retrieves a value from the map for a given key.
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(key.to_bytes())
    }

    /// Retrieves a mutable reference to a value in the map.
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, V>> {
        self.inner.get_mut(key.to_bytes())
    }

    /// Checks if the map contains a value for the specified key.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key.to_bytes())
    }

    /// Retrieves the last entry with a key less than or equal to the given key.
    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_le(key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    /// Retrieves the first entry with a key greater than or equal to the given key.
    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_ge(key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    /// Inserts a key-value pair into the map.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn insert(&mut self, key: &K, value: &V) {
        self.inner.insert(key.to_bytes(), value)
    }

    /// Inserts a key with an already encoded value.
    ///
    /// # Safety
    ///
    /// This is a low-level API for performance-critical scenarios, such as versioned
    /// implementations. Do not use for common purposes.
    #[inline(always)]
    pub unsafe fn insert_encoded_value(&mut self, key: &K, value: impl AsRef<[u8]>) {
        unsafe { self.inner.insert_encoded_value(key.to_bytes(), value) }
    }

    /// Sets the value for a key, overwriting any existing value.
    #[inline(always)]
    pub fn set_value(&mut self, key: &K, value: &V) {
        self.inner.insert(key.to_bytes(), value);
    }

    /// Gets an entry for a given key, allowing for in-place modification.
    #[inline(always)]
    pub fn entry(&mut self, key: &K) -> Entry<'_, V> {
        Entry {
            key: key.to_bytes(),
            hdr: &mut self.inner,
        }
    }

    /// Returns an iterator over the map's entries.
    #[inline(always)]
    pub fn iter(&self) -> MapxOrdIter<'_, K, V> {
        MapxOrdIter {
            inner: self.inner.iter(),
            _p: PhantomData,
        }
    }

    /// Returns a mutable iterator over the map's entries.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxOrdIterMut<'_, K, V> {
        MapxOrdIterMut {
            inner: self.inner.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    /// Returns an iterator over the map's values.
    #[inline(always)]
    pub fn values(&self) -> MapxOrdValues<'_, V> {
        MapxOrdValues {
            inner: self.inner.iter(),
        }
    }

    /// Returns a mutable iterator over the map's values.
    #[inline(always)]
    pub fn values_mut(&mut self) -> MapxOrdValuesMut<'_, V> {
        MapxOrdValuesMut {
            inner: self.inner.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    /// Returns an iterator over a range of entries in the map.
    #[inline(always)]
    pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> MapxOrdIter<'_, K, V> {
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

        MapxOrdIter {
            inner: self.inner.range((l, h)),
            _p: PhantomData,
        }
    }

    /// Returns a mutable iterator over a range of entries in the map.
    #[inline(always)]
    pub fn range_mut<R: RangeBounds<K>>(
        &mut self,
        bounds: R,
    ) -> MapxOrdIterMut<'_, K, V> {
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

        MapxOrdIterMut {
            inner: self.inner.inner.range_mut((l, h)),
            _p: PhantomData,
        }
    }

    /// Retrieves the first entry in the map.
    #[inline(always)]
    pub fn first(&self) -> Option<(K, V)> {
        self.iter().next()
    }

    /// Retrieves the last entry in the map.
    #[inline(always)]
    pub fn last(&self) -> Option<(K, V)> {
        self.iter().next_back()
    }

    /// Removes a key from the map.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) {
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

    /// Start a batch operation.
    ///
    /// This method allows you to perform multiple insert/remove operations
    /// and commit them atomically.
    ///
    /// # Examples
    ///
    /// ```
    /// use vsdb::basic::mapx_ord::MapxOrd;
    /// use vsdb::vsdb_set_base_dir;
    ///
    /// vsdb_set_base_dir("/tmp/vsdb_mapx_ord_batch_entry").unwrap();
    /// let mut map: MapxOrd<u32, String> = MapxOrd::new();
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
    pub fn batch_entry(&mut self) -> MapxOrdBatchEntry<'_, K, V> {
        MapxOrdBatchEntry {
            inner: self.inner.batch_entry(),
            _marker: PhantomData,
        }
    }

    /// Checks if this `MapxOrd` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }
}

/// A batch writer for `MapxOrd`.
pub struct MapxOrdBatch<'a, 'b, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: &'b mut crate::basic::mapx_ord_rawkey::MapxOrdRawKeyBatch<'a, V>,
    _marker: PhantomData<(K, V)>,
}

impl<'a, 'b, K, V> MapxOrdBatch<'a, 'b, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Insert a key-value pair into the batch.
    pub fn insert(&mut self, key: &K, value: &V) {
        self.inner.insert(key.to_bytes(), value);
    }

    /// Remove a key in the batch.
    pub fn remove(&mut self, key: &K) {
        self.inner.remove(key.to_bytes());
    }
}

/// A batch entry for `MapxOrd`.
pub struct MapxOrdBatchEntry<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyBatchEntry<'a, V>,
    _marker: PhantomData<K>,
}

impl<'a, K, V> MapxOrdBatchEntry<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Insert a key-value pair into the batch.
    pub fn insert(&mut self, key: &K, value: &V) {
        self.inner.insert(key.to_bytes(), value);
    }

    /// Remove a key in the batch.
    pub fn remove(&mut self, key: &K) {
        self.inner.remove(key.to_bytes());
    }

    /// Commit the batch.
    pub fn commit(self) -> Result<()> {
        self.inner.commit()
    }
}

impl<K, V> Default for MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// An iterator over the entries of a `MapxOrd`.
pub struct MapxOrdIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyIter<'a, V>,
    _p: PhantomData<K>,
}

impl<K, V> Iterator for MapxOrdIter<'_, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

impl<K, V> DoubleEndedIterator for MapxOrdIter<'_, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// An iterator over the values of a `MapxOrd`.
pub struct MapxOrdValues<'a, V>
where
    V: ValueEnDe,
{
    /// The inner iterator over raw key-value pairs.
    pub(crate) inner: MapxOrdRawKeyIter<'a, V>,
}

impl<V> Iterator for MapxOrdValues<'_, V>
where
    V: ValueEnDe,
{
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<V> DoubleEndedIterator for MapxOrdValues<'_, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| v)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable iterator over the values of a `MapxOrd`.
pub struct MapxOrdValuesMut<'a, V>
where
    V: ValueEnDe,
{
    /// The inner mutable iterator over raw key-value pairs.
    pub(crate) inner: mapx_raw::MapxRawIterMut<'a>,
    /// A phantom data field to hold the value type.
    pub(crate) _p: PhantomData<V>,
}

impl<'a, V> Iterator for MapxOrdValuesMut<'a, V>
where
    V: ValueEnDe,
{
    type Item = ValueIterMut<'a, V>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| ValueIterMut {
            value: pnk!(<V as ValueEnDe>::decode(&v)),
            inner: v,
        })
    }
}

impl<V> DoubleEndedIterator for MapxOrdValuesMut<'_, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| ValueIterMut {
            value: pnk!(<V as ValueEnDe>::decode(&v)),
            inner: v,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable iterator over the entries of a `MapxOrd`.
pub struct MapxOrdIterMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: mapx_raw::MapxRawIterMut<'a>,
    _p: PhantomData<(K, V)>,
}

impl<'a, K, V> Iterator for MapxOrdIterMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, ValueIterMut<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| {
            (
                pnk!(<K as KeyEnDeOrdered>::from_bytes(k)),
                ValueIterMut {
                    value: <V as ValueEnDe>::decode(&v).unwrap(),
                    inner: v,
                },
            )
        })
    }
}

impl<K, V> DoubleEndedIterator for MapxOrdIterMut<'_, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(k, v)| {
            (
                pnk!(<K as KeyEnDeOrdered>::from_bytes(k)),
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

/// A view into a single entry in a map, which may either be vacant or occupied.
pub struct Entry<'a, V>
where
    V: ValueEnDe,
{
    /// The raw key of the entry.
    pub(crate) key: RawKey,
    /// A mutable reference to the map's header.
    pub(crate) hdr: &'a mut MapxOrdRawKey<V>,
}

impl<'a, V> Entry<'a, V>
where
    V: ValueEnDe,
{
    /// Ensures a value is in the entry by inserting the default if empty,
    /// and returns a mutable reference to the value.
    pub fn or_insert(self, default: V) -> ValueMut<'a, V> {
        let hdr = self.hdr as *mut MapxOrdRawKey<V>;
        match unsafe { &mut *hdr }.get_mut(&self.key) {
            Some(v) => v,
            _ => unsafe { &mut *hdr }.mock_value_mut(self.key, default),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
