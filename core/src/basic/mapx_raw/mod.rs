//!
//! A `Map`-like structure that stores data on disk.
//!
//! This module provides `MapxRaw`, a key-value store that functions like a standard `Map`
//! but with the underlying data persisted to disk. It is "raw" because it does not
//! encode or transform keys and values; they are stored as-is.
//!
//! # Examples
//!
//! ```
//! use vsdb_core::basic::mapx_raw::MapxRaw;
//! use vsdb_core::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut m = MapxRaw::new();
//!
//! // Insert key-value pairs
//! m.insert(&[1], &[10]);
//! m.insert(&[2], &[20]);
//! m.insert(&[3], &[30]);
//!
//! // Retrieve a value
//! assert_eq!(m.get(&[2]), Some(vec![20]));
//!
//! // Iterate over the map
//! for (k, v) in m.iter() {
//!     println!("key: {:?}, val: {:?}", k, v);
//! }
//!
//! // Remove a key-value pair
//! m.remove(&[2]);
//! assert!(m.get(&[2]).is_none());
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

use crate::common::{PreBytes, RawKey, RawValue, engine};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, ops::RangeBounds};

/// An iterator over the entries of a `MapxRaw`.
pub type MapxRawIter<'a> = engine::MapxIter<'a>;
/// A mutable iterator over the entries of a `MapxRaw`.
pub type MapxRawIterMut<'a> = engine::MapxIterMut<'a>;
/// A mutable reference to a value in a `MapxRaw`.
pub type ValueMut<'a> = engine::ValueMut<'a>;
/// A mutable iterator over the values of a `MapxRaw`.
pub type ValueIterMut<'a> = engine::ValueIterMut<'a>;

/// A raw, disk-based, key-value map.
///
/// `MapxRaw` provides a `Map`-like interface for storing and retrieving raw byte slices.
/// It is unversioned and does not perform any encoding on keys or values.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxRaw {
    inner: engine::Mapx,
}

impl MapxRaw {
    /// Creates a "shadow" copy of the `MapxRaw` instance.
    ///
    /// This method creates a new `MapxRaw` that shares the same underlying data source.
    /// It is a lightweight operation that can be used to create multiple references
    /// to the same map without the overhead of cloning the entire structure.
    ///
    /// # Safety
    ///
    /// This API breaks the semantic safety guarantees of Rust's ownership and borrowing rules.
    /// It is safe to use in a race-free environment where you can guarantee that no two
    /// threads will access the same data concurrently.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        Self {
            inner: unsafe { self.inner.shadow() },
        }
    }

    /// Creates a new, empty `MapxRaw`.
    ///
    /// # Returns
    ///
    /// A new `MapxRaw` instance.
    #[inline(always)]
    pub fn new() -> Self {
        MapxRaw {
            inner: engine::Mapx::new(),
        }
    }

    /// Retrieves a value from the map corresponding to the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up.
    ///
    /// # Returns
    ///
    /// An `Option<RawValue>` containing the value if the key exists, or `None` otherwise.
    #[inline(always)]
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<RawValue> {
        self.inner.get(key.as_ref())
    }

    /// Retrieves a mutable reference to a value in the map.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up.
    ///
    /// # Returns
    ///
    /// An `Option<ValueMut<'_>>` containing a mutable reference to the value if the key exists,
    /// or `None` otherwise.
    #[inline(always)]
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_>> {
        self.inner.get_mut(key.as_ref())
    }

    /// Mocks a mutable value, typically for use in scenarios where you need to
    /// create a `ValueMut` without actually inserting the value into the map yet.
    ///
    /// # Arguments
    ///
    /// * `key` - The key associated with the value.
    /// * `value` - The value to be mocked.
    ///
    /// # Returns
    ///
    /// A `ValueMut` instance.
    #[inline(always)]
    pub fn mock_value_mut(&mut self, key: RawValue, value: RawValue) -> ValueMut<'_> {
        self.inner.mock_value_mut(key, value)
    }

    /// Checks if the map contains a value for the specified key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check.
    ///
    /// # Returns
    ///
    /// `true` if the map contains the key, `false` otherwise.
    #[inline(always)]
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> bool {
        self.get(key.as_ref()).is_some()
    }

    /// Retrieves the last entry with a key less than or equal to the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to search for.
    ///
    /// # Returns
    ///
    /// An `Option<(RawKey, RawValue)>` containing the key-value pair if found, or `None` otherwise.
    #[inline(always)]
    pub fn get_le(&self, key: impl AsRef<[u8]>) -> Option<(RawKey, RawValue)> {
        self.range(..=Cow::Borrowed(key.as_ref())).next_back()
    }

    /// Retrieves the first entry with a key greater than or equal to the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to search for.
    ///
    /// # Returns
    ///
    /// An `Option<(RawKey, RawValue)>` containing the key-value pair if found, or `None` otherwise.
    #[inline(always)]
    pub fn get_ge(&self, key: impl AsRef<[u8]>) -> Option<(RawKey, RawValue)> {
        self.range(Cow::Borrowed(key.as_ref())..).next()
    }

    /// Gets an entry for the given key, allowing for in-place modification.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the entry.
    ///
    /// # Returns
    ///
    /// An `Entry` that allows for operations on the value.
    #[inline(always)]
    pub fn entry<'a>(&'a mut self, key: &'a [u8]) -> Entry<'a> {
        Entry { key, hdr: self }
    }

    /// Returns an iterator over the map's entries.
    ///
    /// # Returns
    ///
    /// A `MapxRawIter` that iterates over the key-value pairs.
    #[inline(always)]
    pub fn iter(&self) -> MapxRawIter<'_> {
        self.inner.iter()
    }

    /// Returns an iterator over a range of entries in the map.
    ///
    /// # Arguments
    ///
    /// * `bounds` - The range of keys to iterate over.
    ///
    /// # Returns
    ///
    /// A `MapxRawIter` that iterates over the key-value pairs in the specified range.
    #[inline(always)]
    pub fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        bounds: R,
    ) -> MapxRawIter<'a> {
        self.inner.range(bounds)
    }

    /// Returns a detached iterator over a range of entries in the map.
    ///
    /// This iterator is not tied to the lifetime of `&self`, allowing for concurrent
    /// modification of the map during iteration (though the iterator will see a snapshot).
    ///
    /// # Arguments
    ///
    /// * `bounds` - The range of keys to iterate over.
    ///
    /// # Returns
    ///
    /// A `MapxRawIter` that iterates over the key-value pairs in the specified range.
    #[inline(always)]
    pub fn range_detached<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &self,
        bounds: R,
    ) -> MapxRawIter<'a> {
        self.inner.range_detached(bounds)
    }

    /// Returns a mutable iterator over the map's entries.
    ///
    /// # Returns
    ///
    /// A `MapxRawIterMut` that allows for mutable iteration over the key-value pairs.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxRawIterMut<'_> {
        self.inner.iter_mut()
    }

    /// Returns a mutable iterator over a range of entries in the map.
    ///
    /// # Arguments
    ///
    /// * `bounds` - The range of keys to iterate over.
    ///
    /// # Returns
    ///
    /// A `MapxRawIterMut` that allows for mutable iteration over the key-value pairs in the specified range.
    #[inline(always)]
    pub fn range_mut<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a mut self,
        bounds: R,
    ) -> MapxRawIterMut<'a> {
        self.inner.range_mut(bounds)
    }

    /// Retrieves the last entry in the map.
    ///
    /// # Returns
    ///
    /// An `Option<(RawKey, RawValue)>` containing the last key-value pair, or `None` if the map is empty.
    #[inline(always)]
    pub fn last(&self) -> Option<(RawKey, RawValue)> {
        self.iter().next_back()
    }

    /// Inserts a key-value pair into the map.
    ///
    /// Does not return the old value for performance reasons.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert.
    /// * `value` - The value to associate with the key.
    #[inline(always)]
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.inner.insert(key.as_ref(), value.as_ref())
    }

    /// Removes a key from the map.
    ///
    /// Does not return the old value for performance reasons.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove.
    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) {
        self.inner.remove(key.as_ref())
    }

    /// Start a batch operation.
    ///
    /// This method allows you to perform multiple insert/remove operations
    /// and commit them atomically.
    ///
    /// # Examples
    ///
    /// ```
    /// use vsdb_core::basic::mapx_raw::MapxRaw;
    /// use vsdb_core::vsdb_set_base_dir;
    ///
    /// vsdb_set_base_dir("/tmp/vsdb_core_mapx_raw_batch_entry").unwrap();
    /// let mut map = MapxRaw::new();
    ///
    /// {
    ///     let mut batch = map.batch_entry();
    ///     batch.insert(&[1], &[10]);
    ///     batch.insert(&[2], &[20]);
    ///     batch.commit().unwrap();
    /// }
    ///
    /// assert_eq!(map.get(&[1]), Some(vec![10]));
    /// assert_eq!(map.get(&[2]), Some(vec![20]));
    /// ```
    #[inline(always)]
    pub fn batch_entry(&mut self) -> Box<dyn crate::common::BatchTrait + '_> {
        self.inner.batch_begin()
    }

    /// Clears the map, removing all key-value pairs.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Creates a `MapxRaw` from a byte slice.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it assumes the byte slice is a valid representation of a `MapxRaw`.
    /// This function is unsafe because it assumes the byte slice is a valid representation of a `MapxRaw`. Do not use this API unless you know the internal details of the data structure extremely well.
    #[inline(always)]
    pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
        unsafe { Self::from_prefix_slice(s) }
    }

    /// Creates a `MapxRaw` from a prefix slice.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it assumes the prefix slice is a valid representation of a `MapxRaw`.
    /// This function is unsafe because it assumes the byte slice is a valid representation of a `MapxRaw`. Do not use this API unless you know the internal details of the data structure extremely well.
    #[inline(always)]
    pub unsafe fn from_prefix_slice(s: impl AsRef<[u8]>) -> Self {
        Self {
            inner: unsafe { engine::Mapx::from_prefix_slice(s) },
        }
    }

    /// Returns the byte representation of the `MapxRaw`.
    ///
    /// # Returns
    ///
    /// A byte slice `&[u8]` representing the `MapxRaw`.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.as_prefix_slice()
    }

    /// Returns the prefix slice of the `MapxRaw`.
    ///
    /// # Returns
    ///
    /// A `&PreBytes` slice representing the prefix of the `MapxRaw`.
    #[inline(always)]
    pub fn as_prefix_slice(&self) -> &PreBytes {
        self.inner.as_prefix_slice()
    }

    /// Checks if this `MapxRaw` instance is the same as another.
    ///
    /// # Arguments
    ///
    /// * `other_hdr` - The other `MapxRaw` to compare against.
    ///
    /// # Returns
    ///
    /// `true` if both instances refer to the same underlying data, `false` otherwise.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }
}

impl Default for MapxRaw {
    /// Creates a new, empty `MapxRaw`.
    ///
    /// # Returns
    ///
    /// A new `MapxRaw` instance.
    fn default() -> Self {
        Self::new()
    }
}

/// A view into a single entry in a map, which may either be vacant or occupied.
pub struct Entry<'a> {
    key: &'a [u8],
    hdr: &'a mut MapxRaw,
}

impl<'a> Entry<'a> {
    /// Ensures a value is in the entry by inserting the default if empty, and returns
    /// a mutable reference to the value.
    ///
    /// # Arguments
    ///
    /// * `default` - The default value to insert if the entry is empty.
    ///
    /// # Returns
    ///
    /// A `ValueMut` to the value in the entry.
    pub fn or_insert(self, default: &'a [u8]) -> ValueMut<'a> {
        let hdr = self.hdr as *mut MapxRaw;
        match unsafe { &mut *hdr }.get_mut(self.key) {
            Some(v) => v,
            _ => {
                unsafe { &mut *hdr }.mock_value_mut(self.key.to_vec(), default.to_vec())
            }
        }
    }

    /// Ensures a value is in the entry by inserting the result of a function if empty,
    /// and returns a mutable reference to the value.
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that returns the default value to insert if the entry is empty.
    ///
    /// # Returns
    ///
    /// A `ValueMut` to the value in the entry.
    pub fn or_insert_with<F>(self, f: F) -> ValueMut<'a>
    where
        F: FnOnce() -> RawValue,
    {
        let hdr = self.hdr as *mut MapxRaw;
        match unsafe { &mut *hdr }.get_mut(self.key) {
            Some(v) => v,
            _ => unsafe { &mut *hdr }.mock_value_mut(self.key.to_vec(), f()),
        }
    }
}
