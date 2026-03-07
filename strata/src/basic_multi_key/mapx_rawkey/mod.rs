//!
//! A multi-key map with raw keys and typed values.
//!
//! `MapxRawKeyMk` provides a map-like interface where each value is associated
//! with a sequence of raw byte keys. This is useful for creating nested or
//! hierarchical key structures with typed values.
//!
//! # Examples
//!
//! ```
//! use vsdb::basic_multi_key::mapx_rawkey::MapxRawKeyMk;
//! use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut m: MapxRawKeyMk<String> = MapxRawKeyMk::new(2); // Two keys
//!
//! // Insert a value with a multi-key
//! m.insert(&[&[1], &[10]], &"hello".to_string()).unwrap();
//! m.insert(&[&[2], &[20]], &"world".to_string()).unwrap();
//!
//! // Get a value
//! assert_eq!(m.get(&[&[1], &[10]]), Some("hello".to_string()));
//!
//! // Remove a value
//! m.remove(&[&[1], &[10]]).unwrap();
//! assert!(!m.contains_key(&[&[1], &[10]]));
//!
//! // Clean up the directory
//! fs::remove_dir_all(vsdb_get_base_dir()).unwrap();
//! ```

#[cfg(test)]
mod test;

use crate::{basic_multi_key::mapx_raw::MapxRawMk, common::ende::ValueEnDe};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

/// A multi-key map with raw keys and typed values.
#[derive(Serialize, Deserialize, Debug)]
#[serde(bound = "")]
pub struct MapxRawKeyMk<V> {
    inner: MapxRawMk,
    p: PhantomData<V>,
}

impl<V: ValueEnDe> MapxRawKeyMk<V> {
    /// Creates a "shadow" copy of the `MapxRawKeyMk` instance.
    ///
    /// # Safety
    ///
    /// This API breaks Rust's semantic safety guarantees. Use only in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        unsafe {
            Self {
                inner: self.inner.shadow(),
                p: PhantomData,
            }
        }
    }

    /// Creates a new `MapxRawKeyMk` with a specified number of keys.
    ///
    /// # Panics
    ///
    /// Panics if `key_size` is 0.
    #[inline(always)]
    pub fn new(key_size: u32) -> Self {
        Self {
            inner: MapxRawMk::new(key_size),
            p: PhantomData,
        }
    }

    /// Retrieves a value from the map for a given multi-key.
    #[inline(always)]
    pub fn get(&self, key: &[&[u8]]) -> Option<V> {
        self.inner.get(key).map(|v| pnk!(ValueEnDe::decode(&v)))
    }

    /// Retrieves a mutable reference to a value in the map.
    #[inline(always)]
    pub fn get_mut<'a>(&'a mut self, key: &'a [&'a [u8]]) -> Option<ValueMut<'a, V>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    /// Mocks a mutable value for a given key.
    #[inline(always)]
    pub(crate) fn mock_value_mut<'a>(
        &'a mut self,
        key: &'a [&'a [u8]],
        v: V,
    ) -> ValueMut<'a, V> {
        ValueMut::new(self, key, v)
    }

    /// Checks if the map contains a value for the specified multi-key.
    #[inline(always)]
    pub fn contains_key(&self, key: &[&[u8]]) -> bool {
        self.get(key).is_some()
    }

    /// Checks if the map is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Gets an entry for a given key, allowing for in-place modification.
    #[inline(always)]
    pub fn entry<'a>(&'a mut self, key: &'a [&'a [u8]]) -> Result<Entry<'a, V>> {
        if key.len() != self.key_size() as usize {
            Err(eg!())
        } else {
            Ok(Entry { key, hdr: self })
        }
    }

    /// Inserts a key-value pair into the map.
    #[inline(always)]
    pub fn insert(&mut self, key: &[&[u8]], value: &V) -> Result<()> {
        let v = value.encode();
        self.inner.insert(key, &v).c(d!())
    }

    /// Removes a key-value pair from the map. Supports batch removal by providing a partial key.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn remove(&mut self, key: &[&[u8]]) -> Result<()> {
        self.inner.remove(key).c(d!())
    }

    /// Clears the map, removing all key-value pairs.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Checks if this `MapxRawKeyMk` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }

    /// Returns the number of keys in the map.
    #[inline(always)]
    pub fn key_size(&self) -> u32 {
        self.inner.key_size()
    }

    /// Iterates over the map's entries, applying a function to each.
    #[inline(always)]
    pub fn iter_op<F>(&self, op: &mut F) -> Result<()>
    where
        F: FnMut(&[&[u8]], &V) -> Result<()>,
    {
        self.inner.iter_op_typed_value(op).c(d!())
    }

    /// Iterates over the map's entries with a given key prefix, applying a function to each.
    #[inline(always)]
    pub fn iter_op_with_key_prefix<F>(
        &self,
        op: &mut F,
        key_prefix: &[&[u8]],
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], &V) -> Result<()>,
    {
        self.inner
            .iter_op_typed_value_with_key_prefix(op, key_prefix)
            .c(d!())
    }

    // TODO
    // pub fn iter_mut_op
    // pub fn iter_mut_op_with_key_prefix
}

impl<V> Clone for MapxRawKeyMk<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            p: PhantomData,
        }
    }
}

/// A mutable reference to a value in a `MapxRawKeyMk`.
#[derive(Debug)]
pub struct ValueMut<'a, V: ValueEnDe> {
    hdr: &'a mut MapxRawKeyMk<V>,
    key: &'a [&'a [u8]],
    value: V,
}

impl<'a, V: ValueEnDe> ValueMut<'a, V> {
    fn new(hdr: &'a mut MapxRawKeyMk<V>, key: &'a [&'a [u8]], value: V) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<V: ValueEnDe> Drop for ValueMut<'_, V> {
    fn drop(&mut self) {
        pnk!(self.hdr.insert(self.key, &self.value));
    }
}

impl<V: ValueEnDe> Deref for ValueMut<'_, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<V: ValueEnDe> DerefMut for ValueMut<'_, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/// A view into a single entry in a map, which may either be vacant or occupied.
pub struct Entry<'a, V> {
    key: &'a [&'a [u8]],
    hdr: &'a mut MapxRawKeyMk<V>,
}

impl<'a, V: ValueEnDe> Entry<'a, V> {
    /// Ensures a value is in the entry by inserting the default if empty,
    /// and returns a mutable reference to the value.
    pub fn or_insert(self, default: V) -> ValueMut<'a, V> {
        let hdr = self.hdr as *mut MapxRawKeyMk<V>;
        match unsafe { &mut *hdr }.get_mut(self.key) {
            Some(v) => v,
            _ => unsafe { &mut *hdr }.mock_value_mut(self.key, default),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
