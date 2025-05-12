//!
//! A disk-based, `Vec`-like data structure.
//!
//! `Vecx` provides a vector-like interface for storing a sequence of values on disk.
//! Values are encoded using `serde`-like methods, allowing for the storage of
//! complex data types.
//!
//! # Examples
//!
//! ```
//! use vsdb::{Vecx, vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut v: Vecx<String> = Vecx::new();
//!
//! // Push values
//! v.push(&"hello".to_string());
//! v.push(&"world".to_string());
//!
//! // Check the length
//! assert_eq!(v.len(), 2);
//!
//! // Get a value by index
//! assert_eq!(v.get(0), Some("hello".to_string()));
//!
//! // Iterate over the values
//! for value in v.iter() {
//!     println!("{}", value);
//! }
//!
//! // Pop a value
//! assert_eq!(v.pop(), Some("world".to_string()));
//! assert_eq!(v.len(), 1);
//!
//! // Clear the vector
//! v.clear();
//! assert_eq!(v.len(), 0);
//!
//! // Clean up the directory
//! fs::remove_dir_all(vsdb_get_base_dir()).unwrap();
//! ```

#[cfg(test)]
mod test;

use crate::{
    basic::mapx_ord_rawkey::{
        MapxOrdRawKey, MapxOrdRawKeyIter, MapxOrdRawKeyIterMut, ValueIterMut, ValueMut,
    },
    common::ende::ValueEnDe,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, cmp::Ordering};

/// A disk-based, `Vec`-like data structure with typed values.
///
/// `Vecx` stores a sequence of values on disk, encoding them for persistence.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct Vecx<T> {
    inner: MapxOrdRawKey<T>,
}

impl<T: ValueEnDe> Vecx<T> {
    /// Creates a "shadow" copy of the `Vecx` instance.
    ///
    /// # Safety
    ///
    /// This API breaks Rust's semantic safety guarantees. Use only in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        unsafe {
            Self {
                inner: self.inner.shadow(),
            }
        }
    }

    /// Creates a `Vecx` from a byte slice.
    ///
    /// # Safety
    ///
    /// This function is unsafe and assumes the byte slice is a valid representation.
    #[inline(always)]
    pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
        unsafe {
            Self {
                inner: MapxOrdRawKey::from_bytes(s),
            }
        }
    }

    /// Returns the byte representation of the `Vecx`.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    /// Creates a new, empty `Vecx`.
    #[inline(always)]
    pub fn new() -> Self {
        Vecx {
            inner: MapxOrdRawKey::new(),
        }
    }

    /// Retrieves a value at a specific index.
    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<T> {
        self.inner.get((idx as u64).to_be_bytes())
    }

    /// Retrieves a mutable reference to a value at a specific index.
    #[inline(always)]
    pub fn get_mut(&mut self, idx: usize) -> Option<ValueMut<'_, T>> {
        self.inner.get_mut((idx as u64).to_be_bytes())
    }

    /// Retrieves the last value in the vector.
    #[inline(always)]
    pub fn last(&self) -> Option<T> {
        alt!(self.is_empty(), return None);
        Some(
            self.inner
                .get((self.len() as u64 - 1).to_be_bytes())
                .unwrap(),
        )
    }

    /// Returns the number of values in the vector.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Checks if the vector is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Appends a value to the end of the vector.
    #[inline(always)]
    pub fn push(&mut self, v: &T) {
        self.inner.insert((self.len() as u64).to_be_bytes(), v);
    }

    /// Inserts a value at a specific index.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of bounds.
    #[inline(always)]
    pub fn insert(&mut self, idx: usize, v: &T) {
        let idx = idx as u64;
        match (self.len() as u64).cmp(&idx) {
            Ordering::Greater => {
                self.inner
                    .inner
                    .range_detached(
                        Cow::Borrowed(&idx.to_be_bytes()[..])
                            ..Cow::Borrowed(&(self.len() as u64).to_be_bytes()[..]),
                    )
                    .for_each(|(i, iv)| {
                        unsafe {
                            self.inner.insert_encoded_value(
                                (crate::parse_int!(i, u64) + 1).to_be_bytes(),
                                iv,
                            )
                        };
                    });
                self.inner.insert(idx.to_be_bytes(), v);
            }
            Ordering::Equal => {
                self.push(v);
            }
            Ordering::Less => {
                panic!("out of index");
            }
        }
    }

    /// Removes and returns the last value in the vector.
    #[inline(always)]
    pub fn pop(&mut self) -> Option<T> {
        alt!(self.is_empty(), return None);
        self.inner.remove((self.len() as u64 - 1).to_be_bytes())
    }

    /// Removes and returns the value at a specific index.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of bounds.
    #[inline(always)]
    pub fn remove(&mut self, idx: usize) -> T {
        let idx = idx as u64;
        if !self.is_empty() && idx < self.len() as u64 {
            let last_idx = self.len() as u64 - 1;
            let ret = self.inner.remove(idx.to_be_bytes()).unwrap();
            self.inner
                .inner
                .range_detached(Cow::Borrowed(&(1 + idx).to_be_bytes()[..])..)
                .for_each(|(i, v)| {
                    unsafe {
                        self.inner.insert_encoded_value(
                            (crate::parse_int!(i, u64) - 1).to_be_bytes(),
                            v,
                        )
                    };
                });
            self.inner.remove(last_idx.to_be_bytes());
            return ret;
        }
        panic!("out of index");
    }

    /// Removes a value at a specific index and returns it, replacing it with the last value.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of bounds.
    #[inline(always)]
    pub fn swap_remove(&mut self, idx: usize) -> T {
        let idx = idx as u64;
        if !self.is_empty() && idx < self.len() as u64 {
            let last_idx = self.len() as u64 - 1;
            let ret = self.inner.remove(idx.to_be_bytes()).unwrap();
            if let Some(v) = self.inner.remove(last_idx.to_be_bytes()) {
                self.inner.insert(idx.to_be_bytes(), &v);
            }
            return ret;
        }
        panic!("out of index");
    }

    /// Updates the value at a specific index.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of bounds.
    #[inline(always)]
    pub fn update(&mut self, idx: usize, v: &T) -> Option<T> {
        if idx < self.len() {
            return self.inner.insert((idx as u64).to_be_bytes(), v);
        }
        panic!("out of index");
    }

    /// Returns an iterator over the vector's values.
    #[inline(always)]
    pub fn iter(&self) -> VecxIter<'_, T> {
        VecxIter(self.inner.iter())
    }

    /// Returns a mutable iterator over the vector's values.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> VecxIterMut<'_, T> {
        VecxIterMut(self.inner.iter_mut())
    }

    /// Clears the vector, removing all values.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Checks if this `Vecx` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }
}

impl<T> Clone for Vecx<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: ValueEnDe> Default for Vecx<T> {
    fn default() -> Self {
        Self::new()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// An iterator over the values of a `Vecx`.
pub struct VecxIter<'a, T>(MapxOrdRawKeyIter<'a, T>);

impl<T> Iterator for VecxIter<'_, T>
where
    T: ValueEnDe,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(_, v)| v)
    }
}

impl<V> DoubleEndedIterator for VecxIter<'_, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(_, v)| v)
    }
}

/// A mutable iterator over the values of a `Vecx`.
pub struct VecxIterMut<'a, T>(MapxOrdRawKeyIterMut<'a, T>);

impl<'a, T> Iterator for VecxIterMut<'a, T>
where
    T: ValueEnDe,
{
    type Item = ValueIterMut<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(_, v)| v)
    }
}

impl<V> DoubleEndedIterator for VecxIterMut<'_, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(_, v)| v)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
