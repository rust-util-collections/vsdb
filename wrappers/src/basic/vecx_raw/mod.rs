//!
//! A disk-based, `Vec`-like data structure for raw bytes.
//!
//! `VecxRaw` provides a vector-like interface for storing a sequence of raw byte
//! slices on disk. It is suitable for scenarios where you need to manage a
//! collection of binary data without the overhead of serialization and deserialization.
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::vecx_raw::VecxRaw;
//! use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut v = VecxRaw::new();
//!
//! // Push values
//! v.push(&[1, 2, 3]);
//! v.push(&[4, 5, 6]);
//!
//! // Check the length
//! assert_eq!(v.len(), 2);
//!
//! // Get a value by index
//! assert_eq!(v.get(0), Some(vec![1, 2, 3]));
//!
//! // Iterate over the values
//! for value in v.iter() {
//!     println!("{:?}", value);
//! }
//!
//! // Pop a value
//! assert_eq!(v.pop(), Some(vec![4, 5, 6]));
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
    basic::mapx_ord_rawvalue::{MapxOrdRawValue, MapxOrdRawValueIterMut, ValueMut},
    common::RawValue,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, marker::PhantomData};
use vsdb_core::basic::mapx_raw::MapxRawIter;

/// A disk-based, `Vec`-like data structure for raw byte values.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct VecxRaw {
    inner: MapxOrdRawValue<u64>,
}

impl VecxRaw {
    /// Creates a "shadow" copy of the `VecxRaw` instance.
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

    /// Creates a `VecxRaw` from a byte slice.
    ///
    /// # Safety
    ///
    /// This function is unsafe and assumes the byte slice is a valid representation.
    #[inline(always)]
    pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
        unsafe {
            Self {
                inner: MapxOrdRawValue::from_bytes(s),
            }
        }
    }

    /// Returns the byte representation of the `VecxRaw`.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    /// Creates a new, empty `VecxRaw`.
    #[inline(always)]
    pub fn new() -> Self {
        VecxRaw {
            inner: MapxOrdRawValue::new(),
        }
    }

    /// Retrieves a value at a specific index.
    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<RawValue> {
        self.inner.get(&(idx as u64))
    }

    /// Retrieves a mutable reference to a value at a specific index.
    #[inline(always)]
    pub fn get_mut(&mut self, idx: usize) -> Option<ValueMut<'_, u64>> {
        let idx = idx as u64;
        self.inner
            .get(&idx)
            .map(|v| ValueMut::new(&mut self.inner, idx, v))
    }

    /// Retrieves the last value in the vector.
    #[inline(always)]
    pub fn last(&self) -> Option<RawValue> {
        alt!(self.is_empty(), return None);
        Some(self.inner.get(&(self.len() as u64 - 1)).unwrap())
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
    pub fn push(&mut self, v: impl AsRef<[u8]>) {
        self.inner.insert(&(self.len() as u64), v.as_ref());
    }

    /// Inserts a value at a specific index.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of bounds.
    #[inline(always)]
    pub fn insert(&mut self, idx: usize, v: impl AsRef<[u8]>) {
        let idx = idx as u64;
        match (self.len() as u64).cmp(&idx) {
            Ordering::Greater => {
                let shadow = unsafe { self.inner.shadow() };
                shadow
                    .range(&idx..&(self.len() as u64))
                    .for_each(|(i, iv)| {
                        self.inner.insert(&(i + 1), &iv);
                    });
                self.inner.insert(&idx, v.as_ref());
            }
            Ordering::Equal => {
                self.push(v.as_ref());
            }
            Ordering::Less => {
                panic!("out of index");
            }
        }
    }

    /// Removes and returns the last value in the vector.
    #[inline(always)]
    pub fn pop(&mut self) -> Option<RawValue> {
        alt!(self.is_empty(), return None);
        self.inner.remove(&(self.len() as u64 - 1))
    }

    /// Removes and returns the value at a specific index.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of bounds.
    #[inline(always)]
    pub fn remove(&mut self, idx: usize) -> RawValue {
        let idx = idx as u64;
        if !self.is_empty() && idx < self.len() as u64 {
            let last_idx = self.len() as u64 - 1;
            let ret = self.inner.remove(&idx).unwrap();
            let shadow = unsafe { self.inner.shadow() };
            for (i, v) in shadow.range(&(1 + idx)..) {
                self.inner.insert(&(i - 1), &v);
            }
            self.inner.remove(&last_idx);
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
    pub fn swap_remove(&mut self, idx: usize) -> RawValue {
        let idx = idx as u64;
        if !self.is_empty() && idx < self.len() as u64 {
            let last_idx = self.len() as u64 - 1;
            let ret = self.inner.remove(&idx).unwrap();
            if let Some(v) = self.inner.remove(&last_idx) {
                self.inner.insert(&idx, &v);
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
    pub fn update(&mut self, idx: usize, v: impl AsRef<[u8]>) -> Option<RawValue> {
        if idx < self.len() {
            return self.inner.insert(&(idx as u64), v.as_ref());
        }
        panic!("out of index");
    }

    /// Returns an iterator over the vector's values.
    #[inline(always)]
    pub fn iter(&self) -> VecxRawIter {
        VecxRawIter {
            iter: self.inner.inner.iter(),
        }
    }

    /// Returns a mutable iterator over the vector's values.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> VecxRawIterMut {
        VecxRawIterMut {
            inner: self.inner.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    /// Clears the vector, removing all values.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Checks if this `VecxRaw` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }
}

impl Default for VecxRaw {
    fn default() -> Self {
        Self::new()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// An iterator over the values of a `VecxRaw`.
pub struct VecxRawIter<'a> {
    iter: MapxRawIter<'a>,
}

impl Iterator for VecxRawIter<'_> {
    type Item = RawValue;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(_, v)| v)
    }
}

impl DoubleEndedIterator for VecxRawIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|(_, v)| v)
    }
}

/// A mutable iterator over the values of a `VecxRaw`.
type VecxRawIterMut<'a> = MapxOrdRawValueIterMut<'a, usize>;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
