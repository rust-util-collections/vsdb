//!
//! A multi-key version of `MapxRaw`.
//!
//! `MapxRawMk` (MapxRaw Multi-Key) provides a map-like interface where each value
//! is associated with a sequence of keys. This allows for creating nested or
//! hierarchical key structures with raw byte keys and values.
//!
//! # Examples
//!
//! ```
//! use vsdb::basic_multi_key::mapx_raw::MapxRawMk;
//! use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut m = MapxRawMk::new(2); // Two keys
//!
//! // Insert a value with a multi-key
//! m.insert(&[&[1], &[10]], &[100]).unwrap();
//! m.insert(&[&[2], &[20]], &[200]).unwrap();
//!
//! // Get a value
//! assert_eq!(m.get(&[&[1], &[10]]), Some(vec![100]));
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

use crate::{
    MapxRaw,
    common::{RawKey, RawValue, ende::ValueEnDe},
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

/// A multi-key map with raw keys and values.
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(bound = "")]
pub struct MapxRawMk {
    // Will never be changed once created
    key_size: u32,
    // A nested map-structure, looks like:
    // map { key => map { key => map { key => value } } }
    inner: MapxRaw,
}

impl MapxRawMk {
    /// Creates a "shadow" copy of the `MapxRawMk` instance.
    ///
    /// # Safety
    ///
    /// This API breaks Rust's semantic safety guarantees. Use only in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        unsafe {
            Self {
                key_size: self.key_size,
                inner: self.inner.shadow(),
            }
        }
    }

    /// Creates a new `MapxRawMk` with a specified number of keys.
    ///
    /// # Panics
    ///
    /// Panics if `key_size` is 0.
    #[inline(always)]
    pub fn new(key_size: u32) -> Self {
        assert!(0 < key_size);
        Self {
            key_size,
            inner: MapxRaw::new(),
        }
    }

    /// Retrieves a value from the map for a given multi-key.
    #[inline(always)]
    pub fn get(&self, key: &[&[u8]]) -> Option<RawValue> {
        if key.len() != self.key_size as usize {
            return None;
        }

        let mut hdr = unsafe { self.inner.shadow() };
        for (idx, k) in key.iter().enumerate() {
            if let Some(v) = hdr.get(k) {
                if 1 + idx == self.key_size as usize {
                    return Some(v);
                } else {
                    hdr = pnk!(ValueEnDe::decode(&v));
                }
            } else {
                return None;
            }
        }

        None // empty key
    }

    /// Retrieves a mutable reference to a value in the map.
    #[inline(always)]
    pub fn get_mut<'a>(&'a mut self, key: &'a [&'a [u8]]) -> Option<ValueMut<'a>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    /// Mocks a mutable value for a given key.
    #[inline(always)]
    pub(crate) fn mock_value_mut<'a>(
        &'a mut self,
        key: &'a [&'a [u8]],
        v: RawValue,
    ) -> ValueMut<'a> {
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
    pub fn entry<'a>(&'a mut self, key: &'a [&'a [u8]]) -> Result<Entry<'a>> {
        if key.len() != self.key_size() as usize {
            Err(eg!())
        } else {
            Ok(Entry { key, hdr: self })
        }
    }

    /// Inserts a key-value pair into the map.
    #[inline(always)]
    pub fn insert(&mut self, key: &[&[u8]], value: &[u8]) -> Result<Option<RawValue>> {
        if key.len() != self.key_size as usize {
            return Err(eg!("Incorrect key size"));
        }

        let mut ret = None;

        let mut hdr = unsafe { self.inner.shadow() };
        for (idx, k) in key.iter().enumerate() {
            if 1 + idx == self.key_size as usize {
                ret = hdr.insert(k, value);
                break;
            } else {
                let mut new_hdr = None;
                let f = || {
                    new_hdr.replace(MapxRaw::new());
                    new_hdr.as_ref().unwrap().encode()
                };
                let mutv = hdr.entry(k).or_insert_with(f);
                let h = if let Some(h) = new_hdr {
                    h
                } else {
                    pnk!(ValueEnDe::decode(mutv.as_ref()))
                };
                drop(mutv);
                hdr = h;
            }
        }

        Ok(ret)
    }

    /// Removes a key-value pair from the map. Supports batch removal by providing a partial key.
    #[inline(always)]
    pub fn remove(&mut self, key: &[&[u8]]) -> Result<Option<RawValue>> {
        // Support batch removal from key path.
        if key.len() > self.key_size as usize {
            return Err(eg!("Incorrect key size"));
        }

        let mut hdr = unsafe { self.inner.shadow() };
        for (idx, k) in key.iter().enumerate() {
            if let Some(v) = hdr.get(k) {
                // NOTE: use `key.len()` instead of `self.key_size`
                if 1 + idx == key.len() {
                    let ret = hdr.remove(k);
                    // NOTE: use `self.key_size` instead of `key.len()`
                    if 1 + idx == self.key_size as usize {
                        return Ok(ret);
                    } else {
                        return Ok(None);
                    }
                } else {
                    hdr = pnk!(ValueEnDe::decode(&v));
                }
            } else {
                return Ok(None);
            }
        }

        Ok(None) // empty key
    }

    /// Clears the map, removing all key-value pairs.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Checks if this `MapxRawMk` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }

    /// Returns the number of keys in the map.
    #[inline(always)]
    pub fn key_size(&self) -> u32 {
        self.key_size
    }

    /// Iterates over the map's entries, applying a function to each.
    #[inline(always)]
    pub fn iter_op<F>(&self, op: &mut F) -> Result<()>
    where
        F: FnMut(&[&[u8]], &[u8]) -> Result<()>,
    {
        self.iter_op_with_key_prefix(op, &[]).c(d!())
    }

    /// Iterates over the map's entries with a given key prefix, applying a function to each.
    #[inline(always)]
    pub fn iter_op_with_key_prefix<F>(
        &self,
        op: &mut F,
        key_prefix: &[&[u8]],
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], &[u8]) -> Result<()>,
    {
        let key_size = self.key_size() as usize;
        let mut key_buf = vec![RawKey::default(); key_size];
        let mut hdr = unsafe { self.inner.shadow() };
        let mut depth = key_size;

        if key_size < key_prefix.len() {
            return Err(eg!("Invalid key size"));
        } else {
            for (idx, k) in key_prefix.iter().enumerate() {
                if let Some(v) = hdr.get(k) {
                    key_buf[idx] = k.to_vec();
                    if 1 + idx == key_size {
                        let key = key_buf
                            .iter()
                            .map(|sub_k| sub_k.as_ref())
                            .collect::<Vec<_>>();
                        return op(key.as_slice(), &v).c(d!());
                    } else {
                        hdr = pnk!(ValueEnDe::decode(&v));
                        depth -= 1;
                    }
                } else {
                    // key-prefix does not exist
                    return Ok(());
                }
            }
        };

        self.recursive_walk(hdr, key_buf.as_mut_slice(), depth as u32, op)
            .c(d!())
    }

    fn recursive_walk<F>(
        &self,
        hdr: MapxRaw,
        key_buf: &mut [RawKey],
        depth: u32,
        op: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], &[u8]) -> Result<()>,
    {
        let idx = (self.key_size() - depth) as usize;
        if 1 == depth {
            for (k, v) in hdr.iter() {
                key_buf[idx] = k;
                let key = key_buf
                    .iter()
                    .map(|sub_k| sub_k.as_ref())
                    .collect::<Vec<_>>();
                op(key.as_slice(), &v[..]).c(d!())?;
            }
        } else {
            for (k, v) in hdr.iter() {
                key_buf[idx] = k;
                let hdr = pnk!(ValueEnDe::decode(&v));
                self.recursive_walk(hdr, key_buf, depth - 1, op).c(d!())?;
            }
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn iter_op_typed_value<V, F>(&self, op: &mut F) -> Result<()>
    where
        F: FnMut(&[&[u8]], &V) -> Result<()>,
        V: ValueEnDe,
    {
        self.iter_op_typed_value_with_key_prefix(op, &[]).c(d!())
    }

    /// Iterates over the map's entries with a given key prefix, applying a function to each typed value.
    #[inline(always)]
    pub fn iter_op_typed_value_with_key_prefix<V, F>(
        &self,
        op: &mut F,
        key_prefix: &[&[u8]],
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], &V) -> Result<()>,
        V: ValueEnDe,
    {
        let key_size = self.key_size() as usize;
        let mut key_buf = vec![RawKey::default(); key_size];
        let mut hdr = unsafe { self.inner.shadow() };
        let mut depth = key_size;

        if key_size < key_prefix.len() {
            return Err(eg!("Invalid key size"));
        } else {
            for (idx, k) in key_prefix.iter().enumerate() {
                if let Some(v) = hdr.get(k) {
                    key_buf[idx] = k.to_vec();
                    if 1 + idx == key_size {
                        let key = key_buf
                            .iter()
                            .map(|sub_k| sub_k.as_ref())
                            .collect::<Vec<_>>();
                        return op(key.as_slice(), &pnk!(ValueEnDe::decode(&v))).c(d!());
                    } else {
                        hdr = pnk!(ValueEnDe::decode(&v));
                        depth -= 1;
                    }
                } else {
                    // key-prefix does not exist
                    return Ok(());
                }
            }
        };

        self.recursive_walk_typed_value(hdr, key_buf.as_mut_slice(), depth as u32, op)
            .c(d!())
    }

    fn recursive_walk_typed_value<V, F>(
        &self,
        hdr: MapxRaw,
        key_buf: &mut [RawKey],
        depth: u32,
        op: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], &V) -> Result<()>,
        V: ValueEnDe,
    {
        let idx = (self.key_size() - depth) as usize;
        if 1 == depth {
            for (k, v) in hdr.iter() {
                key_buf[idx] = k;
                let key = key_buf
                    .iter()
                    .map(|sub_k| sub_k.as_ref())
                    .collect::<Vec<_>>();
                op(key.as_slice(), &pnk!(ValueEnDe::decode(&v))).c(d!())?;
            }
        } else {
            for (k, v) in hdr.iter() {
                key_buf[idx] = k;
                let hdr = pnk!(ValueEnDe::decode(&v));
                self.recursive_walk_typed_value(hdr, key_buf, depth - 1, op)
                    .c(d!())?;
            }
        }

        Ok(())
    }

    // TODO
    // pub fn iter_mut_op
    // pub fn iter_mut_op_with_key_prefix
    // pub fn iter_mut_op_typed_value
    // pub fn iter_mut_op_typed_value_with_key_prefix
}

/// A mutable reference to a value in a `MapxRawMk`.
#[derive(Debug)]
pub struct ValueMut<'a> {
    hdr: &'a mut MapxRawMk,
    key: &'a [&'a [u8]],
    value: RawValue,
}

impl<'a> ValueMut<'a> {
    fn new(hdr: &'a mut MapxRawMk, key: &'a [&'a [u8]], value: RawValue) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl Drop for ValueMut<'_> {
    fn drop(&mut self) {
        pnk!(self.hdr.insert(self.key, &self.value));
    }
}

impl Deref for ValueMut<'_> {
    type Target = RawValue;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for ValueMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/// A view into a single entry in a map, which may either be vacant or occupied.
pub struct Entry<'a> {
    key: &'a [&'a [u8]],
    hdr: &'a mut MapxRawMk,
}

impl<'a> Entry<'a> {
    /// Ensures a value is in the entry by inserting the default if empty,
    /// and returns a mutable reference to the value.
    pub fn or_insert(self, default: &'a [u8]) -> ValueMut<'a> {
        let hdr = self.hdr as *mut MapxRawMk;
        match unsafe { &mut *hdr }.get_mut(self.key) {
            Some(v) => v,
            _ => unsafe { &mut *hdr }.mock_value_mut(self.key, default.to_vec()),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
