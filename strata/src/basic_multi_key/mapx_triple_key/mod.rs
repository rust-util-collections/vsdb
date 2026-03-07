//!
//! A `Mapx` with a three-level key structure.
//!
//! `MapxTk` (Mapx Triple Key) provides a map-like interface where each value is
//! associated with a triplet of keys (K1, K2, K3). This is useful for creating
//! complex hierarchical or composite key structures.
//!
//! # Examples
//!
//! ```
//! use vsdb::basic_multi_key::mapx_triple_key::MapxTk;
//! use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut m: MapxTk<u32, u32, u32, String> = MapxTk::new();
//!
//! // Insert a value with a triple key
//! m.insert(&(&1, &10, &100), &"hello".to_string());
//! m.insert(&(&2, &20, &200), &"world".to_string());
//!
//! // Get a value
//! assert_eq!(m.get(&(&1, &10, &100)), Some("hello".to_string()));
//!
//! // Remove values
//! m.remove(&(&1, Some((&10, Some(&100)))));
//! assert!(!m.contains_key(&(&1, &10, &100)));
//!
//! // Clean up the directory
//! fs::remove_dir_all(vsdb_get_base_dir()).unwrap();
//! ```

#[cfg(test)]
mod test;

use crate::{
    basic_multi_key::mapx_raw::MapxRawMk,
    common::ende::{KeyEnDe, ValueEnDe},
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

const KEY_SIZE: u32 = 3;

/// A map structure with three-level keys.
#[derive(Serialize, Deserialize, Debug)]
#[serde(bound = "")]
pub struct MapxTk<K1, K2, K3, V> {
    inner: MapxRawMk,
    p: PhantomData<(K1, K2, K3, V)>,
}

impl<K1, K2, K3, V> MapxTk<K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    /// Creates a "shadow" copy of the `MapxTk` instance.
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

    /// Creates a new, empty `MapxTk`.
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            inner: MapxRawMk::new(KEY_SIZE),
            p: PhantomData,
        }
    }

    /// Retrieves a value from the map for a given triple key.
    #[inline(always)]
    pub fn get(&self, key: &(&K1, &K2, &K3)) -> Option<V> {
        let k1 = key.0.encode();
        let k2 = key.1.encode();
        let k3 = key.2.encode();
        self.inner
            .get(&[&k1, &k2, &k3])
            .map(|v| pnk!(ValueEnDe::decode(&v)))
    }

    /// Retrieves a mutable reference to a value in the map.
    #[inline(always)]
    pub fn get_mut<'a>(
        &'a mut self,
        key: &'a (&'a K1, &'a K2, &'a K3),
    ) -> Option<ValueMut<'a, K1, K2, K3, V>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    /// Mocks a mutable value for a given key.
    #[inline(always)]
    pub fn mock_value_mut<'a>(
        &'a mut self,
        key: &'a (&'a K1, &'a K2, &'a K3),
        v: V,
    ) -> ValueMut<'a, K1, K2, K3, V> {
        ValueMut::new(self, key, v)
    }

    /// Checks if the map contains a value for the specified triple key.
    #[inline(always)]
    pub fn contains_key(&self, key: &(&K1, &K2, &K3)) -> bool {
        self.get(key).is_some()
    }

    /// Checks if the map is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Gets an entry for a given key, allowing for in-place modification.
    #[inline(always)]
    pub fn entry<'a>(
        &'a mut self,
        key: &'a (&'a K1, &'a K2, &'a K3),
    ) -> Entry<'a, K1, K2, K3, V> {
        Entry { key, hdr: self }
    }

    /// Inserts a key-value pair into the map.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn insert(&mut self, key: &(&K1, &K2, &K3), value: &V) {
        let k1 = key.0.encode();
        let k2 = key.1.encode();
        let k3 = key.2.encode();
        let v = value.encode();
        pnk!(self.inner.insert(&[&k1, &k2, &k3], &v));
    }

    /// Removes a key-value pair from the map. Supports batch removal by omitting keys.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn remove(&mut self, key: &(&K1, Option<(&K2, Option<&K3>)>)) {
        let k1 = key.0.encode();
        let k2_k3 = key
            .1
            .map(|(k2, k3)| (k2.encode(), k3.map(|k3| k3.encode())));
        let k = if let Some((k2, k3)) = k2_k3.as_ref() {
            let mut res = vec![&k1[..], &k2[..]];
            if let Some(k3) = k3.as_ref() {
                res.push(&k3[..]);
            }
            res
        } else {
            vec![&k1[..]]
        };

        pnk!(self.inner.remove(k.as_slice()));
    }

    /// Clears the map, removing all key-value pairs.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Checks if this `MapxTk` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }

    /// Returns the number of keys in the map.
    #[inline(always)]
    pub fn key_size(&self) -> u32 {
        self.inner.key_size()
    }

    // TODO
    // pub fn iter_op
    // pub fn iter_op_with_key_prefix
    // pub fn iter_mut_op
    // pub fn iter_mut_op_with_key_prefix
}

impl<K1, K2, K3, V> Clone for MapxTk<K1, K2, K3, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            p: PhantomData,
        }
    }
}

impl<K1, K2, K3, V> Default for MapxTk<K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

/// A mutable reference to a value in a `MapxTk`.
#[derive(Debug)]
pub struct ValueMut<'a, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    hdr: &'a mut MapxTk<K1, K2, K3, V>,
    key: &'a (&'a K1, &'a K2, &'a K3),
    value: V,
}

impl<'a, K1, K2, K3, V> ValueMut<'a, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    fn new(
        hdr: &'a mut MapxTk<K1, K2, K3, V>,
        key: &'a (&'a K1, &'a K2, &'a K3),
        value: V,
    ) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<K1, K2, K3, V> Drop for ValueMut<'_, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    fn drop(&mut self) {
        self.hdr.insert(self.key, &self.value);
    }
}

impl<K1, K2, K3, V> Deref for ValueMut<'_, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<K1, K2, K3, V> DerefMut for ValueMut<'_, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/// A view into a single entry in a map, which may either be vacant or occupied.
pub struct Entry<'a, K1, K2, K3, V> {
    hdr: &'a mut MapxTk<K1, K2, K3, V>,
    key: &'a (&'a K1, &'a K2, &'a K3),
}

impl<'a, K1, K2, K3, V> Entry<'a, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    /// Ensures a value is in the entry by inserting the default if empty,
    /// and returns a mutable reference to the value.
    pub fn or_insert(self, default: V) -> ValueMut<'a, K1, K2, K3, V> {
        let hdr = self.hdr as *mut MapxTk<K1, K2, K3, V>;
        match unsafe { &mut *hdr }.get_mut(self.key) {
            Some(v) => v,
            _ => unsafe { &mut *hdr }.mock_value_mut(self.key, default),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
