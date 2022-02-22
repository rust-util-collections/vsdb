//!
//! A multi-key style of `MapxRaw`.
//!
//! NOTE:
//! - Both keys and values will **NOT** be encoded in this structure
//!

use crate::basic::mapx_raw::MapxRaw;
use crate::common::{ende::ValueEnDe, RawValue};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxRawMk {
    // Will never be changed once created
    key_size: usize,
    // A nested map-structure, looks like:
    // map { key => map { key => map { key => value } } }
    inner: MapxRaw,
}

impl MapxRawMk {
    /// # Panic
    /// Will panic if `0 == key_size`.
    #[inline(always)]
    pub fn new(key_size: usize) -> Self {
        assert!(0 < key_size);
        Self {
            key_size,
            inner: MapxRaw::new(),
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &[&[u8]]) -> Option<RawValue> {
        if key.len() != self.key_size {
            return None;
        }

        let mut hdr = self.inner;
        for (idx, k) in key.iter().enumerate() {
            if let Some(v) = hdr.get(k) {
                if idx == self.key_size - 1 {
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

    #[inline(always)]
    pub fn get_mut<'a>(&'a self, key: &'a [&'a [u8]]) -> Option<ValueMut<'a>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &[&[u8]]) -> bool {
        self.get(key).is_some()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    pub fn entry_ref<'a>(&'a self, key: &'a [&'a [u8]]) -> Entry<'a> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn insert(&self, key: &[&[u8]], value: &[u8]) -> Result<Option<RawValue>> {
        if key.len() != self.key_size {
            return Err(eg!("Incorrect key size"));
        }
        let mut ret = None;
        let mut hdr = self.inner;
        for (idx, k) in key.iter().enumerate() {
            if idx == self.key_size - 1 {
                ret = hdr.insert(k, value);
                break;
            } else {
                let h = hdr
                    .entry_ref(k)
                    .or_insert_ref(&MapxRaw::new().encode())
                    .get_hdr();
                hdr = h;
            }
        }
        Ok(ret)
    }

    #[inline(always)]
    pub fn remove(&self, key: &[&[u8]]) -> Result<Option<RawValue>> {
        // Support batch removal from key path.
        if key.len() > self.key_size {
            return Err(eg!("Incorrect key size"));
        }

        let mut hdr = self.inner;
        for (idx, k) in key.iter().enumerate() {
            if let Some(v) = hdr.get(k) {
                if idx == self.key_size - 1 {
                    return Ok(Some(v));
                } else {
                    hdr = pnk!(ValueEnDe::decode(&v));
                }
            } else {
                return Ok(None);
            }
        }

        Ok(None) // empty key
    }

    #[inline(always)]
    pub fn clear(&self) {
        self.inner.clear();
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a> {
    hdr: &'a MapxRawMk,
    key: &'a [&'a [u8]],
    value: RawValue,
}

impl<'a> ValueMut<'a> {
    fn new(hdr: &'a MapxRawMk, key: &'a [&'a [u8]], value: RawValue) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<'a> Drop for ValueMut<'a> {
    fn drop(&mut self) {
        pnk!(self.hdr.insert(self.key, &self.value));
    }
}

impl<'a> Deref for ValueMut<'a> {
    type Target = RawValue;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a> DerefMut for ValueMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub struct Entry<'a> {
    key: &'a [&'a [u8]],
    hdr: &'a MapxRawMk,
}

impl<'a> Entry<'a> {
    pub fn or_insert_ref(self, default: &'a [u8]) -> ValueMut<'a> {
        if !self.hdr.contains_key(self.key) {
            pnk!(self.hdr.insert(self.key, default));
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}
