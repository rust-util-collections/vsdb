//!
//! A `Map`-like structure but storing data in disk.
//!
//! NOTE:
//! - Both keys and values will **NOT** be encoded in this structure
//!
//! # Examples
//!
//! ```
//! use vsdb_core::basic::mapx_raw::MapxRaw;
//!
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_core::vsdb_set_base_dir(&dir);
//!
//! let mut l = MapxRaw::new();
//!
//! l.insert(&[1], &[0]);
//! l.insert(&[1], &[0]);
//! l.insert(&[2], &[0]);
//!
//! l.iter().for_each(|(_, v)| {
//!     assert_eq!(&v[..], &[0]);
//! });
//!
//! l.remove(&[2]);
//! assert_eq!(l.len(), 1);
//!
//! l.clear();
//! assert_eq!(l.len(), 0);
//! ```
//!

#[cfg(test)]
mod test;

use crate::common::{engines, RawKey, RawValue};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, ops::RangeBounds};

pub type MapxRawIter<'a> = engines::MapxIter<'a>;
pub type MapxRawIterMut<'a> = engines::MapxIterMut<'a>;
pub type ValueMut<'a> = engines::ValueMut<'a>;
pub type ValueIterMut<'a> = engines::ValueIterMut<'a>;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxRaw {
    inner: engines::Mapx,
}

impl MapxRaw {
    /// # Safety
    ///
    /// This API breaks the semantic safety guarantees,
    /// but it is safe to use in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        Self {
            inner: self.inner.shadow(),
        }
    }

    #[inline(always)]
    pub fn new() -> Self {
        MapxRaw {
            inner: engines::Mapx::new(),
        }
    }

    #[inline(always)]
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<RawValue> {
        self.inner.get(key.as_ref())
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_>> {
        self.inner.get_mut(key.as_ref())
    }

    #[inline(always)]
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> bool {
        self.get(key.as_ref()).is_some()
    }

    #[inline(always)]
    pub fn get_le(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range(..=Cow::Borrowed(key)).next_back()
    }

    #[inline(always)]
    pub fn get_ge(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range(Cow::Borrowed(key)..).next()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    pub fn entry<'a>(&'a mut self, key: &'a [u8]) -> Entry<'a> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxRawIter {
        self.inner.iter()
    }

    #[inline(always)]
    pub fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(&'a self, bounds: R) -> MapxRawIter {
        self.inner.range(bounds)
    }

    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxRawIterMut {
        self.inner.iter_mut()
    }

    #[inline(always)]
    pub fn range_mut<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a mut self,
        bounds: R,
    ) -> MapxRawIterMut {
        self.inner.range_mut(bounds)
    }

    #[inline(always)]
    pub fn last(&self) -> Option<(RawKey, RawValue)> {
        self.iter().next_back()
    }

    #[inline(always)]
    pub fn insert(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Option<RawValue> {
        self.inner.insert(key.as_ref(), value.as_ref())
    }

    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Option<RawValue> {
        self.inner.remove(key.as_ref())
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// # Safety
    ///
    /// Do not use this API unless you know the internal details extremely well.
    #[inline(always)]
    pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
        Self::from_prefix_slice(s)
    }

    /// # Safety
    ///
    /// Do not use this API unless you know the internal details extremely well.
    #[inline(always)]
    pub unsafe fn from_prefix_slice(s: impl AsRef<[u8]>) -> Self {
        Self {
            inner: engines::Mapx::from_prefix_slice(s),
        }
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.as_prefix_slice()
    }

    #[inline(always)]
    pub fn as_prefix_slice(&self) -> &[u8] {
        self.inner.as_prefix_slice()
    }
}

impl Default for MapxRaw {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Entry<'a> {
    key: &'a [u8],
    hdr: &'a mut MapxRaw,
}

impl<'a> Entry<'a> {
    pub fn or_insert(self, default: &'a [u8]) -> ValueMut<'a> {
        if !self.hdr.contains_key(self.key) {
            self.hdr.insert(self.key, default);
        }
        pnk!(self.hdr.get_mut(self.key))
    }

    pub fn or_insert_with<F>(self, f: F) -> ValueMut<'a>
    where
        F: FnOnce() -> RawValue,
    {
        if !self.hdr.contains_key(self.key) {
            self.hdr.insert(self.key, &f());
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}
