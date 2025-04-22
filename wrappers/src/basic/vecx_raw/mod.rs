//!
//! A disk-storage replacement for the in-memory Vec.
//!
//! NOTE:
//! - Values will be encoded by some `serde`-like methods
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::vecx_raw::VecxRaw;
//!
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb::vsdb_set_base_dir(&dir);
//!
//! let mut l = VecxRaw::new();
//!
//! l.push(&1u8.to_be_bytes());
//! for i in l.iter() {
//!     assert_eq!(&1u8.to_be_bytes(), &i[..]);
//! }
//!
//! l.pop();
//! assert_eq!(l.len(), 0);
//!
//! l.insert(0, &1u8.to_be_bytes());
//! assert_eq!(l.len(), 1);
//!
//! l.clear();
//! assert_eq!(l.len(), 0);
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct VecxRaw {
    inner: MapxOrdRawValue<u64>,
}

impl VecxRaw {
    /// # Safety
    ///
    /// This API breaks the semantic safety guarantees,
    /// but it is safe to use in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        unsafe {
            Self {
                inner: self.inner.shadow(),
            }
        }
    }

    /// # Safety
    ///
    /// Do not use this API unless you know the internal details extremely well.
    #[inline(always)]
    pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
        unsafe {
            Self {
                inner: MapxOrdRawValue::from_bytes(s),
            }
        }
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    #[inline(always)]
    pub fn new() -> Self {
        VecxRaw {
            inner: MapxOrdRawValue::new(),
        }
    }

    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<RawValue> {
        self.inner.get(&(idx as u64))
    }

    #[inline(always)]
    pub fn get_mut(&mut self, idx: usize) -> Option<ValueMut<'_, u64>> {
        let idx = idx as u64;
        self.inner
            .get(&idx)
            .map(|v| ValueMut::new(&mut self.inner, idx, v))
    }

    #[inline(always)]
    pub fn last(&self) -> Option<RawValue> {
        alt!(self.is_empty(), return None);
        Some(self.inner.get(&(self.len() as u64 - 1)).unwrap())
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
    pub fn push(&mut self, v: impl AsRef<[u8]>) {
        self.inner.insert(&(self.len() as u64), v.as_ref());
    }

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

    #[inline(always)]
    pub fn pop(&mut self) -> Option<RawValue> {
        alt!(self.is_empty(), return None);
        self.inner.remove(&(self.len() as u64 - 1))
    }

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

    #[inline(always)]
    pub fn update(&mut self, idx: usize, v: impl AsRef<[u8]>) -> Option<RawValue> {
        if idx < self.len() {
            return self.inner.insert(&(idx as u64), v.as_ref());
        }
        panic!("out of index");
    }

    #[inline(always)]
    pub fn iter(&self) -> VecxRawIter {
        VecxRawIter {
            iter: self.inner.inner.iter(),
        }
    }

    #[inline(always)]
    pub fn iter_mut(&mut self) -> VecxRawIterMut {
        VecxRawIterMut {
            inner: self.inner.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

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

type VecxRawIterMut<'a> = MapxOrdRawValueIterMut<'a, usize>;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
