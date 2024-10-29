//!
//! A disk-storage replacement for the in-memory Vec.
//!
//! NOTE:
//! - Values will be encoded by some `serde`-like methods
//!
//! # Examples
//!
//! ```
//! use vsdb::Vecx;
//!
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb::vsdb_set_base_dir(&dir);
//!
//! let mut l = Vecx::new();
//!
//! l.push(1);
//! for i in l.iter() {
//!     assert_eq!(1, i);
//! }
//!
//! l.pop();
//! assert_eq!(l.len(), 0);
//!
//! l.insert(0, 1);
//! assert_eq!(l.len(), 1);
//!
//! l.clear();
//! assert_eq!(l.len(), 0);
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

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct Vecx<T> {
    inner: MapxOrdRawKey<T>,
}

impl<T: ValueEnDe> Vecx<T> {
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

    /// # Safety
    ///
    /// Do not use this API unless you know the internal details extremely well.
    #[inline(always)]
    pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
        Self {
            inner: MapxOrdRawKey::from_bytes(s),
        }
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    #[inline(always)]
    pub fn new() -> Self {
        Vecx {
            inner: MapxOrdRawKey::new(),
        }
    }

    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<T> {
        self.inner.get((idx as u64).to_be_bytes())
    }

    #[inline(always)]
    pub fn get_mut(&mut self, idx: usize) -> Option<ValueMut<'_, T>> {
        self.inner.get_mut((idx as u64).to_be_bytes())
    }

    #[inline(always)]
    pub fn last(&self) -> Option<T> {
        alt!(self.is_empty(), return None);
        Some(
            self.inner
                .get((self.len() as u64 - 1).to_be_bytes())
                .unwrap(),
        )
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
    pub fn push(&mut self, v: &T) {
        self.inner.insert((self.len() as u64).to_be_bytes(), v);
    }

    #[inline(always)]
    pub fn insert(&mut self, idx: usize, v: &T) {
        let idx = idx as u64;
        match (self.len() as u64).cmp(&idx) {
            Ordering::Greater => {
                let shadow = unsafe { self.inner.shadow() };
                shadow
                    .range(
                        Cow::Borrowed(&idx.to_be_bytes()[..])
                            ..Cow::Borrowed(&(self.len() as u64).to_be_bytes()[..]),
                    )
                    .for_each(|(i, iv)| {
                        self.inner
                            .insert((crate::parse_int!(i, u64) + 1).to_be_bytes(), &iv);
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

    #[inline(always)]
    pub fn pop(&mut self) -> Option<T> {
        alt!(self.is_empty(), return None);
        self.inner.remove((self.len() as u64 - 1).to_be_bytes())
    }

    #[inline(always)]
    pub fn remove(&mut self, idx: usize) -> T {
        let idx = idx as u64;
        if !self.is_empty() && idx < self.len() as u64 {
            let last_idx = self.len() as u64 - 1;
            let ret = self.inner.remove(idx.to_be_bytes()).unwrap();
            let shadow = unsafe { self.inner.shadow() };
            shadow
                .range(Cow::Borrowed(&(1 + idx).to_be_bytes()[..])..)
                .for_each(|(i, v)| {
                    self.inner
                        .insert((crate::parse_int!(i, u64) - 1).to_be_bytes(), &v);
                });
            self.inner.remove(last_idx.to_be_bytes());
            return ret;
        }
        panic!("out of index");
    }

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

    #[inline(always)]
    pub fn update(&mut self, idx: usize, v: &T) -> Option<T> {
        if idx < self.len() {
            return self.inner.insert((idx as u64).to_be_bytes(), v);
        }
        panic!("out of index");
    }

    #[inline(always)]
    pub fn iter(&self) -> VecxIter<T> {
        VecxIter(self.inner.iter())
    }

    #[inline(always)]
    pub fn iter_mut(&mut self) -> VecxIterMut<T> {
        VecxIterMut(self.inner.iter_mut())
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

pub struct VecxIter<'a, T>(MapxOrdRawKeyIter<'a, T>);

impl<'a, T> Iterator for VecxIter<'a, T>
where
    T: ValueEnDe,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(_, v)| v)
    }
}

impl<'a, V> DoubleEndedIterator for VecxIter<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(_, v)| v)
    }
}

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

impl<'a, V> DoubleEndedIterator for VecxIterMut<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(_, v)| v)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
