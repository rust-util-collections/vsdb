//!
//! A disk-storage replacement for the in-memory Vec.
//!
//! NOTE:
//! - Values will be encoded by some `serde`-like methods
//!

#[cfg(test)]
mod test;

use crate::{
    basic::mapx_ord::{MapxOrd, MapxOrdIter, ValueMut},
    common::{
        ende::{SimpleVisitor, ValueEnDe},
        InstanceCfg,
    },
};
use ruc::*;
use std::{cmp::Ordering, result::Result as StdResult};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory 'Vec'.
///
/// - Each time the program is started, a new database is created
#[derive(PartialEq, Eq, Debug)]
pub struct Vecx<T: ValueEnDe> {
    inner: MapxOrd<usize, T>,
}

impl<T: ValueEnDe> From<InstanceCfg> for Vecx<T> {
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            inner: MapxOrd::from(cfg),
        }
    }
}

impl<T: ValueEnDe> Default for Vecx<T> {
    fn default() -> Self {
        Self::new()
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<T: ValueEnDe> Vecx<T> {
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        Vecx {
            inner: MapxOrd::new(),
        }
    }

    // Get the meta-storage path
    fn get_instance_cfg(&self) -> InstanceCfg {
        self.inner.get_instance_cfg()
    }

    /// Imitate the behavior of 'Vec<_>.get(...)'
    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<T> {
        self.inner.get(&idx)
    }

    /// Imitate the behavior of 'Vec<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, idx: usize) -> Option<ValueMut<'_, usize, T>> {
        self.inner
            .get(&idx)
            .map(move |v| ValueMut::new(&mut self.inner, idx, v))
    }

    /// Imitate the behavior of 'Vec<_>.last()'
    #[inline(always)]
    pub fn last(&self) -> Option<T> {
        alt!(self.is_empty(), return None);
        // must exist
        Some(self.inner.get(&(self.len() - 1)).unwrap())
    }

    /// Imitate the behavior of 'Vec<_>.len()'
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// A helper func
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Imitate the behavior of 'Vec<_>.push(...)'
    #[inline(always)]
    pub fn push(&mut self, v: T) {
        self.push_ref(&v)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn push_ref(&mut self, v: &T) {
        self.inner.insert_ref(&self.len(), v);
    }

    /// Imitate the behavior of 'Vec<_>.insert()'
    #[inline(always)]
    pub fn insert(&mut self, idx: usize, v: T) {
        self.insert_ref(idx, &v)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn insert_ref(&mut self, idx: usize, v: &T) {
        match self.len().cmp(&idx) {
            Ordering::Greater => {
                self.inner.range(idx..self.len()).for_each(|(i, iv)| {
                    self.inner.insert(i + 1, iv);
                });
                self.inner.insert_ref(&idx, v);
            }
            Ordering::Equal => {
                self.push_ref(v);
            }
            Ordering::Less => {
                panic!("out of index");
            }
        }
    }

    /// Imitate the behavior of 'Vec<_>.pop()'
    #[inline(always)]
    pub fn pop(&mut self) -> Option<T> {
        alt!(self.is_empty(), return None);
        self.inner.remove(&(self.len() - 1))
    }

    /// Imitate the behavior of 'Vec<_>.remove()'
    #[inline(always)]
    pub fn remove(&mut self, idx: usize) -> T {
        if !self.is_empty() && idx < self.len() {
            let last_idx = self.len() - 1;
            let ret = self.inner.remove(&idx).unwrap();
            self.inner.range((1 + idx)..).for_each(|(i, v)| {
                self.inner.insert(i - 1, v);
            });
            self.inner.remove(&last_idx);
            return ret;
        }
        panic!("out of index");
    }

    /// Imitate the behavior of 'Vec<_>.swap_remove()'
    #[inline(always)]
    pub fn swap_remove(&mut self, idx: usize) -> T {
        if !self.is_empty() && idx < self.len() {
            let last_idx = self.len() - 1;
            let ret = self.inner.remove(&idx).unwrap();
            if let Some(v) = self.inner.remove(&last_idx) {
                self.inner.insert(idx, v);
            }
            return ret;
        }
        panic!("out of index");
    }

    /// Imitate the behavior of 'Vec<_>.update(idx, value)'
    pub fn update(&mut self, idx: usize, v: T) -> Option<T> {
        self.update_ref(idx, &v)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn update_ref(&mut self, idx: usize, v: &T) -> Option<T> {
        if idx < self.len() {
            return self.inner.insert_ref(&idx, v);
        }
        panic!("out of index");
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> VecxIter<T> {
        VecxIter {
            iter: self.inner.iter(),
        }
    }

    /// Clear all data.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

#[allow(missing_docs)]
pub struct VecxIter<T: ValueEnDe> {
    iter: MapxOrdIter<usize, T>,
}

impl<T: ValueEnDe> Iterator for VecxIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.1)
    }
}

impl<T: ValueEnDe> DoubleEndedIterator for VecxIter<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|v| v.1)
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<'a, T: ValueEnDe> serde::Serialize for Vecx<T> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&<InstanceCfg as ValueEnDe>::encode(
            &self.get_instance_cfg(),
        ))
    }
}

impl<'de, T> serde::Deserialize<'de> for Vecx<T>
where
    T: ValueEnDe,
{
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer
            .deserialize_bytes(SimpleVisitor)
            .map(|cfg| Vecx::from(<InstanceCfg as ValueEnDe>::decode(&cfg).unwrap()))
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
