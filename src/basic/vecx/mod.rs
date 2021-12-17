//!
//! A disk-storage replacement for the pure in-memory Vec.
//!

#[cfg(test)]
mod test;

use crate::{
    basic::mapx_oc::{MapxOC, MapxOCIter, ValueMut},
    common::{InstanceCfg, SimpleVisitor},
};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{cmp::Ordering, fmt};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory 'Vec'.
///
/// - Each time the program is started, a new database is created
#[derive(PartialEq, Eq, Debug)]
pub struct Vecx<T>
where
    T: Serialize + DeserializeOwned + fmt::Debug,
{
    inner: MapxOC<usize, T>,
}

impl<T> From<InstanceCfg> for Vecx<T>
where
    T: Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            inner: MapxOC::from(cfg),
        }
    }
}

impl<T> Default for Vecx<T>
where
    T: Serialize + DeserializeOwned + fmt::Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

///////////////////////////////////////////////
// Begin of the self-implementation for Vecx //
/*********************************************/

impl<T> Vecx<T>
where
    T: Serialize + DeserializeOwned + fmt::Debug,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        Vecx {
            inner: MapxOC::new(),
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
    pub fn push(&mut self, b: &T) {
        self.inner.insert(self.len(), b);
    }

    /// Imitate the behavior of 'Vec<_>.insert()'
    #[inline(always)]
    pub fn insert(&mut self, idx: usize, v: &T) {
        match self.len().cmp(&idx) {
            Ordering::Greater => {
                self.inner.range(idx..self.len()).for_each(|(i, v)| {
                    self.inner.insert(i + 1, &v);
                });
                self.inner.insert(idx, v);
            }
            Ordering::Equal => {
                self.push(v);
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
                self.inner.insert(i - 1, &v);
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
                self.inner.insert(idx, &v);
            }
            return ret;
        }
        panic!("out of index");
    }

    /// Imitate the behavior of 'Vec<_>.update(idx, value)'
    #[inline(always)]
    pub fn update(&mut self, idx: usize, b: &T) -> Option<T> {
        if idx < self.len() {
            return self.inner.insert(idx, b);
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

/*******************************************/
// End of the self-implementation for Vecx //
/////////////////////////////////////////////

//////////////////////////////////////////////////
// Begin of the implementation of Iter for Vecx //
/************************************************/

/// Iter over [Vecx](self::Vecx).
pub struct VecxIter<T>
where
    T: Serialize + DeserializeOwned + fmt::Debug,
{
    iter: MapxOCIter<usize, T>,
}

impl<T> Iterator for VecxIter<T>
where
    T: Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.1)
    }
}

impl<T> DoubleEndedIterator for VecxIter<T>
where
    T: Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|v| v.1)
    }
}

/**********************************************/
// End of the implementation of Iter for Vecx //
////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////
// Begin of the implementation of Serialize/Deserialize for Vecx //
/*****************************************************************/

impl<'a, T> serde::Serialize for Vecx<T>
where
    T: Serialize + DeserializeOwned + fmt::Debug,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let v = pnk!(bcs::to_bytes(&self.get_instance_cfg()));
        serializer.serialize_bytes(&v)
    }
}

impl<'de, T> serde::Deserialize<'de> for Vecx<T>
where
    T: Serialize + DeserializeOwned + fmt::Debug,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SimpleVisitor).map(|meta| {
            let meta = pnk!(bcs::from_bytes::<InstanceCfg>(&meta));
            Vecx::from(meta)
        })
    }
}

/***************************************************************/
// End of the implementation of Serialize/Deserialize for Vecx //
/////////////////////////////////////////////////////////////////
