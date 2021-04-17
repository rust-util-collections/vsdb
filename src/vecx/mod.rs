//!
//! # A mem+disk replacement for the pure in-memory Vec
//!
//! This module is non-invasive to external code except the `new` method.
//!

mod backend;
#[cfg(test)]
mod test;

/// Max number of entries stored in memory.
#[cfg(not(feature = "debug_env"))]
pub const IN_MEM_CNT: usize = 1_0000;

/// To make the 'mix storage' to be triggered during tests,
/// set it to 1 with the debug_env feature.
#[cfg(feature = "debug_env")]
pub const IN_MEM_CNT: usize = 1;

use crate::{
    helper::*,
    serde::{CacheMeta, CacheVisitor},
};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{btree_map, BTreeMap},
    fmt,
    iter::{DoubleEndedIterator, Iterator},
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `Vec<_>`.
///
/// - Each time the program is started, a new database is created
/// - Can ONLY be used in append-only scenes like the block storage
#[derive(PartialEq, Debug, Clone)]
pub struct Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    in_mem: BTreeMap<usize, T>,
    in_mem_cnt: usize,
    in_disk: backend::Vecx<T>,
}

///////////////////////////////////////////////
// Begin of the self-implementation for Vecx //
/*********************************************/

impl<T> Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new(path: &str, imc: Option<usize>) -> Result<Self> {
        let in_disk = backend::Vecx::load_or_create(path).c(d!())?;
        let in_mem_cnt = imc.unwrap_or(IN_MEM_CNT);
        Ok(Vecx {
            in_mem: in_disk.iter().rev().take(in_mem_cnt).collect(),
            in_mem_cnt,
            in_disk,
        })
    }

    /// Get the storage path
    pub fn get_data_path(&self) -> &str {
        self.in_disk.get_data_path()
    }

    /// Imitate the behavior of 'Vec<_>.get(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<Value<T>> {
        self.in_mem
            .get(&idx)
            .map(|v| Value::new(Cow::Borrowed(v)))
            .or_else(|| self.in_disk.get(idx).map(|v| Value::new(Cow::Owned(v))))
    }

    /// Imitate the behavior of 'Vec<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, idx: usize) -> Option<ValueMut<'_, T>> {
        self.in_mem
            .get(&idx)
            .cloned()
            .or_else(|| self.in_disk.get(idx))
            .map(move |v| ValueMut::new(self, idx, v))
    }

    /// Imitate the behavior of 'Vec<_>.last()'
    pub fn last(&self) -> Option<Value<T>> {
        self.in_mem
            .values()
            .last()
            .map(|v| Value::new(Cow::Borrowed(v)))
            .or_else(|| self.in_disk.last().map(|v| Value::new(Cow::Owned(v))))
    }

    /// Imitate the behavior of 'Vec<_>.len()'
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.in_disk.len()
    }

    /// A helper func
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.in_disk.is_empty()
    }

    /// Imitate the behavior of 'Vec<_>.push(...)'
    #[inline(always)]
    pub fn push(&mut self, b: T) {
        if self.in_mem.len() > IN_MEM_CNT {
            // Will get the oldest key since we use BTreeMap
            let k = pnk!(self.in_mem.keys().next().cloned());
            self.in_mem.remove(&k);
        }
        self.in_mem.insert(self.in_disk.len(), b.clone());
        self.in_disk.push(b);
    }

    /// Imitate the behavior of 'Vec<_>.insert(idx, value)',
    /// but we do not return the previous value, like `Vecx<_, _>.set_value`.
    #[inline(always)]
    pub fn set_value(&mut self, idx: usize, b: T) {
        if self.in_mem.len() > IN_MEM_CNT {
            // Will get the oldest key since we use BTreeMap
            let k = pnk!(self.in_mem.keys().next().cloned());
            self.in_mem.remove(&k);
        }
        self.in_mem.insert(idx, b.clone());
        self.in_disk.insert(idx, b);
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        debug_assert!(self.in_mem.len() <= self.in_disk.len());
        if self.in_mem.len() == self.in_disk.len() {
            Box::new(VecxIterMem {
                iter: self.in_mem.iter(),
            })
        } else {
            Box::new(VecxIter {
                iter: self.in_disk.iter(),
            })
        }
    }

    /// Flush data to disk
    #[inline(always)]
    pub fn flush_data(&self) {
        self.in_disk.flush();
    }
}

/*******************************************/
// End of the self-implementation for Vecx //
/////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Begin of the implementation of ValueMut(returned by `self.get_mut`) for Vecx //
/********************************************************************************/

/// Returned by `<Vecx>.get_mut(...)`
#[derive(Debug)]
pub struct ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    mapx: &'a mut Vecx<T>,
    idx: usize,
    value: ManuallyDrop<T>,
}

impl<'a, T> ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn new(mapx: &'a mut Vecx<T>, idx: usize, value: T) -> Self {
        ValueMut {
            mapx,
            idx,
            value: ManuallyDrop::new(value),
        }
    }

    /// Clone the inner value.
    pub fn clone_inner(self) -> T {
        ManuallyDrop::into_inner(self.value.clone())
    }
}

///
/// **NOTE**: VERY IMPORTANT !!!
///
impl<'a, T> Drop for ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn drop(&mut self) {
        // This operation is safe within a `drop()`.
        // SEE: [**ManuallyDrop::take**](std::mem::ManuallyDrop::take)
        unsafe {
            self.mapx
                .set_value(self.idx, ManuallyDrop::take(&mut self.value));
        };
    }
}

impl<'a, T> Deref for ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, T> DerefMut for ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'a, T> PartialEq for ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &ValueMut<'a, T>) -> bool {
        self.value == other.value
    }
}

impl<'a, T> PartialEq<T> for ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &T) -> bool {
        self.value.deref() == other
    }
}

impl<'a, T> PartialOrd<T> for ValueMut<'a, T>
where
    T: Clone + PartialEq + Ord + PartialOrd + Serialize + DeserializeOwned + fmt::Debug,
{
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        self.value.deref().partial_cmp(other)
    }
}

/******************************************************************************/
// End of the implementation of ValueMut(returned by `self.get_mut`) for Vecx //
////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////
// Begin of the implementation of Iter for Vecx //
/************************************************/

/// Iter over [Vecx](self::Vecx).
pub struct VecxIter<'a, T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    iter: backend::VecxIter<'a, T>,
}

impl<'a, T> Iterator for VecxIter<'a, T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.1)
    }
}

impl<'a, T> DoubleEndedIterator for VecxIter<'a, T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|v| v.1)
    }
}

/// Iter over [Vecx](self::Vecx).
pub struct VecxIterMem<'a, K, T>
where
    T: 'a + PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    iter: btree_map::Iter<'a, K, T>,
}

impl<'a, T> Iterator for VecxIterMem<'a, usize, T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.1.clone())
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
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let v = pnk!(serde_json::to_string(&CacheMeta {
            in_mem_cnt: self.in_mem_cnt,
            data_path: self.get_data_path(),
        }));

        self.flush_data();
        serializer.serialize_str(&v)
    }
}

impl<'de, T> serde::Deserialize<'de> for Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(CacheVisitor).map(|meta| {
            let meta = pnk!(serde_json::from_str::<CacheMeta>(&meta));
            pnk!(Vecx::new(meta.data_path, Some(meta.in_mem_cnt)))
        })
    }
}

/***************************************************************/
// End of the implementation of Serialize/Deserialize for Vecx //
/////////////////////////////////////////////////////////////////
