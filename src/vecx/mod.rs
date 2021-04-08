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
    collections::{btree_map, BTreeMap},
    fmt,
    iter::Iterator,
};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `Vec<_>`.
///
/// - Each time the program is started, a new database is created
/// - Can ONLY be used in append-only scenes like the block storage
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct Vecx<T>
where
    T: Eq + PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
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
    T: Eq + PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new(path: String, imc: Option<usize>, is_tmp: bool) -> Result<Self> {
        let in_disk = backend::Vecx::load_or_create(path, is_tmp).c(d!())?;
        let mut in_mem = BTreeMap::new();

        if !in_disk.is_empty() {
            let mut lefter = IN_MEM_CNT;
            let mut data = in_disk.iter().rev();
            while lefter > 0 {
                if let Some((idx, v)) = data.next() {
                    in_mem.insert(idx, v);
                } else {
                    break;
                }
                lefter -= 1;
            }
        }

        Ok(Vecx {
            in_mem,
            in_mem_cnt: imc.unwrap_or(IN_MEM_CNT),
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

//////////////////////////////////////////////////
// Begin of the implementation of Iter for Vecx //
/************************************************/

/// Iter over [Vecx](self::Vecx).
pub struct VecxIter<T>
where
    T: Eq + PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    iter: backend::VecxIter<T>,
}

impl<T> Iterator for VecxIter<T>
where
    T: Eq + PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.1)
    }
}

/// Iter over [Vecx](self::Vecx).
pub struct VecxIterMem<'a, K, T>
where
    K: 'a,
    T: 'a + Eq + PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    iter: btree_map::Iter<'a, K, T>,
}

impl<'a, T> Iterator for VecxIterMem<'a, usize, T>
where
    T: Eq + PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
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

impl<T> serde::Serialize for Vecx<T>
where
    T: Eq + PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
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
    T: Eq + PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(CacheVisitor).map(|meta| {
            let meta = pnk!(serde_json::from_str::<CacheMeta>(&meta));
            pnk!(Vecx::new(
                meta.data_path.to_owned(),
                Some(meta.in_mem_cnt),
                false
            ))
        })
    }
}

/***************************************************************/
// End of the implementation of Serialize/Deserialize for Vecx //
/////////////////////////////////////////////////////////////////
