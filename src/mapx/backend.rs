//!
//! # Disk Storage Implementation
//!

use crate::helper::*;
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use sled::IVec;
use std::{
    fmt, fs,
    hash::Hash,
    iter::{DoubleEndedIterator, Iterator},
    marker::PhantomData,
};

// To solve the problem of unlimited memory usage,
// use this to replace the original in-memory `HashMap<_, _>`.
#[derive(Debug, Clone)]
pub(super) struct Mapx<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    db: sled::Db,
    data_path: String,
    cnter_path: String,
    cnter: usize,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

///////////////////////////////////////////////////////
// Begin of the self-implementation of backend::Mapx //
/*****************************************************/

impl<K, V> Mapx<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    // If an old database exists,
    // it will use it directly;
    // Or it will create a new one.
    #[inline(always)]
    pub(super) fn load_or_create(path: &str, is_tmp: bool) -> Result<Self> {
        let db = sled_open(path, is_tmp).c(d!())?;
        let cnter_path = format!("{}/____cnter____", path);

        let cnter = if db.iter().next().is_none() {
            fs::File::create(&cnter_path)
                .c(d!())
                .and_then(|_| write_db_len(&cnter_path, 0).c(d!()))
                .map(|_| 0)?
        } else {
            read_db_len(&cnter_path).c(d!())?
        };

        Ok(Mapx {
            db,
            data_path: path.to_owned(),
            cnter_path,
            cnter,
            _pd0: PhantomData,
            _pd1: PhantomData,
        })
    }

    // Get the storage path
    pub(super) fn get_data_path(&self) -> &str {
        self.data_path.as_str()
    }

    // Imitate the behavior of 'HashMap<_>.get(...)'
    #[inline(always)]
    pub(super) fn get(&self, key: &K) -> Option<V> {
        self.db
            .get(&pnk!(bincode::serialize(key)))
            .ok()
            .flatten()
            .map(|bytes| pnk!(serde_json::from_slice(&bytes)))
    }

    // Imitate the behavior of 'HashMap<_>.len()'.
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        debug_assert_eq!(pnk!(read_db_len(&self.cnter_path)), self.cnter);
        debug_assert_eq!(self.db.len(), self.cnter);
        self.cnter
    }

    // A helper func
    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    // Imitate the behavior of 'HashMap<_>.insert(...)'.
    #[inline(always)]
    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.set_value(key, value)
            .map(|v| pnk!(serde_json::from_slice(&v)))
    }

    // Similar with `insert`, but ignore if the old value is exist.
    #[inline(always)]
    pub(super) fn set_value(&mut self, key: K, value: V) -> Option<IVec> {
        pnk!(self
            .db
            .insert(
                pnk!(bincode::serialize(&key)),
                pnk!(serde_json::to_vec(&value))
            )
            .map(|v| {
                if v.is_none() {
                    self.cnter += 1;
                    pnk!(write_db_len(&self.cnter_path, self.cnter));
                }
                v
            }))
    }

    // Imitate the behavior of '.iter()'
    #[inline(always)]
    pub(super) fn iter(&self) -> MapxIter<K, V> {
        MapxIter {
            iter: self.db.iter(),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    pub(super) fn contains_key(&self, key: &K) -> bool {
        pnk!(self.db.contains_key(pnk!(bincode::serialize(key))))
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        self.unset_value(key)
            .map(|v| pnk!(serde_json::from_slice(&v)))
    }

    pub(super) fn unset_value(&mut self, key: &K) -> Option<IVec> {
        pnk!(self.db.remove(pnk!(bincode::serialize(&key))).map(|v| {
            if v.is_some() {
                self.cnter -= 1;
                pnk!(write_db_len(&self.cnter_path, self.cnter));
            }
            v
        }))
    }

    /// Flush data to disk
    #[inline(always)]
    pub fn flush(&self) {
        pnk!(self.db.flush());
    }
}

/***************************************************/
// End of the self-implementation of backend::Mapx //
/////////////////////////////////////////////////////

///////////////////////////////////////////////////////////
// Begin of the implementation of Iter for backend::Mapx //
/*********************************************************/

// Iter over [Mapx](self::Mapx).
pub(super) struct MapxIter<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    pub(super) iter: sled::Iter,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

impl<K, V> Iterator for MapxIter<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.ok()).flatten().map(|(k, v)| {
            (
                pnk!(bincode::deserialize(&k)),
                pnk!(serde_json::from_slice(&v)),
            )
        })
    }
}

impl<K, V> DoubleEndedIterator for MapxIter<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|v| v.ok())
            .flatten()
            .map(|(k, v)| {
                (
                    pnk!(bincode::deserialize(&k)),
                    pnk!(serde_json::from_slice(&v)),
                )
            })
    }
}

impl<K, V> ExactSizeIterator for MapxIter<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
}

/*******************************************************/
// End of the implementation of Iter for backend::Mapx //
/////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////
// Begin of the implementation of Eq for backend::Mapx //
/*******************************************************/

impl<K, V> PartialEq for Mapx<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &Mapx<K, V>) -> bool {
        !self.iter().zip(other.iter()).any(|(i, j)| i != j)
    }
}

impl<K, V> Eq for Mapx<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
}

/*****************************************************/
// End of the implementation of Eq for backend::Mapx //
///////////////////////////////////////////////////////
