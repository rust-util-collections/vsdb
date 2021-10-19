//!
//! # Disk Storage Implementation
//!

use super::NumKey;
use crate::{helper::*, DB_NUM};
use rocksdb::{DBIterator, DBPinnableSlice, Direction, IteratorMode};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt, iter::Iterator, marker::PhantomData};

// To solve the problem of unlimited memory usage,
// use this to replace the original in-memory `HashMap<_, _>`.
#[derive(Debug, Clone)]
pub(super) struct Mapxnk<K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    path: String,
    cnter: usize,
    prefix: Vec<u8>,
    idx: usize,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

///////////////////////////////////////////////////////
// Begin of the self-implementation of backend::Mapxnk //
/*****************************************************/

impl<K, V> Mapxnk<K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    // If an old database exists,
    // it will use it directly;
    // Or it will create a new one.
    #[inline(always)]
    pub(super) fn load_or_create(path: &str) -> Result<Self> {
        meta_check(path).c(d!())?;
        let prefix = read_prefix_bytes(&format!("{}/{}", path, PREFIX)).c(d!())?;
        let idx = hash(&path) % DB_NUM;

        Ok(Mapxnk {
            path: path.to_owned(),
            cnter: BNC[idx].prefix_iterator(&prefix).count(),
            prefix,
            idx,
            _pd0: PhantomData,
            _pd1: PhantomData,
        })
    }

    // Get the storage path
    pub(super) fn get_path(&self) -> &str {
        self.path.as_str()
    }

    // Imitate the behavior of 'HashMap<_>.get(...)'
    #[inline(always)]
    pub(super) fn get(&self, key: &K) -> Option<V> {
        let mut k = self.prefix.clone();
        k.append(&mut key.to_bytes());
        BNC[self.idx]
            .get(k)
            .ok()
            .flatten()
            .map(|bytes| pnk!(serde_json::from_slice(&bytes)))
    }

    #[inline(always)]
    pub(super) fn get_closest_smaller(&self, key: &K) -> Option<(K, V)> {
        self.get_closest(key, Direction::Reverse)
    }

    #[inline(always)]
    pub(super) fn get_closest_larger(&self, key: &K) -> Option<(K, V)> {
        self.get_closest(key, Direction::Forward)
    }

    #[inline(always)]
    fn get_closest(&self, key: &K, direction: Direction) -> Option<(K, V)> {
        let mut k = self.prefix.clone();
        k.append(&mut key.to_bytes());
        BNC[self.idx]
            .iterator(IteratorMode::From(&k, direction))
            .next()
            .map(|(k, v)| {
                (
                    pnk!(K::from_bytes(&k[self.prefix.len()..])),
                    pnk!(serde_json::from_slice(&v)),
                )
            })
    }

    // Imitate the behavior of 'HashMap<_>.len()'.
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        debug_assert_eq!(
            BNC[self.idx].prefix_iterator(&self.prefix).count(),
            self.cnter
        );
        self.cnter
    }

    // A helper func
    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        BNC[self.idx].prefix_iterator(&self.prefix).next().is_none()
    }

    // Imitate the behavior of 'HashMap<_>.insert(...)'.
    #[inline(always)]
    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.set_value(key, value)
            .map(|v| pnk!(serde_json::from_slice(&v)))
    }

    // Similar with `insert`, but ignore if the old value is exist.
    #[inline(always)]
    pub(super) fn set_value(&mut self, key: K, value: V) -> Option<DBPinnableSlice> {
        let mut k = self.prefix.clone();
        k.append(&mut key.to_bytes());
        let v = pnk!(serde_json::to_vec(&value));
        let old_v = pnk!(BNC[self.idx].get_pinned(&k));

        pnk!(BNC[self.idx].put(k, v));

        if old_v.is_none() {
            self.cnter += 1;
        }

        old_v
    }

    // Imitate the behavior of '.iter()'
    #[inline(always)]
    pub(super) fn iter(&self) -> MapxnkIter<'_, K, V> {
        let i = BNC[self.idx].prefix_iterator(&self.prefix);

        MapxnkIter {
            iter: i,
            hdr: self,
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    pub(super) fn contains_key(&self, key: &K) -> bool {
        let mut k = self.prefix.clone();
        k.append(&mut key.to_bytes());
        pnk!(BNC[self.idx].get_pinned(k)).is_some()
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        self.unset_value(key)
            .map(|v| pnk!(serde_json::from_slice(&v)))
    }

    pub(super) fn unset_value(&mut self, key: &K) -> Option<DBPinnableSlice> {
        let mut k = self.prefix.clone();
        k.append(&mut key.to_bytes());
        let old_v = pnk!(BNC[self.idx].get_pinned(&k));

        pnk!(BNC[self.idx].delete(k));

        if old_v.is_some() {
            self.cnter -= 1;
        }

        old_v
    }
}

/***************************************************/
// End of the self-implementation of backend::Mapxnk //
/////////////////////////////////////////////////////

///////////////////////////////////////////////////////////
// Begin of the implementation of Iter for backend::Mapxnk //
/*********************************************************/

// Iter over [Mapxnk](self::Mapxnk).
pub(super) struct MapxnkIter<'a, K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    pub(super) iter: DBIterator<'a>,
    hdr: &'a Mapxnk<K, V>,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

impl<'a, K, V> Iterator for MapxnkIter<'a, K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| {
            (
                pnk!(K::from_bytes(&k[self.hdr.prefix.len()..])),
                pnk!(serde_json::from_slice(&v)),
            )
        })
    }
}

impl<'a, K, V> ExactSizeIterator for MapxnkIter<'a, K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
}

/*******************************************************/
// End of the implementation of Iter for backend::Mapxnk //
/////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////
// Begin of the implementation of Eq for backend::Mapxnk //
/*******************************************************/

impl<K, V> PartialEq for Mapxnk<K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &Mapxnk<K, V>) -> bool {
        !self.iter().zip(other.iter()).any(|(i, j)| i != j)
    }
}

impl<K, V> Eq for Mapxnk<K, V>
where
    K: NumKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
}

/*****************************************************/
// End of the implementation of Eq for backend::Mapxnk //
///////////////////////////////////////////////////////
