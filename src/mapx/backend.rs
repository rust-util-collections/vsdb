//!
//! # Disk Storage Implementation
//!

use crate::helper::*;
use rocksdb::{DBIterator, DBPinnableSlice, Direction, IteratorMode, DB};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt, fs,
    hash::Hash,
    iter::{DoubleEndedIterator, Iterator},
    marker::PhantomData,
    sync::Arc,
};

// To solve the problem of unlimited memory usage,
// use this to replace the original in-memory `HashMap<_, _>`.
#[derive(Debug, Clone)]
pub(super) struct Mapx<K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    db: Arc<DB>,
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
    pub(super) fn load_or_create(path: &str) -> Result<Self> {
        let db = rocksdb_open(path).c(d!())?;
        let cnter_path = format!("{}/____cnter____", path);

        let cnter = if db.iterator(IteratorMode::Start).next().is_none() {
            fs::File::create(&cnter_path)
                .c(d!())
                .and_then(|_| write_db_len(&cnter_path, 0).c(d!()))
                .map(|_| 0)?
        } else {
            read_db_len(&cnter_path).c(d!())?
        };

        Ok(Mapx {
            db: Arc::new(db),
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

    pub(super) fn last_at(&self, key: &K) -> Option<V> {
        let k = pnk!(bincode::serialize(key));
        self.db
            .iterator(IteratorMode::From(&k, Direction::Reverse))
            .next()
            .map(|(_, v)| pnk!(serde_json::from_slice(&v)))
    }

    // Imitate the behavior of 'HashMap<_>.len()'.
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        debug_assert_eq!(pnk!(read_db_len(&self.cnter_path)), self.cnter);
        debug_assert_eq!(self.db.iterator(IteratorMode::Start).count(), self.cnter);
        self.cnter
    }

    // A helper func
    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        self.db.iterator(IteratorMode::Start).next().is_none()
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
        let k = pnk!(bincode::serialize(&key));
        let v = pnk!(serde_json::to_vec(&value));
        let old_v = pnk!(self.db.get_pinned(&k));

        pnk!(self.db.put(k, v));

        if old_v.is_none() {
            self.cnter += 1;
            pnk!(write_db_len(&self.cnter_path, self.cnter));
        }

        old_v
    }

    // Imitate the behavior of '.iter()'
    #[inline(always)]
    pub(super) fn iter(&self) -> MapxIter<'_, K, V> {
        MapxIter {
            iter: self.db.iterator(IteratorMode::Start),
            iter_rev: self.db.iterator(IteratorMode::End),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    pub(super) fn contains_key(&self, key: &K) -> bool {
        pnk!(self.db.get_pinned(pnk!(bincode::serialize(key)))).is_some()
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        self.unset_value(key)
            .map(|v| pnk!(serde_json::from_slice(&v)))
    }

    pub(super) fn unset_value(&mut self, key: &K) -> Option<DBPinnableSlice> {
        let k = pnk!(bincode::serialize(&key));
        let old_v = pnk!(self.db.get_pinned(&k));

        pnk!(self.db.delete(k));

        if old_v.is_some() {
            self.cnter -= 1;
            pnk!(write_db_len(&self.cnter_path, self.cnter));
        }

        old_v
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
pub(super) struct MapxIter<'a, K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    pub(super) iter: DBIterator<'a>,
    pub(super) iter_rev: DBIterator<'a>,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

impl<'a, K, V> Iterator for MapxIter<'a, K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| {
            (
                pnk!(bincode::deserialize(&k)),
                pnk!(serde_json::from_slice(&v)),
            )
        })
    }
}

impl<'a, K, V> DoubleEndedIterator for MapxIter<'a, K, V>
where
    K: Clone + Eq + PartialEq + Hash + Serialize + DeserializeOwned + fmt::Debug,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter_rev.next().map(|(k, v)| {
            (
                pnk!(bincode::deserialize(&k)),
                pnk!(serde_json::from_slice(&v)),
            )
        })
    }
}

impl<'a, K, V> ExactSizeIterator for MapxIter<'a, K, V>
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
