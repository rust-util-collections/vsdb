//!
//! # Disk Storage Implementation
//!

use crate::helper::*;
use rocksdb::{DBIterator, DBPinnableSlice, Direction, IteratorMode};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
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
    root_path: String,
    cnter_path: String,
    cnter: usize,
    prefix: Vec<u8>,
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
        meta_check(path).c(d!())?;
        let cnter_path = format!("{}/{}", path, CNTER);
        let prefix = read_prefix_bytes(&format!("{}/{}", path, PREFIX)).c(d!())?;
        let cnter = if BNC.prefix_iterator(&prefix).next().is_none() {
            fs::File::create(&cnter_path)
                .c(d!())
                .and_then(|_| write_db_len(&cnter_path, 0).c(d!()))
                .map(|_| 0)?
        } else {
            read_db_len(&cnter_path).c(d!())?
        };

        Ok(Mapx {
            root_path: path.to_owned(),
            cnter_path,
            cnter,
            prefix,
            _pd0: PhantomData,
            _pd1: PhantomData,
        })
    }

    // Get the storage path
    pub(super) fn get_root_path(&self) -> &str {
        self.root_path.as_str()
    }

    // Imitate the behavior of 'HashMap<_>.get(...)'
    #[inline(always)]
    pub(super) fn get(&self, key: &K) -> Option<V> {
        let mut k = self.prefix.clone();
        k.append(&mut pnk!(bincode::serialize(key)));
        BNC.get(k)
            .ok()
            .flatten()
            .map(|bytes| pnk!(serde_json::from_slice(&bytes)))
    }

    // Imitate the behavior of 'HashMap<_>.len()'.
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        debug_assert_eq!(pnk!(read_db_len(&self.cnter_path)), self.cnter);
        debug_assert_eq!(BNC.prefix_iterator(&self.prefix).count(), self.cnter);
        self.cnter
    }

    // A helper func
    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        BNC.prefix_iterator(&self.prefix).next().is_none()
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
        k.append(&mut pnk!(bincode::serialize(&key)));
        let v = pnk!(serde_json::to_vec(&value));
        let old_v = pnk!(BNC.get_pinned(&k));

        pnk!(BNC.put(k, v));

        if old_v.is_none() {
            self.cnter += 1;
            pnk!(write_db_len(&self.cnter_path, self.cnter));
        }

        old_v
    }

    // Imitate the behavior of '.iter()'
    #[inline(always)]
    pub(super) fn iter(&self) -> MapxIter<'_, K, V> {
        let i = BNC.prefix_iterator(&self.prefix);
        let mut i_rev = BNC.prefix_iterator(&self.prefix);
        i_rev.set_mode(IteratorMode::From(&self.prefix, Direction::Reverse));

        MapxIter {
            iter: i,
            iter_rev: i_rev,
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    pub(super) fn contains_key(&self, key: &K) -> bool {
        let mut k = self.prefix.clone();
        k.append(&mut pnk!(bincode::serialize(key)));
        pnk!(BNC.get_pinned(k)).is_some()
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        self.unset_value(key)
            .map(|v| pnk!(serde_json::from_slice(&v)))
    }

    pub(super) fn unset_value(&mut self, key: &K) -> Option<DBPinnableSlice> {
        let mut k = self.prefix.clone();
        k.append(&mut pnk!(bincode::serialize(&key)));
        let old_v = pnk!(BNC.get_pinned(&k));

        pnk!(BNC.delete(k));

        if old_v.is_some() {
            self.cnter -= 1;
            pnk!(write_db_len(&self.cnter_path, self.cnter));
        }

        old_v
    }

    /// Flush data to disk
    #[inline(always)]
    pub fn flush(&self) {
        // pnk!(BNC.flush());
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
