//!
//! # Disk Storage Implementation
//!

use crate::helper::*;
use rocksdb::{DBIterator, IteratorMode, DB};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    convert::TryInto, fmt, fs, iter::Iterator, marker::PhantomData, mem, sync::Arc,
};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `Vec<_>`.
///
/// - Each time the program is started, a new database is created
/// - Can ONLY be used in append-only scenes like the block storage
#[derive(Debug, Clone)]
pub(super) struct Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    db: Arc<DB>,
    data_path: String,
    cnter_path: String,
    cnter: usize,
    _pd: PhantomData<T>,
}

///////////////////////////////////////////////
// Begin of the self-implementation for Vecx //
/*********************************************/

impl<T> Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    /// If an old database exists,
    /// it will use it directly;
    /// Or it will create a new one.
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

        Ok(Vecx {
            db: Arc::new(db),
            data_path: path.to_owned(),
            cnter_path,
            cnter,
            _pd: PhantomData,
        })
    }

    /// Get the storage path
    pub(super) fn get_data_path(&self) -> &str {
        self.data_path.as_str()
    }

    /// Imitate the behavior of 'Vec<_>.get(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub(super) fn get(&self, idx: usize) -> Option<T> {
        self.db
            .get(&usize::to_le_bytes(idx)[..])
            .ok()
            .flatten()
            .map(|bytes| pnk!(serde_json::from_slice(&bytes)))
    }

    /// Imitate the behavior of 'Vec<_>.last()'
    pub(super) fn last(&self) -> Option<T> {
        self.db
            .iterator(IteratorMode::End)
            .next()
            .map(|(_, v)| pnk!(serde_json::from_slice(&v)))
    }

    /// Imitate the behavior of 'Vec<_>.len()'
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        debug_assert_eq!(pnk!(read_db_len(&self.cnter_path)), self.cnter);
        debug_assert_eq!(self.db.iterator(IteratorMode::Start).count(), self.cnter);
        self.cnter
    }

    /// A helper func
    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        self.db.iterator(IteratorMode::Start).next().is_none()
    }

    /// Imitate the behavior of 'Vec<_>.push(...)'
    #[inline(always)]
    pub(super) fn push(&mut self, b: T) {
        let idx = self.cnter;
        let value = pnk!(serde_json::to_vec(&b));
        pnk!(self.db.put(idx.to_le_bytes(), value));

        // There has no `remove`-like methods provided,
        // so we can increase this value directly.
        self.cnter += 1;

        pnk!(write_db_len(&self.cnter_path, self.cnter));
    }

    /// Imitate the behavior of 'Vec<_>.insert(idx, value)'
    #[inline(always)]
    pub(super) fn insert(&mut self, idx: usize, b: T) {
        let value = pnk!(serde_json::to_vec(&b));
        pnk!(self.db.put(idx.to_le_bytes(), value));

        if idx >= self.cnter {
            // There has no `remove` like methods provided,
            // so we can increase this value directly.
            self.cnter += 1;

            pnk!(write_db_len(&self.cnter_path, self.cnter));
        }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub(super) fn iter(&self) -> VecxIter<'_, T> {
        VecxIter {
            iter: self.db.iterator(IteratorMode::Start),
            iter_rev: self.db.iterator(IteratorMode::End),
            _pd: PhantomData,
        }
    }

    /// Flush data to disk
    #[inline(always)]
    pub fn flush(&self) {
        pnk!(self.db.flush());
    }
}

/*******************************************/
// End of the self-implementation for Vecx //
/////////////////////////////////////////////

//////////////////////////////////////////////////
// Begin of the implementation of Iter for Vecx //
/************************************************/

/// Iter over [Vecx](self::Vecx).
pub(super) struct VecxIter<'a, T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    pub(super) iter: DBIterator<'a>,
    pub(super) iter_rev: DBIterator<'a>,
    _pd: PhantomData<T>,
}

impl<'a, T> Iterator for VecxIter<'a, T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (usize, T);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(idx, v)| {
            (
                usize::from_le_bytes(idx[..mem::size_of::<usize>()].try_into().unwrap()),
                pnk!(serde_json::from_slice(&v)),
            )
        })
    }
}

impl<'a, T> DoubleEndedIterator for VecxIter<'a, T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter_rev.next().map(|(idx, v)| {
            (
                usize::from_le_bytes(idx[..mem::size_of::<usize>()].try_into().unwrap()),
                pnk!(serde_json::from_slice(&v)),
            )
        })
    }
}

impl<'a, T> ExactSizeIterator for VecxIter<'a, T> where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug
{
}

/**********************************************/
// End of the implementation of Iter for Vecx //
////////////////////////////////////////////////

////////////////////////////////////////////////
// Begin of the implementation of Eq for Vecx //
/**********************************************/

impl<T> PartialEq for Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &Vecx<T>) -> bool {
        !self.iter().zip(other.iter()).any(|(i, j)| i != j)
    }
}

impl<T> Eq for Vecx<T> where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug
{
}

/********************************************/
// End of the implementation of Eq for Vecx //
//////////////////////////////////////////////
