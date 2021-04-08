//!
//! # Disk Storage Implementation
//!

use crate::helper::*;
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{convert::TryInto, fs, iter::Iterator, marker::PhantomData, mem};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `Vec<_>`.
///
/// - Each time the program is started, a new database is created
/// - Can ONLY be used in append-only scenes like the block storage
#[derive(Debug, Clone)]
pub(super) struct Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + std::fmt::Debug,
{
    db: sled::Db,
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
    T: PartialEq + Clone + Serialize + DeserializeOwned + std::fmt::Debug,
{
    /// If an old database exists,
    /// it will use it directly;
    /// Or it will create a new one.
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

        Ok(Vecx {
            db,
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
        pnk!(self.db.last()).map(|(_, v)| pnk!(serde_json::from_slice(&v)))
    }

    /// Imitate the behavior of 'Vec<_>.len()'
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        debug_assert_eq!(self.db.len(), self.cnter);
        debug_assert_eq!(pnk!(read_db_len(&self.cnter_path)), self.cnter);
        self.cnter
    }

    /// A helper func
    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    /// Imitate the behavior of 'Vec<_>.push(...)'
    #[inline(always)]
    pub(super) fn push(&mut self, b: T) {
        let idx = self.cnter;
        let value = pnk!(serde_json::to_vec(&b));
        pnk!(self.db.insert(idx.to_le_bytes(), value));

        // There is no `remove` like methods provided,
        // so we can increase this value directly.
        self.cnter += 1;

        pnk!(write_db_len(&self.cnter_path, self.cnter));
    }

    /// Imitate the behavior of 'Vec<_>.insert(idx, value)'
    #[inline(always)]
    pub(super) fn insert(&mut self, idx: usize, b: T) {
        let value = pnk!(serde_json::to_vec(&b));
        pnk!(self.db.insert(idx.to_le_bytes(), value));

        if idx >= self.cnter {
            // There is no `remove` like methods provided,
            // so we can increase this value directly.
            self.cnter += 1;

            pnk!(write_db_len(&self.cnter_path, self.cnter));
        }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub(super) fn iter(&self) -> VecxIter<T> {
        VecxIter {
            iter: self.db.iter(),
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
pub(super) struct VecxIter<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + std::fmt::Debug,
{
    pub(super) iter: sled::Iter,
    _pd: PhantomData<T>,
}

impl<T> Iterator for VecxIter<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + std::fmt::Debug,
{
    type Item = (usize, T);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.ok()).flatten().map(|(idx, v)| {
            (
                usize::from_le_bytes(idx[..mem::size_of::<usize>()].try_into().unwrap()),
                pnk!(serde_json::from_slice(&v)),
            )
        })
    }
}

impl<T> DoubleEndedIterator for VecxIter<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + std::fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|v| v.ok())
            .flatten()
            .map(|(idx, v)| {
                (
                    usize::from_le_bytes(
                        idx[..mem::size_of::<usize>()].try_into().unwrap(),
                    ),
                    pnk!(serde_json::from_slice(&v)),
                )
            })
    }
}

impl<T> ExactSizeIterator for VecxIter<T> where
    T: PartialEq + Clone + Serialize + DeserializeOwned + std::fmt::Debug
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
    T: PartialEq + Clone + Serialize + DeserializeOwned + std::fmt::Debug,
{
    fn eq(&self, other: &Vecx<T>) -> bool {
        !self.iter().zip(other.iter()).any(|(i, j)| i != j)
    }
}

impl<T> Eq for Vecx<T> where
    T: PartialEq + Clone + Serialize + DeserializeOwned + std::fmt::Debug
{
}

/********************************************/
// End of the implementation of Eq for Vecx //
//////////////////////////////////////////////
