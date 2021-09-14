//!
//! # Disk Storage Implementation
//!

use crate::helper::*;
use rocksdb::{DBIterator, Direction, IteratorMode};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    convert::TryInto, fmt, fs, iter::Iterator, marker::PhantomData, mem::size_of,
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
    root_path: String,
    cnter_path: String,
    cnter: usize,
    prefix: Vec<u8>,
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

        Ok(Vecx {
            root_path: path.to_owned(),
            cnter_path,
            cnter,
            prefix,
            _pd: PhantomData,
        })
    }

    /// Get the storage path
    pub(super) fn get_root_path(&self) -> &str {
        self.root_path.as_str()
    }

    /// Imitate the behavior of 'Vec<_>.get(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub(super) fn get(&self, idx: usize) -> Option<T> {
        let mut k = self.prefix.clone();
        k.extend_from_slice(&usize::to_le_bytes(idx)[..]);
        BNC.get(k)
            .ok()
            .flatten()
            .map(|bytes| pnk!(serde_json::from_slice(&bytes)))
    }

    /// Imitate the behavior of 'Vec<_>.last()'
    pub(super) fn last(&self) -> Option<(usize, T)> {
        // Method 1:
        let idx = self.len().saturating_sub(1);
        self.get(idx).map(|v| (idx, v))

        // // Method 2:
        // let mut i = BNC.prefix_iterator(&self.prefix);
        // i.set_mode(IteratorMode::From(&self.prefix, Direction::Reverse));
        // i.next().map(|(_, v)| pnk!(serde_json::from_slice(&v)))
    }

    /// Imitate the behavior of 'Vec<_>.len()'
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        debug_assert_eq!(pnk!(read_db_len(&self.cnter_path)), self.cnter);
        debug_assert_eq!(BNC.prefix_iterator(&self.prefix).count(), self.cnter);
        self.cnter
    }

    /// A helper func
    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        BNC.prefix_iterator(&self.prefix).next().is_none()
    }

    /// Imitate the behavior of 'Vec<_>.push(...)'
    #[inline(always)]
    pub(super) fn push(&mut self, b: T) {
        let idx = self.cnter;

        let mut k = self.prefix.clone();
        k.extend_from_slice(&idx.to_le_bytes()[..]);
        let value = pnk!(serde_json::to_vec(&b));

        pnk!(BNC.put(k, value));

        // There has no `remove`-like methods provided,
        // so we can increase this value directly.
        self.cnter += 1;

        pnk!(write_db_len(&self.cnter_path, self.cnter));
    }

    /// Imitate the behavior of 'Vec<_>.insert(idx, value)'
    #[inline(always)]
    pub(super) fn insert(&mut self, idx: usize, b: T) {
        let mut k = self.prefix.clone();
        k.extend_from_slice(&idx.to_le_bytes()[..]);
        let value = pnk!(serde_json::to_vec(&b));
        pnk!(BNC.put(k, value));

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
        let i = BNC.prefix_iterator(&self.prefix);
        let mut i_rev = BNC.prefix_iterator(&self.prefix);
        i_rev.set_mode(IteratorMode::From(&self.prefix, Direction::Reverse));

        VecxIter {
            iter: i,
            iter_rev: i_rev,
            _pd: PhantomData,
        }
    }

    /// Flush data to disk
    #[inline(always)]
    pub fn flush(&self) {
        pnk!(BNC.flush());
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
                usize::from_le_bytes(idx[..size_of::<usize>()].try_into().unwrap()),
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
                usize::from_le_bytes(idx[..size_of::<usize>()].try_into().unwrap()),
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
