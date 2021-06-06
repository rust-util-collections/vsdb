//!
//! # Disk Storage Implementation
//!

use crate::{helper::*, DB_NUM};
use rocksdb::DBIterator;
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{convert::TryInto, fmt, iter::Iterator, marker::PhantomData, mem::size_of};

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
    path: String,
    cnter: usize,
    prefix: Vec<u8>,
    idx: usize,
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
        let prefix = read_prefix_bytes(&format!("{}/{}", path, PREFIX)).c(d!())?;
        let idx = hash(&path) % DB_NUM;

        Ok(Vecx {
            path: path.to_owned(),
            cnter: BNC[idx].prefix_iterator(&prefix).count(),
            prefix,
            idx,
            _pd: PhantomData,
        })
    }

    /// Get the storage path
    pub(super) fn get_path(&self) -> &str {
        self.path.as_str()
    }

    /// Imitate the behavior of 'Vec<_>.get(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub(super) fn get(&self, idx: usize) -> Option<T> {
        let mut k = self.prefix.clone();
        k.extend_from_slice(&usize::to_le_bytes(idx)[..]);
        BNC[self.idx]
            .get(k)
            .ok()
            .flatten()
            .map(|bytes| pnk!(serde_json::from_slice(&bytes)))
    }

    /// Imitate the behavior of 'Vec<_>.last()'
    #[inline(always)]
    pub(super) fn last(&self) -> Option<T> {
        self.get(self.len().saturating_sub(1))
    }

    /// Imitate the behavior of 'Vec<_>.len()'
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        debug_assert_eq!(
            BNC[self.idx].prefix_iterator(&self.prefix).count(),
            self.cnter
        );
        self.cnter
    }

    /// A helper func
    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        BNC[self.idx].prefix_iterator(&self.prefix).next().is_none()
    }

    /// Imitate the behavior of 'Vec<_>.push(...)'
    #[inline(always)]
    pub(super) fn push(&mut self, b: T) {
        let idx = self.cnter;

        let mut k = self.prefix.clone();
        k.extend_from_slice(&idx.to_le_bytes()[..]);
        let value = pnk!(serde_json::to_vec(&b));

        pnk!(BNC[self.idx].put(k, value));

        // There has no `remove`-like methods provided,
        // so we can increase this value directly.
        self.cnter += 1;
    }

    /// Imitate the behavior of 'Vec<_>.insert(idx, value)'
    #[inline(always)]
    pub(super) fn insert(&mut self, idx: usize, b: T) {
        let mut k = self.prefix.clone();
        k.extend_from_slice(&idx.to_le_bytes()[..]);
        let value = pnk!(serde_json::to_vec(&b));
        pnk!(BNC[self.idx].put(k, value));

        if idx >= self.cnter {
            // There has no `remove` like methods provided,
            // so we can increase this value directly.
            self.cnter += 1;
        }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub(super) fn iter(&self) -> VecxIter<'_, T> {
        let i = BNC[self.idx].prefix_iterator(&self.prefix);

        VecxIter {
            iter: i,
            _pd: PhantomData,
        }
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
