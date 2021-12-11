//!
//! # Disk Storage Implementation
//!

use crate::{MetaInfo, TREE_NUM, VSDB};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt,
    iter::{DoubleEndedIterator, Iterator},
    marker::PhantomData,
    mem::size_of,
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
    cnter: usize,
    id: usize,
    idx: usize,
    _pd: PhantomData<T>,
}

impl<T> From<MetaInfo> for Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(mi: MetaInfo) -> Self {
        Self {
            cnter: mi.item_cnt,
            id: mi.obj_id,
            idx: mi.tree_idx,
            _pd: PhantomData,
        }
    }
}

impl<T> From<&Vecx<T>> for MetaInfo
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(x: &Vecx<T>) -> Self {
        Self {
            item_cnt: x.cnter,
            obj_id: x.id,
            tree_idx: x.idx,
        }
    }
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
    pub(super) fn load_or_create(id: usize) -> Self {
        let idx = id % TREE_NUM;
        Vecx {
            cnter: VSDB[idx].scan_prefix(id.to_be_bytes()).count(),
            id,
            idx,
            _pd: PhantomData,
        }
    }

    /// Get the storage path
    pub(super) fn get_meta(&self) -> MetaInfo {
        self.into()
    }

    /// Imitate the behavior of 'Vec<_>.get(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub(super) fn get(&self, idx: usize) -> Option<T> {
        let mut k = self.id.to_be_bytes().to_vec();
        k.extend_from_slice(&usize::to_be_bytes(idx)[..]);
        VSDB[self.idx]
            .get(k)
            .ok()
            .flatten()
            .map(|bytes| pnk!(bincode::deserialize(&bytes)))
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
            VSDB[self.idx].scan_prefix(self.id.to_be_bytes()).count(),
            self.cnter
        );
        self.cnter
    }

    /// A helper func
    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        VSDB[self.idx]
            .scan_prefix(self.id.to_be_bytes())
            .next()
            .is_none()
    }

    /// Imitate the behavior of 'Vec<_>.push(...)'
    #[inline(always)]
    pub(super) fn push(&mut self, b: T) {
        let idx = self.cnter;

        let mut k = self.id.to_be_bytes().to_vec();
        k.extend_from_slice(&idx.to_be_bytes()[..]);
        let value = pnk!(bincode::serialize(&b));

        pnk!(VSDB[self.idx].insert(k, value));

        // There has no `remove`-like methods provided,
        // so we can increase this value directly.
        self.cnter += 1;
    }

    /// Imitate the behavior of 'Vec<_>.insert(idx, value)'
    #[inline(always)]
    pub(super) fn insert(&mut self, idx: usize, b: T) {
        let mut k = self.id.to_be_bytes().to_vec();
        k.extend_from_slice(&idx.to_be_bytes()[..]);
        let value = pnk!(bincode::serialize(&b));
        pnk!(VSDB[self.idx].insert(k, value));

        if idx >= self.cnter {
            // There has no `remove` like methods provided,
            // so we can increase this value directly.
            self.cnter += 1;
        }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub(super) fn iter(&self) -> VecxIter<T> {
        let i = VSDB[self.idx].scan_prefix(self.id.to_be_bytes());

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
pub(super) struct VecxIter<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    pub(super) iter: sled::Iter,
    _pd: PhantomData<T>,
}

impl<T> Iterator for VecxIter<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (usize, T);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|i| i.unwrap()).map(|(idx, v)| {
            (
                usize::from_le_bytes(idx[..size_of::<usize>()].try_into().unwrap()),
                pnk!(bincode::deserialize(&v)),
            )
        })
    }
}

impl<T> ExactSizeIterator for VecxIter<T> where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug
{
}

impl<T> DoubleEndedIterator for VecxIter<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|i| i.unwrap()).map(|(idx, v)| {
            (
                usize::from_le_bytes(idx[..size_of::<usize>()].try_into().unwrap()),
                pnk!(bincode::deserialize(&v)),
            )
        })
    }
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
