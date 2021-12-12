//!
//! # Disk Storage Implementation
//!

use crate::common::{MetaInfo, PREFIX_ID_SIZ, VSDB};
use ruc::*;
use sled::{IVec, Iter};
use std::{
    iter::{DoubleEndedIterator, Iterator},
    ops::{Bound, RangeBounds},
};

// To solve the problem of unlimited memory usage,
// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct MapxRaw {
    cnter: u64,
    id: u64,
    idx: usize,
}

impl From<MetaInfo> for MapxRaw {
    fn from(mi: MetaInfo) -> Self {
        Self {
            cnter: mi.item_cnt,
            id: mi.obj_id,
            idx: mi.tree_idx,
        }
    }
}

impl From<&MapxRaw> for MetaInfo {
    fn from(x: &MapxRaw) -> Self {
        Self {
            item_cnt: x.cnter,
            obj_id: x.id,
            tree_idx: x.idx,
        }
    }
}

///////////////////////////////////////////////////////
// Begin of the self-implementation of backend::MapxRaw //
/*****************************************************/

impl MapxRaw {
    // create a new instance
    #[inline(always)]
    pub(super) fn must_new(id: u64) -> Self {
        let idx = id as usize % VSDB.trees.len();

        assert!(VSDB.trees[idx]
            .scan_prefix(id.to_be_bytes())
            .next()
            .is_none());

        MapxRaw { cnter: 0, id, idx }
    }

    // Get the storage path
    pub(super) fn get_meta(&self) -> MetaInfo {
        MetaInfo::from(self)
    }

    // Imitate the behavior of 'BTreeMap<_>.get(...)'
    #[inline(always)]
    pub(super) fn get(&self, key: &[u8]) -> Option<IVec> {
        let mut k = self.id.to_be_bytes().to_vec();
        k.extend_from_slice(key);
        VSDB.trees[self.idx].get(k).unwrap()
    }

    // less or equal
    #[inline(always)]
    pub(super) fn get_le(&self, key: &[u8]) -> Option<(IVec, IVec)> {
        let mut k = self.id.to_be_bytes().to_vec();
        k.extend_from_slice(key);

        VSDB.trees[self.idx]
            .range(..=k)
            .next_back()
            .map(|i| i.unwrap())
            .map(|(k, v)| (k.subslice(PREFIX_ID_SIZ, k.len() - PREFIX_ID_SIZ), v))
    }

    // ge: great or equal
    #[inline(always)]
    pub(super) fn get_ge(&self, key: &[u8]) -> Option<(IVec, IVec)> {
        let mut k = self.id.to_be_bytes().to_vec();
        k.extend_from_slice(key);

        VSDB.trees[self.idx]
            .range(k..)
            .next()
            .map(|i| i.unwrap())
            .map(|(k, v)| (k.subslice(PREFIX_ID_SIZ, k.len() - PREFIX_ID_SIZ), v))
    }

    // Imitate the behavior of 'BTreeMap<_>.len()'.
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        debug_assert_eq!(
            VSDB.trees[self.idx]
                .scan_prefix(self.id.to_be_bytes())
                .count(),
            self.cnter as usize
        );
        self.cnter as usize
    }

    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        0 == self.cnter
    }

    // Imitate the behavior of 'BTreeMap<_>.insert(...)'.
    #[inline(always)]
    pub(super) fn insert(&mut self, key: &[u8], value: &[u8]) -> Option<IVec> {
        let mut k = self.id.to_be_bytes().to_vec();
        k.extend_from_slice(key);

        match VSDB.trees[self.idx].insert(k, value).unwrap() {
            None => {
                self.cnter += 1;
                None
            }
            old_v => old_v,
        }
    }

    // Imitate the behavior of '.iter()'
    #[inline(always)]
    pub(super) fn iter(&self) -> MapxRawIter {
        let i = VSDB.trees[self.idx].scan_prefix(self.id.to_be_bytes());
        MapxRawIter { iter: i }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range<'a, R: RangeBounds<&'a [u8]>>(&'a self, bounds: R) -> MapxRawIter {
        let mut b_lo = self.id.to_be_bytes().to_vec();
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                b_lo.extend_from_slice(lo);
                Bound::Included(b_lo)
            }
            Bound::Excluded(lo) => {
                b_lo.extend_from_slice(lo);
                Bound::Excluded(b_lo)
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let mut b_hi = self.id.to_be_bytes().to_vec();
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.extend_from_slice(hi);
                Bound::Included(b_hi)
            }
            Bound::Excluded(hi) => {
                b_hi.extend_from_slice(hi);
                Bound::Excluded(b_hi)
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        MapxRawIter {
            iter: VSDB.trees[self.idx].range((l, h)),
        }
    }

    pub(super) fn contains_key(&self, key: &[u8]) -> bool {
        let mut k = self.id.to_be_bytes().to_vec();
        k.extend_from_slice(key);
        pnk!(VSDB.trees[self.idx].contains_key(k))
    }

    pub(super) fn remove(&mut self, key: &[u8]) -> Option<IVec> {
        let mut k = self.id.to_be_bytes().to_vec();
        k.extend_from_slice(key);

        match VSDB.trees[self.idx].remove(k).unwrap() {
            None => None,
            old_v => {
                self.cnter -= 1;
                old_v
            }
        }
    }

    pub(super) fn clear(&mut self) {
        VSDB.trees[self.idx]
            .scan_prefix(self.id.to_be_bytes())
            .keys()
            .map(|k| k.unwrap())
            .for_each(|k| {
                VSDB.trees[self.idx].remove(k).unwrap();
                self.cnter -= 1;
            });
    }
}

/***************************************************/
// End of the self-implementation of backend::MapxRaw //
/////////////////////////////////////////////////////

///////////////////////////////////////////////////////////
// Begin of the implementation of Iter for backend::MapxRaw //
/*********************************************************/

// Iter over [MapxRaw](self::Mapxnk).
pub(super) struct MapxRawIter {
    pub(super) iter: Iter,
}

impl Iterator for MapxRawIter {
    type Item = (IVec, IVec);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|i| i.unwrap())
            .map(|(k, v)| (k.subslice(PREFIX_ID_SIZ, k.len() - PREFIX_ID_SIZ), v))
    }
}

impl DoubleEndedIterator for MapxRawIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|i| i.unwrap())
            .map(|(k, v)| (k.subslice(PREFIX_ID_SIZ, k.len() - PREFIX_ID_SIZ), v))
    }
}

impl ExactSizeIterator for MapxRawIter {}
