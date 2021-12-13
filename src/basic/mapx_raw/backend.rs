//!
//! # Disk Storage Implementation
//!

use crate::common::{InstanceCfg, Prefix, PREFIX_SIZ, VSDB};
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
    // the unique ID of each instance
    prefix: Vec<u8>,
    idx: usize,
}

impl From<InstanceCfg> for MapxRaw {
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            cnter: cfg.item_cnt,
            prefix: cfg.prefix,
            idx: cfg.data_set_idx,
        }
    }
}

impl From<&MapxRaw> for InstanceCfg {
    fn from(x: &MapxRaw) -> Self {
        Self {
            item_cnt: x.cnter,
            prefix: x.prefix.clone(),
            data_set_idx: x.idx,
        }
    }
}

///////////////////////////////////////////////////////
// Begin of the self-implementation of backend::MapxRaw //
/*****************************************************/

impl MapxRaw {
    // create a new instance
    #[inline(always)]
    pub(super) fn must_new(prefix: Prefix) -> Self {
        // NOTE: this is NOT equal to
        // `prefix as usize % VSDB.data_set.len()`, the MAX value of
        // the type used by `len()` of almost all known OS-platforms
        // can be considered to be always less than Prefix::MAX(u64::MAX),
        // but the reverse logic can NOT be guaranteed.
        let idx = (prefix % VSDB.data_set.len() as Prefix) as usize;

        let prefix = prefix.to_be_bytes().to_vec();

        assert!(VSDB.data_set[idx]
            .scan_prefix(prefix.as_slice())
            .next()
            .is_none());

        MapxRaw {
            cnter: 0,
            prefix,
            idx,
        }
    }

    // Get the storage path
    pub(super) fn get_instance_cfg(&self) -> InstanceCfg {
        InstanceCfg::from(self)
    }

    // Imitate the behavior of 'BTreeMap<_>.get(...)'
    #[inline(always)]
    pub(super) fn get(&self, key: &[u8]) -> Option<IVec> {
        let mut k = self.prefix.clone();
        k.extend_from_slice(key);
        VSDB.data_set[self.idx].get(k).unwrap()
    }

    // less or equal
    #[inline(always)]
    pub(super) fn get_le(&self, key: &[u8]) -> Option<(IVec, IVec)> {
        let mut k = self.prefix.clone();
        k.extend_from_slice(key);

        VSDB.data_set[self.idx]
            .range(..=k)
            .next_back()
            .map(|i| i.unwrap())
            .map(|(k, v)| (k.subslice(PREFIX_SIZ, k.len() - PREFIX_SIZ), v))
    }

    // ge: great or equal
    #[inline(always)]
    pub(super) fn get_ge(&self, key: &[u8]) -> Option<(IVec, IVec)> {
        let mut k = self.prefix.clone();
        k.extend_from_slice(key);

        VSDB.data_set[self.idx]
            .range(k..)
            .next()
            .map(|i| i.unwrap())
            .map(|(k, v)| (k.subslice(PREFIX_SIZ, k.len() - PREFIX_SIZ), v))
    }

    // Imitate the behavior of 'BTreeMap<_>.len()'.
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        debug_assert_eq!(
            VSDB.data_set[self.idx]
                .scan_prefix(self.prefix.as_slice())
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
        let mut k = self.prefix.clone();
        k.extend_from_slice(key);

        match VSDB.data_set[self.idx].insert(k, value).unwrap() {
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
        let i = VSDB.data_set[self.idx].scan_prefix(self.prefix.as_slice());
        MapxRawIter { iter: i }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range<'a, R: RangeBounds<&'a [u8]>>(&'a self, bounds: R) -> MapxRawIter {
        let mut b_lo = self.prefix.clone();
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

        let mut b_hi = self.prefix.clone();
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
            iter: VSDB.data_set[self.idx].range((l, h)),
        }
    }

    pub(super) fn contains_key(&self, key: &[u8]) -> bool {
        let mut k = self.prefix.clone();
        k.extend_from_slice(key);
        pnk!(VSDB.data_set[self.idx].contains_key(k))
    }

    pub(super) fn remove(&mut self, key: &[u8]) -> Option<IVec> {
        let mut k = self.prefix.clone();
        k.extend_from_slice(key);

        match VSDB.data_set[self.idx].remove(k).unwrap() {
            None => None,
            old_v => {
                self.cnter -= 1;
                old_v
            }
        }
    }

    pub(super) fn clear(&mut self) {
        VSDB.data_set[self.idx]
            .scan_prefix(self.prefix.as_slice())
            .keys()
            .map(|k| k.unwrap())
            .for_each(|k| {
                VSDB.data_set[self.idx].remove(k).unwrap();
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
            .map(|(k, v)| (k.subslice(PREFIX_SIZ, k.len() - PREFIX_SIZ), v))
    }
}

impl DoubleEndedIterator for MapxRawIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|i| i.unwrap())
            .map(|(k, v)| (k.subslice(PREFIX_SIZ, k.len() - PREFIX_SIZ), v))
    }
}

impl ExactSizeIterator for MapxRawIter {}
