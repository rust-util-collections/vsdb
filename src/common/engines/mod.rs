//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

#[cfg(all(feature = "rocks_engine", not(feature = "sled_engine")))]
mod rocks_db;

#[cfg(all(feature = "sled_engine", not(feature = "rocks_engine")))]
mod sled_db;

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

#[cfg(all(feature = "rocks_engine", not(feature = "sled_engine")))]
pub(crate) use rocks_db::RocksEngine as RocksDB;

#[cfg(all(feature = "sled_engine", not(feature = "rocks_engine")))]
pub(crate) use sled_db::SledEngine as Sled;

#[cfg(all(feature = "sled_engine", not(feature = "rocks_engine")))]
pub type MapxIter = sled_db::SledIter;

#[cfg(all(feature = "rocks_engine", not(feature = "sled_engine")))]
pub type MapxIter = rocks_db::RocksIter;

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

use crate::common::{BranchID, InstanceCfg, Prefix, PrefixBytes, VersionID, VSDB};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

/// Low-level database interface.
pub trait Engine: Sized {
    fn new() -> Result<Self>;
    fn alloc_prefix(&self) -> Prefix;
    fn alloc_branch_id(&self) -> BranchID;
    fn alloc_version_id(&self) -> VersionID;
    fn area_count(&self) -> u8;
    fn flush(&self);

    fn iter(&self, area_idx: usize, meta_prefix: PrefixBytes) -> MapxIter;

    fn range<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        area_idx: usize,
        meta_prefix: PrefixBytes,
        bounds: R,
    ) -> MapxIter;

    fn get(
        &self,
        area_idx: usize,
        meta_prefix: PrefixBytes,
        key: &[u8],
    ) -> Option<Vec<u8>>;

    fn insert(
        &self,
        area_idx: usize,
        meta_prefix: PrefixBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<Vec<u8>>;

    fn remove(
        &self,
        area_idx: usize,
        meta_prefix: PrefixBytes,
        key: &[u8],
    ) -> Option<Vec<u8>>;
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

#[derive(Eq, Debug, Serialize, Deserialize)]
pub(crate) struct Mapx {
    item_cnt: u64,
    area_idx: usize,
    // the unique ID of each instance
    prefix: PrefixBytes,
}

impl Mapx {
    #[inline(always)]
    pub(crate) fn new() -> Self {
        let prefix = VSDB.db.alloc_prefix();

        // NOTE: this is NOT equal to
        // `prefix as usize % VSDB.area_count()`, the MAX value of
        // the type used by `len()` of almost all known OS-platforms
        // can be considered to be always less than Prefix::MAX(u64::MAX),
        // but the reverse logic can NOT be guaranteed.
        let area_idx = (prefix % VSDB.db.area_count() as Prefix) as usize;

        let prefix_bytes = prefix.to_be_bytes();

        assert!(VSDB.db.iter(area_idx, prefix_bytes).next().is_none());

        Mapx {
            item_cnt: 0,
            area_idx,
            prefix: prefix_bytes,
        }
    }

    pub(crate) fn get_instance_cfg(&self) -> InstanceCfg {
        InstanceCfg::from(self)
    }

    #[inline(always)]
    pub(crate) fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        VSDB.db.get(self.area_idx, self.prefix, key)
    }

    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.item_cnt as usize
    }

    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        0 == self.item_cnt
    }

    #[inline(always)]
    pub(crate) fn iter(&self) -> MapxIter {
        VSDB.db.iter(self.area_idx, self.prefix)
    }

    #[inline(always)]
    pub(crate) fn range<'a, R: RangeBounds<&'a [u8]>>(&'a self, bounds: R) -> MapxIter {
        VSDB.db.range(self.area_idx, self.prefix, bounds)
    }

    #[inline(always)]
    pub(crate) fn insert(&mut self, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
        let ret = VSDB.db.insert(self.area_idx, self.prefix, key, value);
        if ret.is_none() {
            self.item_cnt += 1;
        }
        ret
    }

    #[inline(always)]
    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let ret = VSDB.db.remove(self.area_idx, self.prefix, key);
        if ret.is_some() {
            self.item_cnt -= 1;
        }
        ret
    }

    #[inline(always)]
    pub(crate) fn clear(&mut self) {
        VSDB.db.iter(self.area_idx, self.prefix).for_each(|(k, _)| {
            VSDB.db.remove(self.area_idx, self.prefix, &k);
            self.item_cnt -= 1;
        });
    }
}

impl PartialEq for Mapx {
    fn eq(&self, other: &Mapx) -> bool {
        self.item_cnt == other.item_cnt
            && self
                .iter()
                .zip(other.iter())
                .all(|((k, v), (ko, vo))| k == ko && v == vo)
    }
}

impl From<InstanceCfg> for Mapx {
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            prefix: cfg.prefix,
            item_cnt: cfg.item_cnt,
            area_idx: cfg.area_idx,
        }
    }
}

impl From<&Mapx> for InstanceCfg {
    fn from(x: &Mapx) -> Self {
        Self {
            prefix: x.prefix,
            item_cnt: x.item_cnt,
            area_idx: x.area_idx,
        }
    }
}
