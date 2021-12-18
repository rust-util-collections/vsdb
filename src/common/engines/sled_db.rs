use crate::common::{
    get_data_dir, vsdb_set_base_dir, BranchID, Engine, Prefix, PrefixBytes, VersionID,
    PREFIX_SIZ, RESERVED_ID_CNT,
};
use ruc::*;
use sled::{Config, Db, IVec, Iter, Mode, Tree};
use std::ops::{Bound, RangeBounds};

const DATA_SET_NUM: u8 = 8;

const META_KEY_BRANCH_ID: [u8; 1] = [u8::MAX - 1];
const META_KEY_VERSION_ID: [u8; 1] = [u8::MAX - 2];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

pub(crate) struct SledEngine {
    meta: Db,
    areas: Vec<Tree>,
    prefix_allocator: PrefixAllocator,
}

impl Engine for SledEngine {
    fn new() -> Result<Self> {
        let meta = sled_open().c(d!())?;

        let areas = (0..DATA_SET_NUM)
            .map(|idx| meta.open_tree(idx.to_be_bytes()).c(d!()))
            .collect::<Result<Vec<_>>>()?;

        let (prefix_allocator, initial_value) = PrefixAllocator::init();

        if meta.get(&META_KEY_BRANCH_ID).c(d!())?.is_none() {
            meta.insert(META_KEY_BRANCH_ID, 0_usize.to_be_bytes())
                .c(d!())?;
        }

        if meta.get(&META_KEY_VERSION_ID).c(d!())?.is_none() {
            meta.insert(META_KEY_VERSION_ID, 0_usize.to_be_bytes())
                .c(d!())?;
        }

        if meta.get(prefix_allocator.key).c(d!())?.is_none() {
            meta.insert(prefix_allocator.key, initial_value).c(d!())?;
        }

        Ok(SledEngine {
            meta,
            areas,
            prefix_allocator,
        })
    }

    fn alloc_prefix(&self) -> Prefix {
        crate::parse_prefix!(
            self.meta
                .update_and_fetch(self.prefix_allocator.key, PrefixAllocator::next)
                .unwrap()
                .unwrap()
                .as_ref()
        )
    }

    fn alloc_branch_id(&self) -> BranchID {
        let ret = crate::parse_int!(
            self.meta.get(META_KEY_BRANCH_ID).unwrap().unwrap().to_vec(),
            BranchID
        );
        self.meta
            .insert(META_KEY_BRANCH_ID, (1 + ret).to_be_bytes())
            .unwrap();
        ret
    }

    fn alloc_version_id(&self) -> VersionID {
        let ret = crate::parse_int!(
            self.meta
                .get(META_KEY_VERSION_ID)
                .unwrap()
                .unwrap()
                .to_vec(),
            VersionID
        );
        self.meta
            .insert(META_KEY_VERSION_ID, (1 + ret).to_be_bytes())
            .unwrap();
        ret
    }

    fn area_count(&self) -> u8 {
        self.areas.len() as u8
    }

    fn flush(&self) {
        (0..self.areas.len()).for_each(|i| {
            self.areas[i].flush().unwrap();
        });
    }

    fn iter(&self, area_idx: usize, meta_prefix: PrefixBytes) -> SledIter {
        SledIter {
            inner: self.areas[area_idx].scan_prefix(meta_prefix.as_slice()),
            bounds: (Bound::Unbounded, Bound::Unbounded),
        }
    }

    fn range<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        area_idx: usize,
        meta_prefix: PrefixBytes,
        bounds: R,
    ) -> SledIter {
        let mut b_lo = meta_prefix.to_vec();
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                b_lo.extend_from_slice(lo);
                Bound::Included(IVec::from(b_lo))
            }
            Bound::Excluded(lo) => {
                b_lo.extend_from_slice(lo);
                Bound::Excluded(IVec::from(b_lo))
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let mut b_hi = meta_prefix.to_vec();
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.extend_from_slice(hi);
                Bound::Included(IVec::from(b_hi))
            }
            Bound::Excluded(hi) => {
                b_hi.extend_from_slice(hi);
                Bound::Excluded(IVec::from(b_hi))
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        SledIter {
            inner: self.areas[area_idx].scan_prefix(meta_prefix.as_slice()),
            bounds: (l, h),
        }
    }

    fn get(
        &self,
        area_idx: usize,
        meta_prefix: PrefixBytes,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        self.areas[area_idx].get(k).unwrap().map(|iv| iv.to_vec())
    }

    fn insert(
        &self,
        area_idx: usize,
        meta_prefix: PrefixBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<Vec<u8>> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        self.areas[area_idx]
            .insert(k, value)
            .unwrap()
            .map(|iv| iv.to_vec())
    }

    fn remove(
        &self,
        area_idx: usize,
        meta_prefix: PrefixBytes,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        self.areas[area_idx]
            .remove(k)
            .unwrap()
            .map(|iv| iv.to_vec())
    }
}

pub struct SledIter {
    inner: Iter,
    bounds: (Bound<IVec>, Bound<IVec>),
}

impl Iterator for SledIter {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((k, v))) = self.inner.next() {
            if self.bounds.contains(&k) {
                return Some((k[PREFIX_SIZ..].to_vec(), v.to_vec()));
            }
        }
        None
    }
}

impl DoubleEndedIterator for SledIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some(Ok((k, v))) = self.inner.next_back() {
            if self.bounds.contains(&k) {
                return Some((k[PREFIX_SIZ..].to_vec(), v.to_vec()));
            }
        }
        None
    }
}

// key of the prefix allocator in the 'meta'
struct PrefixAllocator {
    key: [u8; 1],
}

impl PrefixAllocator {
    const fn init() -> (Self, PrefixBytes) {
        (
            Self {
                key: META_KEY_PREFIX_ALLOCATOR,
            },
            (RESERVED_ID_CNT + Prefix::MIN).to_be_bytes(),
        )
    }

    fn next(base: Option<&[u8]>) -> Option<[u8; PREFIX_SIZ]> {
        base.map(|bytes| (crate::parse_prefix!(bytes) + 1).to_be_bytes())
    }
}

fn sled_open() -> Result<Db> {
    let dir = get_data_dir();

    let db = Config::new()
        .path(&dir)
        .mode(Mode::HighThroughput)
        .use_compression(true)
        .open()
        .c(d!())?;

    // avoid setting again on an opened DB
    info_omit!(vsdb_set_base_dir(dir));

    Ok(db)
}
