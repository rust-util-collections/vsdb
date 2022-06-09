use crate::common::{
    vsdb_get_base_dir, vsdb_set_base_dir, BranchID, Engine, Pre, PreBytes, RawKey,
    RawValue, VersionID, GB, INITIAL_BRANCH_ID, META_KEY_SIZ, RESERVED_ID_CNT,
};
use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use ruc::*;
use sled::{Config, Db, IVec, Iter, Mode, Tree};
use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

// the 'prefix search' in sled is just a global scaning,
// use a relative larger number to sharding the `Tree` pressure.
const DATA_SET_NUM: usize = 796;

const META_KEY_BRANCH_ID: [u8; META_KEY_SIZ] = (u64::MAX - 1).to_be_bytes();
const META_KEY_VERSION_ID: [u8; META_KEY_SIZ] = (u64::MAX - 2).to_be_bytes();
const META_KEY_PREFIX_ALLOCATOR: [u8; META_KEY_SIZ] = u64::MIN.to_be_bytes();

pub(crate) struct SledEngine {
    meta: Db,
    areas: Vec<Tree>,
    prefix_allocator: PreAllocator,

    data_cache: Arc<RwLock<LruCache<RawKey, Option<RawValue>>>>,
    meta_cache: Arc<RwLock<LruCache<[u8; META_KEY_SIZ], u64>>>,
}

impl Engine for SledEngine {
    fn new() -> Result<Self> {
        let meta = sled_open().c(d!())?;

        let areas = (0..DATA_SET_NUM)
            .map(|idx| meta.open_tree(idx.to_be_bytes()).c(d!()))
            .collect::<Result<Vec<_>>>()?;

        let (prefix_allocator, initial_value) = PreAllocator::init();

        if meta.get(&META_KEY_BRANCH_ID).c(d!())?.is_none() {
            meta.insert(
                META_KEY_BRANCH_ID,
                (1 + INITIAL_BRANCH_ID as usize).to_be_bytes(),
            )
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
            meta_cache: Arc::new(RwLock::new(LruCache::new(100_0000))),
            data_cache: Arc::new(RwLock::new(LruCache::new(1_0000_0000))),
        })
    }

    // 'step 1' and 'step 2' is not atomic in multi-threads scene,
    // so we use a `Mutex` lock for thread safe.
    #[allow(unused_variables)]
    fn alloc_prefix(&self) -> Pre {
        static LK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
        let x = LK.lock();

        let mut meta_cache = self.meta_cache.write();

        // step 1
        let ret = if let Some(v) = meta_cache.get(&self.prefix_allocator.key) {
            *v
        } else {
            crate::parse_prefix!(
                self.meta
                    .get(self.prefix_allocator.key)
                    .unwrap()
                    .unwrap()
                    .as_ref()
            )
        };

        // step 2
        meta_cache.put(self.prefix_allocator.key, 1 + ret);
        drop(meta_cache);

        self.meta
            .insert(self.prefix_allocator.key, (1 + ret).to_be_bytes())
            .unwrap();

        ret
    }

    // 'step 1' and 'step 2' is not atomic in multi-threads scene,
    // so we use a `Mutex` lock for thread safe.
    #[allow(unused_variables)]
    fn alloc_branch_id(&self) -> BranchID {
        static LK: Lazy<Mutex<bool>> = Lazy::new(|| Mutex::new(false));
        let x = LK.lock();

        let mut meta_cache = self.meta_cache.write();

        // step 1
        let ret = if let Some(v) = meta_cache.get(&META_KEY_BRANCH_ID) {
            *v
        } else {
            crate::parse_int!(
                self.meta.get(META_KEY_BRANCH_ID).unwrap().unwrap().as_ref(),
                BranchID
            )
        };

        // step 2
        meta_cache.put(META_KEY_BRANCH_ID, 1 + ret);
        drop(meta_cache);

        self.meta
            .insert(META_KEY_BRANCH_ID, (1 + ret).to_be_bytes())
            .unwrap();

        ret
    }

    // 'step 1' and 'step 2' is not atomic in multi-threads scene,
    // so we use a `Mutex` lock for thread safe.
    #[allow(unused_variables)]
    fn alloc_version_id(&self) -> VersionID {
        static LK: Lazy<Mutex<bool>> = Lazy::new(|| Mutex::new(false));
        let x = LK.lock();

        let mut meta_cache = self.meta_cache.write();

        // step 1
        let ret = if let Some(v) = meta_cache.get(&META_KEY_VERSION_ID) {
            *v
        } else {
            crate::parse_int!(
                self.meta
                    .get(META_KEY_VERSION_ID)
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                VersionID
            )
        };

        // step 2
        meta_cache.put(META_KEY_VERSION_ID, 1 + ret);
        drop(meta_cache);

        self.meta
            .insert(META_KEY_VERSION_ID, (1 + ret).to_be_bytes())
            .unwrap();

        ret
    }

    fn area_count(&self) -> usize {
        self.areas.len()
    }

    fn flush(&self) {
        (0..self.areas.len()).for_each(|i| {
            self.areas[i].flush().unwrap();
        });
    }

    fn iter(&self, area_idx: usize, meta_prefix: PreBytes) -> SledIter {
        SledIter {
            inner: self.areas[area_idx].scan_prefix(meta_prefix.as_slice()),
            bounds: (Bound::Unbounded, Bound::Unbounded),
        }
    }

    fn range<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        area_idx: usize,
        meta_prefix: PreBytes,
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
        meta_prefix: PreBytes,
        key: &[u8],
    ) -> Option<RawValue> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        let k = k.into_boxed_slice();

        if let Some(v) = self.data_cache.read().peek(&k) {
            v.clone()
        } else {
            self.areas[area_idx]
                .get(k)
                .unwrap()
                .map(|iv| iv.to_vec().into_boxed_slice())
        }
    }

    fn insert(
        &self,
        area_idx: usize,
        meta_prefix: PreBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<RawValue> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        let k = k.into_boxed_slice();

        self.data_cache
            .write()
            .put(k.clone(), Some(value.to_vec().into()));

        self.areas[area_idx]
            .insert(k, value)
            .unwrap()
            .map(|iv| iv.to_vec().into_boxed_slice())
    }

    fn remove(
        &self,
        area_idx: usize,
        meta_prefix: PreBytes,
        key: &[u8],
    ) -> Option<RawValue> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        let k = k.into_boxed_slice();

        self.data_cache.write().put(k.clone(), None);

        self.areas[area_idx]
            .remove(k)
            .unwrap()
            .map(|iv| iv.to_vec().into_boxed_slice())
    }

    fn get_instance_len(&self, instance_prefix: PreBytes) -> u64 {
        if let Some(v) = self.meta_cache.read().peek(&instance_prefix) {
            *v
        } else {
            crate::parse_int!(self.meta.get(instance_prefix).unwrap().unwrap(), u64)
        }
    }

    fn set_instance_len(&self, instance_prefix: PreBytes, new_len: u64) {
        self.meta_cache.write().put(instance_prefix, new_len);
        self.meta
            .insert(instance_prefix, new_len.to_be_bytes())
            .unwrap();
    }
}

pub struct SledIter {
    inner: Iter,
    bounds: (Bound<IVec>, Bound<IVec>),
}

impl Iterator for SledIter {
    type Item = (RawKey, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((k, v)) = self.inner.next().map(|i| i.unwrap()) {
            if self.bounds.contains(&k) {
                return Some((
                    k[META_KEY_SIZ..].to_vec().into_boxed_slice(),
                    v.to_vec().into_boxed_slice(),
                ));
            }
        }
        None
    }
}

impl DoubleEndedIterator for SledIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some((k, v)) = self.inner.next_back().map(|i| i.unwrap()) {
            if self.bounds.contains(&k) {
                return Some((
                    k[META_KEY_SIZ..].to_vec().into_boxed_slice(),
                    v.to_vec().into_boxed_slice(),
                ));
            }
        }
        None
    }
}

// key of the prefix allocator in the 'meta'
struct PreAllocator {
    key: [u8; META_KEY_SIZ],
}

impl PreAllocator {
    const fn init() -> (Self, PreBytes) {
        (
            Self {
                key: META_KEY_PREFIX_ALLOCATOR,
            },
            (RESERVED_ID_CNT + Pre::MIN).to_be_bytes(),
        )
    }
}

fn sled_open() -> Result<Db> {
    let dir = vsdb_get_base_dir();

    // avoid setting again on an opened DB
    info_omit!(vsdb_set_base_dir(&dir));

    let mut cfg = Config::new()
        .path(&dir)
        .mode(Mode::HighThroughput)
        .cache_capacity(10 * GB);

    #[cfg(feature = "compress")]
    {
        cfg = cfg.use_compression(true).compression_factor(1);
    }

    #[cfg(not(feature = "compress"))]
    {
        cfg = cfg.use_compression(false);
    }

    cfg.open().c(d!())
}
