use crate::common::{
    BatchTrait, Engine, PREFIX_SIZE, Pre, PreBytes, RESERVED_ID_CNT, RawKey, RawValue,
    vsdb_get_base_dir, vsdb_set_base_dir,
};
use parity_db::{BTreeIterator, CompressionType, Db as DB, Options};
use parking_lot::Mutex;
use ruc::*;
use std::{
    borrow::Cow,
    fs,
    ops::{Bound, RangeBounds},
    path::Path,
    sync::{
        LazyLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

// NOTE:
// The last COLID is preserved for the meta storage,
// so the max value should be `u8::MAX - 1`
const DATA_SET_NUM: u8 = 2;
const SHARD_CNT: usize = 16;

const META_COLID: u8 = DATA_SET_NUM;

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];
const META_KEY_NULL: [u8; 0] = [0; 0];

pub struct ParityEngine {
    hdr: &'static DB,
    shards: Vec<&'static DB>,
    prefix_allocator: PreAllocator,
    max_keylen: AtomicUsize,
}

// Optimization: Helper function to build full key with pre-allocated capacity
#[inline(always)]
fn make_full_key(meta_prefix: &[u8], key: &[u8]) -> Vec<u8> {
    let total_len = meta_prefix.len() + key.len();
    let mut full_key = Vec::with_capacity(total_len);
    full_key.extend_from_slice(meta_prefix);
    full_key.extend_from_slice(key);
    full_key
}

impl ParityEngine {
    #[inline(always)]
    fn get_shard_idx(&self, prefix: PreBytes) -> usize {
        (prefix[0] as usize) % SHARD_CNT
    }

    #[inline(always)]
    fn get_db(&self, prefix: PreBytes) -> &'static DB {
        self.shards[self.get_shard_idx(prefix)]
    }

    #[inline(always)]
    fn get_max_keylen(&self) -> usize {
        self.max_keylen.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn set_max_key_len(&self, len: usize) {
        let current = self.max_keylen.load(Ordering::Relaxed);
        if len > current {
            self.hdr
                .commit([(
                    META_COLID,
                    META_KEY_MAX_KEYLEN,
                    Some(len.to_be_bytes().to_vec()),
                )])
                .unwrap();
            self.max_keylen.store(len, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    fn get_upper_bound_value(&self, hdr_prefix: PreBytes) -> Vec<u8> {
        const BUF: [u8; 256] = [u8::MAX; 256];

        let mut max_guard = hdr_prefix.to_vec();

        let l = self.get_max_keylen();
        if l < 257 {
            max_guard.extend_from_slice(&BUF[..l]);
        } else {
            max_guard.extend_from_slice(&vec![u8::MAX; l]);
        }

        max_guard
    }
}

impl Engine for ParityEngine {
    fn new() -> Result<Self> {
        let base_dir = vsdb_get_base_dir();
        // avoid setting again on an opened DB
        omit!(vsdb_set_base_dir(&base_dir));

        let mut shards = Vec::with_capacity(SHARD_CNT);

        // Ensure base dir exists
        fs::create_dir_all(&base_dir).c(d!())?;

        for i in 0..SHARD_CNT {
            let dir = base_dir.join(format!("shard_{}", i));
            let db = paritydb_open_shard(&dir)?;
            shards.push(Box::leak(Box::new(db)) as &'static DB);
        }

        let hdr = shards[0];

        let (prefix_allocator, initial_value) = PreAllocator::init();

        if hdr.get(META_COLID, &META_KEY_MAX_KEYLEN).c(d!())?.is_none() {
            hdr.commit([(
                META_COLID,
                META_KEY_MAX_KEYLEN,
                Some(0_usize.to_be_bytes().to_vec()),
            )])
            .c(d!())?;
        }

        if hdr
            .get(META_COLID, &prefix_allocator.key)
            .c(d!())?
            .is_none()
        {
            hdr.commit([(
                META_COLID,
                prefix_allocator.key,
                Some(initial_value.to_vec()),
            )])
            .c(d!())?;
        }

        let max_keylen = AtomicUsize::new(crate::parse_int!(
            hdr.get(META_COLID, &META_KEY_MAX_KEYLEN).unwrap().unwrap(),
            usize
        ));

        Ok(ParityEngine {
            hdr,
            shards,
            prefix_allocator,
            // length of the raw key, exclude the hdr prefix
            max_keylen,
        })
    }

    // 'step 1' and 'step 2' is not atomic in multi-threads scene,
    // Optimization: Use AtomicU64 for better performance, persist periodically
    #[allow(unused_variables)]
    fn alloc_prefix(&self) -> Pre {
        static COUNTER: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
        static LK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

        // Try lock-free fast path first
        let current = COUNTER.load(Ordering::Relaxed);
        if current > 0 {
            let next = COUNTER.fetch_add(1, Ordering::AcqRel);
            // Persist every 1024 allocations to reduce write amplification
            if next.is_multiple_of(1024) {
                let _ = self.hdr.commit([(
                    META_COLID,
                    self.prefix_allocator.key,
                    Some((next + 1024).to_be_bytes().to_vec()),
                )]);
            }
            return next;
        }

        // Slow path: initialize from DB
        let x = LK.lock();
        let db_value = COUNTER.load(Ordering::Relaxed);
        if db_value == 0 {
            // step 1
            let ret = crate::parse_prefix!(
                self.hdr
                    .get(META_COLID, &self.prefix_allocator.key)
                    .unwrap()
                    .unwrap()
            );
            COUNTER.store(ret + 1, Ordering::Release);

            // step 2
            self.hdr
                .commit([(
                    META_COLID,
                    self.prefix_allocator.key,
                    Some((ret + 1024).to_be_bytes().to_vec()),
                )])
                .unwrap();

            ret
        } else {
            drop(x);
            self.alloc_prefix()
        }
    }

    fn area_count(&self) -> usize {
        DATA_SET_NUM as usize
    }

    fn flush(&self) {}

    fn iter(&self, hdr_prefix: PreBytes) -> ParityIter {
        let db = self.get_db(hdr_prefix);
        let area_idx = self.area_idx(hdr_prefix);

        let mut inner = db.iter(area_idx as u8).unwrap();
        inner.seek(&hdr_prefix).unwrap();

        let mut inner_rev = db.iter(area_idx as u8).unwrap();
        inner_rev
            .seek(&self.get_upper_bound_value(hdr_prefix))
            .unwrap();

        ParityIter {
            inner,
            inner_rev,
            prefix: hdr_prefix,
            range: (Bound::Unbounded, Bound::Unbounded),
        }
    }

    fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        hdr_prefix: PreBytes,
        bounds: R,
    ) -> ParityIter {
        let db = self.get_db(hdr_prefix);
        let area_idx = self.area_idx(hdr_prefix);

        let mut inner = db.iter(area_idx as u8).unwrap();
        let mut b_lo = hdr_prefix.to_vec();
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                b_lo.extend_from_slice(lo);
                inner.seek(&b_lo).unwrap();
                Bound::Included(b_lo)
            }
            Bound::Excluded(lo) => {
                b_lo.extend_from_slice(lo);
                b_lo.push(0);
                inner.seek(&b_lo).unwrap();
                Bound::Excluded(b_lo)
            }
            Bound::Unbounded => {
                inner.seek(&hdr_prefix).unwrap();
                Bound::Unbounded
            }
        };

        let mut inner_rev = db.iter(area_idx as u8).unwrap();
        let mut b_hi = hdr_prefix.to_vec();
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.extend_from_slice(hi);
                inner_rev.seek(&b_hi).unwrap();
                Bound::Included(b_hi)
            }
            Bound::Excluded(hi) => {
                let mut hi = hi.to_vec();
                if let Some(i) = hi.last_mut() {
                    if 0 == *i {
                        hi.pop().unwrap();
                    } else {
                        *i -= 1;
                    }
                    b_hi.extend_from_slice(&hi);
                } else {
                    b_hi = META_KEY_NULL.to_vec();
                }
                inner_rev.seek(&b_hi).unwrap();
                Bound::Included(b_hi) // use `Included` here!
            }
            Bound::Unbounded => {
                inner_rev
                    .seek(&self.get_upper_bound_value(hdr_prefix))
                    .unwrap();
                Bound::Unbounded
            }
        };

        ParityIter {
            inner,
            inner_rev,
            prefix: hdr_prefix,
            range: (l, h),
        }
    }

    fn get(&self, hdr_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let db = self.get_db(hdr_prefix);
        let area_idx = self.area_idx(hdr_prefix);

        // Optimization: Use helper function with pre-allocated capacity
        let full_key = make_full_key(hdr_prefix.as_slice(), key);
        db.get(area_idx as u8, &full_key).unwrap()
    }

    fn insert(&self, hdr_prefix: PreBytes, key: &[u8], value: &[u8]) {
        let db = self.get_db(hdr_prefix);
        let area_idx = self.area_idx(hdr_prefix);

        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }

        // Optimization: Use helper function with pre-allocated capacity
        let full_key = make_full_key(hdr_prefix.as_slice(), key);

        // Direct insert without read-before-write
        db.commit([(area_idx as u8, full_key, Some(value.to_vec()))])
            .unwrap();
    }

    fn remove(&self, hdr_prefix: PreBytes, key: &[u8]) {
        let db = self.get_db(hdr_prefix);
        let area_idx = self.area_idx(hdr_prefix);

        // Optimization: Use helper function with pre-allocated capacity
        let full_key = make_full_key(hdr_prefix.as_slice(), key);

        // Direct remove without read-before-write
        db.commit([(area_idx as u8, full_key, None)]).unwrap();
    }

    fn batch_begin<'a>(&'a self, meta_prefix: PreBytes) -> Box<dyn BatchTrait + 'a> {
        Box::new(ParityBatch::new(meta_prefix, self))
    }
}

pub struct ParityBatch<'a> {
    ops: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
    area_idx: u8,
    meta_prefix: PreBytes,
    max_key_len: usize,
    engine: &'a ParityEngine,
}

impl<'a> ParityBatch<'a> {
    fn new(meta_prefix: PreBytes, engine: &'a ParityEngine) -> Self {
        let area_idx = engine.area_idx(meta_prefix);
        Self {
            ops: Vec::with_capacity(16),
            area_idx: area_idx as u8,
            meta_prefix,
            max_key_len: 0,
            engine,
        }
    }
}

impl BatchTrait for ParityBatch<'_> {
    #[inline(always)]
    fn insert(&mut self, key: &[u8], value: &[u8]) {
        let full_key = make_full_key(self.meta_prefix.as_slice(), key);
        if key.len() > self.max_key_len {
            self.max_key_len = key.len();
        }
        self.ops
            .push((self.area_idx, full_key, Some(value.to_vec())));
    }

    #[inline(always)]
    fn remove(&mut self, key: &[u8]) {
        let full_key = make_full_key(self.meta_prefix.as_slice(), key);
        self.ops.push((self.area_idx, full_key, None));
    }

    #[inline(always)]
    fn commit(&mut self) -> Result<()> {
        let db = self.engine.get_db(self.meta_prefix);
        db.commit(self.ops.drain(..)).unwrap();

        if self.max_key_len > self.engine.get_max_keylen() {
            self.engine.set_max_key_len(self.max_key_len);
        }

        Ok(())
    }
}

pub struct ParityIter {
    inner: BTreeIterator<'static>,
    inner_rev: BTreeIterator<'static>,
    prefix: PreBytes,
    range: (Bound<RawKey>, Bound<RawKey>),
}

impl Iterator for ParityIter {
    type Item = (RawKey, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next().unwrap() {
            Some((mut ik, iv)) => {
                if !ik.starts_with(&self.prefix) {
                    return None;
                }
                match self.range.1.as_ref() {
                    Bound::Unbounded => {
                        ik.drain(..PREFIX_SIZE);
                        Some((ik, iv))
                    }
                    Bound::Excluded(u) => {
                        if u[..] > ik[..] {
                            ik.drain(..PREFIX_SIZE);
                            Some((ik, iv))
                        } else {
                            None
                        }
                    }
                    Bound::Included(u) => {
                        if u[..] >= ik[..] {
                            ik.drain(..PREFIX_SIZE);
                            Some((ik, iv))
                        } else {
                            None
                        }
                    }
                }
            }
            None => None,
        }
    }
}

impl DoubleEndedIterator for ParityIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self.inner_rev.prev().unwrap() {
            Some((mut ik, iv)) => {
                if !ik.starts_with(&self.prefix) {
                    return None;
                }
                match self.range.0.as_ref() {
                    Bound::Unbounded => {
                        ik.drain(..PREFIX_SIZE);
                        Some((ik, iv))
                    }
                    Bound::Excluded(l) => {
                        if l[..] < ik[..] {
                            ik.drain(..PREFIX_SIZE);
                            Some((ik, iv))
                        } else {
                            None
                        }
                    }
                    Bound::Included(l) => {
                        if l[..] <= ik[..] {
                            ik.drain(..PREFIX_SIZE);
                            Some((ik, iv))
                        } else {
                            None
                        }
                    }
                }
            }
            None => None,
        }
    }
}

// key of the prefix allocator in the 'hdr'
struct PreAllocator {
    key: [u8; 1],
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

    // fn next(base: &[u8]) -> [u8; PREFIX_SIZE] {
    //     (crate::parse_prefix!(base) + 1).to_be_bytes()
    // }
}

fn paritydb_open_shard(dir: &Path) -> Result<DB> {
    let mut cfg = Options::with_columns(dir, 1 + DATA_SET_NUM);
    cfg.columns.iter_mut().for_each(|c| {
        c.btree_index = true;
    });

    #[cfg(feature = "compress")]
    cfg.columns.iter_mut().for_each(|c| {
        c.compression = CompressionType::Snappy;
    });

    #[cfg(not(feature = "compress"))]
    cfg.columns.iter_mut().for_each(|c| {
        c.compression = CompressionType::NoCompression;
    });

    let db = DB::open_or_create(&cfg).c(d!())?;

    Ok(db)
}
