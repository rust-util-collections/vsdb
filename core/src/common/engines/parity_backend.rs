use crate::common::{
    vsdb_get_base_dir, vsdb_set_base_dir, Engine, Pre, PreBytes, RawKey, RawValue,
    PREFIX_SIZE, RESERVED_ID_CNT,
};
use parity_db::{BTreeIterator, CompressionType, Db as DB, Options};
use parking_lot::Mutex;
use ruc::*;
use std::{
    borrow::Cow,
    ops::{Bound, RangeBounds},
    sync::{
        atomic::{AtomicUsize, Ordering},
        LazyLock,
    },
};

// NOTE:
// The last COLID is preserved for the meta storage,
// so the max value should be `u8::MAX - 1`
const DATA_SET_NUM: u8 = 2;

const META_COLID: u8 = DATA_SET_NUM;

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];
const META_KEY_NULL: [u8; 0] = [0; 0];

static HDR: LazyLock<DB> = LazyLock::new(|| paritydb_open().unwrap());

pub struct ParityEngine {
    hdr: &'static DB,
    prefix_allocator: PreAllocator,
    max_keylen: AtomicUsize,
}

impl ParityEngine {
    #[inline(always)]
    fn get_max_keylen(&self) -> usize {
        self.max_keylen.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn set_max_key_len(&self, len: usize) {
        self.max_keylen.store(len, Ordering::Relaxed);
        self.hdr
            .commit([(
                META_COLID,
                META_KEY_MAX_KEYLEN,
                Some(len.to_be_bytes().to_vec()),
            )])
            .unwrap();
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
        let hdr = &HDR;

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
            prefix_allocator,
            // length of the raw key, exclude the hdr prefix
            max_keylen,
        })
    }

    // 'step 1' and 'step 2' is not atomic in multi-threads scene,
    // so we use a `Mutex` lock for thread safe.
    #[allow(unused_variables)]
    fn alloc_prefix(&self) -> Pre {
        static LK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
        let x = LK.lock();

        // step 1
        let ret = crate::parse_prefix!(self
            .hdr
            .get(META_COLID, &self.prefix_allocator.key)
            .unwrap()
            .unwrap());

        // step 2
        self.hdr
            .commit([(
                META_COLID,
                self.prefix_allocator.key,
                Some((1 + ret).to_be_bytes().to_vec()),
            )])
            .unwrap();

        ret
    }

    fn area_count(&self) -> usize {
        DATA_SET_NUM as usize
    }

    fn flush(&self) {}

    fn iter(&self, hdr_prefix: PreBytes) -> ParityIter {
        let area_idx = self.area_idx(hdr_prefix);

        let mut inner = self.hdr.iter(area_idx as u8).unwrap();
        inner.seek(&hdr_prefix).unwrap();

        let mut inner_rev = self.hdr.iter(area_idx as u8).unwrap();
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
        let area_idx = self.area_idx(hdr_prefix);

        let mut inner = self.hdr.iter(area_idx as u8).unwrap();
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

        let mut inner_rev = self.hdr.iter(area_idx as u8).unwrap();
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
        let area_idx = self.area_idx(hdr_prefix);

        let mut k = hdr_prefix.to_vec();
        k.extend_from_slice(key);
        self.hdr.get(area_idx as u8, &k).unwrap()
    }

    fn insert(
        &self,
        hdr_prefix: PreBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<RawValue> {
        let area_idx = self.area_idx(hdr_prefix);

        let mut k = hdr_prefix.to_vec();
        k.extend_from_slice(key);

        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }

        let old_v = self.hdr.get(area_idx as u8, &k).unwrap();
        self.hdr
            .commit([(area_idx as u8, k, Some(value.to_vec()))])
            .unwrap();
        old_v
    }

    fn remove(&self, hdr_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let area_idx = self.area_idx(hdr_prefix);

        let mut k = hdr_prefix.to_vec();
        k.extend_from_slice(key);
        let old_v = self.hdr.get(area_idx as u8, &k).unwrap();
        self.hdr.commit([(area_idx as u8, k, None)]).unwrap();
        old_v
    }

    fn get_instance_len_hint(&self, instance_prefix: PreBytes) -> u64 {
        crate::parse_int!(
            self.hdr.get(META_COLID, &instance_prefix).unwrap().unwrap(),
            u64
        )
    }

    fn set_instance_len_hint(&self, instance_prefix: PreBytes, new_len: u64) {
        self.hdr
            .commit([(
                META_COLID,
                instance_prefix,
                Some(new_len.to_be_bytes().to_vec()),
            )])
            .unwrap();
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
            Some((ik, iv)) => {
                if !ik.starts_with(&self.prefix) {
                    return None;
                }
                let k = &ik[PREFIX_SIZE..];
                match self.range.1.as_ref() {
                    Bound::Unbounded => Some((k.to_vec(), iv)),
                    Bound::Excluded(u) => {
                        if &u[PREFIX_SIZE..] > k {
                            Some((k.to_vec(), iv))
                        } else {
                            None
                        }
                    }
                    Bound::Included(u) => {
                        if &u[PREFIX_SIZE..] >= k {
                            Some((k.to_vec(), iv))
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
            Some((ik, iv)) => {
                if !ik.starts_with(&self.prefix) {
                    return None;
                }
                let k = &ik[PREFIX_SIZE..];
                match self.range.0.as_ref() {
                    Bound::Unbounded => Some((k.to_vec(), iv)),
                    Bound::Excluded(l) => {
                        if &l[PREFIX_SIZE..] < k {
                            Some((k.to_vec(), iv))
                        } else {
                            None
                        }
                    }
                    Bound::Included(l) => {
                        if &l[PREFIX_SIZE..] <= k {
                            Some((k.to_vec(), iv))
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

fn paritydb_open() -> Result<DB> {
    let dir = vsdb_get_base_dir();

    // avoid setting again on an opened DB
    omit!(vsdb_set_base_dir(&dir));

    let mut cfg = Options::with_columns(&dir, 1 + DATA_SET_NUM);
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
