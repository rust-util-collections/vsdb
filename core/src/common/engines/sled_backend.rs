use crate::common::{
    vsdb_get_base_dir, vsdb_set_base_dir, Engine, Pre, PreBytes, RawKey, RawValue,
    PREFIX_SIZE, RESERVED_ID_CNT,
};
use parking_lot::Mutex;
use ruc::*;
use sled_db::{self, Db as DB, Iter, Mode};
use std::{
    borrow::Cow,
    ops::{Bound, RangeBounds},
    sync::{
        atomic::{AtomicUsize, Ordering},
        LazyLock,
    },
};

// NOTE:
// do NOT make the number of areas bigger than `u8::MAX`
const DATA_SET_NUM: usize = 2;

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

static HDR: LazyLock<DB> = LazyLock::new(|| sled_db_open().unwrap());

#[derive(Debug)]
pub struct SledEngine {
    hdr: &'static DB,
    prefix_allocator: PreAllocator,
    max_keylen: AtomicUsize,
}

impl SledEngine {
    #[inline(always)]
    fn get_max_keylen(&self) -> usize {
        self.max_keylen.load(Ordering::Relaxed)
    }

    // record max_key_len on default tree
    #[inline(always)]
    fn set_max_key_len(&self, len: usize) {
        self.max_keylen.store(len, Ordering::Relaxed);
        self.hdr
            .insert(META_KEY_MAX_KEYLEN, len.to_be_bytes().to_vec())
            .unwrap();
    }

    #[inline(always)]
    fn get_upper_bound_value(&self, meta_prefix: PreBytes) -> Vec<u8> {
        const BUF: [u8; 256] = [u8::MAX; 256];

        let mut max_guard = meta_prefix.to_vec();

        let l = self.get_max_keylen();
        if l < 257 {
            max_guard.extend_from_slice(&BUF[..l]);
        } else {
            max_guard.extend_from_slice(&vec![u8::MAX; l]);
        }

        max_guard
    }
    // ==== assist functions ====
    //  get specific Tree  by area_idx
    #[inline(always)]
    pub fn get_area_tree(&self, hdr_prefix: PreBytes) -> sled_db::Tree {
        let area_idx = self.area_idx(hdr_prefix);
        self.hdr.open_tree(area_idx.to_string()).unwrap()
    }
}

impl Engine for SledEngine {
    fn new() -> Result<Self> {
        let hdr = &HDR;

        let (prefix_allocator, initial_value) = PreAllocator::init();

        if hdr.get(META_KEY_MAX_KEYLEN).c(d!())?.is_none() {
            hdr.insert(META_KEY_MAX_KEYLEN, 0_usize.to_be_bytes().to_vec())
                .c(d!())?;
        }

        if hdr.get(prefix_allocator.key).unwrap().is_none() {
            hdr.insert(prefix_allocator.key, initial_value.to_vec())
                .unwrap();
        }
        let max_keylen = AtomicUsize::new(crate::parse_int!(
            hdr.get(META_KEY_MAX_KEYLEN)
                .unwrap()
                .unwrap()
                .as_ref()
                .to_vec(),
            usize
        ));

        Ok(SledEngine {
            hdr,
            prefix_allocator,
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
            .get(self.prefix_allocator.key)
            .unwrap()
            .unwrap());

        // step 2
        self.hdr
            .insert(self.prefix_allocator.key, (1 + ret).to_be_bytes().to_vec())
            .unwrap();
        ret
    }

    fn area_count(&self) -> usize {
        DATA_SET_NUM
    }

    fn flush(&self) {
        self.hdr.flush().unwrap();
    }

    fn insert(
        &self,
        hdr_prefix: PreBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<RawValue> {
        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }
        let mut k = hdr_prefix.to_vec();
        k.extend_from_slice(key);
        let area_tree = self.get_area_tree(hdr_prefix);
        let old_v = area_tree.get(&k).unwrap().map(|iv| iv.as_ref().to_vec());
        area_tree.insert(k, value).unwrap();
        old_v
    }

    fn get(&self, hdr_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let area_tree = self.get_area_tree(hdr_prefix);
        let mut k = hdr_prefix.to_vec();
        k.extend_from_slice(key);
        area_tree.get(k).unwrap().map(|iv| iv.as_ref().to_vec())
    }

    fn remove(&self, hdr_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let mut k = hdr_prefix.to_vec();
        k.extend_from_slice(key);
        let area_tree = self.get_area_tree(hdr_prefix);
        let old_v = area_tree.get(&k).unwrap();
        area_tree.remove(k).unwrap();
        let old_v = old_v.map(|iv| iv.as_ref().to_vec());
        old_v
    }

    fn get_instance_len_hint(&self, instance_prefix: PreBytes) -> u64 {
        let l = self.hdr.get(instance_prefix).unwrap().unwrap();
        let l = u64::from_be_bytes(l.as_ref().try_into().unwrap());
        l
    }

    fn set_instance_len_hint(&self, instance_prefix: PreBytes, new_len: u64) {
        self.hdr
            .insert(instance_prefix, new_len.to_be_bytes().to_vec())
            .unwrap();
    }
    fn iter(&self, hdr_prefix: PreBytes) -> SledIter {
        let inner = self.get_area_tree(hdr_prefix).scan_prefix(hdr_prefix);
        SledIter { inner }
    }
    fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        hdr_prefix: PreBytes,
        bounds: R,
    ) -> SledIter {
        let mut b_lo = hdr_prefix.to_vec();
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                b_lo.extend_from_slice(lo);
                b_lo.as_slice() // For inclusive lower bound, no extra byte
            }
            Bound::Excluded(lo) => {
                b_lo.extend_from_slice(lo);
                b_lo.push(0u8); // Excluding, so add a byte to ensure exclusivity
                b_lo.as_slice()
            }
            _ => hdr_prefix.as_ref(),
        };

        let mut b_hi = hdr_prefix.to_vec();
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.extend_from_slice(hi);
                b_hi.push(0u8); // Include the upper bound
                b_hi
            }
            Bound::Excluded(hi) => {
                b_hi.extend_from_slice(hi);
                b_hi
            }
            _ => self.get_upper_bound_value(hdr_prefix),
        };

        // Query the tree with the constructed bounds
        let inner = self.get_area_tree(hdr_prefix).range(l..h.as_ref());

        SledIter { inner }
    }
}

pub struct SledIter {
    inner: Iter,
}

impl Iterator for SledIter {
    type Item = (RawKey, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().and_then(|result| {
            result
                .map(|(ik, iv)| (ik[PREFIX_SIZE..].to_vec(), iv.as_ref().to_vec()))
                .ok()
        })
    }
}

impl DoubleEndedIterator for SledIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().and_then(|result| {
            result
                .map(|(ik, iv)| (ik[PREFIX_SIZE..].to_vec(), iv.as_ref().to_vec()))
                .ok()
        })
    }
}

// key of the prefix allocator in the 'meta'
#[derive(Debug)]
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

fn sled_db_open() -> Result<DB> {
    let dir = vsdb_get_base_dir();
    // avoid setting again on an opened DB
    omit!(vsdb_set_base_dir(&dir));

    let cfg = sled_db::Config::default()
        .path(&dir)
        //set system page cache 256 MB
        .cache_capacity(256 * 1024 * 1024)
        // fastest
        .mode(Mode::HighThroughput);

    // #[cfg(feature = "compress")]
    // let cfg = cfg.use_compression(true).compression_factor(20);
    let db = cfg.open().c(d!(dir.to_str().unwrap()))?;
    Ok(db)
}
