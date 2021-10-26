use crate::common::{
    get_data_dir, vsdb_set_base_dir, BranchID, Engine, Prefix, PrefixBytes, VersionID,
    PREFIX_SIZ, RESERVED_ID_CNT,
};
use lazy_static::lazy_static;
use num_bigint::BigUint;
use num_traits::CheckedSub;
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, DBIterator, Direction,
    IteratorMode, Options, ReadOptions, SliceTransform, DB,
};
use ruc::*;
use std::{
    mem::size_of,
    ops::{Add, Bound, RangeBounds},
    sync::atomic::{AtomicUsize, Ordering},
};

const DATA_SET_NUM: u8 = 4;

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_BRANCH_ID: [u8; 1] = [u8::MAX - 1];
const META_KEY_VERSION_ID: [u8; 1] = [u8::MAX - 2];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

lazy_static! {
    static ref HDR: (DB, Vec<String>) = rocksdb_open().unwrap();
}

pub(crate) struct RocksEngine {
    meta: &'static DB,
    areas: Vec<&'static str>,
    prefix_allocator: PrefixAllocator,
    max_keylen: AtomicUsize,
}

impl RocksEngine {
    #[inline(always)]
    fn cf_hdr(&self, area_idx: usize) -> &ColumnFamily {
        self.meta.cf_handle(self.areas[area_idx]).unwrap()
    }

    #[inline(always)]
    fn get_max_keylen(&self) -> usize {
        self.max_keylen.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn set_max_key_len(&self, len: usize) {
        self.max_keylen.store(len, Ordering::Relaxed);
        self.meta
            .put(META_KEY_MAX_KEYLEN, len.to_be_bytes())
            .unwrap();
    }

    #[inline(always)]
    fn get_upper_bound_value(&self, meta_prefix: PrefixBytes) -> Vec<u8> {
        lazy_static! {
            static ref BUF: Vec<u8> = vec![u8::MAX; 512];
        }

        let mut max_guard = meta_prefix.to_vec();

        let l = self.get_max_keylen();
        if l < 513 {
            max_guard.extend_from_slice(&BUF[..l]);
        } else {
            max_guard.extend_from_slice(&vec![u8::MAX; l]);
        }

        max_guard
    }
}

impl Engine for RocksEngine {
    fn new() -> Result<Self> {
        let (meta, areas) =
            (&HDR.0, HDR.1.iter().map(|i| i.as_str()).collect::<Vec<_>>());

        let (prefix_allocator, initial_value) = PrefixAllocator::init();

        if meta.get(&META_KEY_MAX_KEYLEN).c(d!())?.is_none() {
            meta.put(META_KEY_MAX_KEYLEN, 0_usize.to_be_bytes())
                .c(d!())?;
        }

        if meta.get(&META_KEY_BRANCH_ID).c(d!())?.is_none() {
            meta.put(META_KEY_BRANCH_ID, 0_usize.to_be_bytes())
                .c(d!())?;
        }

        if meta.get(&META_KEY_VERSION_ID).c(d!())?.is_none() {
            meta.put(META_KEY_VERSION_ID, 0_usize.to_be_bytes())
                .c(d!())?;
        }

        if meta.get(prefix_allocator.key).c(d!())?.is_none() {
            meta.put(prefix_allocator.key, initial_value).c(d!())?;
        }

        let max_keylen = AtomicUsize::new(crate::parse_int!(
            meta.get(&META_KEY_MAX_KEYLEN).unwrap().unwrap(),
            usize
        ));

        Ok(RocksEngine {
            meta,
            areas,
            prefix_allocator,
            // length of the raw key, exclude the meta prefix
            max_keylen,
        })
    }

    fn alloc_prefix(&self) -> Prefix {
        let ret = self.meta.get(self.prefix_allocator.key).unwrap().unwrap();
        self.meta
            .put(self.prefix_allocator.key, PrefixAllocator::next(&ret))
            .unwrap();
        crate::parse_prefix!(ret)
    }

    fn alloc_branch_id(&self) -> BranchID {
        let ret = crate::parse_int!(
            self.meta.get(META_KEY_BRANCH_ID).unwrap().unwrap(),
            BranchID
        );
        self.meta
            .put(META_KEY_BRANCH_ID, (1 + ret).to_be_bytes())
            .unwrap();
        ret
    }

    fn alloc_version_id(&self) -> VersionID {
        let ret = crate::parse_int!(
            self.meta.get(META_KEY_BRANCH_ID).unwrap().unwrap(),
            VersionID
        );
        self.meta
            .put(META_KEY_VERSION_ID, (1 + ret).to_be_bytes())
            .unwrap();
        ret
    }

    fn area_count(&self) -> u8 {
        DATA_SET_NUM
    }

    fn flush(&self) {
        self.meta.flush().unwrap();
        (0..DATA_SET_NUM).for_each(|i| {
            self.meta.flush_cf(self.cf_hdr(i as usize)).unwrap();
        });
    }

    fn iter(&self, area_idx: usize, meta_prefix: PrefixBytes) -> RocksIter {
        let inner = self
            .meta
            .prefix_iterator_cf(self.cf_hdr(area_idx), meta_prefix);

        let mut opt = ReadOptions::default();
        opt.set_prefix_same_as_start(true);

        let inner_rev = self.meta.iterator_cf_opt(
            self.cf_hdr(area_idx),
            opt,
            IteratorMode::From(
                &self.get_upper_bound_value(meta_prefix),
                Direction::Reverse,
            ),
        );

        RocksIter { inner, inner_rev }
    }

    fn range<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        area_idx: usize,
        meta_prefix: PrefixBytes,
        bounds: R,
    ) -> RocksIter {
        let mut opt = ReadOptions::default();
        let mut opt_rev = ReadOptions::default();

        let mut b_lo = meta_prefix.to_vec();
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                b_lo.extend_from_slice(lo);
                opt.set_iterate_lower_bound(b_lo.as_slice());
                opt_rev.set_iterate_lower_bound(b_lo.as_slice());
                b_lo.as_slice()
            }
            Bound::Excluded(lo) => {
                b_lo.extend_from_slice(lo);
                if let Some(i) = BigUint::from_bytes_be(&b_lo)
                    .checked_sub(&BigUint::from_bytes_le(&1_u8.to_le_bytes()))
                {
                    opt.set_iterate_lower_bound(i.to_bytes_be());
                    opt_rev.set_iterate_lower_bound(i.to_bytes_be());
                }
                b_lo.as_slice()
            }
            _ => meta_prefix.as_slice(),
        };

        let mut b_hi = meta_prefix.to_vec();
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.extend_from_slice(hi);
                opt.set_iterate_upper_bound(
                    BigUint::from_bytes_be(&b_hi).add(&1_u8).to_bytes_be(),
                );
                opt_rev.set_iterate_upper_bound(
                    BigUint::from_bytes_be(&b_hi).add(&1_u8).to_bytes_be(),
                );
                b_hi
            }
            Bound::Excluded(hi) => {
                b_hi.extend_from_slice(hi);
                opt.set_iterate_upper_bound(b_hi.as_slice());
                opt_rev.set_iterate_upper_bound(b_hi.as_slice());
                b_hi
            }
            _ => self.get_upper_bound_value(meta_prefix),
        };

        opt.set_prefix_same_as_start(true);
        opt_rev.set_prefix_same_as_start(true);

        let inner = self.meta.iterator_cf_opt(
            self.cf_hdr(area_idx),
            opt,
            IteratorMode::From(l, Direction::Forward),
        );

        let inner_rev = self.meta.iterator_cf_opt(
            self.cf_hdr(area_idx),
            opt_rev,
            IteratorMode::From(&h, Direction::Reverse),
        );

        RocksIter { inner, inner_rev }
    }

    fn get(
        &self,
        area_idx: usize,
        meta_prefix: PrefixBytes,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        self.meta.get_cf(self.cf_hdr(area_idx), k).unwrap()
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

        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }

        let old_v = self.meta.get_cf(self.cf_hdr(area_idx), &k).unwrap();
        self.meta.put_cf(self.cf_hdr(area_idx), k, value).unwrap();
        old_v
    }

    fn remove(
        &self,
        area_idx: usize,
        meta_prefix: PrefixBytes,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        let old_v = self.meta.get_cf(self.cf_hdr(area_idx), &k).unwrap();
        self.meta.delete_cf(self.cf_hdr(area_idx), k).unwrap();
        old_v
    }
}

pub struct RocksIter {
    inner: DBIterator<'static>,
    inner_rev: DBIterator<'static>,
}

impl Iterator for RocksIter {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(ik, iv)| (ik[PREFIX_SIZ..].to_vec(), iv.to_vec()))
    }
}

impl DoubleEndedIterator for RocksIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner_rev
            .next()
            .map(|(ik, iv)| (ik[PREFIX_SIZ..].to_vec(), iv.to_vec()))
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

    fn next(base: &[u8]) -> [u8; PREFIX_SIZ] {
        (crate::parse_prefix!(base) + 1).to_be_bytes()
    }
}

fn rocksdb_open() -> Result<(DB, Vec<String>)> {
    let dir = get_data_dir();

    let mut cfg = Options::default();
    cfg.create_if_missing(true);
    cfg.increase_parallelism(num_cpus::get() as i32);
    cfg.set_compression_type(DBCompressionType::Lz4);
    cfg.set_max_open_files(4096);
    cfg.set_allow_mmap_writes(true);
    cfg.set_allow_mmap_reads(true);
    cfg.create_missing_column_families(true);
    cfg.set_atomic_flush(true);
    cfg.set_prefix_extractor(SliceTransform::create_fixed_prefix(size_of::<Prefix>()));

    let cfhdrs = (0..DATA_SET_NUM).map(|i| i.to_string()).collect::<Vec<_>>();

    let cfs = cfhdrs
        .iter()
        .map(|i| ColumnFamilyDescriptor::new(i, cfg.clone()))
        .collect::<Vec<_>>();

    let db = DB::open_cf_descriptors(&cfg, &dir, cfs).c(d!())?;

    // avoid setting again on an opened DB
    info_omit!(vsdb_set_base_dir(dir));

    Ok((db, cfhdrs))
}
