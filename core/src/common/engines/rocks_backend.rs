use crate::common::{
    Engine, GB, MB, PREFIX_SIZE, Pre, PreBytes, RESERVED_ID_CNT, RawKey, RawValue,
    vsdb_get_base_dir, vsdb_set_base_dir,
};
use parking_lot::Mutex;
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, DBIterator, Direction,
    IteratorMode, Options, ReadOptions, SliceTransform,
};
use ruc::*;
use std::{
    borrow::Cow,
    fs,
    mem::size_of,
    ops::{Bound, RangeBounds},
    sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    thread::available_parallelism,
};

// NOTE:
// do NOT make the number of areas bigger than `u8::MAX`
const DATA_SET_NUM: usize = 2;

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

static HDR: LazyLock<(DB, Vec<String>)> = LazyLock::new(|| rocksdb_open().unwrap());

pub struct RocksEngine {
    meta: &'static DB,
    areas: Vec<&'static str>,
    prefix_allocator: PreAllocator,
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
}

impl Engine for RocksEngine {
    fn new() -> Result<Self> {
        let (meta, areas) =
            (&HDR.0, HDR.1.iter().map(|i| i.as_str()).collect::<Vec<_>>());

        let (prefix_allocator, initial_value) = PreAllocator::init();

        if meta.get(META_KEY_MAX_KEYLEN).c(d!())?.is_none() {
            meta.put(META_KEY_MAX_KEYLEN, 0_usize.to_be_bytes())
                .c(d!())?;
        }

        if meta.get(prefix_allocator.key).c(d!())?.is_none() {
            meta.put(prefix_allocator.key, initial_value).c(d!())?;
        }

        let max_keylen = AtomicUsize::new(crate::parse_int!(
            meta.get(META_KEY_MAX_KEYLEN).unwrap().unwrap(),
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

    // 'step 1' and 'step 2' is not atomic in multi-threads scene,
    // so we use a `Mutex` lock for thread safe.
    #[allow(unused_variables)]
    fn alloc_prefix(&self) -> Pre {
        static LK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
        let x = LK.lock();

        // step 1
        let ret = crate::parse_prefix!(
            self.meta.get(self.prefix_allocator.key).unwrap().unwrap()
        );

        // step 2
        self.meta
            .put(self.prefix_allocator.key, (1 + ret).to_be_bytes())
            .unwrap();

        ret
    }

    fn area_count(&self) -> usize {
        DATA_SET_NUM
    }

    fn flush(&self) {
        self.meta.flush().unwrap();
        (0..DATA_SET_NUM).for_each(|i| {
            self.meta.flush_cf(self.cf_hdr(i)).unwrap();
        });
    }

    fn iter(&self, meta_prefix: PreBytes) -> RocksIter {
        let area_idx = self.area_idx(meta_prefix);

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

    fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> RocksIter {
        let area_idx = self.area_idx(meta_prefix);

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
                b_lo.push(0u8);
                opt.set_iterate_lower_bound(b_lo.as_slice());
                opt_rev.set_iterate_lower_bound(b_lo.as_slice());
                b_lo.as_slice()
            }
            _ => meta_prefix.as_slice(),
        };

        let mut b_hi = meta_prefix.to_vec();
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.extend_from_slice(hi);
                b_hi.push(0u8);
                opt.set_iterate_upper_bound(b_hi.as_slice());
                opt_rev.set_iterate_upper_bound(b_hi.as_slice());
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

    fn get(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let area_idx = self.area_idx(meta_prefix);

        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        self.meta.get_cf(self.cf_hdr(area_idx), k).unwrap()
    }

    fn insert(
        &self,
        meta_prefix: PreBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<RawValue> {
        let area_idx = self.area_idx(meta_prefix);

        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);

        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }

        let old_v = self.meta.get_cf(self.cf_hdr(area_idx), &k).unwrap();
        self.meta.put_cf(self.cf_hdr(area_idx), k, value).unwrap();
        old_v
    }

    fn remove(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let area_idx = self.area_idx(meta_prefix);

        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        let old_v = self.meta.get_cf(self.cf_hdr(area_idx), &k).unwrap();
        self.meta.delete_cf(self.cf_hdr(area_idx), k).unwrap();
        old_v
    }

    fn get_instance_len_hint(&self, instance_prefix: PreBytes) -> u64 {
        crate::parse_int!(self.meta.get(instance_prefix).unwrap().unwrap(), u64)
    }

    fn set_instance_len_hint(&self, instance_prefix: PreBytes, new_len: u64) {
        self.meta
            .put(instance_prefix, new_len.to_be_bytes())
            .unwrap();
    }
}

pub struct RocksIter {
    inner: DBIterator<'static>,
    inner_rev: DBIterator<'static>,
}

impl Iterator for RocksIter {
    type Item = (RawKey, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|v| v.unwrap())
            .map(|(ik, iv)| (ik[PREFIX_SIZE..].to_vec(), iv.into_vec()))
    }
}

impl DoubleEndedIterator for RocksIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner_rev
            .next()
            .map(|v| v.unwrap())
            .map(|(ik, iv)| (ik[PREFIX_SIZE..].to_vec(), iv.into_vec()))
    }
}

// key of the prefix allocator in the 'meta'
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

fn rocksdb_open() -> Result<(DB, Vec<String>)> {
    let dir = vsdb_get_base_dir();

    // avoid setting again on an opened DB
    omit!(vsdb_set_base_dir(&dir));

    let mut cfg = Options::default();

    cfg.create_if_missing(true);
    cfg.create_missing_column_families(true);

    cfg.set_prefix_extractor(SliceTransform::create_fixed_prefix(size_of::<Pre>()));

    cfg.set_allow_mmap_writes(true);
    cfg.set_allow_mmap_reads(true);

    const WR_BUF_NUM: u8 = 2;
    const G: usize = GB as usize;

    cfg.set_min_write_buffer_number(WR_BUF_NUM as i32);
    cfg.set_max_write_buffer_number(1 + WR_BUF_NUM as i32);

    let wr_buffer_size = if cfg!(target_os = "linux") {
        let memsiz = fs::read_to_string("/proc/meminfo")
            .c(d!())?
            .lines()
            .find(|l| l.contains("MemAvailable"))
            .c(d!())?
            .replace(|ch: char| !ch.is_numeric(), "")
            .parse::<usize>()
            .c(d!())?
            * 1024;
        alt!((32 * G) < memsiz, 8 * G, G) / DATA_SET_NUM
    } else {
        G / DATA_SET_NUM
    };

    // println!(
    //     "[vsdb]: The `write_buffer_size` of rocksdb is {}MB, per column family",
    //     wr_buffer_size / MB as usize
    // );

    cfg.set_write_buffer_size(wr_buffer_size);

    cfg.set_enable_blob_files(true);
    cfg.set_enable_blob_gc(true);
    cfg.set_min_blob_size(MB);

    // // SEE: https://rocksdb.org/blog/2021/05/26/integrated-blob-db.html
    // cfg.set_blob_file_size(wr_buffer_size as u64);
    // cfg.set_target_file_size_base(wr_buffer_size as u64 / 10);
    // cfg.set_max_bytes_for_level_base(wr_buffer_size as u64);

    let parallelism = available_parallelism().c(d!())?.get() as i32;
    cfg.increase_parallelism(parallelism);

    #[cfg(feature = "compress")]
    cfg.set_compression_type(DBCompressionType::Zstd);

    #[cfg(not(feature = "compress"))]
    cfg.set_compression_type(DBCompressionType::None);

    let cfhdrs = (0..DATA_SET_NUM).map(|i| i.to_string()).collect::<Vec<_>>();

    let cfs = cfhdrs
        .iter()
        .map(|i| ColumnFamilyDescriptor::new(i, cfg.clone()))
        .collect::<Vec<_>>();

    let db = DB::open_cf_descriptors(&cfg, &dir, cfs).c(d!())?;

    Ok((db, cfhdrs))
}
