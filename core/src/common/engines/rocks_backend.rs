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
    path::Path,
    sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    thread::available_parallelism,
};

// NOTE:
// do NOT make the number of areas bigger than `u8::MAX`
const DATA_SET_NUM: usize = 2;
const SHARD_CNT: usize = 16;

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

pub struct RocksEngine {
    meta: &'static DB,
    shards: Vec<&'static DB>,
    shards_cfs: Vec<Vec<&'static ColumnFamily>>,
    prefix_allocator: PreAllocator,
    max_keylen: AtomicUsize,
}

impl RocksEngine {
    #[inline(always)]
    fn get_shard_idx(&self, prefix: PreBytes) -> usize {
        (prefix[0] as usize) % SHARD_CNT
    }

    #[inline(always)]
    fn get_cf(&self, prefix: PreBytes) -> &'static ColumnFamily {
        let shard_idx = self.get_shard_idx(prefix);
        // Reuse the `area_idx` logic from Engine trait which defaults to prefix[0] % area_count()
        let cf_idx = self.area_idx(prefix);
        self.shards_cfs[shard_idx][cf_idx]
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
        let base_dir = vsdb_get_base_dir();
        // avoid setting again on an opened DB
        omit!(vsdb_set_base_dir(&base_dir));

        let mut shards = Vec::with_capacity(SHARD_CNT);
        let mut shards_cfs = Vec::with_capacity(SHARD_CNT);

        // Ensure base dir exists
        fs::create_dir_all(&base_dir).c(d!())?;

        for i in 0..SHARD_CNT {
            let dir = base_dir.join(format!("shard_{}", i));
            let (db, cf_names) = rocksdb_open_shard(&dir)?;
            let db = Box::leak(Box::new(db));

            let cfs = cf_names
                .iter()
                .map(|name| db.cf_handle(name).unwrap())
                .collect::<Vec<_>>();

            shards.push(db as &'static DB);
            shards_cfs.push(cfs);
        }

        // Use shard 0 as the meta shard
        let meta = shards[0];

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
            shards,
            shards_cfs,
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
        for (i, db) in self.shards.iter().enumerate() {
            db.flush().unwrap();
            for cf in &self.shards_cfs[i] {
                db.flush_cf(cf).unwrap();
            }
        }
    }

    fn iter(&self, meta_prefix: PreBytes) -> RocksIter {
        let db = self.get_db(meta_prefix);
        let cf = self.get_cf(meta_prefix);

        let inner = db.prefix_iterator_cf(cf, meta_prefix);

        let mut opt = ReadOptions::default();
        opt.set_prefix_same_as_start(true);

        let inner_rev = db.iterator_cf_opt(
            cf,
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
        let db = self.get_db(meta_prefix);
        let cf = self.get_cf(meta_prefix);

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

        let inner =
            db.iterator_cf_opt(cf, opt, IteratorMode::From(l, Direction::Forward));

        let inner_rev =
            db.iterator_cf_opt(cf, opt_rev, IteratorMode::From(&h, Direction::Reverse));

        RocksIter { inner, inner_rev }
    }

    fn get(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let db = self.get_db(meta_prefix);
        let cf = self.get_cf(meta_prefix);

        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        db.get_cf(cf, k).unwrap()
    }

    fn insert(
        &self,
        meta_prefix: PreBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<RawValue> {
        let db = self.get_db(meta_prefix);
        let cf = self.get_cf(meta_prefix);

        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);

        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }

        let old_v = db.get_cf(cf, &k).unwrap();
        db.put_cf(cf, k, value).unwrap();
        old_v
    }

    fn remove(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let db = self.get_db(meta_prefix);
        let cf = self.get_cf(meta_prefix);

        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        let old_v = db.get_cf(cf, &k).unwrap();
        db.delete_cf(cf, k).unwrap();
        old_v
    }

    fn get_instance_len_hint(&self, instance_prefix: PreBytes) -> u64 {
        self.meta
            .get(instance_prefix)
            .unwrap()
            .map(|v| crate::parse_int!(v, u64))
            .unwrap_or(0)
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
        self.inner.next().map(|v| v.unwrap()).map(|(ik, iv)| {
            let mut k = ik.into_vec();
            k.drain(..PREFIX_SIZE);
            (k, iv.into_vec())
        })
    }
}

impl DoubleEndedIterator for RocksIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner_rev.next().map(|v| v.unwrap()).map(|(ik, iv)| {
            let mut k = ik.into_vec();
            k.drain(..PREFIX_SIZE);
            (k, iv.into_vec())
        })
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

fn rocksdb_open_shard(dir: &Path) -> Result<(DB, Vec<String>)> {
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
        alt!((16 * G) < memsiz, memsiz / 4, G) / (DATA_SET_NUM * SHARD_CNT)
    } else {
        G / (DATA_SET_NUM * SHARD_CNT)
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

    let db = DB::open_cf_descriptors(&cfg, dir, cfs).c(d!())?;

    Ok((db, cfhdrs))
}
