use crate::common::{
    BatchTrait, Engine, GB, MB, PREFIX_SIZE, Pre, PreBytes, RESERVED_ID_CNT, RawKey,
    RawValue, vsdb_get_base_dir, vsdb_set_base_dir,
};
use parking_lot::Mutex;
use rocksdb::{
    BlockBasedOptions, Cache, DB, DBIterator, Direction, IteratorMode, Options,
    ReadOptions, SliceTransform, WriteBatch,
};
use ruc::*;
use std::{
    borrow::Cow,
    cell::Cell,
    fs,
    mem::size_of,
    ops::{Bound, RangeBounds},
    path::Path,
    sync::{
        LazyLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread::available_parallelism,
};

const SHARD_CNT: usize = 1;

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

// Number of prefixes to reserve per alloc_prefix slow-path DB write.
// Larger values reduce lock contention at the cost of wasting prefix IDs on crash.
// With u64 prefix space this is negligible.
const PREFIX_ALLOC_BATCH: u64 = 8192;

pub struct RocksEngine {
    meta: &'static DB,
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

/// Compute the successor of a byte slice: the smallest byte string
/// that is strictly greater than `key`. Returns `None` if `key` is
/// all `0xFF` bytes (no finite successor exists).
fn successor(key: &[u8]) -> Option<Vec<u8>> {
    let mut s = key.to_vec();
    // Walk backwards, incrementing the first byte that isn't 0xFF
    for i in (0..s.len()).rev() {
        if s[i] < u8::MAX {
            s[i] += 1;
            s.truncate(i + 1);
            return Some(s);
        }
    }
    // All bytes are 0xFF — no finite successor
    None
}

impl RocksEngine {
    #[inline(always)]
    fn get_shard_idx(&self, _prefix: PreBytes) -> usize {
        0
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
        // Optimization: Check if update is needed
        let current = self.max_keylen.load(Ordering::Relaxed);
        if len > current {
            // SAFETY: Always persist to meta DB before updating memory to ensure consistency on crash.
            // Performance impact is acceptable as key length growth usually stabilizes quickly.
            self.meta
                .put(META_KEY_MAX_KEYLEN, len.to_be_bytes())
                .unwrap();
            self.max_keylen.store(len, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    fn get_upper_bound_value(&self, meta_prefix: PreBytes) -> Vec<u8> {
        const BUF: [u8; 256] = [u8::MAX; 256];

        let l = self.get_max_keylen();
        let total_len = PREFIX_SIZE + l;
        let mut max_guard = Vec::with_capacity(total_len);
        max_guard.extend_from_slice(&meta_prefix);

        if l < 257 {
            max_guard.extend_from_slice(&BUF[..l]);
        } else {
            max_guard.resize(total_len, u8::MAX);
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

        // Ensure base dir exists
        fs::create_dir_all(&base_dir).c(d!())?;

        for i in 0..SHARD_CNT {
            let dir = base_dir.join(format!("shard_{}", i));
            let db = rocksdb_open_shard(&dir)?;
            let db = Box::leak(Box::new(db));

            shards.push(db as &'static DB);
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
            prefix_allocator,
            // length of the raw key, exclude the meta prefix
            max_keylen,
        })
    }

    // Per-thread batch allocation to avoid cross-CCD atomic contention
    // on multi-CCD CPUs (e.g. EPYC 9474F).
    //
    // Each thread reserves a batch of PREFIX_ALLOC_BATCH prefixes from
    // the global counter, then hands them out locally with zero
    // cross-core contention. The global atomic is only touched once
    // per PREFIX_ALLOC_BATCH allocations per thread.
    #[allow(unused_variables)]
    fn alloc_prefix(&self) -> Pre {
        thread_local! {
            static LOCAL_NEXT: Cell<u64> = const { Cell::new(0) };
            static LOCAL_CEIL: Cell<u64> = const { Cell::new(0) };
        }

        LOCAL_NEXT.with(|next_cell| {
            LOCAL_CEIL.with(|ceil_cell| {
                let next = next_cell.get();
                let ceil = ceil_cell.get();
                if next > 0 && next < ceil {
                    next_cell.set(next + 1);
                    return next;
                }

                // Slow path: reserve a new batch from the global counter
                static GLOBAL_COUNTER: LazyLock<AtomicU64> =
                    LazyLock::new(|| AtomicU64::new(0));
                static GLOBAL_CEILING: LazyLock<AtomicU64> =
                    LazyLock::new(|| AtomicU64::new(0));
                static LK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

                let gc = GLOBAL_COUNTER.load(Ordering::Relaxed);
                if gc == 0 {
                    // First-time initialization from DB
                    let _x = LK.lock();
                    if GLOBAL_COUNTER.load(Ordering::Relaxed) == 0 {
                        let ret = crate::parse_prefix!(
                            self.meta.get(self.prefix_allocator.key).unwrap().unwrap()
                        );
                        let new_ceil = ret + PREFIX_ALLOC_BATCH;
                        self.meta
                            .put(self.prefix_allocator.key, new_ceil.to_be_bytes())
                            .unwrap();
                        GLOBAL_COUNTER.store(ret, Ordering::Release);
                        GLOBAL_CEILING.store(new_ceil, Ordering::Release);
                    }
                }

                // Reserve a thread-local batch from the global
                // counter. This is the only cross-CCD atomic RMW
                // and it happens once per PREFIX_ALLOC_BATCH
                // allocations per thread.
                let batch_start =
                    GLOBAL_COUNTER.fetch_add(PREFIX_ALLOC_BATCH, Ordering::AcqRel);
                let batch_end = batch_start + PREFIX_ALLOC_BATCH;

                // If we've exceeded the persisted ceiling, extend it
                let old_ceil = GLOBAL_CEILING.load(Ordering::Acquire);
                if batch_end > old_ceil {
                    let _x = LK.lock();
                    let old_ceil2 = GLOBAL_CEILING.load(Ordering::Acquire);
                    if batch_end > old_ceil2 {
                        let new_ceil = batch_end + PREFIX_ALLOC_BATCH;
                        self.meta
                            .put(self.prefix_allocator.key, new_ceil.to_be_bytes())
                            .unwrap();
                        GLOBAL_CEILING.store(new_ceil, Ordering::Release);
                    }
                }

                next_cell.set(batch_start + 1);
                ceil_cell.set(batch_end);
                batch_start
            })
        })
    }

    fn flush(&self) {
        for db in self.shards.iter() {
            db.flush().unwrap();
        }
    }

    fn iter(&self, meta_prefix: PreBytes) -> RocksIter {
        let db = self.get_db(meta_prefix);

        let inner = db.prefix_iterator(meta_prefix);

        let mut opt = ReadOptions::default();
        opt.set_prefix_same_as_start(true);

        let inner_rev = db.iterator_opt(
            IteratorMode::From(
                &self.get_upper_bound_value(meta_prefix),
                Direction::Reverse,
            ),
            opt,
        );

        RocksIter { inner, inner_rev }
    }

    fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> RocksIter {
        let db = self.get_db(meta_prefix);

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

        // RocksDB upper bound is exclusive.
        // For Included(hi): compute successor(prefix + hi) so that hi itself is included.
        // For Excluded(hi): use prefix + hi directly as the exclusive upper bound.
        let mut b_hi = meta_prefix.to_vec();
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.extend_from_slice(hi);
                // Compute the successor of the full key so that
                // the upper bound is strictly past `hi`.
                let upper = match successor(&b_hi) {
                    Some(s) => s,
                    // All 0xFF: fall back to the max guard value
                    None => self.get_upper_bound_value(meta_prefix),
                };
                opt.set_iterate_upper_bound(upper.as_slice());
                opt_rev.set_iterate_upper_bound(upper.as_slice());
                upper
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

        let inner = db.iterator_opt(IteratorMode::From(l, Direction::Forward), opt);

        let inner_rev =
            db.iterator_opt(IteratorMode::From(&h, Direction::Reverse), opt_rev);

        RocksIter { inner, inner_rev }
    }

    fn get(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let db = self.get_db(meta_prefix);

        // Optimization: Use helper function with pre-allocated capacity
        let full_key = make_full_key(meta_prefix.as_slice(), key);
        db.get(full_key).unwrap()
    }

    fn insert(&self, meta_prefix: PreBytes, key: &[u8], value: &[u8]) {
        let db = self.get_db(meta_prefix);

        // Optimization: Check and update max_keylen with reduced frequency
        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }

        // Optimization: Use helper function with pre-allocated capacity
        let full_key = make_full_key(meta_prefix.as_slice(), key);

        // Direct insert without read-before-write - major performance improvement
        db.put(full_key, value).unwrap();
    }

    fn remove(&self, meta_prefix: PreBytes, key: &[u8]) {
        let db = self.get_db(meta_prefix);

        // Optimization: Use helper function with pre-allocated capacity
        let full_key = make_full_key(meta_prefix.as_slice(), key);

        // Direct remove without read-before-write - major performance improvement
        db.delete(full_key).unwrap();
    }

    fn batch_begin<'a>(&'a self, meta_prefix: PreBytes) -> Box<dyn BatchTrait + 'a> {
        Box::new(RocksBatch::new(meta_prefix, self))
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
}

/// Batch write operations for RocksDB
pub struct RocksBatch<'a> {
    inner: WriteBatch,
    meta_prefix: PreBytes,
    max_key_len: usize,
    engine: &'a RocksEngine,
}

impl<'a> RocksBatch<'a> {
    fn new(meta_prefix: PreBytes, engine: &'a RocksEngine) -> Self {
        Self {
            inner: WriteBatch::default(),
            meta_prefix,
            max_key_len: 0,
            engine,
        }
    }

    /// Insert a key-value pair in this batch
    #[inline(always)]
    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        let full_key = make_full_key(self.meta_prefix.as_slice(), key);
        self.inner.put(full_key, value);
        if key.len() > self.max_key_len {
            self.max_key_len = key.len();
        }
    }

    /// Remove a key in this batch
    #[inline(always)]
    pub fn remove(&mut self, key: &[u8]) {
        let full_key = make_full_key(self.meta_prefix.as_slice(), key);
        self.inner.delete(full_key);
    }
}

impl BatchTrait for RocksBatch<'_> {
    #[inline(always)]
    fn insert(&mut self, key: &[u8], value: &[u8]) {
        self.insert(key, value);
    }

    #[inline(always)]
    fn remove(&mut self, key: &[u8]) {
        self.remove(key);
    }

    #[inline(always)]
    fn commit(&mut self) -> Result<()> {
        let db = self.engine.get_db(self.meta_prefix);
        let batch = std::mem::take(&mut self.inner);
        db.write(batch).c(d!())?;

        if self.max_key_len > 0 && self.max_key_len > self.engine.get_max_keylen() {
            self.engine.set_max_key_len(self.max_key_len);
        }

        Ok(())
    }
}

fn rocksdb_open_shard(dir: &Path) -> Result<DB> {
    let mut cfg = Options::default();

    cfg.create_if_missing(true);

    cfg.set_prefix_extractor(SliceTransform::create_fixed_prefix(size_of::<Pre>()));

    cfg.set_allow_mmap_writes(true);
    cfg.set_allow_mmap_reads(true);

    // ---- Write buffer ----
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
        alt!((16 * G) < memsiz, memsiz / 4, G) / SHARD_CNT
    } else {
        G / SHARD_CNT
    };

    cfg.set_write_buffer_size(wr_buffer_size);

    // ---- Block cache + Bloom filter ----
    // Block cache: use ~1/8 of available memory, shared across all CFs in this shard
    let block_cache_size = if cfg!(target_os = "linux") {
        let memsiz = fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.contains("MemAvailable"))
                    .and_then(|l| {
                        l.replace(|ch: char| !ch.is_numeric(), "")
                            .parse::<usize>()
                            .ok()
                    })
            })
            .unwrap_or(G)
            * 1024;
        memsiz / 8 / SHARD_CNT
    } else {
        128 * MB as usize // 128MB fallback per shard
    };
    let cache = Cache::new_lru_cache(block_cache_size);

    let mut table_opts = BlockBasedOptions::default();
    table_opts.set_block_cache(&cache);
    // Bloom filter: 10 bits/key, ~1% false positive rate
    table_opts.set_bloom_filter(10.0, false);
    // Pin index/filter blocks in cache to avoid re-reading from disk
    table_opts.set_cache_index_and_filter_blocks(true);
    table_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    cfg.set_block_based_table_factory(&table_opts);

    // ---- Memtable bloom for faster prefix lookups ----
    cfg.set_memtable_prefix_bloom_ratio(0.02);

    // ---- Compaction tuning ----
    cfg.set_level_compaction_dynamic_level_bytes(true);
    // Delay L0 compaction trigger for write-heavy workloads
    cfg.set_level_zero_file_num_compaction_trigger(8);

    // ---- Parallelism ----
    let parallelism = available_parallelism().c(d!())?.get() as i32;
    cfg.increase_parallelism(parallelism);

    let db = DB::open(&cfg, dir).c(d!())?;

    Ok(db)
}
