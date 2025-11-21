use crate::common::{
    BatchTrait, GB, MB, PREFIX_SIZE, Pre, PreBytes, RESERVED_ID_CNT, RawKey, RawValue,
    vsdb_get_base_dir, vsdb_set_base_dir,
};
use parking_lot::Mutex;
use rocksdb::{
    BlockBasedOptions, Cache, DB, DBCompressionType, DBIterator, Direction,
    IteratorMode, Options, ReadOptions, SliceTransform, WriteBatch,
};
use ruc::*;
use std::{
    borrow::Cow,
    cell::Cell,
    cmp, fs,
    mem::size_of,
    ops::{Bound, RangeBounds},
    path::Path,
    sync::{
        LazyLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread::available_parallelism,
};

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

// Number of prefixes to reserve per alloc_prefix slow-path DB write.
// Larger values reduce lock contention at the cost of wasting prefix IDs on crash.
// With u64 prefix space this is negligible.
const PREFIX_ALLOC_BATCH: u64 = 8192;

// Stack threshold for FullKey: PREFIX_SIZE (8) + up to 56 bytes of user key.
const FULL_KEY_STACK_CAP: usize = 64;

/// A key that lives on the stack when short enough, heap otherwise.
enum FullKey {
    Stack {
        buf: [u8; FULL_KEY_STACK_CAP],
        len: usize,
    },
    Heap(Vec<u8>),
}

impl AsRef<[u8]> for FullKey {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        match self {
            FullKey::Stack { buf, len } => &buf[..*len],
            FullKey::Heap(v) => v.as_slice(),
        }
    }
}

// Optimization: Helper function to build full key, stack-allocated when possible
#[inline(always)]
fn make_full_key(meta_prefix: &[u8], key: &[u8]) -> FullKey {
    let total_len = meta_prefix.len() + key.len();
    if total_len <= FULL_KEY_STACK_CAP {
        let mut buf = [0u8; FULL_KEY_STACK_CAP];
        buf[..meta_prefix.len()].copy_from_slice(meta_prefix);
        buf[meta_prefix.len()..total_len].copy_from_slice(key);
        FullKey::Stack {
            buf,
            len: total_len,
        }
    } else {
        let mut v = Vec::with_capacity(total_len);
        v.extend_from_slice(meta_prefix);
        v.extend_from_slice(key);
        FullKey::Heap(v)
    }
}

pub struct RocksEngine {
    db: &'static DB,
    prefix_allocator: PreAllocator,
    max_keylen: AtomicUsize,
}

#[cfg(test)]
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
    fn get_max_keylen(&self) -> usize {
        self.max_keylen.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn set_max_key_len(&self, len: usize) {
        let prev = self.max_keylen.fetch_max(len, Ordering::Relaxed);
        if len > prev {
            self.db
                .put(META_KEY_MAX_KEYLEN, len.to_be_bytes())
                .expect("vsdb: meta write failed");
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

impl RocksEngine {
    pub(crate) fn new() -> Result<Self> {
        let base_dir = vsdb_get_base_dir();
        // avoid setting again on an opened DB
        omit!(vsdb_set_base_dir(&base_dir));

        // Ensure base dir exists
        fs::create_dir_all(&base_dir).c(d!())?;

        // Keep directory name `shard_0` for backward compatibility.
        let dir = base_dir.join("shard_0");
        let db = rocksdb_open_shard(&dir)?;
        let db: &'static DB = Box::leak(Box::new(db));

        let (prefix_allocator, initial_value) = PreAllocator::init();

        if db.get(META_KEY_MAX_KEYLEN).c(d!())?.is_none() {
            db.put(META_KEY_MAX_KEYLEN, 0_usize.to_be_bytes()).c(d!())?;
        }

        if db.get(prefix_allocator.key).c(d!())?.is_none() {
            db.put(prefix_allocator.key, initial_value).c(d!())?;
        }

        let max_keylen = AtomicUsize::new(crate::parse_int!(
            db.get(META_KEY_MAX_KEYLEN).unwrap().unwrap(),
            usize
        ));

        Ok(RocksEngine {
            db,
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
    //
    // NOTE: The static GLOBAL_COUNTER / GLOBAL_CEILING / LK variables
    // are process-global. This is correct as long as only a single
    // RocksEngine instance exists (enforced by the LazyLock<VsDB<..>>
    // singleton in common/mod.rs). Creating multiple RocksEngine
    // instances in the same process would cause prefix collisions.
    #[allow(unused_variables)]
    pub(crate) fn alloc_prefix(&self) -> Pre {
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
                            self.db
                                .get(self.prefix_allocator.key)
                                .expect("vsdb: meta read failed")
                                .unwrap()
                        );
                        let new_ceil = ret + PREFIX_ALLOC_BATCH;
                        self.db
                            .put(self.prefix_allocator.key, new_ceil.to_be_bytes())
                            .expect("vsdb: meta write failed");
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
                        self.db
                            .put(self.prefix_allocator.key, new_ceil.to_be_bytes())
                            .expect("vsdb: meta write failed");
                        GLOBAL_CEILING.store(new_ceil, Ordering::Release);
                    }
                }

                next_cell.set(batch_start + 1);
                ceil_cell.set(batch_end);
                batch_start
            })
        })
    }

    pub(crate) fn flush(&self) {
        self.db.flush().expect("vsdb: rocksdb flush failed");
    }

    pub(crate) fn iter(&self, meta_prefix: PreBytes) -> RocksIter {
        let inner = self.db.prefix_iterator(meta_prefix);

        let mut opt = ReadOptions::default();
        opt.set_prefix_same_as_start(true);

        let inner_rev = self.db.iterator_opt(
            IteratorMode::From(
                &self.get_upper_bound_value(meta_prefix),
                Direction::Reverse,
            ),
            opt,
        );

        RocksIter {
            inner,
            inner_rev,
            prefix: meta_prefix,
            range: (Bound::Unbounded, Bound::Unbounded),
            done: false,
            last_fwd_full_key: Vec::new(),
            last_rev_full_key: Vec::new(),
        }
    }

    pub(crate) fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        meta_prefix: PreBytes,
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
                b_lo.push(0u8);
                opt.set_iterate_lower_bound(b_lo.as_slice());
                opt_rev.set_iterate_lower_bound(b_lo.as_slice());
                b_lo.as_slice()
            }
            _ => meta_prefix.as_slice(),
        };

        // Range bounds are enforced in RocksIter itself to correctly handle
        // variable-length keys (e.g. Included("ab") must NOT include "ab\0").
        // Iterator bounds are only used to constrain iteration to this prefix.
        let prefix_upper = self.get_upper_bound_value(meta_prefix);
        opt.set_iterate_upper_bound(prefix_upper.as_slice());
        opt_rev.set_iterate_upper_bound(prefix_upper.as_slice());

        // Full-key bounds for in-iterator checks (prefix + user key).
        let lo_full: Bound<Vec<u8>> = match bounds.start_bound() {
            Bound::Included(lo) => {
                let mut v = meta_prefix.to_vec();
                v.extend_from_slice(lo);
                Bound::Included(v)
            }
            Bound::Excluded(lo) => {
                let mut v = meta_prefix.to_vec();
                v.extend_from_slice(lo);
                Bound::Excluded(v)
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let hi_full: Bound<Vec<u8>> = match bounds.end_bound() {
            Bound::Included(hi) => {
                let mut v = meta_prefix.to_vec();
                v.extend_from_slice(hi);
                Bound::Included(v)
            }
            Bound::Excluded(hi) => {
                let mut v = meta_prefix.to_vec();
                v.extend_from_slice(hi);
                Bound::Excluded(v)
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        opt.set_prefix_same_as_start(true);
        opt_rev.set_prefix_same_as_start(true);

        let inner = self
            .db
            .iterator_opt(IteratorMode::From(l, Direction::Forward), opt);

        // For reverse, start from the end bound if present; otherwise from the prefix upper.
        let rev_seek = match bounds.end_bound() {
            Bound::Included(hi) | Bound::Excluded(hi) => {
                make_full_key(meta_prefix.as_slice(), hi)
            }
            Bound::Unbounded => {
                let v = prefix_upper.clone();
                FullKey::Heap(v)
            }
        };

        let inner_rev = self.db.iterator_opt(
            IteratorMode::From(rev_seek.as_ref(), Direction::Reverse),
            opt_rev,
        );

        RocksIter {
            inner,
            inner_rev,
            prefix: meta_prefix,
            range: (lo_full, hi_full),
            done: false,
            last_fwd_full_key: Vec::new(),
            last_rev_full_key: Vec::new(),
        }
    }

    pub(crate) fn get(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let full_key = make_full_key(meta_prefix.as_slice(), key);
        self.db
            .get(full_key.as_ref())
            .expect("vsdb: rocksdb get failed")
    }

    pub(crate) fn insert(&self, meta_prefix: PreBytes, key: &[u8], value: &[u8]) {
        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }

        let full_key = make_full_key(meta_prefix.as_slice(), key);
        self.db
            .put(full_key.as_ref(), value)
            .expect("vsdb: rocksdb put failed");
    }

    pub(crate) fn remove(&self, meta_prefix: PreBytes, key: &[u8]) {
        let full_key = make_full_key(meta_prefix.as_slice(), key);
        self.db
            .delete(full_key.as_ref())
            .expect("vsdb: rocksdb delete failed");
    }

    pub(crate) fn batch_begin<'a>(
        &'a self,
        meta_prefix: PreBytes,
    ) -> Box<dyn BatchTrait + 'a> {
        Box::new(RocksBatch::new(meta_prefix, self))
    }
}

pub struct RocksIter {
    inner: DBIterator<'static>,
    inner_rev: DBIterator<'static>,
    prefix: PreBytes,
    range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    done: bool,
    // Track the last full keys (prefix + user key) returned by
    // forward/reverse iteration to detect cursor overlap and
    // prevent returning duplicate entries when next() and
    // next_back() are interleaved.
    //
    // Empty Vec means no element has been yielded yet from that direction.
    // Reuses heap allocation across calls via clear() + extend_from_slice().
    last_fwd_full_key: Vec<u8>,
    last_rev_full_key: Vec<u8>,
}

impl RocksIter {
    #[inline(always)]
    fn check_upper_bound(&self, full_key: &[u8]) -> bool {
        match &self.range.1 {
            Bound::Unbounded => true,
            Bound::Included(u) => full_key <= u.as_slice(),
            Bound::Excluded(u) => full_key < u.as_slice(),
        }
    }

    #[inline(always)]
    fn check_lower_bound(&self, full_key: &[u8]) -> bool {
        match &self.range.0 {
            Bound::Unbounded => true,
            Bound::Included(l) => full_key >= l.as_slice(),
            Bound::Excluded(l) => full_key > l.as_slice(),
        }
    }

    #[inline(always)]
    fn cursors_crossed(&self, fwd_full_key: &[u8]) -> bool {
        if !self.last_rev_full_key.is_empty() {
            fwd_full_key >= self.last_rev_full_key.as_slice()
        } else {
            false
        }
    }

    #[inline(always)]
    fn cursors_crossed_rev(&self, rev_full_key: &[u8]) -> bool {
        if !self.last_fwd_full_key.is_empty() {
            rev_full_key <= self.last_fwd_full_key.as_slice()
        } else {
            false
        }
    }
}

impl Iterator for RocksIter {
    type Item = (RawKey, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let Some((ik, iv)) = self.inner.next().map(|v| v.unwrap()) else {
                self.done = true;
                return None;
            };

            if !ik.as_ref().starts_with(self.prefix.as_slice()) {
                self.done = true;
                return None;
            }

            if !self.check_lower_bound(ik.as_ref()) {
                continue;
            }

            if !self.check_upper_bound(ik.as_ref()) {
                self.done = true;
                return None;
            }

            if self.cursors_crossed(ik.as_ref()) {
                self.done = true;
                return None;
            }

            self.last_fwd_full_key.clear();
            self.last_fwd_full_key.extend_from_slice(&ik);

            let mut k = ik.into_vec();
            k.drain(..PREFIX_SIZE);
            return Some((k, iv.into_vec()));
        }
    }
}

impl DoubleEndedIterator for RocksIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let Some((ik, iv)) = self.inner_rev.next().map(|v| v.unwrap()) else {
                self.done = true;
                return None;
            };

            if !ik.as_ref().starts_with(self.prefix.as_slice()) {
                self.done = true;
                return None;
            }

            if !self.check_upper_bound(ik.as_ref()) {
                continue;
            }

            if !self.check_lower_bound(ik.as_ref()) {
                self.done = true;
                return None;
            }

            if self.cursors_crossed_rev(ik.as_ref()) {
                self.done = true;
                return None;
            }

            self.last_rev_full_key.clear();
            self.last_rev_full_key.extend_from_slice(&ik);

            let mut k = ik.into_vec();
            k.drain(..PREFIX_SIZE);
            return Some((k, iv.into_vec()));
        }
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
        self.inner.put(full_key.as_ref(), value);
        if key.len() > self.max_key_len {
            self.max_key_len = key.len();
        }
    }

    /// Remove a key in this batch
    #[inline(always)]
    pub fn remove(&mut self, key: &[u8]) {
        let full_key = make_full_key(self.meta_prefix.as_slice(), key);
        self.inner.delete(full_key.as_ref());
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
        let batch = std::mem::take(&mut self.inner);
        self.engine.db.write(batch).c(d!())?;

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

    cfg.set_allow_mmap_reads(true);

    // ---- Per-level compression: LZ4 for L0-L1, ZSTD for L2+ ----
    cfg.set_compression_per_level(&[
        DBCompressionType::Lz4,  // L0
        DBCompressionType::Lz4,  // L1
        DBCompressionType::Zstd, // L2
        DBCompressionType::Zstd, // L3
        DBCompressionType::Zstd, // L4
        DBCompressionType::Zstd, // L5
        DBCompressionType::Zstd, // L6
    ]);

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
        alt!((16 * G) < memsiz, memsiz / 4, G)
    } else {
        G
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
        memsiz / 8
    } else {
        128 * MB as usize // 128MB fallback
    };
    let cache = Cache::new_lru_cache(block_cache_size);

    let mut table_opts = BlockBasedOptions::default();
    table_opts.set_block_cache(&cache);
    table_opts.set_block_size(16 * 1024); // 16 KB — better for SSD (fewer seeks, better compression)
    // Bloom filter: 10 bits/key, ~1% false positive rate
    table_opts.set_bloom_filter(10.0, false);
    // Pin index/filter blocks in cache to avoid re-reading from disk
    table_opts.set_cache_index_and_filter_blocks(true);
    table_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    cfg.set_block_based_table_factory(&table_opts);

    // ---- Memtable bloom for faster prefix lookups ----
    cfg.set_memtable_prefix_bloom_ratio(0.1);

    // ---- Compaction tuning ----
    cfg.set_level_compaction_dynamic_level_bytes(true);
    // Delay L0 compaction trigger for write-heavy workloads
    cfg.set_level_zero_file_num_compaction_trigger(8);

    // ---- Parallelism (capped for many-core EPYC) ----
    let parallelism = cmp::min(available_parallelism().c(d!())?.get(), 8) as i32;
    cfg.increase_parallelism(parallelism);

    let db = DB::open(&cfg, dir).c(d!())?;

    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir(tag: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vsdb-rocks-{tag}-{nanos}"))
    }

    #[test]
    fn rocks_range_included_end_excludes_longer_keys() {
        let dir = tmp_dir("range-included");
        let db = rocksdb_open_shard(&dir).unwrap();
        let db: &'static DB = Box::leak(Box::new(db));

        let prefix: PreBytes = 7_u64.to_be_bytes();

        let fk_ab = make_full_key(prefix.as_slice(), b"ab");
        let fk_ab0 = make_full_key(prefix.as_slice(), b"ab\0");
        let fk_ac = make_full_key(prefix.as_slice(), b"ac");

        db.put(fk_ab.as_ref(), b"1").unwrap();
        db.put(fk_ab0.as_ref(), b"2").unwrap();
        db.put(fk_ac.as_ref(), b"3").unwrap();

        // Constrain iterator to this prefix only (exclusive upper bound is successor(prefix)).
        let mut opt = ReadOptions::default();
        let mut opt_rev = ReadOptions::default();

        opt.set_prefix_same_as_start(true);
        opt_rev.set_prefix_same_as_start(true);

        let prefix_upper = successor(prefix.as_slice())
            .expect("fixed-size prefix always has successor");
        opt.set_iterate_upper_bound(prefix_upper.as_slice());
        opt_rev.set_iterate_upper_bound(prefix_upper.as_slice());

        let inner = db.iterator_opt(
            IteratorMode::From(prefix.as_slice(), Direction::Forward),
            opt,
        );

        let inner_rev = db.iterator_opt(
            IteratorMode::From(fk_ab.as_ref(), Direction::Reverse),
            opt_rev,
        );

        let mut it = RocksIter {
            inner,
            inner_rev,
            prefix,
            range: (Bound::Unbounded, Bound::Included(fk_ab.as_ref().to_vec())),
            done: false,
            last_fwd_full_key: Vec::new(),
            last_rev_full_key: Vec::new(),
        };

        let collected = it.collect::<Vec<_>>();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].0, b"ab".to_vec());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
