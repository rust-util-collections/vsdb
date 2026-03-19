use crate::common::{
    BatchTrait, GB, PREFIX_SIZE, Pre, PreBytes, RESERVED_ID_CNT, RawKey, RawValue,
    vsdb_get_base_dir, vsdb_set_base_dir,
};
use mmdb::{BidiIterator, CompressionType, DB, DbOptions, WriteBatch};
use parking_lot::Mutex;
use ruc::*;
use std::{
    borrow::Cow,
    cell::Cell,
    cmp, fs,
    ops::{Bound, RangeBounds},
    sync::{
        LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    thread::available_parallelism,
};

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

const PREFIX_ALLOC_BATCH: u64 = 8192;

pub struct MmDB {
    db: &'static DB,
    prefix_allocator: PreAllocator,
}

impl MmDB {
    pub(crate) fn new() -> Result<Self> {
        let base_dir = vsdb_get_base_dir();
        omit!(vsdb_set_base_dir(&base_dir));

        fs::create_dir_all(&base_dir).c(d!())?;

        let dir = base_dir.join("mmdb");
        fs::create_dir_all(&dir).c(d!())?;

        let db = mmdb_open(&dir)?;
        let db: &'static DB = Box::leak(Box::new(db));

        let (prefix_allocator, initial_value) = PreAllocator::init();

        if db.get(&META_KEY_MAX_KEYLEN).c(d!())?.is_none() {
            db.put(&META_KEY_MAX_KEYLEN, &0_usize.to_be_bytes())
                .c(d!())?;
        }

        if db.get(&prefix_allocator.key).c(d!())?.is_none() {
            db.put(&prefix_allocator.key, &initial_value).c(d!())?;
        }

        Ok(MmDB {
            db,
            prefix_allocator,
        })
    }

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

                static GLOBAL_COUNTER: LazyLock<AtomicU64> =
                    LazyLock::new(|| AtomicU64::new(0));
                static GLOBAL_CEILING: LazyLock<AtomicU64> =
                    LazyLock::new(|| AtomicU64::new(0));
                static LK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

                let gc = GLOBAL_COUNTER.load(Ordering::Relaxed);
                if gc == 0 {
                    let _x = LK.lock();
                    if GLOBAL_COUNTER.load(Ordering::Relaxed) == 0 {
                        let ret = crate::parse_prefix!(
                            self.db
                                .get(&self.prefix_allocator.key)
                                .expect("vsdb: meta read failed")
                                .unwrap()
                        );
                        let new_ceil = ret + PREFIX_ALLOC_BATCH;
                        self.db
                            .put(&self.prefix_allocator.key, &new_ceil.to_be_bytes())
                            .expect("vsdb: meta write failed");
                        GLOBAL_COUNTER.store(ret, Ordering::Release);
                        GLOBAL_CEILING.store(new_ceil, Ordering::Release);
                    }
                }

                let batch_start =
                    GLOBAL_COUNTER.fetch_add(PREFIX_ALLOC_BATCH, Ordering::AcqRel);
                let batch_end = batch_start + PREFIX_ALLOC_BATCH;

                let old_ceil = GLOBAL_CEILING.load(Ordering::Acquire);
                if batch_end > old_ceil {
                    let _x = LK.lock();
                    let old_ceil2 = GLOBAL_CEILING.load(Ordering::Acquire);
                    if batch_end > old_ceil2 {
                        let new_ceil = batch_end + PREFIX_ALLOC_BATCH;
                        self.db
                            .put(&self.prefix_allocator.key, &new_ceil.to_be_bytes())
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
        self.db.flush().expect("vsdb: mmdb flush failed");
    }

    pub(crate) fn get(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let full_key = make_full_key(&meta_prefix, key);
        self.db.get(&full_key).expect("vsdb: mmdb get failed")
    }

    pub(crate) fn insert(&self, meta_prefix: PreBytes, key: &[u8], value: &[u8]) {
        let full_key = make_full_key(&meta_prefix, key);
        self.db
            .put(&full_key, value)
            .expect("vsdb: mmdb put failed");
    }

    pub(crate) fn remove(&self, meta_prefix: PreBytes, key: &[u8]) {
        let full_key = make_full_key(&meta_prefix, key);
        self.db.delete(&full_key).expect("vsdb: mmdb delete failed");
    }

    pub(crate) fn iter(&self, meta_prefix: PreBytes) -> MmdbIter {
        // iter_with_prefix uses prefix bloom filters to skip entire SST files that
        // don't contain the prefix, and seeks directly to the prefix start.
        // The returned BidiIterator is fully lazy (streaming, O(1) memory per step).
        let db_iter = self
            .db
            .iter_with_prefix(&meta_prefix, &mmdb::ReadOptions::default())
            .expect("vsdb: mmdb iter_with_prefix failed");
        let iter =
            BidiIterator::lazy(db_iter).map(|(k, v)| (k[PREFIX_SIZE..].to_vec(), v));
        MmdbIter(Box::new(iter))
    }

    pub(crate) fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> MmdbIter {
        let prefixed = |b: &Bound<&Cow<'_, [u8]>>| -> Bound<Vec<u8>> {
            match b {
                Bound::Included(k) => Bound::Included(make_full_key(&meta_prefix, k)),
                Bound::Excluded(k) => Bound::Excluded(make_full_key(&meta_prefix, k)),
                Bound::Unbounded => Bound::Unbounded,
            }
        };

        let lo_full = prefixed(&bounds.start_bound());
        let hi_full = prefixed(&bounds.end_bound());

        // SST-level pruning hints: start from lo or prefix start, end at prefix boundary.
        let start_hint: Option<Vec<u8>> = match &lo_full {
            Bound::Included(v) | Bound::Excluded(v) => Some(v.clone()),
            Bound::Unbounded => Some(meta_prefix.to_vec()),
        };
        let end_hint = prefix_successor(&meta_prefix);

        let mut db_iter = self
            .db
            .iter_with_range(
                &mmdb::ReadOptions::default(),
                start_hint.as_deref(),
                end_hint.as_deref(),
            )
            .expect("vsdb: mmdb iter_with_range failed");

        // Seek the forward cursor to the lower bound so next() starts there in O(1)
        // rather than scanning entries below lo_full one by one.
        if let Bound::Included(ref lo) | Bound::Excluded(ref lo) = lo_full {
            db_iter.seek(lo);
        }

        // Wrap in BidiIterator for DoubleEndedIterator support, then apply
        // bound checks lazily. Filter::next_back() correctly scans backward,
        // so entries above hi_full are skipped without full materialisation.
        let iter = BidiIterator::lazy(db_iter)
            .filter(move |(k, _)| {
                k.starts_with(&meta_prefix)
                    && check_bound_lo(k.as_slice(), &lo_full)
                    && check_bound_hi(k.as_slice(), &hi_full)
            })
            .map(|(k, v)| (k[PREFIX_SIZE..].to_vec(), v));

        MmdbIter(Box::new(iter))
    }

    pub(crate) fn batch_begin<'a>(
        &'a self,
        meta_prefix: PreBytes,
    ) -> Box<dyn BatchTrait + 'a> {
        Box::new(MmdbBatch::new(meta_prefix, self))
    }
}

// ---- Iterator ----

/// A lazy, bidirectional iterator over key-value pairs in a single prefix namespace.
///
/// Wraps a boxed `DoubleEndedIterator` so that the concrete streaming type
/// (e.g. `Map<Filter<BidiIterator, _>, _>`) is hidden behind a stable ABI.
/// No entries are collected into memory upfront; data flows from mmdb's
/// streaming SST/memtable sources one item at a time.
pub struct MmdbIter(Box<dyn DoubleEndedIterator<Item = (RawKey, RawValue)>>);

impl Iterator for MmdbIter {
    type Item = (RawKey, RawValue);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl DoubleEndedIterator for MmdbIter {
    #[inline(always)]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back()
    }
}

// ---- Batch ----

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

pub struct MmdbBatch<'a> {
    inner: WriteBatch,
    meta_prefix: PreBytes,
    engine: &'a MmDB,
}

impl<'a> MmdbBatch<'a> {
    fn new(meta_prefix: PreBytes, engine: &'a MmDB) -> Self {
        Self {
            inner: WriteBatch::new(),
            meta_prefix,
            engine,
        }
    }
}

impl BatchTrait for MmdbBatch<'_> {
    #[inline(always)]
    fn insert(&mut self, key: &[u8], value: &[u8]) {
        let full_key = make_full_key(&self.meta_prefix, key);
        self.inner.put(&full_key, value);
    }

    #[inline(always)]
    fn remove(&mut self, key: &[u8]) {
        let full_key = make_full_key(&self.meta_prefix, key);
        self.inner.delete(&full_key);
    }

    #[inline(always)]
    fn commit(&mut self) -> Result<()> {
        let batch = std::mem::replace(&mut self.inner, WriteBatch::new());
        self.engine.db.write(batch).c(d!())?;
        Ok(())
    }
}

// ---- Helpers ----

#[inline(always)]
fn make_full_key(meta_prefix: &[u8], key: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(meta_prefix.len() + key.len());
    v.extend_from_slice(meta_prefix);
    v.extend_from_slice(key);
    v
}

/// Compute the byte-string successor of a prefix (increment the last non-0xFF byte).
/// Returns `None` if all bytes are 0xFF.
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut s = prefix.to_vec();
    for i in (0..s.len()).rev() {
        if s[i] < u8::MAX {
            s[i] += 1;
            s.truncate(i + 1);
            return Some(s);
        }
    }
    None
}

#[inline(always)]
fn check_bound_lo(full_key: &[u8], bound: &Bound<Vec<u8>>) -> bool {
    match bound {
        Bound::Unbounded => true,
        Bound::Included(l) => full_key >= l.as_slice(),
        Bound::Excluded(l) => full_key > l.as_slice(),
    }
}

#[inline(always)]
fn check_bound_hi(full_key: &[u8], bound: &Bound<Vec<u8>>) -> bool {
    match bound {
        Bound::Unbounded => true,
        Bound::Included(u) => full_key <= u.as_slice(),
        Bound::Excluded(u) => full_key < u.as_slice(),
    }
}

fn mmdb_open(dir: &std::path::Path) -> Result<DB> {
    const G: usize = GB as usize;

    // Read available memory once (Linux only).
    let avail_mem_bytes: usize = if cfg!(target_os = "linux") {
        fs::read_to_string("/proc/meminfo")
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
            .unwrap_or(G / 1024)
            * 1024
    } else {
        G
    };

    let wr_buffer_size = if avail_mem_bytes > 16 * G {
        avail_mem_bytes / 4
    } else {
        G
    };

    let block_cache_size = (avail_mem_bytes as u64) / 8;

    // ---- Parallelism: min(cpus, 16) ----
    let parallelism = cmp::min(available_parallelism().c(d!())?.get(), 16);

    let opts = DbOptions {
        create_if_missing: true,
        prefix_len: PREFIX_SIZE,

        // Per-level compression: LZ4 for L0-L1, ZSTD for L2+
        compression_per_level: vec![
            CompressionType::Lz4,  // L0
            CompressionType::Lz4,  // L1
            CompressionType::Zstd, // L2
            CompressionType::Zstd, // L3
            CompressionType::Zstd, // L4
            CompressionType::Zstd, // L5
            CompressionType::Zstd, // L6
        ],

        // Write buffer
        write_buffer_size: wr_buffer_size,
        max_write_buffer_number: 5,
        max_immutable_memtables: 4,

        // Block cache + block size
        block_cache_capacity: block_cache_size,
        block_size: 16 * 1024, // 16 KB

        // Memtable prefix bloom
        memtable_prefix_bloom_ratio: 0.1,

        // Compaction tuning
        level_compaction_dynamic_level_bytes: true,
        l0_compaction_trigger: 8,
        max_subcompactions: 4,

        // Parallelism
        max_background_compactions: parallelism,

        // Concurrent memtable writes
        allow_concurrent_memtable_write: true,

        ..DbOptions::default()
    };

    DB::open(opts, dir).c(d!())
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
        std::env::temp_dir().join(format!("vsdb-mmdb-{tag}-{nanos}"))
    }

    #[test]
    fn mmdb_basic_get_put_delete() {
        let dir = tmp_dir("basic");
        let db = mmdb_open(&dir).unwrap();
        let db: &'static DB = Box::leak(Box::new(db));

        db.put(b"hello", b"world").unwrap();
        assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));

        db.delete(b"hello").unwrap();
        assert_eq!(db.get(b"hello").unwrap(), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mmdb_prefix_iteration() {
        let dir = tmp_dir("prefix-iter");
        let db = mmdb_open(&dir).unwrap();

        let prefix_a: PreBytes = 1_u64.to_be_bytes();
        let prefix_b: PreBytes = 2_u64.to_be_bytes();

        // Insert entries under two different prefixes
        let fk = |p: &[u8], k: &[u8]| make_full_key(p, k);

        db.put(&fk(&prefix_a, b"k1"), b"v1").unwrap();
        db.put(&fk(&prefix_a, b"k2"), b"v2").unwrap();
        db.put(&fk(&prefix_b, b"k3"), b"v3").unwrap();

        // Iterate prefix_a
        let start = Some(prefix_a.as_slice());
        let end = prefix_successor(&prefix_a);
        let end_ref = end.as_deref();
        let entries: Vec<_> = db
            .iter_with_range(&mmdb::ReadOptions::default(), start, end_ref)
            .unwrap()
            .filter(|(k, _)| k.starts_with(&prefix_a))
            .map(|(k, v)| (k[PREFIX_SIZE..].to_vec(), v))
            .collect();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, b"k1".to_vec());
        assert_eq!(entries[1].0, b"k2".to_vec());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
