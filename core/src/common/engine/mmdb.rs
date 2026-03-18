use crate::common::{
    BatchTrait, PREFIX_SIZE, Pre, PreBytes, RESERVED_ID_CNT, RawKey, RawValue,
    vsdb_get_base_dir, vsdb_set_base_dir,
};
use mmdb::{DB, DbOptions, WriteBatch};
use parking_lot::Mutex;
use ruc::*;
use std::{
    borrow::Cow,
    cell::Cell,
    fs,
    ops::{Bound, RangeBounds},
    sync::{
        LazyLock,
        atomic::{AtomicU64, Ordering},
    },
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
        let entries = self.collect_prefix_entries(meta_prefix);
        MmdbIter::new(entries)
    }

    pub(crate) fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> MmdbIter {
        // Build full-key bounds for filtering
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
            Bound::Unbounded => Bound::Included(meta_prefix.to_vec()),
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
            Bound::Unbounded => {
                // Upper bound is the prefix successor
                match prefix_successor(&meta_prefix) {
                    Some(succ) => Bound::Excluded(succ),
                    None => Bound::Unbounded,
                }
            }
        };

        // Use iter_with_range for efficient SST file pruning
        let start_hint = match &lo_full {
            Bound::Included(v) | Bound::Excluded(v) => Some(v.as_slice()),
            Bound::Unbounded => None,
        };
        let end_hint = match &hi_full {
            Bound::Included(v) | Bound::Excluded(v) => Some(v.as_slice()),
            Bound::Unbounded => None,
        };

        let db_iter = self
            .db
            .iter_with_range(&mmdb::ReadOptions::default(), start_hint, end_hint)
            .expect("vsdb: mmdb iter_with_range failed");

        let entries: Vec<(RawKey, RawValue)> = db_iter
            .filter(|(k, _)| {
                if !k.starts_with(&meta_prefix) {
                    return false;
                }
                let full_key = k.as_slice();
                check_bound_lo(full_key, &lo_full) && check_bound_hi(full_key, &hi_full)
            })
            .map(|(k, v)| (k[PREFIX_SIZE..].to_vec(), v))
            .collect();

        MmdbIter::new(entries)
    }

    pub(crate) fn batch_begin<'a>(
        &'a self,
        meta_prefix: PreBytes,
    ) -> Box<dyn BatchTrait + 'a> {
        Box::new(MmdbBatch::new(meta_prefix, self))
    }

    /// Collect all entries matching the given prefix, stripping the prefix from keys.
    fn collect_prefix_entries(&self, meta_prefix: PreBytes) -> Vec<(RawKey, RawValue)> {
        let prefix_end = prefix_successor(&meta_prefix);

        let start_hint = Some(meta_prefix.as_slice());
        let end_hint = prefix_end.as_deref();

        let db_iter = self
            .db
            .iter_with_range(&mmdb::ReadOptions::default(), start_hint, end_hint)
            .expect("vsdb: mmdb iter_with_range failed");

        db_iter
            .filter(|(k, _)| k.starts_with(&meta_prefix))
            .map(|(k, v)| (k[PREFIX_SIZE..].to_vec(), v))
            .collect()
    }
}

// ---- Iterator ----

pub struct MmdbIter {
    entries: Vec<(RawKey, RawValue)>,
    fwd_idx: usize,
    rev_idx: usize,
}

impl MmdbIter {
    fn new(entries: Vec<(RawKey, RawValue)>) -> Self {
        let len = entries.len();
        Self {
            entries,
            fwd_idx: 0,
            rev_idx: len,
        }
    }
}

impl Iterator for MmdbIter {
    type Item = (RawKey, RawValue);

    fn next(&mut self) -> Option<Self::Item> {
        if self.fwd_idx >= self.rev_idx {
            return None;
        }
        let item = self.entries[self.fwd_idx].clone();
        self.fwd_idx += 1;
        Some(item)
    }
}

impl DoubleEndedIterator for MmdbIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.rev_idx == 0 || self.fwd_idx >= self.rev_idx {
            return None;
        }
        self.rev_idx -= 1;
        let item = self.entries[self.rev_idx].clone();
        Some(item)
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
    let opts = DbOptions {
        create_if_missing: true,
        prefix_len: PREFIX_SIZE,
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
