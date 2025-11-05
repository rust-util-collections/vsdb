use crate::common::{
    BatchTrait, Engine, PREFIX_SIZE, Pre, PreBytes, RESERVED_ID_CNT, RawKey, RawValue,
    vsdb_get_base_dir, vsdb_set_base_dir,
};
use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, PageSize, RO, ReadWriteOptions,
    SyncMode, Table, TableFlags, Transaction, WriteFlags,
};
use parking_lot::{Mutex, RwLock};
use ruc::*;
use std::{
    borrow::Cow,
    cell::Cell,
    collections::BTreeMap,
    fs,
    ops::{Bound, RangeBounds},
    sync::{
        Arc, LazyLock, Once,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

// It is safe to declare `atexit` as a safe fn because it only accepts
// safe `extern "C" fn()` function pointers.
unsafe extern "C" {
    safe fn atexit(func: extern "C" fn()) -> std::os::raw::c_int;
}

/// Wrapper around libc `atexit`.
///
/// Note: The registered function must be safe to call at program exit.
/// We only register `atexit_flush` which satisfies this requirement.
fn register_atexit(func: extern "C" fn()) {
    atexit(func);
}

type WriteBuf = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

struct FlushOnExit {
    write_bufs: std::sync::Weak<Vec<parking_lot::RwLock<WriteBuf>>>,
    shards: Vec<&'static Database<NoWriteMap>>,
}

static FLUSH_REGISTRY: LazyLock<Mutex<Vec<FlushOnExit>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));
static INIT_ATEXIT: Once = Once::new();

extern "C" fn atexit_flush() {
    if let Some(registry) = FLUSH_REGISTRY.try_lock() {
        for state in registry.iter() {
            if let Some(bufs_arc) = state.write_bufs.upgrade() {
                for (shard_idx, buf_mtx) in bufs_arc.iter().enumerate() {
                    if let Some(mut buf) = buf_mtx.try_write() {
                        flush_buffer_impl_shard(state.shards[shard_idx], &mut buf);
                    }
                }
                for db in &state.shards {
                    let _ = db.sync(true);
                }
            }
        }
    }
}

// NOTE:
// The last table is preserved for the meta storage,
// so the max value should be `u8::MAX - 1`
//
// Use 64 shards to reduce write-lock contention on multi-CCD CPUs
// (e.g. EPYC 9474F with 48 cores). With 16 shards there were ~3
// cores per shard on average, causing frequent lock collisions.
const SHARD_CNT: usize = 64;

const TABLE_DATA: &str = "data";
const TABLE_META: &str = "meta";

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

// Flush the write buffer to DB when it reaches this size.
// Amortizes MDBX per-transaction overhead across many writes.
const WRITE_BUF_THRESHOLD: usize = 4096;

// Number of prefixes to reserve per alloc_prefix slow-path DB write.
// Larger values reduce lock contention at the cost of wasting prefix IDs on crash.
// With u64 prefix space this is negligible.
const PREFIX_ALLOC_BATCH: u64 = 8192;

pub struct MdbxEngine {
    hdr: &'static Database<NoWriteMap>,
    shards: Vec<&'static Database<NoWriteMap>>,
    prefix_allocator: PreAllocator,
    max_keylen: AtomicUsize,
    // Write buffer: full_key -> Option<value> (None = tombstone/delete)
    // Amortizes MDBX per-transaction overhead by batching writes.
    // Arc so the background flush thread can hold a reference.
    write_bufs: Arc<Vec<RwLock<WriteBuf>>>,
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

impl MdbxEngine {
    #[inline(always)]
    fn get_shard_idx(&self, prefix: PreBytes) -> usize {
        // NOTE: The prefix is a big-endian encoded u64 that increments from 0.
        // We use the last byte (LSB) for sharding to ensure even distribution.
        // Using the first byte would cause all keys to hit shard 0 until the
        // prefix exceeds 2^56.
        (prefix[PREFIX_SIZE - 1] as usize) % SHARD_CNT
    }

    #[inline(always)]
    fn get_db(&self, prefix: PreBytes) -> &'static Database<NoWriteMap> {
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
            let txn = self.hdr.begin_rw_txn().unwrap();
            let table = txn.open_table(Some(TABLE_META)).unwrap();
            txn.put(
                &table,
                META_KEY_MAX_KEYLEN,
                len.to_be_bytes(),
                WriteFlags::UPSERT,
            )
            .unwrap();
            txn.commit().unwrap();
            self.max_keylen.store(len, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    fn get_table_name(&self, _meta_prefix: PreBytes) -> &'static str {
        TABLE_DATA
    }

    /// Flush buffered writes to DB. Caller must hold the Mutex guard.
    #[inline(always)]
    fn flush_locked(&self, shard_idx: usize, buf: &mut WriteBuf) {
        flush_buffer_impl_shard(self.shards[shard_idx], buf);
    }
}

/// Standalone flush: commits one txn for a single shard.
fn flush_buffer_impl_shard(db: &'static Database<NoWriteMap>, buf: &mut WriteBuf) {
    if buf.is_empty() {
        return;
    }

    let txn = db.begin_rw_txn().unwrap();
    let table = txn.open_table(Some(TABLE_DATA)).unwrap();

    for (key, value) in buf.iter() {
        match value {
            Some(v) => {
                txn.put(&table, key.as_slice(), v.as_slice(), WriteFlags::UPSERT)
                    .unwrap();
            }
            None => {
                let _ = txn.del(&table, key.as_slice(), None);
            }
        }
    }

    txn.commit().unwrap();
    buf.clear();
}

// Background flush interval
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

impl Engine for MdbxEngine {
    fn new() -> Result<Self> {
        let base_dir = vsdb_get_base_dir();
        // avoid setting again on an opened DB
        omit!(vsdb_set_base_dir(&base_dir));

        let mut shards = Vec::with_capacity(SHARD_CNT);

        // Ensure base dir exists
        fs::create_dir_all(&base_dir).c(d!())?;

        for i in 0..SHARD_CNT {
            let dir = base_dir.join(format!("shard_{}", i));
            let db = mdbx_open_shard(&dir)?;
            shards.push(Box::leak(Box::new(db)) as &'static Database<NoWriteMap>);
        }

        let hdr = shards[0];

        let (prefix_allocator, initial_value) = PreAllocator::init();

        // Initialize meta table entries if they don't exist
        {
            let txn = hdr.begin_rw_txn().c(d!())?;
            let table = txn.open_table(Some(TABLE_META)).c(d!())?;

            if txn
                .get::<Vec<u8>>(&table, &META_KEY_MAX_KEYLEN)
                .c(d!())?
                .is_none()
            {
                txn.put(
                    &table,
                    META_KEY_MAX_KEYLEN,
                    0_usize.to_be_bytes(),
                    WriteFlags::UPSERT,
                )
                .c(d!())?;
            }

            if txn
                .get::<Vec<u8>>(&table, &prefix_allocator.key)
                .c(d!())?
                .is_none()
            {
                txn.put(
                    &table,
                    prefix_allocator.key,
                    initial_value,
                    WriteFlags::UPSERT,
                )
                .c(d!())?;
            }

            txn.commit().c(d!())?;
        }

        let max_keylen = {
            let txn = hdr.begin_ro_txn().unwrap();
            let table = txn.open_table(Some(TABLE_META)).unwrap();
            let val = txn
                .get::<Vec<u8>>(&table, &META_KEY_MAX_KEYLEN)
                .unwrap()
                .unwrap();
            AtomicUsize::new(crate::parse_int!(val, usize))
        };

        let write_bufs = Arc::new(
            (0..SHARD_CNT)
                .map(|_| RwLock::new(BTreeMap::new()))
                .collect::<Vec<_>>(),
        );

        // Spawn background flush thread
        {
            let bufs = Arc::clone(&write_bufs);
            let shards_ref = shards.clone();
            std::thread::Builder::new()
                .name("vsdb-flush".into())
                .spawn(move || {
                    loop {
                        std::thread::sleep(FLUSH_INTERVAL);
                        for (i, buf_mtx) in bufs.iter().enumerate() {
                            let mut guard = buf_mtx.write();
                            flush_buffer_impl_shard(shards_ref[i], &mut guard);
                        }
                    }
                })
                .unwrap();
        }

        // Register atexit hook to flush buffered writes on normal process exit.
        // Does not help with kill -9, but covers exit()/main-return/panic-unwind.
        INIT_ATEXIT.call_once(|| {
            register_atexit(atexit_flush);
        });

        {
            let mut registry = FLUSH_REGISTRY.lock();
            registry.retain(|state| state.write_bufs.strong_count() > 0);
            registry.push(FlushOnExit {
                write_bufs: std::sync::Arc::downgrade(&write_bufs),
                shards: shards.clone(),
            });
        }

        Ok(MdbxEngine {
            hdr,
            shards,
            prefix_allocator,
            max_keylen,
            write_bufs,
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
                        let txn = self.hdr.begin_ro_txn().unwrap();
                        let table = txn.open_table(Some(TABLE_META)).unwrap();
                        let ret = crate::parse_prefix!(
                            txn.get::<Vec<u8>>(&table, &self.prefix_allocator.key)
                                .unwrap()
                                .unwrap()
                        );
                        drop(txn);

                        let new_ceil = ret + PREFIX_ALLOC_BATCH;
                        let txn = self.hdr.begin_rw_txn().unwrap();
                        let table = txn.open_table(Some(TABLE_META)).unwrap();
                        txn.put(
                            &table,
                            self.prefix_allocator.key,
                            new_ceil.to_be_bytes(),
                            WriteFlags::UPSERT,
                        )
                        .unwrap();
                        txn.commit().unwrap();

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
                        let txn = self.hdr.begin_rw_txn().unwrap();
                        let table = txn.open_table(Some(TABLE_META)).unwrap();
                        txn.put(
                            &table,
                            self.prefix_allocator.key,
                            new_ceil.to_be_bytes(),
                            WriteFlags::UPSERT,
                        )
                        .unwrap();
                        txn.commit().unwrap();
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
        for (i, buf_mtx) in self.write_bufs.iter().enumerate() {
            let mut buf = buf_mtx.write();
            self.flush_locked(i, &mut buf);
        }
        for db in &self.shards {
            db.sync(true).unwrap();
        }
    }

    fn iter(&self, hdr_prefix: PreBytes) -> MdbxIter {
        let shard_idx = self.get_shard_idx(hdr_prefix);
        // Flush buffer so the iterator sees all pending writes
        {
            let mut buf = self.write_bufs[shard_idx].write();
            self.flush_locked(shard_idx, &mut buf);
        }

        let db = self.get_db(hdr_prefix);
        let table_name = self.get_table_name(hdr_prefix);

        // For reverse cursor, seek to "prefix + 1" so set_range lands past our prefix
        let next_prefix =
            (crate::parse_prefix!(&hdr_prefix).wrapping_add(1)).to_be_bytes();

        MdbxIter::create(
            db,
            table_name,
            hdr_prefix,
            (Bound::Unbounded, Bound::Unbounded),
            &hdr_prefix,
            &next_prefix,
        )
    }

    fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        hdr_prefix: PreBytes,
        bounds: R,
    ) -> MdbxIter {
        let shard_idx = self.get_shard_idx(hdr_prefix);
        // Flush buffer so the iterator sees all pending writes
        {
            let mut buf = self.write_bufs[shard_idx].write();
            self.flush_locked(shard_idx, &mut buf);
        }

        let db = self.get_db(hdr_prefix);
        let table_name = self.get_table_name(hdr_prefix);

        // Build full-key bounds for forward/reverse cursor positioning
        let mut b_lo = hdr_prefix.to_vec();
        let lo_bound = match bounds.start_bound() {
            Bound::Included(lo) => {
                b_lo.extend_from_slice(lo);
                Bound::Included(b_lo)
            }
            Bound::Excluded(lo) => {
                b_lo.extend_from_slice(lo);
                Bound::Excluded(b_lo)
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let mut b_hi = hdr_prefix.to_vec();
        let hi_bound = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.extend_from_slice(hi);
                Bound::Included(b_hi)
            }
            Bound::Excluded(hi) => {
                b_hi.extend_from_slice(hi);
                Bound::Excluded(b_hi)
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        // Determine seek position for the forward cursor
        let fwd_seek = match &lo_bound {
            Bound::Included(lo) => lo.clone(),
            Bound::Excluded(lo) => {
                let mut s = lo.clone();
                s.push(0);
                s
            }
            Bound::Unbounded => hdr_prefix.to_vec(),
        };

        // Determine seek position for the reverse cursor:
        // We want to seek PAST the upper bound, then use prev() to find the
        // last entry within bounds.
        let rev_seek = match &hi_bound {
            Bound::Included(hi) => {
                // Seek past hi: append a zero byte so set_range lands after hi
                let mut s = hi.clone();
                s.push(0);
                s
            }
            Bound::Excluded(hi) => {
                // Seek to hi itself; set_range will land at or after hi,
                // then prev() will find the last entry < hi
                hi.clone()
            }
            Bound::Unbounded => {
                // Seek to prefix + 1
                (crate::parse_prefix!(&hdr_prefix).wrapping_add(1))
                    .to_be_bytes()
                    .to_vec()
            }
        };

        MdbxIter::create(
            db,
            table_name,
            hdr_prefix,
            (lo_bound, hi_bound),
            &fwd_seek,
            &rev_seek,
        )
    }

    fn get(&self, hdr_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let full_key = make_full_key(hdr_prefix.as_slice(), key);
        let shard_idx = self.get_shard_idx(hdr_prefix);

        // Check write buffer first
        {
            let buf = self.write_bufs[shard_idx].read();
            if let Some(entry) = buf.get(&full_key) {
                return entry.clone(); // Some(value) or None (tombstone)
            }
        }

        // Not in buffer, check DB
        let db = self.get_db(hdr_prefix);
        let table_name = self.get_table_name(hdr_prefix);
        let txn = db.begin_ro_txn().unwrap();
        let table = txn.open_table(Some(table_name)).unwrap();
        txn.get::<Vec<u8>>(&table, full_key.as_slice()).unwrap()
    }

    fn insert(&self, hdr_prefix: PreBytes, key: &[u8], value: &[u8]) {
        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }

        let full_key = make_full_key(hdr_prefix.as_slice(), key);
        let shard_idx = self.get_shard_idx(hdr_prefix);
        let mut buf = self.write_bufs[shard_idx].write();
        buf.insert(full_key, Some(value.to_vec()));
        if buf.len() >= WRITE_BUF_THRESHOLD {
            self.flush_locked(shard_idx, &mut buf);
        }
    }

    fn remove(&self, hdr_prefix: PreBytes, key: &[u8]) {
        let full_key = make_full_key(hdr_prefix.as_slice(), key);
        let shard_idx = self.get_shard_idx(hdr_prefix);
        let mut buf = self.write_bufs[shard_idx].write();
        buf.insert(full_key, None);
        if buf.len() >= WRITE_BUF_THRESHOLD {
            self.flush_locked(shard_idx, &mut buf);
        }
    }

    fn batch_begin<'a>(&'a self, meta_prefix: PreBytes) -> Box<dyn BatchTrait + 'a> {
        Box::new(MdbxBatch::new(meta_prefix, self))
    }
}

pub struct MdbxBatch<'a> {
    ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    meta_prefix: PreBytes,
    max_key_len: usize,
    engine: &'a MdbxEngine,
}

impl<'a> MdbxBatch<'a> {
    fn new(meta_prefix: PreBytes, engine: &'a MdbxEngine) -> Self {
        Self {
            ops: Vec::with_capacity(16),
            meta_prefix,
            max_key_len: 0,
            engine,
        }
    }
}

impl BatchTrait for MdbxBatch<'_> {
    #[inline(always)]
    fn insert(&mut self, key: &[u8], value: &[u8]) {
        let full_key = make_full_key(self.meta_prefix.as_slice(), key);
        if key.len() > self.max_key_len {
            self.max_key_len = key.len();
        }
        self.ops.push((full_key, Some(value.to_vec())));
    }

    #[inline(always)]
    fn remove(&mut self, key: &[u8]) {
        let full_key = make_full_key(self.meta_prefix.as_slice(), key);
        self.ops.push((full_key, None));
    }

    #[inline(always)]
    fn commit(&mut self) -> Result<()> {
        let shard_idx = self.engine.get_shard_idx(self.meta_prefix);
        let mut buf = self.engine.write_bufs[shard_idx].write();

        for (key, value) in self.ops.drain(..) {
            buf.insert(key, value);
        }

        if buf.len() >= WRITE_BUF_THRESHOLD {
            self.engine.flush_locked(shard_idx, &mut buf);
        }

        if self.max_key_len > 0 && self.max_key_len > self.engine.get_max_keylen() {
            self.engine.set_max_key_len(self.max_key_len);
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////
// MdbxIter — the tricky part (lifetime erasure with transmute)
/////////////////////////////////////////////////////////////////////////////

// We need to erase the lifetimes because:
// 1. Database is &'static (Box::leak), so Transaction<'db> is effectively 'static
// 2. Cursor<'txn> must live shorter than Transaction, which is guaranteed by
//    struct field drop order (cursors declared before _txn are dropped first)

pub struct MdbxIter {
    // SAFETY: cursors must be declared before _txn so they drop first.
    // Rust drops struct fields in declaration order.
    inner_fwd: Option<libmdbx::Cursor<'static, RO>>,
    inner_rev: Option<libmdbx::Cursor<'static, RO>>,
    _table: Table<'static>,
    _txn: Transaction<'static, RO, NoWriteMap>,
    prefix: PreBytes,
    range: (Bound<RawKey>, Bound<RawKey>),
    // Buffered entries: set_range/last returns the current entry,
    // so we buffer it and return it on the first next()/next_back() call.
    fwd_pending: Option<(Vec<u8>, Vec<u8>)>,
    rev_pending: Option<(Vec<u8>, Vec<u8>)>,
    fwd_done: bool,
    rev_done: bool,
}

impl MdbxIter {
    fn create(
        db: &'static Database<NoWriteMap>,
        table_name: &str,
        prefix: PreBytes,
        range: (Bound<RawKey>, Bound<RawKey>),
        fwd_seek: &[u8],
        rev_seek: &[u8],
    ) -> Self {
        let txn = db.begin_ro_txn().unwrap();
        // SAFETY: db is &'static, so 'db lifetime is 'static.
        // The compiler can't deduce this through the borrow chain.
        let txn: Transaction<'static, RO, NoWriteMap> =
            unsafe { std::mem::transmute(txn) };

        let table = txn.open_table(Some(table_name)).unwrap();
        // SAFETY: table borrows txn which we keep alive in the struct
        let table: Table<'static> = unsafe { std::mem::transmute(table) };

        let cursor_fwd = txn.cursor(&table).unwrap();
        // SAFETY: cursor borrows txn; txn outlives cursor due to drop order
        let mut cursor_fwd: libmdbx::Cursor<'static, RO> =
            unsafe { std::mem::transmute(cursor_fwd) };

        let cursor_rev = txn.cursor(&table).unwrap();
        let mut cursor_rev: libmdbx::Cursor<'static, RO> =
            unsafe { std::mem::transmute(cursor_rev) };

        // Position the forward cursor and capture the initial entry
        let fwd_pending = cursor_fwd.set_range::<Vec<u8>, Vec<u8>>(fwd_seek).unwrap();

        // Position the reverse cursor:
        // rev_seek is always set PAST the desired upper bound, so set_range
        // will land at or after the upper bound. Then prev() gives us the
        // last entry within bounds.
        let rev_pending = if cursor_rev
            .set_range::<Vec<u8>, Vec<u8>>(rev_seek)
            .unwrap()
            .is_some()
        {
            // Cursor is at or past upper bound; step back to the last valid entry
            cursor_rev.prev::<Vec<u8>, Vec<u8>>().unwrap()
        } else {
            // No entry >= rev_seek, so all entries are before it.
            // The last entry in the table is the starting point.
            cursor_rev.last::<Vec<u8>, Vec<u8>>().unwrap()
        };

        let fwd_done = fwd_pending.is_none();
        let rev_done = rev_pending.is_none();

        MdbxIter {
            inner_fwd: if !fwd_done { Some(cursor_fwd) } else { None },
            inner_rev: if !rev_done { Some(cursor_rev) } else { None },
            _table: table,
            _txn: txn,
            prefix,
            range,
            fwd_pending,
            rev_pending,
            fwd_done,
            rev_done,
        }
    }

    #[inline(always)]
    fn check_upper_bound(&self, full_key: &[u8]) -> bool {
        match &self.range.1 {
            Bound::Unbounded => true,
            Bound::Included(u) => full_key[..] <= u[..],
            Bound::Excluded(u) => full_key[..] < u[..],
        }
    }

    #[inline(always)]
    fn check_lower_bound(&self, full_key: &[u8]) -> bool {
        match &self.range.0 {
            Bound::Unbounded => true,
            Bound::Included(l) => full_key[..] >= l[..],
            Bound::Excluded(l) => full_key[..] > l[..],
        }
    }
}

impl Iterator for MdbxIter {
    type Item = (RawKey, RawValue);

    fn next(&mut self) -> Option<Self::Item> {
        if self.fwd_done {
            return None;
        }

        // Take the buffered pending entry
        let (ik, iv) = self.fwd_pending.take()?;

        // Check prefix boundary
        if !ik.starts_with(&self.prefix) {
            self.fwd_done = true;
            return None;
        }

        // Check lower bound (for Excluded start bounds where the
        // cursor may have landed exactly on the excluded key)
        if !self.check_lower_bound(&ik) {
            // Skip this entry and try the next one
            if let Some(cursor) = self.inner_fwd.as_mut() {
                self.fwd_pending = cursor.next::<Vec<u8>, Vec<u8>>().unwrap();
                if self.fwd_pending.is_none() {
                    self.fwd_done = true;
                }
            } else {
                self.fwd_done = true;
            }
            return self.next();
        }

        // Check upper bound
        if !self.check_upper_bound(&ik) {
            self.fwd_done = true;
            return None;
        }

        // Advance cursor and buffer the next entry
        if let Some(cursor) = self.inner_fwd.as_mut() {
            self.fwd_pending = cursor.next::<Vec<u8>, Vec<u8>>().unwrap();
            if self.fwd_pending.is_none() {
                self.fwd_done = true;
            }
        }

        // Strip prefix from key and return
        let mut k = ik;
        k.drain(..PREFIX_SIZE);
        Some((k, iv))
    }
}

impl DoubleEndedIterator for MdbxIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.rev_done {
            return None;
        }

        // Take the buffered pending entry
        let (ik, iv) = self.rev_pending.take()?;

        // Check prefix boundary
        if !ik.starts_with(&self.prefix) {
            self.rev_done = true;
            return None;
        }

        // Check upper bound (for Excluded end bounds where the
        // cursor may have landed exactly on the excluded key)
        if !self.check_upper_bound(&ik) {
            // Skip this entry and try the previous one
            if let Some(cursor) = self.inner_rev.as_mut() {
                self.rev_pending = cursor.prev::<Vec<u8>, Vec<u8>>().unwrap();
                if self.rev_pending.is_none() {
                    self.rev_done = true;
                }
            } else {
                self.rev_done = true;
            }
            return self.next_back();
        }

        // Check lower bound
        if !self.check_lower_bound(&ik) {
            self.rev_done = true;
            return None;
        }

        // Advance cursor backward and buffer the previous entry
        if let Some(cursor) = self.inner_rev.as_mut() {
            self.rev_pending = cursor.prev::<Vec<u8>, Vec<u8>>().unwrap();
            if self.rev_pending.is_none() {
                self.rev_done = true;
            }
        }

        // Strip prefix from key and return
        let mut k = ik;
        k.drain(..PREFIX_SIZE);
        Some((k, iv))
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
}

fn mdbx_open_shard(dir: &std::path::Path) -> Result<Database<NoWriteMap>> {
    fs::create_dir_all(dir).c(d!())?;

    // Use Durable: fsync on each commit for full crash safety.
    // The application-level write buffer (WRITE_BUF_THRESHOLD) batches thousands
    // of writes into a single transaction, so the per-entry fsync cost is negligible.
    let opts = DatabaseOptions {
        max_tables: Some(2), // data table + meta table
        page_size: Some(PageSize::Set(4096)),
        mode: Mode::ReadWrite(ReadWriteOptions {
            sync_mode: SyncMode::Durable,
            ..ReadWriteOptions::default()
        }),
        ..DatabaseOptions::default()
    };

    let db = Database::<NoWriteMap>::open_with_options(dir, opts).c(d!())?;

    // Pre-create all tables so they exist for readers
    {
        let txn = db.begin_rw_txn().c(d!())?;
        txn.create_table(Some(TABLE_DATA), TableFlags::default())
            .c(d!())?;
        txn.create_table(Some(TABLE_META), TableFlags::default())
            .c(d!())?;
        txn.commit().c(d!())?;
    }

    Ok(db)
}
