use crate::common::{
    GB, PREFIX_ALLOC_START, PREFIX_SIZE, Pre, PreBytes, RawKey, RawRangeBounds,
    RawValue, atomic_write_file, configured_mem_budget_mb, create_dir_all_durable,
    error::{Result, VsdbError, corrupt, from_mmdb_err},
    vsdb_freeze_base_dir, vsdb_get_base_dir, vsdb_is_read_only,
};
use mmdb::{
    BidiIterator, BlockCachePool, CompressionType, DB, DbOptions, ReadOptions, Snapshot,
    WriteBatch, WriteOptions,
};
use parking_lot::{Mutex, RwLock};
use std::{
    cell::Cell,
    cmp,
    collections::{HashMap, HashSet},
    fs,
    io::{self, Read},
    ops::Bound,
    path::{Path, PathBuf},
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, ThreadId},
};

/// A synchronous deletion of this reserved system key fences a shard's WAL.
/// Collection keys include an eight-byte prefix, so this one-byte key cannot
/// overlap user data, including entries with empty user keys.
const META_KEY_WAL_FENCE: [u8; 1] = [1];

/// v16+ location of the prefix-allocator ceiling, relative to the VSDB
/// base dir: an 8-byte little-endian `u64`, written durably via
/// tmp/fsync/rename plus a parent-dir fsync. Living outside the shard
/// DBs, it keeps prefix allocation independent of the default engine —
/// a prerequisite for namespaces sharing one global allocator.
const PREFIX_CEILING_REL_PATH: &str = "__SYSTEM__/__prefix_ceiling__";

/// Records that all shard directories have been created and synced.
const READY_PATH: &str = "__SYSTEM__/.shards_ready";

/// Initialization sentinel, relative to an engine root.
///
/// Written durably BEFORE the first shard of a brand-new root is
/// created and removed after the format marker lands, it lets a later
/// open distinguish "a create crashed mid-way — safe to resume, no
/// allocation ever targeted this root" from "a previously working
/// dataset is missing shard dirs — damaged, refuse loudly". Without it
/// the two states are indistinguishable and either roots get bricked
/// or damaged datasets get silently reinitialized.
const INIT_SENTINEL_REL_PATH: &str = "__SYSTEM__/__initializing__";

const PREFIX_ALLOC_BATCH: u64 = 8192;

/// Thread-local guard that removes this thread's [`PENDING_WINDOWS`]
/// entry on thread exit. Registered (via `.with(|_| {})`) the first
/// time a thread claims a batch; its `Drop` impl runs during that
/// thread's TLS teardown, so a dead thread's entry is reclaimed
/// automatically instead of staying registered for the life of the
/// process (a `ThreadId` is never reused, so without this the registry
/// would grow by one entry per historical thread in a thread-per-task
/// workload).
///
/// This is always safe to remove eagerly, regardless of whether the
/// thread's window was fully issued before it exited: the window's
/// un-issued tail can never be issued by *any* thread (only the
/// now-gone thread's `LOCAL_NEXT`/`LOCAL_CEIL` could have continued
/// issuing from it — `GLOBAL_COUNTER` has already moved past the whole
/// batch), so there is nothing left for `reserve_recovered_prefix` to
/// protect once the owning thread is gone. The drop also purges any
/// recovered-prefix reservations in that un-issued range.
struct PendingWindowGuard;

impl Drop for PendingWindowGuard {
    fn drop(&mut self) {
        let window = PENDING_WINDOWS.write().remove(&thread::current().id());
        let Some((cursor, end)) = window else {
            return;
        };
        // A reservation in this window's un-issued tail can never be
        // generated again now that its owner is gone. Purge it so the
        // reservation set (and the allocator's fast-path flag) does not
        // live for the rest of the process.
        let cursor = cursor.load(Ordering::SeqCst);
        let mut reserved = RECOVERED_PREFIXES.lock();
        let before = reserved.len();
        reserved.retain(|prefix| *prefix < cursor || *prefix >= end);
        if reserved.len() != before && reserved.is_empty() {
            RECOVERED_NONEMPTY.store(false, Ordering::SeqCst);
        }
    }
}

thread_local! {
    static LOCAL_NEXT: Cell<u64> = const { Cell::new(0) };
    static LOCAL_CEIL: Cell<u64> = const { Cell::new(0) };
    /// Shared with this thread's [`PENDING_WINDOWS`] entry so
    /// `reserve_recovered_prefix` (possibly running on another thread)
    /// can see this thread's live position within its still-open batch,
    /// not just the batch's static bounds. Reused across this thread's
    /// batches — only the value is reset on every new batch claim.
    static LOCAL_CURSOR: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    static PENDING_WINDOW_GUARD: PendingWindowGuard = const { PendingWindowGuard };
}

/// Next un-issued batch start. `0` means "not yet initialized from the
/// persisted allocator value" (real prefixes start at `PREFIX_ALLOC_START`).
static GLOBAL_COUNTER: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
/// In-memory mirror of the persisted allocator ceiling (always kept in
/// sync with the on-disk value once initialized).
static GLOBAL_CEILING: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
/// The persisted allocator value at process initialization. Every prefix
/// below it was issued by a previous run and can never be issued again.
static GLOBAL_FLOOR: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
static PREFIX_ALLOC_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// A thread's live position within its still-open batch: a shared
/// cursor (the next value this thread will hand out — mirrors
/// `LOCAL_NEXT`, updated on every local issuance) paired with the
/// batch's exclusive end (`GLOBAL_COUNTER.fetch_add`'s claimed upper
/// bound).
type PendingWindow = (Arc<AtomicU64>, u64);

/// Per-thread pending allocation windows. Only `[cursor, end)` is truly
/// un-issued: values below `cursor` were already handed out earlier in
/// this same still-open batch and can never recur as a future
/// `alloc_prefix` candidate, so `reserve_recovered_prefix` must not
/// treat them as pending — doing so would permanently leak the entry
/// (nothing removes an already-issued value from `RECOVERED_PREFIXES`,
/// since it can never be regenerated to be matched and evicted).
/// Refills replace the same thread's entry; a live thread's entry is
/// removed by [`PendingWindowGuard`] when that thread exits, so the
/// registry stays bounded by the number of currently-live threads that
/// have allocated at least one prefix batch (not one entry per
/// historical thread).
static PENDING_WINDOWS: LazyLock<RwLock<HashMap<ThreadId, PendingWindow>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Prefixes accepted by [`MmDB::reserve_recovered_prefix`] that the
/// allocator has not issued yet; `alloc_prefix` must skip them.
static RECOVERED_PREFIXES: LazyLock<Mutex<HashSet<Pre>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));
/// Fast-path guard: `alloc_prefix` only locks `RECOVERED_PREFIXES` after
/// at least one reservation has been recorded (never in normal operation).
static RECOVERED_NONEMPTY: AtomicBool = AtomicBool::new(false);

/// Number of DB shards in the DEFAULT namespace. Pinned forever: each
/// prefix is routed to one shard via `prefix % shard_count`, so changing
/// the count under an existing dataset would silently re-route every
/// prefix. Non-default namespaces persist their own creation-time count
/// in the namespace registry.
const NUM_SHARDS: usize = 16;

/// Serializes durable allocator read-modify-write operations. When both
/// locks are needed, acquire PREFIX_ALLOC_LOCK before this lock.
static SYS_META_LOCK: Mutex<()> = Mutex::new(());

pub struct MmDB {
    /// Sharded DB handlers (`shards` of them), owned by this engine —
    /// dropping the engine closes every shard (flushing writable state,
    /// joining compaction threads, and releasing LOCK files).
    dbs: Box<[DB]>,
    read_only: bool,
    mem_budget: usize,
}

impl MmDB {
    #[inline(always)]
    pub(crate) fn is_read_only(&self) -> bool {
        self.read_only
    }

    #[inline(always)]
    pub(crate) fn mem_budget(&self) -> usize {
        self.mem_budget
    }

    pub(crate) fn new() -> Result<Self> {
        vsdb_freeze_base_dir();
        Self::open_at(
            &vsdb_get_base_dir(),
            NUM_SHARDS,
            EngineSizing::from_process_budget(),
            0,
        )
    }

    /// Opens an engine instance rooted at `root` with `shards` shards.
    ///
    /// Shard readiness and universe identity are verified before any writable
    /// shard is opened. An interrupted initialization retains its sentinel.
    pub(crate) fn open_at(
        root: &Path,
        shards: usize,
        sizing: EngineSizing,
        ns_id: u64,
    ) -> Result<Self> {
        debug_assert!((1..=64).contains(&shards));
        let universe_id = crate::common::universe::active()?.id;
        let read_only = vsdb_is_read_only();
        let dir = root.join("mmdb");
        let sentinel_path = root.join(INIT_SENTINEL_REL_PATH);
        let _adoption = if !read_only && ns_id != 0 {
            create_dir_all_durable(root)?;
            let guard = crate::common::universe::lock_root_dir(root)?;
            crate::common::universe::validate_pending_root(
                root,
                universe_id,
                ns_id,
                shards,
            )?;
            Some(guard)
        } else {
            None
        };
        if read_only {
            // The completion marker is published durably before best-effort
            // sentinel removal. A leftover sentinel is advisory once the
            // marker and exact shard layout validate; readers never unlink it.
            crate::common::universe::verify_engine(root, universe_id, ns_id, shards)?;
            if ns_id != 0 {
                crate::common::universe::check_format_marker(root, false)?;
            }
        } else {
            create_dir_all_durable(root)?;
            crate::common::universe::validate_ready_engine(
                root,
                universe_id,
                ns_id,
                shards,
            )?;

            let marker_present = root.join(READY_PATH).exists();
            let scan = scan_shard_layout(&dir, shards)?;
            validate_shard_layout(&dir, shards, marker_present, &sentinel_path, &scan)?;

            // Brand-new root: raise the initialization sentinel durably
            // BEFORE the first shard dir exists, so a crash mid-creation is
            // provably a resumable create (see INIT_SENTINEL_REL_PATH).
            if !marker_present && scan.present == 0 && !sentinel_path.exists() {
                let _g = SYS_META_LOCK.lock();
                create_dir_all_durable(sentinel_path.parent().expect("has parent"))?;
                atomic_write_file(
                    &sentinel_path,
                    &crate::common::universe::engine_bytes(universe_id, ns_id, shards),
                )?;
            }
            if ns_id != 0 {
                // The initializer proves ownership across a crash while the
                // marker is installed. The base was protected at bootstrap.
                crate::common::universe::check_format_marker(root, true)?;
            }
            create_dir_all_durable(&dir)?;
        }

        // Shards are owned all the way through: if a later shard (or any
        // meta-init step below) fails, this Vec's normal Drop glue closes
        // every already-opened shard (flushing its WAL and joining its
        // compaction thread) via the `?` early return — the same Drop
        // glue that later powers `MmDB::close`.
        //
        // One block-cache pool per ENGINE, shared by its shards
        // (shared-mem-pool RFC tier (i)): the shards are one tenant —
        // same dataset, same budget — so pooling their cache slice
        // trades no isolation, while a hot shard (one collection lives
        // entirely inside one shard: `prefix % shards` routing) can now
        // use the whole slice instead of `1/shards` of it. The capacity
        // is exactly the sum of the per-shard capacities of the static
        // split, so engine totals are unchanged.
        let pool = Arc::new(BlockCachePool::new(
            shards as u64 * per_shard_block_cache_size(&sizing, shards),
        ));
        let mut dbs_vec: Vec<DB> = Vec::with_capacity(shards);
        for i in 0..shards {
            let shard_dir = dir.join(format!("shard_{:02}", i));
            if !read_only {
                create_dir_all_durable(&shard_dir)?;
            }
            let db = mmdb_open(&shard_dir, shards, sizing, &pool)?;
            dbs_vec.push(db);
        }

        if !read_only {
            crate::common::universe::install_engine(root, universe_id, ns_id, shards)?;
            // Initialization is complete and durably marked: retire the
            // sentinel best-effort. Marker-present validation never consults
            // it, so a stuck/unremovable sentinel must not brick reopen of an
            // already-complete root.
            let _ = fs::remove_file(&sentinel_path);
        }

        Ok(MmDB {
            dbs: dbs_vec.into_boxed_slice(),
            read_only,
            mem_budget: sizing.mem_budget,
        })
    }

    /// Cleanly closes every shard and consumes the engine.
    ///
    /// For writable shards, `DB::close` flushes the active memtable and
    /// syncs the WAL, surfacing errors that plain `Drop` would swallow.
    /// Read-only shards release resources without flushing or syncing.
    /// The subsequent drop releases each shard's LOCK file and joins any
    /// compaction threads. All shards are closed even if one errors; the
    /// first error is returned.
    pub(crate) fn close(self) -> Result<()> {
        let mut ret = Ok(());
        for db in &self.dbs {
            if let Err(e) = db.close()
                && ret.is_ok()
            {
                ret = Err(from_mmdb_err(e));
            }
        }
        ret
    }

    /// Index of the shard a prefix routes to.
    #[inline(always)]
    pub(crate) fn shard_of(&self, meta_prefix: &PreBytes) -> usize {
        let prefix = u64::from_le_bytes(*meta_prefix);
        (prefix % self.dbs.len() as u64) as usize
    }

    /// Route a prefix to its shard.
    #[inline(always)]
    fn shard(&self, meta_prefix: &PreBytes) -> &DB {
        &self.dbs[self.shard_of(meta_prefix)]
    }

    /// See the module-level [`alloc_prefix`] — kept as a method so the
    /// typed layer keeps calling through its engine handle.
    #[inline(always)]
    pub(crate) fn alloc_prefix(&self) -> Result<Pre> {
        if self.read_only {
            return Err(VsdbError::ReadOnly {
                operation: "prefix allocation",
            });
        }
        alloc_prefix()
    }

    /// See the module-level [`reserve_recovered_prefix`].
    #[inline(always)]
    pub(crate) fn reserve_recovered_prefix(
        &self,
        meta_prefix: PreBytes,
    ) -> Result<bool> {
        reserve_recovered_prefix(meta_prefix)
    }

    pub(crate) fn flush(&self) {
        if self.read_only {
            return;
        }
        for db in &self.dbs {
            db.flush().expect("vsdb: mmdb flush failed");
        }
    }

    /// One engine-property reading per shard, in shard order
    /// (measurement pre-work of the shared-mem-pool RFC, step 0).
    ///
    /// Property names are mmdb's `DB::get_property` names; `None` marks
    /// a name unknown to the engine. Per-shard cache telemetry stays
    /// meaningful under the engine-level cache pool: hit/miss counters
    /// are counted per shard at the read site, while
    /// `"block-cache-usage"` reports the pool-wide total from every
    /// shard (all shards of one engine share one pool by design).
    pub(crate) fn shard_properties(&self, name: &str) -> Vec<Option<String>> {
        self.dbs.iter().map(|db| db.get_property(name)).collect()
    }

    pub(crate) fn get(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        with_full_key(&meta_prefix, key, |k| self.shard(&meta_prefix).get(k))
            .expect("vsdb: mmdb get failed")
    }

    pub(crate) fn insert(&self, meta_prefix: PreBytes, key: &[u8], value: &[u8]) {
        assert!(
            !self.read_only,
            "vsdb: insert is unavailable in read-only mode"
        );
        with_full_key(&meta_prefix, key, |k| {
            self.shard(&meta_prefix).put(k, value)
        })
        .expect("vsdb: mmdb put failed");
    }

    pub(crate) fn remove(&self, meta_prefix: PreBytes, key: &[u8]) {
        assert!(
            !self.read_only,
            "vsdb: remove is unavailable in read-only mode"
        );
        with_full_key(&meta_prefix, key, |k| self.shard(&meta_prefix).delete(k))
            .expect("vsdb: mmdb delete failed");
    }

    /// Marks a key for deferred removal during the next compaction.
    ///
    /// Unlike [`remove`](Self::remove), this does **not** write a
    /// tombstone immediately.  The key stays readable until mmdb's
    /// compaction filter physically drops it.
    ///
    /// mmdb keeps dead-key registrations in memory only — they are not
    /// persisted to the WAL or SSTs.  A restart before compaction loses
    /// them, so deletion is best-effort per process lifetime and callers
    /// must re-register after recovery.
    pub(crate) fn lazy_delete(&self, meta_prefix: PreBytes, key: &[u8]) {
        if self.read_only {
            return;
        }
        with_full_key(&meta_prefix, key, |k| {
            self.shard(&meta_prefix).lazy_delete(k)
        });
    }

    /// Batch version of [`lazy_delete`](Self::lazy_delete).
    ///
    /// All keys share the same prefix (and therefore the same shard).
    /// Triggers auto-compaction when the dead-key count crosses the
    /// threshold configured in `DbOptions`.
    pub(crate) fn lazy_delete_batch(
        &self,
        meta_prefix: PreBytes,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) {
        if self.read_only {
            return;
        }
        let shard = self.shard(&meta_prefix);
        let full_keys: Vec<Vec<u8>> = keys
            .into_iter()
            .map(|k| make_full_key(&meta_prefix, k.as_ref()))
            .collect();
        shard.lazy_delete_batch(full_keys);
    }

    pub(crate) fn iter(&self, meta_prefix: PreBytes) -> MmdbIter {
        self.range(meta_prefix, ..)
    }

    pub(crate) fn sync_wal(&self, meta_prefix: PreBytes) {
        self.try_sync_wal(meta_prefix)
            .expect("vsdb: mmdb WAL synchronization failed");
    }

    pub(crate) fn try_sync_wal(&self, meta_prefix: PreBytes) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        Self::sync_db_wal(self.shard(&meta_prefix))
    }

    pub(crate) fn try_sync_all_wals(&self) -> Result<()> {
        if !self.read_only {
            for db in &self.dbs {
                Self::sync_db_wal(db)?;
            }
        }
        Ok(())
    }

    fn sync_db_wal(db: &DB) -> Result<()> {
        // mmdb has no standalone WAL-sync API and skips empty batches.
        // A real synchronous deletion syncs prior successful writes on this
        // shard without forcing the active memtable into an SST. Earlier
        // rotated WALs have already been durably installed as SSTs before
        // their successful write requests return.
        db.delete_with_options(
            &WriteOptions {
                sync: true,
                ..WriteOptions::default()
            },
            &META_KEY_WAL_FENCE,
        )
        .map_err(from_mmdb_err)?;
        Ok(())
    }

    pub(crate) fn range<R: RawRangeBounds>(
        &self,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> MmdbIter {
        let db = self.shard(&meta_prefix);

        let snapshot = db.snapshot();
        Self::range_at(db, meta_prefix, bounds, snapshot.read_options())
    }

    fn range_at<R: RawRangeBounds>(
        db: &DB,
        meta_prefix: PreBytes,
        bounds: R,
        mut options: ReadOptions,
    ) -> MmdbIter {
        // mmdb intersects these bounds with the prefix and performs the seek.
        // In byte order the immediate successor of a key is key || 0.
        let endpoint = |bound: Bound<&[u8]>, lower: bool| {
            let (key, advance) = match bound {
                Bound::Included(key) => (key, !lower),
                Bound::Excluded(key) => (key, lower),
                Bound::Unbounded => return None,
            };
            let mut bytes = make_full_key(&meta_prefix, key);
            if advance {
                bytes.push(0);
            }
            Some(bytes)
        };
        let (lo, hi) = bounds.raw_bounds();
        options.iterate_lower_bound = endpoint(lo, true);
        options.iterate_upper_bound = endpoint(hi, false);
        let iter = db
            .iter_with_prefix(&meta_prefix, &options)
            .expect("vsdb: mmdb prefix range failed");
        MmdbIter(CheckedBidiIter(BidiIterator::lazy(iter)))
    }

    pub(crate) fn read_view(&self, prefix: PreBytes) -> MmdbReadView<'_> {
        let db = self.shard(&prefix);
        MmdbReadView {
            db,
            prefix,
            snapshot: db.snapshot(),
        }
    }

    pub(crate) fn batch_begin(&self, meta_prefix: PreBytes) -> MmdbBatch<'_> {
        MmdbBatch::new(meta_prefix, self)
    }

    /// Like [`batch_begin`](Self::batch_begin), but the batch is
    /// pre-staged with the removal of every existing key of the prefix
    /// (one range tombstone). See `MmdbBatch::new_wiped`.
    pub(crate) fn batch_begin_wiped(&self, meta_prefix: PreBytes) -> MmdbBatch<'_> {
        MmdbBatch::new_wiped(meta_prefix, self)
    }
}

// ---- Iterator ----

pub(crate) struct MmdbReadView<'a> {
    db: &'a DB,
    prefix: PreBytes,
    snapshot: Snapshot<'a>,
}

impl MmdbReadView<'_> {
    pub(crate) fn get(&self, key: &[u8]) -> Option<RawValue> {
        with_full_key(&self.prefix, key, |k| {
            self.db.get_with_options(&self.snapshot.read_options(), k)
        })
        .expect("vsdb: mmdb snapshot get failed")
    }

    pub(crate) fn range<R: RawRangeBounds>(&self, bounds: R) -> MmdbIter {
        MmDB::range_at(self.db, self.prefix, bounds, self.snapshot.read_options())
    }
}

// Check before filtering/type erasure: a failed source can coexist with an
// item from another source, so inspect errors after every advance, not only EOF.
struct CheckedBidiIter(BidiIterator);

impl CheckedBidiIter {
    fn check_error(&self) {
        if let Some(error) = self.0.error() {
            panic!("vsdb: mmdb iteration failed: {error}");
        }
    }
}

impl Iterator for CheckedBidiIter {
    type Item = (RawKey, RawValue);

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.0.next();
        self.check_error();
        item
    }
}

impl DoubleEndedIterator for CheckedBidiIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        let item = self.0.next_back();
        self.check_error();
        item
    }
}

/// A lazy, bidirectional iterator over key-value pairs in a single prefix range.
pub struct MmdbIter(CheckedBidiIter);

fn strip_prefix((mut key, value): (RawKey, RawValue)) -> (RawKey, RawValue) {
    key.drain(..PREFIX_SIZE);
    (key, value)
}

impl Iterator for MmdbIter {
    type Item = (RawKey, RawValue);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(strip_prefix)
    }
}

impl DoubleEndedIterator for MmdbIter {
    #[inline(always)]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(strip_prefix)
    }
}

// ---- Global prefix allocator (all namespaces) ----
//
// ONE allocator serves every engine instance in the process, so prefixes
// are unique across all namespaces by construction. Backing store: the
// ceiling file under the DEFAULT base dir. Free functions — the engine
// handles delegate here.

/// The allocator's durable backing store, always under the default base
/// dir (namespace roots never carry allocator state).
fn ceiling_file_path() -> PathBuf {
    vsdb_get_base_dir().join(PREFIX_CEILING_REL_PATH)
}

/// Strictly validates a completed dataset without opening or mutating it.
///
/// `CURRENT` is MMDB's recover-vs-create discriminator: a missing anchor
/// would make `DB::open` silently create an empty shard. Exact shard names,
/// marker compatibility, and every anchor must therefore agree before a
/// completed root is adopted or accepted as a relocation target.
pub(crate) fn validate_completed_dataset(root: &Path, shards: usize) -> Result<()> {
    let mmdb_dir = root.join("mmdb");
    let scan = scan_shard_layout(&mmdb_dir, shards)?;
    validate_shard_layout(
        &mmdb_dir,
        shards,
        true,
        &root.join(INIT_SENTINEL_REL_PATH),
        &scan,
    )?;
    if scan.present != shards {
        return Err(corrupt(format!(
            "dataset at {} is not complete: found {} of {} shard dirs",
            root.display(),
            scan.present,
            shards
        )));
    }
    validate_shard_anchors(&mmdb_dir, shards)
}

/// Computes the floor before opening any engines. Recovery scans all issued
/// prefixes, including empty maps whose only evidence is an embedded handle.
pub(crate) fn recover_ceiling(
    base: &Path,
    universe: u128,
    writable: bool,
) -> Result<u64> {
    let file = base.join(PREFIX_CEILING_REL_PATH);
    if let Some(floor) = read_ceiling_file(&file)? {
        return Ok(floor);
    }
    let max = highest_issued_prefix(base, universe)?;
    let floor = match max {
        Some(id) => id
            .checked_add(1)
            .ok_or_else(|| corrupt("prefix space exhausted"))?,
        None => PREFIX_ALLOC_START,
    };
    if writable {
        atomic_write_file(&file, &floor.to_le_bytes())?;
    }
    Ok(floor)
}

/// Allocates a fresh, never-before-issued prefix.
fn alloc_prefix() -> Result<Pre> {
    loop {
        let candidate = alloc_prefix_candidate()?;
        // Normal operation records no reservations, so allocation
        // stays lock-free here. `SeqCst` pairs with the `SeqCst`
        // cursor store in `alloc_prefix_candidate` and the flag
        // store + cursor loads in `reserve_recovered_prefix`: of the
        // two racing sides, at least one must observe the other
        // (Dekker), so a candidate issued concurrently with a
        // reservation is either checked against the set here or its
        // cursor advance is caught by the reserver's verify step.
        if !RECOVERED_NONEMPTY.load(Ordering::SeqCst) {
            return Ok(candidate);
        }
        let mut reserved = RECOVERED_PREFIXES.lock();
        if !reserved.remove(&candidate) {
            return Ok(candidate);
        }
        if reserved.is_empty() {
            RECOVERED_NONEMPTY.store(false, Ordering::SeqCst);
        }
    }
}

/// Loads the persisted allocator state into the process-wide atomics
/// (idempotent). `GLOBAL_FLOOR` snapshots the persisted value at
/// initialization: every prefix below it was issued by a previous run.
fn ensure_alloc_init() -> Result<()> {
    if GLOBAL_COUNTER.load(Ordering::Acquire) != 0 {
        return Ok(());
    }
    let _x = PREFIX_ALLOC_LOCK.lock();
    if GLOBAL_COUNTER.load(Ordering::Acquire) != 0 {
        return Ok(());
    }
    let mut start = crate::common::universe::active()?.ceiling;
    if !vsdb_is_read_only() {
        let _g = SYS_META_LOCK.lock();
        start = start.max(read_ceiling_file(&ceiling_file_path())?.unwrap_or(0));
    }
    // Publish only the existing floor. The first thread reserves its window
    // through the same durable refill path as every subsequent thread.
    GLOBAL_FLOOR.store(start, Ordering::Release);
    GLOBAL_CEILING.store(start, Ordering::Release);
    // The counter doubles as the init guard — store it last.
    GLOBAL_COUNTER.store(start, Ordering::Release);
    Ok(())
}

fn alloc_prefix_candidate() -> Result<Pre> {
    LOCAL_NEXT.with(|next_cell| {
        LOCAL_CEIL.with(|ceil_cell| {
            let next = next_cell.get();
            let ceil = ceil_cell.get();
            if next > 0 && next < ceil {
                next_cell.set(next + 1);
                // Keep the shared cursor in lock-step with `next_cell`
                // so `reserve_recovered_prefix` immediately sees this
                // value as already-issued from this point on. `SeqCst`
                // pairs with the `SeqCst` flag load in `alloc_prefix`
                // and the flag store + cursor loads in
                // `reserve_recovered_prefix` (see `alloc_prefix`).
                LOCAL_CURSOR.with(|cursor| cursor.store(next + 1, Ordering::SeqCst));
                return Ok(next);
            }

            ensure_alloc_init()?;

            // Ensure this thread's cleanup guard is registered
            // before taking a batch, so the corresponding
            // `PENDING_WINDOWS` entry inserted below is guaranteed
            // to be removed when the thread exits (bounding the
            // registry to currently-live threads instead of every
            // thread that ever allocated a prefix).
            PENDING_WINDOW_GUARD.with(|_| {});

            // Serialize refills so a failed durable reservation leaves the
            // batch available to retry, even at the end of the ID space.
            let _refill = PREFIX_ALLOC_LOCK.lock();
            let batch_start = GLOBAL_COUNTER.load(Ordering::Acquire);
            if batch_start == Pre::MAX {
                return Err(VsdbError::Other {
                    detail: "prefix allocator exhausted".into(),
                });
            }
            let batch_end = batch_start.saturating_add(PREFIX_ALLOC_BATCH);
            if batch_end > GLOBAL_CEILING.load(Ordering::Acquire) {
                let _g = SYS_META_LOCK.lock();
                let file = ceiling_file_path();
                let current = read_ceiling_file(&file)?.unwrap_or(0);
                let reserve = if batch_start == GLOBAL_FLOOR.load(Ordering::Relaxed) {
                    batch_end
                } else {
                    batch_end.saturating_add(PREFIX_ALLOC_BATCH)
                };
                let new_ceil = reserve.max(current);
                if current < new_ceil {
                    atomic_write_file(&file, &new_ceil.to_le_bytes())?;
                } else {
                    // A failed directory sync leaves the previous attempt's
                    // rename visible. Fence it before issuing any prefix.
                    fs::File::open(file.parent().expect("ceiling has a parent"))?
                        .sync_all()?;
                }
                GLOBAL_CEILING.store(new_ceil, Ordering::Release);
            }
            // Publish the counter and live cursor together so a concurrent
            // handle restore cannot miss this thread's reservable range.
            {
                let mut reg = PENDING_WINDOWS.write();
                GLOBAL_COUNTER.store(batch_end, Ordering::Release);
                let cursor = LOCAL_CURSOR.with(Arc::clone);
                cursor.store(batch_start + 1, Ordering::Release);
                reg.insert(thread::current().id(), (cursor, batch_end));
            }
            next_cell.set(batch_start + 1);
            ceil_cell.set(batch_end);
            Ok(batch_start)
        })
    })
}

/// Validates a prefix recovered through a *safe* restore path
/// (serde / `from_meta`), and reserves it when necessary.
///
/// Returns `false` when the prefix lies outside the allocator-issued
/// range (`< PREFIX_ALLOC_START` or `>= ceiling`) — such metadata cannot
/// come from a legitimately allocated instance — or when the verify
/// step below proves the prefix was issued to a fresh instance while
/// the reservation was being recorded (accepting it would alias two
/// live instances, violating INV-E1).
///
/// Accepted prefixes need a reservation only if the allocator could
/// still issue them in this run: `>= counter` (not yet claimed by any
/// batch), or at-or-past a registered thread's *live cursor* but still
/// below that window's end. Values below a thread's cursor were already
/// issued earlier in this same still-open batch and must NOT be
/// reserved — they can never recur as a future `alloc_prefix`
/// candidate, so reserving one would sit in `RECOVERED_PREFIXES`
/// forever (never matched, never evicted). Prefixes below the
/// process-start floor — the overwhelmingly common case for real
/// restores — are accepted with a few atomic loads and no locking,
/// keeping nested-handle decoding cheap and the reservation set
/// bounded.
///
/// # Synchronization
///
/// The whole evaluate → reserve → verify sequence runs under one
/// [`PENDING_WINDOWS`] read acquisition: batch claims (the only
/// writers of `GLOBAL_COUNTER`) and thread-exit window removals take
/// the write lock, so the counter and the window set are frozen for
/// the duration. Without the hold, a deschedule between evaluating
/// and inserting lets the allocator claim batches and issue `prefix`
/// to a new instance first — a prefix collision — while the late
/// insert leaves a permanent, never-evicted entry that degrades every
/// later `alloc_prefix` to the locked slow path.
///
/// Live-window *cursors* keep advancing lock-free, so after the
/// insert the window clause is re-checked (the mutex is held
/// throughout, so the allocator cannot consume the entry mid-verify):
/// a cursor that moved past `prefix` means the fast path issued it
/// without having seen the reservation — the dead entry is evicted
/// and the restore rejected. The `SeqCst` pairing described in
/// [`alloc_prefix`] guarantees the two sides cannot miss each other.
fn reserve_recovered_prefix(meta_prefix: PreBytes) -> Result<bool> {
    let prefix = Pre::from_le_bytes(meta_prefix);
    if prefix < PREFIX_ALLOC_START {
        return Ok(false);
    }
    ensure_alloc_init()?;

    if prefix >= GLOBAL_CEILING.load(Ordering::Acquire) {
        return Ok(false);
    }
    if prefix < GLOBAL_FLOOR.load(Ordering::Acquire) {
        // Issued by a previous run — can never be issued again.
        return Ok(true);
    }

    let reg = PENDING_WINDOWS.read();
    let unissued = || {
        // The counter clause cannot change while `reg` is held (its
        // only writer runs under the write lock); only live cursors
        // move, monotonically forward.
        prefix >= GLOBAL_COUNTER.load(Ordering::Acquire)
            || reg.values().any(|(cursor, end)| {
                prefix >= cursor.load(Ordering::SeqCst) && prefix < *end
            })
    };

    if !unissued() {
        // Already issued in this run — the same-run save/restore flow.
        // The allocator can never produce it again, so no reservation
        // is needed (recording one would leak the entry permanently).
        return Ok(true);
    }

    let mut reserved = RECOVERED_PREFIXES.lock();
    #[cfg(test)]
    if let Some(advance_cursor) = tests::AFTER_RESERVATION_LOCK.take() {
        advance_cursor();
    }
    if !reserved.insert(prefix) {
        // A prior restore already owns this reservation. If the allocator's
        // cursor advances now, it must still consume and skip that candidate.
        return Ok(true);
    }
    RECOVERED_NONEMPTY.store(true, Ordering::SeqCst);

    if unissued() {
        return Ok(true);
    }
    // A live window's fast path issued `prefix` to a fresh instance
    // before the reservation landed (it cannot have *consumed* the
    // reservation instead: the set mutex has been held since the
    // insert). Evict the dead entry — nothing would ever match it —
    // and reject the restore.
    reserved.remove(&prefix);
    if reserved.is_empty() {
        RECOVERED_NONEMPTY.store(false, Ordering::SeqCst);
    }
    Ok(false)
}

// ---- Batch ----

fn note_issued_prefix(max: &mut Option<Pre>, prefix: Pre) {
    if prefix >= PREFIX_ALLOC_START {
        *max = Some(max.map_or(prefix, |current| current.max(prefix)));
    }
}

fn note_db_prefixes(db: &DB, max: &mut Option<Pre>) -> Result<()> {
    let mut iter = db.iter().map_err(from_mmdb_err)?;
    for (key, value) in iter.by_ref() {
        if key.len() >= PREFIX_SIZE {
            let prefix = Pre::from_le_bytes(key[..PREFIX_SIZE].try_into().unwrap());
            note_issued_prefix(max, prefix);
            note_meta_bytes(max, &key[PREFIX_SIZE..]);
        }
        // Either side of a stored pair may carry the only handle to an empty map.
        note_meta_bytes(max, &value);
    }
    if let Some(error) = iter.error() {
        return Err(corrupt(error));
    }
    Ok(())
}

fn note_meta_bytes(max: &mut Option<Pre>, bytes: &[u8]) {
    for (pos, magic) in bytes.windows(8).enumerate() {
        let offset = match magic {
            b"VSMAPX02" => 8,
            b"VSTYPE04" => 16,
            _ => continue,
        };
        if let Some(raw) = bytes.get(pos + offset..pos + offset + PREFIX_SIZE) {
            note_issued_prefix(max, Pre::from_le_bytes(raw.try_into().unwrap()));
        }
    }
}

fn note_meta_dir(max: &mut Option<Pre>, dir: &Path, instance_meta: bool) -> Result<()> {
    let mut pending = vec![dir.to_owned()];
    let mut buf = vec![0; 64 * 1024];
    while let Some(path) = pending.pop() {
        let entries = match fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(e) if path == dir && e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let entry = entry?;
            let ty = entry.file_type()?;
            if ty.is_dir() {
                pending.push(entry.path());
            } else if ty.is_file() {
                // Application metadata may be opaque. The filename itself
                // reserves the ID, even for an empty instance.
                if instance_meta
                    && path == dir
                    && let Some(name) = entry.file_name().to_str()
                    && name.len() == 16
                    && let Ok(id) = Pre::from_str_radix(name, 16)
                {
                    note_issued_prefix(max, id);
                }
                let mut file = fs::File::open(entry.path())?;
                note_meta_stream(max, &mut file, &mut buf)?;
            }
        }
    }
    Ok(())
}

fn note_meta_stream(
    max: &mut Option<Pre>,
    reader: &mut impl Read,
    buf: &mut [u8],
) -> io::Result<()> {
    let mut carry = 0;
    loop {
        let read = match reader.read(&mut buf[carry..]) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        let end = carry + read;
        note_meta_bytes(max, &buf[..end]);
        // A magic and prefix may straddle two reads.
        carry = end.min(16 + PREFIX_SIZE - 1);
        buf.copy_within(end - carry..end, 0);
    }
}

fn note_shard_dir(max: &mut Option<Pre>, shard: &Path) -> Result<()> {
    if !shard.is_dir() {
        return Ok(());
    }
    // Read-only: recovery must not create a shard or take a write lock
    // while the caller already holds the default engine.
    let db = DB::open_read_only_with_options(
        DbOptions {
            prefix_len: PREFIX_SIZE,
            ..DbOptions::default()
        },
        shard,
    )
    .map_err(|e| {
        from_mmdb_err(
            e.context(format!("recover prefix ceiling at {}", shard.display())),
        )
    })?;
    note_db_prefixes(&db, max)
}

fn note_engine_tree(max: &mut Option<Pre>, root: &Path) -> Result<()> {
    note_meta_dir(
        max,
        &root.join("__SYSTEM__").join("__instance_meta__"),
        true,
    )?;
    let mmdb_dir = root.join("mmdb");
    let entries = match fs::read_dir(&mmdb_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("shard_") {
            note_shard_dir(max, &entry.path())?;
        }
    }
    Ok(())
}

/// Called under universe initialization, before any engine is open.
fn highest_issued_prefix(base_dir: &Path, universe: u128) -> Result<Option<Pre>> {
    let mut max = None;
    if base_dir.join(READY_PATH).try_exists()?
        || base_dir
            .join("mmdb/__SYSTEM__/engine_header")
            .try_exists()?
    {
        note_engine_tree(&mut max, base_dir)?;
    } else {
        // An incomplete default engine has never served a writable map.
        note_meta_dir(
            &mut max,
            &base_dir.join("__SYSTEM__/__instance_meta__"),
            true,
        )?;
    }
    for root in crate::common::namespace::recovery_roots(base_dir, universe)? {
        note_engine_tree(&mut max, &root)?;
    }
    // Top-level handles are bootstrapped from `{base}/__CUSTOM__/`
    // (see `vsdb_get_custom_dir`), often before those maps have keys.
    note_meta_dir(&mut max, &base_dir.join("__CUSTOM__"), false)?;
    Ok(max)
}

/// Reads the ceiling file: 8-byte little-endian `u64`.
///
/// `None` when absent. Absence is not itself a fresh universe: callers
/// recover issued prefixes from the datasets before publishing a new
/// floor. Any other read/format problem is an error — the ceiling
/// guards prefix uniqueness, so a truncated or padded file must abort
/// the open, never be guessed at.
fn read_ceiling_file(path: &Path) -> Result<Option<Pre>> {
    match fs::read(path) {
        Ok(bytes) if bytes.len() == PREFIX_SIZE => {
            let ceiling = crate::common::parse_prefix!(bytes);
            if ceiling < PREFIX_ALLOC_START {
                return Err(corrupt("allocator ceiling below the allocation floor"));
            }
            Ok(Some(ceiling))
        }
        Ok(bytes) => Err(corrupt(format!(
            "corrupt allocator ceiling file {} ({} bytes, expected {})",
            path.display(),
            bytes.len(),
            PREFIX_SIZE
        ))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Exact shard-set scan of `mmdb_dir` for an expected count of
/// `shards`: how many of the expected `shard_00..shard_{N-1}` dirs are
/// present, and whether any unexpected `shard_*` entry exists (wrong
/// index, non-directory, or misnamed) — counting prefixes alone would
/// let `shard_backup` + `shard_00` masquerade as a complete 2-shard set.
struct ShardScan {
    present: usize,
    unexpected: Option<String>,
}

fn scan_shard_layout(mmdb_dir: &Path, shards: usize) -> Result<ShardScan> {
    let mut present = 0usize;
    let mut unexpected = None;
    let expected: HashSet<String> =
        (0..shards).map(|i| format!("shard_{:02}", i)).collect();
    match fs::read_dir(mmdb_dir) {
        Ok(entries) => {
            for e in entries {
                let e = e?;
                let name = e.file_name().to_string_lossy().into_owned();
                if !name.starts_with("shard_") {
                    continue;
                }

                if expected.contains(&name) && e.path().is_dir() {
                    present += 1;
                } else if unexpected.is_none() {
                    unexpected = Some(name);
                }
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    Ok(ShardScan {
        present,
        unexpected,
    })
}

fn validate_shard_anchors(mmdb_dir: &Path, shards: usize) -> Result<()> {
    for i in 0..shards {
        let current = mmdb_dir.join(format!("shard_{i:02}")).join("CURRENT");
        if !current.is_file() {
            return Err(corrupt(format!(
                "damaged dataset at {}: shard_{i:02} has no MMDB CURRENT \
                 anchor — refusing to recreate it as an empty shard",
                mmdb_dir.display()
            )));
        }
    }
    Ok(())
}

/// Rejects opening a root whose shard layout contradicts its lifecycle
/// state — routing is `prefix % shard_count`, so opening a wrong or
/// incomplete shard set silently re-routes (or loses) prefixes.
///
/// Three states, keyed on the format marker (written only AFTER every
/// shard exists) and the initialization sentinel (written durably
/// BEFORE the first shard of a brand-new root):
///
/// * **Marker present** ⇒ initialization once completed ⇒ the exact
///   expected shard set must exist — anything else (including ZERO
///   shard dirs: e.g. a manually deleted `mmdb/`) is damage and is
///   refused, never silently reinitialized.
/// * **Sentinel present (no marker)** ⇒ provably a create that crashed
///   mid-way; no allocation ever targeted the root ⇒ resumable: the
///   missing shard dirs are created idempotently by the caller.
/// * **Neither** permits a fresh root without shard directories.
fn validate_shard_layout(
    mmdb_dir: &Path,
    shards: usize,
    marker_present: bool,
    sentinel_path: &Path,
    scan: &ShardScan,
) -> Result<()> {
    if let Some(name) = &scan.unexpected {
        return Err(corrupt(format!(
            "unexpected shard entry {:?} at {} (expected exactly \
             shard_00..shard_{:02})",
            name,
            mmdb_dir.display(),
            shards - 1
        )));
    }
    if marker_present {
        if scan.present != shards {
            return Err(corrupt(format!(
                "damaged dataset at {}: initialization completed (format \
                 marker present) but only {} of {} shard dirs exist — \
                 refusing to reinitialize over it",
                mmdb_dir.display(),
                scan.present,
                shards
            )));
        }
        return validate_shard_anchors(mmdb_dir, shards);
    }
    if sentinel_path.exists() {
        // Resumable create-crash; present <= shards is guaranteed by
        // the exact-set scan above.
        return Ok(());
    }
    if scan.present == 0 {
        return Ok(());
    }
    Err(corrupt(format!(
        "damaged dataset at {}: {} of {} shard dirs exist with no \
         initialization sentinel — missing shards would mean silent \
         data loss; refusing to open",
        mmdb_dir.display(),
        scan.present,
        shards
    )))
}

/// An atomic write batch over one prefix (hence one shard).
pub(crate) struct MmdbBatch<'a> {
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

    /// A batch whose first entry is a range tombstone covering the whole
    /// prefix range. Entries within one batch carry position-based
    /// sequence numbers, so operations added afterwards apply on top of
    /// the wipe, and the whole set commits atomically.
    fn new_wiped(meta_prefix: PreBytes, engine: &'a MmDB) -> Self {
        let mut b = Self::new(meta_prefix, engine);
        match prefix_successor(&meta_prefix) {
            Some(end) => b.inner.delete_range(&meta_prefix, &end),
            None => {
                // Unreachable in practice (an all-0xFF prefix would require
                // exhausting the u64 prefix space), but stay correct: stage
                // an individual delete for every existing key. Sound under
                // the SWMR contract — no other writer can add keys between
                // building and committing the batch.
                for (k, _) in engine.iter(meta_prefix) {
                    b.remove(&k);
                }
            }
        }
        b
    }
}

impl MmdbBatch<'_> {
    #[inline(always)]
    pub(crate) fn insert(&mut self, key: &[u8], value: &[u8]) {
        with_full_key(&self.meta_prefix, key, |k| self.inner.put(k, value));
    }

    #[inline(always)]
    pub(crate) fn remove(&mut self, key: &[u8]) {
        with_full_key(&self.meta_prefix, key, |k| self.inner.delete(k));
    }

    pub(crate) fn delete_range(&mut self, start: &[u8], end: &[u8]) {
        if start < end {
            with_full_key(&self.meta_prefix, start, |lo| {
                with_full_key(&self.meta_prefix, end, |hi| {
                    self.inner.delete_range(lo, hi)
                })
            });
        }
    }

    /// Applies every buffered operation atomically. The batch is consumed
    /// either way. A persistence failure may leave recoverable WAL data.
    #[inline(always)]
    pub(crate) fn commit(self) -> Result<()> {
        self.commit_with_sync(false)
    }

    pub(crate) fn commit_sync(self) -> Result<()> {
        self.commit_with_sync(true)
    }

    fn commit_with_sync(self, sync: bool) -> Result<()> {
        let batch = self.inner;
        if self.engine.read_only {
            return Err(crate::common::error::VsdbError::ReadOnly {
                operation: "batch commit",
            });
        }
        self.engine
            .shard(&self.meta_prefix)
            .write_with_options(
                &WriteOptions {
                    sync,
                    ..Default::default()
                },
                batch,
            )
            .map_err(from_mmdb_err)?;
        Ok(())
    }
}

// ---- Helpers ----

fn with_full_key<R>(prefix: &PreBytes, key: &[u8], f: impl FnOnce(&[u8]) -> R) -> R {
    let mut buf = [0u8; 128];
    if key.len() > buf.len() - PREFIX_SIZE {
        return f(&make_full_key(prefix, key));
    }
    let len = PREFIX_SIZE + key.len();
    buf[..PREFIX_SIZE].copy_from_slice(prefix);
    buf[PREFIX_SIZE..len].copy_from_slice(key);
    f(&buf[..len])
}

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

const G: usize = GB as usize;

/// Default memory budget for the DEFAULT namespace, in MB: a fixed
/// 2 GiB. vsdb deliberately does NOT size itself from the host's RAM
/// or its cgroup: the library cannot know how much of either belongs
/// to it, and a moment-in-time reading bakes deployment noise into
/// engine sizing. A fixed default makes the sizing input predictable;
/// applications can tune it with `VsdbOptions::with_mem_budget_mb` or
/// `VSDB_MEM_BUDGET_MB`. Floors/caps and allocations outside those caches
/// mean it is not a hard RSS limit; performance depends on the workload.
const DEFAULT_MEM_BUDGET_MB: usize = 2048;

/// Resolve the effective DEFAULT-namespace budget in bytes.
///
/// - `budget_mb` (`VsdbOptions::mem_budget_mb`, else `VSDB_MEM_BUDGET_MB`),
///   when present, non-zero, and expressible in bytes, is applied
///   verbatim — the operator asked for that exact number; it is the ONLY
///   way to grow (or shrink) the default engine's memory.
/// - Otherwise the fixed [`DEFAULT_MEM_BUDGET_MB`] default stands.
///
/// Cache and write-buffer sizing derive from this input, subject to
/// floors and caps (see `mmdb_open`); it is not a hard memory limit.
fn effective_mem_budget(budget_mb: Option<usize>) -> usize {
    budget_mb
        .filter(|&mb| mb > 0)
        .and_then(|mb| mb.checked_mul(1024 * 1024))
        .unwrap_or(DEFAULT_MEM_BUDGET_MB * 1024 * 1024)
}

/// The effective budget for this process, computed once: every shard
/// must size off the same number, and `MmDB::new` opens `NUM_SHARDS`
/// shards -- re-parsing the env override per shard would be redundant.
static MEM_BUDGET: LazyLock<usize> = LazyLock::new(|| {
    let budget_mb = configured_mem_budget_mb().or_else(|| {
        std::env::var("VSDB_MEM_BUDGET_MB")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
    });

    effective_mem_budget(budget_mb)
});

#[derive(Clone, Copy)]
pub(crate) struct EngineSizing {
    mem_budget: usize,
}

impl EngineSizing {
    /// The sizing used by the default namespace.
    pub(crate) fn from_process_budget() -> Self {
        Self {
            mem_budget: *MEM_BUDGET,
        }
    }

    /// Explicit budget.
    pub(crate) fn from_budget_mb(mb: usize) -> Self {
        Self {
            mem_budget: mb.saturating_mul(1024 * 1024).max(8 * 1024 * 1024),
        }
    }
}

/// Per-shard block-cache capacity under the static split — kept as its
/// own function because the engine-level pool (tier (i)) must size
/// itself to exactly `shards ×` this value so pooling changes cache
/// *allocation*, never engine totals.
///
/// Floored alongside the write-buffer floor in `mmdb_open`: a
/// degenerate budget must degrade to a small-but-functional cache,
/// never to `block_cache_capacity: 0`, which mmdb treats as "caching
/// disabled entirely".
fn per_shard_block_cache_size(sizing: &EngineSizing, shards: usize) -> u64 {
    cmp::max(
        (sizing.mem_budget as u64) / 8 / shards as u64,
        4 * 1024 * 1024,
    )
}

fn mmdb_open(
    dir: &Path,
    shards: usize,
    sizing: EngineSizing,
    pool: &Arc<BlockCachePool>,
) -> Result<DB> {
    let EngineSizing { mem_budget } = sizing;
    let read_only = vsdb_is_read_only();

    // Per-shard sizes: divide totals by the shard count.
    //
    // In `mmdb 4.3`, `max_immutable_memtables` is unused: when a shard's
    // active memtable fills, the write leader synchronously runs
    // `freeze_memtable_sync` + `flush_frozen_memtable`, bounding per-shard
    // memtable residency to <= 2x `wr_buffer_size` (active + one flushing
    // table) while releasing `DB::inner` during SST I/O. Keeping the total
    // active budget at `mem_budget / 8` across all shards (<= 2/8 of budget
    // at peak multi-shard flush, alongside `mem_budget / 8` for
    // `BlockCachePool`, `<= 1.56%` per active `InternalPageCache`, and
    // compaction/query transients) avoids pinning anonymous memory at
    // cgroup `memory.high` during multi-shard ingest bursts, where reclaim
    // pressure stalls the flush writers that are the only way out
    // (previously observed as tens of thousands of `memory.events:high`).
    // Write-intensive single-prefix (`VerMap`) or co-located (`DagMap`)
    // workloads can place hot instances in a dedicated 1-shard `Namespace`
    // before any default `wr_buffer_size` recalibration (see
    // `docs/proposals/v18-roadmap.md` §10.7). The first term preserves the
    // historical shape (`G / shards`, opening to `budget / 4 / shards` past
    // 16 GiB, capped at 512 MiB) for large explicit budgets.
    let legacy_wr = cmp::min(
        if mem_budget > 16 * G {
            mem_budget / 4 / shards
        } else {
            G / shards
        },
        512 * 1024 * 1024,
    );
    let wr_buffer_size = cmp::max(
        cmp::min(legacy_wr, mem_budget / 8 / shards),
        4 * 1024 * 1024,
    );

    // Per-shard capacity of the static split — retained in the options
    // as belt-and-braces (mmdb documents it as IGNORED when a shared
    // pool is attached; if the pool wiring were ever removed, each
    // shard would fall back to exactly this private capacity).
    let block_cache_size = per_shard_block_cache_size(&sizing, shards);

    // Writable opens use one compaction thread per shard. MMDB's native
    // read-only open overrides this to zero and starts no background worker.
    let opts = DbOptions {
        create_if_missing: !read_only,
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

        // Write buffer (per-shard)
        write_buffer_size: wr_buffer_size,
        max_immutable_memtables: 4,

        // Block cache: one pool per engine, shared by its shards (the
        // per-shard capacity above is documentation/fallback only).
        block_cache: Some(pool.clone()),
        block_cache_capacity: block_cache_size,
        block_size: 16 * 1024, // 16 KB

        // Compaction tuning. Keep the compaction trigger well below mmdb's
        // write-slowdown trigger (8): with both at 8, the instant L0 becomes
        // compactable every write already pays the slowdown penalty — no
        // buffer zone for background compaction to work in.
        l0_compaction_trigger: 4,
        max_subcompactions: 4,

        // Single compaction thread per shard — shard fan-out gives natural parallelism
        max_background_compactions: 1,

        ..DbOptions::default()
    };

    if read_only {
        DB::open_read_only_with_options(opts, dir).map_err(from_mmdb_err)
    } else {
        DB::open(opts, dir).map_err(from_mmdb_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn ceiling_scan_retries_interrupted_reads() {
        struct InterruptedOnce(bool);
        impl Read for InterruptedOnce {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                if std::mem::take(&mut self.0) {
                    Err(io::ErrorKind::Interrupted.into())
                } else {
                    Ok(0)
                }
            }
        }

        let map = crate::MapxRaw::new();
        let bytes = postcard::to_allocvec(&map).unwrap();
        let mut reader = InterruptedOnce(true).chain(bytes.as_slice());
        let mut max = None;
        note_meta_stream(&mut max, &mut reader, &mut [0; 32]).unwrap();
        assert_eq!(max, Some(map.instance_id().map_id));
    }

    #[test]
    fn ceiling_scan_carries_the_full_typed_prefix_across_reads() {
        for split in 1..24 {
            let mut data = vec![0; 64 - split];
            data.extend_from_slice(b"VSTYPE04");
            data.extend_from_slice(&[0xab; 8]);
            data.extend_from_slice(&(PREFIX_ALLOC_START + 42).to_le_bytes());
            let mut max = None;
            note_meta_stream(&mut max, &mut data.as_slice(), &mut [0; 64]).unwrap();
            assert_eq!(max, Some(PREFIX_ALLOC_START + 42));
        }
    }

    #[test]
    fn ceiling_scan_observes_a_handle_in_a_user_key() {
        let handle = crate::MapxRaw::new();
        let bytes = postcard::to_allocvec(&handle).unwrap();
        let id = handle.instance_id().map_id;
        let dir = tmp_dir("handle-key");
        let db = DB::open(DbOptions::default(), &dir).unwrap();
        db.put(&make_full_key(&(id - 1).to_le_bytes(), &bytes), b"label")
            .unwrap();
        let mut max = None;
        note_db_prefixes(&db, &mut max).unwrap();
        drop(db);
        fs::remove_dir_all(dir).unwrap();
        assert_eq!(max, Some(id));
    }

    #[test]
    fn opaque_instance_metadata_reserves_its_filename() {
        let dir = tmp_dir("opaque-meta");
        fs::create_dir_all(&dir).unwrap();
        let id = PREFIX_ALLOC_START + 3;
        fs::write(dir.join(format!("{id:016x}")), b"opaque").unwrap();
        let mut max = None;
        note_meta_dir(&mut max, &dir, true).unwrap();
        assert_eq!(max, Some(id));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ceiling_scan_observes_a_handle_in_a_nested_bootstrap_file() {
        let handle = crate::MapxRaw::new();
        let bytes = postcard::to_allocvec(&handle).unwrap();
        let dir = tmp_dir("nested-bootstrap");
        let app = dir.join("app");
        fs::create_dir_all(&app).unwrap();
        let mut file = vec![0; 64 * 1024 - 5];
        file.extend_from_slice(&bytes);
        fs::write(app.join("root"), file).unwrap();
        let mut max = None;
        note_meta_dir(&mut max, &dir, false).unwrap();
        fs::remove_dir_all(dir).unwrap();
        assert_eq!(max, Some(handle.instance_id().map_id));
    }

    #[test]
    fn synchronous_batch_reports_closed_engine() {
        let dir = tmp_dir("sync-batch-fault");
        let db = DB::open(DbOptions::default(), &dir).unwrap();
        db.close().unwrap();
        let engine = MmDB {
            dbs: vec![db].into_boxed_slice(),
            read_only: false,
            mem_budget: 8 * 1024 * 1024,
        };
        let mut batch = engine.batch_begin(42u64.to_le_bytes());
        batch.insert(b"receipt", b"unacknowledged");
        let error = batch.commit_sync().unwrap_err();
        assert!(matches!(
            error,
            crate::VsdbError::Storage {
                kind: crate::common::error::StorageErrorKind::DbClosed,
                ..
            }
        ));
        assert!(std::error::Error::source(&error).is_some());
        drop(engine);
        let db = DB::open(DbOptions::default(), &dir).unwrap();
        assert!(
            db.get(&make_full_key(&42u64.to_le_bytes(), b"receipt"))
                .unwrap()
                .is_none()
        );
        drop(db);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn streaming_scans_surface_late_block_corruption() {
        let dir = tmp_dir("iterator-corruption");
        let prefix = 42u64.to_be_bytes();
        let opts = DbOptions {
            create_if_missing: true,
            prefix_len: PREFIX_SIZE,
            block_size: 1024,
            block_cache_capacity: 0,
            pin_l0_filter_and_index_blocks_in_cache: false,
            compression: CompressionType::None,
            ..DbOptions::default()
        };
        let db = DB::open(opts.clone(), &dir).unwrap();
        for i in 0..64u8 {
            db.put(&make_full_key(&prefix, &[i]), &[i; 256]).unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
        drop(db);

        // Damage only a middle value in this test's uncompressed SST. The
        // footer/index and end blocks remain readable, so both directions
        // can start successfully before encountering a checksum failure.
        let ssts: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "sst"))
            .collect();
        assert_eq!(ssts.len(), 1);
        let mut bytes = fs::read(&ssts[0]).unwrap();
        let offset = bytes.windows(256).position(|v| v == [32u8; 256]).unwrap();
        bytes[offset + 128] ^= 1;
        fs::write(&ssts[0], bytes).unwrap();

        let engine = MmDB {
            dbs: vec![DB::open_read_only_with_options(opts, &dir).unwrap()]
                .into_boxed_slice(),
            read_only: true,
            mem_budget: 8 * 1024 * 1024,
        };
        for mode in 0..3 {
            for reverse in [false, true] {
                let mut seen = 0;
                let failure = catch_unwind(AssertUnwindSafe(|| {
                    let view = (mode == 2).then(|| engine.read_view(prefix));
                    let mut iter = match &view {
                        Some(view) => view.range(..),
                        None if mode == 1 => engine.range(prefix, ..),
                        None => engine.iter(prefix),
                    };
                    while if reverse {
                        iter.next_back()
                    } else {
                        iter.next()
                    }
                    .is_some()
                    {
                        seen += 1;
                    }
                }));
                assert!(seen > 0, "failure must occur after successful reads");
                let error =
                    failure.expect_err("corrupt scan must not finish successfully");
                let message = error
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| error.downcast_ref::<&str>().copied())
                    .unwrap();
                assert!(message.contains("vsdb: mmdb iteration failed"), "{message}");
            }
        }
        let view = engine.read_view(prefix);
        assert!(catch_unwind(AssertUnwindSafe(|| view.get(&[32]))).is_err());
        drop(view);
        drop(engine);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn effective_mem_budget_semantics() {
        const MB: usize = 1024 * 1024;

        // No override: the fixed 2 GiB default stands.
        assert_eq!(effective_mem_budget(None), 2 * G);

        // The explicit override is verbatim and authoritative, in
        // both directions.
        assert_eq!(effective_mem_budget(Some(512)), 512 * MB);
        assert_eq!(effective_mem_budget(Some(64 * 1024)), 64 * G);

        // Degenerate overrides (zero, or too large to express in
        // bytes) are ignored, not applied.
        assert_eq!(effective_mem_budget(Some(0)), 2 * G);
        assert_eq!(effective_mem_budget(Some(usize::MAX)), 2 * G);
    }

    #[test]
    fn ceiling_file_requires_exact_width() {
        let base = tmp_dir("ceiling");
        fs::create_dir_all(&base).unwrap();
        let f = base.join("__prefix_ceiling__");
        assert_eq!(read_ceiling_file(&f).unwrap(), None);
        atomic_write_file(&f, &(PREFIX_ALLOC_START + 42).to_le_bytes()).unwrap();
        assert_eq!(
            read_ceiling_file(&f).unwrap(),
            Some(PREFIX_ALLOC_START + 42)
        );
        fs::write(&f, (PREFIX_ALLOC_START - 1).to_le_bytes()).unwrap();
        assert!(read_ceiling_file(&f).is_err());
        fs::write(&f, [0u8; 3]).unwrap();
        assert!(read_ceiling_file(&f).is_err());
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn open_at_succeeds_when_sentinel_unremovable() {
        // After the format marker lands the sentinel is advisory. A
        // directory at the sentinel path makes `remove_file` fail
        // (IsADirectory) while leaving a complete marked root — open
        // must still succeed and serve data.
        let root = tmp_dir("stuck-sentinel");
        let shards = 2;
        let sizing = EngineSizing::from_budget_mb(64);
        let db = MmDB::open_at(&root, shards, sizing, 1).unwrap();
        let prefix = 1u64.to_le_bytes();
        db.insert(prefix, b"k", b"v");
        db.close().unwrap();

        let sentinel = root.join(INIT_SENTINEL_REL_PATH);
        fs::create_dir_all(&sentinel).unwrap();

        let db = MmDB::open_at(&root, shards, sizing, 1).unwrap();
        assert_eq!(db.get(prefix, b"k").as_deref(), Some(&b"v"[..]));
        db.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn engine_header_before_ready_resumes_with_matching_initializer() {
        let root = tmp_dir("header-before-ready");
        let shards = 1;
        let ns = 1;
        let open = || MmDB::open_at(&root, shards, EngineSizing::from_budget_mb(64), ns);
        let db = open().unwrap();
        db.insert(PREFIX_ALLOC_START.to_le_bytes(), b"k", b"preserved");
        db.close().unwrap();
        let marker = root.join(READY_PATH);
        let sentinel = root.join(INIT_SENTINEL_REL_PATH);
        fs::write(&marker, b"torn").unwrap();
        assert!(open().is_err());
        fs::remove_file(&marker).unwrap();
        assert!(open().is_err());
        let id = crate::common::universe::active().unwrap().id;
        fs::write(
            &sentinel,
            crate::common::universe::engine_bytes(id, ns + 1, shards),
        )
        .unwrap();
        assert!(open().is_err());
        fs::write(
            &sentinel,
            crate::common::universe::engine_bytes(id, ns, shards),
        )
        .unwrap();
        let db = open().unwrap();
        assert_eq!(
            db.get(PREFIX_ALLOC_START.to_le_bytes(), b"k").as_deref(),
            Some(b"preserved".as_slice())
        );
        assert_eq!(fs::read(marker).unwrap(), b"18");
        db.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn shard_layout_lifecycle_states() {
        let base = tmp_dir("shard-layout");
        let mmdb_dir = base.join("mmdb");
        let sentinel = base.join(INIT_SENTINEL_REL_PATH);
        let check = |shards: usize, marker: bool| {
            scan_shard_layout(&mmdb_dir, shards).and_then(|scan| {
                validate_shard_layout(&mmdb_dir, shards, marker, &sentinel, &scan)
            })
        };

        // Absent dir: brand-new root, fine without a marker; with a
        // marker it is damage (a completed dataset lost its mmdb dir)
        // and must NOT be silently reinitialized.
        assert!(check(4, false).is_ok());
        assert!(check(4, true).is_err());

        // Partial set, no marker, NO sentinel: a damaged dataset (e.g.
        // a legacy 16-shard base missing dirs) — refused, because
        // "resuming" it would silently lose the missing shards' data.
        fs::create_dir_all(mmdb_dir.join("shard_00")).unwrap();
        assert!(check(4, false).is_err());
        // The same partial set WITH the sentinel is a provable
        // create-crash: resumable.
        fs::create_dir_all(sentinel.parent().unwrap()).unwrap();
        fs::write(&sentinel, "1").unwrap();
        assert!(check(4, false).is_ok());
        fs::remove_file(&sentinel).unwrap();

        // Exact match is complete only when every shard carries MMDB's
        // recover-vs-create anchor.
        fs::create_dir_all(mmdb_dir.join("shard_01")).unwrap();
        fs::write(mmdb_dir.join("shard_00/CURRENT"), "MANIFEST-000001").unwrap();
        fs::write(mmdb_dir.join("shard_01/CURRENT"), "MANIFEST-000001").unwrap();
        assert!(check(2, false).is_err());
        assert!(check(2, true).is_ok());
        fs::remove_file(mmdb_dir.join("shard_01/CURRENT")).unwrap();
        assert!(check(2, false).is_err());
        assert!(check(2, true).is_err());
        fs::write(mmdb_dir.join("shard_01/CURRENT"), "MANIFEST-000001").unwrap();

        // Marker present + missing shards: damage, refused.
        assert!(check(4, true).is_err());

        // More shard dirs than expected = unexpected names: refused
        // unconditionally (shard_01 is not in a 1-shard set).
        assert!(check(1, false).is_err());
        assert!(check(1, true).is_err());

        // Non-directory entry occupying an expected name: refused.
        fs::write(mmdb_dir.join("shard_02"), "junk").unwrap();
        assert!(check(3, false).is_err());
        // Misnamed shard entry: refused even when the count matches.
        fs::remove_file(mmdb_dir.join("shard_02")).unwrap();
        fs::create_dir_all(mmdb_dir.join("shard_zz")).unwrap();
        assert!(check(2, false).is_err());

        let _ = fs::remove_dir_all(&base);
    }

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
        let db = mmdb_open(
            &dir,
            NUM_SHARDS,
            EngineSizing::from_process_budget(),
            &Arc::new(BlockCachePool::new(64 * 1024 * 1024)),
        )
        .unwrap();

        db.put(b"hello", b"world").unwrap();
        assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));

        db.delete(b"hello").unwrap();
        assert_eq!(db.get(b"hello").unwrap(), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mmdb_prefix_iteration() {
        let dir = tmp_dir("prefix-iter");
        let db = mmdb_open(
            &dir,
            NUM_SHARDS,
            EngineSizing::from_process_budget(),
            &Arc::new(BlockCachePool::new(64 * 1024 * 1024)),
        )
        .unwrap();

        let prefix_a: PreBytes = 1_u64.to_le_bytes();
        let prefix_b: PreBytes = 2_u64.to_le_bytes();

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

    #[test]
    fn pending_window_guard_reclaims_entry_on_thread_exit() {
        // Directly exercises the Drop-based cleanup mechanism (without
        // needing a full `MmDB`/DB setup): a thread that registers a
        // `PENDING_WINDOWS` entry and touches its cleanup guard must
        // have that entry removed once the thread exits — otherwise
        // the registry would grow by one entry per historical thread
        // for the life of the process.
        let handle = thread::spawn(|| {
            let id = thread::current().id();
            PENDING_WINDOWS
                .write()
                .insert(id, (Arc::new(AtomicU64::new(0)), 1));
            // Registers this thread's `PendingWindowGuard`, whose
            // `Drop` removes the entry above on thread exit.
            PENDING_WINDOW_GUARD.with(|_| {});
            assert!(PENDING_WINDOWS.read().contains_key(&id));
            id
        });
        let spawned_id = handle.join().unwrap();

        // Only the spawned thread's entry matters: other tests may be
        // allocating (registering/reclaiming their own windows) in
        // parallel, so total-length comparisons would be racy.
        assert!(!PENDING_WINDOWS.read().contains_key(&spawned_id));
    }

    // ---- Prefix allocator (INV-E1: prefix uniqueness) ----
    //
    // These tests go through the process-global `VSDB` singleton — the
    // allocator's state (counter, ceiling, thread windows) is global by
    // design, so an isolated `MmDB` cannot exercise the real code path.
    // The suite runs multithreaded (no `--test-threads=1`), so other
    // tests may allocate concurrently — assertions below are written to
    // be race-tolerant rather than assuming isolation.

    /// Regression test: reserving a prefix already issued earlier in
    /// the calling thread's own still-open batch must be a no-op. Such
    /// a value can never recur as a future `alloc_prefix` candidate (the
    /// per-thread cursor only advances), so reserving it would insert a
    /// permanent, never-evicted entry into `RECOVERED_PREFIXES` —
    /// degrading every subsequent `alloc_prefix()` call in the process
    /// to the mutex+hashset slow path forever.
    #[test]
    fn reserve_recovered_prefix_ignores_already_issued_in_window_value() {
        let ns = crate::Namespace::default_ns();
        let db = ns.engine();
        let issued = db.alloc_prefix().unwrap();

        assert!(
            db.reserve_recovered_prefix(issued.to_le_bytes()).unwrap(),
            "an in-range prefix must still validate as accepted"
        );
        assert!(
            !RECOVERED_PREFIXES.lock().contains(&issued),
            "an already-issued, same-window prefix must never be \
             reserved: it can never recur as a future candidate, so \
             reserving it would leak permanently"
        );
    }

    /// Reserving a not-yet-issued prefix must divert the allocator
    /// around it: the reservation is matched (and consumed) at
    /// candidate-generation time, so the reserved value is never
    /// returned to any caller. Race-tolerant: `future` is derived from
    /// this thread's own just-issued value, so it lies inside this
    /// thread's still-open window (other tests' allocations come from
    /// disjoint batches) unless the window boundary happens to fall in
    /// between — in which case the loop below simply never generates
    /// `future` and the assertion stays vacuously true.
    #[test]
    fn reserve_recovered_prefix_diverts_allocator_around_reservation() {
        let ns = crate::Namespace::default_ns();
        let db = ns.engine();
        let p0 = db.alloc_prefix().unwrap();
        let future = p0 + 3;

        assert!(
            db.reserve_recovered_prefix(future.to_le_bytes()).unwrap(),
            "an in-range, un-issued prefix must be accepted and reserved"
        );
        for _ in 0..8 {
            let p = db.alloc_prefix().unwrap();
            assert_ne!(
                p, future,
                "allocator issued a prefix that was reserved as recovered"
            );
        }
    }

    thread_local! {
        pub(super) static AFTER_RESERVATION_LOCK: Cell<Option<Box<dyn FnOnce()>>> = const { Cell::new(None) };
    }

    #[test]
    fn duplicate_restore_preserves_an_earlier_reservation_as_the_cursor_advances() {
        use std::sync::mpsc;

        let (window_tx, window_rx) = mpsc::channel();
        let (issue_tx, issue_rx) = mpsc::channel();
        let allocator = thread::spawn(move || {
            let ns = crate::Namespace::default_ns();
            let db = ns.engine();
            let first = db.alloc_prefix().unwrap();
            window_tx
                .send((first + 1, LOCAL_CURSOR.with(Arc::clone)))
                .unwrap();
            issue_rx.recv().unwrap();
            db.alloc_prefix().unwrap()
        });
        let (target, cursor) = window_rx.recv().unwrap();
        let ns = crate::Namespace::default_ns();
        let db = ns.engine();
        assert!(db.reserve_recovered_prefix(target.to_le_bytes()).unwrap());
        AFTER_RESERVATION_LOCK.set(Some(Box::new(move || {
            issue_tx.send(()).unwrap();
            // The allocator has selected the target but is waiting for the
            // second restore to release the reservation mutex.
            while cursor.load(Ordering::SeqCst) <= target {
                thread::yield_now();
            }
        })));
        let restored = db.reserve_recovered_prefix(target.to_le_bytes()).unwrap();
        assert_ne!(allocator.join().unwrap(), target);
        assert!(restored);
    }

    #[test]
    fn abandoned_window_reservation_is_purged() {
        let (prefix_tx, prefix_rx) = std::sync::mpsc::channel();
        let (exit_tx, exit_rx) = std::sync::mpsc::channel();
        let handle = thread::spawn(move || {
            let ns = crate::Namespace::default_ns();
            let db = ns.engine();
            let prefix = db.alloc_prefix().unwrap();
            prefix_tx.send(prefix).unwrap();
            exit_rx.recv().unwrap();
        });

        let prefix = prefix_rx.recv().unwrap();
        let reserved = prefix + 1;
        let ns = crate::Namespace::default_ns();
        let db = ns.engine();
        assert!(
            db.reserve_recovered_prefix(reserved.to_le_bytes()).unwrap(),
            "an unissued prefix in a live window must be reserved"
        );
        assert!(RECOVERED_PREFIXES.lock().contains(&reserved));

        exit_tx.send(()).unwrap();
        handle.join().unwrap();
        assert!(
            !RECOVERED_PREFIXES.lock().contains(&reserved),
            "thread exit must purge reservations in its abandoned window"
        );
    }

    /// Per-thread allocations must be strictly increasing — across
    /// thread-local batch refills too (the loop spans several
    /// `PREFIX_ALLOC_BATCH` windows).  A repeat or regression would
    /// alias two instances' key ranges.
    #[test]
    fn prefix_allocator_unique_and_monotonic_per_thread() {
        let ns = crate::Namespace::default_ns();
        let db = ns.engine();
        let mut prev = db.alloc_prefix().unwrap();
        assert!(prev >= PREFIX_ALLOC_START);
        for _ in 0..(2 * PREFIX_ALLOC_BATCH + 8) {
            let next = db.alloc_prefix().unwrap();
            assert!(next > prev, "allocator regressed: {next} after {prev}");
            prev = next;
        }
    }

    /// Concurrent allocators must never issue the same prefix twice.
    /// Each thread allocates more than one batch, so window refills
    /// interleave with other threads claiming from `GLOBAL_COUNTER`.
    #[test]
    fn prefix_allocator_multithread_uniqueness() {
        const THREADS: usize = 8;
        let per_thread = (PREFIX_ALLOC_BATCH + 16) as usize;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                thread::spawn(move || {
                    let ns = crate::Namespace::default_ns();
                    let db = ns.engine();
                    (0..per_thread)
                        .map(|_| db.alloc_prefix().unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect();

        let mut seen = HashSet::with_capacity(THREADS * per_thread);
        for h in handles {
            for p in h.join().unwrap() {
                assert!(p >= PREFIX_ALLOC_START);
                assert!(seen.insert(p), "prefix {p} issued twice");
            }
        }
    }

    /// Crash safety of the allocator counter: the durably persisted
    /// ceiling must stay strictly above every issued prefix at all
    /// times.  A restarted process resumes issuing at the persisted
    /// value (`ensure_alloc_init`), so anything below it can never be
    /// issued again — provided this invariant never lapses, a crash at
    /// any point cannot lead to prefix reuse.
    ///
    /// Race-tolerant by design (the suite runs multithreaded): other
    /// tests may allocate concurrently, so assertions bound OUR issued
    /// prefixes against monotone global state instead of demanding
    /// exact equality between snapshots.
    #[test]
    fn prefix_allocator_persisted_ceiling_covers_issued() {
        let ns = crate::Namespace::default_ns();
        let db = ns.engine();
        let persisted_ceiling = || -> Pre {
            read_ceiling_file(&ceiling_file_path())
                .expect("ceiling read failed")
                .expect("ceiling file missing")
        };

        // Spans a batch refill, so the ceiling-bump-then-issue ordering
        // inside `alloc_prefix_candidate` is exercised, not just the
        // fast path within an already-covered window.
        let mut my_max = 0;
        for _ in 0..(PREFIX_ALLOC_BATCH + 16) {
            let p = db.alloc_prefix().unwrap();
            my_max = my_max.max(p);
            let ceil = GLOBAL_CEILING.load(Ordering::Acquire);
            assert!(ceil > p, "in-memory ceiling {ceil} not above issued {p}");
        }
        // The disk value is written before the in-memory mirror is
        // bumped (under PREFIX_ALLOC_LOCK), so at any observation point
        // disk >= any mirror value read earlier, and it must already
        // cover everything we were issued.
        let disk = persisted_ceiling();
        assert!(disk > my_max, "persisted ceiling {disk} not above {my_max}");
        assert!(disk >= GLOBAL_CEILING.load(Ordering::Acquire).min(disk));
    }
}
