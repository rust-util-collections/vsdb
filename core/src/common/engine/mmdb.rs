use crate::common::{
    BatchTrait, GB, PREFIX_ALLOC_START, PREFIX_SIZE, Pre, PreBytes, RawKey, RawValue,
    VSDB, vsdb_freeze_base_dir, vsdb_get_base_dir,
};
use mmdb::{BidiIterator, BlockCachePool, CompressionType, DB, DbOptions, WriteBatch};
use parking_lot::{Mutex, RwLock};
use ruc::*;
use std::{
    borrow::Cow,
    cell::Cell,
    cmp,
    collections::{HashMap, HashSet},
    fs,
    io::{self, Write},
    ops::{Bound, RangeBounds},
    path::{Path, PathBuf},
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, ThreadId},
};

/// Legacy (pre-v16) location of the prefix-allocator ceiling: a meta key
/// in shard 0. Read once at open for the take-max migration; never
/// written again (v16+ persists the ceiling in [`PREFIX_CEILING_REL_PATH`]).
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

/// v16+ location of the prefix-allocator ceiling, relative to the VSDB
/// base dir: an 8-byte little-endian `u64`, written durably via
/// tmp/fsync/rename plus a parent-dir fsync. Living outside the shard
/// DBs, it keeps prefix allocation independent of the default engine —
/// a prerequisite for namespaces sharing one global allocator.
const PREFIX_CEILING_REL_PATH: &str = "__SYSTEM__/__prefix_ceiling__";

/// On-disk format-version marker, relative to the VSDB base dir
/// (ASCII decimal).
///
/// Format 16 relocated the prefix-allocator ceiling out of shard 0; the
/// marker is written durably at open, *before* the file-based allocator
/// can issue anything, so a v15.0.2+ binary pointed at this dataset
/// refuses to open instead of reading the stale shard-0 ceiling and
/// re-issuing prefixes already used by the new layout (silent data
/// corruption). Absence of the marker means a pre-v16 dataset.
const FORMAT_VERSION_REL_PATH: &str = "__SYSTEM__/format_version";

/// The on-disk format this binary reads and writes. Anything newer is
/// refused at open (downgrade is unsupported by policy — fail loudly).
const SUPPORTED_FORMAT_VERSION: u64 = 16;

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
/// protect once the owning thread is gone.
struct PendingWindowGuard;

impl Drop for PendingWindowGuard {
    fn drop(&mut self) {
        PENDING_WINDOWS.write().remove(&thread::current().id());
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

/// Serializes writers of the small `__SYSTEM__` meta files (allocator
/// ceiling, format marker). `write_file_durable` stages through a fixed
/// sibling `*.tmp` name, so two concurrent writers of the same path
/// could interleave create/truncate/rename into a torn result; every
/// writer takes this lock first. Lock order: `PREFIX_ALLOC_LOCK` (if
/// held at all) strictly before this one.
static SYS_META_LOCK: Mutex<()> = Mutex::new(());

pub struct MmDB {
    /// Sharded DB handlers (`shards` of them), owned by this engine —
    /// dropping the engine closes every shard (flushing its WAL, joining
    /// its compaction threads, releasing its LOCK file). In the default
    /// namespace, shard 0 additionally holds the read-only legacy
    /// allocator key from pre-v16 datasets.
    dbs: Box<[DB]>,
}

impl MmDB {
    /// Opens the DEFAULT-namespace engine rooted at the global base dir,
    /// running the pre-v16 allocator-ceiling migration.
    pub(crate) fn new() -> Result<Self> {
        let base_dir = vsdb_get_base_dir();
        // Lock in the base dir so later `vsdb_set_base_dir` calls fail.
        // Deliberately does NOT mutate the process environment: this runs
        // lazily on the first DB operation, possibly long after worker
        // threads were spawned, where `env::set_var` would be unsound.
        vsdb_freeze_base_dir();

        let this =
            Self::open_at(&base_dir, NUM_SHARDS, EngineSizing::from_process_budget())?;

        // Absorb a pre-v16 ceiling (legacy shard-0 key) into the ceiling
        // file — see `migrate_ceiling` for the crash-safety argument.
        // The legacy key itself is never written again.
        let legacy = this.dbs[0]
            .get(&META_KEY_PREFIX_ALLOCATOR)
            .c(d!())?
            .map(|v| crate::common::parse_prefix!(v));
        migrate_ceiling(&base_dir, legacy)?;

        Ok(this)
    }

    /// Opens an engine instance rooted at `root` with `shards` shards.
    ///
    /// Namespace-generic: refuses datasets marked with a newer on-disk
    /// format, validates the persisted shard layout (a mismatched count
    /// would silently re-route every prefix), and writes the format
    /// marker. Does NOT perform the legacy shard-0 migration — only the
    /// default namespace can carry pre-v16 state (see [`MmDB::new`]).
    pub(crate) fn open_at(
        root: &Path,
        shards: usize,
        sizing: EngineSizing,
    ) -> Result<Self> {
        debug_assert!((1..=64).contains(&shards));
        fs::create_dir_all(root).c(d!())?;

        check_format_version(root).c(d!())?;

        let dir = root.join("mmdb");
        fs::create_dir_all(&dir).c(d!())?;

        let marker_present = root.join(FORMAT_VERSION_REL_PATH).exists();
        let sentinel_path = root.join(INIT_SENTINEL_REL_PATH);
        let scan = scan_shard_layout(&dir, shards)?;
        validate_shard_layout(&dir, shards, marker_present, &sentinel_path, &scan)?;

        // Brand-new root: raise the initialization sentinel durably
        // BEFORE the first shard dir exists, so a crash mid-creation is
        // provably a resumable create (see INIT_SENTINEL_REL_PATH).
        if !marker_present && scan.present == 0 && !sentinel_path.exists() {
            let _g = SYS_META_LOCK.lock();
            fs::create_dir_all(sentinel_path.parent().expect("has parent")).c(d!())?;
            write_file_durable(&sentinel_path, b"1")?;
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
            fs::create_dir_all(&shard_dir).c(d!())?;
            let db = mmdb_open(&shard_dir, shards, sizing, &pool)?;
            dbs_vec.push(db);
        }

        // Mark the root's on-disk format so an older binary pointed at
        // it (base dir or namespace root alike) refuses to open.
        write_format_marker(root)?;
        // Initialization is complete and durably marked: retire the
        // sentinel (best-effort — a stale sentinel next to a marker is
        // cleaned up here on the next open, and the marker-present
        // validation path never consults it).
        match fs::remove_file(&sentinel_path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).c(d!()),
        }

        Ok(MmDB {
            dbs: dbs_vec.into_boxed_slice(),
        })
    }

    /// Cleanly closes every shard and consumes the engine.
    ///
    /// `DB::close` flushes the active memtable and syncs the WAL,
    /// surfacing errors that plain `Drop` would swallow; the subsequent
    /// drop of each shard then joins its compaction threads and releases
    /// its LOCK file. All shards are closed even if one errors; the
    /// first error is returned.
    pub(crate) fn close(self) -> Result<()> {
        let mut ret = Ok(());
        for db in &self.dbs {
            if let Err(e) = db.close()
                && ret.is_ok()
            {
                ret = Err(e).c(d!());
            }
        }
        ret
    }

    /// Route a prefix to its shard.
    #[inline(always)]
    fn shard(&self, meta_prefix: &PreBytes) -> &DB {
        let prefix = u64::from_le_bytes(*meta_prefix);
        &self.dbs[(prefix % self.dbs.len() as u64) as usize]
    }

    /// See the module-level [`alloc_prefix`] — kept as a method so the
    /// typed layer keeps calling through its engine handle.
    #[inline(always)]
    pub(crate) fn alloc_prefix(&self) -> Pre {
        alloc_prefix()
    }

    /// See the module-level [`reserve_recovered_prefix`].
    #[inline(always)]
    pub(crate) fn reserve_recovered_prefix(&self, meta_prefix: PreBytes) -> bool {
        reserve_recovered_prefix(meta_prefix)
    }

    pub(crate) fn flush(&self) {
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
        let full_key = make_full_key(&meta_prefix, key);
        self.shard(&meta_prefix)
            .get(&full_key)
            .expect("vsdb: mmdb get failed")
    }

    pub(crate) fn insert(&self, meta_prefix: PreBytes, key: &[u8], value: &[u8]) {
        let full_key = make_full_key(&meta_prefix, key);
        self.shard(&meta_prefix)
            .put(&full_key, value)
            .expect("vsdb: mmdb put failed");
    }

    pub(crate) fn remove(&self, meta_prefix: PreBytes, key: &[u8]) {
        let full_key = make_full_key(&meta_prefix, key);
        self.shard(&meta_prefix)
            .delete(&full_key)
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
        let full_key = make_full_key(&meta_prefix, key);
        self.shard(&meta_prefix).lazy_delete(&full_key);
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
        let shard = self.shard(&meta_prefix);
        let full_keys: Vec<Vec<u8>> = keys
            .into_iter()
            .map(|k| make_full_key(&meta_prefix, k.as_ref()))
            .collect();
        shard.lazy_delete_batch(full_keys);
    }

    pub(crate) fn iter(&self, meta_prefix: PreBytes) -> MmdbIter {
        let db = self.shard(&meta_prefix);
        let db_iter = db
            .iter_with_prefix(&meta_prefix, &mmdb::ReadOptions::default())
            .expect("vsdb: mmdb iter_with_prefix failed");
        // Defense-in-depth prefix bound (parity with `range`): never surface
        // keys from an adjacent prefix in the same shard, even if the
        // engine's prefix iterator were to over-scan its upper boundary.
        let iter = BidiIterator::lazy(db_iter)
            .filter(move |(k, _)| k.starts_with(&meta_prefix))
            .map(|(k, v)| (k[PREFIX_SIZE..].to_vec(), v));
        MmdbIter(Box::new(iter))
    }

    pub(crate) fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &self,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> MmdbIter {
        let db = self.shard(&meta_prefix);

        let prefixed = |b: &Bound<&Cow<'_, [u8]>>| -> Bound<Vec<u8>> {
            match b {
                Bound::Included(k) => Bound::Included(make_full_key(&meta_prefix, k)),
                Bound::Excluded(k) => Bound::Excluded(make_full_key(&meta_prefix, k)),
                Bound::Unbounded => Bound::Unbounded,
            }
        };

        let lo_full = prefixed(&bounds.start_bound());
        let hi_full = prefixed(&bounds.end_bound());

        // SST-level pruning hints: start from lo or prefix start, end at
        // the requested upper bound capped by the prefix boundary.
        let start_hint: Option<Vec<u8>> = match &lo_full {
            Bound::Included(v) | Bound::Excluded(v) => Some(v.clone()),
            Bound::Unbounded => Some(meta_prefix.to_vec()),
        };
        let prefix_end = prefix_successor(&meta_prefix);
        let requested_end = match &hi_full {
            Bound::Included(v) => prefix_successor(v).or_else(|| prefix_end.clone()),
            Bound::Excluded(v) => Some(v.clone()),
            Bound::Unbounded => prefix_end.clone(),
        };
        let end_hint = match (requested_end, prefix_end) {
            (Some(end), Some(prefix_end)) => Some(cmp::min(end, prefix_end)),
            (Some(end), None) => Some(end),
            (None, Some(prefix_end)) => Some(prefix_end),
            (None, None) => None,
        };

        let mut db_iter = db
            .iter_with_range(
                &mmdb::ReadOptions::default(),
                start_hint.as_deref(),
                end_hint.as_deref(),
            )
            .expect("vsdb: mmdb iter_with_range failed");

        if let Bound::Included(ref lo) | Bound::Excluded(ref lo) = lo_full {
            db_iter.seek(lo);
        }

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

    /// Like [`batch_begin`](Self::batch_begin), but the batch is
    /// pre-staged with the removal of every existing key of the prefix
    /// (one range tombstone). See `MmdbBatch::new_wiped`.
    pub(crate) fn batch_begin_wiped<'a>(
        &'a self,
        meta_prefix: PreBytes,
    ) -> Box<dyn BatchTrait + 'a> {
        Box::new(MmdbBatch::new_wiped(meta_prefix, self))
    }
}

// ---- Iterator ----

/// A lazy, bidirectional iterator over key-value pairs in a single prefix range.
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
pub(crate) fn validate_completed_dataset(
    root: &Path,
    shards: usize,
    require_marker: bool,
) -> Result<()> {
    check_format_version(root)?;
    let marker_present = root.join(FORMAT_VERSION_REL_PATH).is_file();
    if require_marker && !marker_present {
        return Err(eg!(format!(
            "dataset at {} has no format marker",
            root.display()
        )));
    }
    let mmdb_dir = root.join("mmdb");
    let scan = scan_shard_layout(&mmdb_dir, shards)?;
    validate_shard_layout(
        &mmdb_dir,
        shards,
        marker_present,
        &root.join(INIT_SENTINEL_REL_PATH),
        &scan,
    )?;
    if scan.present != shards {
        return Err(eg!(format!(
            "dataset at {} is not complete: found {} of {} shard dirs",
            root.display(),
            scan.present,
            shards
        )));
    }
    validate_shard_anchors(&mmdb_dir, shards)
}

/// Folds `legacy` (the pre-v16 shard-0 value, when present) and the
/// allocation floor into the ceiling file — idempotent take-max, run
/// under [`SYS_META_LOCK`].
///
/// Crash-safety around [`MmDB::new`]'s sequence — which is
/// mark-then-fold: `open_at` writes the format marker before this fold
/// runs. The order is safe because nothing gates on it: a crash after
/// the marker but before the fold leaves a marked dataset with no
/// ceiling file, and every allocation path funnels through
/// `ensure_alloc_init`, which refuses to issue anything until a fold
/// has produced the file (re-running it here on the next open, or
/// running it itself for a fresh universe). The legacy shard-0 key is
/// preserved forever and the fold is idempotent take-max, so repeated
/// folds never regress (that is also why the fold runs at EVERY open: a
/// pre-tripwire v15 binary advancing the legacy key after migration is
/// re-absorbed instead of causing prefix reuse; post-marker, the v15
/// tripwire refuses the dataset outright).
///
/// Never races a ceiling bump: bumps require the allocator to be
/// initialized (`GLOBAL_COUNTER != 0`), and every path that initializes
/// it either runs this fold first ([`MmDB::new`] → first allocation) or
/// holds `PREFIX_ALLOC_LOCK` around the fold (`ensure_alloc_init`).
fn migrate_ceiling(base_dir: &Path, legacy: Option<Pre>) -> Result<()> {
    let _g = SYS_META_LOCK.lock();
    let file = base_dir.join(PREFIX_CEILING_REL_PATH);
    fs::create_dir_all(file.parent().expect("has parent")).c(d!())?;
    let filed = read_ceiling_file(&file)?;
    let ceiling = effective_initial_ceiling(filed, legacy);
    if filed != Some(ceiling) {
        write_file_durable(&file, &ceiling.to_le_bytes())?;
    }
    Ok(())
}

/// Allocates a fresh, never-before-issued prefix.
pub(crate) fn alloc_prefix() -> Pre {
    loop {
        let candidate = alloc_prefix_candidate();
        // Normal operation records no reservations, so allocation
        // stays lock-free here. `SeqCst` pairs with the `SeqCst`
        // cursor store in `alloc_prefix_candidate` and the flag
        // store + cursor loads in `reserve_recovered_prefix`: of the
        // two racing sides, at least one must observe the other
        // (Dekker), so a candidate issued concurrently with a
        // reservation is either checked against the set here or its
        // cursor advance is caught by the reserver's verify step.
        if !RECOVERED_NONEMPTY.load(Ordering::SeqCst) {
            return candidate;
        }
        let mut reserved = RECOVERED_PREFIXES.lock();
        if !reserved.remove(&candidate) {
            return candidate;
        }
        if reserved.is_empty() {
            RECOVERED_NONEMPTY.store(false, Ordering::SeqCst);
        }
    }
}

/// Loads the persisted allocator state into the process-wide atomics
/// (idempotent). `GLOBAL_FLOOR` snapshots the persisted value at
/// initialization: every prefix below it was issued by a previous run.
fn ensure_alloc_init() {
    if GLOBAL_COUNTER.load(Ordering::Acquire) != 0 {
        return;
    }
    let _x = PREFIX_ALLOC_LOCK.lock();
    if GLOBAL_COUNTER.load(Ordering::Acquire) != 0 {
        return;
    }
    let base_dir = vsdb_get_base_dir();
    let file = base_dir.join(PREFIX_CEILING_REL_PATH);
    let ret =
        match read_ceiling_file(&file).expect("vsdb: allocator ceiling read failed") {
            Some(v) => v,
            None if base_dir.join("mmdb").exists() => {
                // A pre-v16 dataset sits at the default base and no v16
                // open has migrated it yet (we were reached through a
                // non-default namespace). The authoritative ceiling still
                // lives in the legacy shard-0 key: force the default
                // engine open — its init runs the take-max fold exactly
                // once. Never re-entrant: engine init allocates nothing,
                // and every other allocating thread is blocked on
                // `PREFIX_ALLOC_LOCK` (held here) or sees the counter
                // still zero.
                LazyLock::force(&VSDB);
                read_ceiling_file(&file)
                    .expect("vsdb: allocator ceiling read failed")
                    .expect("vsdb: default-engine migration must create the ceiling")
            }
            None => {
                // Fresh universe: initialize the backing store.
                migrate_ceiling(&base_dir, None)
                    .expect("vsdb: allocator ceiling init failed");
                PREFIX_ALLOC_START
            }
        };
    let new_ceil = ret + PREFIX_ALLOC_BATCH;
    {
        let _g = SYS_META_LOCK.lock();
        write_file_durable(&file, &new_ceil.to_le_bytes())
            .expect("vsdb: allocator ceiling write failed");
    }
    GLOBAL_FLOOR.store(ret, Ordering::Release);
    GLOBAL_CEILING.store(new_ceil, Ordering::Release);
    // The counter doubles as the init guard — store it last.
    GLOBAL_COUNTER.store(ret, Ordering::Release);
}

fn alloc_prefix_candidate() -> Pre {
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
                return next;
            }

            ensure_alloc_init();

            // Ensure this thread's cleanup guard is registered
            // before taking a batch, so the corresponding
            // `PENDING_WINDOWS` entry inserted below is guaranteed
            // to be removed when the thread exits (bounding the
            // registry to currently-live threads instead of every
            // thread that ever allocated a prefix).
            PENDING_WINDOW_GUARD.with(|_| {});

            // Claim the next batch and register it — cursor already
            // advanced past `batch_start`, which is issued below — as
            // this thread's pending window in one exclusive section, so
            // that `reserve_recovered_prefix` (which reads the counter
            // and the registry under the read lock) can never observe
            // the advanced counter without the matching, correctly
            // positioned window.
            let batch_start = {
                let mut reg = PENDING_WINDOWS.write();
                let batch_start =
                    GLOBAL_COUNTER.fetch_add(PREFIX_ALLOC_BATCH, Ordering::AcqRel);
                let cursor = LOCAL_CURSOR.with(|c| c.clone());
                cursor.store(batch_start + 1, Ordering::Release);
                reg.insert(
                    thread::current().id(),
                    (cursor, batch_start + PREFIX_ALLOC_BATCH),
                );
                batch_start
            };
            let batch_end = batch_start + PREFIX_ALLOC_BATCH;

            let old_ceil = GLOBAL_CEILING.load(Ordering::Acquire);
            if batch_end > old_ceil {
                let _x = PREFIX_ALLOC_LOCK.lock();
                let old_ceil2 = GLOBAL_CEILING.load(Ordering::Acquire);
                if batch_end > old_ceil2 {
                    let new_ceil = batch_end + PREFIX_ALLOC_BATCH;
                    let _g = SYS_META_LOCK.lock();
                    write_file_durable(&ceiling_file_path(), &new_ceil.to_le_bytes())
                        .expect("vsdb: allocator ceiling write failed");
                    GLOBAL_CEILING.store(new_ceil, Ordering::Release);
                }
            }

            next_cell.set(batch_start + 1);
            ceil_cell.set(batch_end);
            batch_start
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
pub(crate) fn reserve_recovered_prefix(meta_prefix: PreBytes) -> bool {
    let prefix = Pre::from_le_bytes(meta_prefix);
    if prefix < PREFIX_ALLOC_START {
        return false;
    }
    ensure_alloc_init();

    if prefix >= GLOBAL_CEILING.load(Ordering::Acquire) {
        return false;
    }
    if prefix < GLOBAL_FLOOR.load(Ordering::Acquire) {
        // Issued by a previous run — can never be issued again.
        return true;
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
        return true;
    }

    let mut reserved = RECOVERED_PREFIXES.lock();
    reserved.insert(prefix);
    RECOVERED_NONEMPTY.store(true, Ordering::SeqCst);

    if unissued() {
        return true;
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
    false
}

// ---- Batch ----

/// The initial allocator ceiling for this open: the max of the v16 file
/// value, the pre-v16 legacy shard-0 value, and the allocation floor.
/// Pure take-max — folding in a stale source can only raise the ceiling
/// (wasting at most one gap of ids), never lower it below anything
/// already issued.
fn effective_initial_ceiling(filed: Option<Pre>, legacy: Option<Pre>) -> Pre {
    filed
        .unwrap_or(0)
        .max(legacy.unwrap_or(0))
        .max(PREFIX_ALLOC_START)
}

/// Reads the ceiling file: 8-byte little-endian `u64`.
///
/// `None` when absent (fresh or pre-v16 dataset). Any other read/format
/// problem is an error — the ceiling guards prefix uniqueness, so a
/// truncated or padded file must abort the open, never be guessed at.
fn read_ceiling_file(path: &Path) -> Result<Option<Pre>> {
    match fs::read(path) {
        Ok(bytes) if bytes.len() == PREFIX_SIZE => {
            Ok(Some(crate::common::parse_prefix!(bytes)))
        }
        Ok(bytes) => Err(eg!(format!(
            "corrupt allocator ceiling file {} ({} bytes, expected {})",
            path.display(),
            bytes.len(),
            PREFIX_SIZE
        ))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).c(d!()),
    }
}

/// Durable file replacement: tmp, fsync, rename, then parent-dir fsync.
///
/// **Locking contract**: staging goes through a fixed sibling `*.tmp`
/// name, so concurrent writers of the SAME path would interleave
/// create/truncate/rename into a torn result — every caller must hold
/// the lock guarding that file's class first ([`SYS_META_LOCK`] for
/// allocator/marker/sentinel files, `REGISTRY_LOCK` for the namespace
/// registry). Not taken here: several callers already hold their lock
/// (parking_lot mutexes are non-reentrant).
///
/// The parent-dir fsync matters: without it a power loss can drop the
/// rename itself, and a regressed allocator ceiling means prefix reuse —
/// silent data corruption. (The plain `atomic_write_file` used for
/// instance metas skips the dir fsync; the allocator cannot.)
pub(crate) fn write_file_durable(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut f = fs::File::create(&tmp).c(d!())?;
        f.write_all(bytes).c(d!())?;
        f.sync_all().c(d!())?;
    }
    fs::rename(&tmp, path).c(d!())?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent).c(d!())?.sync_all().c(d!())?;
    }
    Ok(())
}

/// Marks the dataset as [`SUPPORTED_FORMAT_VERSION`] (idempotent,
/// serialized by [`SYS_META_LOCK`]).
fn write_format_marker(base_dir: &Path) -> Result<()> {
    let _g = SYS_META_LOCK.lock();
    let path = base_dir.join(FORMAT_VERSION_REL_PATH);
    if let Ok(s) = fs::read_to_string(&path)
        && s.trim().parse::<u64>() == Ok(SUPPORTED_FORMAT_VERSION)
    {
        return Ok(());
    }
    fs::create_dir_all(path.parent().expect("has parent")).c(d!())?;
    write_file_durable(&path, SUPPORTED_FORMAT_VERSION.to_string().as_bytes())
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
                let e = e.c(d!())?;
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
        Err(err) => return Err(err).c(d!()),
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
            return Err(eg!(format!(
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
/// * **Neither** ⇒ only two shapes are legitimate: a brand-new root
///   (zero shards) or a complete pre-v16/pre-sentinel dataset (exact
///   set, e.g. a legacy 16/16 default base awaiting migration). A
///   partial set here is a damaged dataset — missing shard dirs mean
///   silent data loss if "resumed" — and is refused.
fn validate_shard_layout(
    mmdb_dir: &Path,
    shards: usize,
    marker_present: bool,
    sentinel_path: &Path,
    scan: &ShardScan,
) -> Result<()> {
    if let Some(name) = &scan.unexpected {
        return Err(eg!(format!(
            "unexpected shard entry {:?} at {} (expected exactly \
             shard_00..shard_{:02})",
            name,
            mmdb_dir.display(),
            shards - 1
        )));
    }
    if marker_present {
        if scan.present != shards {
            return Err(eg!(format!(
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
    if scan.present == shards {
        return validate_shard_anchors(mmdb_dir, shards);
    }
    Err(eg!(format!(
        "damaged dataset at {}: {} of {} shard dirs exist with no \
         initialization sentinel — missing shards would mean silent \
         data loss; refusing to open (an interrupted pre-16.0.2 create \
         can be reclaimed via vsdb_ns_destroy)",
        mmdb_dir.display(),
        scan.present,
        shards
    )))
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
    fn commit(&mut self) -> crate::common::error::Result<()> {
        let batch = std::mem::replace(&mut self.inner, WriteBatch::new());
        // `.c(d!())` attaches file/line context; the `?` conversion into
        // `VsdbError` preserves the complete ruc chain.
        self.engine.shard(&self.meta_prefix).write(batch).c(d!())?;
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

const G: usize = GB as usize;

/// Default memory budget for the DEFAULT namespace, in MB: a fixed
/// 2 GiB. vsdb deliberately does NOT size itself from the host's RAM
/// or its cgroup: the library cannot know how much of either belongs
/// to it, and a moment-in-time reading bakes deployment noise into
/// engine sizing. A fixed, conservative default keeps the footprint
/// predictable everywhere; applications that can afford more memory
/// should raise `VSDB_MEM_BUDGET_MB` — a larger budget enlarges the
/// block cache and write buffers, which improves performance.
const DEFAULT_MEM_BUDGET_MB: usize = 2048;

/// Resolve the effective DEFAULT-namespace budget in bytes.
///
/// - `env_budget_mb` (`VSDB_MEM_BUDGET_MB`), when present, non-zero,
///   and expressible in bytes, is applied verbatim — the operator
///   asked for that exact number; it is the ONLY way to grow (or
///   shrink) the default engine's memory.
/// - Otherwise the fixed [`DEFAULT_MEM_BUDGET_MB`] default stands.
///
/// Every budget is a binding limit: write-buffer sizing scales with
/// it (see `mmdb_open`).
fn effective_mem_budget(env_budget_mb: Option<usize>) -> usize {
    env_budget_mb
        .filter(|&mb| mb > 0)
        .and_then(|mb| mb.checked_mul(1024 * 1024))
        .unwrap_or(DEFAULT_MEM_BUDGET_MB * 1024 * 1024)
}

/// The effective budget for this process, computed once: every shard
/// must size off the same number, and `MmDB::new` opens `NUM_SHARDS`
/// shards -- re-parsing the env override per shard would be redundant.
static MEM_BUDGET: LazyLock<usize> = LazyLock::new(|| {
    let env_budget_mb = std::env::var("VSDB_MEM_BUDGET_MB")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok());

    effective_mem_budget(env_budget_mb)
});

/// Refuses to open a dataset marked with a newer on-disk format.
///
/// Absent marker ⇒ pre-v16 layout ⇒ proceed (the take-max migration in
/// [`MmDB::new`] handles it). A marker naming a format above
/// [`SUPPORTED_FORMAT_VERSION`] — or an unreadable one (conservative:
/// the marker guards against silent corruption, so garbage aborts) —
/// fails the open with a descriptive error.
fn check_format_version(base_dir: &Path) -> Result<()> {
    let path = base_dir.join(FORMAT_VERSION_REL_PATH);
    let raw = match fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).c(d!()),
    };
    let ver = raw.trim().parse::<u64>().map_err(|_| {
        eg!(format!(
            "unreadable format marker {} (content: {:?}); refusing to open",
            path.display(),
            raw
        ))
    })?;
    if ver > SUPPORTED_FORMAT_VERSION {
        return Err(eg!(format!(
            "this dataset uses on-disk format {} (marker: {}), but this \
             binary supports at most format {}; downgrade is not supported — \
             export the data with the newer release instead",
            ver,
            path.display(),
            SUPPORTED_FORMAT_VERSION
        )));
    }
    Ok(())
}

/// Engine memory sizing, resolved before shards open.
///
/// The default namespace sizes from [`MEM_BUDGET`]
/// (`VSDB_MEM_BUDGET_MB` override / fixed 2 GiB default). Non-default
/// namespaces size from an explicit per-namespace budget so that opening
/// N namespaces cannot silently multiply the process footprint. Every
/// budget is a binding limit; nothing sizes from the host or its cgroup.
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

    // Per-shard sizes: divide totals by the shard count.
    //
    // Every budget is a binding limit, so the write-buffer term must
    // scale with it: each shard can hold one active memtable plus
    // `max_immutable_memtables` frozen ones awaiting flush, so the
    // worst-case memtable footprint is wr_buffer_size *
    // (1 + max_immutable) * shards. budget/8 across all shards keeps
    // that worst case (5x active) at ~5/8 of budget, leaving the rest
    // for the block cache (budget/8) and per-connection transients —
    // sizing memtables above the budget instead pins anonymous memory
    // at the memory line during ingest bursts, and the reclaim
    // pressure slows the very flush threads that are the only way out
    // (observed as a service wedged at memory.high with tens of
    // thousands of memory.events:high). The first term keeps the
    // historical shape (G/shards, opening to budget/4/shards past
    // 16 G, capped at 512 MB) so large explicit budgets size exactly
    // like the old host-derived path.
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

    // Single compaction thread per shard (N shards = N parallel compactions)
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

    DB::open(opts, dir).c(d!())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

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
    fn format_version_tripwire() {
        let base = tmp_dir("format-ver");
        fs::create_dir_all(&base).unwrap();

        // Absent marker = pre-v16 dataset: accepted (migration path).
        assert!(check_format_version(&base).is_ok());

        fs::create_dir_all(base.join("__SYSTEM__")).unwrap();
        let marker = base.join(FORMAT_VERSION_REL_PATH);

        // Current-or-older format numbers: accepted.
        fs::write(&marker, "16").unwrap();
        assert!(check_format_version(&base).is_ok());
        fs::write(&marker, "15\n").unwrap();
        assert!(check_format_version(&base).is_ok());

        // Newer format: refused loudly.
        fs::write(&marker, "17").unwrap();
        assert!(check_format_version(&base).is_err());

        // Unreadable marker: refused (conservative).
        fs::write(&marker, "garbage").unwrap();
        assert!(check_format_version(&base).is_err());

        // The writer marks exactly the supported version, idempotently,
        // and the result round-trips through the checker.
        fs::remove_file(&marker).unwrap();
        write_format_marker(&base).unwrap();
        write_format_marker(&base).unwrap();
        assert_eq!(
            fs::read_to_string(&marker).unwrap(),
            SUPPORTED_FORMAT_VERSION.to_string()
        );
        assert!(check_format_version(&base).is_ok());

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn ceiling_migration_take_max() {
        // Pure fold semantics.
        assert_eq!(effective_initial_ceiling(None, None), PREFIX_ALLOC_START);
        assert_eq!(
            effective_initial_ceiling(None, Some(PREFIX_ALLOC_START + 7)),
            PREFIX_ALLOC_START + 7
        );
        assert_eq!(
            effective_initial_ceiling(Some(PREFIX_ALLOC_START + 9), None),
            PREFIX_ALLOC_START + 9
        );
        // A legacy value above the file value (pre-tripwire v15 binary
        // ran after migration) is re-absorbed, never ignored.
        assert_eq!(
            effective_initial_ceiling(
                Some(PREFIX_ALLOC_START + 100),
                Some(PREFIX_ALLOC_START + 200)
            ),
            PREFIX_ALLOC_START + 200
        );
        assert_eq!(
            effective_initial_ceiling(
                Some(PREFIX_ALLOC_START + 200),
                Some(PREFIX_ALLOC_START + 100)
            ),
            PREFIX_ALLOC_START + 200
        );

        // File round-trip: durable write, exact-size read, corruption
        // and absence both surface correctly.
        let base = tmp_dir("ceiling-file");
        fs::create_dir_all(&base).unwrap();
        let f = base.join("__prefix_ceiling__");
        assert_eq!(read_ceiling_file(&f).unwrap(), None);
        write_file_durable(&f, &(PREFIX_ALLOC_START + 42).to_le_bytes()).unwrap();
        assert_eq!(
            read_ceiling_file(&f).unwrap(),
            Some(PREFIX_ALLOC_START + 42)
        );
        fs::write(&f, [0u8; 3]).unwrap();
        assert!(read_ceiling_file(&f).is_err());
        let _ = fs::remove_dir_all(&base);
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
        assert!(check(2, false).is_ok());
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
        let db = VSDB.engine();
        let issued = db.alloc_prefix();

        assert!(
            db.reserve_recovered_prefix(issued.to_le_bytes()),
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
        let db = VSDB.engine();
        let p0 = db.alloc_prefix();
        let future = p0 + 3;

        assert!(
            db.reserve_recovered_prefix(future.to_le_bytes()),
            "an in-range, un-issued prefix must be accepted and reserved"
        );
        for _ in 0..8 {
            let p = db.alloc_prefix();
            assert_ne!(
                p, future,
                "allocator issued a prefix that was reserved as recovered"
            );
        }
    }

    /// Per-thread allocations must be strictly increasing — across
    /// thread-local batch refills too (the loop spans several
    /// `PREFIX_ALLOC_BATCH` windows).  A repeat or regression would
    /// alias two instances' key ranges.
    #[test]
    fn prefix_allocator_unique_and_monotonic_per_thread() {
        let db = VSDB.engine();
        let mut prev = db.alloc_prefix();
        assert!(prev >= PREFIX_ALLOC_START);
        for _ in 0..(2 * PREFIX_ALLOC_BATCH + 8) {
            let next = db.alloc_prefix();
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
                    let db = VSDB.engine();
                    (0..per_thread)
                        .map(|_| db.alloc_prefix())
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
        let db = VSDB.engine();
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
            let p = db.alloc_prefix();
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
