use crate::common::{
    BatchTrait, GB, PREFIX_ALLOC_START, PREFIX_SIZE, Pre, PreBytes, RawKey, RawValue,
    vsdb_freeze_base_dir, vsdb_get_base_dir,
};
use mmdb::{BidiIterator, CompressionType, DB, DbOptions, WriteBatch, WriteOptions};
use parking_lot::{Mutex, RwLock};
use ruc::*;
use std::{
    borrow::Cow,
    cell::Cell,
    cmp,
    collections::{HashMap, HashSet},
    fs,
    ops::{Bound, RangeBounds},
    sync::{
        LazyLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, ThreadId},
};

const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

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

/// Per-thread pending allocation windows (`[start, end)` handed out by
/// `GLOBAL_COUNTER.fetch_add` but not yet fully issued). Refills replace
/// the same thread's entry; a live thread's entry is removed by
/// [`PendingWindowGuard`] when that thread exits, so the registry stays
/// bounded by the number of currently-live threads that have allocated
/// at least one prefix batch (not one entry per historical thread).
static PENDING_WINDOWS: LazyLock<RwLock<HashMap<ThreadId, (u64, u64)>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Prefixes accepted by [`MmDB::reserve_recovered_prefix`] that the
/// allocator has not issued yet; `alloc_prefix` must skip them.
static RECOVERED_PREFIXES: LazyLock<Mutex<HashSet<Pre>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));
/// Fast-path guard: `alloc_prefix` only locks `RECOVERED_PREFIXES` after
/// at least one reservation has been recorded (never in normal operation).
static RECOVERED_NONEMPTY: AtomicBool = AtomicBool::new(false);

/// Number of DB shards. Each prefix is routed to one shard via `prefix % NUM_SHARDS`.
/// This gives 16 independent write locks, compaction queues, and WALs.
const NUM_SHARDS: usize = 16;

/// WriteOptions with WAL fsync enabled.
/// Used for metadata writes (the prefix allocator) that must survive
/// process exit without DB::drop() (e.g. Box::leak singleton pattern).
fn sync_write_opts() -> WriteOptions {
    WriteOptions {
        sync: true,
        ..Default::default()
    }
}

pub struct MmDB {
    /// Sharded DB handlers. Meta keys live in shard 0.
    dbs: [&'static DB; NUM_SHARDS],
    prefix_allocator: PreAllocator,
}

impl MmDB {
    pub(crate) fn new() -> Result<Self> {
        let base_dir = vsdb_get_base_dir();
        // Lock in the base dir so later `vsdb_set_base_dir` calls fail.
        // Deliberately does NOT mutate the process environment: this runs
        // lazily on the first DB operation, possibly long after worker
        // threads were spawned, where `env::set_var` would be unsound.
        vsdb_freeze_base_dir();

        fs::create_dir_all(&base_dir).c(d!())?;

        let dir = base_dir.join("mmdb");
        fs::create_dir_all(&dir).c(d!())?;

        // Open NUM_SHARDS independent DB instances into an owned,
        // non-`'static` Vec first. If a later shard (or the meta-init
        // step below) fails, this Vec's normal Drop glue closes every
        // already-opened shard (flushing its WAL and joining its
        // compaction thread) via the `?` early return, instead of
        // leaking them. Shards are only promoted to `'static` — via
        // `Box::leak` — after every fallible step below has succeeded,
        // so a partial-init failure can never leave leaked, unreachable
        // shard handles behind.
        let mut dbs_vec: Vec<DB> = Vec::with_capacity(NUM_SHARDS);
        for i in 0..NUM_SHARDS {
            let shard_dir = dir.join(format!("shard_{:02}", i));
            fs::create_dir_all(&shard_dir).c(d!())?;
            let db = mmdb_open(&shard_dir)?;
            dbs_vec.push(db);
        }

        // Meta keys live in shard 0
        let (prefix_allocator, initial_value) = PreAllocator::init();

        if dbs_vec[0].get(&prefix_allocator.key).c(d!())?.is_none() {
            dbs_vec[0]
                .put_with_options(
                    &sync_write_opts(),
                    &prefix_allocator.key,
                    &initial_value,
                )
                .c(d!())?;
        }

        // Every fallible step has succeeded: safe to leak now.
        let dbs: [&'static DB; NUM_SHARDS] = dbs_vec
            .into_iter()
            .map(|db| -> &'static DB { Box::leak(Box::new(db)) })
            .collect::<Vec<_>>()
            .try_into()
            .ok()
            .expect("shard count mismatch");

        Ok(MmDB {
            dbs,
            prefix_allocator,
        })
    }

    /// Route a prefix to its shard.
    #[inline(always)]
    fn shard(&self, meta_prefix: &PreBytes) -> &'static DB {
        let prefix = u64::from_le_bytes(*meta_prefix);
        self.dbs[(prefix % NUM_SHARDS as u64) as usize]
    }

    /// Shard 0 holds meta keys (the prefix allocator).
    #[inline(always)]
    fn meta_db(&self) -> &'static DB {
        self.dbs[0]
    }

    pub(crate) fn alloc_prefix(&self) -> Pre {
        loop {
            let candidate = self.alloc_prefix_candidate();
            // Normal operation records no reservations, so allocation
            // stays lock-free here.
            if !RECOVERED_NONEMPTY.load(Ordering::Acquire) {
                return candidate;
            }
            let mut reserved = RECOVERED_PREFIXES.lock();
            if !reserved.remove(&candidate) {
                return candidate;
            }
            if reserved.is_empty() {
                RECOVERED_NONEMPTY.store(false, Ordering::Release);
            }
        }
    }

    /// Loads the persisted allocator state into the process-wide atomics
    /// (idempotent). `GLOBAL_FLOOR` snapshots the persisted value at
    /// initialization: every prefix below it was issued by a previous run.
    fn ensure_alloc_init(&self) {
        if GLOBAL_COUNTER.load(Ordering::Acquire) != 0 {
            return;
        }
        let _x = PREFIX_ALLOC_LOCK.lock();
        if GLOBAL_COUNTER.load(Ordering::Acquire) != 0 {
            return;
        }
        let ret = crate::common::parse_prefix!(
            self.meta_db()
                .get(&self.prefix_allocator.key)
                .expect("vsdb: meta read failed")
                .unwrap()
        );
        let new_ceil = ret + PREFIX_ALLOC_BATCH;
        self.meta_db()
            .put_with_options(
                &sync_write_opts(),
                &self.prefix_allocator.key,
                &new_ceil.to_le_bytes(),
            )
            .expect("vsdb: meta write failed");
        GLOBAL_FLOOR.store(ret, Ordering::Release);
        GLOBAL_CEILING.store(new_ceil, Ordering::Release);
        // The counter doubles as the init guard — store it last.
        GLOBAL_COUNTER.store(ret, Ordering::Release);
    }

    fn alloc_prefix_candidate(&self) -> Pre {
        LOCAL_NEXT.with(|next_cell| {
            LOCAL_CEIL.with(|ceil_cell| {
                let next = next_cell.get();
                let ceil = ceil_cell.get();
                if next > 0 && next < ceil {
                    next_cell.set(next + 1);
                    return next;
                }

                self.ensure_alloc_init();

                // Ensure this thread's cleanup guard is registered
                // before taking a batch, so the corresponding
                // `PENDING_WINDOWS` entry inserted below is guaranteed
                // to be removed when the thread exits (bounding the
                // registry to currently-live threads instead of every
                // thread that ever allocated a prefix).
                PENDING_WINDOW_GUARD.with(|_| {});

                // Claim the next batch and register it as this thread's
                // pending window in one exclusive section, so that
                // `reserve_recovered_prefix` (which reads the counter and
                // the registry under the read lock) can never observe the
                // advanced counter without the matching window.
                let batch_start = {
                    let mut reg = PENDING_WINDOWS.write();
                    let batch_start =
                        GLOBAL_COUNTER.fetch_add(PREFIX_ALLOC_BATCH, Ordering::AcqRel);
                    reg.insert(
                        thread::current().id(),
                        (batch_start, batch_start + PREFIX_ALLOC_BATCH),
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
                        self.meta_db()
                            .put_with_options(
                                &sync_write_opts(),
                                &self.prefix_allocator.key,
                                &new_ceil.to_le_bytes(),
                            )
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

    /// Validates a prefix recovered through a *safe* restore path
    /// (serde / `from_meta`), and reserves it when necessary.
    ///
    /// Returns `false` when the prefix lies outside the allocator-issued
    /// range (`< PREFIX_ALLOC_START` or `>= ceiling`) — such metadata cannot
    /// come from a legitimately allocated instance.
    ///
    /// Accepted prefixes need a reservation only if the allocator could
    /// still issue them in this run (`>= counter`, or inside a registered
    /// pending thread window). Prefixes below the process-start floor —
    /// the overwhelmingly common case for real restores — are accepted
    /// with a few atomic loads and no locking, keeping nested-handle
    /// decoding cheap and the reservation set bounded.
    pub(crate) fn reserve_recovered_prefix(&self, meta_prefix: PreBytes) -> bool {
        let prefix = Pre::from_le_bytes(meta_prefix);
        if prefix < PREFIX_ALLOC_START {
            return false;
        }
        self.ensure_alloc_init();

        if prefix >= GLOBAL_CEILING.load(Ordering::Acquire) {
            return false;
        }
        if prefix < GLOBAL_FLOOR.load(Ordering::Acquire) {
            // Issued by a previous run — can never be issued again.
            return true;
        }

        let pending = {
            let reg = PENDING_WINDOWS.read();
            prefix >= GLOBAL_COUNTER.load(Ordering::Acquire)
                || reg.values().any(|&(s, e)| (s..e).contains(&prefix))
        };
        if pending {
            RECOVERED_PREFIXES.lock().insert(prefix);
            RECOVERED_NONEMPTY.store(true, Ordering::Release);
        }
        true
    }

    pub(crate) fn flush(&self) {
        for db in &self.dbs {
            db.flush().expect("vsdb: mmdb flush failed");
        }
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
        &'a self,
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
            (PREFIX_ALLOC_START + Pre::MIN).to_le_bytes(),
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

/// The memory ceiling imposed on THIS process by its cgroup, if any.
///
/// systemd resource limits (`MemoryHigh`/`MemoryMax`) and container
/// runtimes express themselves as cgroup memory controllers; a process
/// running under one sees whole-host numbers in `/proc/meminfo`, so
/// cache sizing must clamp to the controller's limit or the engine's
/// caches alone can exceed the OOM-kill line. Returns the tightest
/// limit found walking from this process's cgroup up to the root
/// (limits are hierarchical: an ancestor's ceiling binds every
/// descendant), or `None` when unlimited/undetectable (non-Linux,
/// no cgroup, or all-`max` paths).
#[cfg(target_os = "linux")]
fn cgroup_mem_limit_bytes() -> Option<usize> {
    fn parse_limit(s: &str) -> Option<usize> {
        let t = s.trim();
        if t.is_empty() || t == "max" {
            return None;
        }
        // v1 reports "no limit" as a platform-dependent huge number
        // (PAGE_COUNTER_MAX); treat anything >= 2^60 as unlimited.
        t.parse::<usize>().ok().filter(|&v| v < (1usize << 60))
    }

    // /proc/self/cgroup line: `0::/system.slice/foo.service` (v2) or
    // `N:memory:/path` (v1).
    let cg = fs::read_to_string("/proc/self/cgroup").ok()?;
    let mut tightest: Option<usize> = None;
    let mut consider = |v: usize| {
        tightest = Some(tightest.map_or(v, |t| cmp::min(t, v)));
    };

    for line in cg.lines() {
        let mut parts = line.splitn(3, ':');
        let (Some(_), Some(controllers), Some(path)) =
            (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let (mount, files): (&str, &[&str]) = if controllers.is_empty() {
            // v2 unified hierarchy: memory.max is the hard (OOM) line;
            // memory.high is the throttle line -- respect the tighter.
            ("/sys/fs/cgroup", &["memory.max", "memory.high"])
        } else if controllers.split(',').any(|c| c == "memory") {
            ("/sys/fs/cgroup/memory", &["memory.limit_in_bytes"])
        } else {
            continue;
        };

        // Walk this cgroup and every ancestor up to the mount root.
        let mut rel = path.trim_start_matches('/');
        loop {
            for f in files {
                let p = if rel.is_empty() {
                    format!("{mount}/{f}")
                } else {
                    format!("{mount}/{rel}/{f}")
                };
                if let Some(v) =
                    fs::read_to_string(p).ok().as_deref().and_then(parse_limit)
                {
                    consider(v);
                }
            }
            match rel.rfind('/') {
                Some(i) => rel = &rel[..i],
                None if !rel.is_empty() => rel = "",
                None => break,
            }
        }
    }
    tightest
}

#[cfg(not(target_os = "linux"))]
fn cgroup_mem_limit_bytes() -> Option<usize> {
    None
}

fn mmdb_open(dir: &std::path::Path) -> Result<DB> {
    const G: usize = GB as usize;

    // Detect available physical memory (platform-specific).
    let host_avail_bytes: usize = {
        #[cfg(target_os = "linux")]
        {
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
        }
        #[cfg(any(target_os = "freebsd", target_os = "macos"))]
        {
            // FreeBSD: hw.physmem, macOS: hw.memsize (both return bytes)
            let key = if cfg!(target_os = "freebsd") {
                "hw.physmem"
            } else {
                "hw.memsize"
            };
            std::process::Command::new("sysctl")
                .arg("-n")
                .arg(key)
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .and_then(|s| s.trim().parse::<usize>().ok())
                .unwrap_or(G)
        }
        #[cfg(not(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "macos"
        )))]
        {
            G
        }
    };

    // The effective budget is the smallest of: host MemAvailable, this
    // process's own cgroup memory ceiling (a containerized/systemd-limited
    // process must not size caches off whole-host numbers -- the sum of
    // write buffers + block cache would land right at the cgroup's kill
    // line), and an explicit `VSDB_MEM_BUDGET_MB` override (highest
    // precedence bound; useful when the operator wants engine memory well
    // below any detected limit).
    let mut budget_limited = false;
    let avail_mem_bytes = {
        let mut budget = host_avail_bytes;
        if let Some(limit) = cgroup_mem_limit_bytes()
            && limit < budget
        {
            budget = limit;
            budget_limited = true;
        }
        if let Some(v) = std::env::var("VSDB_MEM_BUDGET_MB")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .and_then(|mb| mb.checked_mul(1024 * 1024))
            && v < budget
        {
            budget = v;
            budget_limited = true;
        }
        budget
    };

    // Per-shard sizes: divide totals by NUM_SHARDS.
    //
    // Under a DETECTED limit (cgroup or env override) the write-buffer
    // term must also scale with the budget: each shard can hold one
    // active memtable plus `max_immutable_memtables` frozen ones
    // awaiting flush, so the worst-case memtable footprint is
    // wr_buffer_size * (1 + max_immutable) * NUM_SHARDS. With the
    // legacy fixed floor (GB / NUM_SHARDS) that worst case is ~5 GB
    // regardless of any 2-3 GB ceiling -- an ingest burst then pins
    // anonymous memory at the throttle line, and the reclaim pressure
    // slows the very flush threads that are the only way out (observed
    // as a service wedged at memory.high with tens of thousands of
    // memory.events:high). budget/8 across all shards keeps the worst
    // case (5x active) at ~5/8 of budget, leaving the rest for the
    // block cache (budget/8) and per-connection transients. On
    // unconstrained hosts the sizing is unchanged.
    let legacy_wr = cmp::min(
        if avail_mem_bytes > 16 * G {
            avail_mem_bytes / 4 / NUM_SHARDS
        } else {
            G / NUM_SHARDS
        },
        512 * 1024 * 1024,
    );
    let wr_buffer_size = if budget_limited {
        cmp::max(
            cmp::min(legacy_wr, avail_mem_bytes / 8 / NUM_SHARDS),
            4 * 1024 * 1024,
        )
    } else {
        legacy_wr
    };

    let block_cache_size = (avail_mem_bytes as u64) / 8 / NUM_SHARDS as u64;

    // Single compaction thread per shard (16 shards = 16 parallel compactions)
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

        // Block cache + block size (per-shard)
        block_cache_capacity: block_cache_size,
        block_size: 16 * 1024, // 16 KB

        // Compaction tuning. Keep the compaction trigger well below mmdb's
        // write-slowdown trigger (8): with both at 8, the instant L0 becomes
        // compactable every write already pays the slowdown penalty — no
        // buffer zone for background compaction to work in.
        l0_compaction_trigger: 4,
        max_subcompactions: 4,

        // Single compaction thread per shard — 16 shards give natural parallelism
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
    fn cgroup_mem_limit_is_sane_or_absent() {
        // Non-Linux platforms must report None; on Linux the walk must
        // not panic and any detected limit must be a real bound (the
        // parser rejects v1's PAGE_COUNTER_MAX-style "unlimited"
        // sentinels), never zero.
        match cgroup_mem_limit_bytes() {
            None => {}
            Some(v) => {
                assert!(v > 0);
                assert!(v < (1usize << 60));
            }
        }
        #[cfg(not(target_os = "linux"))]
        assert!(cgroup_mem_limit_bytes().is_none());
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
        let before = PENDING_WINDOWS.read().len();

        let handle = thread::spawn(|| {
            let id = thread::current().id();
            PENDING_WINDOWS.write().insert(id, (0, 1));
            // Registers this thread's `PendingWindowGuard`, whose
            // `Drop` removes the entry above on thread exit.
            PENDING_WINDOW_GUARD.with(|_| {});
            assert!(PENDING_WINDOWS.read().contains_key(&id));
            id
        });
        let spawned_id = handle.join().unwrap();

        assert!(!PENDING_WINDOWS.read().contains_key(&spawned_id));
        assert_eq!(PENDING_WINDOWS.read().len(), before);
    }
}
