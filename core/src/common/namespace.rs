//!
//! # Namespaces — anonymous placement groups
//!
//! A [`Namespace`] is an independently-rooted engine instance: its own
//! base dir, mmdb shards, and `__SYSTEM__` tree. Collections created in
//! different namespaces share no data-path state (no WAL, compaction
//! queue, or memtable budget contention); the only shared components are
//! cold-path metadata (the global prefix allocator and the registry).
//!
//! Design: `docs/proposals/namespaces.md`. The load-bearing rules:
//!
//! * **Anonymous placement groups.** Users never name a namespace, never
//!   persist an id for one, never pass a path on the normal tier. The
//!   everyday primitive is *co-location*: `existing.namespace()` +
//!   `new_in`/[`Namespace::scope`].
//! * **`NsId` is a routing token**, not a user-facing name: it surfaces
//!   only at the admin tier ([`vsdb_ns_list`]/[`vsdb_ns_destroy`]/
//!   [`vsdb_ns_relocate`], epoch-rotation bookkeeping).
//! * **Path is configuration, not identity**: stored only in the
//!   registry; omitted, it derives from the id under
//!   `{default_base}/__NAMESPACES__/{ns_id:016x}` and is recorded as
//!   derived (`None`), so the whole universe stays movable as one tree.
//! * **One universe = one process**: registry mutations are serialized
//!   by an in-process mutex; there is no cross-process coordination
//!   anywhere (mmdb's per-shard LOCK rejects double-opens).
//!

use crate::common::{
    engine::{Engine, EngineSizing, root_holds_dataset, write_file_durable},
    error::{Result, VsdbError},
    vsdb_freeze_base_dir, vsdb_get_base_dir,
};
use parking_lot::Mutex;
use ruc::pnk;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::HashMap,
    fmt, fs, io,
    path::{Component, Path, PathBuf},
    str::FromStr,
    sync::{Arc, LazyLock},
    time::{SystemTime, UNIX_EPOCH},
};

/// A namespace identifier: small, stable, allocated once at creation,
/// never reused.
pub type NsId = u64;

/// The default namespace's id — a fixed constant, never allocated,
/// never present in the registry, never looked up.
pub const DEFAULT_NS_ID: NsId = 0;

/// Registry file, relative to the default base dir. Maps every
/// non-default `NsId` to its configuration.
const NS_REGISTRY_REL_PATH: &str = "__SYSTEM__/__namespaces__";

/// Parent dir (relative to the default base dir) of derived namespace
/// roots — namespaces created without an explicit path.
const NS_DERIVED_DIR: &str = "__NAMESPACES__";

/// Default shard count for non-default namespaces (the default
/// namespace is pinned to 16 forever). Leaner than the default ns:
/// most non-default namespaces are secondary datasets, and per-shard
/// cost is a compaction thread + WAL + memtable set + file handles.
const DEFAULT_NS_SHARDS: usize = 4;

/// Default memory budget for non-default namespaces, in MB. Deliberately
/// small and fixed: opening N namespaces must not silently multiply the
/// process footprint (the process-wide budget pipeline applies to the
/// default namespace only).
const DEFAULT_NS_BUDGET_MB: usize = 512;

/////////////////////////////////////////////////////////////////////////////

/// The complete public identity of a collection instance — the same
/// shape at every layer: in-memory comparison, the persisted meta bytes,
/// and this token. `ns: None` ⇔ default namespace ⇔ the 16-byte meta
/// form; a bare `u64` converts losslessly (`From<u64>` ⇒ `ns: None`).
///
/// `Display`/`FromStr` round-trip as `"42"` (default ns) or `"42@7"`
/// (ns 7) — config/log friendly.
///
/// **Canonical form**: the default namespace is spelled `ns: None`,
/// never `Some(DEFAULT_NS_ID)`. Every constructor under this type's
/// control (`From<u64>`, `FromStr`, `Deserialize`, and the handles'
/// `instance_id()`) canonicalizes, so `Eq`/`Hash` are reliable for
/// tokens obtained through the API. Routing treats both spellings as
/// the default namespace regardless.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(from = "InstanceIdWire")]
pub struct InstanceId {
    /// The storage prefix — what pre-v16 releases called `instance_id`.
    pub map_id: u64,
    /// The owning namespace; `None` = default namespace (canonical —
    /// see the type docs).
    pub ns: Option<NsId>,
}

/// Wire-side mirror of [`InstanceId`]: deserialization funnels through
/// it so a non-canonical `Some(DEFAULT_NS_ID)` folds to `None`.
#[derive(Deserialize)]
struct InstanceIdWire {
    map_id: u64,
    ns: Option<NsId>,
}

impl InstanceId {
    /// Canonical constructor: `DEFAULT_NS_ID` folds to `ns: None`.
    /// The single construction point handles use — keeps canonical-form
    /// logic out of every call site.
    pub fn new(map_id: u64, ns: NsId) -> Self {
        Self {
            map_id,
            ns: (ns != DEFAULT_NS_ID).then_some(ns),
        }
    }
}

impl From<InstanceIdWire> for InstanceId {
    fn from(w: InstanceIdWire) -> Self {
        Self {
            map_id: w.map_id,
            ns: w.ns.filter(|&n| n != DEFAULT_NS_ID),
        }
    }
}

impl From<u64> for InstanceId {
    fn from(map_id: u64) -> Self {
        Self { map_id, ns: None }
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.ns {
            None => write!(f, "{}", self.map_id),
            Some(ns) => write!(f, "{}@{}", self.map_id, ns),
        }
    }
}

impl FromStr for InstanceId {
    type Err = VsdbError;

    fn from_str(s: &str) -> Result<Self> {
        let parse = |v: &str, what: &str| {
            v.parse::<u64>().map_err(|_| VsdbError::Decode {
                detail: format!("invalid InstanceId {what}: {v:?}"),
            })
        };
        match s.split_once('@') {
            None => Ok(Self {
                map_id: parse(s, "map_id")?,
                ns: None,
            }),
            Some((m, n)) => Ok(Self {
                map_id: parse(m, "map_id")?,
                // "42@0" is a non-canonical spelling of the default
                // namespace: fold it, don't reject it.
                ns: Some(parse(n, "ns_id")?).filter(|&n| n != DEFAULT_NS_ID),
            }),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////

/// Creation-time options; persisted in the registry. Everything is
/// defaulted — this struct exists for the advanced tier.
#[derive(Clone, Debug)]
pub struct NamespaceOpts {
    /// `None` ⇒ derived under `{default_base}/__NAMESPACES__/`, recorded
    /// as derived so the whole universe stays movable as one tree.
    /// `Some` ⇒ explicit root (e.g. a dir on another volume), stored
    /// absolute and pinned ([`vsdb_ns_relocate`] to move). Rejected if
    /// it nests inside the default base dir or another registered
    /// namespace root (or vice versa).
    pub path: Option<PathBuf>,
    /// Shard count, fixed at creation (routing is `prefix % shards`).
    /// Clamped to `1..=64`.
    pub shards: usize,
    /// Memory budget in MB; `None` ⇒ a conservative fixed default.
    pub mem_budget_mb: Option<usize>,
}

impl Default for NamespaceOpts {
    fn default() -> Self {
        Self {
            path: None,
            shards: DEFAULT_NS_SHARDS,
            mem_budget_mb: None,
        }
    }
}

/// A registry entry as reported by [`vsdb_ns_list`].
#[derive(Clone, Debug)]
pub struct NsInfo {
    /// The namespace id.
    pub id: NsId,
    /// The resolved root directory.
    pub path: PathBuf,
    /// Whether `path` was explicit (`true`) or derived from the id.
    pub pinned: bool,
    /// Shard count fixed at creation.
    pub shards: usize,
    /// Creation time (unix seconds).
    pub created_at: u64,
}

/////////////////////////////////////////////////////////////////////////////

/// Persisted registry record (postcard).
#[derive(Serialize, Deserialize, Clone)]
struct NsRecord {
    id: NsId,
    /// Explicit root as UTF-8, or `None` = derived from the id.
    path: Option<String>,
    shards: u32,
    mem_budget_mb: Option<u64>,
    created_at: u64,
}

/// The whole registry file (postcard). `next_id` starts at 1 and never
/// decreases — ids are never reused, even across destroys.
#[derive(Serialize, Deserialize)]
struct RegistryFile {
    next_id: NsId,
    entries: Vec<NsRecord>,
}

impl Default for RegistryFile {
    fn default() -> Self {
        Self {
            next_id: 1,
            entries: Vec::new(),
        }
    }
}

/// Serializes every registry read-modify-write AND namespace open, so
/// two threads opening the same id cannot race into a double engine
/// open (mmdb's shard LOCK would fail the loser anyway — this keeps the
/// path clean instead of error-driven). Cold path only.
static REGISTRY_LOCK: Mutex<()> = Mutex::new(());

/// Every non-default namespace open in this process, by id. Each entry
/// holds one strong `Arc`; [`vsdb_ns_close`] removes an entry only
/// after proving it is the *last* strong reference, so a removal is
/// always immediately followed by the engine's teardown.
static OPEN_NAMESPACES: LazyLock<Mutex<HashMap<NsId, Namespace>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The default namespace: the owner of the default engine. A static
/// never drops, so the default engine lives for the whole process —
/// which is exactly why the default namespace is not closeable.
static DEFAULT_NS: LazyLock<Namespace> = LazyLock::new(|| {
    Namespace(Arc::new(NsInner {
        id: DEFAULT_NS_ID,
        path: vsdb_get_base_dir(),
        engine: pnk!(Engine::new()),
    }))
});

thread_local! {
    /// The ambient-placement stack driven by [`Namespace::scope`].
    static NS_STACK: RefCell<Vec<Namespace>> = const { RefCell::new(Vec::new()) };
}

fn registry_path() -> PathBuf {
    vsdb_get_base_dir().join(NS_REGISTRY_REL_PATH)
}

fn load_registry() -> Result<RegistryFile> {
    match fs::read(registry_path()) {
        Ok(bytes) => Ok(postcard::from_bytes(&bytes)?),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(RegistryFile::default()),
        Err(e) => Err(e.into()),
    }
}

/// Persists the registry durably (tmp + fsync + rename + parent-dir
/// fsync — losing the rename on power loss would orphan every
/// registered namespace root). Caller holds [`REGISTRY_LOCK`].
fn save_registry(reg: &RegistryFile) -> Result<()> {
    let path = registry_path();
    fs::create_dir_all(path.parent().expect("has parent"))?;
    let bytes = postcard::to_allocvec(reg)?;
    write_file_durable(&path, &bytes).map_err(VsdbError::from)
}

/// Resolves a record's root dir.
fn resolve_root(base: &Path, rec: &NsRecord) -> PathBuf {
    match &rec.path {
        Some(p) => PathBuf::from(p),
        None => base.join(NS_DERIVED_DIR).join(format!("{:016x}", rec.id)),
    }
}

/// Lexical is-prefix check on components.
fn path_contains(outer: &Path, inner: &Path) -> bool {
    let o: Vec<Component<'_>> = outer.components().collect();
    let i: Vec<Component<'_>> = inner.components().collect();
    i.len() >= o.len() && i[..o.len()] == o[..]
}

/// Best-effort physical normalization for overlap checks: canonicalize
/// the deepest EXISTING ancestor (resolving symlinks), then re-append
/// the not-yet-existing lexical tail. Falls back to the input when no
/// ancestor exists (then the lexical check still applies).
fn normalize_physical(p: &Path) -> PathBuf {
    let mut existing = p;
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    loop {
        match existing.canonicalize() {
            Ok(mut c) => {
                for seg in tail.iter().rev() {
                    c.push(seg);
                }
                return c;
            }
            Err(_) => match (existing.parent(), existing.file_name()) {
                (Some(parent), Some(name)) => {
                    tail.push(name.to_owned());
                    existing = parent;
                }
                _ => return p.to_path_buf(),
            },
        }
    }
}

fn ns_err(detail: impl Into<String>) -> VsdbError {
    VsdbError::Namespace {
        detail: detail.into(),
    }
}

/// Validates an explicit root against the default base and every other
/// registered root: no nesting in either direction.
///
/// Alias-hardened: `.`/`..` components are rejected outright, and the
/// overlap comparison runs on physically normalized paths (symlinked
/// spellings of the base or of another root are caught). Symlinks
/// created *after* registration are out of scope — that is filesystem
/// administration, not addressing.
fn validate_explicit_root(
    base: &Path,
    reg: &RegistryFile,
    candidate: &Path,
) -> Result<()> {
    if !candidate.is_absolute() {
        return Err(ns_err(format!(
            "namespace path must be absolute: {}",
            candidate.display()
        )));
    }
    if candidate.as_os_str().to_str().is_none() {
        return Err(ns_err(format!(
            "namespace path must be valid UTF-8: {}",
            candidate.display()
        )));
    }
    if candidate
        .components()
        .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(ns_err(format!(
            "namespace path must not contain `.`/`..` components: {}",
            candidate.display()
        )));
    }
    let cand_norm = normalize_physical(candidate);
    let base_norm = normalize_physical(base);
    if path_contains(&base_norm, &cand_norm) || path_contains(&cand_norm, &base_norm) {
        return Err(ns_err(format!(
            "namespace path {} overlaps the default base dir {}",
            candidate.display(),
            base.display()
        )));
    }
    for rec in &reg.entries {
        let other = normalize_physical(&resolve_root(base, rec));
        if path_contains(&other, &cand_norm) || path_contains(&cand_norm, &other) {
            return Err(ns_err(format!(
                "namespace path {} overlaps namespace {}'s root",
                candidate.display(),
                rec.id,
            )));
        }
    }
    Ok(())
}

/// A brand-new explicit root must be nonexistent or an empty dir;
/// returns whether it pre-existed (as an empty dir).
///
/// Adopting an existing non-empty directory is refused: a foreign
/// dataset's prefixes have unknown provenance (the allocator ceiling
/// lives under THIS universe's default base), so new allocations could
/// collide with the adopted data — and `destroy` would later delete
/// whatever else lived there. Importing/attaching foreign roots is an
/// explicit non-goal (see docs/proposals/namespaces.md §9).
fn ensure_root_adoptable(root: &Path) -> Result<bool> {
    match fs::read_dir(root) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Ok(mut entries) => {
            if entries.next().is_some() {
                Err(ns_err(format!(
                    "explicit namespace root {} already exists and is not \
                     empty; importing foreign data dirs is unsupported",
                    root.display()
                )))
            } else {
                Ok(true)
            }
        }
        Err(e) => Err(e.into()),
    }
}

/// Best-effort removal of a root a failed `create` (partially) filled,
/// so the same path is immediately retryable and no unregistered
/// VSDB-owned residue survives. Safe by construction: the adoptable
/// check proved the root was absent or empty before we touched it —
/// everything inside is ours. The dir itself is removed only if we
/// created it (`preexisted == false`), so a user-supplied mount point
/// is emptied, never deleted.
fn cleanup_failed_root(root: &Path, preexisted: bool) {
    if preexisted {
        if let Ok(entries) = fs::read_dir(root) {
            for e in entries.flatten() {
                let p = e.path();
                let _ = if p.is_dir() {
                    fs::remove_dir_all(&p)
                } else {
                    fs::remove_file(&p)
                };
            }
        }
    } else {
        let _ = fs::remove_dir_all(root);
    }
}

fn sizing_for(mem_budget_mb: Option<usize>) -> EngineSizing {
    EngineSizing::from_budget_mb(mem_budget_mb.unwrap_or(DEFAULT_NS_BUDGET_MB))
}

/////////////////////////////////////////////////////////////////////////////

struct NsInner {
    id: NsId,
    path: PathBuf,
    /// The engine, owned: when the last `Arc<NsInner>` drops (only ever
    /// via [`vsdb_ns_close`], which proves exclusivity first), the
    /// engine drops with it — flushing WALs, joining compaction
    /// threads, and releasing LOCK files.
    engine: Engine,
}

/// A cheap, cloneable handle to an engine instance (an *anonymous
/// placement group*). See the module docs for the design rules.
#[derive(Clone)]
pub struct Namespace(Arc<NsInner>);

impl fmt::Debug for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Namespace")
            .field("id", &self.0.id)
            .field("path", &self.0.path)
            .finish()
    }
}

impl Namespace {
    /// The implicit default namespace (the global engine). Infallible,
    /// registry-independent.
    pub fn default_ns() -> Namespace {
        DEFAULT_NS.clone()
    }

    /// Starts a NEW placement group: a fresh `NsId` on every call, root
    /// derived from the id — collision-free (ids are never reused).
    /// Zero parameters; tuning lives in [`Self::create_with`].
    pub fn create() -> Result<Namespace> {
        Self::create_with(NamespaceOpts::default())
    }

    /// [`Self::create`] with explicit options (volume placement, shard
    /// count, memory budget).
    pub fn create_with(opts: NamespaceOpts) -> Result<Namespace> {
        // The registry materializes under the default base dir, pinning
        // it — same rule as every other derived path.
        vsdb_freeze_base_dir();
        let base = vsdb_get_base_dir();
        let shards = opts.shards.clamp(1, 64);

        let _g = REGISTRY_LOCK.lock();
        let mut reg = load_registry()?;

        // Derived roots (id-named, fresh id) never pre-exist; explicit
        // roots may pre-exist as an empty dir (e.g. a mount point).
        let mut root_preexisted = false;
        if let Some(p) = &opts.path {
            validate_explicit_root(&base, &reg, p)?;
            root_preexisted = ensure_root_adoptable(p)?;
        }

        let id = reg.next_id;
        let rec = NsRecord {
            id,
            path: opts
                .path
                .as_ref()
                .map(|p| p.to_str().expect("validated UTF-8").to_owned()),
            shards: shards as u32,
            mem_budget_mb: opts.mem_budget_mb.map(|v| v as u64),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };
        let root = resolve_root(&base, &rec);

        // Reserve the id durably BEFORE opening the engine: a crash
        // after this point leaves a registered-but-empty namespace
        // (re-openable, destroyable), never an unreachable orphan dir.
        reg.next_id += 1;
        reg.entries.push(rec.clone());
        save_registry(&reg)?;

        match open_record_locked(&base, &rec, &root) {
            Ok(ns) => Ok(ns),
            Err(e) => {
                // Non-crash open failure (bad path, disk full, …): roll
                // the entry back so a failed `create` leaves no registry
                // residue. `next_id` deliberately stays advanced — ids
                // are never reused, and a burnt id is free. If the
                // rollback write itself fails we are simply in the
                // crash-equivalent state documented above: the entry is
                // visible in `vsdb_ns_list()`, re-openable via `open`
                // and reclaimable via `destroy`.
                reg.entries.retain(|r| r.id != rec.id);
                let _ = save_registry(&reg);
                // …and clear whatever the failed open left under the
                // root: an explicit path stays immediately retryable
                // (the adoptable check would otherwise refuse the now
                // non-empty dir forever), and a derived root leaves no
                // unregistered, never-reusable VSDB-owned residue.
                cleanup_failed_root(&root, root_preexisted);
                Err(e)
            }
        }
    }

    /// Opens an already-registered namespace by its stable id — the
    /// admin tier ([`vsdb_ns_list`], epoch-rotation bookkeeping).
    /// Normal flows never call this: deserialization and `from_meta`
    /// auto-open namespaces via the ids embedded in metas.
    ///
    /// `open(DEFAULT_NS_ID)` short-circuits to [`Self::default_ns`]
    /// without touching the registry. Idempotent in-process.
    pub fn open(id: NsId) -> Result<Namespace> {
        if id == DEFAULT_NS_ID {
            return Ok(Self::default_ns());
        }
        if let Some(ns) = OPEN_NAMESPACES.lock().get(&id) {
            return Ok(ns.clone());
        }
        // Reading the registry materializes a base-dir-derived path:
        // freeze the base dir (same contract as every other derived
        // path) so a later `vsdb_set_base_dir` fails loudly instead of
        // moving the allocator's backing store to another universe
        // under this already-open namespace.
        vsdb_freeze_base_dir();
        let base = vsdb_get_base_dir();

        let _g = REGISTRY_LOCK.lock();
        // Re-check under the lock: a racing open may have finished.
        if let Some(ns) = OPEN_NAMESPACES.lock().get(&id) {
            return Ok(ns.clone());
        }
        let reg = load_registry()?;
        let rec = reg.entries.iter().find(|r| r.id == id).ok_or_else(|| {
            ns_err(format!(
                "namespace {id} is not registered (destroyed, or from \
                 another universe)"
            ))
        })?;
        let root = resolve_root(&base, rec);
        open_record_locked(&base, rec, &root)
    }

    /// Scoped ambient placement: inside `f`, plain `MapxXXX::new()`
    /// creates its storage in `self`.
    ///
    /// **Placement only, never routing**: the ambient namespace is
    /// consulted at exactly one instant — collection creation. It never
    /// affects reads, writes, deserialization, or `from_meta` (those
    /// take the namespace from the handle/meta itself). Thread-local
    /// and nestable; popped on unwind; **not inherited by spawned
    /// threads** (pass handles or use `new_in` across threads).
    pub fn scope<R>(&self, f: impl FnOnce() -> R) -> R {
        /// Popped on drop so a panic inside `f` unwinds the stack too.
        struct PopGuard;
        impl Drop for PopGuard {
            fn drop(&mut self) {
                NS_STACK.with(|s| {
                    s.borrow_mut().pop();
                });
            }
        }
        NS_STACK.with(|s| s.borrow_mut().push(self.clone()));
        let _guard = PopGuard;
        f()
    }

    /// The top of this thread's scope stack; the default namespace when
    /// empty. (`MapxXXX::new()` ≡ `new_in(&Namespace::current())`.)
    pub fn current() -> Namespace {
        NS_STACK
            .with(|s| s.borrow().last().cloned())
            .unwrap_or_else(Self::default_ns)
    }

    /// This namespace's id — a getter; an input only at the admin tier.
    pub fn id(&self) -> NsId {
        self.0.id
    }

    /// The root directory of this namespace.
    pub fn path(&self) -> &Path {
        &self.0.path
    }

    /// This namespace's `__SYSTEM__` dir (internal metadata: instance
    /// metas, trie caches). Reserved for VSDB internal use.
    pub fn system_dir(&self) -> PathBuf {
        self.0.path.join("__SYSTEM__")
    }

    /// This namespace's instance-meta dir
    /// (`{root}/__SYSTEM__/__instance_meta__/`).
    pub fn meta_dir(&self) -> PathBuf {
        self.system_dir().join("__instance_meta__")
    }

    /// The meta file path for `map_id` inside this namespace's tree —
    /// the single source of truth for instance-meta naming (identical
    /// to the legacy `vsdb_meta_path` for the default namespace, whose
    /// root IS the base dir).
    pub fn meta_path(&self, map_id: u64) -> PathBuf {
        let mut p = self.meta_dir();
        p.push(format!("{:016x}", map_id));
        p
    }

    /// Flushes this namespace's engine to disk.
    pub fn flush(&self) {
        self.0.engine.flush()
    }

    /// The engine backing this namespace (crate-internal routing).
    ///
    /// A plain borrow of the `Arc`-owned engine: it cannot outlive the
    /// handle it came from, so no reference can survive a
    /// [`vsdb_ns_close`] (which requires every handle gone first).
    #[inline(always)]
    pub(crate) fn engine(&self) -> &Engine {
        &self.0.engine
    }

    /// Consuming form of [`vsdb_ns_close`]: closes this namespace,
    /// releasing **all** of its resources (see `vsdb_ns_close` for the
    /// full contract — flush-first teardown, registry untouched,
    /// re-openable afterwards).
    ///
    /// `self` must be the *last* live handle: every collection handle,
    /// iterator, and other `Namespace` clone must already be dropped.
    /// The consumed `self` itself is accounted for — unlike
    /// `vsdb_ns_close(id)`, no separate `drop(ns)` is needed first.
    ///
    /// # Errors
    ///
    /// - `Err((Some(handle), e))` — the close was **refused** (other
    ///   live handles, or the default namespace): nothing happened, and
    ///   the consumed handle is returned for continued use.
    /// - `Err((None, e))` — the close **ran** but the engine teardown
    ///   reported an error while flushing/syncing: the namespace is no
    ///   longer open (same terminal state as `vsdb_ns_close` returning
    ///   an error), so there is no handle to give back.
    ///
    /// ```ignore
    /// match ns.close() {
    ///     Ok(()) => {}
    ///     Err((Some(ns), e)) => { /* refused — `ns` is still usable */ }
    ///     Err((None, e)) => { /* closed, but teardown reported `e` */ }
    /// }
    /// ```
    pub fn close(self) -> std::result::Result<(), (Option<Namespace>, VsdbError)> {
        let id = self.0.id;
        if id == DEFAULT_NS_ID {
            return Err((Some(self), ns_err("the default namespace cannot be closed")));
        }
        let _g = REGISTRY_LOCK.lock();
        let ns_owned = {
            let mut open = OPEN_NAMESPACES.lock();
            let Some(entry) = open.get(&id) else {
                // Unreachable through safe use (a live handle pins its
                // table entry: removal proves exclusivity first), kept
                // as a defensive error path rather than a panic.
                return Err((
                    Some(self),
                    ns_err(format!("namespace {id} is not open in this process")),
                ));
            };
            // A live handle is always a clone of the current table
            // entry: the entry can only be replaced by a successful
            // close, which proves no external handle existed.
            debug_assert!(Arc::ptr_eq(&entry.0, &self.0));
            // Strong refs accounted here: the table's entry + `self`.
            // Stable while both locks are held — see `vsdb_ns_close`.
            let others = Arc::strong_count(&entry.0) - 2;
            if others > 0 {
                return Err((
                    Some(self),
                    ns_err(format!(
                        "namespace {id} still has {others} other live handle(s); \
                         drop every collection handle and `Namespace` clone first"
                    )),
                ));
            }
            // Provably exclusive; release `self`'s ref (count 2 -> 1)
            // and take sole ownership through the table's entry.
            drop(self);
            open.remove(&id).expect("present: checked above")
        };
        let inner = Arc::try_unwrap(ns_owned.0)
            .unwrap_or_else(|_| unreachable!("count was 1 under both locks"));
        inner.engine.close().map_err(|e| (None, VsdbError::from(e)))
    }
}

/// The record's shard count, bounds-checked. Registry entries are
/// written pre-clamped, so an out-of-range count means corruption or
/// hand-editing — refuse cleanly: `shards == 0` would otherwise reach
/// `prefix % 0` (a release-mode panic) on the first routed operation,
/// and would vacuously pass `root_holds_dataset`'s per-shard checks.
fn validated_shards(rec: &NsRecord) -> Result<usize> {
    let shards = rec.shards as usize;
    if !(1..=64).contains(&shards) {
        return Err(ns_err(format!(
            "registry entry for namespace {} carries an invalid shard \
             count ({}); the registry file is damaged",
            rec.id, rec.shards
        )));
    }
    Ok(shards)
}

/// Opens the engine for `rec` and caches the handle. Caller holds
/// [`REGISTRY_LOCK`] (serializes double-opens).
fn open_record_locked(_base: &Path, rec: &NsRecord, root: &Path) -> Result<Namespace> {
    let shards = validated_shards(rec)?;
    let sizing = sizing_for(rec.mem_budget_mb.map(|v| v as usize));
    let engine = Engine::open_at(root, shards, sizing).map_err(VsdbError::from)?;
    let ns = Namespace(Arc::new(NsInner {
        id: rec.id,
        path: root.to_path_buf(),
        engine,
    }));
    OPEN_NAMESPACES.lock().insert(rec.id, ns.clone());
    Ok(ns)
}

/////////////////////////////////////////////////////////////////////////////

/// Lists every registered (non-default) namespace.
pub fn vsdb_ns_list() -> Result<Vec<NsInfo>> {
    // Reading the registry materializes base-dir-derived paths — same
    // freeze contract as open/destroy/relocate, so the returned roots
    // cannot be split from the universe by a later `vsdb_set_base_dir`.
    vsdb_freeze_base_dir();
    let base = vsdb_get_base_dir();
    let _g = REGISTRY_LOCK.lock();
    let reg = load_registry()?;
    Ok(reg
        .entries
        .iter()
        .map(|rec| NsInfo {
            id: rec.id,
            path: resolve_root(&base, rec),
            pinned: rec.path.is_some(),
            shards: rec.shards as usize,
            created_at: rec.created_at,
        })
        .collect())
}

/// Destroys a namespace: removes its registry entry, then deletes its
/// whole directory tree — O(1) bulk reclaim.
///
/// The target must not be open in this process ([`vsdb_ns_close`] it
/// first). A crash between the registry update and the tree removal
/// leaves an orphaned-but-harmless dir.
pub fn vsdb_ns_destroy(id: NsId) -> Result<()> {
    if id == DEFAULT_NS_ID {
        return Err(ns_err("the default namespace cannot be destroyed"));
    }
    vsdb_freeze_base_dir();
    let base = vsdb_get_base_dir();
    let _g = REGISTRY_LOCK.lock();
    // The not-open check MUST run under REGISTRY_LOCK: `open` inserts
    // into OPEN_NAMESPACES while holding it, so checking here closes
    // the TOCTOU window where a racing open could cache a live engine
    // whose root we are about to delete.
    if OPEN_NAMESPACES.lock().contains_key(&id) {
        return Err(ns_err(format!(
            "namespace {id} is open in this process; destroy requires a \
             not-open target"
        )));
    }
    let mut reg = load_registry()?;
    let Some(pos) = reg.entries.iter().position(|r| r.id == id) else {
        return Err(ns_err(format!("namespace {id} is not registered")));
    };
    let root = resolve_root(&base, &reg.entries[pos]);
    reg.entries.remove(pos);
    save_registry(&reg)?;
    match fs::remove_dir_all(&root) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Re-points a namespace at a new root directory (e.g. after moving a
/// volume). Updates the registry only — **moving the data is the
/// operator's job**, done before calling this. A target that does not
/// already hold an initialized dataset (format marker + per-shard
/// engine anchors) is refused: repointing at it would durably orphan
/// the real data behind a silent success. Whether it is the *right*
/// dataset cannot be verified — roots carry no namespace id.
///
/// The target must not be open in this process.
pub fn vsdb_ns_relocate(id: NsId, new_path: impl AsRef<Path>) -> Result<()> {
    if id == DEFAULT_NS_ID {
        return Err(ns_err(
            "the default namespace's root is the base dir; relocate it \
             via VSDB_BASE_DIR / vsdb_set_base_dir before first use",
        ));
    }
    vsdb_freeze_base_dir();
    let base = vsdb_get_base_dir();
    let new_path = new_path.as_ref();

    let _g = REGISTRY_LOCK.lock();
    // Under REGISTRY_LOCK for the same TOCTOU reason as destroy.
    if OPEN_NAMESPACES.lock().contains_key(&id) {
        return Err(ns_err(format!(
            "namespace {id} is open in this process; relocate requires a \
             not-open target"
        )));
    }
    let mut reg = load_registry()?;
    let Some(rec) = reg.entries.iter().find(|r| r.id == id) else {
        return Err(ns_err(format!("namespace {id} is not registered")));
    };
    let rec_shards = validated_shards(rec)?;
    // Validate against every OTHER root (skip the record being moved).
    let mut probe = reg.clone_without(id);
    validate_explicit_root(&base, &probe, new_path)?;
    drop(probe.entries.drain(..));

    // The registry only re-points; moving the data is the operator's
    // job — done BEFORE calling this. Repointing at a dir that does not
    // hold an initialized dataset (marker + per-shard engine anchors)
    // would durably orphan the real data with zero errors: the next
    // `open` would silently initialize a fresh, empty root. Refuse
    // instead. (Which dataset lives there cannot be verified — roots
    // carry no namespace id; that part stays on the operator.)
    if !root_holds_dataset(new_path, rec_shards) {
        return Err(ns_err(format!(
            "relocate target {} does not hold an initialized dataset \
             (expected a format marker and {rec_shards} shard dir(s) \
             each containing engine files); move namespace {id}'s data \
             there first, then relocate",
            new_path.display(),
        )));
    }

    let rec = reg
        .entries
        .iter_mut()
        .find(|r| r.id == id)
        .expect("checked above");
    rec.path = Some(
        new_path
            .to_str()
            .expect("validated UTF-8 in validate_explicit_root")
            .to_owned(),
    );
    save_registry(&reg)
}

/// Closes an open namespace, releasing **all** of its resources: engine
/// memory, compaction threads, fds, and mmdb `LOCK` files. The active
/// memtables are flushed and the WALs synced first (errors surface
/// here, unlike a plain drop).
///
/// Refused unless every handle is gone: all collection handles,
/// iterators, and `Namespace` clones must be dropped first — `close`
/// either reclaims a provably-unreferenced namespace or returns an
/// error naming the live-handle count; it never invalidates a live
/// handle. Refused for the default namespace.
///
/// The registry entry is untouched: a closed namespace can be re-opened
/// via [`Namespace::open`] (restart-equivalent recovery) or reclaimed
/// via [`vsdb_ns_destroy`] — `create → fill → close → destroy` is the
/// in-process epoch-rotation loop.
///
/// Detached snapshot iterators (e.g. `MapxRaw::range_detached`) hold
/// their engine sources via internal refcounts, not through the
/// namespace handle: one may outlive a `close` and keep yielding its
/// (consistent, stale) snapshot. Memory-safe by construction; don't
/// rely on it observing the close.
///
/// The handle-consuming form is [`Namespace::close`], which accounts
/// for the handle it consumes and returns it on refusal.
pub fn vsdb_ns_close(id: NsId) -> Result<()> {
    if id == DEFAULT_NS_ID {
        return Err(ns_err("the default namespace cannot be closed"));
    }
    let _g = REGISTRY_LOCK.lock();
    let ns = {
        let mut open = OPEN_NAMESPACES.lock();
        let Some(entry) = open.get(&id) else {
            return Err(ns_err(format!(
                "namespace {id} is not open in this process"
            )));
        };
        // The table's own strong ref is the `- 1`. Stable while both
        // locks are held: cloning requires an existing `Namespace`, and
        // a count of 1 proves none exists outside the table (the other
        // cloning paths — `open`'s cache hit and `flush_all_open` —
        // block on the table lock).
        let live = Arc::strong_count(&entry.0) - 1;
        if live > 0 {
            return Err(ns_err(format!(
                "namespace {id} still has {live} live handle(s); drop \
                 every collection handle and `Namespace` clone first"
            )));
        }
        open.remove(&id).expect("present: checked above")
        // The table lock is released here — the (possibly slow) engine
        // teardown below must not block unrelated namespaces' cache
        // hits; REGISTRY_LOCK keeps serializing open/create/destroy of
        // THIS id until the teardown finished.
    };
    let inner = Arc::try_unwrap(ns.0)
        .unwrap_or_else(|_| unreachable!("count was 1 under both locks"));
    inner.engine.close().map_err(VsdbError::from)
}

impl RegistryFile {
    fn clone_without(&self, id: NsId) -> RegistryFile {
        RegistryFile {
            next_id: self.next_id,
            entries: self
                .entries
                .iter()
                .filter(|r| r.id != id)
                .cloned()
                .collect(),
        }
    }
}

/// Flushes every open non-default namespace (the default engine is
/// flushed separately by `vsdb_flush`).
///
/// Handles are cloned out first: engine flushes can take seconds, and
/// holding the table lock across them would block every concurrent
/// `Namespace::open`/meta restore.
pub(crate) fn flush_all_open() {
    let handles: Vec<Namespace> = OPEN_NAMESPACES.lock().values().cloned().collect();
    for ns in handles {
        ns.flush();
    }
}
