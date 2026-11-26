//!
//! # Namespaces — anonymous placement groups
//!
//! A [`Namespace`] is an independently-rooted engine instance: its own
//! base dir, mmdb shards, and `__SYSTEM__` tree. Collections created in
//! different namespaces share no data-path state (no WAL, compaction
//! queue, or memtable budget contention); the only shared components are
//! cold-path metadata (the global prefix allocator and the registry).
//!
//! See `docs/namespaces.md` for namespace placement and lifetime rules.
//!
//! * **Anonymous placement groups.** Users never name a namespace, never
//!   persist an id for one, never pass a path on the normal tier. The
//!   everyday primitive is namespace placement: `existing.namespace()` +
//!   `new_in`/[`Namespace::scope`]. This alone does not select the same shard.
//! * **`NsId` is a routing token**, not a user-facing name: it surfaces
//!   only at the admin tier ([`Namespace::list`]/[`Namespace::destroy`]/
//!   [`Namespace::relocate`], epoch-rotation bookkeeping).
//! * **Path is configuration, not identity**: stored only in the
//!   registry; omitted, it derives from the id under
//!   `{default_base}/__NAMESPACES__/{ns_id:016x}` and is recorded as
//!   derived (`None`), so the whole universe stays movable as one tree.
//! * **One writable process per universe.** The writer holds an exclusive
//!   universe lock. Readers take a shared lock when the lock file exists.
//!   On Unix, MMDB also locks existing shard lock files in the same mode.
//!

use crate::common::{
    atomic_write_file,
    engine::{Engine, EngineSizing},
    error::{Result, VsdbError, corrupt},
    vsdb_freeze_base_dir, vsdb_get_base_dir, vsdb_is_read_only,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    fmt, fs, io,
    path::{Component, Path, PathBuf},
    str::FromStr,
    sync::{
        Arc, LazyLock, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

/// Byte charges in the two independently bounded evictable cache stores.
pub use mmdb::BlockCachePoolUsage;
/// Memory attributed to a shard's current memtables and its L0 block pins.
pub use mmdb::DbMemoryUsage as ShardMemoryUsage;

/// Engine-side memory observations for one namespace. Shard order matches
/// [`Namespace::shard_properties`]. Gauges are sampled independently.
/// Caller-retained pins, old read views and collection caches can retain
/// additional memory outside these charges.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct NamespaceMemoryUsage {
    /// The shared evictable pool, counted once for all shards.
    pub pool: BlockCachePoolUsage,
    /// Member-local observations, one per shard.
    pub shards: Vec<ShardMemoryUsage>,
    /// The active write-buffer freeze threshold on each shard, in bytes.
    pub write_buffer_bytes_per_shard: usize,
    /// Maximum queued immutable generations on each writable shard.
    pub immutable_limit_per_shard: usize,
}

/// A namespace identifier: small, stable, allocated once at creation,
/// never reused.
pub type NsId = u64;

/// The default namespace's id — a fixed constant, never allocated,
/// never present in the registry, never looked up.
pub const DEFAULT_NS_ID: NsId = 0;

/// Registry file, relative to the default base dir. Maps every
/// non-default `NsId` to its configuration.
const NS_REGISTRY_REL_PATH: &str = "__SYSTEM__/__namespaces__";

/// Records explicit-root creations currently in flight or rolled back before
/// directory cleanup could finish, relative to the default base dir.
const NS_PENDING_CREATE_REL_PATH: &str = "__SYSTEM__/__pending_create__";

/// Parent dir (relative to the default base dir) of derived namespace
/// roots — namespaces created without an explicit path.
const NS_DERIVED_DIR: &str = "__NAMESPACES__";

/// Default shard count for non-default namespaces (the default
/// namespace is pinned to 16 forever). Leaner than the default ns:
/// most non-default namespaces are secondary datasets, and per-shard
/// cost is a compaction thread + WAL + memtable set + file handles.
const DEFAULT_NS_SHARDS: usize = 4;

/// Default memory budget for non-default namespaces, in MB. Deliberately
/// small and fixed so each new namespace does not receive a host-sized
/// allowance. Each open namespace still adds memory costs. The default
/// namespace uses 2 GiB unless configured through `VsdbOptions` or
/// `VSDB_MEM_BUDGET_MB`; neither budget is a hard RSS limit.
const DEFAULT_NS_BUDGET_MB: usize = 512;

/////////////////////////////////////////////////////////////////////////////

/// Identifies a collection by its map prefix and namespace within a universe.
/// A bare `u64` selects the default namespace (`ns: None`). The v18
/// `VSMAPX02` raw handle stores both IDs in a fixed 24-byte frame,
/// encoding the default namespace as zero.
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
    /// absolute and pinned ([`Namespace::relocate`] to move). Rejected if
    /// it nests inside the default base dir or another registered
    /// namespace root (or vice versa).
    pub path: Option<PathBuf>,
    /// Shard count, fixed at creation (routing is `prefix % shards`).
    /// Clamped to `1..=64`.
    pub shards: usize,
    /// Memory sizing input in MiB; `None` ⇒ 512 MiB. Cache/buffer floors
    /// and caps apply; this is not a hard RSS limit.
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

/// A registry entry as reported by [`Namespace::list`].
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
    state: NsLifecycleState,
}

/// The whole registry file (postcard). `next_id` starts at 1 and never
/// decreases — ids are never reused, even across destroys.
#[derive(Serialize, Deserialize)]
pub(super) struct RegistryFile {
    next_id: NsId,
    entries: Vec<NsRecord>,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
enum NsLifecycleState {
    Pending,
    Established,
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
/// holds one strong `Arc`; [`Namespace::close_by_id`] removes an entry only
/// after proving it is the *last* strong reference, so a removal is
/// always immediately followed by the engine's teardown.
static OPEN_NAMESPACES: LazyLock<Mutex<HashMap<NsId, Namespace>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The default namespace: the owner of the default engine. A static
/// never drops, so the default engine lives for the whole process —
/// which is exactly why the default namespace is not closeable.
static DEFAULT_NS: OnceLock<Namespace> = OnceLock::new();
static DEFAULT_INIT: Mutex<()> = Mutex::new(());
static REGISTRY_DIR_UNSYNCED: AtomicBool = AtomicBool::new(false);

thread_local! {
    /// The ambient-placement stack driven by [`Namespace::scope`].
    static NS_STACK: RefCell<Vec<Namespace>> = const { RefCell::new(Vec::new()) };
}

const REGISTRY_MAGIC: &[u8; 8] = b"VSNREG02";
const PENDING_CREATE_MAGIC: &[u8; 8] = b"VSNPCR01";

#[derive(Serialize, Deserialize, Clone)]
struct PendingCreate {
    universe: u128,
    id: NsId,
    shards: u32,
    path: String,
}

fn read_pending_creates(base: &Path) -> Result<Vec<PendingCreate>> {
    let bytes = match fs::read(base.join(NS_PENDING_CREATE_REL_PATH)) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        result => result?,
    };
    let payload = bytes
        .strip_prefix(PENDING_CREATE_MAGIC)
        .ok_or_else(|| corrupt("invalid pending namespace creation format"))?;
    let (entries, rest) = postcard::take_from_bytes(payload)
        .map_err(|e| corrupt(format!("invalid pending namespace creation: {e}")))?;
    if !rest.is_empty() {
        return Err(corrupt("trailing pending namespace creation data"));
    }
    Ok(entries)
}

fn save_pending_creates(base: &Path, entries: &[PendingCreate]) -> Result<()> {
    let mut bytes = PENDING_CREATE_MAGIC.to_vec();
    postcard::to_io(entries, &mut bytes)?;
    REGISTRY_DIR_UNSYNCED.store(true, Ordering::Release);
    atomic_write_file(&base.join(NS_PENDING_CREATE_REL_PATH), &bytes)?;
    REGISTRY_DIR_UNSYNCED.store(false, Ordering::Release);
    Ok(())
}

/// Make a directory entry change durable. Unlink and rename survive power
/// loss only after their parent directory is synchronized.
fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    fs::File::open(parent)?.sync_all()
}

/// Fence an empty root or its absence before dropping the last recovery record.
fn durably_empty_root(path: &Path) -> io::Result<bool> {
    match fs::read_dir(path) {
        Ok(mut entries) => {
            if entries.next().transpose()?.is_some() {
                return Ok(false);
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    for ancestor in path.ancestors() {
        match fs::File::open(ancestor) {
            Ok(dir) => {
                dir.sync_all()?;
                return Ok(true);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(false)
}

fn record_pending_create(
    base: &Path,
    reg: &RegistryFile,
    pending: PendingCreate,
) -> Result<()> {
    let mut list = read_pending_creates(base)?;
    list.retain(|e| {
        e.universe == pending.universe
            && e.id != pending.id
            && !reg
                .entries
                .iter()
                .any(|r| r.id == e.id && r.state == NsLifecycleState::Established)
            && (reg.entries.iter().any(|r| r.id == e.id)
                || !durably_empty_root(Path::new(&e.path)).unwrap_or(false))
    });
    list.push(pending);
    save_pending_creates(base, &list)
}

fn clear_pending_create(base: &Path, id: NsId) -> Result<()> {
    let mut list = read_pending_creates(base)?;
    let before = list.len();
    list.retain(|e| e.id != id);
    if list.len() != before {
        save_pending_creates(base, &list)?;
    }
    Ok(())
}

fn read_registry(base: &Path) -> Result<RegistryFile> {
    let bytes = match fs::read(base.join(NS_REGISTRY_REL_PATH)) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(corrupt("missing namespace registry"));
        }
        result => result?,
    };
    let payload = bytes
        .strip_prefix(REGISTRY_MAGIC)
        .ok_or_else(|| corrupt("invalid namespace registry format"))?;
    let (reg, rest) = postcard::take_from_bytes(payload)
        .map_err(|e| corrupt(format!("invalid namespace registry: {e}")))?;
    if !rest.is_empty() {
        return Err(corrupt("trailing namespace registry data"));
    }
    validate_registry(&reg)?;
    Ok(reg)
}

fn load_registry() -> Result<RegistryFile> {
    super::universe::active()?;
    let base = vsdb_get_base_dir();
    if !vsdb_is_read_only() && REGISTRY_DIR_UNSYNCED.load(Ordering::Acquire) {
        fs::File::open(base.join("__SYSTEM__"))?.sync_all()?;
        REGISTRY_DIR_UNSYNCED.store(false, Ordering::Release);
    }
    read_registry(&base)
}
pub(super) fn check_registry_at(base: &Path) -> Result<()> {
    read_registry(base).map(drop)
}

pub(super) fn bootstrap_files_valid(base: &Path) -> Result<bool> {
    let registry = base.join(NS_REGISTRY_REL_PATH);
    if registry.try_exists()? {
        let Ok(reg) = read_registry(base) else {
            return Ok(false);
        };
        if reg.next_id != 1 || !reg.entries.is_empty() {
            return Ok(false);
        }
    }
    let ceiling = base.join("__SYSTEM__/__prefix_ceiling__");
    Ok(!ceiling.try_exists()?
        || fs::read(ceiling)? == super::PREFIX_ALLOC_START.to_le_bytes())
}
pub(super) fn bootstrap_registry(base: &Path) -> Result<()> {
    RegistryFile::default().save(base)
}

/// Completed engine roots to scan before the first engine opens in a
/// universe with a missing ceiling. Unavailable established roots are an
/// error: they could contain the only evidence of issued prefixes.
pub(crate) fn recovery_roots(base: &Path, universe: u128) -> Result<Vec<PathBuf>> {
    let registry = read_registry(base)?;
    let mut roots = Vec::new();
    for rec in registry.entries {
        let root = resolve_root(base, &rec);
        if rec.state == NsLifecycleState::Established {
            super::universe::verify_engine(
                &root,
                universe,
                rec.id,
                rec.shards as usize,
            )?;
            roots.push(root);
        } else if root.try_exists()? {
            super::universe::validate_pending_root(
                &root,
                universe,
                rec.id,
                rec.shards as usize,
            )?;
            super::universe::validate_ready_engine(
                &root,
                universe,
                rec.id,
                rec.shards as usize,
            )?;
            if root.join("__SYSTEM__/engine_header").try_exists()? {
                roots.push(root);
            }
        }
    }
    Ok(roots)
}

/// Rejects a registry whose allocator or entry set cannot describe a
/// valid universe. A lost/reset registry must not silently reuse ids or
/// adopt roots from an earlier namespace.
fn validate_registry(reg: &RegistryFile) -> Result<()> {
    if reg.next_id == DEFAULT_NS_ID {
        return Err(ns_err(
            "registry next_id is zero; the default id is reserved",
        ));
    }
    let mut seen = HashSet::with_capacity(reg.entries.len());
    let mut max_id = DEFAULT_NS_ID;
    for rec in &reg.entries {
        if rec.id == DEFAULT_NS_ID {
            return Err(ns_err(
                "registry contains the reserved default namespace id 0",
            ));
        }
        if !seen.insert(rec.id) {
            return Err(ns_err(format!(
                "registry contains duplicate namespace id {}",
                rec.id
            )));
        }
        if !(1u32..=64).contains(&rec.shards) {
            return Err(ns_err(format!(
                "registry entry {} has invalid shard count {}",
                rec.id, rec.shards
            )));
        }
        max_id = max_id.max(rec.id);
    }
    if reg.next_id <= max_id {
        return Err(ns_err(format!(
            "registry next_id {} does not exceed the largest entry id {}",
            reg.next_id, max_id
        )));
    }
    Ok(())
}

/// Persists the registry durably (tmp + fsync + rename + parent-dir
/// fsync — losing the rename on power loss would orphan every
/// registered namespace root). Caller holds [`REGISTRY_LOCK`].
fn save_registry(reg: &RegistryFile) -> Result<()> {
    reg.save(&vsdb_get_base_dir())
}

/// Resolves a record's root dir.
fn resolve_root(base: &Path, rec: &NsRecord) -> PathBuf {
    match &rec.path {
        Some(p) => PathBuf::from(p),
        None => derived_root(base, rec.id),
    }
}

fn derived_root(base: &Path, id: NsId) -> PathBuf {
    base.join(NS_DERIVED_DIR).join(format!("{id:016x}"))
}

/// Lexical is-prefix check on components.
fn path_contains(outer: &Path, inner: &Path) -> bool {
    let o: Vec<Component<'_>> = outer.components().collect();
    let i: Vec<Component<'_>> = inner.components().collect();
    i.len() >= o.len() && i[..o.len()] == o[..]
}

/// Lexically resolves `.` and `..` components without touching the
/// filesystem. Used after making a path absolute, so a not-yet-existing
/// `..` tail cannot hide an overlap from the component check.
fn lexical_normalize(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in p.components() {
        match component {
            Component::ParentDir => {
                if !out.pop() {
                    out.push(component.as_os_str());
                }
            }
            Component::CurDir => {}
            _ => out.push(component.as_os_str()),
        }
    }
    out
}

/// Best-effort physical normalization for overlap checks: canonicalize
/// the deepest EXISTING ancestor (resolving symlinks), then re-append
/// the not-yet-existing lexical tail. A relative path is first made
/// absolute and its `.`/`..` components are resolved lexically, so a
/// spelling such as `vsdb_data` or `/srv/new/../data` cannot bypass the
/// overlap check when those prefixes do not exist yet.
fn normalize_physical(p: &Path) -> PathBuf {
    if let Ok(canonical) = p.canonicalize() {
        return canonical;
    }
    let absolute = std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf());
    let normalized = lexical_normalize(&absolute);
    let mut existing = normalized.as_path();
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
                _ => return normalized,
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
/// whatever else lived there. See `docs/namespaces.md` for placement and relocation.
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

fn is_atomic_tmp_for(name: &str, prefix: &str) -> bool {
    name.strip_prefix(prefix)
        .and_then(|s| s.strip_suffix(".tmp"))
        .and_then(|s| s.split_once('.'))
        .is_some_and(|(pid, seq)| {
            pid.parse::<u32>().is_ok() && seq.parse::<u64>().is_ok()
        })
}

fn is_empty_shard_file(entry: &fs::DirEntry) -> Result<bool> {
    if !entry.file_type()?.is_file() {
        return Ok(false);
    }
    let raw = entry.file_name();
    let Some(name) = raw.to_str() else {
        return Ok(false);
    };
    let is_u64 = |s: &str| {
        !s.is_empty()
            && s.bytes().all(|b| b.is_ascii_digit())
            && s.parse::<u64>().is_ok()
    };
    if name == "LOCK"
        || name == "CURRENT"
        || name == "CURRENT.tmp"
        || name.strip_prefix("CURRENT.tmp.").is_some_and(is_u64)
        || name.strip_prefix("MANIFEST-").is_some_and(is_u64)
    {
        return Ok(true);
    }
    if let Some(stem) = name.strip_suffix(".wal")
        && is_u64(stem)
    {
        return Ok(entry.metadata()?.len() == 0);
    }
    Ok(false)
}

/// The caller holds the root lock. Keep an identity file until the removal
/// of all other engine artifacts is durable, so a partial cleanup is retryable.
fn cleanup_failed_root(
    root: &Path,
    preexisted: bool,
    universe_id: u128,
    ns: NsId,
    shards: usize,
) -> Result<bool> {
    let expected_identity = super::universe::engine_bytes(universe_id, ns, shards);
    let mut identity = None;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            return Ok(false);
        }
        let name = entry.file_name();
        if name == "__SYSTEM__" {
            for sys in fs::read_dir(entry.path())? {
                let sys = sys?;
                if !sys.file_type()?.is_file() {
                    return Ok(false);
                }
                let raw = sys.file_name();
                let Some(fname) = raw.to_str() else {
                    return Ok(false);
                };
                match fname {
                    "engine_header" | "__initializing__" => {
                        if fs::read(sys.path())? != expected_identity {
                            return Ok(false);
                        }
                        identity = Some(sys.path());
                    }
                    ".shards_ready" | "format_version" => {
                        if fs::read(sys.path())? != b"18" {
                            return Ok(false);
                        }
                    }
                    "LOCK" => {}
                    other
                        if is_atomic_tmp_for(other, "engine_header.")
                            || is_atomic_tmp_for(other, "__initializing__.") =>
                    {
                        let staged = fs::read(sys.path())?;
                        if staged.len() > expected_identity.len()
                            || !expected_identity.starts_with(&staged)
                        {
                            return Ok(false);
                        }
                        if staged == expected_identity {
                            identity = Some(sys.path());
                        }
                    }
                    other
                        if is_atomic_tmp_for(other, ".shards_ready.")
                            || is_atomic_tmp_for(other, "format_version.") =>
                    {
                        let staged = fs::read(sys.path())?;
                        if staged.len() > 2 || !b"18".starts_with(&staged) {
                            return Ok(false);
                        }
                    }
                    _ => return Ok(false),
                }
            }
        } else if name == "mmdb" {
            for shard in fs::read_dir(entry.path())? {
                let shard = shard?;
                if !shard.file_type()?.is_dir() {
                    return Ok(false);
                }
                let raw = shard.file_name();
                let Some(sname) = raw.to_str() else {
                    return Ok(false);
                };
                if !(0..shards).any(|i| sname == format!("shard_{i:02}")) {
                    return Ok(false);
                }
                for file in fs::read_dir(shard.path())? {
                    if !is_empty_shard_file(&file?)? {
                        return Ok(false);
                    }
                }
            }
        } else {
            return Ok(false);
        }
    }
    if identity.is_none() {
        match super::universe::validate_pending_root(root, universe_id, ns, shards) {
            Ok(()) => {}
            Err(VsdbError::Corrupt { .. }) => return Ok(false),
            Err(err) => return Err(err),
        }
    }
    let engine = root.join("mmdb");
    if engine.try_exists()? {
        fs::remove_dir_all(engine)?;
    }
    let root_dir = fs::File::open(root)?;
    root_dir.sync_all()?;
    let system = root.join("__SYSTEM__");
    if system.try_exists()? {
        for entry in fs::read_dir(&system)? {
            let path = entry?.path();
            if identity.as_ref() != Some(&path) {
                fs::remove_file(path)?;
            }
        }
        if let Some(identity) = identity {
            fs::File::open(&system)?.sync_all()?;
            fs::remove_file(identity)?;
        }
        fs::remove_dir(system)?;
    }
    if preexisted {
        root_dir.sync_all()?;
    } else {
        fs::remove_dir(root)?;
        fs::File::open(root.parent().expect("namespace root has a parent"))?
            .sync_all()?;
    }
    Ok(true)
}

/// Reclaims a non-empty explicit root left behind when an earlier failed
/// creation rolled back its registry entry, after `load_registry` has durably
/// synchronized `__SYSTEM__` and confirmed the matching pending-creation
/// record's burnt ID is absent and the root contains no user data or sidecars.
fn reclaim_unregistered_owned_root(
    base: &Path,
    root: &Path,
    reg: &RegistryFile,
) -> Result<()> {
    let universe_id = super::universe::active()?.id;
    let is_unregistered_burnt = |id: NsId| {
        id != DEFAULT_NS_ID && id < reg.next_id && reg.entries.iter().all(|r| r.id != id)
    };
    let target = normalize_physical(root);
    let pending = read_pending_creates(base)?;
    for matched in pending.into_iter().filter(|e| {
        e.universe == universe_id
            && (1..=64).contains(&(e.shards as usize))
            && is_unregistered_burnt(e.id)
            && normalize_physical(Path::new(&e.path)) == target
    }) {
        let cleaned = match super::universe::lock_root_dir(root) {
            Ok(_guard) => cleanup_failed_root(
                root,
                true,
                universe_id,
                matched.id,
                matched.shards as usize,
            )?,
            Err(VsdbError::Io(e)) if e.kind() == io::ErrorKind::NotFound => {
                durably_empty_root(root)?
            }
            Err(err) => return Err(err),
        };
        if cleaned {
            clear_pending_create(base, matched.id)?;
        }
    }
    Ok(())
}

fn sizing_for(mem_budget_mb: Option<usize>) -> EngineSizing {
    EngineSizing::from_budget_mb(mem_budget_mb.unwrap_or(DEFAULT_NS_BUDGET_MB))
}

/////////////////////////////////////////////////////////////////////////////

struct NsInner {
    id: NsId,
    path: PathBuf,
    /// The engine, owned: when the last `Arc<NsInner>` drops (only ever
    /// via [`Namespace::close_by_id`], which proves exclusivity first), the
    /// engine drops with it — flushing writable state, joining any
    /// compaction threads, and releasing LOCK files.
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
    /// The implicit default namespace (the global engine), independent of the
    /// non-default namespace registry.
    ///
    /// # Panics
    ///
    /// Panics if the default engine cannot open its configured dataset. In
    /// read-only mode the dataset must already be complete and supported.
    pub fn default_ns() -> Namespace {
        Self::try_default_ns().expect("vsdb: open default namespace")
    }

    fn try_default_ns() -> Result<Namespace> {
        if let Some(ns) = DEFAULT_NS.get() {
            return Ok(ns.clone());
        }
        let _guard = DEFAULT_INIT.lock();
        if let Some(ns) = DEFAULT_NS.get() {
            return Ok(ns.clone());
        }
        vsdb_freeze_base_dir();
        super::universe::active()?;
        let ns = Namespace(Arc::new(NsInner {
            id: DEFAULT_NS_ID,
            path: vsdb_get_base_dir(),
            engine: Engine::new()?,
        }));
        Ok(DEFAULT_NS.get_or_init(|| ns).clone())
    }

    /// Stable identity shared by all namespaces in this universe.
    pub fn universe_id(&self) -> u128 {
        super::universe::active()
            .expect("open namespace has an active universe")
            .id
    }

    /// Whether this namespace was opened without write capability.
    ///
    /// Open mode is process-wide, so every namespace in one process reports
    /// the same value.
    #[inline(always)]
    pub fn is_read_only(&self) -> bool {
        self.0.engine.is_read_only()
    }

    #[doc(hidden)]
    #[inline(always)]
    pub fn mem_budget_bytes(&self) -> usize {
        self.0.engine.mem_budget()
    }

    /// Starts a NEW placement group: a fresh `NsId` on every call, root
    /// derived from the id — collision-free (ids are never reused).
    /// Zero parameters; tuning lives in [`Self::create_with`].
    ///
    /// # Errors
    ///
    /// Returns [`VsdbError::ReadOnly`] in read-only mode.
    pub fn create() -> Result<Namespace> {
        Self::create_with(NamespaceOpts::default())
    }

    /// [`Self::create`] with explicit options (volume placement, shard
    /// count, memory budget).
    ///
    /// # Errors
    ///
    /// Returns [`VsdbError::ReadOnly`] in read-only mode, or a storage/
    /// namespace error if the requested placement cannot be created.
    pub fn create_with(opts: NamespaceOpts) -> Result<Namespace> {
        if vsdb_is_read_only() {
            return Err(VsdbError::ReadOnly {
                operation: "namespace creation",
            });
        }
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
            reclaim_unregistered_owned_root(&base, p, &reg)?;
            root_preexisted = ensure_root_adoptable(p)?;
        }

        let id = reg.next_id;
        let rec = NsRecord {
            state: NsLifecycleState::Pending,
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

        let derived_preexisted = ensure_root_adoptable(&derived_root(&base, id))?;
        if opts.path.is_none() {
            root_preexisted = derived_preexisted;
        }

        // The first registry commit reserves the ID and publishes its pending root.
        reg.next_id = reg
            .next_id
            .checked_add(1)
            .ok_or_else(|| ns_err("namespace id space exhausted"))?;
        reg.entries.push(rec.clone());
        if let Some(path) = &rec.path {
            record_pending_create(
                &base,
                &reg,
                PendingCreate {
                    universe: super::universe::active()?.id,
                    id,
                    shards: shards as u32,
                    path: path.clone(),
                },
            )?;
        }
        if let Err(err) = save_registry(&reg) {
            reg.entries.retain(|r| r.id != rec.id);
            if save_registry(&reg).is_ok() && rec.path.is_some() {
                let _ = clear_pending_create(&base, id);
            }
            return Err(err);
        }
        #[cfg(test)]
        tests::interrupt_creation_after_pending(id);

        match open_record_locked(&base, &rec, &root) {
            Ok(ns) => {
                if rec.path.is_some() {
                    let _ = clear_pending_create(&base, id);
                }
                Ok(ns)
            }
            Err(e) => {
                // Non-crash open failure (bad path, disk full, …): roll
                // the entry back so a failed `create` leaves no registry
                // residue. `next_id` deliberately stays advanced — ids
                // are never reused, and a burnt id is free. If the
                // rollback write itself fails we are simply in the
                // crash-equivalent state documented above: the entry is
                // visible in `Namespace::list()`, re-openable via `open`
                // and reclaimable via `destroy`.
                reg.entries.retain(|r| r.id != rec.id);
                // Delete the root only after the rollback registry write AND its
                // parent-directory fsync succeed. If that fence fails, keep the
                // root intact until a later registry load completes the fence.
                if save_registry(&reg).is_ok() {
                    match super::universe::lock_root_dir(&root) {
                        Ok(_guard) => {
                            if cleanup_failed_root(
                                &root,
                                root_preexisted,
                                super::universe::active()?.id,
                                id,
                                shards,
                            )
                            .unwrap_or(false)
                                && rec.path.is_some()
                            {
                                let _ = clear_pending_create(&base, id);
                            }
                        }
                        Err(VsdbError::Io(io_err))
                            if io_err.kind() == io::ErrorKind::NotFound =>
                        {
                            if rec.path.is_some()
                                && durably_empty_root(&root).unwrap_or(false)
                            {
                                let _ = clear_pending_create(&base, id);
                            }
                        }
                        Err(_) => {}
                    }
                }
                Err(e)
            }
        }
    }

    /// Opens an already-registered namespace by its stable id — the
    /// admin tier ([`Namespace::list`], epoch-rotation bookkeeping).
    /// Normal flows never call this: deserialization and `from_meta`
    /// auto-open namespaces via the ids embedded in metas.
    ///
    /// `open(DEFAULT_NS_ID)` short-circuits to [`Self::default_ns`]
    /// without touching the registry. Idempotent in-process. In read-only
    /// mode this validates and opens the existing namespace without writing
    /// lifecycle state or format metadata; pending/incomplete roots fail.
    pub fn open(id: NsId) -> Result<Namespace> {
        if id == DEFAULT_NS_ID {
            return Self::try_default_ns();
        }
        if let Some(ns) = OPEN_NAMESPACES.lock().get(&id) {
            return Ok(ns.clone());
        }
        // Reading the registry materializes a base-dir-derived path:
        // freeze the base dir (same contract as every other derived
        // path) so a later `vsdb_configure` fails loudly instead of
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
    /// the single source of truth for instance-meta naming (the default
    /// namespace's root is the base dir).
    pub fn meta_path(&self, map_id: u64) -> PathBuf {
        let mut p = self.meta_dir();
        p.push(format!("{:016x}", map_id));
        p
    }

    /// Flushes this namespace's engine to disk. This is a no-op for a
    /// read-only namespace.
    ///
    /// # Panics
    ///
    /// Panics if a shard fails to flush, for example on an I/O error or
    /// after the engine has stopped on an earlier write or background error.
    /// Later shards are then left unflushed. For a durability fence that
    /// returns engine errors, use [`Namespace::try_sync_wal`].
    pub fn flush(&self) {
        if !self.is_read_only() {
            self.0.engine.flush()
        }
    }

    /// Makes prior successful writes to every shard in this namespace durable
    /// without forcing memtable flushes. A read-only namespace is a no-op.
    ///
    /// Returns the first engine synchronization error. Earlier shards may
    /// already be synchronized when a later shard fails; this is a durability
    /// fence, not a transaction across collections or shards. Other namespaces
    /// are unaffected.
    pub fn try_sync_wal(&self) -> Result<()> {
        self.0.engine.try_sync_all_wals()
    }

    /// One engine-property reading per shard, in shard order — the
    /// observability tier for capacity planning and cache telemetry.
    ///
    /// Property names are the storage engine's (mmdb `DB::get_property`)
    /// names; unknown names yield `None` per shard. The ones that
    /// matter for cache telemetry:
    ///
    /// - `"stats.block_cache_hits"` / `"stats.block_cache_misses"` —
    ///   per-shard counters (counted at each shard's read site, so
    ///   per-shard hit rates stay meaningful under the shared
    ///   per-engine cache pool);
    /// - `"stats.cache_hit_rate"` — per-shard rate in `[0, 1]`;
    /// - `"block-cache-usage"` — approximate entry count of the
    ///   engine's cache **pool** plus that shard's own pinned entries
    ///   (all shards of one engine share one pool, so the dominant term
    ///   is the same engine-wide total in every shard's reading).
    ///
    /// ```ignore
    /// let ns = Namespace::default_ns();
    /// let hits = ns.shard_properties("stats.block_cache_hits");
    /// let misses = ns.shard_properties("stats.block_cache_misses");
    /// // hits.len() == misses.len() == the engine's shard count
    /// ```
    pub fn shard_properties(&self, name: &str) -> Vec<Option<String>> {
        self.0.engine.shard_properties(name)
    }

    /// Samples the shared pool once and the current memtables and L0 pins on each
    /// shard. Sizes are estimates. Configured thresholds can be exceeded by a batch.
    pub fn memory_usage(&self) -> NamespaceMemoryUsage {
        self.0.engine.memory_usage()
    }

    /// The engine backing this namespace (crate-internal routing).
    ///
    /// A plain borrow of the `Arc`-owned engine: it cannot outlive the
    /// handle it came from, so no reference can survive a
    /// [`Namespace::close_by_id`] (which requires every handle gone first).
    #[inline(always)]
    pub(crate) fn engine(&self) -> &Engine {
        &self.0.engine
    }

    /// Consuming form of [`Namespace::close_by_id`]: closes this namespace,
    /// releasing engine-owned resources (see `Namespace::close_by_id` for the
    /// full contract — flush-first teardown, registry untouched,
    /// re-openable afterwards).
    ///
    /// `self` must be the last owning handle. Drop collection handles,
    /// readers and other `Namespace` clones first. Detached snapshot
    /// iterators can outlive the close and retain their sources.
    /// The consumed `self` itself is accounted for — unlike
    /// `Namespace::close_by_id(id)`, no separate `drop(ns)` is needed first.
    ///
    /// # Errors
    ///
    /// - `Err((Some(handle), e))` — the close was **refused** (other
    ///   live handles, or the default namespace): nothing happened, and
    ///   the consumed handle is returned for continued use.
    /// - `Err((None, e))` — the close **ran** but the engine teardown
    ///   reported an error while flushing/syncing: the namespace is no
    ///   longer open, so there is no handle to give back. In contrast, a
    ///   precondition error from `close_by_id` performs no engine teardown.
    ///
    /// ```ignore
    /// match ns.close() {
    ///     Ok(()) => {}
    ///     Err((Some(ns), e)) => { /* refused — `ns` is still usable */ }
    ///     Err((None, e)) => { /* closed, but teardown reported `e` */ }
    /// }
    /// ```
    pub fn close(self) -> std::result::Result<(), (Option<Namespace>, VsdbError)> {
        ns_close_impl(self.0.id, Some(self))
    }
}

/// The record's shard count, bounds-checked. Registry entries are
/// written pre-clamped, so an out-of-range count means corruption or
/// hand-editing — refuse cleanly: `shards == 0` would otherwise reach
/// `prefix % 0` (a release-mode panic) on the first routed operation,
/// and would vacuously pass completed-dataset per-shard checks.
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
fn open_record_locked(base: &Path, rec: &NsRecord, root: &Path) -> Result<Namespace> {
    let shards = validated_shards(rec)?;
    let universe_id = super::universe::active()?.id;
    match rec.state {
        NsLifecycleState::Established => {
            super::universe::verify_engine(root, universe_id, rec.id, shards)?
        }
        NsLifecycleState::Pending if vsdb_is_read_only() => {
            return Err(ns_err("pending namespace cannot open read-only"));
        }
        NsLifecycleState::Pending => {}
    }
    let sizing = sizing_for(rec.mem_budget_mb.map(|mb| mb as usize));
    let engine = Engine::open_at(root, shards, sizing, rec.id)?;
    if rec.state == NsLifecycleState::Pending {
        let mut reg = read_registry(base)?;
        reg.entries
            .iter_mut()
            .find(|r| r.id == rec.id)
            .expect("record under registry lock")
            .state = NsLifecycleState::Established;
        save_registry(&reg)?;
    }
    let ns = Namespace(Arc::new(NsInner {
        id: rec.id,
        path: root.to_path_buf(),
        engine,
    }));
    OPEN_NAMESPACES.lock().insert(rec.id, ns.clone());
    Ok(ns)
}

/////////////////////////////////////////////////////////////////////////////

/// Registry administration.
impl Namespace {
    /// Lists every registered (non-default) namespace.
    pub fn list() -> Result<Vec<NsInfo>> {
        // Reading the registry materializes base-dir-derived paths — same
        // freeze contract as open/destroy/relocate, so the returned roots
        // cannot be split from the universe by a later `vsdb_configure`.
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
    /// whole directory tree without per-key traversal. Filesystem work scales
    /// with the number of files and directories. A root that cannot itself be
    /// removed, such as a mount point adopted by [`Namespace::create_with`],
    /// is emptied and kept.
    ///
    /// The target must not be open in this process ([`Namespace::close_by_id`] it
    /// first). A crash between the registry update and the tree removal
    /// leaves an orphaned-but-harmless dir.
    pub fn destroy(id: NsId) -> Result<()> {
        if vsdb_is_read_only() {
            return Err(VsdbError::ReadOnly {
                operation: "namespace destruction",
            });
        }
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
        let rec = &reg.entries[pos];
        let link_meta = match fs::symlink_metadata(&root) {
            Ok(meta) => Some(meta),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        let target = if root.try_exists()? {
            Some(root.canonicalize()?)
        } else {
            None
        };
        let root_lease = if let Some(target) = &target {
            let lease = super::universe::lock_root_dir(target)?;
            let universe = super::universe::active()?.id;
            match rec.state {
                NsLifecycleState::Established => super::universe::check_identity(
                    target,
                    universe,
                    id,
                    rec.shards as usize,
                )?,
                NsLifecycleState::Pending => super::universe::validate_pending_root(
                    target,
                    universe,
                    id,
                    rec.shards as usize,
                )?,
            }
            Some(lease)
        } else {
            None
        };
        clear_pending_create(&base, id)?;
        reg.entries.remove(pos);
        save_registry(&reg)?;
        if link_meta.is_some_and(|m| m.file_type().is_symlink()) {
            match fs::remove_file(&root) {
                Ok(()) => sync_parent(&root)?,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        let Some(target) = target else {
            return Ok(());
        };
        let _keep_lease = root_lease;
        #[cfg(test)]
        tests::before_root_wipe(&target);
        let entries = match fs::read_dir(&target) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                sync_parent(&target)?;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let entry = entry?;
            let removed = match entry.file_type() {
                Ok(kind) if kind.is_dir() => fs::remove_dir_all(entry.path()),
                Ok(_) => fs::remove_file(entry.path()),
                Err(e) => Err(e),
            };
            match removed {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        match fs::remove_dir(&target) {
            Ok(()) => {
                sync_parent(&target)?;
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                sync_parent(&target)?;
                Ok(())
            }
            // An adopted root, such as a mount point or a directory under a
            // parent the process cannot modify, stays behind empty.
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::PermissionDenied | io::ErrorKind::ResourceBusy
                ) =>
            {
                fs::File::open(&target)?.sync_all()?;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Re-points a namespace at a new root directory (e.g. after moving a
    /// volume). Updates the registry only — **moving the data is the
    /// operator's job**, done before calling this. A target that does not
    /// already hold an initialized dataset (format marker + per-shard
    /// engine anchors) is refused: repointing at it would durably orphan
    /// the real data. The engine header must match the universe and namespace.
    ///
    /// The target must not be open in this process.
    pub fn relocate(id: NsId, new_path: impl AsRef<Path>) -> Result<()> {
        if vsdb_is_read_only() {
            return Err(VsdbError::ReadOnly {
                operation: "namespace relocation",
            });
        }
        if id == DEFAULT_NS_ID {
            return Err(ns_err(
                "the default namespace's root is the base dir; relocate it \
                 via VSDB_BASE_DIR / vsdb_configure before first use",
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

        super::universe::verify_engine(
            new_path,
            super::universe::active()?.id,
            id,
            rec_shards,
        )?;
        super::universe::check_format_marker(new_path, false)?;
        let rec = reg
            .entries
            .iter_mut()
            .find(|r| r.id == id)
            .expect("checked above");
        rec.state = NsLifecycleState::Established;
        rec.path = Some(
            new_path
                .to_str()
                .expect("validated UTF-8 in validate_explicit_root")
                .to_owned(),
        );
        save_registry(&reg)
    }

    /// Closes an open namespace, releasing engine-owned memory, workers,
    /// file handles, and mmdb locks. Detached snapshots can retain their
    /// own source resources, as described below. The active
    /// memtables are flushed and the WALs synced first (errors surface
    /// here, unlike a plain drop).
    ///
    /// Drop every owning collection handle, reader and `Namespace`
    /// clone first. A refusal reports the number of remaining owners.
    /// The default namespace stays open for the life of the process.
    ///
    /// The registry entry is untouched: a closed namespace can be re-opened
    /// via [`Namespace::open`] (restart-equivalent recovery) or reclaimed
    /// via [`Namespace::destroy`] — `create → fill → close → destroy` is the
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
    pub fn close_by_id(id: NsId) -> Result<()> {
        // No handle is consumed, so the refusal side never carries one.
        ns_close_impl(id, None).map_err(|(_, e)| e)
    }
}

/// The close protocol shared by [`Namespace::close_by_id`] and
/// [`Namespace::close`]: prove exclusivity under [`REGISTRY_LOCK`] +
/// the table lock, remove the table entry, then tear the engine down.
///
/// `caller_handle` is the handle a consuming caller accounts for
/// (`None` for the free function).  Once exclusivity is proven it is
/// dropped **under the table lock** — that 2→1 atomic decrement is the
/// accounting step itself, entitling the removed entry to
/// `Arc::try_unwrap` as the sole strong ref.  The (possibly slow)
/// engine teardown runs after the table lock is released, so unrelated
/// namespaces' cache hits never block on it; `REGISTRY_LOCK` keeps
/// serializing open/create/destroy of THIS id until the teardown
/// finished.
///
/// On refusal the consumed handle (if any) is returned intact; past
/// the point of no return the error side is always `(None, e)`.
fn ns_close_impl(
    id: NsId,
    caller_handle: Option<Namespace>,
) -> std::result::Result<(), (Option<Namespace>, VsdbError)> {
    if id == DEFAULT_NS_ID {
        return Err((
            caller_handle,
            ns_err("the default namespace cannot be closed"),
        ));
    }
    let _g = REGISTRY_LOCK.lock();
    let ns_owned = {
        let mut open = OPEN_NAMESPACES.lock();
        let Some(entry) = open.get(&id) else {
            // For a consuming caller this arm is unreachable through
            // safe use (a live handle pins its table entry: removal
            // proves exclusivity first) — kept as a defensive error
            // path rather than a panic.
            return Err((
                caller_handle,
                ns_err(format!("namespace {id} is not open in this process")),
            ));
        };
        // A consumed handle is always a clone of the current table
        // entry: the entry can only be replaced by a successful close,
        // which proves no external handle existed.
        if let Some(h) = &caller_handle {
            debug_assert!(Arc::ptr_eq(&entry.0, &h.0));
        }
        // Strong refs accounted here: the table's entry, plus the
        // consumed handle if any.  Stable while both locks are held:
        // cloning requires an existing `Namespace`, and the other
        // cloning paths — `open`'s cache hit and `flush_all_open` —
        // block on the table lock.
        let accounted = 1 + usize::from(caller_handle.is_some());
        let others = Arc::strong_count(&entry.0) - accounted;
        if others > 0 {
            let qualifier = if caller_handle.is_some() {
                "other "
            } else {
                ""
            };
            return Err((
                caller_handle,
                ns_err(format!(
                    "namespace {id} still has {others} {qualifier}live handle(s); \
                     drop every collection handle and `Namespace` clone first"
                )),
            ));
        }
        // Provably exclusive; release the consumed handle's ref and
        // take sole ownership through the table's entry.
        drop(caller_handle);
        open.remove(&id).expect("present: checked above")
        // The table lock is released here — the (possibly slow) engine
        // teardown below must not block unrelated namespaces' cache
        // hits.
    };
    let inner = Arc::try_unwrap(ns_owned.0)
        .unwrap_or_else(|_| unreachable!("count was 1 under both locks"));
    inner.engine.close().map_err(|e| (None, e))
}

impl RegistryFile {
    pub(super) fn save(&self, base: &Path) -> Result<()> {
        let mut bytes = REGISTRY_MAGIC.to_vec();
        postcard::to_io(self, &mut bytes)?;
        REGISTRY_DIR_UNSYNCED.store(true, Ordering::Release);
        atomic_write_file(&base.join(NS_REGISTRY_REL_PATH), &bytes)?;
        REGISTRY_DIR_UNSYNCED.store(false, Ordering::Release);
        Ok(())
    }
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
    if let Some(ns) = DEFAULT_NS.get() {
        ns.flush();
    }
    let handles: Vec<Namespace> = OPEN_NAMESPACES.lock().values().cloned().collect();
    for ns in handles {
        ns.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, panic::catch_unwind};

    thread_local! {
        static OCCUPY_PENDING_ROOT: RefCell<Option<PathBuf>> = const {RefCell::new(None)};
        static INTERRUPT_AFTER_PENDING: Cell<bool> = const { Cell::new(false) };
        static INTERRUPTED_ID: Cell<Option<NsId>> = const { Cell::new(None) };
        static REMOVE_ROOT_BEFORE_WIPE: Cell<bool> = const { Cell::new(false) };
    }

    pub(super) fn before_root_wipe(root: &Path) {
        if REMOVE_ROOT_BEFORE_WIPE.with(|flag| flag.replace(false)) {
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn destroy_succeeds_when_the_root_disappears_after_unregistering() {
        let ns = Namespace::create().unwrap();
        let id = ns.id();
        let root = ns.path().to_path_buf();
        ns.close().unwrap();
        // Another actor removes the root after the registry update.
        REMOVE_ROOT_BEFORE_WIPE.with(|flag| flag.set(true));
        Namespace::destroy(id).unwrap();
        assert!(!Namespace::list().unwrap().iter().any(|info| info.id == id));
        assert!(!root.exists());
    }

    pub(super) fn interrupt_creation_after_pending(id: NsId) {
        OCCUPY_PENDING_ROOT.with(|path| {
            if let Some(path) = path.take() {
                fs::write(path.join("unrelated"), b"preserved").unwrap();
            }
        });
        if INTERRUPT_AFTER_PENDING.with(|flag| flag.replace(false)) {
            INTERRUPTED_ID.with(|slot| slot.set(Some(id)));
            panic!("interrupted namespace creation after pending lifecycle");
        }
    }

    #[test]
    fn pending_creation_interruption_does_not_block_later_creation() {
        INTERRUPT_AFTER_PENDING.with(|flag| flag.set(true));
        assert!(catch_unwind(Namespace::create).is_err());
        let interrupted = INTERRUPTED_ID.with(|slot| slot.take()).unwrap();
        Namespace::open(interrupted).unwrap().close().unwrap();
        let ns =
            Namespace::create().expect("an interrupted reservation must be skipped");
        assert!(ns.id() > interrupted);
        let id = ns.id();
        ns.close().unwrap();
        Namespace::destroy(id).unwrap();
        Namespace::destroy(interrupted).unwrap();
    }

    #[test]
    fn pending_root_never_adopts_or_deletes_another_owners_files() {
        let root = std::env::temp_dir()
            .join(format!("vsdb-pending-owner-{}", rand::random::<u128>()));
        fs::create_dir(&root).unwrap();
        let opts = NamespaceOpts {
            path: Some(root.clone()),
            shards: 1,
            ..Default::default()
        };
        // An unrelated owner populates a previously empty root after the
        // registry reservation. Failed-create cleanup must preserve its data.
        OCCUPY_PENDING_ROOT.with(|path| path.replace(Some(root.clone())));
        assert!(Namespace::create_with(opts.clone()).is_err());
        assert_eq!(fs::read(root.join("unrelated")).unwrap(), b"preserved");
        fs::remove_file(root.join("unrelated")).unwrap();
        // The same ownership check applies to open and destroy after restart.
        INTERRUPT_AFTER_PENDING.with(|flag| flag.set(true));
        assert!(catch_unwind(|| Namespace::create_with(opts)).is_err());
        let id = INTERRUPTED_ID.with(|slot| slot.take().unwrap());
        fs::write(root.join("unrelated"), b"preserved").unwrap();
        assert!(Namespace::open(id).is_err());
        assert!(Namespace::destroy(id).is_err());
        assert_eq!(fs::read(root.join("unrelated")).unwrap(), b"preserved");
        fs::remove_file(root.join("unrelated")).unwrap();
        let system = root.join("__SYSTEM__");
        fs::create_dir(&system).unwrap();
        fs::write(
            system.join("__initializing__"),
            super::super::universe::engine_bytes(0, id, 1),
        )
        .unwrap();
        assert!(Namespace::open(id).is_err());
        assert!(Namespace::destroy(id).is_err());
        fs::remove_file(system.join("__initializing__")).unwrap();
        let foreign_tmp = system.join("__initializing__.4242.0.tmp");
        fs::write(&foreign_tmp, super::super::universe::engine_bytes(0, id, 1)).unwrap();
        assert!(Namespace::open(id).is_err());
        assert!(Namespace::destroy(id).is_err());
        assert!(foreign_tmp.is_file());
        fs::remove_file(&foreign_tmp).unwrap();
        Namespace::destroy(id).unwrap();
        assert!(!root.exists());

        // Also check `reclaim_unregistered_owned_root` against a foreign `.tmp`
        // left behind after a rolled-back explicit creation.
        fs::create_dir_all(&system).unwrap();
        let burnt_id = {
            let _g = REGISTRY_LOCK.lock();
            let reg = load_registry().unwrap();
            let uid = super::super::universe::active().unwrap().id;
            let burnt = id;
            save_pending_creates(
                &vsdb_get_base_dir(),
                &[PendingCreate {
                    universe: uid,
                    id: burnt,
                    shards: 1,
                    path: root.to_str().unwrap().to_owned(),
                }],
            )
            .unwrap();
            assert!(!reg.entries.iter().any(|r| r.id == burnt));
            burnt
        };
        fs::write(
            &foreign_tmp,
            super::super::universe::engine_bytes(0, burnt_id, 1),
        )
        .unwrap();
        assert!(
            Namespace::create_with(NamespaceOpts {
                path: Some(root.clone()),
                shards: 1,
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            foreign_tmp.is_file(),
            "foreign staging .tmp was wiped by reclaim_unregistered_owned_root"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn normalize_physical_resolves_unresolvable_parent_dirs() {
        let scratch = std::env::temp_dir().join(format!(
            "vsdb_norm_{}_{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir_all(&scratch).unwrap();

        let via_parent = scratch.join("missing").join("..").join("target");
        let direct = scratch.join("target");
        let normalized = normalize_physical(&via_parent);
        assert_eq!(normalized, normalize_physical(&direct));
        assert!(path_contains(
            &normalized,
            &normalize_physical(&scratch.join("target").join("child"))
        ));

        fs::remove_dir_all(&scratch).unwrap();
    }

    fn record(id: NsId, shards: u32) -> NsRecord {
        NsRecord {
            state: NsLifecycleState::Established,
            id,
            path: None,
            shards,
            mem_budget_mb: None,
            created_at: 0,
        }
    }

    #[test]
    fn validate_registry_rejects_bad_allocators_and_entries() {
        let mut reg = RegistryFile::default();
        assert!(validate_registry(&reg).is_ok());

        reg.next_id = 0;
        assert!(validate_registry(&reg).is_err());

        reg = RegistryFile::default();
        reg.entries.push(record(1, 4));
        assert!(validate_registry(&reg).is_err());
        reg.next_id = 2;
        assert!(validate_registry(&reg).is_ok());

        reg.entries.push(record(1, 4));
        assert!(validate_registry(&reg).is_err());
        reg.entries.pop();

        reg.entries[0] = record(0, 4);
        assert!(validate_registry(&reg).is_err());
        reg.entries[0] = record(1, 0);
        assert!(validate_registry(&reg).is_err());
        reg.entries[0] = record(1, 65);
        assert!(validate_registry(&reg).is_err());
    }
}
