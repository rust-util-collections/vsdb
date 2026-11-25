//!
//! Persistent B+ Tree with copy-on-write structural sharing.
//!
//! A [`NodeId`] identifies a root in this tree's [`MapxRaw`] node pool.
//! A point update copies the modified path and shares unaffected subtrees.
//! Non-root pages hold 16 to 32 keys. Leaf values larger than 1 KiB use
//! separate blob rows.
//!
//! [`crate::VerMap`] records the branch and commit roots it owns.
//! With a standalone tree the caller retains the root IDs. Pass every live
//! root to [`PersistentBTree::rebuild_ref_counts`] after reopening a
//! standalone store or to [`PersistentBTree::gc`] for a full sweep. An
//! omitted root can leave its pages eligible for reclamation.

#![forbid(unsafe_code)]

#[cfg(test)]
mod test;

mod diff;
mod insert;
mod iter;
mod merge;
mod nodes;
mod persist;
mod remove;
mod types;

pub use diff::{BTreeDiffIter, TreeDiff};
pub use iter::BTreeIter;

use crate::{
    common::{
        InstanceId,
        error::{Result, VsdbError},
    },
    versioned::{
        BranchId,
        store::{BranchHead, HEAD, RETIRED, row},
    },
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering as KeyOrdering,
    collections::{HashMap, HashSet},
    ops::{Bound, Deref},
    result::Result as StdResult,
    sync::{
        Arc, LazyLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
};
use vsdb_core::basic::mapx_raw::{MapxRaw, MapxRawReader};

pub(crate) use nodes::{
    BLOB, BLOB_LEAF, CLASS_MASK, LEAF, LeafVal, MAX_KEYS, MIN_KEYS, Node, NodeView,
    object_id, object_key,
};

pub use types::{EMPTY_ROOT, NodeId};
const STANDALONE_ALLOCATOR: [u8; 1] = [9];
pub(crate) use types::{InsertResult, NodeRef, RemoveResult};

// =========================================================================
// PersistentBTree
// =========================================================================

#[cfg(test)]
thread_local! {
    /// Node decodes performed by this thread (test instrumentation).
    pub(crate) static NODE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// InternalPageCache hits observed by this thread (test instrumentation).
    pub(crate) static PAGE_CACHE_HITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

const PAGE_CACHE_SHARDS: usize = 8;
const MIN_PAGE_CACHE_BYTES: usize = 64 << 10;
const MAX_PAGE_CACHE_BYTES: usize = 16 << 20;

#[derive(Debug)]
struct CacheEntry {
    id: NodeId,
    bytes: Arc<[u8]>,
    referenced: bool,
}

#[derive(Debug)]
struct CacheShard {
    index: HashMap<NodeId, usize>,
    ring: Vec<CacheEntry>,
    hand: usize,
    used_bytes: usize,
    cap_bytes: usize,
}

impl CacheShard {
    fn new(cap_bytes: usize) -> Self {
        Self {
            index: HashMap::new(),
            ring: Vec::new(),
            hand: 0,
            used_bytes: 0,
            cap_bytes,
        }
    }

    fn weight(bytes: &[u8]) -> usize {
        bytes.len().saturating_add(48)
    }

    fn get(&mut self, id: NodeId) -> Option<Arc<[u8]>> {
        let &slot = self.index.get(&id)?;
        self.ring[slot].referenced = true;
        #[cfg(test)]
        PAGE_CACHE_HITS.with(|c| c.set(c.get() + 1));
        Some(Arc::clone(&self.ring[slot].bytes))
    }

    fn insert(&mut self, id: NodeId, bytes: Arc<[u8]>) {
        let w = Self::weight(&bytes);
        if w > self.cap_bytes || self.index.contains_key(&id) {
            return;
        }
        while self.used_bytes + w > self.cap_bytes && !self.ring.is_empty() {
            if self.hand >= self.ring.len() {
                self.hand = 0;
            }
            if self.ring[self.hand].referenced {
                self.ring[self.hand].referenced = false;
                self.hand += 1;
            } else {
                let evicted = self.ring.swap_remove(self.hand);
                self.index.remove(&evicted.id);
                self.used_bytes =
                    self.used_bytes.saturating_sub(Self::weight(&evicted.bytes));
                if self.hand < self.ring.len() {
                    let moved_id = self.ring[self.hand].id;
                    self.index.insert(moved_id, self.hand);
                }
            }
        }
        let slot = self.ring.len();
        self.ring.push(CacheEntry {
            id,
            bytes,
            referenced: true,
        });
        self.index.insert(id, slot);
        self.used_bytes += w;
    }

    fn remove(&mut self, id: NodeId) {
        if let Some(slot) = self.index.remove(&id) {
            let evicted = self.ring.swap_remove(slot);
            self.used_bytes =
                self.used_bytes.saturating_sub(Self::weight(&evicted.bytes));
            if slot < self.ring.len() {
                let moved_id = self.ring[slot].id;
                self.index.insert(moved_id, slot);
            }
            if self.hand > self.ring.len() {
                self.hand = 0;
            }
        }
    }
}

/// Byte-bounded 8-way lock-sharded clock cache of committed internal B+ tree
/// pages (`id & CLASS_MASK == 0`). Internal pages are immutable and `NodeId`
/// serials are strictly monotonic within a [`TreeRuntime`].
#[derive(Debug)]
pub(crate) struct InternalPageCache {
    shards: [Mutex<CacheShard>; PAGE_CACHE_SHARDS],
}

impl InternalPageCache {
    pub(crate) fn from_namespace_budget(ns_budget_bytes: usize) -> Self {
        let total =
            (ns_budget_bytes / 64).clamp(MIN_PAGE_CACHE_BYTES, MAX_PAGE_CACHE_BYTES);
        Self::with_budget_bytes(total)
    }

    pub(crate) fn with_budget_bytes(total_bytes: usize) -> Self {
        let per_shard = (total_bytes / PAGE_CACHE_SHARDS).max(1024);
        Self {
            shards: std::array::from_fn(|_| Mutex::new(CacheShard::new(per_shard))),
        }
    }

    #[inline]
    fn shard(&self, id: NodeId) -> &Mutex<CacheShard> {
        let idx = (id.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 61) as usize;
        &self.shards[idx]
    }

    pub(crate) fn get(&self, id: NodeId) -> Option<Arc<[u8]>> {
        self.shard(id).lock().get(id)
    }

    pub(crate) fn insert(&self, id: NodeId, bytes: Arc<[u8]>) {
        self.shard(id).lock().insert(id, bytes);
    }

    pub(crate) fn remove(&self, id: NodeId) {
        self.shard(id).lock().remove(id);
    }

    #[cfg(test)]
    pub(crate) fn used_bytes(&self) -> usize {
        self.shards.iter().map(|s| s.lock().used_bytes).sum()
    }
}

pub(crate) enum PageBytes<'a> {
    Borrowed(&'a [u8]),
    Shared(Arc<[u8]>),
    Owned(Vec<u8>),
}

impl Deref for PageBytes<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Self::Borrowed(b) => b,
            Self::Shared(b) => b,
            Self::Owned(b) => b,
        }
    }
}

impl AsRef<[u8]> for PageBytes<'_> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl PageBytes<'_> {
    #[inline]
    fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Borrowed(b) => b.to_vec(),
            Self::Shared(b) => b.to_vec(),
            Self::Owned(b) => b,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RefState {
    counts: HashMap<NodeId, u32>,
    edges: HashMap<NodeId, Box<[NodeId]>>,
    zombies: HashSet<NodeId>,
    ready: bool,
    /// One reference per live snapshot lease, shared by its iterators.
    pins: HashMap<NodeId, u32>,
    /// Live branch head cache synchronized under the same lock as `counts`.
    pub(crate) heads: HashMap<BranchId, BranchHead>,
    /// Released keys waiting for a WAL sync, shared by every pool alias.
    deferred: Option<Vec<Vec<u8>>>,
}

impl RefState {
    fn install(&mut self, expanded: HashMap<NodeId, NodeRef>) {
        self.counts.clear();
        self.edges.clear();
        self.heads.clear();
        self.counts.reserve(expanded.len());
        for (id, node) in expanded {
            self.counts.insert(id, node.ref_count);
            if !node.children.is_empty() {
                self.edges.insert(id, node.children.into_boxed_slice());
            }
        }
        self.ready = true;
    }

    pub(crate) fn set_head(&mut self, branch: BranchId, head: Option<BranchHead>) {
        match head {
            Some(head) => {
                self.heads.insert(branch, head);
            }
            None => {
                self.heads.remove(&branch);
            }
        }
    }

    pub(crate) fn resolve_branch_raw(
        &mut self,
        nodes: &MapxRaw,
        branch: BranchId,
    ) -> Result<BranchHead> {
        if let Some(&head) = self.heads.get(&branch) {
            return Ok(head);
        }
        let bytes = nodes
            .get(row(HEAD, branch.raw()))
            .ok_or(VsdbError::BranchNotFound { branch_id: branch })?;
        let head = BranchHead::decode(&bytes)?;
        self.heads.insert(branch, head);
        Ok(head)
    }

    pub(crate) fn resolve_branch_reader(
        &mut self,
        nodes: &MapxRawReader,
        branch: BranchId,
    ) -> Result<BranchHead> {
        if let Some(&head) = self.heads.get(&branch) {
            return Ok(head);
        }
        let bytes = nodes
            .get(row(HEAD, branch.raw()))
            .ok_or(VsdbError::BranchNotFound { branch_id: branch })?;
        let head = BranchHead::decode(&bytes)?;
        self.heads.insert(branch, head);
        Ok(head)
    }

    fn unpin(&mut self, root: NodeId) -> Vec<NodeId> {
        if root == EMPTY_ROOT {
            return Vec::new();
        }
        let pins = self.pins.get_mut(&root).expect("missing snapshot pin");
        *pins -= 1;
        if *pins == 0 {
            self.pins.remove(&root);
            if self.zombies.remove(&root) {
                self.deferred
                    .get_or_insert_with(Vec::new)
                    .push(row(RETIRED, root).to_vec());
            }
        }
        // A reader may outlive the writer. A later writer drains these
        // releases after its WAL fence, or recovers them on the next open.
        let dead = self.release(root);
        let mut dead_internal = Vec::new();
        let deferred = self.deferred.get_or_insert_with(Vec::new);
        for id in dead {
            if id & CLASS_MASK == 0 {
                dead_internal.push(id);
            }
            deferred.push(object_key(id).to_vec());
        }
        dead_internal
    }

    fn release(&mut self, id: NodeId) -> Vec<NodeId> {
        let mut dead = Vec::new();
        if id == EMPTY_ROOT || !self.ready {
            return dead;
        }
        let mut work = vec![id];
        while let Some(nid) = work.pop() {
            let Some(nr) = self.counts.get_mut(&nid) else {
                continue;
            };
            debug_assert!(*nr > 0, "release of unowned node {nid}");
            if *nr == 0 {
                continue;
            }
            *nr -= 1;
            if *nr == 0 {
                if let Some(children) = self.edges.remove(&nid) {
                    work.extend(Vec::from(children));
                }
                self.counts.remove(&nid);
                dead.push(nid);
            }
        }
        dead
    }

    /// Return only keys that may be registered without another owner sync.
    fn defer_or_return(&mut self, keys: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        if let Some(queue) = self.deferred.as_mut() {
            queue.extend(keys);
            Vec::new()
        } else {
            keys
        }
    }
}

/// Retains an immutable root independently of the source handle.
/// The snapshot and its derived iterators share this lease.
pub(crate) struct RootLease {
    pub(crate) root: NodeId,
    runtime: Arc<TreeRuntime>,
    nodes: MapxRawReader,
}

impl RootLease {
    pub(crate) fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        lookup(self.root, key, |id| {
            read_cached_reader(&self.runtime, &self.nodes, id)
        })
    }

    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        find_leaf(self.root, key, |id| {
            read_cached_reader(&self.runtime, &self.nodes, id)
        })
        .is_some()
    }

    pub(crate) fn range(
        self: &Arc<Self>,
        lo: Bound<Vec<u8>>,
        hi: Bound<Vec<u8>>,
    ) -> BTreeIter<'static> {
        BTreeIter::new(
            self.nodes.clone(),
            Arc::clone(&self.runtime),
            self.root,
            lo,
            hi,
            Some(self.clone()),
        )
    }
}

impl Drop for RootLease {
    fn drop(&mut self) {
        if self.root == EMPTY_ROOT {
            return;
        }
        let dead_internal = self.runtime.refs.lock().unpin(self.root);
        for id in dead_internal {
            self.runtime.page_cache.remove(id);
        }
    }
}

struct ScopedPin<'a> {
    root: NodeId,
    runtime: &'a TreeRuntime,
}

impl Drop for ScopedPin<'_> {
    fn drop(&mut self) {
        let dead_internal = self.runtime.refs.lock().unpin(self.root);
        for id in dead_internal {
            self.runtime.page_cache.remove(id);
        }
    }
}

fn read_map_object(nodes: &MapxRaw, id: NodeId) -> Vec<u8> {
    #[cfg(test)]
    NODE_READS.with(|c| c.set(c.get() + 1));
    nodes
        .get(object_key(id))
        .unwrap_or_else(|| panic!("PersistentBTree: missing object {id}"))
}

fn read_object(nodes: &MapxRawReader, id: NodeId) -> Vec<u8> {
    #[cfg(test)]
    NODE_READS.with(|c| c.set(c.get() + 1));
    nodes
        .get(object_key(id))
        .unwrap_or_else(|| panic!("PersistentBTree: missing object {id}"))
}

pub(crate) fn read_cached_reader_arc(
    runtime: &TreeRuntime,
    nodes: &MapxRawReader,
    id: NodeId,
) -> Arc<[u8]> {
    debug_assert_eq!(id & CLASS_MASK, 0);
    #[cfg(test)]
    NODE_READS.with(|c| c.set(c.get() + 1));
    if let Some(cached) = runtime.page_cache.get(id) {
        return cached;
    }
    let raw: Arc<[u8]> = nodes
        .get(object_key(id))
        .unwrap_or_else(|| panic!("PersistentBTree: missing object {id}"))
        .into();
    runtime.page_cache.insert(id, Arc::clone(&raw));
    raw
}

fn read_cached_reader<'a>(
    runtime: &TreeRuntime,
    nodes: &MapxRawReader,
    id: NodeId,
) -> PageBytes<'a> {
    if id & CLASS_MASK == 0 {
        PageBytes::Shared(read_cached_reader_arc(runtime, nodes, id))
    } else {
        PageBytes::Owned(read_object(nodes, id))
    }
}

fn read_cached_map<'a>(
    runtime: &TreeRuntime,
    nodes: &MapxRaw,
    id: NodeId,
) -> PageBytes<'a> {
    if id & CLASS_MASK == 0 {
        #[cfg(test)]
        NODE_READS.with(|c| c.set(c.get() + 1));
        if let Some(cached) = runtime.page_cache.get(id) {
            return PageBytes::Shared(cached);
        }
        let raw: Arc<[u8]> = nodes
            .get(object_key(id))
            .unwrap_or_else(|| panic!("PersistentBTree: missing object {id}"))
            .into();
        runtime.page_cache.insert(id, Arc::clone(&raw));
        PageBytes::Shared(raw)
    } else {
        PageBytes::Owned(read_map_object(nodes, id))
    }
}

fn lookup<'a>(
    root: NodeId,
    key: &[u8],
    object: impl Fn(NodeId) -> PageBytes<'a>,
) -> Option<Vec<u8>> {
    let (raw, i) = find_leaf(root, key, &object)?;
    let page = NodeView::open(&raw).expect("valid leaf");
    Some(page.value(i, |id| object(id).into_vec()))
}

fn find_leaf<'a>(
    mut root: NodeId,
    key: &[u8],
    object: impl Fn(NodeId) -> PageBytes<'a>,
) -> Option<(PageBytes<'a>, usize)> {
    while root != EMPTY_ROOT {
        let raw = object(root);
        let page = NodeView::open(&raw).expect("invalid B+ tree page");
        let i = page.lower_bound(key, !page.leaf);
        if page.leaf {
            return (i < page.len && page.key(i) == key).then_some((raw, i));
        }
        root = page.child(i);
    }
    None
}

#[derive(Debug)]
pub(crate) struct TreeRuntime {
    next_id: AtomicU64,
    refs: Mutex<RefState>,
    pub(crate) page_cache: InternalPageCache,
}

static TREE_RUNTIMES: LazyLock<Mutex<HashMap<InstanceId, Weak<TreeRuntime>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn tree_runtime(
    id: InstanceId,
    next_id_floor: NodeId,
    ns_budget_bytes: usize,
    initial_refs: RefState,
) -> Arc<TreeRuntime> {
    let mut runtimes = TREE_RUNTIMES.lock();
    if let Some(runtime) = runtimes.get(&id).and_then(Weak::upgrade) {
        runtime.next_id.fetch_max(next_id_floor, Ordering::AcqRel);
        return runtime;
    }
    if runtimes.len() >= 1024 {
        runtimes.retain(|_, runtime| runtime.strong_count() > 0);
    }
    let runtime = Arc::new(TreeRuntime {
        next_id: AtomicU64::new(next_id_floor),
        refs: Mutex::new(initial_refs),
        page_cache: InternalPageCache::from_namespace_budget(ns_budget_bytes),
    });
    runtimes.insert(id, Arc::downgrade(&runtime));
    runtime
}

/// A persistent (copy-on-write) B+ tree backed by [`MapxRaw`].
///
/// All nodes live in a single flat pool keyed by [`NodeId`]. A "tree
/// version" is represented by a root [`NodeId`]; different versions share
/// unchanged subtrees automatically.
///
/// # Examples
///
/// ```
/// use vsdb::basic::persistent_btree::{PersistentBTree, EMPTY_ROOT};
/// use vsdb::{VsdbOptions, vsdb_configure};
/// use std::fs;
///
/// let dir = std::env::temp_dir().join(format!("vsdb-{}", rand::random::<u128>()));
/// vsdb_configure(VsdbOptions::new(&dir)).unwrap();
///
/// let mut tree = PersistentBTree::new();
///
/// // Version 1: insert two entries.
/// let v1 = tree.insert(EMPTY_ROOT, b"alice", b"100");
/// let v1 = tree.insert(v1, b"bob", b"200");
///
/// // Version 2: fork from v1, modify one entry.
/// let v2 = tree.insert(v1, b"alice", b"150");
///
/// // Both versions coexist — structural sharing keeps cost low.
/// assert_eq!(tree.get(v1, b"alice").unwrap(), b"100");
/// assert_eq!(tree.get(v2, b"alice").unwrap(), b"150");
/// assert_eq!(tree.get(v2, b"bob").unwrap(), b"200");
///
/// fs::remove_dir_all(&dir).unwrap();
/// ```
#[derive(Debug)]
pub struct PersistentBTree {
    /// Flat pool of slotted pages and large-value blobs, keyed by tag and big-endian ID.
    pub(crate) nodes: MapxRaw,
    /// Process-local allocator and reference-count state shared by every
    /// alias of this node pool.
    runtime: Arc<TreeRuntime>,
    /// Write buffer for the mutating operation currently in flight.
    ///
    /// `alloc` stages encoded nodes here instead of issuing one engine
    /// put per node; each public mutating entry point (`insert`,
    /// `remove`, `bulk_load`) drains it through a single engine write
    /// batch before returning (`flush_pending`), so per-node engine
    /// overhead (shard lock, WAL record) is paid once per operation and
    /// the operation's node group lands atomically.  `node()` reads
    /// through the buffer, because the remove/underflow and `bulk_load`
    /// paths read back nodes allocated earlier in the same operation.
    /// Nodes discarded before the flush (intra-operation churn from
    /// split/borrow/merge) are dropped from the buffer and never reach
    /// the engine at all.
    ///
    /// **Empty between operations** — every public mutating method
    /// flushes before returning, so serialization (which cannot run
    /// concurrently with a `&mut self` operation) never observes
    /// buffered nodes, and `Clone` only ever copies an empty map.
    pending: HashMap<NodeId, Vec<u8>>,
    pending_bytes: usize,
    stage: Option<Vec<NodeId>>,
}

impl Serialize for PersistentBTree {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // The write buffer is drained by every mutating entry point
        // before it returns, so a serialization (which cannot overlap a
        // `&mut self` operation) must never observe buffered nodes —
        // they would be silently dropped from the snapshot.
        debug_assert!(
            self.pending.is_empty(),
            "PersistentBTree serialized with a non-empty write buffer"
        );
        crate::common::serialize_prefix_handle::<Self, _>(&self.nodes, &[], serializer)
    }
}

impl<'de> Deserialize<'de> for PersistentBTree {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> StdResult<Self, D::Error> {
        crate::common::deserialize_prefix_handle(
            d,
            |crate::common::PrefixFrame(nodes, extra)| {
                let invalid = || VsdbError::Corrupt {
                    detail: "invalid standalone tree allocator".into(),
                };
                if !extra.is_empty() || nodes.contains_key([0]) {
                    return Err(invalid());
                }
                let raw = nodes.get(STANDALONE_ALLOCATOR).ok_or_else(invalid)?;
                let next_id = u64::from_le_bytes(raw.try_into().map_err(|_| invalid())?);
                if next_id == 0 || next_id > LEAF {
                    return Err(invalid());
                }
                for class in [0, LEAF, BLOB_LEAF, BLOB] {
                    if let Some((k, _)) = nodes.get_le(object_key(class | !CLASS_MASK))
                        && let Some(id) = object_id(&k)
                        && id & CLASS_MASK == class
                        && id & !CLASS_MASK >= next_id
                    {
                        return Err(invalid());
                    }
                }
                let runtime = tree_runtime(
                    nodes.instance_id(),
                    next_id,
                    nodes.namespace().mem_budget_bytes(),
                    RefState {
                        counts: HashMap::new(),
                        edges: HashMap::new(),
                        zombies: HashSet::new(),
                        pins: HashMap::new(),
                        heads: HashMap::new(),
                        deferred: None,
                        ready: false,
                    },
                );
                Ok(Self {
                    nodes,
                    runtime,
                    pending: HashMap::new(),
                    pending_bytes: 0,
                    stage: None,
                })
            },
        )
    }
}

impl PersistentBTree {
    /// Returns the unique instance ID of this `PersistentBTree`.
    pub fn instance_id(&self) -> InstanceId {
        self.nodes.instance_id()
    }

    /// Persists this instance's metadata to disk so that it can be
    /// recovered later via [`from_meta`](Self::from_meta).
    ///
    /// Returns the `instance_id` that should be passed to `from_meta`.
    pub fn save_meta(&self) -> Result<InstanceId> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }

    /// Recovers a `PersistentBTree` instance from previously saved metadata.
    ///
    /// The caller must ensure that the underlying VSDB database still
    /// contains the data referenced by this instance ID.
    pub fn from_meta(instance_id: impl Into<InstanceId>) -> Result<Self> {
        let id = instance_id.into();
        crate::common::load_instance_meta_checked(id, Self::instance_id)
    }

    /// Creates a new, empty persistent B+ tree.
    pub fn new_in(ns: &crate::common::Namespace) -> Self {
        ns.scope(Self::new)
    }

    /// The namespace this tree lives in.
    pub fn namespace(&self) -> crate::common::Namespace {
        self.nodes.namespace()
    }

    pub fn new() -> Self {
        let mut nodes = MapxRaw::new();
        let mut batch = nodes.batch();
        batch.insert(STANDALONE_ALLOCATOR, 1u64.to_le_bytes());
        batch
            .commit_sync()
            .expect("failed to create standalone B+ tree");
        let runtime = tree_runtime(
            nodes.instance_id(),
            1,
            nodes.namespace().mem_budget_bytes(),
            RefState {
                counts: HashMap::new(),
                edges: HashMap::new(),
                zombies: HashSet::new(),
                ready: true,
                pins: HashMap::new(),
                heads: HashMap::new(),
                deferred: None,
            },
        );
        Self {
            nodes,
            runtime,
            pending: HashMap::new(),
            pending_bytes: 0,
            stage: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn deferred_reclaims(&self) -> usize {
        self.runtime
            .refs
            .lock()
            .deferred
            .as_ref()
            .map_or(0, Vec::len)
    }

    pub(crate) fn get_branch_head(&self, branch: BranchId) -> Result<BranchHead> {
        self.runtime
            .refs
            .lock()
            .resolve_branch_raw(&self.nodes, branch)
    }

    pub(crate) fn clear_cached_heads(&self) {
        self.runtime.refs.lock().heads.clear();
    }

    pub(crate) fn with_pinned_branch<R>(
        &self,
        branch: BranchId,
        f: impl FnOnce(NodeId) -> R,
    ) -> Result<R> {
        let root = {
            let mut refs = self.runtime.refs.lock();
            let root = refs.resolve_branch_raw(&self.nodes, branch)?.dirty_root;
            if root != EMPTY_ROOT {
                assert!(refs.ready, "lookup requires initialized tree references");
                let count =
                    refs.counts
                        .get_mut(&root)
                        .ok_or_else(|| VsdbError::Corrupt {
                            detail: format!("untracked branch root {root}"),
                        })?;
                *count += 1;
                *refs.pins.entry(root).or_insert(0) += 1;
            }
            root
        };
        let _pin = (root != EMPTY_ROOT).then_some(ScopedPin {
            root,
            runtime: &self.runtime,
        });
        Ok(f(root))
    }

    pub(crate) fn get_committed(&self, root: NodeId, key: &[u8]) -> Option<Vec<u8>> {
        lookup(root, key, |id| {
            read_cached_map(&self.runtime, &self.nodes, id)
        })
    }

    pub(crate) fn contains_committed(&self, root: NodeId, key: &[u8]) -> bool {
        find_leaf(root, key, |id| {
            read_cached_map(&self.runtime, &self.nodes, id)
        })
        .is_some()
    }

    /// Capture and retain a published root while releases/recounts are
    /// excluded. Reading the owning row under this lock closes the gap
    /// between observing its root and acquiring the reader reference.
    pub(crate) fn lease_root(
        &self,
        resolve: impl FnOnce(&mut RefState) -> Result<NodeId>,
    ) -> Result<Arc<RootLease>> {
        let [lease] = self.lease_roots(|refs| Ok([resolve(refs)?]))?;
        Ok(lease)
    }

    /// Capture related roots under one lock, retaining every state before
    /// a writer can release any of them. Duplicate roots get distinct leases.
    pub(crate) fn lease_roots<const N: usize>(
        &self,
        resolve: impl FnOnce(&mut RefState) -> Result<[NodeId; N]>,
    ) -> Result<[Arc<RootLease>; N]> {
        self.read_state().lease(&self.nodes.reader(), resolve)
    }

    pub(crate) fn read_state(&self) -> TreeReadState {
        TreeReadState(Arc::clone(&self.runtime))
    }
    // ----- low-level helpers -----

    /// Number of buffered nodes above which `bulk_load` flushes
    /// mid-operation, bounding the write buffer's memory footprint
    /// (the same larger-than-RAM hazard `Mapx::clear`/`Clone` chunk
    /// against).  Path-copying operations (`insert`/`remove`) stay far
    /// below this — they buffer O(depth) nodes.
    const PENDING_FLUSH_THRESHOLD: usize = 1024;

    fn alloc_object(
        &mut self,
        class: u64,
        raw: Vec<u8>,
        children: Vec<NodeId>,
    ) -> NodeId {
        let serial = self
            .runtime
            .next_id
            .try_update(Ordering::AcqRel, Ordering::Acquire, |next| {
                (next < LEAF).then_some(next + 1)
            })
            .expect("B+ tree object IDs exhausted");
        let id = class | serial;
        self.pending_bytes += raw.len() + 9;
        self.pending.insert(id, raw);
        let mut refs = self.runtime.refs.lock();
        if refs.ready {
            for child in &children {
                *refs.counts.get_mut(child).expect("untracked child") += 1;
            }
            refs.counts.insert(id, 0);
            if !children.is_empty() {
                refs.edges.insert(id, children.into_boxed_slice());
            }
        }
        id
    }

    fn alloc(&mut self, node: &Node) -> NodeId {
        let mut edges = Vec::new();
        let raw = node.encode_with(&mut edges, |v| {
            self.alloc_object(BLOB, v.to_vec(), Vec::new())
        });
        let class = match node {
            Node::Internal { .. } => 0,
            Node::Leaf { .. } if edges.is_empty() => LEAF,
            Node::Leaf { .. } => BLOB_LEAF,
        };
        self.alloc_object(class, raw, edges)
    }

    fn object(&self, id: NodeId) -> PageBytes<'_> {
        #[cfg(test)]
        NODE_READS.with(|c| c.set(c.get() + 1));
        if let Some(raw) = self.pending.get(&id) {
            return PageBytes::Borrowed(raw);
        }
        if id & CLASS_MASK == 0 {
            if let Some(cached) = self.runtime.page_cache.get(id) {
                return PageBytes::Shared(cached);
            }
            let raw: Arc<[u8]> = self
                .nodes
                .get(object_key(id))
                .unwrap_or_else(|| panic!("missing B+ tree object {id}"))
                .into();
            self.runtime.page_cache.insert(id, Arc::clone(&raw));
            return PageBytes::Shared(raw);
        }
        PageBytes::Owned(
            self.nodes
                .get(object_key(id))
                .unwrap_or_else(|| panic!("missing B+ tree object {id}")),
        )
    }

    pub(crate) fn node(&self, id: NodeId) -> Node {
        Node::decode(&self.object(id))
    }

    pub(crate) fn leaf_val_eq(&self, existing: &LeafVal, new_bytes: &[u8]) -> bool {
        match existing {
            LeafVal::Bytes(b) => b.as_slice() == new_bytes,
            LeafVal::Blob(id) => {
                new_bytes.len() > 1024 && self.object(*id).as_ref() == new_bytes
            }
        }
    }

    /// Drains the write buffer into a single engine write batch.
    ///
    /// Called by every public mutating entry point before it returns,
    /// and mid-`bulk_load` at [`Self::PENDING_FLUSH_THRESHOLD`].  An
    /// engine-level commit failure panics, matching the per-put
    /// failure behavior this replaces (mutating signatures are
    /// infallible). Preflight rejection leaves the batch unapplied;
    /// persistence errors can leave recoverable WAL data, so a panic
    /// does not prove that the write was rolled back.
    pub(crate) fn flush_pending(&mut self) {
        if self.stage.is_some() {
            return;
        }
        self.pending_bytes = 0;
        if self.pending.is_empty() {
            return;
        }
        let next = self.next_id();
        let mut batch = self.nodes.batch();
        batch.insert(STANDALONE_ALLOCATOR, next.to_le_bytes());
        for (id, raw) in self.pending.drain() {
            batch.insert(object_key(id), &raw);
        }
        batch
            .commit()
            .expect("vsdb: node flush failed during btree write");
    }

    /// Binary-search `keys` for `target`.  Returns child index to descend.
    fn child_index(keys: &[Vec<u8>], target: &[u8]) -> usize {
        match keys.binary_search_by(|k| k.as_slice().cmp(target)) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    }

    // =================================================================
    // GET
    // =================================================================

    /// Looks up `key` in the tree rooted at `root`.
    ///
    /// Returns `None` if the tree is empty or the key is absent.
    pub fn get(&self, root: NodeId, key: &[u8]) -> Option<Vec<u8>> {
        lookup(root, key, |id| self.object(id))
    }

    /// Returns `true` if `key` exists in the tree rooted at `root`.
    #[inline]
    pub fn contains_key(&self, root: NodeId, key: &[u8]) -> bool {
        find_leaf(root, key, |id| self.object(id)).is_some()
    }

    // =================================================================
    // ITERATION
    // =================================================================

    /// Returns an iterator over **all** entries in ascending key order.
    pub fn iter(&self, root: NodeId) -> BTreeIter<'_> {
        BTreeIter::new(
            self.nodes.reader(),
            Arc::clone(&self.runtime),
            root,
            Bound::Unbounded,
            Bound::Unbounded,
            None,
        )
    }

    /// Returns an iterator over the given key range.
    pub fn range(
        &self,
        root: NodeId,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
    ) -> BTreeIter<'_> {
        let lo = match lo {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let hi = match hi {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        BTreeIter::new(
            self.nodes.reader(),
            Arc::clone(&self.runtime),
            root,
            lo,
            hi,
            None,
        )
    }

    // =================================================================
    // BULK LOAD
    // =================================================================

    /// Builds a tree from a **pre-sorted** list of `(key, value)` pairs.
    ///
    /// Much faster than inserting one-by-one, and produces an optimally
    /// packed tree.
    ///
    /// Retains at most two groups per level, plus a bounded write batch.
    /// Adjacent duplicate keys keep the last supplied value.
    pub fn bulk_load(
        &mut self,
        entries: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> NodeId {
        use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
        let start = self.next_id();
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.try_bulk_load(entries.into_iter().map(Ok))
        }));
        match result {
            Ok(Ok(root)) => root,
            failed => {
                // All nodes in a bottom-up build are private to that build.
                // Keep any pre-existing external root and its counts intact.
                self.pending.clear();
                self.pending_bytes = 0;
                let mut refs = self.runtime.refs.lock();
                refs.counts.retain(|id, _| id & !CLASS_MASK < start);
                refs.edges.retain(|id, _| id & !CLASS_MASK < start);
                drop(refs);
                let abandoned = self
                    .nodes
                    .iter()
                    .filter_map(|(key, _)| {
                        object_id(&key)
                            .filter(|id| id & !CLASS_MASK >= start)
                            .map(|_| key)
                    })
                    .collect::<Vec<_>>();
                self.nodes.lazy_delete_batch(abandoned);
                match failed {
                    Ok(Err(err)) => panic!("{err}"),
                    Err(payload) => resume_unwind(payload),
                    Ok(Ok(_)) => unreachable!(),
                }
            }
        }
    }

    pub(crate) fn try_bulk_load(
        &mut self,
        entries: impl IntoIterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
    ) -> Result<NodeId> {
        let mut leaves = Vec::new();
        let mut levels = Vec::new();
        for entry in entries {
            let (k, v) = entry?;
            // The last key stays in the retained leaf tail across every flush.
            if let Some((previous, value)) = leaves.last_mut() {
                match k.cmp(previous) {
                    KeyOrdering::Less => return Err(VsdbError::InvalidConfig {
                        detail:
                            "PersistentBTree::bulk_load entries must be sorted by key"
                                .into(),
                    }),
                    KeyOrdering::Equal => {
                        *value = v;
                        continue;
                    }
                    KeyOrdering::Greater => {}
                }
            }
            leaves.push((k, v));
            if leaves.len() == MAX_KEYS + MIN_KEYS {
                let child = self.bulk_leaf(leaves.drain(..MAX_KEYS))?;
                self.push_bulk_child(&mut levels, 0, child)?;
            }
        }
        if leaves.is_empty() {
            return Ok(EMPTY_ROOT);
        }
        let tail = if leaves.len() > MAX_KEYS {
            leaves.split_off(leaves.len().div_ceil(2))
        } else {
            Vec::new()
        };
        for group in [leaves, tail].into_iter().filter(|g| !g.is_empty()) {
            let child = self.bulk_leaf(group.into_iter())?;
            self.push_bulk_child(&mut levels, 0, child)?;
        }
        let mut level = 0;
        loop {
            let mut children = std::mem::take(&mut levels[level]);
            if children.len() == 1 && level + 1 == levels.len() {
                self.flush_pending();
                return Ok(children[0].1);
            }
            let tail = if children.len() > MAX_KEYS + 1 {
                children.split_off(children.len().div_ceil(2))
            } else {
                Vec::new()
            };
            for group in [children, tail].into_iter().filter(|g| !g.is_empty()) {
                let parent = self.bulk_internal(group)?;
                self.push_bulk_child(&mut levels, level + 1, parent)?;
            }
            level += 1;
        }
    }

    fn bulk_leaf(
        &mut self,
        entries: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> Result<(Vec<u8>, NodeId)> {
        let (keys, values): (Vec<_>, Vec<_>) =
            entries.map(|(k, v)| (k, LeafVal::Bytes(v))).unzip();
        let first = keys[0].clone();
        let id = self.alloc(&Node::Leaf { keys, values });
        self.flush_pending_if_large()?;
        Ok((first, id))
    }

    fn bulk_internal(
        &mut self,
        entries: Vec<(Vec<u8>, NodeId)>,
    ) -> Result<(Vec<u8>, NodeId)> {
        let mut entries = entries.into_iter();
        let (first, left) = entries.next().unwrap();
        let mut keys = Vec::with_capacity(entries.len());
        let mut children = Vec::with_capacity(entries.len() + 1);
        children.push(left);
        for (separator, child) in entries {
            keys.push(separator);
            children.push(child);
        }
        let id = self.alloc(&Node::Internal { keys, children });
        self.flush_pending_if_large()?;
        Ok((first, id))
    }

    fn push_bulk_child(
        &mut self,
        levels: &mut Vec<Vec<(Vec<u8>, NodeId)>>,
        mut level: usize,
        mut child: (Vec<u8>, NodeId),
    ) -> Result<()> {
        loop {
            if level == levels.len() {
                levels.push(Vec::new());
            }
            let group = &mut levels[level];
            group.push(child);
            if group.len() < MAX_KEYS + MIN_KEYS + 2 {
                return Ok(());
            }
            let tail = group.split_off(MAX_KEYS + 1);
            child = self.bulk_internal(std::mem::replace(group, tail))?;
            level += 1;
        }
    }

    // =================================================================
    // NODE REFERENCE COUNTING
    // =================================================================

    /// Increments the in-memory reference count for `id`.
    pub fn acquire_node(&mut self, id: NodeId) {
        if id == EMPTY_ROOT {
            return;
        }
        let mut refs = self.runtime.refs.lock();
        if !refs.ready {
            return;
        }
        if let Some(nr) = refs.counts.get_mut(&id) {
            *nr += 1;
        }
    }

    /// Decrements the in-memory reference count for `id`.
    /// If it reaches zero, cascades to all children, removes the entry
    /// from the in-memory map, and registers the node for deferred disk
    /// deletion via the storage engine's compaction filter.
    pub fn release_node(&mut self, id: NodeId) {
        // Callers (VerMap) only release between tree operations; the
        // write buffer must already be drained.
        debug_assert!(
            self.pending.is_empty(),
            "release_node called with a non-empty write buffer"
        );
        self.release_buffered(id);
    }

    /// [`release_node`](Self::release_node) inside a buffered multi-step
    /// operation: a node that dies while still in the write buffer is
    /// dropped there and never reaches the engine; flushed ones take the
    /// usual deferred deletion.
    pub(crate) fn release_buffered(&mut self, id: NodeId) {
        if id == EMPTY_ROOT {
            return;
        }
        let mut dead_internal = Vec::new();
        let immediate = {
            let mut refs = self.runtime.refs.lock();
            let dead_keys = refs
                .release(id)
                .into_iter()
                .filter(|&nid| {
                    if nid & CLASS_MASK == 0 {
                        dead_internal.push(nid);
                    }
                    if let Some(raw) = self.pending.remove(&nid) {
                        self.pending_bytes -= 9 + raw.len();
                        false
                    } else {
                        true
                    }
                })
                .map(|nid| object_key(nid).to_vec())
                .collect();
            refs.defer_or_return(dead_keys)
        };
        for nid in dead_internal {
            self.runtime.page_cache.remove(nid);
        }
        if !immediate.is_empty() {
            self.nodes.lazy_delete_batch(immediate);
        }
    }

    /// Drops buffered nodes that belong only to `dead` when ref tracking
    /// is off. `release_buffered` cannot see those nodes yet: a restored
    /// tree stays `ready == false` until `rebuild_ref_counts`, and acquire
    /// / release are no-ops in that mode. On-disk nodes are left in place
    /// because, without counts, they may still belong to the pre-merge tree.
    fn drop_untracked_pending(&mut self, dead: NodeId, survivor: NodeId) {
        if self.runtime.refs.lock().ready {
            return;
        }
        let keep = self.pending_closure(survivor);
        for id in self.pending_closure(dead) {
            if !keep.contains(&id)
                && let Some(raw) = self.pending.remove(&id)
            {
                self.pending_bytes -= raw.len() + 9;
            }
        }
    }

    /// Pending node ids reachable from `root`. An id that is not buffered
    /// is recorded but not expanded: its children are already durable and
    /// cannot point at nodes allocated later in this operation.
    pub(super) fn pending_closure(&self, root: NodeId) -> HashSet<NodeId> {
        let mut seen = HashSet::new();
        let mut stack = vec![root];
        while let Some(id) = stack.pop() {
            if id == EMPTY_ROOT || !seen.insert(id) {
                continue;
            }
            let Some(raw) = self.pending.get(&id) else {
                continue;
            };
            if id & CLASS_MASK != BLOB {
                stack.extend(Node::decode_children(raw));
            }
        }
        seen
    }

    /// Drops one reference taken with [`acquire_node`](Self::acquire_node)
    /// **without** reclaiming the node at zero: it stays alive as an
    /// unowned fresh root (like a [`bulk_load`](Self::bulk_load) result)
    /// for the caller to adopt.
    pub(crate) fn disown_node(&mut self, id: NodeId) {
        if id == EMPTY_ROOT {
            return;
        }
        let mut refs = self.runtime.refs.lock();
        if !refs.ready {
            return;
        }
        if let Some(nr) = refs.counts.get_mut(&id) {
            debug_assert!(*nr > 0, "disown_node on unowned node {id}");
            *nr = nr.saturating_sub(1);
        }
    }

    /// Flushes the write buffer once it reaches the bulk threshold, bounding
    /// memory during a buffered multi-step operation.
    pub(crate) fn flush_pending_if_large(&mut self) -> Result<()> {
        if self.stage.is_some() {
            if self.pending_bytes >= 16 << 20 {
                self.flush_stage()?;
            }
        } else if self.pending.len() >= Self::PENDING_FLUSH_THRESHOLD
            || self.pending_bytes >= 16 << 20
        {
            self.flush_pending();
        }
        Ok(())
    }

    /// Number of internal levels above the leaves (0 for a leaf root).
    pub(crate) fn height(&self, root: NodeId) -> u32 {
        let mut h = 0;
        let mut cur = root;
        while cur != EMPTY_ROOT {
            let raw = self.object(cur);
            let page = NodeView::open(&raw).expect("invalid B+ tree page");
            if page.leaf {
                break;
            }
            h += 1;
            cur = page.child(0);
        }
        h
    }

    /// Estimates entry count in `O(height)` leftmost-spine page-header reads
    /// without reading sibling subtrees or `[0x12]` blobs.
    pub(crate) fn estimate_entries(&self, root: NodeId) -> usize {
        if root == EMPTY_ROOT {
            return 0;
        }
        let mut factor = 1usize;
        let mut cur = root;
        loop {
            let raw = self.object(cur);
            let page = NodeView::open(&raw).expect("invalid B+ tree page");
            if page.leaf {
                return factor.saturating_mul(page.len);
            }
            factor = factor.saturating_mul(page.len + 1);
            cur = page.child(0);
        }
    }

    /// Rebuilds the in-memory reference-count map from scratch by
    /// walking all nodes reachable from `live_roots` and live snapshot leases.
    ///
    /// Also registers unreachable nodes for deferred disk deletion.
    pub fn rebuild_ref_counts(&mut self, live_roots: &[NodeId]) {
        self.finish_ref_counts(HashMap::new(), live_roots)
            .expect("B+ tree recovery failed");
    }

    /// Trace durable branch and commit roots before any recovery writes.
    /// Structural writers are excluded during recovery. Reader pins are
    /// added separately under the pool lock immediately before installation.
    pub(crate) fn required_node_refs(
        &self,
        roots: &[NodeId],
    ) -> Result<HashMap<NodeId, NodeRef>> {
        let mut counts = HashMap::new();
        self.count_nodes(&mut counts, roots.iter().map(|&id| (id, 1)))?;
        Ok(counts)
    }

    pub(crate) fn restore_runtime_refs(&self, roots: &[NodeId]) -> Result<()> {
        let mut counts = self.required_node_refs(roots)?;
        let mut refs = self.runtime.refs.lock();
        self.count_nodes(
            &mut counts,
            refs.pins.iter().map(|(&root, &pins)| (root, pins)),
        )?;
        refs.install(counts);
        Ok(())
    }
    fn count_nodes(
        &self,
        counts: &mut HashMap<NodeId, NodeRef>,
        roots: impl Iterator<Item = (NodeId, u32)>,
    ) -> Result<()> {
        let mut queue: Vec<_> = roots.filter(|&(id, _)| id != EMPTY_ROOT).collect();
        while let Some((id, owners)) = queue.pop() {
            if let Some(node) = counts.get_mut(&id) {
                node.ref_count =
                    node.ref_count.checked_add(owners).ok_or_else(|| {
                        VsdbError::Corrupt {
                            detail: "B+ tree reference count overflow".into(),
                        }
                    })?;
                continue;
            }
            let children = match self.nodes.get(object_key(id)) {
                Some(_) if id & CLASS_MASK == BLOB => Vec::new(),
                Some(bytes) => {
                    let page = NodeView::open(&bytes)?;
                    page.validate()?;
                    let children = page.edges();
                    let class = if !page.leaf {
                        0
                    } else if children.is_empty() {
                        LEAF
                    } else {
                        BLOB_LEAF
                    };
                    if class != id & CLASS_MASK
                        || children.iter().any(|&c| {
                            c & !CLASS_MASK >= id & !CLASS_MASK || c & !CLASS_MASK == 0
                        })
                    {
                        return Err(VsdbError::Corrupt {
                            detail: format!("invalid child or class on page {id}"),
                        });
                    }
                    children
                }
                None => {
                    return Err(VsdbError::Corrupt {
                        detail: format!("missing reachable B+ tree node {id}"),
                    });
                }
            };
            queue.extend(children.iter().map(|&child| (child, 1)));
            counts.insert(
                id,
                NodeRef {
                    ref_count: owners,
                    children,
                },
            );
        }
        Ok(())
    }

    pub(crate) fn finish_ref_counts(
        &mut self,
        mut new_refs: HashMap<NodeId, NodeRef>,
        other_roots: &[NodeId],
    ) -> Result<()> {
        let mut refs = self.runtime.refs.lock();
        let roots = other_roots
            .iter()
            .map(|&id| (id, 1))
            .chain(refs.pins.iter().map(|(&id, &pins)| (id, pins)));
        self.count_nodes(&mut new_refs, roots)?;

        // Register unreachable nodes for deferred disk deletion, and find
        // the maximum stored NodeId.  After crash recovery from a stale
        // meta snapshot, `next_id` may lag behind nodes already written to
        // the engine; allocating over them would mutate live (or pending
        // lazy-delete) nodes in place and corrupt shared snapshots.
        let mut max_id = 0;
        let dead_keys: Vec<Vec<u8>> = self
            .nodes
            .iter()
            .filter_map(|(k, _)| {
                let id = object_id(&k)?;
                max_id = max_id.max(id & !CLASS_MASK);
                (!new_refs.contains_key(&id)).then_some(k)
            })
            .collect();

        self.runtime.next_id.fetch_max(
            max_id
                .checked_add(1)
                .expect("PersistentBTree: NodeId space exhausted"),
            Ordering::AcqRel,
        );

        refs.install(new_refs);
        drop(refs);
        if self.stage.is_some() && !self.namespace().is_read_only() {
            for group in dead_keys.chunks(8192) {
                let mut batch = self.nodes.batch();
                for key in group {
                    batch.remove(key);
                }
                batch.commit_sync()?;
            }
        } else if !dead_keys.is_empty() {
            self.nodes.lazy_delete_batch(dead_keys);
        }
        Ok(())
    }

    // =================================================================
    // GARBAGE COLLECTION
    // =================================================================

    /// Rebuilds the in-memory reference-count map and registers any
    /// unreachable nodes for deferred disk deletion.
    ///
    /// In normal operation this is **not required** — [`Self::release_node`]
    /// already registers dead nodes for compaction.  Call this only for:
    ///
    /// - **Crash recovery** — when runtime reference counts are unavailable
    ///   after deserialization or an interrupted cascade.
    /// - **Forced full sweep** — when you want to guarantee that every
    ///   unreachable node is registered, even if a prior `release_node`
    ///   cascade was incomplete.
    pub fn gc(&mut self, live_roots: &[NodeId]) {
        self.rebuild_ref_counts(live_roots);
    }

    /// Asserts the incremental reference counts equal a from-scratch
    /// recount for `owned` (one entry per acquired root reference): every
    /// reachable node is tracked with its exact parent + root count, and
    /// nothing unreachable is still tracked.
    #[cfg(test)]
    pub(crate) fn assert_refs_match_recount(&self, owned: &[NodeId]) {
        let mut expected: HashMap<NodeId, u32> = HashMap::new();
        let mut seen = HashSet::new();
        let mut queue = Vec::new();
        for &root in owned {
            if root != EMPTY_ROOT {
                *expected.entry(root).or_insert(0) += 1;
                queue.push(root);
            }
        }
        while let Some(id) = queue.pop() {
            if !seen.insert(id) {
                continue;
            }
            if id & CLASS_MASK != BLOB {
                for c in Node::decode_children(&self.object(id)) {
                    *expected.entry(c).or_insert(0) += 1;
                    queue.push(c);
                }
            }
        }
        let refs = self.runtime.refs.lock();
        assert!(refs.ready);
        let actual: HashMap<NodeId, u32> =
            refs.counts.iter().map(|(&id, &nr)| (id, nr)).collect();
        assert_eq!(
            actual, expected,
            "incremental ref counts diverge from recount"
        );
    }
}

impl Default for PersistentBTree {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for PersistentBTree {
    fn clone(&self) -> Self {
        self.clone_in(&self.namespace())
            .expect("vsdb: PersistentBTree clone failed")
    }
}

impl PersistentBTree {
    /// Deep-copies this node pool into a fresh prefix in `ns`.
    pub fn clone_in(&self, ns: &crate::common::Namespace) -> Result<Self> {
        debug_assert!(
            self.pending.is_empty(),
            "PersistentBTree cloned with a non-empty write buffer"
        );
        let nodes = self.nodes.clone_in(ns)?;
        let refs = self.runtime.refs.lock();
        let mut counts = refs.counts.clone();
        // Reader leases belong to the original pool. Any resulting zero-ref
        // root remains unowned, as with standalone mutation results; VerMap's
        // clone recounts from its copied persistent graph before returning.
        for (&root, &pins) in &refs.pins {
            let count = counts.get_mut(&root).expect("untracked pin");
            *count = count
                .checked_sub(pins)
                .expect("pin count exceeds references");
        }
        let runtime = tree_runtime(
            nodes.instance_id(),
            self.runtime.next_id.load(Ordering::Acquire),
            nodes.namespace().mem_budget_bytes(),
            RefState {
                counts,
                edges: refs.edges.clone(),
                zombies: HashSet::new(),
                ready: refs.ready,
                pins: HashMap::new(),
                heads: HashMap::new(),
                deferred: None,
            },
        );
        Ok(Self {
            nodes,
            runtime,
            pending: HashMap::new(),
            pending_bytes: 0,
            stage: None,
        })
    }
}

/// Opaque reference-count state retained by version store readers.
#[derive(Clone, Debug)]
pub struct TreeReadState(Arc<TreeRuntime>);
impl TreeReadState {
    pub(crate) fn lease<const N: usize>(
        &self,
        nodes: &MapxRawReader,
        resolve: impl FnOnce(&mut RefState) -> Result<[NodeId; N]>,
    ) -> Result<[Arc<RootLease>; N]> {
        let mut refs = self.0.refs.lock();
        let roots = resolve(&mut refs)?;
        // Validate the whole set before changing counts, so a corrupt
        // later root cannot strand an earlier pin during unwinding.
        for &root in roots.iter().filter(|&&root| root != EMPTY_ROOT) {
            assert!(refs.ready, "snapshot requires initialized tree references");
            if !refs.counts.contains_key(&root) {
                return Err(VsdbError::Corrupt {
                    detail: format!("untracked snapshot root {root}"),
                });
            }
        }
        for &root in roots.iter().filter(|&&root| root != EMPTY_ROOT) {
            *refs.counts.get_mut(&root).expect("untracked snapshot root") += 1;
            *refs.pins.entry(root).or_insert(0) += 1;
        }
        drop(refs);
        Ok(roots.map(|root| {
            Arc::new(RootLease {
                root,
                runtime: Arc::clone(&self.0),
                nodes: nodes.clone(),
            })
        }))
    }
}
