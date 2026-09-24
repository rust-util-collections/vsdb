//!
//! Persistent B+ Tree with copy-on-write structural sharing.
//!
//! Every mutation returns a new root [`NodeId`], leaving previous versions
//! intact. Nodes live in a flat pool backed by [`MapxRaw`], so unchanged
//! subtrees are shared across versions at the node level.
//!
//! This data structure is analogous to Git's *tree object*: a single
//! [`NodeId`] is a complete, self-contained snapshot of an ordered map.
//!
//! # Design
//!
//! * **Branching factor** — each node holds between `B` and `2B` keys
//!   (except the root which may hold fewer). The default `B = 16` gives
//!   nodes of 16..32 keys and a tree depth of ~4 for 1 million entries.
//! * **Path copying** — inserting or removing a single key allocates at
//!   most `O(depth)` new nodes (~4), sharing all others.
//! * **Garbage collection** — nodes released via [`PersistentBTree::release_node`]
//!   are automatically registered for deferred deletion when their
//!   reference count reaches zero.  Call [`PersistentBTree::gc`] for
//!   crash recovery or to sweep nodes that became unreachable through
//!   internal mutation paths.
//!

#[cfg(test)]
mod test;

mod diff;
mod insert;
mod iter;
mod merge;
mod nodes;
mod remove;
mod types;

pub use diff::TreeDiff;
pub use iter::BTreeIter;

use crate::common::{InstanceId, error::Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    ops::Bound,
    result::Result as StdResult,
    sync::{
        Arc, LazyLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
};
use vsdb_core::basic::mapx_raw::MapxRaw;

pub(crate) use nodes::{MAX_KEYS, MIN_KEYS, Node};
pub use types::{EMPTY_ROOT, NodeId};
pub(crate) use types::{InsertResult, LeafState, NodeRef, RemoveResult};

// =========================================================================
// PersistentBTree
// =========================================================================

#[cfg(test)]
thread_local! {
    /// Node decodes performed by this thread (test instrumentation).
    pub(crate) static NODE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[derive(Debug)]
struct RefState {
    counts: HashMap<NodeId, NodeRef>,
    ready: bool,
    /// One reference per live snapshot lease, shared by its iterators.
    pins: HashMap<NodeId, u32>,
    /// Released keys waiting for a WAL sync, shared by every pool alias.
    deferred: Option<Vec<Vec<u8>>>,
}

impl RefState {
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
            debug_assert!(nr.ref_count > 0, "release of unowned node {nid}");
            if nr.ref_count == 0 {
                continue;
            }
            nr.ref_count -= 1;
            if nr.ref_count == 0 {
                work.extend(std::mem::take(&mut nr.children));
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

/// Owns a captured root independently of the map borrow. Keeping this type
/// lifetime-free preserves last-use borrowing of Snapshot.
/// Its Arc is shared by the snapshot and every iterator derived from it.
pub(crate) struct RootLease {
    pub(crate) root: NodeId,
    runtime: Arc<TreeRuntime>,
    nodes: MapxRaw,
}

impl Drop for RootLease {
    fn drop(&mut self) {
        if self.root == EMPTY_ROOT {
            return;
        }
        let immediate = {
            let mut refs = self.runtime.refs.lock();
            let pins = refs.pins.get_mut(&self.root).expect("missing snapshot pin");
            *pins -= 1;
            if *pins == 0 {
                refs.pins.remove(&self.root);
            }
            // A captured root only reaches published, already-flushed nodes;
            // unlike release_buffered, this path has no pending entries.
            let dead = refs
                .release(self.root)
                .into_iter()
                .map(|id| id.to_le_bytes().to_vec())
                .collect();
            refs.defer_or_return(dead)
        };
        if !immediate.is_empty() {
            self.nodes.lazy_delete_batch(immediate);
        }
    }
}

#[derive(Debug)]
struct TreeRuntime {
    next_id: AtomicU64,
    refs: Mutex<RefState>,
}

static TREE_RUNTIMES: LazyLock<Mutex<HashMap<InstanceId, Weak<TreeRuntime>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn tree_runtime(
    id: InstanceId,
    next_id_floor: NodeId,
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
/// let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
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
    /// Flat node pool.  Key = little-endian NodeId, Value = encoded Node.
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
        use serde::ser::SerializeTuple;
        let mut t = serializer.serialize_tuple(2)?;
        t.serialize_element(&self.nodes)?;
        t.serialize_element(&self.runtime.next_id.load(Ordering::Acquire))?;
        t.end()
    }
}

impl<'de> Deserialize<'de> for PersistentBTree {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Vis;
        impl<'de> serde::de::Visitor<'de> for Vis {
            type Value = PersistentBTree;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("PersistentBTree")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> StdResult<PersistentBTree, A::Error> {
                let nodes: MapxRaw = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let stored_next_id: NodeId = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                // Defensively recover `next_id` against a stale snapshot.
                // If the meta was saved before later allocations were
                // flushed to the shared node pool, `stored_next_id` may lag
                // the highest on-disk NodeId.  Allocating over those ids
                // would overwrite live (or pending lazy-delete) nodes in
                // place and corrupt shared snapshots (INV-BT1).  Dead ids
                // are left as a safe gap and reclaimed by a later `gc`.
                // VerMap recomputes this again in rebuild_ref_counts, but
                // standalone PersistentBTree users may mutate before any
                // rebuild, so the floor must be safe immediately.
                let mut next_id = stored_next_id;
                for (k, _) in nodes.iter() {
                    let id = NodeId::from_le_bytes(k[..8].try_into().unwrap());
                    next_id = next_id.max(id.saturating_add(1));
                }
                let runtime = tree_runtime(
                    nodes.instance_id(),
                    next_id,
                    RefState {
                        counts: HashMap::new(),
                        ready: false,
                        pins: HashMap::new(),
                        deferred: None,
                    },
                );
                Ok(PersistentBTree {
                    nodes,
                    runtime,
                    pending: Default::default(),
                })
            }
        }
        deserializer.deserialize_tuple(2, Vis)
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
        let nodes = MapxRaw::new();
        let runtime = tree_runtime(
            nodes.instance_id(),
            1,
            RefState {
                counts: HashMap::new(),
                ready: true,
                pins: HashMap::new(),
                deferred: None,
            },
        );
        Self {
            nodes,
            runtime,
            pending: HashMap::new(),
        }
    }

    /// Switches this node pool, including every alias and snapshot lease,
    /// to deferred reclamation.
    ///
    /// By default a node whose last reference is released is registered
    /// for physical deletion immediately, which is only safe once the
    /// write that dropped that reference is durable — a caller that fences
    /// its own WAL first (or never publishes roots) needs nothing else.
    /// An owner that does *not* sync before releasing opts in here and
    /// calls [`register_deferred_reclaims`](Self::register_deferred_reclaims)
    /// after each of its WAL syncs.  Unregistered keys are not a leak:
    /// the next [`rebuild_ref_counts`](Self::rebuild_ref_counts) sweeps
    /// every unreachable node.
    pub(crate) fn defer_reclaim(&mut self) {
        self.runtime
            .refs
            .lock()
            .deferred
            .get_or_insert_with(Vec::new);
    }

    /// Number of released nodes waiting for registration.
    pub(crate) fn deferred_reclaims(&self) -> usize {
        self.runtime
            .refs
            .lock()
            .deferred
            .as_ref()
            .map_or(0, Vec::len)
    }

    /// Registers the queued nodes for physical deletion. Call only after
    /// the writes that released them are durable.
    pub(crate) fn register_deferred_reclaims(&mut self) {
        let keys = self
            .runtime
            .refs
            .lock()
            .deferred
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default();
        if !keys.is_empty() {
            self.nodes.lazy_delete_batch(keys);
        }
    }

    /// Capture and retain a published root while releases/recounts are
    /// excluded. Reading the owning row under this lock closes the gap
    /// between observing its root and acquiring the reader reference.
    pub(crate) fn lease_root(
        &self,
        resolve: impl FnOnce() -> Result<NodeId>,
    ) -> Result<Arc<RootLease>> {
        let mut refs = self.runtime.refs.lock();
        let root = resolve()?;
        if root != EMPTY_ROOT {
            assert!(refs.ready, "snapshot requires initialized tree references");
            refs.counts
                .get_mut(&root)
                .expect("untracked snapshot root")
                .ref_count += 1;
            *refs.pins.entry(root).or_insert(0) += 1;
        }
        drop(refs);
        Ok(Arc::new(RootLease {
            root,
            runtime: Arc::clone(&self.runtime),
            // SAFETY: this handle only registers deletion of immutable nodes
            // after their final reference is released. NodeIds are never
            // reused, and co-located owners defer registration until WAL sync.
            nodes: unsafe { self.nodes.shadow() },
        }))
    }

    // ----- low-level helpers -----

    /// Number of buffered nodes above which `bulk_load` flushes
    /// mid-operation, bounding the write buffer's memory footprint
    /// (the same larger-than-RAM hazard `Mapx::clear`/`Clone` chunk
    /// against).  Path-copying operations (`insert`/`remove`) stay far
    /// below this — they buffer O(depth) nodes.
    const PENDING_FLUSH_THRESHOLD: usize = 1024;

    fn alloc(&mut self, node: &Node) -> NodeId {
        let id = self
            .runtime
            .next_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |next_id| {
                next_id.checked_add(1)
            })
            .expect("PersistentBTree: NodeId space exhausted");
        debug_assert!(
            !self.pending.contains_key(&id)
                && self.nodes.get(id.to_le_bytes()).is_none(),
            "PersistentBTree: NodeId {id} already occupied — allocator regression"
        );
        self.pending.insert(id, node.encode());

        // Populate in-memory ref tracking.
        let mut refs = self.runtime.refs.lock();
        if refs.ready {
            let children = match node {
                Node::Internal { children, .. } => {
                    for &child in children {
                        if let Some(cr) = refs.counts.get_mut(&child) {
                            cr.ref_count += 1;
                        }
                    }
                    children.clone()
                }
                Node::Leaf { .. } => Vec::new(),
            };
            refs.counts.insert(
                id,
                NodeRef {
                    ref_count: 0,
                    children,
                },
            );
        }

        id
    }

    pub(crate) fn node(&self, id: NodeId) -> Node {
        #[cfg(test)]
        NODE_READS.with(|c| c.set(c.get() + 1));
        // Read-through for nodes allocated earlier in the operation in
        // flight (remove's underflow repair and bulk_load's first_key
        // descend into them).  The emptiness guard keeps the read-only
        // hot path (get / iter) at a single branch — the buffer is only
        // non-empty inside a mutating operation.
        if !self.pending.is_empty()
            && let Some(raw) = self.pending.get(&id)
        {
            return Node::decode(raw);
        }
        let raw = self
            .nodes
            .get(id.to_le_bytes())
            .unwrap_or_else(|| panic!("PersistentBTree: missing node {id}"));
        Node::decode(&raw)
    }

    /// Drains the write buffer into a single engine write batch.
    ///
    /// Called by every public mutating entry point before it returns,
    /// and mid-`bulk_load` at [`Self::PENDING_FLUSH_THRESHOLD`].  An
    /// engine-level commit failure panics, matching the per-put
    /// failure behavior this replaces (mutating signatures are
    /// infallible); nothing from the batch lands in that case.
    pub(crate) fn flush_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let mut batch = self.nodes.batch();
        for (id, raw) in self.pending.drain() {
            batch.insert(id.to_le_bytes(), &raw);
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
        if root == EMPTY_ROOT {
            return None;
        }
        let mut cur = root;
        loop {
            match self.node(cur) {
                Node::Leaf { keys, values } => {
                    return match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                        Ok(i) => Some(values[i].clone()),
                        Err(_) => None,
                    };
                }
                Node::Internal { keys, children } => {
                    cur = children[Self::child_index(&keys, key)];
                }
            }
        }
    }

    /// Returns `true` if `key` exists in the tree rooted at `root`.
    #[inline]
    pub fn contains_key(&self, root: NodeId, key: &[u8]) -> bool {
        self.get(root, key).is_some()
    }

    // =================================================================
    // ITERATION
    // =================================================================

    /// Returns an iterator over **all** entries in ascending key order.
    pub fn iter(&self, root: NodeId) -> BTreeIter<'_> {
        BTreeIter::new(self, root, Bound::Unbounded, Bound::Unbounded)
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
        BTreeIter::new(self, root, lo, hi)
    }

    // =================================================================
    // BULK LOAD
    // =================================================================

    /// Builds a tree from a **pre-sorted** list of `(key, value)` pairs.
    ///
    /// Much faster than inserting one-by-one, and produces an optimally
    /// packed tree.
    ///
    /// The whole input is collected into memory first (validated and
    /// deduplicated), so peak memory is proportional to the total size
    /// of `entries`.
    pub fn bulk_load(
        &mut self,
        entries: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> NodeId {
        let entries: Vec<_> = entries.into_iter().fold(
            Vec::<(Vec<u8>, Vec<u8>)>::new(),
            |mut acc, (k, v)| {
                if let Some((last_k, last_v)) = acc.last_mut() {
                    if k == *last_k {
                        *last_v = v;
                        return acc;
                    }
                    assert!(
                        k > *last_k,
                        "PersistentBTree::bulk_load entries must be sorted by key"
                    );
                }
                acc.push((k, v));
                acc
            },
        );
        if entries.is_empty() {
            return EMPTY_ROOT;
        }
        // Split `total` items into chunks of at most `cap`, rebalancing the
        // trailing pair so no chunk falls below `min` (single-chunk results
        // are exempt — they become the root, which has no minimum).
        // `min <= (cap + 1) / 2` guarantees the rebalanced halves fit.
        fn chunk_sizes(total: usize, cap: usize, min: usize) -> Vec<usize> {
            debug_assert!(min <= cap.div_ceil(2));
            let mut sizes = Vec::with_capacity(total.div_ceil(cap));
            let mut remaining = total;
            while remaining > cap {
                sizes.push(cap);
                remaining -= cap;
            }
            if remaining > 0 {
                sizes.push(remaining);
            }
            let n = sizes.len();
            if n >= 2 && sizes[n - 1] < min {
                // Merge the trailing chunk with its left sibling and split
                // evenly: both halves land in `[min, cap]`.
                let merged = sizes[n - 2] + sizes[n - 1];
                sizes[n - 2] = merged.div_ceil(2);
                sizes[n - 1] = merged / 2;
            }
            sizes
        }

        // 1. Pack into leaves (each `MIN_KEYS..=MAX_KEYS`, INV-BT3).
        let mut leaf_ids = Vec::new();
        let mut off = 0;
        for size in chunk_sizes(entries.len(), MAX_KEYS, MIN_KEYS) {
            let chunk = &entries[off..off + size];
            off += size;
            let keys = chunk.iter().map(|(k, _)| k.clone()).collect();
            let values = chunk.iter().map(|(_, v)| v.clone()).collect();
            leaf_ids.push(self.alloc(&Node::Leaf { keys, values }));
            // Bound the write buffer on larger-than-RAM loads; flushed
            // nodes stay readable through the engine, so `first_key`'s
            // read-back below works across flush boundaries.
            if self.pending.len() >= Self::PENDING_FLUSH_THRESHOLD {
                self.flush_pending();
            }
        }
        // 2. Build internal levels bottom-up. Each internal node gets
        //    `MIN_KEYS + 1 ..= MAX_KEYS + 1` children (uniform height —
        //    a lone or undersized trailing group would either panic later
        //    in remove()'s borrow/merge or violate minimum occupancy).
        let mut level = leaf_ids;
        while level.len() > 1 {
            let mut next = Vec::new();
            let mut i = 0;
            for take in chunk_sizes(level.len(), MAX_KEYS + 1, MIN_KEYS + 1) {
                let chunk = &level[i..i + take];
                let mut keys = Vec::with_capacity(chunk.len() - 1);
                for &cid in &chunk[1..] {
                    keys.push(self.first_key(cid));
                }
                next.push(self.alloc(&Node::Internal {
                    keys,
                    children: chunk.to_vec(),
                }));
                if self.pending.len() >= Self::PENDING_FLUSH_THRESHOLD {
                    self.flush_pending();
                }
                i += take;
            }
            level = next;
        }
        self.flush_pending();
        level[0]
    }

    /// Returns the smallest key reachable from node `id`.
    fn first_key(&self, id: NodeId) -> Vec<u8> {
        let mut cur = id;
        loop {
            match self.node(cur) {
                Node::Leaf { keys, .. } => return keys[0].clone(),
                Node::Internal { children, .. } => cur = children[0],
            }
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
            nr.ref_count += 1;
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
        let immediate = {
            let mut refs = self.runtime.refs.lock();
            let dead_keys = refs
                .release(id)
                .into_iter()
                .filter(|nid| self.pending.remove(nid).is_none())
                .map(|nid| nid.to_le_bytes().to_vec())
                .collect();
            refs.defer_or_return(dead_keys)
        };
        if !immediate.is_empty() {
            self.nodes.lazy_delete_batch(immediate);
        }
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
            debug_assert!(nr.ref_count > 0, "disown_node on unowned node {id}");
            nr.ref_count = nr.ref_count.saturating_sub(1);
        }
    }

    /// Flushes the write buffer once it reaches the bulk threshold, bounding
    /// memory during a buffered multi-step operation.
    pub(crate) fn flush_pending_if_large(&mut self) {
        if self.pending.len() >= Self::PENDING_FLUSH_THRESHOLD {
            self.flush_pending();
        }
    }

    /// Number of internal levels above the leaves (0 for a leaf root).
    pub(crate) fn height(&self, root: NodeId) -> u32 {
        let mut h = 0;
        let mut cur = root;
        while cur != EMPTY_ROOT {
            match self.node(cur) {
                Node::Leaf { .. } => break,
                Node::Internal { children, .. } => {
                    h += 1;
                    cur = children[0];
                }
            }
        }
        h
    }

    /// Rebuilds the in-memory reference-count map from scratch by
    /// walking all nodes reachable from `live_roots` and live snapshot leases.
    ///
    /// Also registers unreachable nodes for deferred disk deletion.
    pub fn rebuild_ref_counts(&mut self, live_roots: &[NodeId]) {
        // A lease must not appear/disappear between pin seeding and the
        // sweep/count replacement. Readers of already-leased nodes need no
        // reference lock and can continue during this maintenance walk.
        let mut refs = self.runtime.refs.lock();
        let mut new_refs: HashMap<NodeId, NodeRef> = HashMap::new();
        let mut visited = HashSet::new();

        // Seed: each root gets +1.
        let mut queue: Vec<NodeId> = Vec::new();
        for &root in live_roots {
            if root != EMPTY_ROOT {
                new_refs
                    .entry(root)
                    .or_insert_with(|| NodeRef {
                        ref_count: 0,
                        children: Vec::new(),
                    })
                    .ref_count += 1;
                queue.push(root);
            }
        }

        for (&root, &pins) in &refs.pins {
            new_refs
                .entry(root)
                .or_insert_with(|| NodeRef {
                    ref_count: 0,
                    children: Vec::new(),
                })
                .ref_count += pins;
            queue.push(root);
        }

        // BFS: walk all reachable nodes, count parent→child references.
        while let Some(id) = queue.pop() {
            if !visited.insert(id) {
                continue;
            }
            if let Some(raw) = self.nodes.get(id.to_le_bytes()) {
                let node = Node::decode(&raw);
                let children = match &node {
                    Node::Internal { children, .. } => {
                        for &child in children {
                            new_refs
                                .entry(child)
                                .or_insert_with(|| NodeRef {
                                    ref_count: 0,
                                    children: Vec::new(),
                                })
                                .ref_count += 1;
                            queue.push(child);
                        }
                        children.clone()
                    }
                    Node::Leaf { .. } => Vec::new(),
                };
                new_refs
                    .entry(id)
                    .or_insert_with(|| NodeRef {
                        ref_count: 0,
                        children: Vec::new(),
                    })
                    .children = children;
            }
        }

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
                let id = u64::from_le_bytes(k[..8].try_into().unwrap());
                max_id = max_id.max(id);
                (!visited.contains(&id)).then_some(k)
            })
            .collect();
        if !dead_keys.is_empty() {
            self.nodes.lazy_delete_batch(dead_keys);
        }
        self.runtime.next_id.fetch_max(
            max_id
                .checked_add(1)
                .expect("PersistentBTree: NodeId space exhausted"),
            Ordering::AcqRel,
        );

        refs.counts = new_refs;
        refs.ready = true;
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
            if let Node::Internal { children, .. } = self.node(id) {
                for c in children {
                    *expected.entry(c).or_insert(0) += 1;
                    queue.push(c);
                }
            }
        }
        let refs = self.runtime.refs.lock();
        assert!(refs.ready);
        let actual: HashMap<NodeId, u32> = refs
            .counts
            .iter()
            .map(|(&id, nr)| (id, nr.ref_count))
            .collect();
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
        debug_assert!(
            self.pending.is_empty(),
            "PersistentBTree cloned with a non-empty write buffer"
        );
        let nodes = self.nodes.clone();
        let refs = self.runtime.refs.lock();
        let mut counts = refs.counts.clone();
        // Reader leases belong to the original pool. Any resulting zero-ref
        // root remains unowned, as with standalone mutation results; VerMap's
        // clone recounts from its copied persistent graph before returning.
        for (&root, &pins) in &refs.pins {
            let count = &mut counts.get_mut(&root).expect("untracked pin").ref_count;
            *count = count
                .checked_sub(pins)
                .expect("pin count exceeds references");
        }
        let runtime = tree_runtime(
            nodes.instance_id(),
            self.runtime.next_id.load(Ordering::Acquire),
            RefState {
                counts,
                ready: refs.ready,
                pins: HashMap::new(),
                deferred: None,
            },
        );
        Self {
            nodes,
            runtime,
            pending: HashMap::new(),
        }
    }
}
