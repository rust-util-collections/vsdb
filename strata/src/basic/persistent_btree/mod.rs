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

mod insert;
mod iter;
mod nodes;
mod remove;
mod types;

pub use iter::BTreeIter;

use crate::common::{InstanceId, error::Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    ops::Bound,
    result::Result as StdResult,
};
use vsdb_core::basic::mapx_raw::MapxRaw;

pub(crate) use nodes::{MAX_KEYS, MIN_KEYS, Node};
pub use types::{EMPTY_ROOT, NodeId};
pub(crate) use types::{InsertResult, LeafState, NodeRef, RemoveResult};

// =========================================================================
// PersistentBTree
// =========================================================================

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
/// use vsdb::vsdb_set_base_dir;
/// use std::fs;
///
/// let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
/// vsdb_set_base_dir(&dir).unwrap();
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
#[derive(Clone, Debug)]
pub struct PersistentBTree {
    /// Flat node pool.  Key = little-endian NodeId, Value = encoded Node.
    pub(crate) nodes: MapxRaw,
    /// Next node ID to allocate (monotonically increasing).
    next_id: NodeId,
    /// In-memory reference counts and cached children lists.
    /// Rebuilt from disk by [`rebuild_ref_counts`].
    pub(crate) ref_counts: HashMap<NodeId, NodeRef>,
    /// Whether `ref_counts` has been populated (false after deserialization).
    pub(crate) ref_counts_ready: bool,
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
        t.serialize_element(&self.next_id)?;
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
                Ok(PersistentBTree {
                    nodes,
                    next_id,
                    ref_counts: Default::default(),
                    ref_counts_ready: false,
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
        crate::common::load_instance_meta(instance_id.into())
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
        Self {
            nodes: MapxRaw::new(),
            next_id: 1, // 0 is EMPTY_ROOT sentinel
            ref_counts: HashMap::new(),
            ref_counts_ready: true, // empty tree — nothing to rebuild
            pending: HashMap::new(),
        }
    }

    // ----- low-level helpers -----

    /// Number of buffered nodes above which `bulk_load` flushes
    /// mid-operation, bounding the write buffer's memory footprint
    /// (the same larger-than-RAM hazard `Mapx::clear`/`Clone` chunk
    /// against).  Path-copying operations (`insert`/`remove`) stay far
    /// below this — they buffer O(depth) nodes.
    const PENDING_FLUSH_THRESHOLD: usize = 1024;

    fn alloc(&mut self, node: &Node) -> NodeId {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("PersistentBTree: NodeId space exhausted");
        debug_assert!(
            !self.pending.contains_key(&id)
                && self.nodes.get(id.to_le_bytes()).is_none(),
            "PersistentBTree: NodeId {id} already occupied — allocator regression"
        );
        self.pending.insert(id, node.encode());

        // Populate in-memory ref tracking.
        if self.ref_counts_ready {
            let children = match node {
                Node::Internal { children, .. } => {
                    for &child in children {
                        if let Some(cr) = self.ref_counts.get_mut(&child) {
                            cr.ref_count += 1;
                        }
                    }
                    children.clone()
                }
                Node::Leaf { .. } => Vec::new(),
            };
            self.ref_counts.insert(
                id,
                NodeRef {
                    ref_count: 0,
                    children,
                },
            );
        }

        id
    }

    fn node(&self, id: NodeId) -> Node {
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
    fn flush_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let mut batch = self.nodes.batch_entry();
        for (id, raw) in self.pending.drain() {
            batch.insert(&id.to_le_bytes(), &raw);
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
        if id == EMPTY_ROOT || !self.ref_counts_ready {
            return;
        }
        if let Some(nr) = self.ref_counts.get_mut(&id) {
            nr.ref_count += 1;
        }
    }

    /// Decrements the in-memory reference count for `id`.
    /// If it reaches zero, cascades to all children, removes the entry
    /// from the in-memory map, and registers the node for deferred disk
    /// deletion via the storage engine's compaction filter.
    pub fn release_node(&mut self, id: NodeId) {
        if id == EMPTY_ROOT || !self.ref_counts_ready {
            return;
        }
        // Callers (VerMap) only release between tree operations; the
        // write buffer must already be drained, otherwise the cascade
        // below would lazy-delete keys the buffer has not written yet.
        debug_assert!(
            self.pending.is_empty(),
            "release_node called with a non-empty write buffer"
        );
        let mut dead_keys = Vec::new();
        let mut work = vec![id];
        while let Some(nid) = work.pop() {
            if nid == EMPTY_ROOT {
                continue;
            }
            let Some(nr) = self.ref_counts.get_mut(&nid) else {
                continue;
            };
            debug_assert!(
                nr.ref_count > 0,
                "release_node called on node {nid} with ref_count=0"
            );
            if nr.ref_count == 0 {
                continue;
            }
            nr.ref_count -= 1;
            if nr.ref_count == 0 {
                let children = std::mem::take(&mut nr.children);
                self.ref_counts.remove(&nid);
                dead_keys.push(nid.to_le_bytes().to_vec());
                work.extend(children);
            }
        }
        if !dead_keys.is_empty() {
            self.nodes.lazy_delete_batch(dead_keys);
        }
    }

    /// Rebuilds the in-memory reference-count map from scratch by
    /// walking all nodes reachable from `live_roots`.
    ///
    /// Also registers unreachable nodes for deferred disk deletion.
    pub fn rebuild_ref_counts(&mut self, live_roots: &[NodeId]) {
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
        self.next_id = self.next_id.max(
            max_id
                .checked_add(1)
                .expect("PersistentBTree: NodeId space exhausted"),
        );

        self.ref_counts = new_refs;
        self.ref_counts_ready = true;
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
    /// - **Crash recovery** — when `ref_counts_ready` is false after
    ///   deserialization or an interrupted cascade.
    /// - **Forced full sweep** — when you want to guarantee that every
    ///   unreachable node is registered, even if a prior `release_node`
    ///   cascade was incomplete.
    pub fn gc(&mut self, live_roots: &[NodeId]) {
        self.rebuild_ref_counts(live_roots);
    }
}

impl Default for PersistentBTree {
    fn default() -> Self {
        Self::new()
    }
}
