//!
//! A raw, disk-based, directed acyclic graph (DAG) map.
//!
//! `DagMapRaw` provides a map-like interface where each instance can have a parent
//! and multiple children, forming a directed acyclic graph. This is useful for
//! representing versioned or hierarchical data.
//!
//! # Examples
//!
//! ```
//! use vsdb::DagMapRaw;
//! use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut dag = DagMapRaw::new(None);
//!
//! // Insert a value
//! dag.insert(&[1], &[10]);
//! assert_eq!(dag.get(&[1]), Some(vec![10]));
//!
//! // Create a child
//! let child = DagMapRaw::new(Some(&mut dag));
//! assert_eq!(child.get(&[1]), Some(vec![10]));
//!
//! // Clean up the directory
//! fs::remove_dir_all(vsdb_get_base_dir()).unwrap();
//! ```

#[cfg(test)]
mod test;

use crate::{
    DagMapId, MapxOrdRawKey, Orphan,
    common::{
        InstanceId,
        error::{Result, VsdbError},
    },
};
use serde::{Deserialize, Serialize, de};
use std::{
    collections::HashSet,
    fmt,
    ops::{Deref, DerefMut},
    result::Result as StdResult,
};
use vsdb_core::{
    basic::mapx_raw::{self, MapxRaw},
    common::RawBytes,
};

type DagHead = DagMapRaw;

/// A raw, disk-based, directed acyclic graph (DAG) map.
///
/// Deliberately does **not** implement [`Default`]: every `DagMapRaw`
/// is a real, disk-backed structure — `Orphan::new()`'s eager write for
/// the `parent` slot means construction always allocates engine
/// prefixes and performs a disk write.  A derived or `new(None)`-backed
/// `Default` would make that cost invisible at every call site that
/// generically constructs "an empty value" (`Option::unwrap_or_default()`,
/// `HashMap::entry().or_default()`, and especially `std::mem::take`,
/// whose entire idiom relies on `Default` being a cheap, side-effect-free
/// placeholder) — silently creating orphaned, unreachable disk state one
/// call at a time. Use [`Self::new`] explicitly instead.
#[derive(Clone, Debug)]
pub struct DagMapRaw {
    data: MapxRaw,

    // Owned by this node.  Holds an aliasing handle of the parent node
    // (or `None` for a root), so `destroy()` can unlink persistently
    // without affecting siblings.
    parent: Orphan<Option<DagMapRaw>>,

    // child id --> child instance
    children: MapxOrdRawKey<DagMapRaw>,
}

impl Serialize for DagMapRaw {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple;
        let mut t = serializer.serialize_tuple(3)?;
        t.serialize_element(&self.data)?;
        t.serialize_element(&self.parent)?;
        t.serialize_element(&self.children)?;
        t.end()
    }
}

impl<'de> Deserialize<'de> for DagMapRaw {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Vis;
        impl<'de> de::Visitor<'de> for Vis {
            type Value = DagMapRaw;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("DagMapRaw")
            }
            fn visit_seq<A: de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> StdResult<DagMapRaw, A::Error> {
                let data: MapxRaw = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let parent: Orphan<Option<DagMapRaw>> = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let children: MapxOrdRawKey<DagMapRaw> = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let ns = data.namespace().id();
                if parent.namespace().id() != ns || children.namespace().id() != ns {
                    return Err(de::Error::custom(
                        "DagMapRaw components belong to different namespaces",
                    ));
                }
                Ok(DagMapRaw {
                    data,
                    parent,
                    children,
                })
            }
        }
        deserializer.deserialize_tuple(3, Vis)
    }
}

impl DagMapRaw {
    /// [`new`](Self::new) placed in `ns` — every internal component
    /// lands in the same namespace (a composite never spans namespaces).
    ///
    /// With a parent, the child ALWAYS inherits the parent's namespace
    /// (one DAG = one namespace); passing a different `ns` here is a
    /// caller bug (`debug_assert`ed, ignored in release).
    pub fn new_in(ns: &crate::common::Namespace, parent: Option<&mut Self>) -> Self {
        if let Some(p) = &parent {
            debug_assert_eq!(
                ns.id(),
                p.namespace().id(),
                "a DAG never spans namespaces: child must live in its \
                 parent's namespace"
            );
        }
        ns.scope(|| Self::new(parent))
    }

    /// The namespace this structure lives in.
    pub fn namespace(&self) -> crate::common::Namespace {
        self.data.namespace()
    }

    /// Creates a new `DagMapRaw`, optionally attached under `parent`.
    ///
    /// The node stores an aliasing handle of the parent node in its own
    /// (per-node) parent slot: later mutations of the parent's data are
    /// visible to this node when resolving inherited reads.  Do not
    /// attach a node under one of its own descendants — parent chains
    /// are cycle-guarded, so lookups on such a graph degrade to `None`
    /// (and `prune` reports an error) instead of hanging, but the graph
    /// itself is logically invalid.
    ///
    /// Parented construction inherits the **parent's namespace**,
    /// overriding any ambient scope: one DAG never spans namespaces
    /// (destroying/relocating either side would otherwise leave
    /// dangling cross-engine references).
    pub fn new(parent: Option<&mut Self>) -> Self {
        match parent {
            Some(p) => {
                let ns = p.namespace();
                ns.scope(|| Self::build(Some(p)))
            }
            None => Self::build(None),
        }
    }

    fn build(parent: Option<&mut Self>) -> Self {
        // Fields are constructed explicitly: `..Default::default()` would
        // materialize (and immediately discard) a default `parent` Orphan,
        // which eagerly writes an entry under a fresh engine prefix —
        // permanently leaking one storage slot per node creation.
        let r = Self {
            // SAFETY: The shadow is serialized (read-only) into the
            // node's own parent slot right here; no mutation ever goes
            // through it (all writes go through the caller's handle),
            // satisfying the SWMR contract.  Note: `Clone` cannot be
            // used — it deep-copies the storage instead of aliasing it.
            parent: Orphan::new(parent.as_deref().map(|p| unsafe { p.shadow() })),
            data: MapxRaw::new(),
            children: MapxOrdRawKey::new(),
        };

        if let Some(p) = parent {
            let child_id = super::gen_dag_map_id_num().to_le_bytes();
            // gen_dag_map_id_num() is monotonically increasing, so
            // duplicate IDs are impossible under normal operation.
            // The assertion guards against ID counter corruption
            // (e.g. crash-induced rollback).
            debug_assert!(
                p.children.get(child_id).is_none(),
                "Child ID already exists — possible ID counter rollback"
            );
            p.children.insert(child_id, &r);
        }

        r
    }

    /// Creates a second handle to the same underlying storage.
    ///
    /// # Safety
    ///
    /// The caller must ensure no concurrent writes to the same key
    /// through any handle.  Multiple writers on disjoint keys are safe.
    /// Note: structural mutations (`destroy`, reparenting) have
    /// cross-key side effects — only one such operation at a time.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        // SAFETY: forwards this fn's `unsafe` contract — the caller
        // guarantees no concurrent writes to the same key (and only one
        // structural mutation at a time), per the doc comment above.
        unsafe {
            Self {
                data: self.data.shadow(),
                parent: self.parent.shadow(),
                children: self.children.shadow(),
            }
        }
    }

    /// Returns the unique instance ID of this `DagMapRaw`.
    #[inline(always)]
    pub fn instance_id(&self) -> InstanceId {
        self.data.instance_id()
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

    /// Recovers a `DagMapRaw` instance from previously saved metadata.
    ///
    /// The caller must ensure that the underlying VSDB database still
    /// contains the data referenced by this instance ID.
    pub fn from_meta(instance_id: impl Into<InstanceId>) -> Result<Self> {
        let id = instance_id.into();
        crate::common::load_instance_meta_checked(id, Self::instance_id)
    }

    /// Checks if the DAG map is dead (i.e., has no live data, parent, or children).
    ///
    /// `remove()` writes an empty-value tombstone rather than deleting
    /// the entry outright (see [`Self::insert`]'s doc comment), so a
    /// plain "is the backing store empty" check would incorrectly
    /// return `false` for a node whose only key was removed. This
    /// matches [`Self::get`]/[`Self::get_mut`]'s convention of treating
    /// an empty value as absent.
    #[inline(always)]
    pub fn is_dead(&self) -> bool {
        self.data.iter().all(|(_, v)| v.is_empty())
            && self.parent.get_value().is_none()
            && self.no_children()
    }

    /// Checks if the DAG map has no children.
    #[inline(always)]
    pub fn no_children(&self) -> bool {
        self.children.inner.iter().next().is_none()
    }

    /// Returns the internal registry IDs of all direct children.
    ///
    /// These are the IDs accepted by
    /// [`prune_children_include`](Self::prune_children_include) and
    /// [`prune_children_exclude`](Self::prune_children_exclude); they are
    /// distinct from each child's storage [`InstanceId`].
    pub fn child_ids(&self) -> Vec<RawBytes> {
        self.children.keys().collect()
    }

    /// Returns this parent's registry ID for `child`, if it is currently
    /// registered as a direct child.
    pub fn child_id(&self, child: &Self) -> Option<RawBytes> {
        let child = child.instance_id();
        self.children
            .iter()
            .find_map(|(id, entry)| (entry.instance_id() == child).then_some(id))
    }

    /// Retrieves a value from the DAG map, traversing up to the parent if necessary.
    ///
    /// The traversal tracks visited instance IDs to avoid looping forever
    /// if on-disk metadata is corrupt and contains a parent cycle.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        let key = key.as_ref();

        let mut hdr = self;
        let mut hdr_owned;
        let mut seen = HashSet::new();

        loop {
            if !seen.insert(hdr.instance_id()) {
                return None;
            }
            if let Some(v) = hdr.data.get(key) {
                return if v.is_empty() { None } else { Some(v) };
            }
            match hdr.parent.get_value() {
                Some(p) => {
                    hdr_owned = p;
                    hdr = &hdr_owned;
                }
                _ => {
                    return None;
                }
            }
        }
    }

    /// Retrieves a mutable reference to a value in this node's local data.
    ///
    /// Unlike [`get`](Self::get), this does **not** traverse parent links.
    /// Returns `None` if the key is not in this node's own storage, even if
    /// a parent would return it via `get`.
    #[inline(always)]
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_>> {
        self.data.get_mut(key.as_ref()).and_then(|inner| {
            if inner.is_empty() {
                return None;
            }
            Some(ValueMut {
                value: inner.clone(),
                inner,
                dirty: false,
            })
        })
    }

    /// Inserts a key-value pair into the DAG map.
    ///
    /// Does not return the old value for performance reasons.
    ///
    /// # Panics
    ///
    /// Panics if `value` is empty — an empty byte slice is used
    /// internally as a deletion tombstone.  Use [`remove`](Self::remove)
    /// to delete a key instead.
    #[inline(always)]
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        assert!(
            !value.as_ref().is_empty(),
            "empty value is a tombstone; call remove() instead"
        );
        self.data.insert(key.as_ref(), value)
    }

    /// Removes a key-value pair from the DAG map.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) {
        self.data.insert(key.as_ref(), [])
    }

    /// Prunes the DAG, merging all nodes in the mainline into the genesis node.
    ///
    /// Returns the new head of the mainline — the genesis node, which keeps
    /// its instance ID, so metadata saved for the genesis *before* the prune
    /// still resolves to the merged result afterwards.
    ///
    /// # Crash safety
    ///
    /// The prune is ordered as **merge → flush → re-parent → flush → clear**:
    /// nothing is cleared before the genesis holds the complete merged state
    /// and every surviving child has been re-pointed at it (the two
    /// [`Namespace::flush`](crate::common::Namespace::flush) barriers, scoped
    /// to this DAG's own namespace, pin that ordering across the engine's
    /// independently-recovered shards).  Because overlay reads resolve
    /// top-down, the in-place enrichment of the genesis is invisible through
    /// the head, so a crash (e.g. `kill -9` or power loss) at **any** point
    /// leaves the canonical access paths — the returned head / the genesis
    /// (including its pre-prune metadata) and the head's children —
    /// value-exact: they observe either the complete pre-prune state or the
    /// complete post-prune state, never a torn mix.
    ///
    /// The head and the intermediate mainline nodes are *consumed* by the
    /// prune (this is the ordinary, non-crash contract as well): handles or
    /// saved metadata still pointing at them may observe partially cleared
    /// nodes after a crash and must not be used.  Storage left behind by an
    /// interrupted prune remains reachable through the genesis' children
    /// registry and is reclaimed by the next prune's side-branch
    /// destruction — a crash costs at most temporarily leaked space, never
    /// corruption.
    #[inline(always)]
    pub fn prune(self) -> Result<DagHead> {
        self.prune_mainline()
    }

    // Return the new head of mainline (the genesis node).
    //
    // The phases below are deliberately ordered so that every crash point
    // leaves the canonical view (genesis + surviving children) value-exact;
    // see `prune` for the externally visible contract and each phase
    // method for the per-phase argument.
    fn prune_mainline(mut self) -> Result<DagHead> {
        // Phase 0 (read-only): collect the mainline chain.
        let mut linebuf = self.prune_collect_mainline()?;
        if linebuf.is_empty() {
            return Ok(self);
        }

        // Instance IDs on the mainline path — must not be destroyed.
        let mainline_ids: Vec<InstanceId> = {
            let mut ids = vec![self.instance_id()];
            ids.extend(linebuf.iter().map(|n| n.instance_id()));
            ids
        };

        // The head's children are survivors (re-parented in phase 3).
        // An earlier interrupted prune may have left them
        // double-registered under the genesis already — phase 1 must
        // not mistake those registry copies for genesis side branches.
        let pending_reparent: HashSet<RawBytes> =
            self.children.iter().map(|(id, _)| id).collect();

        // Phase 1: side branches are doomed by the prune contract — kill
        // them while the mainline is still fully intact.
        Self::prune_destroy_side_branches(
            &mut linebuf,
            &mainline_ids,
            &pending_reparent,
        );

        // Phase 2: fold the whole mainline into the genesis WITHOUT
        // clearing anything (read-transparent, idempotent).
        self.prune_merge_into_genesis(&mut linebuf);

        // Barrier A: the merged genesis must be durable before any child
        // is re-pointed at it. Writes to different shards *within this
        // DAG's namespace* recover independently, so ordering across
        // prefixes needs an explicit flush — scoped to this namespace
        // (a DAG never spans namespaces, see `new_in`), not the whole
        // process: a transient flush failure in some unrelated open
        // namespace must not be able to panic a prune() on a healthy one.
        self.namespace().flush();

        // Phase 3: re-point the head's children at the merged genesis.
        let kept = self.prune_reparent_children(linebuf.last_mut().unwrap());

        // Barrier B: all pointer flips must be durable before the old
        // chain is torn down.
        self.namespace().flush();

        // Phases 4-5: clear the consumed nodes (head first, then
        // intermediates newest → oldest).
        self.prune_clear_consumed(&mut linebuf);

        // Barrier C: the consumed nodes' data/parent clears must be
        // durable before phase 6 removes their last discoverability
        // entries from the genesis registry.
        self.namespace().flush();

        // Phase 6: drop the now-dead mainline entry from the genesis'
        // children registry (side branches were already destroyed in
        // phase 1; the ownership check inside skips foreign residue).
        let mut genesis = linebuf.pop().unwrap();
        genesis.prune_children_exclude(&kept);

        Ok(genesis)
    }

    /// Prune phase 0 (read-only): walk the parent chain and return
    /// `[parent, grandparent, ..., genesis]`, or an empty vector when
    /// `self` is parentless.  Fails on parent cycles.
    fn prune_collect_mainline(&self) -> Result<Vec<Self>> {
        let p = match self.parent.get_value() {
            Some(p) => p,
            _ => return Ok(vec![]),
        };

        let mut seen = HashSet::new();
        seen.insert(self.instance_id());
        let mut linebuf = vec![p];
        loop {
            let current_id = linebuf.last().unwrap().instance_id();
            if !seen.insert(current_id) {
                return Err(VsdbError::Other {
                    detail: "DAG mainline contains a parent cycle".to_owned(),
                });
            }
            match linebuf.last().unwrap().parent.get_value() {
                Some(p) => linebuf.push(p),
                None => break,
            }
        }
        Ok(linebuf)
    }

    /// Prune phase 1: destroy every non-mainline child (side branch) of
    /// every chain node, genesis included.
    ///
    /// This must happen **before** the genesis is enriched in phase 2: a
    /// live branch forked below the topmost holder of a key would observe
    /// the in-place merge.  Destroying branches first keeps every read
    /// exact until the branch dies — and dying is the prune contract.
    ///
    /// Crash mid-phase: the mainline is untouched; a partially destroyed
    /// branch is an intended deletion that the next prune completes.
    fn prune_destroy_side_branches(
        linebuf: &mut [Self],
        mainline_ids: &[InstanceId],
        pending_reparent: &HashSet<RawBytes>,
    ) {
        for node in linebuf.iter_mut() {
            let node_id = node.instance_id();
            // Collect first — destroy() unlinks entries from
            // `node.children` while we would be iterating it.
            let doomed: Vec<_> = node
                .children
                .iter()
                .filter(|(id, child)| {
                    !mainline_ids.contains(&child.instance_id())
                        && !pending_reparent.contains(id)
                })
                .collect();
            for (id, mut child) in doomed {
                // Registry entries are only an index; the child's own
                // parent slot is the ownership truth.  A child owned by a
                // *different* live parent (e.g. one already re-pointed at
                // the genesis by an interrupted prune, still listed under
                // the old head) must not be destroyed through the stale
                // registry copy — only the index entry is dropped.
                if Self::owned_or_residue(node_id, &child) {
                    child.destroy();
                }
                node.children.remove(&id);
            }
        }
    }

    // Ownership test used by every registry-driven destruction walk.
    //
    // `child` may be destroyed on behalf of the node whose instance ID is
    // `owner_id` iff its own parent slot still points back at that node
    // (owned), or is `None` (residue of an interrupted clear — live roots
    // are never listed in any children registry, so a parentless entry is
    // always reclaimable).  A child owned by a *different* live parent is
    // foreign: its registry entry is a stale index copy.
    fn owned_or_residue(owner_id: InstanceId, child: &Self) -> bool {
        match child.parent.get_value() {
            None => true,
            Some(p) => p.instance_id() == owner_id,
        }
    }

    /// Prune phase 2: fold every mainline node's data into the genesis,
    /// oldest → newest, clearing **nothing**.
    ///
    /// In-place enrichment of the genesis is invisible to reads through
    /// the head: overlay resolution stops at the topmost holder of a key,
    /// and every key written here still has its holder above the genesis.
    /// Tombstones are elided (the genesis is parentless, so absence is
    /// equivalent) — equally invisible, since the tombstone-bearing node
    /// still shadows the key.  Re-running the fold after a crash replays
    /// the same sequence and converges to the same merged state.
    fn prune_merge_into_genesis(&self, linebuf: &mut [Self]) {
        let mid = linebuf.len() - 1;
        let (others, genesis) = linebuf.split_at_mut(mid);
        let genesis = &mut genesis[0];

        for i in others.iter().rev() {
            Self::prune_fold_node(genesis, i);
        }
        Self::prune_fold_node(genesis, self);
    }

    // Merge one mainline node's data into the genesis node.
    fn prune_fold_node(genesis: &mut Self, src: &Self) {
        for (k, v) in src.data.iter() {
            // The genesis node is parentless, so a tombstone there is
            // equivalent to the key being absent — drop tombstones
            // instead of accumulating dead entries forever.
            if v.is_empty() {
                genesis.data.remove(&k);
            } else {
                genesis.data.insert(k, v);
            }
        }
    }

    /// Prune phase 3: re-point every child of the head at the (fully
    /// merged) genesis and register it there.
    ///
    /// Each flip is a single-slot write and is value-exact on both sides:
    /// `child → old chain` and `child → merged genesis` resolve every key
    /// identically because phase 2 completed first.  A crash between
    /// flips therefore leaves every child on a correct view, and the
    /// whole phase is idempotent.
    ///
    /// Returns the IDs of the re-parented children (the survivors of the
    /// genesis' children registry).
    fn prune_reparent_children(&mut self, genesis: &mut Self) -> Vec<RawBytes> {
        let mut kept = vec![];
        for (id, mut child) in self.children.iter_mut() {
            // Reparent: the child's owned parent slot now aliases the
            // genesis node.
            // SAFETY: The shadow is serialized (read-only) immediately in
            // the following two insert calls; no mutation occurs through
            // it (all writes go through `genesis`), satisfying the SWMR
            // contract.  `Clone` cannot be used — it deep-copies the
            // storage instead of aliasing it.
            *child.parent.get_mut() = Some(unsafe { genesis.shadow() });
            genesis.children.insert(&id, &child);
            kept.push(id);
        }
        kept
    }

    /// Prune phases 4-5: clear the consumed nodes — the head first, then
    /// the intermediates newest → oldest.
    ///
    /// Per-node order: **parent → children → data**. Nulling the head's
    /// parent first makes interrupted-prune re-entry structurally safe:
    /// before any clearing starts, re-running `prune` on the head is a
    /// complete, convergent re-run (refold + idempotent flips); the
    /// instant clearing starts, the head is parentless and a re-run is
    /// refused by the early return — a re-fold against a half-cleared
    /// head (which would resurrect older values over the merged genesis)
    /// is impossible.
    ///
    /// Self-healing invariant: whenever a node still holds data, it is
    /// reachable through the not-yet-cleared children registry of the
    /// next-older mainline node (head-side nodes are cleared first) and
    /// its parent slot is either intact or `None` — both reclaimable by
    /// the ownership rule ([`owned_or_residue`](Self::owned_or_residue)),
    /// so the next prune's side-branch destruction sweeps the residue.
    /// The caller flushes immediately after this phase, before phase 6
    /// unregisters the consumed chain.
    fn prune_clear_consumed(&mut self, linebuf: &mut [Self]) {
        let mid = linebuf.len() - 1;
        let others = &mut linebuf[..mid];

        *self.parent.get_mut() = None;
        self.children.clear();
        self.data.clear();

        for i in others.iter_mut() {
            *i.parent.get_mut() = None;
            i.children.clear();
            i.data.clear();
        }
    }

    /// Prunes children that are in the `include_targets` list.
    #[inline(always)]
    pub fn prune_children_include(&mut self, include_targets: &[impl AsRef<DagMapId>]) {
        self.prune_children(include_targets, false);
    }

    /// Prunes children that are not in the `exclude_targets` list.
    #[inline(always)]
    pub fn prune_children_exclude(&mut self, exclude_targets: &[impl AsRef<DagMapId>]) {
        self.prune_children(exclude_targets, true);
    }

    fn prune_children(&mut self, targets: &[impl AsRef<DagMapId>], exclude_mode: bool) {
        let self_id = self.instance_id();
        let targets = targets.iter().map(|i| i.as_ref()).collect::<HashSet<_>>();

        let dropped_children = if exclude_mode {
            self.children
                .iter()
                .filter(|(id, _)| !targets.contains(&id.as_slice()))
                .collect::<Vec<_>>()
        } else {
            self.children
                .iter()
                .filter(|(id, _)| targets.contains(&id.as_slice()))
                .collect::<Vec<_>>()
        };

        // Destroy each owned child fully — `destroy()` clears it (and its
        // descendants) before removing its own registry entry — and only
        // THEN drop the index entry here. Removing every entry upfront
        // (the previous ordering) let a crash strand not-yet-destroyed
        // children with fully intact data and no registry path back to
        // them; per-child destroy-then-drop keeps every not-yet-processed
        // entry discoverable through this still-intact registry.
        for (id, mut child) in dropped_children.into_iter() {
            // Only destroy children this node actually owns (or parentless
            // residue); an entry whose child now lives under a different
            // parent is a stale index copy left by an interrupted
            // multi-step operation — dropping the entry below suffices.
            if Self::owned_or_residue(self_id, &child) {
                child.destroy();
            }
            self.children.remove(&id);
        }
    }

    /// Destroys this node and all its **owned** descendant children,
    /// clearing all data.
    ///
    /// If this node is attached to a parent, it persistently nulls its own
    /// parent slot and clears its data and descendants **first**, and only
    /// as the **last** step removes its own child entry from the parent's
    /// `children` collection. Because both the data clearing and the
    /// parent unlink are persisted, **every** handle of this node —
    /// including clones taken earlier and handles later restored via
    /// [`from_meta`](Self::from_meta) — observes the destroyed state and
    /// can no longer resolve inherited reads through the parent chain.
    ///
    /// # Crash safety
    ///
    /// This ordering mirrors [`prune_clear_consumed`](Self::prune_clear_consumed)'s
    /// self-healing pattern: nothing is unregistered from the parent's
    /// registry until this node (and everything beneath it) is already
    /// fully cleared. Each step is an independent write to a different
    /// engine prefix that can recover independently after a crash — if the
    /// registry removal happened *first* (the previous, unsafe ordering),
    /// a crash right after it becomes durable but before the data/parent
    /// clearing lands would make this node permanently unreachable via any
    /// registry-driven walk while its storage remained fully allocated on
    /// disk (a leak with no recovery path, since discovery is entirely
    /// registry-driven). With the registry entry removed last, a crash at
    /// any earlier point still leaves this node discoverable — and safely
    /// reclaimable, since [`owned_or_residue`](Self::owned_or_residue)'s
    /// residue arm accepts a `None` parent slot — through the parent's
    /// still-intact registry entry, so a retried `destroy()` or a
    /// subsequent `prune()` converges instead of leaking.
    ///
    /// A namespace flush separates the data/parent pass from registry
    /// clearing, making this ordering durable across independent shard WALs.
    ///
    /// The descendant walk follows the children registries but treats each
    /// child's own parent slot as the ownership truth: an entry whose child
    /// meanwhile lives under a different parent (a stale index copy left by
    /// an interrupted multi-step operation such as [`prune`](Self::prune))
    /// is skipped, never destroyed through the stale path.
    #[inline(always)]
    pub fn destroy(&mut self) {
        let self_id = self.instance_id();

        // Captured now (before nulling below) so the parent's registry
        // can be updated as the LAST step, once this node is fully
        // cleared — see "Crash safety" above.
        let parent = self.parent.get_value();

        // The parent slot is owned by this node (never shared with
        // siblings), so nulling it is a persistent, node-local unlink.
        *self.parent.get_mut() = None;
        self.data.clear();

        // Clear all owned descendants iteratively. A recursive walk
        // overflows the stack on deep DAGs; mirror the iterative,
        // cycle-guarded design used by `get()` and `prune_mainline`.
        // Each stack entry carries the instance ID of the registry owner
        // it was discovered under, for the ownership check on pop.
        //
        // Two passes, in this order, are required for the same crash
        // safety reason as above: pass 1 clears every owned descendant's
        // `data`/`parent` but leaves every `children` registry (including
        // `self.children`) untouched, so the not-yet-visited tail of
        // `stack` stays fully discoverable through its immediate
        // ancestor's still-intact registry for the whole pass. Only once
        // EVERY descendant's data is already gone does pass 2 clear the
        // (by then purely cosmetic — no live data can be lost through
        // them any more) children registries; interrupting pass 2 can
        // leave stale-but-harmless entries pointing at already-empty
        // nodes, never a loss. The previous single-pass version cleared
        // each node's `children` immediately after reading it, so a
        // crash between that clear and the next stack pop orphaned
        // still-intact grandchildren with no recovery path.
        let mut seen = HashSet::new();
        seen.insert(self_id);
        let mut stack = self
            .children
            .iter()
            .map(|(_, c)| (self_id, c))
            .collect::<Vec<_>>();

        let mut owned = Vec::new();
        while let Some((owner_id, mut node)) = stack.pop() {
            // Ownership FIRST, duplicate-suppression second: a foreign
            // entry (stale index copy owned by a different live parent)
            // must not poison `seen` — otherwise the true owned entry
            // for the same node, popped later, would be skipped and the
            // node would survive destruction (INV-DG5).
            if !Self::owned_or_residue(owner_id, &node) {
                // Foreign entry — not ours to destroy.
                continue;
            }
            if !seen.insert(node.instance_id()) {
                continue;
            }
            let node_id = node.instance_id();
            node.data.clear();
            // Break the upward traversal link so a handle to any
            // descendant cannot walk through the cleared node to
            // ancestors above the destroyed subtree.
            *node.parent.get_mut() = None;
            stack.extend(node.children.iter().map(|(_, c)| (node_id, c)));
            owned.push(node);
        }

        // Data and parent slots may live on different shards from their
        // registries. Make every destructive clear durable before any
        // registry entry can disappear on a later independently-recovered
        // WAL.
        self.namespace().flush();

        // Pass 2: every owned descendant's data is already gone, so
        // clearing the children registries in any order — even if
        // interrupted — can no longer lose live data.
        self.children.clear();
        for mut node in owned {
            node.children.clear();
        }

        // Only now — after `self` and everything beneath it is fully
        // cleared — unregister `self` from the parent's registry.
        if let Some(mut parent) = parent {
            let child_ids = parent
                .children
                .iter()
                .filter_map(|(id, child)| (child.instance_id() == self_id).then_some(id))
                .collect::<Vec<_>>();
            for id in child_ids {
                parent.children.remove(id);
            }
        }
    }

    /// Checks if this `DagMapRaw` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.data.is_the_same_instance(&other_hdr.data)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable reference to a value in a `DagMapRaw`.
#[derive(Debug)]
pub struct ValueMut<'a> {
    value: RawBytes,
    inner: mapx_raw::ValueMut<'a>,
    dirty: bool,
}

impl Drop for ValueMut<'_> {
    fn drop(&mut self) {
        if self.dirty {
            // Same invariant as `insert()`: the empty byte string is the
            // internal deletion tombstone and must not be produced through
            // the mutable-reference path either.
            assert!(
                !self.value.is_empty(),
                "empty value is a tombstone; call remove() instead"
            );
            self.inner.clone_from(&self.value);
        }
    }
}

impl Deref for ValueMut<'_> {
    type Target = RawBytes;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for ValueMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.value
    }
}
