//!
//! [`VerMap`] — a typed, versioned key-value map with branch / commit /
//! merge support, modelled after Git semantics.
//!

use super::{BranchId, Commit, CommitId, NO_COMMIT, diff::DiffEntry};
use crate::{
    Mapx, MapxOrd, Orphan,
    basic::persistent_btree::{EMPTY_ROOT, NodeId, PersistentBTree, TreeDiff},
    common::{
        Colocate, InstanceId,
        ende::{KeyEnDeOrdered, OrderedKeyRef, ValueEnDe},
        error::{Result, VsdbError},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    collections::{BinaryHeap, HashMap, HashSet},
    marker::PhantomData,
    time::{SystemTime, UNIX_EPOCH},
};

// =========================================================================
// BranchState
// =========================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct BranchState {
    pub(crate) name: String,
    /// The latest commit on this branch.
    pub(crate) head: CommitId,
    /// B+ tree root of uncommitted (working) state.
    /// Starts as the root of `head`'s commit; mutations update this.
    pub(crate) dirty_root: NodeId,
}

// =========================================================================
// VerMap
// =========================================================================

/// A persistent, versioned, ordered key-value map.
///
/// `VerMap` provides Git-style version control for a typed key-value store:
/// branching, committing, three-way merge, rollback, and garbage collection,
/// all backed by a persistent B+ tree with copy-on-write structural sharing.
///
/// # Lifecycle
///
/// A typical workflow mirrors the Git mental model:
///
/// 1. **Create** — `VerMap::new()` gives you a map with a single `main`
///    branch and an empty working state.
/// 2. **Write** — `insert` / `remove` mutate the *working state* of a
///    branch (analogous to editing files in a Git working directory).
/// 3. **Commit** — `commit` snapshots the current working state into an
///    immutable [`Commit`].  Each commit records the B+ tree root, parent
///    linkage, and a wall-clock timestamp.
/// 4. **Branch** — `create_branch` forks a lightweight branch from any
///    existing branch.  The new branch shares all history via structural
///    sharing — no data is copied.
/// 5. **Merge** — `merge(source, target)` performs a three-way merge.
///    Deletion is treated as "assigning ∅", so all conflicts are resolved
///    uniformly: **source wins** — whether source wrote a new value or
///    deleted the key.
/// 6. **Rollback** — `rollback_to` rewinds a branch to an earlier commit;
///    `discard` throws away uncommitted changes.
/// 7. **History** — `log`, and [`at`](Self::at) snapshots of any commit, let you
///    inspect any historical snapshot.
/// 8. **GC** — garbage collection is automatic: commits are deleted via
///    reference counting and dead B+ tree nodes are reclaimed by the
///    storage engine's background compaction.  [`gc`](Self::gc) is only
///    needed for crash recovery or a forced full sweep.
///
/// # Durability and recovery
///
/// Every component of a map created by this version shares one engine
/// shard, so one WAL orders all of its writes: working-state changes
/// (`insert`, `remove`, `discard`) issue no fsync, and history operations
/// (`commit`, `merge`, branch create/delete, `rollback_to`,
/// `set_main_branch`) end with a single WAL sync — they are durable when they
/// return. Released tree nodes are registered for physical deletion only
/// after the next sync. Maps created by earlier versions keep their
/// per-shard layout and synchronize each shard WAL at every publication
/// boundary instead; [`Clone`] produces a co-located copy.
/// Reference-count changes carry a dirty marker until their completion.
/// Restoration validates all reachable commit records before reclaiming any
/// history; a missing HEAD or parent produces a deserialization error.
///
/// # Read-only mode
///
/// A restored `VerMap` remains fully queryable in process-wide read-only
/// mode, including branch lookup, historical reads, diffs, and logs. Mutating
/// methods that return `Result` fail with [`VsdbError::ReadOnly`], and
/// [`gc`](Self::gc) becomes a no-op because it cannot persist repairs or
/// deferred-deletion registrations.
///
/// # Quick start
///
/// ```
/// use vsdb::versioned::map::VerMap;
/// use vsdb::{VsdbOptions, vsdb_configure, vsdb_get_base_dir};
/// use std::fs;
///
/// let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
/// vsdb_configure(VsdbOptions::new(&dir)).unwrap();
///
/// let mut m: VerMap<u32, String> = VerMap::new();
/// let main = m.main_branch();
///
/// // 1. Write on the default "main" branch.
/// m.insert(main, &1, &"hello".into()).unwrap();
/// m.insert(main, &2, &"world".into()).unwrap();
/// let c1 = m.commit(main).unwrap();
///
/// // 2. Fork a feature branch from main.
/// let feat = m.create_branch("feature", main).unwrap();
/// m.insert(feat, &1, &"hi".into()).unwrap();
/// let c2 = m.commit(feat).unwrap();
///
/// // 3. Branches are isolated — main is unchanged.
/// assert_eq!(m.get(main, &1).unwrap(), Some("hello".into()));
/// assert_eq!(m.get(feat, &1).unwrap(), Some("hi".into()));
///
/// // 4. Merge feature → main (source wins on conflict).
/// m.merge(feat, main).unwrap();
/// assert_eq!(m.get(main, &1).unwrap(), Some("hi".into()));
///
/// // 5. Clean up: delete the feature branch.
/// //    Dead commits and B+ tree nodes are reclaimed automatically.
/// m.delete_branch(feat).unwrap();
///
/// fs::remove_dir_all(&dir).unwrap();
/// ```
#[derive(Debug)]
pub struct VerMap<K, V> {
    /// The underlying persistent B+ tree (shared node pool).
    pub(crate) tree: PersistentBTree,

    /// CommitId → Commit
    pub(crate) commits: MapxOrd<u64, Commit>,

    /// BranchId → BranchState
    pub(crate) branches: MapxOrd<u64, BranchState>,

    /// branch name → BranchId
    pub(crate) branch_names: Mapx<String, u64>,

    /// ID allocators
    pub(crate) next_commit: Orphan<u64>,
    pub(crate) next_branch: Orphan<u64>,

    /// The branch currently designated as "main" (protected from deletion).
    pub(crate) main_branch: Orphan<u64>,

    /// Set `true` before a ref-count cascade, `false` after.
    /// If `true` on startup → run `rebuild_ref_counts()` to repair.
    pub(crate) gc_dirty: Orphan<bool>,

    /// Every component routes to the node pool's engine shard, so one WAL
    /// orders all of their writes (runtime property, recomputed on
    /// restore; maps created before co-location keep per-shard fences).
    pub(crate) colocated: bool,

    _phantom: PhantomData<(K, V)>,
}

/// Released nodes queued before a working-state operation forces a WAL
/// sync to register them (bounds the queue when no commit comes).
const RECLAIM_BACKLOG: usize = 16 * 1024;

// Keep the same K/V bounds as the former derived Clone implementation.
impl<K: Clone, V: Clone> Clone for VerMap<K, V> {
    /// Deep copy. The copy is always co-located (one WAL for all of its
    /// components), including a copy of a map created before co-location.
    fn clone(&self) -> Self {
        let mut tree = self.tree.clone();
        tree.defer_reclaim();
        let anchor = &tree.nodes;
        fn copy<T>(r: Result<T>) -> T {
            r.expect("vsdb: clone failed — I/O error")
        }
        let commits = copy(self.commits.clone_colocated(anchor));
        let branches = copy(self.branches.clone_colocated(anchor));
        let branch_names = copy(self.branch_names.clone_colocated(anchor));
        let next_commit = copy(self.next_commit.clone_colocated(anchor));
        let next_branch = copy(self.next_branch.clone_colocated(anchor));
        let main_branch = copy(self.main_branch.clone_colocated(anchor));
        let gc_dirty = copy(self.gc_dirty.clone_colocated(anchor));
        let mut cloned = Self {
            tree,
            commits,
            branches,
            branch_names,
            next_commit,
            next_branch,
            main_branch,
            gc_dirty,
            colocated: true,
            _phantom: PhantomData,
        };
        // Establish the copy's durable graph and exclude reader leases that
        // belong only to the original map before the new handle escapes.
        cloned.rebuild_tree_ref_counts();
        cloned
    }
}

impl<K, V> Serialize for VerMap<K, V> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // The K/V type parameters do not occur in any field type, so the
        // typed-handle envelope (tagged with `VerMap<K, V>`) is the only
        // guard against restoring this map under different K/V.
        crate::common::serialize_typed_handle_meta::<Self, S>(
            &(
                &self.tree,
                &self.commits,
                &self.branches,
                &self.branch_names,
                &self.next_commit,
                &self.next_branch,
                &self.main_branch,
                &self.gc_dirty,
            ),
            serializer,
        )
    }
}

type VerMapPayload = (
    PersistentBTree,
    MapxOrd<u64, Commit>,
    MapxOrd<u64, BranchState>,
    Mapx<String, u64>,
    Orphan<u64>,
    Orphan<u64>,
    Orphan<u64>,
    Orphan<bool>,
);

impl<'de, K, V> Deserialize<'de> for VerMap<K, V> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (
            tree,
            commits,
            branches,
            branch_names,
            next_commit,
            next_branch,
            main_branch,
            gc_dirty,
        ) = crate::common::deserialize_typed_handle_meta::<Self, VerMapPayload, D>(
            deserializer,
        )?;
        let mut m = VerMap {
            tree,
            commits,
            branches,
            branch_names,
            next_commit,
            next_branch,
            main_branch,
            gc_dirty,
            colocated: false,
            _phantom: PhantomData,
        };
        let anchor = &m.tree.nodes;
        m.colocated = [
            m.commits.raw(),
            m.branches.raw(),
            m.branch_names.raw(),
            m.next_commit.raw(),
            m.next_branch.raw(),
            m.main_branch.raw(),
            m.gc_dirty.raw(),
        ]
        .into_iter()
        .all(|component| component.is_colocated_with(anchor));
        if m.colocated {
            m.tree.defer_reclaim();
        }
        m.repair_commit_ref_counts_if_needed()
            .map_err(serde::de::Error::custom)?;
        m.rebuild_tree_ref_counts();
        Ok(m)
    }
}

// Separate impl block without K/V trait bounds so that the
// Deserialize visitor (which has no trait bounds on K/V) can call it.

impl<K, V> Default for VerMap<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> VerMap<K, V> {
    /// Returns the unique instance ID of this `VerMap`.
    pub fn instance_id(&self) -> InstanceId {
        self.tree.instance_id()
    }

    /// The namespace this map lives in.
    pub fn namespace(&self) -> crate::common::Namespace {
        self.tree.namespace()
    }

    fn ensure_writable(&self, operation: &'static str) -> Result<()> {
        crate::common::ensure_writable(&self.namespace(), operation)
    }

    /// Establishes the component graph before a new/restored handle escapes
    /// or a full node sweep can retire storage. These are WAL fences, not
    /// namespace-wide memtable flushes. Read-only restoration never writes.
    pub(crate) fn sync_storage(&self) {
        if self.namespace().is_read_only() {
            return;
        }
        self.tree.nodes.sync_wal();
        if self.colocated {
            // One shard, one WAL: the sync above covers every component.
            return;
        }
        self.next_commit.sync_wal();
        self.next_branch.sync_wal();
        self.commits.sync_wal();
        self.branches.sync_wal();
        self.branch_names.sync_wal();
        self.main_branch.sync_wal();
        self.gc_dirty.sync_wal();
    }

    pub(crate) fn get_branch(&self, id: BranchId) -> Result<BranchState> {
        self.branches
            .get(&id.0)
            .ok_or(VsdbError::BranchNotFound { branch_id: id.0 })
    }

    pub(crate) fn get_commit_inner(&self, id: CommitId) -> Result<Commit> {
        self.commits
            .get(&id.0)
            .ok_or(VsdbError::CommitNotFound { commit_id: id.0 })
    }

    /// [`diff_commits`](VerMap::diff_commits) as stored bytes (keys in
    /// their `KeyEnDeOrdered` encoding, values in their `ValueEnDe`
    /// encoding) — e.g. to update an external hash structure incrementally.
    pub fn raw_diff_commits(
        &self,
        from: CommitId,
        to: CommitId,
    ) -> Result<Vec<TreeDiff>> {
        let from_commit = self.get_commit_inner(from)?;
        let to_commit = self.get_commit_inner(to)?;
        Ok(self.tree.diff(from_commit.root, to_commit.root))
    }

    /// [`diff_uncommitted`](VerMap::diff_uncommitted) as stored bytes (see
    /// [`raw_diff_commits`](VerMap::raw_diff_commits)).
    pub fn raw_diff_uncommitted(&self, branch: BranchId) -> Result<Vec<TreeDiff>> {
        let state = self.get_branch(branch)?;
        let head_root = if state.head == NO_COMMIT {
            EMPTY_ROOT
        } else {
            self.get_commit_inner(state.head)?.root
        };
        Ok(self.tree.diff(head_root, state.dirty_root))
    }

    pub(crate) fn begin_ref_update(&mut self) {
        *self.gc_dirty.get_mut() = true;
        self.fence(|m| m.gc_dirty.sync_wal());
    }

    /// Call only after every changed branch/commit row is ordered before
    /// it and all reference withdrawals preceded any physical node
    /// retirement.
    pub(crate) fn end_ref_update(&mut self) {
        *self.gc_dirty.get_mut() = false;
        self.fence(|m| m.gc_dirty.sync_wal());
    }

    /// Orders every write issued so far to one component before any later
    /// write to another. Co-located components share one WAL, whose
    /// program order already provides that (a crash keeps a prefix of
    /// it); otherwise the component's own shard WAL must be synced.
    #[inline]
    pub(crate) fn fence(&self, sync: impl FnOnce(&Self)) {
        if !self.colocated {
            sync(self);
        }
    }

    /// Ends a history-changing operation: makes it durable (one WAL sync
    /// when co-located; per-shard layouts fenced every step already), then
    /// registers the nodes it released for physical deletion.
    pub(crate) fn settle(&mut self) {
        if self.colocated && !self.namespace().is_read_only() {
            self.tree.nodes.sync_wal();
            self.tree.register_deferred_reclaims();
        }
    }

    /// Working-state operations skip the sync; their released nodes wait
    /// for the next [`settle`](Self::settle) unless the queue grows large.
    fn settle_if_backlogged(&mut self) {
        if self.tree.deferred_reclaims() >= RECLAIM_BACKLOG {
            self.settle();
        }
    }
}

impl<K, V> VerMap<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Creates a new, empty versioned map with a default `main` branch.
    ///
    /// Equivalent to `new_with_main("main")`.
    pub fn new() -> Self {
        Self::new_with_main("main")
    }

    /// [`new`](Self::new) placed in `ns` — every internal component of
    /// the map lands in the same namespace (a composite never spans
    /// namespaces).
    pub fn new_in(ns: &crate::common::Namespace) -> Self {
        ns.scope(Self::new)
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

    /// Recovers a `VerMap` instance from previously saved metadata.
    ///
    /// The caller must ensure that the underlying VSDB database still
    /// contains the data referenced by this instance ID.
    pub fn from_meta(instance_id: impl Into<InstanceId>) -> Result<Self> {
        // Deserialize already calls rebuild_tree_ref_counts().
        let id = instance_id.into();
        crate::common::load_instance_meta_checked(id, Self::instance_id)
    }

    /// Creates a new, empty versioned map whose initial branch has the
    /// given `name` (e.g. `"genesis"`, `"canonical"`).
    ///
    /// The initial branch is automatically designated as the *main* branch
    /// and cannot be deleted until another branch is promoted via
    /// [`set_main_branch`](Self::set_main_branch).
    pub fn new_with_main(name: &str) -> Self {
        // Every component shares the node pool's shard (one WAL), so
        // operations need no fence between components.
        let mut tree = PersistentBTree::new();
        tree.defer_reclaim();
        let anchor = &tree.nodes;
        let mut branches: MapxOrd<u64, BranchState> = Colocate::new_colocated(anchor);
        let mut branch_names: Mapx<String, u64> = Colocate::new_colocated(anchor);

        let initial_id = BranchId(1);

        let main = BranchState {
            name: name.into(),
            head: NO_COMMIT,
            dirty_root: EMPTY_ROOT,
        };
        branches.insert(&initial_id.0, &main);
        branch_names.insert(&name.to_string(), &initial_id.0);

        let commits = Colocate::new_colocated(anchor);
        let next_commit = Orphan::new_colocated(anchor, NO_COMMIT.0 + 1);
        let next_branch = Orphan::new_colocated(anchor, initial_id.0 + 1);
        let main_branch = Orphan::new_colocated(anchor, initial_id.0);
        let gc_dirty = Orphan::new_colocated(anchor, false);
        let map = Self {
            tree,
            commits,
            branches,
            branch_names,
            next_commit,
            next_branch,
            main_branch,
            gc_dirty,
            colocated: true,
            _phantom: PhantomData,
        };
        // Initial Orphan slots and branch metadata must survive alongside
        // metadata saved immediately after construction.
        map.sync_storage();
        map
    }

    /// A map in the pre-co-location layout: every component on its own
    /// prefix, generally on different shards (pinned by tests of the
    /// per-shard fence path and of `clone` upgrading it).
    #[cfg(test)]
    pub(crate) fn with_legacy_layout() -> Self {
        let tree = PersistentBTree::new();
        let anchor = tree.nodes.clone();
        // Force at least one component onto another shard.
        let commits = loop {
            let c: MapxOrd<u64, Commit> = MapxOrd::new();
            if !c.raw().is_colocated_with(&anchor) {
                break c;
            }
        };
        let mut branches: MapxOrd<u64, BranchState> = MapxOrd::new();
        let mut branch_names: Mapx<String, u64> = Mapx::new();
        branches.insert(
            &1,
            &BranchState {
                name: "main".into(),
                head: NO_COMMIT,
                dirty_root: EMPTY_ROOT,
            },
        );
        branch_names.insert(&"main".to_string(), &1);
        let map = Self {
            tree,
            commits,
            branches,
            branch_names,
            next_commit: Orphan::new(1),
            next_branch: Orphan::new(2),
            main_branch: Orphan::new(1),
            gc_dirty: Orphan::new(false),
            colocated: false,
            _phantom: PhantomData,
        };
        map.sync_storage();
        map
    }

    // =================================================================
    // Internal helpers
    // =================================================================

    fn branch_name_exists(&self, name: &str) -> bool {
        self.branches.iter().any(|(_, state)| state.name == name)
    }

    // =================================================================
    // Main branch
    // =================================================================

    /// Returns the [`BranchId`] of the current main branch.
    pub fn main_branch(&self) -> BranchId {
        BranchId(self.main_branch.get_value())
    }

    /// Designates `branch` as the new main branch.
    ///
    /// The previous main branch becomes an ordinary branch (deletable).
    /// The new main branch is protected from deletion.
    pub fn set_main_branch(&mut self, branch: BranchId) -> Result<()> {
        self.ensure_writable("main branch update")?;
        self.get_branch(branch)?;
        *self.main_branch.get_mut() = branch.0;
        // A later delete of the old main must not leave the durable main
        // pointer referring to that deleted branch.
        self.fence(|m| m.main_branch.sync_wal());
        self.settle();
        Ok(())
    }

    // =================================================================
    // Branch management
    // =================================================================

    /// Creates a new branch forked from `source_branch`.
    ///
    /// The new branch inherits both the committed head and the current
    /// working state (uncommitted changes, if any) of the source branch.
    pub fn create_branch(
        &mut self,
        name: &str,
        source_branch: BranchId,
    ) -> Result<BranchId> {
        self.ensure_writable("branch creation")?;
        if self.branch_name_exists(name) {
            return Err(VsdbError::BranchAlreadyExists {
                name: name.to_string(),
            });
        }
        let src = self.get_branch(source_branch)?;

        // Mark dirty before any ref-count mutation so a crash mid-update is
        // repaired by rebuild_ref_counts on recovery (matches commit/merge/
        // rollback). increment_ref does not touch gc_dirty itself.
        self.begin_ref_update();

        let id = BranchId(self.next_branch.get_value());
        *self.next_branch.get_mut() = id.0 + 1;
        // Persist the allocator before a row can make this ID observable.
        self.fence(|m| m.next_branch.sync_wal());

        let state = BranchState {
            name: name.into(),
            head: src.head,
            dirty_root: src.dirty_root,
        };
        self.branches.insert(&id.0, &state);
        self.branch_names.insert(&name.to_string(), &id.0);

        // New branch HEAD adds a reference to the shared commit.
        self.increment_ref(src.head);
        // New branch's dirty_root references the shared tree root.
        self.tree.acquire_node(src.dirty_root);

        self.fence(|m| {
            m.commits.sync_wal();
            m.branches.sync_wal();
            m.branch_names.sync_wal();
        });
        self.end_ref_update();
        self.settle();

        Ok(id)
    }

    /// Deletes a branch and automatically cleans up orphaned commits.
    ///
    /// Decrements the ref-count on the branch's HEAD commit. If it
    /// reaches zero, the commit is hard-deleted and the decrement
    /// cascades to its parents.
    ///
    /// B+ tree nodes are reclaimed inline through the same ref-count
    /// cascade; [`gc`](Self::gc) is only needed to recover from a crash.
    pub fn delete_branch(&mut self, branch: BranchId) -> Result<()> {
        self.ensure_writable("branch deletion")?;
        if branch == self.main_branch() {
            return Err(VsdbError::CannotDeleteMainBranch);
        }
        let state = self.get_branch(branch)?;
        let dead_head = state.head;
        let dead_dirty = state.dirty_root;

        // Mark dirty before removing the branch tables so a crash before the
        // ref-count cascade is repaired on recovery. decrement_ref is
        // reentrant-safe (its already_dirty guard won't clear our flag).
        self.begin_ref_update();

        self.branch_names.remove(&state.name);
        self.branches.remove(&branch.0);
        // Old roots cannot be retired while the durable branch still owns them.
        self.fence(|m| {
            m.branches.sync_wal();
            m.branch_names.sync_wal();
        });

        // Release tree root ref from the branch's dirty_root.
        self.tree.release_node(dead_dirty);
        // Cascade commit ref counting (may also release commit.root refs).
        self.decrement_ref(dead_head);

        self.end_ref_update();
        self.settle();

        Ok(())
    }

    /// Lists all branches as `(BranchId, name)`.
    pub fn list_branches(&self) -> Vec<(BranchId, String)> {
        self.branches
            .iter()
            .map(|(id, s)| (BranchId(id), s.name))
            .collect()
    }

    /// Looks up a branch by name, returning its ID if it exists.
    pub fn branch_id(&self, name: &str) -> Option<BranchId> {
        if self.namespace().is_read_only() {
            self.branches
                .iter()
                .find_map(|(id, state)| (state.name == name).then_some(BranchId(id)))
        } else {
            self.branch_names.get(&name.to_string()).map(BranchId)
        }
    }

    /// Returns the name of a branch given its ID.
    pub fn branch_name(&self, branch: BranchId) -> Option<String> {
        self.branches.get(&branch.0).map(|s| s.name)
    }

    /// Returns `true` if the branch has uncommitted changes (dirty state
    /// differs from the head commit's snapshot).
    pub fn has_uncommitted(&self, branch: BranchId) -> Result<bool> {
        let state = self.get_branch(branch)?;
        if state.head == NO_COMMIT {
            Ok(state.dirty_root != EMPTY_ROOT)
        } else {
            let head_root = self.get_commit_inner(state.head)?.root;
            Ok(state.dirty_root != head_root)
        }
    }

    // Write (working state)
    // =================================================================

    /// Inserts a key-value pair into the working state of `branch`.
    pub fn insert(&mut self, branch: BranchId, key: &K, value: &V) -> Result<()> {
        self.ensure_writable("versioned map insert")?;
        let mut state = self.get_branch(branch)?;
        let old_root = state.dirty_root;
        state.dirty_root = self.tree.insert(old_root, &key.to_bytes(), &value.encode());
        // The node batch is atomic, but it must be ordered before the
        // branch row that publishes the new root.
        self.fence(|m| m.tree.nodes.sync_wal());
        self.tree.acquire_node(state.dirty_root);
        // Persist the branch pointer BEFORE retiring the old root: a
        // crash must never leave the durable branch state pointing at
        // deleted nodes (co-located maps defer the physical-deletion
        // registration to the next sync instead).  Crash before the
        // insert below only leaks the new nodes, which the next recovery
        // sweep (`rebuild_tree_ref_counts`) registers for deletion.
        self.branches.insert(&branch.0, &state);
        self.fence(|m| m.branches.sync_wal());
        self.tree.release_node(old_root);
        self.settle_if_backlogged();
        Ok(())
    }

    /// Removes a key from the working state of `branch`.
    pub fn remove<Q>(&mut self, branch: BranchId, key: &Q) -> Result<()>
    where
        K: Borrow<Q>,
        Q: OrderedKeyRef + ?Sized,
    {
        self.ensure_writable("versioned map remove")?;
        let mut state = self.get_branch(branch)?;
        let old_root = state.dirty_root;
        state.dirty_root = self.tree.remove(old_root, &key.ordered_key_bytes());
        if state.dirty_root == old_root {
            return Ok(());
        }
        self.fence(|m| m.tree.nodes.sync_wal());
        self.tree.acquire_node(state.dirty_root);
        // Persist before release — see `insert` for the crash-ordering
        // rationale.
        self.branches.insert(&branch.0, &state);
        self.fence(|m| m.branches.sync_wal());
        self.tree.release_node(old_root);
        self.settle_if_backlogged();
        Ok(())
    }

    // =================================================================
    // Commit / Rollback
    // =================================================================

    /// Commits the current working state of `branch`, creating a new
    /// immutable [`Commit`].  Returns the commit ID.
    pub fn commit(&mut self, branch: BranchId) -> Result<CommitId> {
        self.ensure_writable("versioned map commit")?;
        let state = self.get_branch(branch)?;

        // Mark dirty before any structural mutation so that crash
        // recovery (gc → rebuild_ref_counts) will repair orphaned
        // commits or imbalanced ref-counts.
        self.begin_ref_update();

        let id = CommitId(self.next_commit.get_value());
        *self.next_commit.get_mut() = id.0 + 1;
        self.fence(|m| m.next_commit.sync_wal());

        let parents = if state.head == NO_COMMIT {
            vec![]
        } else {
            vec![state.head]
        };

        // ref_count accounting for this operation:
        //   - new commit = 1: the branch HEAD moves onto it below.
        //   - old HEAD   = net 0: it loses this branch's HEAD (-1) but
        //     gains a parent-link from the new commit (+1), so its
        //     stored count is deliberately left untouched.  This pairing
        //     is load-bearing — if commit() ever stops listing the old
        //     HEAD in `parents` (or the HEAD moves anywhere other than
        //     onto a child of the old HEAD), the old HEAD's count must
        //     be adjusted explicitly.  INV-V1 is pinned mechanically by
        //     the `ref_counts_match_ground_truth_recount` test.
        let commit = Commit {
            id,
            root: state.dirty_root,
            parents,
            timestamp_us: now_us(),
            ref_count: 1,
        };
        self.commits.insert(&id.0, &commit);
        // A durable branch HEAD must never name a missing commit record.
        self.fence(|m| m.commits.sync_wal());

        // commit.root now also references dirty_root → acquire.
        self.tree.acquire_node(state.dirty_root);

        // Update branch head; dirty_root stays the same (it IS the snapshot).
        let new_state = BranchState { head: id, ..state };
        self.branches.insert(&branch.0, &new_state);
        self.fence(|m| m.branches.sync_wal());

        self.end_ref_update();
        self.settle();

        Ok(id)
    }

    /// Discards uncommitted changes, resetting the working state to the
    /// branch head.
    pub fn discard(&mut self, branch: BranchId) -> Result<()> {
        self.ensure_writable("working-state discard")?;
        let state = self.get_branch(branch)?;
        let old_dirty = state.dirty_root;
        let root = if state.head == NO_COMMIT {
            EMPTY_ROOT
        } else {
            self.get_commit_inner(state.head)?.root
        };
        let new_state = BranchState {
            dirty_root: root,
            ..state
        };
        self.tree.acquire_node(root);
        // Persist before release — see `insert` for the crash-ordering
        // rationale.
        self.branches.insert(&branch.0, &new_state);
        self.fence(|m| m.branches.sync_wal());
        self.tree.release_node(old_dirty);
        self.settle_if_backlogged();
        Ok(())
    }

    /// Rolls back `branch` to a previous commit, discarding all commits
    /// after `target` on this branch.
    ///
    /// `target` must be an ancestor of the branch's current head.
    /// Commits between `target` and the previous head that are exclusively
    /// reachable from this branch are immediately deleted via ref-count
    /// cascade.  Commits still referenced by other branches are preserved.
    /// Call [`gc`](Self::gc) only to recover from a crash or force a full
    /// B+ tree sweep.
    pub fn rollback_to(&mut self, branch: BranchId, target: CommitId) -> Result<()> {
        self.ensure_writable("branch rollback")?;
        let state = self.get_branch(branch)?;
        let _ = self.get_commit_inner(target)?;

        // A branch with no commits has no ancestors — rolling it back to
        // an arbitrary commit would silently attach it to another
        // branch's history.
        if state.head == NO_COMMIT {
            return Err(VsdbError::NotAncestor {
                commit_id: target.0,
                branch_id: branch.0,
            });
        }

        // Rolling back — whether to the current HEAD or to a strict
        // ancestor — always overwrites `dirty_root`, silently losing any
        // uncommitted changes.  Reject unconditionally (matching
        // `merge()`'s unconditional guard) and tell the caller to use
        // `discard()` instead. This check must run before the
        // target-vs-head branching below, not just the `target ==
        // state.head` arm — an ancestor target must not bypass it.
        if self.has_uncommitted(branch)? {
            return Err(VsdbError::UncommittedChanges {
                branch_id: branch.0,
            });
        }

        if target == state.head {
            // target == head with a clean working state (uncommitted
            // changes are already ruled out above): a complete no-op.
            // Return before the mutation path below — setting gc_dirty
            // and rewriting identical state would only widen the
            // crash-recovery window for zero effect.
            return Ok(());
        }

        // Verify target is reachable from the branch head.
        {
            let mut queue = vec![state.head];
            let mut visited = HashSet::new();
            let mut found = false;
            while let Some(cur) = queue.pop() {
                if cur == NO_COMMIT || !visited.insert(cur) {
                    continue;
                }
                if cur == target {
                    found = true;
                    break;
                }
                if let Some(c) = self.commits.get(&cur.0) {
                    queue.extend_from_slice(&c.parents);
                }
            }
            if !found {
                return Err(VsdbError::NotAncestor {
                    commit_id: target.0,
                    branch_id: branch.0,
                });
            }
        }

        // Mark dirty before any structural mutation so that crash
        // recovery (gc → rebuild_ref_counts) will repair orphaned
        // commits or imbalanced ref-counts.
        self.begin_ref_update();

        let commit = self.get_commit_inner(target)?;
        let old_head = state.head;
        let old_dirty = state.dirty_root;
        let new_state = BranchState {
            name: state.name,
            head: target,
            dirty_root: commit.root,
        };
        self.branches.insert(&branch.0, &new_state);
        self.fence(|m| m.branches.sync_wal());

        // Tree root: dirty_root changes to commit.root.
        self.tree.acquire_node(commit.root);
        self.tree.release_node(old_dirty);

        // Commit ref counts: target gains a branch-HEAD, old head
        // loses one.  Increment FIRST to protect target from cascade.
        self.increment_ref(target);
        self.decrement_ref(old_head);

        self.end_ref_update();
        self.settle();

        Ok(())
    }

    // =================================================================
    // Merge
    // =================================================================

    /// Merges `source` branch into `target` branch using three-way merge.
    ///
    /// Both branches must be committed (no uncommitted changes).
    ///
    /// # Conflict resolution: source wins on conflicts
    ///
    /// First, non-conflicting single-sided changes are preserved using the
    /// ancestor snapshot. If both sides changed the same key differently,
    /// **source wins**. A deletion is treated as "assigning ∅", so
    /// delete-vs-modify is also resolved by source priority.
    ///
    /// | source | target | result |
    /// |--------|--------|--------|
    /// | unchanged (A) | changed to T | **T** (target-only change preserved) |
    /// | changed to S | unchanged (A) | **S** (source-only change preserved) |
    /// | changed to S | changed to T | **S** (conflict → source wins) |
    /// | deleted (∅) | changed to T | **∅** (conflict → source wins → delete) |
    /// | changed to S | deleted (∅) | **S** (conflict → source wins → keep) |
    ///
    /// The caller controls priority by choosing which branch to pass as
    /// `source` vs `target`.
    ///
    /// If `target` has no commits, performs a fast-forward (no merge commit
    /// is created). Equal source/target heads return the existing commit.
    /// Other successful merges create a two-parent commit on `target`.
    ///
    /// # Cost
    ///
    /// The merge replays the source-side delta (keys changed since the
    /// merge bases) onto the target tree by copy-on-write. Replay work and
    /// buffering follow changed paths; diff traversal skips shared NodeIds
    /// but can scan the full tree when versions share no nodes. Finding
    /// merge bases also walks commit history. Unaffected target subtrees
    /// remain shared where COW paths permit it.
    pub fn merge(&mut self, source: BranchId, target: BranchId) -> Result<CommitId> {
        self.ensure_writable("branch merge")?;
        if source == target {
            return Err(VsdbError::SelfMerge {
                branch_id: source.0,
            });
        }

        // Reject if either branch has uncommitted changes.
        if self.has_uncommitted(source)? {
            return Err(VsdbError::UncommittedChanges {
                branch_id: source.0,
            });
        }
        if self.has_uncommitted(target)? {
            return Err(VsdbError::UncommittedChanges {
                branch_id: target.0,
            });
        }

        let src = self.get_branch(source)?;
        let tgt = self.get_branch(target)?;

        if src.head == NO_COMMIT {
            return Err(VsdbError::NoCommits {
                branch_id: source.0,
            });
        }

        // Both branches point to the same commit — nothing to merge.
        // Creating a merge commit with duplicate parents would permanently
        // overcount the commit's ref_count (GC leak).  Return the existing
        // commit as a successful no-op instead.
        if src.head == tgt.head {
            return Ok(tgt.head);
        }

        // Mark dirty before any structural mutation so that crash
        // recovery (gc → rebuild_ref_counts) will repair orphaned
        // commits or imbalanced ref-counts.
        self.begin_ref_update();

        if tgt.head == NO_COMMIT {
            // Target is empty — just fast-forward.
            //
            // Precondition: `head == NO_COMMIT` ⟺ the branch has never
            // committed.  Branch creation copies the source's head (so a
            // NO_COMMIT head is only inherited from another never-committed
            // branch), `commit` moves the head onto a real commit and never
            // back, and `rollback_to` rejects NO_COMMIT heads outright.
            // The target therefore has NO history at all, so adopting the
            // source's chain wholesale cannot discard or cross-link any
            // target history (INV-V2); `get_commit_inner` just above
            // guarantees the adopted head is a real commit.
            let src_commit = self.get_commit_inner(src.head)?;
            let new_state = BranchState {
                head: src.head,
                dirty_root: src_commit.root,
                ..tgt
            };
            self.branches.insert(&target.0, &new_state);
            self.fence(|m| m.branches.sync_wal());
            // Target branch HEAD now points to src.head → +1 ref.
            self.increment_ref(src.head);
            // Tree root: dirty_root changes to src_commit.root.
            self.tree.acquire_node(src_commit.root);
            self.tree.release_node(tgt.dirty_root);
            self.fence(|m| m.commits.sync_wal());
            self.end_ref_update();
            self.settle();
            return Ok(src.head);
        }

        let src_commit = self.get_commit_inner(src.head)?;
        let tgt_commit = self.get_commit_inner(tgt.head)?;

        let ancestor_roots: Vec<NodeId> = self
            .find_merge_bases(src.head, tgt.head)
            .into_iter()
            .map(|aid| self.get_commit_inner(aid).map(|c| c.root))
            .collect::<Result<_>>()?;
        let ancestor_roots = if ancestor_roots.is_empty() {
            vec![EMPTY_ROOT]
        } else {
            ancestor_roots
        };

        let merged_root =
            self.tree
                .merge(&ancestor_roots, src_commit.root, tgt_commit.root);

        // Order the complete merged tree before publishing its root.
        self.fence(|m| m.tree.nodes.sync_wal());

        // Acquire both owners before readers can observe either published
        // root. A short-lived snapshot must not release an unowned merge
        // result back to zero while publication is still in progress.
        self.tree.acquire_node(merged_root); // commit.root
        self.tree.acquire_node(merged_root); // dirty_root

        // Create merge commit.
        let id = CommitId(self.next_commit.get_value());
        *self.next_commit.get_mut() = id.0 + 1;
        self.fence(|m| m.next_commit.sync_wal());

        // ref_count = 1: the target branch HEAD.
        // tgt.head: net 0 (loses branch-HEAD, gains parent-link).
        // src.head: +1 (gains parent-link from merge commit).
        let commit = Commit {
            id,
            root: merged_root,
            parents: vec![tgt.head, src.head],
            timestamp_us: now_us(),
            ref_count: 1,
        };
        self.commits.insert(&id.0, &commit);
        self.increment_ref(src.head);
        self.fence(|m| m.commits.sync_wal());

        let new_state = BranchState {
            head: id,
            dirty_root: merged_root,
            ..tgt
        };
        self.branches.insert(&target.0, &new_state);
        self.fence(|m| m.branches.sync_wal());

        self.tree.release_node(tgt.dirty_root); // old target dirty

        self.end_ref_update();
        self.settle();

        Ok(id)
    }

    /// Finds all lowest common ancestors (merge bases) of two commits.
    ///
    /// A criss-cross DAG can have multiple incomparable merge bases.  Feeding
    /// only the highest-id base into a three-way merge can classify a true
    /// conflict as a target-only change and violate the source-wins policy.
    ///
    /// Implementation: git-style "paint down to common".  The DAG is walked
    /// in descending `CommitId` order using a max-heap (`CommitId`s are
    /// monotonic — a parent is always created before its child), flagging
    /// each commit with the side(s) it is reachable from.  A commit popped
    /// with both flags and no `STALE` bit is a merge base; its ancestry is
    /// then painted `STALE`, so dominated common ancestors are never
    /// reported.  The walk ends when the frontier holds only stale entries,
    /// bounding the cost to the fork region instead of the full history.
    fn find_merge_bases(&self, a: CommitId, b: CommitId) -> Vec<CommitId> {
        const FROM_A: u8 = 0b001;
        const FROM_B: u8 = 0b010;
        const BOTH: u8 = FROM_A | FROM_B;
        const STALE: u8 = 0b100;

        // Nonexistent IDs are not DAG nodes; without this guard,
        // `find_merge_bases(x, x)` would report a nonexistent `x`
        // as its own merge base.
        if self.commits.get(&a.0).is_none() || self.commits.get(&b.0).is_none() {
            return vec![];
        }

        fn mark(
            flags: &mut HashMap<CommitId, u8>,
            heap: &mut BinaryHeap<CommitId>,
            id: CommitId,
            flag: u8,
        ) {
            if id == NO_COMMIT {
                return;
            }
            let slot = flags.entry(id).or_insert(0);
            if *slot == 0 {
                heap.push(id);
            }
            *slot |= flag;
        }

        let mut flags: HashMap<CommitId, u8> = HashMap::new();
        let mut heap: BinaryHeap<CommitId> = BinaryHeap::new();
        let mut bases = Vec::new();

        mark(&mut flags, &mut heap, a, FROM_A);
        mark(&mut flags, &mut heap, b, FROM_B);

        while heap.iter().any(|id| flags[id] & STALE == 0) {
            // The loop guard ensures the heap is nonempty.
            let id = heap.pop().unwrap();
            // Every id pushed onto the heap was first inserted into `flags`.
            let mut f = flags[&id];
            if f & BOTH == BOTH {
                if f & STALE == 0 {
                    bases.push(id);
                }
                // Everything below a common commit is dominated.
                f |= STALE;
            }
            if let Some(c) = self.commits.get(&id.0) {
                for &parent in &c.parents {
                    mark(&mut flags, &mut heap, parent, f);
                }
            }
        }

        bases
    }

    /// Finds one lowest (most-recent) common ancestor of two commits.
    fn find_common_ancestor(&self, a: CommitId, b: CommitId) -> Option<CommitId> {
        self.find_merge_bases(a, b).into_iter().max()
    }

    // =================================================================
    // History
    // =================================================================

    /// Returns one lowest common ancestor (fork point) of two commits.
    /// If there are several merge bases, selects the one with the greatest ID;
    /// merge itself considers all lowest common ancestors.
    ///
    /// Useful for branching scenarios: given two divergent tips, this finds
    /// the commit where they diverged.  Returns `None` only if the two
    /// commits share no common history.
    pub fn fork_point(&self, a: CommitId, b: CommitId) -> Option<CommitId> {
        self.find_common_ancestor(a, b)
    }

    /// Counts the number of first-parent commits between `from` and
    /// `ancestor` (exclusive).
    ///
    /// Walks the first-parent chain starting at `from` until `ancestor`
    /// is reached.  Returns `None` if `ancestor` is not a first-parent
    /// ancestor of `from`.
    ///
    /// # Example — comparing fork lengths
    ///
    /// ```ignore
    /// let lca = map.fork_point(tip_a, tip_b).unwrap();
    /// let ahead_a = map.commit_distance(tip_a, lca).unwrap();
    /// let ahead_b = map.commit_distance(tip_b, lca).unwrap();
    /// // The longer fork wins.
    /// ```
    pub fn commit_distance(&self, from: CommitId, ancestor: CommitId) -> Option<u64> {
        // Both endpoints must be real commits; otherwise
        // `commit_distance(x, x)` would report distance 0 for a
        // nonexistent `x`.
        self.commits.get(&from.0)?;
        self.commits.get(&ancestor.0)?;
        let mut cur = from;
        let mut count = 0u64;
        while cur != ancestor {
            if cur == NO_COMMIT {
                return None;
            }
            let c = self.commits.get(&cur.0)?;
            cur = c.parents.first().copied().unwrap_or(NO_COMMIT);
            count += 1;
        }
        Some(count)
    }

    /// Retrieves a commit by its ID.
    pub fn get_commit(&self, commit_id: CommitId) -> Option<Commit> {
        self.commits.get(&commit_id.0)
    }

    /// Returns the commit at the head of `branch`.
    pub fn head_commit(&self, branch: BranchId) -> Result<Option<Commit>> {
        let state = self.get_branch(branch)?;
        if state.head == NO_COMMIT {
            Ok(None)
        } else {
            Ok(self.commits.get(&state.head.0))
        }
    }

    /// Walks the first-parent commit history of `branch` from head to root.
    ///
    /// For merge commits, only the first parent (the target branch at merge
    /// time) is followed — analogous to `git log --first-parent`.
    pub fn log(&self, branch: BranchId) -> Result<Vec<Commit>> {
        let state = self.get_branch(branch)?;
        let mut result = Vec::new();
        let mut cur = state.head;
        while cur != NO_COMMIT {
            if let Some(c) = self.commits.get(&cur.0) {
                cur = c.parents.first().copied().unwrap_or(NO_COMMIT);
                result.push(c);
            } else {
                break;
            }
        }
        Ok(result)
    }

    // =================================================================
    // Diff
    // =================================================================

    /// Computes the diff between two commits.
    ///
    /// Returns every key that was added, removed, or modified between
    /// `from` and `to`, in ascending key order. Subtrees the two commits
    /// share are skipped, so the cost follows the size of the change.
    ///
    /// # Panics
    ///
    /// Panics if a changed entry cannot be decoded — see [`Snapshot`](super::Snapshot).
    pub fn diff_commits(
        &self,
        from: CommitId,
        to: CommitId,
    ) -> Result<Vec<DiffEntry<K, V>>> {
        Ok(self
            .raw_diff_commits(from, to)?
            .into_iter()
            .map(DiffEntry::decode)
            .collect())
    }

    /// Computes the diff of uncommitted (working) changes on `branch`,
    /// relative to its head commit — analogous to `git diff`.
    ///
    /// # Panics
    ///
    /// Panics if a changed entry cannot be decoded — see [`Snapshot`](super::Snapshot).
    pub fn diff_uncommitted(&self, branch: BranchId) -> Result<Vec<DiffEntry<K, V>>> {
        Ok(self
            .raw_diff_uncommitted(branch)?
            .into_iter()
            .map(DiffEntry::decode)
            .collect())
    }

    // =================================================================
    // GC
    // =================================================================

    /// Performs crash recovery and a full B+ tree node sweep.
    ///
    /// In normal operation **you do not need to call this method**.
    /// Both commit cleanup and B+ tree node cleanup happen
    /// automatically:
    ///
    /// - **Commits** are immediately hard-deleted when their reference
    ///   count reaches zero (via
    ///   [`delete_branch`](Self::delete_branch) /
    ///   [`rollback_to`](Self::rollback_to)).
    /// - **B+ tree nodes** are registered for deferred disk deletion
    ///   via the storage engine's compaction filter when
    ///   [`PersistentBTree::release_node`] drops their reference count
    ///   to zero.  The underlying MMDB engine reclaims disk space
    ///   during background compaction — no user action required.
    ///
    /// This method is still useful in two scenarios:
    ///
    /// 1. **Crash recovery** — if a ref-count cascade was interrupted
    ///    (`gc_dirty` flag), rebuilds all commit ref counts from
    ///    scratch and removes orphaned commits.
    /// 2. **Forced full sweep** — guarantees that every unreachable
    ///    B+ tree node is registered for compaction, even if a prior
    ///    cascade was incomplete.
    ///
    /// This is a no-op in read-only mode: recovery metadata cannot be
    /// rewritten and deferred deletions cannot be registered there.
    ///
    /// # Errors
    ///
    /// [`VsdbError::CommitNotFound`] — before removing anything — if a
    /// reachable HEAD or parent record is missing (the graph is damaged;
    /// deleting "orphans" would destroy older history).
    ///
    /// # Panics
    ///
    /// Storage synchronization failures panic.
    pub fn gc(&mut self) -> Result<()> {
        if self.namespace().is_read_only() {
            return Ok(());
        }
        // Validate even a clean graph before any deletion/sweep. A missing
        // HEAD/parent must not turn older durable history into "orphans".
        self.validate_commit_graph()?;

        // 1. Crash recovery: rebuild ref counts if the dirty flag is
        //    set, or if any commit has ref_count == 0 (migration from
        //    pre-ref-count data).
        if self.gc_dirty.get_value()
            || self.commits.iter().any(|(_, c)| c.ref_count == 0)
        {
            self.rebuild_ref_counts()?;
        }
        self.sync_storage();
        // Everything released so far is durable now; the sweep below
        // would find those nodes unreachable anyway.
        self.tree.register_deferred_reclaims();

        // 2. Collect live roots from all commits + dirty roots.
        let mut live_roots: Vec<NodeId> =
            self.commits.iter().map(|(_, c)| c.root).collect();
        for (_, s) in self.branches.iter() {
            if s.dirty_root != EMPTY_ROOT {
                live_roots.push(s.dirty_root);
            }
        }

        // 3. GC the B+ tree node pool.
        self.tree.gc(&live_roots);
        Ok(())
    }

    // =================================================================
    // Reference counting
    // =================================================================

    /// Increments the ref_count of the given commit by 1.
    fn increment_ref(&mut self, commit_id: CommitId) {
        if commit_id == NO_COMMIT {
            return;
        }
        if let Some(mut c) = self.commits.get(&commit_id.0) {
            c.ref_count += 1;
            self.commits.insert(&commit_id.0, &c);
        }
    }

    /// Decrements the ref_count of the given commit by 1.
    /// If it reaches zero, hard-deletes the commit and cascades
    /// to each parent.
    fn decrement_ref(&mut self, commit_id: CommitId) {
        if commit_id == NO_COMMIT {
            return;
        }

        let already_dirty = self.gc_dirty.get_value();
        if !already_dirty {
            self.begin_ref_update();
        }

        let mut dead_roots = Vec::new();
        let mut work = vec![commit_id];
        while let Some(id) = work.pop() {
            if id == NO_COMMIT {
                continue;
            }
            let Some(mut c) = self.commits.get(&id.0) else {
                continue; // already deleted (crash recovery case)
            };
            c.ref_count = c.ref_count.saturating_sub(1);
            if c.ref_count == 0 {
                let parents = c.parents.clone();
                // Retire roots only after the whole commit cascade is durable.
                // One fence per cascade avoids one fsync per deleted commit.
                dead_roots.push(c.root);
                self.commits.remove(&id.0);
                work.extend(parents);
            } else {
                self.commits.insert(&id.0, &c);
            }
        }

        self.fence(|m| m.commits.sync_wal());
        for root in dead_roots {
            self.tree.release_node(root);
        }

        if !already_dirty {
            self.end_ref_update();
        }
    }
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}
