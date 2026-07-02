//!
//! [`VerMap`] — a typed, versioned key-value map with branch / commit /
//! merge support, modelled after Git semantics.
//!

use super::{
    BranchId, Commit, CommitId, NO_COMMIT,
    diff::{DiffEntry, diff_roots},
    handle::{Branch, BranchMut},
};
use crate::basic::persistent_btree::{EMPTY_ROOT, NodeId, PersistentBTree};
use crate::common::ende::{KeyEnDeOrdered, ValueEnDe};
use crate::common::error::{Result, VsdbError};
use crate::{Mapx, MapxOrd, Orphan};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    fmt,
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
/// 7. **History** — `log`, `get_at_commit`, `iter_at_commit` let you
///    inspect any historical snapshot.
/// 8. **GC** — garbage collection is automatic: commits are deleted via
///    reference counting and dead B+ tree nodes are reclaimed by the
///    storage engine's background compaction.  [`gc`](Self::gc) is only
///    needed for crash recovery or a forced full sweep.
///
/// # Quick start
///
/// ```
/// use vsdb::versioned::map::VerMap;
/// use vsdb::versioned::NO_COMMIT;
/// use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
/// use std::fs;
///
/// let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
/// vsdb_set_base_dir(&dir).unwrap();
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
#[derive(Clone, Debug)]
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

    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Serialize for VerMap<K, V> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple;
        let mut t = serializer.serialize_tuple(8)?;
        t.serialize_element(&self.tree)?;
        t.serialize_element(&self.commits)?;
        t.serialize_element(&self.branches)?;
        t.serialize_element(&self.branch_names)?;
        t.serialize_element(&self.next_commit)?;
        t.serialize_element(&self.next_branch)?;
        t.serialize_element(&self.main_branch)?;
        t.serialize_element(&self.gc_dirty)?;
        t.end()
    }
}

impl<'de, K, V> Deserialize<'de> for VerMap<K, V> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Vis<K, V>(PhantomData<(K, V)>);
        impl<'de, K, V> serde::de::Visitor<'de> for Vis<K, V> {
            type Value = VerMap<K, V>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("VerMap")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<VerMap<K, V>, A::Error> {
                let tree = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let commits = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let branches = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                let branch_names = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(3, &self))?;
                let next_commit = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(4, &self))?;
                let next_branch = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(5, &self))?;
                let main_branch = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(6, &self))?;
                let gc_dirty = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(7, &self))?;
                let mut m = VerMap {
                    tree,
                    commits,
                    branches,
                    branch_names,
                    next_commit,
                    next_branch,
                    main_branch,
                    gc_dirty,
                    _phantom: PhantomData,
                };
                m.repair_commit_ref_counts_if_needed();
                m.rebuild_tree_ref_counts();
                Ok(m)
            }
        }
        deserializer.deserialize_tuple(8, Vis(PhantomData))
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

    /// Returns the unique instance ID of this `VerMap`.
    pub fn instance_id(&self) -> u64 {
        self.tree.instance_id()
    }

    /// Persists this instance's metadata to disk so that it can be
    /// recovered later via [`from_meta`](Self::from_meta).
    ///
    /// Returns the `instance_id` that should be passed to `from_meta`.
    pub fn save_meta(&self) -> Result<u64> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }

    /// Recovers a `VerMap` instance from previously saved metadata.
    ///
    /// The caller must ensure that the underlying VSDB database still
    /// contains the data referenced by this instance ID.
    pub fn from_meta(instance_id: u64) -> Result<Self> {
        // Deserialize already calls rebuild_tree_ref_counts().
        crate::common::load_instance_meta(instance_id)
    }

    /// Creates a new, empty versioned map whose initial branch has the
    /// given `name` (e.g. `"genesis"`, `"canonical"`).
    ///
    /// The initial branch is automatically designated as the *main* branch
    /// and cannot be deleted until another branch is promoted via
    /// [`set_main_branch`](Self::set_main_branch).
    pub fn new_with_main(name: &str) -> Self {
        let mut branches: MapxOrd<u64, BranchState> = MapxOrd::new();
        let mut branch_names: Mapx<String, u64> = Mapx::new();

        let initial_id: BranchId = 1;

        let main = BranchState {
            name: name.into(),
            head: NO_COMMIT,
            dirty_root: EMPTY_ROOT,
        };
        branches.insert(&initial_id, &main);
        branch_names.insert(&name.to_string(), &initial_id);

        Self {
            tree: PersistentBTree::new(),
            commits: MapxOrd::new(),
            branches,
            branch_names,
            next_commit: Orphan::new(1), // 0 = NO_COMMIT
            next_branch: Orphan::new(initial_id + 1),
            main_branch: Orphan::new(initial_id),
            gc_dirty: Orphan::new(false),
            _phantom: PhantomData,
        }
    }

    // =================================================================
    // Internal helpers
    // =================================================================

    pub(crate) fn get_branch(&self, id: BranchId) -> Result<BranchState> {
        self.branches
            .get(&id)
            .ok_or(VsdbError::BranchNotFound { branch_id: id })
    }

    pub(crate) fn get_commit_inner(&self, id: CommitId) -> Result<Commit> {
        self.commits
            .get(&id)
            .ok_or(VsdbError::CommitNotFound { commit_id: id })
    }

    // =================================================================
    // Main branch
    // =================================================================

    /// Returns the [`BranchId`] of the current main branch.
    pub fn main_branch(&self) -> BranchId {
        self.main_branch.get_value()
    }

    /// Designates `branch` as the new main branch.
    ///
    /// The previous main branch becomes an ordinary branch (deletable).
    /// The new main branch is protected from deletion.
    pub fn set_main_branch(&mut self, branch: BranchId) -> Result<()> {
        self.get_branch(branch)?;
        *self.main_branch.get_mut() = branch;
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
        if self.branch_names.contains_key(&name.to_string()) {
            return Err(VsdbError::BranchAlreadyExists {
                name: name.to_string(),
            });
        }
        let src = self.get_branch(source_branch)?;

        // Mark dirty before any ref-count mutation so a crash mid-update is
        // repaired by rebuild_ref_counts on recovery (matches commit/merge/
        // rollback). increment_ref does not touch gc_dirty itself.
        *self.gc_dirty.get_mut() = true;

        let id = self.next_branch.get_value();
        *self.next_branch.get_mut() = id + 1;

        let state = BranchState {
            name: name.into(),
            head: src.head,
            dirty_root: src.dirty_root,
        };
        self.branches.insert(&id, &state);
        self.branch_names.insert(&name.to_string(), &id);

        // New branch HEAD adds a reference to the shared commit.
        self.increment_ref(src.head);
        // New branch's dirty_root references the shared tree root.
        self.tree.acquire_node(src.dirty_root);

        *self.gc_dirty.get_mut() = false;

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
        if branch == self.main_branch.get_value() {
            return Err(VsdbError::CannotDeleteMainBranch);
        }
        let state = self.get_branch(branch)?;
        let dead_head = state.head;
        let dead_dirty = state.dirty_root;

        // Mark dirty before removing the branch tables so a crash before the
        // ref-count cascade is repaired on recovery. decrement_ref is
        // reentrant-safe (its already_dirty guard won't clear our flag).
        *self.gc_dirty.get_mut() = true;

        self.branch_names.remove(&state.name);
        self.branches.remove(&branch);

        // Release tree root ref from the branch's dirty_root.
        self.tree.release_node(dead_dirty);
        // Cascade commit ref counting (may also release commit.root refs).
        self.decrement_ref(dead_head);

        *self.gc_dirty.get_mut() = false;

        Ok(())
    }

    /// Lists all branches as `(BranchId, name)`.
    pub fn list_branches(&self) -> Vec<(BranchId, String)> {
        self.branches.iter().map(|(id, s)| (id, s.name)).collect()
    }

    /// Looks up a branch by name, returning its ID if it exists.
    pub fn branch_id(&self, name: &str) -> Option<BranchId> {
        self.branch_names.get(&name.to_string())
    }

    /// Returns the name of a branch given its ID.
    pub fn branch_name(&self, branch: BranchId) -> Option<String> {
        self.branches.get(&branch).map(|s| s.name)
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
        let mut state = self.get_branch(branch)?;
        let old_root = state.dirty_root;
        state.dirty_root = self.tree.insert(old_root, &key.to_bytes(), &value.encode());
        self.tree.acquire_node(state.dirty_root);
        self.tree.release_node(old_root);
        self.branches.insert(&branch, &state);
        Ok(())
    }

    /// Removes a key from the working state of `branch`.
    pub fn remove(&mut self, branch: BranchId, key: &K) -> Result<()> {
        let mut state = self.get_branch(branch)?;
        let old_root = state.dirty_root;
        state.dirty_root = self.tree.remove(old_root, &key.to_bytes());
        self.tree.acquire_node(state.dirty_root);
        self.tree.release_node(old_root);
        self.branches.insert(&branch, &state);
        Ok(())
    }

    // =================================================================
    // Commit / Rollback
    // =================================================================

    /// Commits the current working state of `branch`, creating a new
    /// immutable [`Commit`].  Returns the commit ID.
    pub fn commit(&mut self, branch: BranchId) -> Result<CommitId> {
        let state = self.get_branch(branch)?;

        // Mark dirty before any structural mutation so that crash
        // recovery (gc → rebuild_ref_counts) will repair orphaned
        // commits or imbalanced ref-counts.
        *self.gc_dirty.get_mut() = true;

        let id = self.next_commit.get_value();
        *self.next_commit.get_mut() = id + 1;

        let parents = if state.head == NO_COMMIT {
            vec![]
        } else {
            vec![state.head]
        };

        // ref_count = 1: the branch HEAD that will point here.
        // Old HEAD: net 0 (loses branch-HEAD, gains parent-link).
        let commit = Commit {
            id,
            root: state.dirty_root,
            parents,
            timestamp_us: now_us(),
            ref_count: 1,
        };
        self.commits.insert(&id, &commit);

        // commit.root now also references dirty_root → acquire.
        self.tree.acquire_node(state.dirty_root);

        // Update branch head; dirty_root stays the same (it IS the snapshot).
        let new_state = BranchState { head: id, ..state };
        self.branches.insert(&branch, &new_state);

        *self.gc_dirty.get_mut() = false;

        Ok(id)
    }

    /// Discards uncommitted changes, resetting the working state to the
    /// branch head.
    pub fn discard(&mut self, branch: BranchId) -> Result<()> {
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
        self.tree.release_node(old_dirty);
        self.branches.insert(&branch, &new_state);
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
        let state = self.get_branch(branch)?;
        let _ = self.get_commit_inner(target)?;

        // A branch with no commits has no ancestors — rolling it back to
        // an arbitrary commit would silently attach it to another
        // branch's history.
        if state.head == NO_COMMIT {
            return Err(VsdbError::Other {
                detail: "target commit is not an ancestor of this branch's head".into(),
            });
        }

        // Verify target is reachable from the branch head.
        if target != state.head {
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
                if let Some(c) = self.commits.get(&cur) {
                    queue.extend_from_slice(&c.parents);
                }
            }
            if !found {
                return Err(VsdbError::Other {
                    detail: "target commit is not an ancestor of this branch's head"
                        .into(),
                });
            }
        } else if self.has_uncommitted(branch)? {
            // Rolling back to the current HEAD discards the dirty_root,
            // silently losing uncommitted changes.  Reject and tell the
            // caller to use `discard()` instead.
            return Err(VsdbError::UncommittedChanges { branch_id: branch });
        }

        // Mark dirty before any structural mutation so that crash
        // recovery (gc → rebuild_ref_counts) will repair orphaned
        // commits or imbalanced ref-counts.
        *self.gc_dirty.get_mut() = true;

        let commit = self.get_commit_inner(target)?;
        let old_head = state.head;
        let old_dirty = state.dirty_root;
        let new_state = BranchState {
            name: state.name,
            head: target,
            dirty_root: commit.root,
        };
        self.branches.insert(&branch, &new_state);

        // Tree root: dirty_root changes to commit.root.
        self.tree.acquire_node(commit.root);
        self.tree.release_node(old_dirty);

        // Commit ref counts: target gains a branch-HEAD, old head
        // loses one.  Increment FIRST to protect target from cascade.
        self.increment_ref(target);
        self.decrement_ref(old_head);

        *self.gc_dirty.get_mut() = false;

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
    /// is created).  Otherwise creates a merge commit on `target` with two
    /// parents.
    pub fn merge(&mut self, source: BranchId, target: BranchId) -> Result<CommitId> {
        if source == target {
            return Err(VsdbError::Other {
                detail: "cannot merge a branch into itself".into(),
            });
        }

        // Reject if either branch has uncommitted changes.
        if self.has_uncommitted(source)? {
            return Err(VsdbError::UncommittedChanges { branch_id: source });
        }
        if self.has_uncommitted(target)? {
            return Err(VsdbError::UncommittedChanges { branch_id: target });
        }

        let src = self.get_branch(source)?;
        let tgt = self.get_branch(target)?;

        if src.head == NO_COMMIT {
            return Err(VsdbError::Other {
                detail: format!("source branch {source} has no commits"),
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
        *self.gc_dirty.get_mut() = true;

        if tgt.head == NO_COMMIT {
            // Target is empty — just fast-forward.
            let src_commit = self.get_commit_inner(src.head)?;
            let new_state = BranchState {
                head: src.head,
                dirty_root: src_commit.root,
                ..tgt
            };
            self.branches.insert(&target, &new_state);
            // Target branch HEAD now points to src.head → +1 ref.
            self.increment_ref(src.head);
            // Tree root: dirty_root changes to src_commit.root.
            self.tree.acquire_node(src_commit.root);
            self.tree.release_node(tgt.dirty_root);
            *self.gc_dirty.get_mut() = false;
            return Ok(src.head);
        }

        let src_commit = self.get_commit_inner(src.head)?;
        let tgt_commit = self.get_commit_inner(tgt.head)?;

        // Find common ancestor.
        let ancestor_id = self.find_common_ancestor(src.head, tgt.head);
        let ancestor_root = match ancestor_id {
            Some(aid) => self.get_commit_inner(aid)?.root,
            None => EMPTY_ROOT,
        };

        let merged_root = super::merge::three_way_merge(
            &mut self.tree,
            ancestor_root,
            src_commit.root,
            tgt_commit.root,
        );

        // Create merge commit.
        let id = self.next_commit.get_value();
        *self.next_commit.get_mut() = id + 1;

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
        self.commits.insert(&id, &commit);

        let new_state = BranchState {
            head: id,
            dirty_root: merged_root,
            ..tgt
        };
        self.branches.insert(&target, &new_state);

        // Tree root: commit.root + dirty_root both reference merged_root.
        self.tree.acquire_node(merged_root); // commit.root
        self.tree.acquire_node(merged_root); // dirty_root
        self.tree.release_node(tgt.dirty_root); // old target dirty

        self.increment_ref(src.head);

        *self.gc_dirty.get_mut() = false;

        Ok(id)
    }

    /// Finds the lowest (most-recent) common ancestor of two commits.
    ///
    /// Walks the commit DAG in **descending `CommitId` order** using a
    /// max-heap, flagging each commit with the side(s) it is reachable
    /// from (`a`, `b`, or both).  `CommitId`s are monotonic — a parent is
    /// always created before its child, so `parent.id < child.id`.  That
    /// means by the time a commit is popped, every descendant that could
    /// flag it has already been processed, so the first commit popped with
    /// **both** flags is the highest-id (i.e. lowest / most-recent) common
    /// ancestor.
    ///
    /// A naive "first BFS intersection" is **not** correct here: a
    /// criss-cross merge (a merge whose second parent is an older commit)
    /// introduces a shortcut edge that can surface a *higher* common
    /// ancestor first.  Feeding that too-old base into `three_way_merge`
    /// would misclassify a single-sided change as a conflict and silently
    /// drop it under the source-wins rule.
    fn find_common_ancestor(&self, a: CommitId, b: CommitId) -> Option<CommitId> {
        const FROM_A: u8 = 0b01;
        const FROM_B: u8 = 0b10;
        const BOTH: u8 = FROM_A | FROM_B;

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

        mark(&mut flags, &mut heap, a, FROM_A);
        mark(&mut flags, &mut heap, b, FROM_B);

        while let Some(id) = heap.pop() {
            // Every id pushed onto the heap was first inserted into `flags`.
            let f = flags[&id];
            if f == BOTH {
                return Some(id);
            }
            if let Some(c) = self.commits.get(&id) {
                for &parent in &c.parents {
                    mark(&mut flags, &mut heap, parent, f);
                }
            }
        }
        None
    }

    // =================================================================
    // History
    // =================================================================

    /// Returns the lowest common ancestor (fork point) of two commits.
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
        let mut cur = from;
        let mut count = 0u64;
        while cur != ancestor {
            if cur == NO_COMMIT {
                return None;
            }
            let c = self.commits.get(&cur)?;
            cur = c.parents.first().copied().unwrap_or(NO_COMMIT);
            count += 1;
        }
        Some(count)
    }

    /// Retrieves a commit by its ID.
    pub fn get_commit(&self, commit_id: CommitId) -> Option<Commit> {
        self.commits.get(&commit_id)
    }

    /// Returns the commit at the head of `branch`.
    pub fn head_commit(&self, branch: BranchId) -> Result<Option<Commit>> {
        let state = self.get_branch(branch)?;
        if state.head == NO_COMMIT {
            Ok(None)
        } else {
            Ok(self.commits.get(&state.head))
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
            if let Some(c) = self.commits.get(&cur) {
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
    /// Returns a list of [`DiffEntry`] in
    /// ascending key order, describing every key that was added, removed,
    /// or modified between `from` and `to`.
    pub fn diff_commits(&self, from: CommitId, to: CommitId) -> Result<Vec<DiffEntry>> {
        let from_commit = self.get_commit_inner(from)?;
        let to_commit = self.get_commit_inner(to)?;
        Ok(diff_roots(&self.tree, from_commit.root, to_commit.root))
    }

    /// Computes the diff of uncommitted (working) changes on `branch`.
    ///
    /// Analogous to `git diff` (unstaged changes relative to HEAD).
    pub fn diff_uncommitted(&self, branch: BranchId) -> Result<Vec<DiffEntry>> {
        let state = self.get_branch(branch)?;
        let head_root = if state.head == NO_COMMIT {
            EMPTY_ROOT
        } else {
            self.get_commit_inner(state.head)?.root
        };
        Ok(diff_roots(&self.tree, head_root, state.dirty_root))
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
    pub fn gc(&mut self) {
        // 1. Crash recovery: rebuild ref counts if the dirty flag is
        //    set, or if any commit has ref_count == 0 (migration from
        //    pre-ref-count data).
        if self.gc_dirty.get_value()
            || self.commits.iter().any(|(_, c)| c.ref_count == 0)
        {
            self.rebuild_ref_counts();
        }

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
    }

    // =================================================================
    // Branch handles
    // =================================================================

    /// Returns a read-only handle bound to the given branch.
    ///
    /// All operations on the returned [`Branch`]
    /// automatically target this branch, removing the need to pass a
    /// `BranchId` on every call.
    pub fn branch(&self, id: BranchId) -> Result<Branch<'_, K, V>> {
        self.get_branch(id)?;
        Ok(Branch { map: self, id })
    }

    /// Returns a mutable handle bound to the given branch.
    ///
    /// All operations on the returned [`BranchMut`]
    /// automatically target this branch.
    pub fn branch_mut(&mut self, id: BranchId) -> Result<BranchMut<'_, K, V>> {
        self.get_branch(id)?;
        Ok(BranchMut { map: self, id })
    }

    /// Shortcut for `self.branch(self.main_branch())`.
    pub fn main(&self) -> Branch<'_, K, V> {
        Branch {
            map: self,
            id: self.main_branch(),
        }
    }

    /// Shortcut for `self.branch_mut(self.main_branch())`.
    pub fn main_mut(&mut self) -> BranchMut<'_, K, V> {
        let id = self.main_branch();
        BranchMut { map: self, id }
    }

    // =================================================================
    // Reference counting
    // =================================================================

    /// Increments the ref_count of the given commit by 1.
    fn increment_ref(&mut self, commit_id: CommitId) {
        if commit_id == NO_COMMIT {
            return;
        }
        if let Some(mut c) = self.commits.get(&commit_id) {
            c.ref_count += 1;
            self.commits.insert(&commit_id, &c);
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
        *self.gc_dirty.get_mut() = true;

        let mut work = vec![commit_id];
        while let Some(id) = work.pop() {
            if id == NO_COMMIT {
                continue;
            }
            let Some(mut c) = self.commits.get(&id) else {
                continue; // already deleted (crash recovery case)
            };
            c.ref_count = c.ref_count.saturating_sub(1);
            if c.ref_count == 0 {
                let parents = c.parents.clone();
                // Release the B+ tree root owned by this commit.
                self.tree.release_node(c.root);
                self.commits.remove(&id);
                work.extend(parents);
            } else {
                self.commits.insert(&id, &c);
            }
        }

        if !already_dirty {
            *self.gc_dirty.get_mut() = false;
        }
    }
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}
