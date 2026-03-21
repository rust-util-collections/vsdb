//!
//! [`VerMap`] — a typed, versioned key-value map with branch / commit /
//! merge support, modelled after Git semantics.
//!

use super::{BranchId, Commit, CommitId, NO_COMMIT};
use crate::common::ende::{KeyEnDeOrdered, ValueEnDe};
use crate::{Mapx, MapxOrd, Orphan};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};
use vsdb_core::basic::persistent_btree::{EMPTY_ROOT, NodeId, PersistentBTree};

// =========================================================================
// BranchState
// =========================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BranchState {
    name: String,
    /// The latest commit on this branch.
    head: CommitId,
    /// B+ tree root of uncommitted (working) state.
    /// Starts as the root of `head`'s commit; mutations update this.
    dirty_root: NodeId,
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
/// 8. **GC** — `gc` reclaims commits and B+ tree nodes that are no longer
///    reachable from any live branch.
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
/// // 5. Clean up: delete the feature branch and run GC.
/// m.delete_branch(feat).unwrap();
/// m.gc();
///
/// fs::remove_dir_all(&dir).unwrap();
/// ```
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(bound = "")]
pub struct VerMap<K, V> {
    /// The underlying persistent B+ tree (shared node pool).
    tree: PersistentBTree,

    /// CommitId → Commit
    commits: MapxOrd<u64, Commit>,

    /// BranchId → BranchState
    branches: MapxOrd<u64, BranchState>,

    /// branch name → BranchId
    branch_names: Mapx<String, u64>,

    /// ID allocators
    next_commit: Orphan<u64>,
    next_branch: Orphan<u64>,

    /// The branch currently designated as "main" (protected from deletion).
    main_branch: Orphan<u64>,

    /// Dead branch heads awaiting GC. Persisted for crash recovery.
    /// Maps a monotonic sequence → dead head CommitId.
    #[serde(default)]
    pending_gc: MapxOrd<u64, u64>,
    /// Allocator for pending_gc sequence IDs.
    #[serde(default)]
    next_gc_seq: Orphan<u64>,

    #[serde(skip)]
    _phantom: PhantomData<(K, V)>,
}

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
            pending_gc: MapxOrd::new(),
            next_gc_seq: Orphan::new(1),
            _phantom: PhantomData,
        }
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
        self.branches.get(&branch).c(d!("branch not found"))?;
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
            return Err(eg!("branch '{}' already exists", name));
        }
        let src = self
            .branches
            .get(&source_branch)
            .c(d!("source branch not found"))?;

        let id = self.next_branch.get_value();
        *self.next_branch.get_mut() = id + 1;

        let state = BranchState {
            name: name.into(),
            head: src.head,
            dirty_root: src.dirty_root,
        };
        self.branches.insert(&id, &state);
        self.branch_names.insert(&name.to_string(), &id);
        Ok(id)
    }

    /// Deletes a branch.
    ///
    /// The dead branch head is recorded in [`pending_gc`] for crash-safe
    /// cleanup.  Call [`gc`](Self::gc) to actually reclaim the orphaned
    /// commits and B+ tree nodes, or rely on
    /// [`recover_pending_gc`](Self::recover_pending_gc) after a restart.
    pub fn delete_branch(&mut self, branch: BranchId) -> Result<()> {
        if branch == self.main_branch.get_value() {
            return Err(eg!("cannot delete the main branch"));
        }
        let state = self.branches.get(&branch).c(d!("branch not found"))?;

        // Record the dead head for crash-safe cleanup.
        if state.head != NO_COMMIT {
            let seq = self.next_gc_seq.get_value();
            *self.next_gc_seq.get_mut() = seq + 1;
            self.pending_gc.insert(&seq, &state.head);
        }

        self.branch_names.remove(&state.name);
        self.branches.remove(&branch);

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
        let state = self.branches.get(&branch).c(d!("branch not found"))?;
        if state.head == NO_COMMIT {
            Ok(state.dirty_root != EMPTY_ROOT)
        } else {
            let head_root = self
                .commits
                .get(&state.head)
                .c(d!("head commit {} missing", state.head))?
                .root;
            Ok(state.dirty_root != head_root)
        }
    }

    // =================================================================
    // Read
    // =================================================================

    /// Reads a value from the working state of `branch`.
    pub fn get(&self, branch: BranchId, key: &K) -> Result<Option<V>> {
        let state = self.branches.get(&branch).c(d!("branch not found"))?;
        let raw = self.tree.get(state.dirty_root, &key.to_bytes());
        match raw {
            Some(v) => Ok(Some(pnk!(V::decode(&v)))),
            None => Ok(None),
        }
    }

    /// Reads a value at a specific historical commit.
    pub fn get_at_commit(&self, commit_id: CommitId, key: &K) -> Result<Option<V>> {
        let commit = self.commits.get(&commit_id).c(d!("commit not found"))?;
        let raw = self.tree.get(commit.root, &key.to_bytes());
        match raw {
            Some(v) => Ok(Some(pnk!(V::decode(&v)))),
            None => Ok(None),
        }
    }

    /// Checks if `key` exists in the working state of `branch`.
    pub fn contains_key(&self, branch: BranchId, key: &K) -> Result<bool> {
        let state = self.branches.get(&branch).c(d!("branch not found"))?;
        Ok(self.tree.contains_key(state.dirty_root, &key.to_bytes()))
    }

    /// Iterates all entries on `branch` in ascending key order.
    pub fn iter(&self, branch: BranchId) -> Result<impl Iterator<Item = (K, V)> + '_> {
        let state = self.branches.get(&branch).c(d!("branch not found"))?;
        Ok(self
            .tree
            .iter(state.dirty_root)
            .map(|(k, v)| (pnk!(K::from_slice(&k)), pnk!(V::decode(&v)))))
    }

    /// Iterates entries in `[lo, hi)` on `branch` in ascending key order.
    pub fn range(
        &self,
        branch: BranchId,
        lo: Bound<&K>,
        hi: Bound<&K>,
    ) -> Result<impl Iterator<Item = (K, V)> + '_> {
        let state = self.branches.get(&branch).c(d!("branch not found"))?;
        let lo_raw = match lo {
            Bound::Included(k) => Bound::Included(k.to_bytes()),
            Bound::Excluded(k) => Bound::Excluded(k.to_bytes()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let hi_raw = match hi {
            Bound::Included(k) => Bound::Included(k.to_bytes()),
            Bound::Excluded(k) => Bound::Excluded(k.to_bytes()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(self
            .tree
            .range(
                state.dirty_root,
                lo_raw.as_ref().map(|v| v.as_slice()),
                hi_raw.as_ref().map(|v| v.as_slice()),
            )
            .map(|(k, v)| (pnk!(K::from_slice(&k)), pnk!(V::decode(&v)))))
    }

    /// Iterates all entries at a specific historical commit.
    pub fn iter_at_commit(
        &self,
        commit_id: CommitId,
    ) -> Result<impl Iterator<Item = (K, V)> + '_> {
        let commit = self.commits.get(&commit_id).c(d!("commit not found"))?;
        Ok(self
            .tree
            .iter(commit.root)
            .map(|(k, v)| (pnk!(K::from_slice(&k)), pnk!(V::decode(&v)))))
    }

    /// Iterates entries in `[lo, hi)` at a specific historical commit
    /// in ascending key order.
    pub fn range_at_commit(
        &self,
        commit_id: CommitId,
        lo: Bound<&K>,
        hi: Bound<&K>,
    ) -> Result<impl Iterator<Item = (K, V)> + '_> {
        let commit = self.commits.get(&commit_id).c(d!("commit not found"))?;
        let lo_raw = match lo {
            Bound::Included(k) => Bound::Included(k.to_bytes()),
            Bound::Excluded(k) => Bound::Excluded(k.to_bytes()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let hi_raw = match hi {
            Bound::Included(k) => Bound::Included(k.to_bytes()),
            Bound::Excluded(k) => Bound::Excluded(k.to_bytes()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(self
            .tree
            .range(
                commit.root,
                lo_raw.as_ref().map(|v| v.as_slice()),
                hi_raw.as_ref().map(|v| v.as_slice()),
            )
            .map(|(k, v)| (pnk!(K::from_slice(&k)), pnk!(V::decode(&v)))))
    }

    /// Iterates all raw (untyped) key-value pairs on a branch.
    ///
    /// Returns `(Vec<u8>, Vec<u8>)` without decoding, useful for
    /// feeding into external consumers (e.g. MPT hash computation).
    pub fn raw_iter(
        &self,
        branch: BranchId,
    ) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_> {
        let state = self.branches.get(&branch).c(d!("branch not found"))?;
        Ok(self.tree.iter(state.dirty_root))
    }

    /// Iterates all raw (untyped) key-value pairs at a historical commit.
    pub fn raw_iter_at_commit(
        &self,
        commit_id: CommitId,
    ) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_> {
        let commit = self.commits.get(&commit_id).c(d!("commit not found"))?;
        Ok(self.tree.iter(commit.root))
    }

    /// Checks if `key` exists at a specific historical commit.
    pub fn contains_key_at_commit(&self, commit_id: CommitId, key: &K) -> Result<bool> {
        let commit = self.commits.get(&commit_id).c(d!("commit not found"))?;
        Ok(self.tree.contains_key(commit.root, &key.to_bytes()))
    }

    // =================================================================
    // Write (working state)
    // =================================================================

    /// Inserts a key-value pair into the working state of `branch`.
    pub fn insert(&mut self, branch: BranchId, key: &K, value: &V) -> Result<()> {
        let mut state = self.branches.get(&branch).c(d!("branch not found"))?;
        state.dirty_root =
            self.tree
                .insert(state.dirty_root, &key.to_bytes(), &value.encode());
        self.branches.insert(&branch, &state);
        Ok(())
    }

    /// Removes a key from the working state of `branch`.
    pub fn remove(&mut self, branch: BranchId, key: &K) -> Result<()> {
        let mut state = self.branches.get(&branch).c(d!("branch not found"))?;
        state.dirty_root = self.tree.remove(state.dirty_root, &key.to_bytes());
        self.branches.insert(&branch, &state);
        Ok(())
    }

    // =================================================================
    // Commit / Rollback
    // =================================================================

    /// Commits the current working state of `branch`, creating a new
    /// immutable [`Commit`].  Returns the commit ID.
    pub fn commit(&mut self, branch: BranchId) -> Result<CommitId> {
        let state = self.branches.get(&branch).c(d!("branch not found"))?;

        let id = self.next_commit.get_value();
        *self.next_commit.get_mut() = id + 1;

        let parents = if state.head == NO_COMMIT {
            vec![]
        } else {
            vec![state.head]
        };

        let commit = Commit {
            id,
            root: state.dirty_root,
            parents,
            timestamp_us: now_us(),
        };
        self.commits.insert(&id, &commit);

        // Update branch head; dirty_root stays the same (it IS the snapshot).
        let new_state = BranchState { head: id, ..state };
        self.branches.insert(&branch, &new_state);
        Ok(id)
    }

    /// Discards uncommitted changes, resetting the working state to the
    /// branch head.
    pub fn discard(&mut self, branch: BranchId) -> Result<()> {
        let state = self.branches.get(&branch).c(d!("branch not found"))?;
        let root = if state.head == NO_COMMIT {
            EMPTY_ROOT
        } else {
            self.commits
                .get(&state.head)
                .c(d!("head commit {} missing", state.head))?
                .root
        };
        let new_state = BranchState {
            dirty_root: root,
            ..state
        };
        self.branches.insert(&branch, &new_state);
        Ok(())
    }

    /// Rolls back `branch` to a previous commit, discarding all commits
    /// after `target` on this branch.
    ///
    /// `target` must be an ancestor of the branch's current head.
    /// The discarded commits are not deleted (they may be reachable from
    /// other branches).  Call [`gc`](Self::gc) to reclaim them.
    pub fn rollback_to(&mut self, branch: BranchId, target: CommitId) -> Result<()> {
        use std::collections::HashSet;

        let state = self.branches.get(&branch).c(d!("branch not found"))?;
        let _ = self.commits.get(&target).c(d!("commit not found"))?;

        // Verify target is reachable from the branch head.
        if state.head != NO_COMMIT && target != state.head {
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
                return Err(eg!(
                    "target commit is not an ancestor of this branch's head"
                ));
            }
        }

        let commit = self
            .commits
            .get(&target)
            .c(d!("target commit {} missing", target))?;
        let new_state = BranchState {
            name: state.name,
            head: target,
            dirty_root: commit.root,
        };
        self.branches.insert(&branch, &new_state);
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
        // Reject if either branch has uncommitted changes.
        if self.has_uncommitted(source)? {
            return Err(eg!("source branch has uncommitted changes"));
        }
        if self.has_uncommitted(target)? {
            return Err(eg!("target branch has uncommitted changes"));
        }

        let src = self
            .branches
            .get(&source)
            .c(d!("source branch not found"))?;
        let tgt = self
            .branches
            .get(&target)
            .c(d!("target branch not found"))?;

        if src.head == NO_COMMIT {
            return Err(eg!("source branch has no commits"));
        }
        if tgt.head == NO_COMMIT {
            // Target is empty — just fast-forward.
            let src_commit = self
                .commits
                .get(&src.head)
                .c(d!("source head commit {} missing", src.head))?;
            let new_state = BranchState {
                head: src.head,
                dirty_root: src_commit.root,
                ..tgt
            };
            self.branches.insert(&target, &new_state);
            return Ok(src.head);
        }

        let src_commit = self
            .commits
            .get(&src.head)
            .c(d!("source head commit {} missing", src.head))?;
        let tgt_commit = self
            .commits
            .get(&tgt.head)
            .c(d!("target head commit {} missing", tgt.head))?;

        // Find common ancestor.
        let ancestor_id = self.find_common_ancestor(src.head, tgt.head);
        let ancestor_root = match ancestor_id {
            Some(aid) => {
                self.commits
                    .get(&aid)
                    .c(d!("ancestor commit {} missing", aid))?
                    .root
            }
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

        let commit = Commit {
            id,
            root: merged_root,
            parents: vec![tgt.head, src.head],
            timestamp_us: now_us(),
        };
        self.commits.insert(&id, &commit);

        let new_state = BranchState {
            head: id,
            dirty_root: merged_root,
            ..tgt
        };
        self.branches.insert(&target, &new_state);
        Ok(id)
    }

    /// Finds the lowest common ancestor of two commits via alternating BFS.
    fn find_common_ancestor(&self, a: CommitId, b: CommitId) -> Option<CommitId> {
        use std::collections::HashSet;

        let mut visited_a = HashSet::new();
        let mut visited_b = HashSet::new();
        let mut queue_a = vec![a];
        let mut queue_b = vec![b];

        loop {
            if queue_a.is_empty() && queue_b.is_empty() {
                return None;
            }
            // Expand a — reuse the same Vec via drain + extend in place.
            let drain_end = queue_a.len();
            for i in 0..drain_end {
                let id = queue_a[i];
                if id == NO_COMMIT || !visited_a.insert(id) {
                    continue;
                }
                if visited_b.contains(&id) {
                    return Some(id);
                }
                if let Some(c) = self.commits.get(&id) {
                    queue_a.extend_from_slice(&c.parents);
                }
            }
            queue_a.drain(..drain_end);

            // Expand b.
            let drain_end = queue_b.len();
            for i in 0..drain_end {
                let id = queue_b[i];
                if id == NO_COMMIT || !visited_b.insert(id) {
                    continue;
                }
                if visited_a.contains(&id) {
                    return Some(id);
                }
                if let Some(c) = self.commits.get(&id) {
                    queue_b.extend_from_slice(&c.parents);
                }
            }
            queue_b.drain(..drain_end);
        }
    }

    // =================================================================
    // History
    // =================================================================

    /// Returns the lowest common ancestor (fork point) of two commits.
    ///
    /// Useful for blockchain scenarios: given two chain tips, this finds
    /// the block where they diverged.  Returns `None` only if the two
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
        let state = self.branches.get(&branch).c(d!("branch not found"))?;
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
        let state = self.branches.get(&branch).c(d!("branch not found"))?;
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
    /// Returns a list of [`DiffEntry`](super::diff::DiffEntry) in
    /// ascending key order, describing every key that was added, removed,
    /// or modified between `from` and `to`.
    pub fn diff_commits(
        &self,
        from: CommitId,
        to: CommitId,
    ) -> Result<Vec<super::diff::DiffEntry>> {
        let from_commit = self.commits.get(&from).c(d!("from commit not found"))?;
        let to_commit = self.commits.get(&to).c(d!("to commit not found"))?;
        Ok(super::diff::diff_roots(
            &self.tree,
            from_commit.root,
            to_commit.root,
        ))
    }

    /// Computes the diff of uncommitted (working) changes on `branch`.
    ///
    /// Analogous to `git diff` (unstaged changes relative to HEAD).
    pub fn diff_uncommitted(
        &self,
        branch: BranchId,
    ) -> Result<Vec<super::diff::DiffEntry>> {
        let state = self.branches.get(&branch).c(d!("branch not found"))?;
        let head_root = if state.head == NO_COMMIT {
            EMPTY_ROOT
        } else {
            self.commits
                .get(&state.head)
                .c(d!("head commit {} missing", state.head))?
                .root
        };
        Ok(super::diff::diff_roots(
            &self.tree,
            head_root,
            state.dirty_root,
        ))
    }

    // =================================================================
    // GC
    // =================================================================

    /// Garbage-collects unreachable commits and B+ tree nodes.
    ///
    /// Since [`delete_branch`](Self::delete_branch) now performs automatic
    /// incremental cleanup, calling `gc` explicitly is rarely needed.
    /// It may still be useful as a full sweep to reclaim garbage from
    /// older data or after crash recovery.
    pub fn gc(&mut self) {
        // Process any crash-recovery pending entries first.
        self.process_pending_gc();

        use std::collections::HashSet;

        // 1. Find all reachable commits.
        let mut reachable_commits = HashSet::new();
        let mut queue: Vec<CommitId> = self
            .branches
            .iter()
            .map(|(_, s)| s.head)
            .filter(|&h| h != NO_COMMIT)
            .collect();

        while let Some(id) = queue.pop() {
            if reachable_commits.insert(id)
                && let Some(c) = self.commits.get(&id)
            {
                queue.extend_from_slice(&c.parents);
            }
        }

        // 2. Delete unreachable commits.
        let all_commits: Vec<u64> = self.commits.iter().map(|(id, _)| id).collect();
        for id in all_commits {
            if !reachable_commits.contains(&id) {
                self.commits.remove(&id);
            }
        }

        // 3. Collect live tree roots (from reachable commits + dirty roots).
        let mut live_roots: Vec<NodeId> = reachable_commits
            .iter()
            .filter_map(|id| self.commits.get(id).map(|c| c.root))
            .collect();
        for (_, s) in self.branches.iter() {
            if s.dirty_root != EMPTY_ROOT {
                live_roots.push(s.dirty_root);
            }
        }

        // 4. GC the B+ tree node pool.
        self.tree.gc(&live_roots);
    }

    /// Processes all pending GC entries, removing unreachable commits
    /// and their associated B+ tree nodes.
    ///
    /// This method is **idempotent** — it can be called multiple times
    /// or interrupted by a crash and resumed safely. The `pending_gc`
    /// entries are only removed after cleanup is complete.
    fn process_pending_gc(&mut self) {
        use std::collections::HashSet;

        let entries: Vec<(u64, u64)> = self.pending_gc.iter().collect();
        if entries.is_empty() {
            return;
        }

        // 1. Build the set of reachable commits from all live branches.
        let mut live_commits = HashSet::new();
        let mut queue: Vec<CommitId> = self
            .branches
            .iter()
            .map(|(_, s)| s.head)
            .filter(|&h| h != NO_COMMIT)
            .collect();
        while let Some(id) = queue.pop() {
            if live_commits.insert(id) {
                if let Some(c) = self.commits.get(&id) {
                    queue.extend_from_slice(&c.parents);
                }
            }
        }

        // 2. For each pending entry, BFS from dead head to find dead commits.
        let mut all_dead_commits = HashSet::new();
        let mut processed_seqs = Vec::new();

        for &(seq, dead_head) in &entries {
            if live_commits.contains(&dead_head) {
                // This commit is still reachable (e.g. shared ancestor).
                processed_seqs.push(seq);
                continue;
            }

            let mut dead_queue = vec![dead_head];
            while let Some(id) = dead_queue.pop() {
                if live_commits.contains(&id) {
                    continue; // Shared ancestor — stop traversal.
                }
                if all_dead_commits.insert(id) {
                    if let Some(c) = self.commits.get(&id) {
                        dead_queue.extend_from_slice(&c.parents);
                    }
                }
            }
            processed_seqs.push(seq);
        }

        if all_dead_commits.is_empty() {
            // Nothing to clean, just remove the pending entries.
            for seq in processed_seqs {
                self.pending_gc.remove(&seq);
            }
            return;
        }

        // 3. Collect dead tree roots and live tree roots.
        let dead_roots: Vec<NodeId> = all_dead_commits
            .iter()
            .filter_map(|id| self.commits.get(id).map(|c| c.root))
            .collect();

        let mut live_roots: Vec<NodeId> = live_commits
            .iter()
            .filter_map(|id| self.commits.get(id).map(|c| c.root))
            .collect();
        for (_, s) in self.branches.iter() {
            if s.dirty_root != EMPTY_ROOT {
                live_roots.push(s.dirty_root);
            }
        }

        // 4. Delete dead commits.
        for id in &all_dead_commits {
            self.commits.remove(id);
        }

        // 5. GC the B+ tree — targeted sweep of dead roots,
        //    preserving all nodes reachable from live roots.
        self.tree.gc_targeted(&dead_roots, &live_roots);

        // 6. Remove processed pending_gc entries.
        for seq in processed_seqs {
            self.pending_gc.remove(&seq);
        }
    }

    /// Reprocesses any pending GC entries left over from a previous crash.
    /// Call this once after opening/deserializing a `VerMap`.
    pub fn recover_pending_gc(&mut self) {
        self.process_pending_gc();
    }
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}
