//! Reference-count recovery for the commit DAG and B+ tree node pool.
//!
//! Called after deserialisation and on crash recovery.

use std::collections::{HashMap, HashSet};

use crate::{
    basic::persistent_btree::{EMPTY_ROOT, NodeId},
    common::error::{Result, VsdbError},
};

use super::{CommitId, NO_COMMIT, map::VerMap};

impl<K, V> VerMap<K, V> {
    pub(crate) fn rebuild_branch_name_index(&mut self) {
        let mut seen = HashSet::new();
        // Preserve valid rows so concurrent readers through another handle
        // never lose a live name while this derived index is reconciled.
        for (id, state) in self.branches.iter() {
            if seen.insert(state.name.clone())
                && self.branch_names.get(&state.name) != Some(id)
            {
                self.branch_names.insert(&state.name, &id);
            }
        }
        let stale: Vec<_> = self
            .branch_names
            .keys()
            .filter(|name| !seen.contains(name))
            .collect();
        for name in stale {
            self.branch_names.remove(&name);
        }
    }

    /// Establishes the deep copy's independent tree owners from its
    /// copied commits and branches before returning the new map.
    pub(crate) fn rebuild_tree_ref_counts(&mut self) {
        // No old durable pointer may survive a sweep that retires its nodes.
        self.sync_storage();
        self.tree.register_deferred_reclaims();
        let mut live_roots: Vec<NodeId> =
            self.commits.iter().map(|(_, c)| c.root).collect();
        for (_, s) in self.branches.iter() {
            if s.dirty_root != EMPTY_ROOT {
                live_roots.push(s.dirty_root);
            }
        }
        self.tree.rebuild_ref_counts(&live_roots);
    }

    /// Validate all reachable history and working roots before removing any
    /// rows. Reuse the traced children to install counts after durable repair.
    pub(crate) fn recover_ref_counts(&mut self, restoring: bool) -> Result<()> {
        let ref_counts = self.validate_commit_graph()?;
        let mut required: Vec<_> =
            self.branches.iter().map(|(_, s)| s.dirty_root).collect();
        let mut orphan_roots = Vec::new();
        let mut has_zero_count = false;
        for (id, commit) in self.commits.iter() {
            has_zero_count |= commit.ref_count == 0;
            if ref_counts.contains_key(&CommitId(id)) {
                required.push(commit.root);
            } else {
                orphan_roots.push(commit.root);
            }
        }
        let counts = self.tree.required_node_refs(&required)?;
        if !self.namespace().is_read_only() {
            if restoring {
                self.repair_id_allocators();
                self.rebuild_branch_name_index();
            }
            if has_zero_count || self.gc_dirty.get_value() {
                self.apply_ref_count_repair(ref_counts);
                orphan_roots.clear();
            }
        }

        // Commit removals and root publications must be durable before sweep.
        self.sync_storage();
        self.tree.finish_ref_counts(counts, &orphan_roots);
        self.tree.register_deferred_reclaims();
        Ok(())
    }

    /// Advances the commit/branch id allocators past every stored row.
    ///
    /// Writable restores only: read-only callers return before this point.
    /// The allocator rows are advanced before the repair can return, so a
    /// subsequent `commit`/`create_branch` cannot reuse an existing id.
    fn repair_id_allocators(&mut self) {
        let max_commit = self
            .commits
            .iter()
            .map(|(id, _)| id)
            .max()
            .unwrap_or(NO_COMMIT.0);
        let max_branch = self
            .branches
            .iter()
            .map(|(id, _)| id)
            .max()
            .unwrap_or(NO_COMMIT.0);

        let current_commit = self.next_commit.get_value();
        let current_branch = self.next_branch.get_value();
        let next_commit = current_commit.max(max_commit.saturating_add(1));
        let next_branch = current_branch.max(max_branch.saturating_add(1));
        let commit_changed = next_commit != current_commit;
        let branch_changed = next_branch != current_branch;

        if commit_changed {
            *self.next_commit.get_mut() = next_commit;
        }
        if branch_changed {
            *self.next_branch.get_mut() = next_branch;
        }
        if commit_changed || branch_changed {
            self.fence(|m| {
                m.next_commit.sync_wal();
                m.next_branch.sync_wal();
            });
        }
    }

    /// Counts every branch HEAD and reachable parent edge without writing.
    /// A missing reachable record must be reported before any orphan cleanup.
    pub(crate) fn validate_commit_graph(&self) -> Result<HashMap<CommitId, u32>> {
        let mut visited = HashSet::new();
        let mut ref_counts: HashMap<CommitId, u32> = HashMap::new();
        let mut queue: Vec<CommitId> = Vec::new();

        for (_, s) in self.branches.iter() {
            if s.head != NO_COMMIT {
                *ref_counts.entry(s.head).or_insert(0) += 1;
                queue.push(s.head);
            }
        }

        while let Some(id) = queue.pop() {
            if !visited.insert(id) {
                continue;
            }
            let c = self
                .commits
                .get(&id.0)
                .ok_or(VsdbError::CommitNotFound { commit_id: id.0 })?;
            if c.id != id {
                return Err(VsdbError::Corrupt {
                    detail: format!("commit row {} carries id {}", id.0, c.id.0),
                });
            }
            for &parent in &c.parents {
                if parent == NO_COMMIT {
                    return Err(VsdbError::Corrupt {
                        detail: format!(
                            "commit {} lists the sentinel as a parent",
                            id.0
                        ),
                    });
                }
                if parent.0 >= id.0 {
                    return Err(VsdbError::Corrupt {
                        detail: format!(
                            "commit {} lists non-older parent {}",
                            id.0, parent.0
                        ),
                    });
                }
                *ref_counts.entry(parent).or_insert(0) += 1;
                queue.push(parent);
            }
        }
        Ok(ref_counts)
    }

    /// Apply a count map validated before any repair or reclamation starts.
    /// Structural writers are serialized throughout validation and repair.
    fn apply_ref_count_repair(&mut self, ref_counts: HashMap<CommitId, u32>) {
        // A legacy zero-count repair may begin with a false marker. Pin true
        // before changing counts, and settle the graph before deleting rows.
        self.begin_ref_update();
        self.fence(|m| {
            m.tree.nodes.sync_wal();
            m.commits.sync_wal();
            m.branches.sync_wal();
        });

        for (&id, &correct) in &ref_counts {
            // The full graph was validated above and structural writers are
            // serialized, so every reachable commit still exists.
            let mut c = self
                .commits
                .get(&id.0)
                .expect("validated commit disappeared");
            if c.ref_count != correct {
                c.ref_count = correct;
                self.commits.insert(&id.0, &c);
            }
        }

        let all_ids: Vec<CommitId> =
            self.commits.iter().map(|(id, _)| CommitId(id)).collect();
        for id in all_ids {
            if !ref_counts.contains_key(&id) {
                self.commits.remove(&id.0);
            }
        }

        // A durable false marker must never outlive unsynced count repairs.
        self.fence(|m| m.commits.sync_wal());
        self.end_ref_update();
    }
}
