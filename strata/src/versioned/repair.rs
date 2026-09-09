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
        self.branch_names.clear();
        for (id, state) in self.branches.iter() {
            if seen.insert(state.name.clone()) {
                self.branch_names.insert(&state.name, &id);
            }
        }
    }

    /// Rebuilds the B+ tree's in-memory ref-count map from the
    /// current set of live roots (all commit roots + dirty roots).
    ///
    /// Called after every deserialization path (serde, from_meta) because a
    /// newly restored node-pool runtime starts without reference counts.
    pub(crate) fn rebuild_tree_ref_counts(&mut self) {
        // No old durable pointer may survive a sweep that retires its nodes.
        // The helper is a no-op for read-only restoration.
        self.sync_storage();
        let mut live_roots: Vec<NodeId> =
            self.commits.iter().map(|(_, c)| c.root).collect();
        for (_, s) in self.branches.iter() {
            if s.dirty_root != EMPTY_ROOT {
                live_roots.push(s.dirty_root);
            }
        }
        self.tree.rebuild_ref_counts(&live_roots);
    }

    pub(crate) fn repair_commit_ref_counts_if_needed(&mut self) -> Result<()> {
        // Validate even clean/read-only restores before changing any index or
        // reclaiming anything. Missing HEADs/parents are corruption, not proof
        // that the remaining stored history is unreachable.
        self.validate_commit_graph()?;

        // Read-only restores must not rewrite the branch-name index or
        // crash-recovery metadata. Queries use the authoritative branch
        // table directly where needed, while tree runtime state is rebuilt
        // separately below without changing durable data.
        if self.namespace().is_read_only() {
            return Ok(());
        }
        self.rebuild_branch_name_index();
        if self.gc_dirty.get_value()
            || self.commits.iter().any(|(_, c)| c.ref_count == 0)
        {
            self.rebuild_ref_counts()?;
        }
        Ok(())
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
                .get(&id)
                .ok_or(VsdbError::CommitNotFound { commit_id: id })?;
            for &parent in &c.parents {
                if parent != NO_COMMIT {
                    *ref_counts.entry(parent).or_insert(0) += 1;
                    queue.push(parent);
                }
            }
        }
        Ok(ref_counts)
    }

    /// Rebuilds all commit ref_counts from scratch by walking all
    /// live branches. Hard-deletes any unreachable commits only after the
    /// entire reachable graph has been validated.
    ///
    /// Called on crash recovery (`gc_dirty == true`) or when migrating
    /// from pre-ref-count data (`ref_count == 0` on all commits).
    pub(crate) fn rebuild_ref_counts(&mut self) -> Result<()> {
        let ref_counts = self.validate_commit_graph()?;

        // A legacy zero-count repair may begin with a false marker. Pin true
        // before changing counts, and settle the graph before deleting rows.
        self.begin_ref_update();
        self.tree.nodes.sync_wal();
        self.commits.sync_wal();
        self.branches.sync_wal();

        for (&id, &correct) in &ref_counts {
            // The full graph was validated above and structural writers are
            // serialized, so every reachable commit still exists.
            let mut c = self.commits.get(&id).expect("validated commit disappeared");
            if c.ref_count != correct {
                c.ref_count = correct;
                self.commits.insert(&id, &c);
            }
        }

        let all_ids: Vec<u64> = self.commits.iter().map(|(id, _)| id).collect();
        for id in all_ids {
            if !ref_counts.contains_key(&id) {
                self.commits.remove(&id);
            }
        }

        // A durable false marker must never outlive unsynced count repairs.
        self.commits.sync_wal();
        self.end_ref_update();
        Ok(())
    }
}
