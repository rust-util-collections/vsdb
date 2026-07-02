//! Reference-count recovery for the commit DAG and B+ tree node pool.
//!
//! Called after deserialisation and on crash recovery.

use std::collections::{HashMap, HashSet};

use crate::basic::persistent_btree::{EMPTY_ROOT, NodeId};

use super::map::VerMap;
use super::{CommitId, NO_COMMIT};

impl<K, V> VerMap<K, V> {
    /// Rebuilds the B+ tree's in-memory ref-count map from the
    /// current set of live roots (all commit roots + dirty roots).
    ///
    /// Called after every deserialization path (serde, from_meta)
    /// because PersistentBTree's Deserialize sets `ref_counts_ready = false`.
    pub(crate) fn rebuild_tree_ref_counts(&mut self) {
        let mut live_roots: Vec<NodeId> =
            self.commits.iter().map(|(_, c)| c.root).collect();
        for (_, s) in self.branches.iter() {
            if s.dirty_root != EMPTY_ROOT {
                live_roots.push(s.dirty_root);
            }
        }
        self.tree.rebuild_ref_counts(&live_roots);
    }

    pub(crate) fn repair_commit_ref_counts_if_needed(&mut self) {
        if self.gc_dirty.get_value()
            || self.commits.iter().any(|(_, c)| c.ref_count == 0)
        {
            self.rebuild_ref_counts();
        }
    }

    /// Rebuilds all commit ref_counts from scratch by walking all
    /// live branches.  Hard-deletes any unreachable commits.
    ///
    /// Called on crash recovery (`gc_dirty == true`) or when migrating
    /// from pre-ref-count data (`ref_count == 0` on all commits).
    pub(crate) fn rebuild_ref_counts(&mut self) {
        // 1. Walk all branches to find live commits via BFS.
        let mut reachable = HashSet::new();
        let mut ref_counts: HashMap<CommitId, u32> = HashMap::new();
        let mut queue: Vec<CommitId> = Vec::new();

        // Branch HEADs contribute +1 each.
        for (_, s) in self.branches.iter() {
            if s.head != NO_COMMIT {
                *ref_counts.entry(s.head).or_insert(0) += 1;
                queue.push(s.head);
            }
        }

        // BFS — parent links contribute +1 each.
        while let Some(id) = queue.pop() {
            if !reachable.insert(id) {
                continue;
            }
            if let Some(c) = self.commits.get(&id) {
                for &parent in &c.parents {
                    if parent != NO_COMMIT {
                        *ref_counts.entry(parent).or_insert(0) += 1;
                        queue.push(parent);
                    }
                }
            }
        }

        // 2. Update ref_counts on all reachable commits.
        for &id in &reachable {
            if let Some(mut c) = self.commits.get(&id) {
                let correct = *ref_counts.get(&id).unwrap_or(&0);
                if c.ref_count != correct {
                    c.ref_count = correct;
                    self.commits.insert(&id, &c);
                }
            }
        }

        // 3. Hard-delete any unreachable commits.
        let all_ids: Vec<u64> = self.commits.iter().map(|(id, _)| id).collect();
        for id in all_ids {
            if !reachable.contains(&id) {
                self.commits.remove(&id);
            }
        }

        // 4. Clear the dirty flag.
        *self.gc_dirty.get_mut() = false;
    }
}
