//!
//! [`VerMapWithProof`] — a versioned KV map with Merkle root computation.
//!
//! Combines [`VerMap`] (versioning, branching, merging) with
//! [`MptCalc`](super::MptCalc) (stateless Merkle Patricia Trie)
//! to provide cryptographic commitments over versioned state.
//!
//! The MPT is treated as a disposable computation layer: it holds an
//! in-memory trie that can be rebuilt from any VerMap snapshot at any
//! time.  An optional on-disk cache avoids full rebuilds on restart.

use crate::common::ende::{KeyEnDeOrdered, ValueEnDe};
use crate::versioned::diff::DiffEntry;
use crate::versioned::map::VerMap;
use crate::versioned::{BranchId, CommitId};
use ruc::*;
use std::path::Path;

use super::MptCalc;

/// A versioned key-value map with Merkle root hash computation.
///
/// Wraps a [`VerMap<K, V>`] and an [`MptCalc`] to provide a
/// [`merkle_root`](Self::merkle_root) method that lazily computes
/// the 32-byte Merkle root hash for any branch or commit.
///
/// # Incremental updates
///
/// The internal MPT tracks a *sync point* — the commit it was last
/// synchronized to.  When `merkle_root` is called:
///
/// 1. If the MPT is already synced to the target → return cached hash.
/// 2. If the target is reachable via diff from the sync point →
///    apply diff incrementally.
/// 3. Otherwise → full rebuild from the store's iterator.
///
/// # Cache persistence
///
/// [`save_cache`](Self::save_cache) / [`load_cache`](Self::load_cache)
/// serialize the MPT to disk so that restarts only require an
/// incremental diff rather than a full rebuild.  The cache is
/// disposable — if lost or corrupted, the trie is rebuilt from VerMap.
pub struct VerMapWithProof<K, V> {
    map: VerMap<K, V>,
    mpt: MptCalc,
    /// Snapshot of `mpt` at the last synced commit (before dirty overlay).
    /// Used to reset when re-applying dirty changes.
    mpt_at_head: Option<MptCalc>,
    /// The commit the MPT is currently synced to.
    sync_commit: Option<CommitId>,
    /// The branch the MPT is currently synced to.
    sync_branch: Option<BranchId>,
    /// Whether uncommitted (dirty) changes have been applied on top of HEAD.
    dirty_applied: bool,
}

impl<K, V> VerMapWithProof<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Creates a new `VerMapWithProof` with a fresh VerMap.
    pub fn new() -> Self {
        Self {
            map: VerMap::new(),
            mpt: MptCalc::new(),
            mpt_at_head: None,
            sync_commit: None,
            sync_branch: None,
            dirty_applied: false,
        }
    }

    /// Wraps an existing `VerMap`.
    pub fn from_map(map: VerMap<K, V>) -> Self {
        Self {
            map,
            mpt: MptCalc::new(),
            mpt_at_head: None,
            sync_commit: None,
            sync_branch: None,
            dirty_applied: false,
        }
    }

    /// Returns a reference to the underlying VerMap.
    pub fn map(&self) -> &VerMap<K, V> {
        &self.map
    }

    /// Returns a mutable reference to the underlying VerMap.
    ///
    /// Mutations through this reference will **not** automatically
    /// update the MPT — call [`merkle_root`](Self::merkle_root) to
    /// resynchronize.
    pub fn map_mut(&mut self) -> &mut VerMap<K, V> {
        &mut self.map
    }

    // =================================================================
    // Merkle root computation
    // =================================================================

    /// Computes the Merkle root hash for the current state of `branch`.
    ///
    /// Includes uncommitted changes.  Performs an incremental diff
    /// update when possible, falling back to a full rebuild otherwise.
    pub fn merkle_root(&mut self, branch: BranchId) -> Result<Vec<u8>> {
        // Fast path: same branch, synced to HEAD, no uncommitted changes,
        // and no dirty overlay currently applied.
        if self.sync_branch == Some(branch)
            && !self.dirty_applied
            && let Some(sync_id) = self.sync_commit
        {
            let head = self.map.head_commit(branch)?;
            let head_id = head.as_ref().map(|c| c.id);
            let has_dirty = self.map.has_uncommitted(branch)?;

            if head_id == Some(sync_id) && !has_dirty {
                // MPT matches the branch HEAD exactly.
                return self.mpt.root_hash().c(d!());
            }
        }

        self.sync_to_branch(branch)?;
        self.mpt.root_hash().c(d!())
    }

    /// Computes the Merkle root hash for a specific historical commit.
    pub fn merkle_root_at_commit(&mut self, commit: CommitId) -> Result<Vec<u8>> {
        self.sync_to_commit(commit)?;
        self.mpt.root_hash().c(d!())
    }

    // =================================================================
    // Cache persistence
    // =================================================================

    /// Persists the MPT to disk for fast restoration on restart.
    ///
    /// The MPT must be synced (call `merkle_root` first) and must be
    /// synced to a committed state (not uncommitted changes).
    pub fn save_cache(&mut self, path: &Path) -> Result<()> {
        let tag = self.sync_commit.c(d!("no synced commit"))?;
        self.mpt.save_cache(path, tag).c(d!())
    }

    /// Restores the MPT from a cached file and incrementally catches up
    /// to the current HEAD of `branch`.
    ///
    /// If the cache file is missing or corrupted, falls back to a full
    /// rebuild.
    pub fn load_cache_and_sync(
        &mut self,
        path: &Path,
        branch: BranchId,
    ) -> Result<Vec<u8>> {
        match MptCalc::load_cache(path) {
            Ok((mpt, sync_tag, _root_hash)) => {
                self.mpt = mpt;
                self.mpt_at_head = None;
                self.sync_commit = Some(sync_tag);
                self.sync_branch = None;
                self.dirty_applied = false;
            }
            Err(_) => {
                // Cache corrupted or missing — start fresh.
                self.mpt = MptCalc::new();
                self.mpt_at_head = None;
                self.sync_commit = None;
                self.sync_branch = None;
                self.dirty_applied = false;
            }
        }
        self.merkle_root(branch)
    }

    // =================================================================
    // Internal sync logic
    // =================================================================

    /// Synchronizes the MPT to the current state of `branch`
    /// (including uncommitted changes).
    fn sync_to_branch(&mut self, branch: BranchId) -> Result<()> {
        let head = self.map.head_commit(branch)?;
        let head_id = head.as_ref().map(|c| c.id);

        // If dirty changes were previously applied, restore the MPT
        // to the clean HEAD state before re-syncing.
        if self.dirty_applied {
            if let Some(ref snapshot) = self.mpt_at_head {
                self.mpt = snapshot.clone();
            }
            self.dirty_applied = false;
        }

        // Sync to the branch's HEAD commit.
        if let Some(hid) = head_id {
            if self.sync_commit != Some(hid) {
                self.sync_to_commit(hid)?;
            }
        } else if self.sync_commit.is_some() {
            // Branch has no commits yet but MPT was synced elsewhere → reset.
            self.mpt = MptCalc::new();
            self.mpt_at_head = None;
            self.sync_commit = None;
        }

        // Then, apply any uncommitted changes.
        if self.map.has_uncommitted(branch)? {
            self.mpt_at_head = Some(self.mpt.clone());
            let diff = self.map.diff_uncommitted(branch)?;
            self.apply_diff(&diff)?;
            self.dirty_applied = true;
        } else {
            self.mpt_at_head = None;
            self.dirty_applied = false;
        }

        self.sync_branch = Some(branch);
        Ok(())
    }

    /// Synchronizes the MPT to a specific commit.
    fn sync_to_commit(&mut self, target: CommitId) -> Result<()> {
        if self.sync_commit == Some(target) && !self.dirty_applied {
            return Ok(());
        }

        // Restore clean state if dirty overlay was applied.
        if self.dirty_applied {
            if let Some(ref snapshot) = self.mpt_at_head {
                self.mpt = snapshot.clone();
            }
            self.dirty_applied = false;
            self.mpt_at_head = None;
        }

        if self.sync_commit == Some(target) {
            return Ok(());
        }

        match self.sync_commit {
            Some(current) => {
                // Try incremental diff.
                match self.map.diff_commits(current, target) {
                    Ok(diff) => self.apply_diff(&diff)?,
                    Err(_) => self.full_rebuild_commit(target)?,
                }
            }
            None => {
                self.full_rebuild_commit(target)?;
            }
        }

        self.sync_commit = Some(target);
        self.sync_branch = None;
        Ok(())
    }

    /// Full rebuild: clear MPT and re-insert all entries at `commit`.
    fn full_rebuild_commit(&mut self, commit: CommitId) -> Result<()> {
        let entries: Vec<_> = self.map.raw_iter_at_commit(commit)?.collect();
        self.mpt = MptCalc::from_entries(entries).c(d!())?;
        Ok(())
    }

    /// Apply a diff to the current MPT.
    fn apply_diff(&mut self, diff: &[DiffEntry]) -> Result<()> {
        let ops: Vec<(&[u8], Option<&[u8]>)> = diff
            .iter()
            .map(|entry| match entry {
                DiffEntry::Added { key, value } => {
                    (key.as_slice(), Some(value.as_slice()))
                }
                DiffEntry::Removed { key, .. } => (key.as_slice(), None),
                DiffEntry::Modified { key, new_value, .. } => {
                    (key.as_slice(), Some(new_value.as_slice()))
                }
            })
            .collect();
        self.mpt.batch_update(&ops).c(d!())
    }
}

impl<K, V> Default for VerMapWithProof<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}
