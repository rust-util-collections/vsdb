//!
//! [`VerMapWithProof`] — a versioned KV map with Merkle root computation.
//!
//! Combines [`VerMap`] (versioning, branching, merging) with a
//! [`TrieCalc`](super::TrieCalc) back-end (e.g. [`MptCalc`](super::MptCalc)
//! or [`SmtCalc`](super::SmtCalc)) to provide cryptographic commitments
//! over versioned state.
//!
//! The trie is treated as a disposable computation layer: it holds an
//! in-memory trie that can be rebuilt from any VerMap snapshot at any
//! time.  A transparent on-disk cache avoids full rebuilds on restart.

use crate::common::ende::{KeyEnDeOrdered, ValueEnDe};
use crate::versioned::diff::DiffEntry;
use crate::versioned::map::VerMap;
use crate::versioned::{BranchId, CommitId};
use ruc::*;

use super::{SmtCalc, SmtProof, TrieCalc};

/// A versioned key-value map with Merkle root hash computation.
///
/// Wraps a [`VerMap<K, V>`] and a [`TrieCalc`] back-end `T` to provide
/// a [`merkle_root`](Self::merkle_root) method that lazily computes
/// the 32-byte Merkle root hash for any branch or commit.
///
/// # Incremental updates
///
/// The internal trie tracks a *sync point* — the commit it was last
/// synchronized to.  When `merkle_root` is called:
///
/// 1. If the trie is already synced to the target → return cached hash.
/// 2. If the target is reachable via diff from the sync point →
///    apply diff incrementally.
/// 3. Otherwise → full rebuild from the store's iterator.
///
/// # Automatic cache lifecycle
///
/// The in-memory trie is transparently cached to disk so that process
/// restarts only require an incremental diff rather than a full rebuild.
///
/// - **Auto-load** — when created via [`new`](Self::new) or
///   [`from_map`](Self::from_map), the constructor silently attempts to
///   restore a previous cache file.  On miss or corruption it falls back
///   to a full rebuild on the next [`merkle_root`](Self::merkle_root) call.
/// - **Auto-save** — the committed trie state is persisted eagerly
///   inside [`merkle_root`](Self::merkle_root) (specifically, after
///   `sync_to_commit` completes).  Errors are silently ignored because
///   the cache is **disposable**: the authoritative data lives in the
///   underlying [`VerMap`].
///
/// No manual `save_cache` / `load_cache` calls are needed.
pub struct VerMapWithProof<K, V, T: TrieCalc> {
    map: VerMap<K, V>,
    trie: T,
    /// Snapshot of `trie` at the last synced commit (before dirty overlay).
    /// Used to reset when re-applying dirty changes.
    trie_at_head: Option<T>,
    /// The commit the trie is currently synced to.
    sync_commit: Option<CommitId>,
    /// The branch the trie is currently synced to.
    sync_branch: Option<BranchId>,
    /// Whether uncommitted (dirty) changes have been applied on top of HEAD.
    dirty_applied: bool,
    /// Unique storage prefix of the underlying map, used for cache file naming.
    /// Stored here so that `Drop` can save the cache without needing
    /// `K`/`V` trait bounds.
    cache_id: u64,
    /// Whether the committed trie state has changed since the last
    /// save/load.  Avoids pointless re-serialization in read-only
    /// scenarios.
    cache_dirty: bool,
}

impl<K, V, T> VerMapWithProof<K, V, T>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
    T: TrieCalc,
{
    /// Creates a new `VerMapWithProof` with a fresh VerMap.
    pub fn new() -> Self {
        let map = VerMap::new();
        let cache_id = map.instance_id();
        let mut this = Self {
            map,
            trie: T::default(),
            trie_at_head: None,
            sync_commit: None,
            sync_branch: None,
            dirty_applied: false,
            cache_id,
            cache_dirty: false,
        };
        this.try_load_cache();
        this
    }

    /// Wraps an existing `VerMap`.
    ///
    /// Runs [`gc`](VerMap::gc) for crash recovery (if needed) and
    /// B+ tree cleanup, then attempts to restore the trie cache.
    pub fn from_map(mut map: VerMap<K, V>) -> Self {
        map.gc();
        let cache_id = map.instance_id();
        let mut this = Self {
            map,
            trie: T::default(),
            trie_at_head: None,
            sync_commit: None,
            sync_branch: None,
            dirty_applied: false,
            cache_id,
            cache_dirty: false,
        };
        this.try_load_cache();
        this
    }

    /// Returns a reference to the underlying VerMap.
    pub fn map(&self) -> &VerMap<K, V> {
        &self.map
    }

    /// Returns a mutable reference to the underlying VerMap.
    ///
    /// Mutations through this reference will **not** automatically
    /// update the trie — call [`merkle_root`](Self::merkle_root) to
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
                // Trie matches the branch HEAD exactly.
                return self.trie.root_hash().c(d!());
            }
        }

        self.sync_to_branch(branch)?;
        self.trie.root_hash().c(d!())
    }

    /// Computes the Merkle root hash for a specific historical commit.
    pub fn merkle_root_at_commit(&mut self, commit: CommitId) -> Result<Vec<u8>> {
        self.sync_to_commit(commit)?;
        self.trie.root_hash().c(d!())
    }

    // =================================================================
    // Internal sync logic
    // =================================================================

    /// Synchronizes the trie to the current state of `branch`
    /// (including uncommitted changes).
    fn sync_to_branch(&mut self, branch: BranchId) -> Result<()> {
        let head = self.map.head_commit(branch)?;
        let head_id = head.as_ref().map(|c| c.id);

        // If dirty changes were previously applied, restore the trie
        // to the clean HEAD state before re-syncing.
        if self.dirty_applied {
            if let Some(ref snapshot) = self.trie_at_head {
                self.trie = snapshot.clone();
            }
            self.dirty_applied = false;
        }

        // Sync to the branch's HEAD commit.
        if let Some(hid) = head_id {
            if self.sync_commit != Some(hid) {
                self.sync_to_commit(hid)?;
            }
        } else if self.sync_commit.is_some() {
            // Branch has no commits yet but trie was synced elsewhere → reset.
            self.trie = T::default();
            self.trie_at_head = None;
            self.sync_commit = None;
        }

        // Then, apply any uncommitted changes.
        if self.map.has_uncommitted(branch)? {
            self.trie_at_head = Some(self.trie.clone());
            let diff = self.map.diff_uncommitted(branch)?;
            self.apply_diff(&diff)?;
            self.dirty_applied = true;
        } else {
            self.trie_at_head = None;
            self.dirty_applied = false;
        }

        self.sync_branch = Some(branch);
        Ok(())
    }

    /// Synchronizes the trie to a specific commit.
    fn sync_to_commit(&mut self, target: CommitId) -> Result<()> {
        if self.sync_commit == Some(target) && !self.dirty_applied {
            return Ok(());
        }

        // Restore clean state if dirty overlay was applied.
        if self.dirty_applied {
            if let Some(ref snapshot) = self.trie_at_head {
                self.trie = snapshot.clone();
            }
            self.dirty_applied = false;
            self.trie_at_head = None;
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

        // Eagerly persist the cache while the trie is in a clean
        // committed state (before any dirty overlay is applied).
        // This avoids an expensive clone in `Drop`.
        self.cache_dirty = true;
        if self.trie.save_cache(self.cache_id, target).is_ok() {
            self.cache_dirty = false;
        }
        Ok(())
    }

    /// Full rebuild: clear trie and re-insert all entries at `commit`.
    fn full_rebuild_commit(&mut self, commit: CommitId) -> Result<()> {
        let entries: Vec<_> = self.map.raw_iter_at_commit(commit)?.collect();
        self.trie = T::from_entries(entries).c(d!())?;
        Ok(())
    }

    /// Apply a diff to the current trie.
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
        self.trie.batch_update(&ops).c(d!())
    }
}

// =================================================================
// SMT-specific proof API
// =================================================================

impl<K, V> VerMapWithProof<K, V, SmtCalc>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Generates a Merkle proof for the given key.
    ///
    /// The trie must be synced (call [`merkle_root`](Self::merkle_root)
    /// first) for proof generation to work.
    pub fn prove(&self, key: &[u8]) -> Result<SmtProof> {
        self.trie.prove(key).c(d!())
    }

    /// Verifies a proof against a root hash.
    pub fn verify_proof(root_hash: &[u8; 32], proof: &SmtProof) -> Result<bool> {
        SmtCalc::verify_proof(root_hash, proof).c(d!())
    }
}

impl<K, V, T> Default for VerMapWithProof<K, V, T>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
    T: TrieCalc,
{
    fn default() -> Self {
        Self::new()
    }
}

// =================================================================
// Internal cache lifecycle (auto-load / auto-save)
//
// These methods live in a separate impl block with minimal bounds
// (only `T: TrieCalc`) so they can be called from the `Drop` impl,
// which cannot carry `K`/`V` trait bounds.
// =================================================================

impl<K, V, T: TrieCalc> VerMapWithProof<K, V, T> {
    /// Attempts to restore a previously saved trie from disk.
    ///
    /// On success, sets `sync_commit` so that the next `merkle_root`
    /// call can do an incremental diff instead of a full rebuild.
    /// On failure (missing file, corruption, version mismatch), silently
    /// falls back to the default empty trie.
    fn try_load_cache(&mut self) {
        if let Ok((trie, sync_tag, _root_hash)) = T::load_cache(self.cache_id) {
            self.trie = trie;
            self.trie_at_head = None;
            self.sync_commit = Some(sync_tag);
            self.sync_branch = None;
            self.dirty_applied = false;
            self.cache_dirty = false;
        }
    }

    /// Saves the committed trie state to disk.  Called from `Drop`.
    ///
    /// Normally the cache is already persisted eagerly in
    /// `sync_to_commit`, so this is a no-op.  It only fires when
    /// `cache_dirty` is still set (e.g. the eager save failed).
    fn try_save_cache(&mut self) {
        let Some(tag) = self.sync_commit else {
            return;
        };

        // If a dirty overlay is applied we cannot save the trie as-is
        // (it includes uncommitted data).  Restoring from trie_at_head
        // would require a clone.  Since the eager save already ran
        // (and presumably succeeded — cache_dirty would be false),
        // reaching here with dirty_applied=true means the eager save
        // failed.  Skip rather than clone in the destructor.
        if self.dirty_applied {
            return;
        }

        let _ = self.trie.save_cache(self.cache_id, tag);
    }
}

// =================================================================
// Drop — auto-save cache on handle destruction
// =================================================================

impl<K, V, T: TrieCalc> Drop for VerMapWithProof<K, V, T> {
    fn drop(&mut self) {
        if self.cache_dirty {
            self.try_save_cache();
        }
    }
}
