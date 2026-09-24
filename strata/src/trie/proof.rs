//!
//! [`VerMapWithProof`] — a versioned KV map with Merkle root computation.
//!
//! Combines [`VerMap`] (versioning, branching, merging) with a
//! [`TrieCalc`] back-end (e.g. [`MptCalc`]
//! or [`SmtCalc`]) to provide cryptographic commitments
//! over versioned state.
//!
//! The trie is treated as a disposable computation layer: it holds an
//! in-memory trie that can be rebuilt from any VerMap snapshot at any
//! time.  A transparent on-disk cache avoids full rebuilds on restart.

use crate::{
    basic::persistent_btree::TreeDiff,
    common::{
        InstanceId,
        ende::{KeyEnDeOrdered, ValueEnDe},
        ensure_writable,
        error::{Result, VsdbError},
    },
    versioned::{BranchId, CommitId, map::VerMap},
};

use super::{MptCalc, MptProof, SmtCalc, SmtProof, TrieCalc};

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
/// # Disposable disk cache
///
/// Construction attempts to load an existing cache. A missing, stale, or
/// corrupt cache is rebuilt from the underlying `VerMap` on the next root
/// calculation. Root calculation and `Drop` never write cache files.
///
/// Call [`save_cache`](Self::save_cache) with a committed snapshot at an
/// application-chosen checkpoint. Saving serializes the entire trie, so doing
/// it on every block would turn incremental root calculation into O(total
/// state) work. Cache errors are returned to the caller; the authoritative
/// data remains in `VerMap` and does not depend on a successful cache save.
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
    /// Map identity owning all trie and synchronization state. Commit and
    /// branch IDs are only unique within this instance. Also supplies the
    /// cache filename, provided the underlying map has not been replaced.
    cache_instance: InstanceId,
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
        let cache_instance = map.instance_id();
        let mut this = Self {
            map,
            trie: T::default(),
            trie_at_head: None,
            sync_commit: None,
            sync_branch: None,
            dirty_applied: false,
            cache_instance,
        };
        this.try_load_cache();
        this
    }

    /// Wraps an existing `VerMap` and attempts to restore the trie cache.
    ///
    /// A `VerMap` handle is already recovered (restoring one repairs an
    /// interrupted ref-count cascade and rebuilds node references), so
    /// no extra sweep runs here.
    pub fn from_map(map: VerMap<K, V>) -> Self {
        let cache_instance = map.instance_id();
        let mut this = Self {
            map,
            trie: T::default(),
            trie_at_head: None,
            sync_commit: None,
            sync_branch: None,
            dirty_applied: false,
            cache_instance,
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
    /// resynchronize. Replacing the map through this reference also
    /// invalidates the old trie and cache identity at the next synchronization.
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
        self.reset_if_map_replaced();
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
                return self.trie.root_hash();
            }
        }

        self.sync_to_branch(branch)?;
        self.trie.root_hash()
    }

    /// Computes the Merkle root hash for a specific historical commit.
    pub fn merkle_root_at_commit(&mut self, commit: CommitId) -> Result<Vec<u8>> {
        self.sync_to_commit(commit)?;
        self.trie.root_hash()
    }

    /// Saves the trie for `commit` as a disposable restart cache.
    ///
    /// Synchronizes to that historical commit first, so subsequent proof
    /// calls describe `commit`, without any uncommitted overlay. Call
    /// [`merkle_root`](Self::merkle_root) again to prove a branch's working
    /// state. Saving costs O(total state); choose checkpoints according to
    /// the acceptable restart/rebuild cost, rather than saving every block.
    ///
    /// Cache I/O errors are returned. Losing or skipping this cache never
    /// loses map data. Returns [`VsdbError::ReadOnly`] in read-only mode and
    /// [`VsdbError::CommitNotFound`] if the commit has been reclaimed.
    pub fn save_cache(&mut self, commit: CommitId) -> Result<()> {
        ensure_writable(&self.map.namespace(), "versioned trie cache save")?;
        self.sync_to_commit(commit)?;
        self.trie.save_cache(
            &self.map.namespace().system_dir(),
            self.cache_instance.map_id,
            commit.raw(),
        )
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
            let diff = self.map.raw_diff_uncommitted(branch)?;
            let snapshot = self.trie.clone();
            if let Err(e) = self.apply_diff(&diff) {
                // `batch_update` is not atomic: operations before the
                // failing one are already applied.  Restore the clean
                // HEAD snapshot so the trie keeps matching
                // `sync_commit` — otherwise a later
                // `merkle_root_at_commit(HEAD)` would short-circuit on
                // the still-valid bookkeeping and silently serve a
                // root over the partially applied dirty overlay.
                self.trie = snapshot;
                self.trie_at_head = None;
                return Err(e);
            }
            self.trie_at_head = Some(snapshot);
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
        self.reset_if_map_replaced();
        // A cache is not a retained Snapshot. Rollback or branch deletion can
        // reclaim its commit, even while the trie (or a disk cache) survives.
        if self.map.get_commit(target).is_none() {
            return Err(VsdbError::CommitNotFound {
                commit_id: target.raw(),
            });
        }
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
                match self.map.raw_diff_commits(current, target) {
                    Ok(diff) => {
                        if let Err(e) = self.apply_diff(&diff) {
                            // `batch_update` is not atomic: operations
                            // before the failing one remain applied, so
                            // the trie no longer matches ANY commit.
                            // Poison the sync state — `sync_commit`
                            // must never keep claiming `current`, or a
                            // later `merkle_root_at_commit(current)`
                            // (or a `merkle_root` after rolling the
                            // branch back) would short-circuit and
                            // silently serve a root over the partial
                            // state.  The next sync performs a full
                            // rebuild instead.
                            self.trie = T::default();
                            self.trie_at_head = None;
                            self.sync_commit = None;
                            self.sync_branch = None;
                            self.dirty_applied = false;
                            return Err(e);
                        }
                    }
                    // On a diff failure nothing was applied yet: fall
                    // back to a full rebuild.  `full_rebuild_commit`
                    // only assigns `self.trie` on success, so its
                    // failure leaves the (still `current`-consistent)
                    // trie and bookkeeping untouched.
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

    /// Full rebuild: clear trie and re-insert all entries at `commit`.
    fn full_rebuild_commit(&mut self, commit: CommitId) -> Result<()> {
        let entries: Vec<_> = self.map.at(commit)?.raw_iter().collect();
        self.trie = T::from_entries(entries)?;
        Ok(())
    }

    /// Apply a diff to the current trie.
    fn apply_diff(&mut self, diff: &[TreeDiff]) -> Result<()> {
        let ops: Vec<(&[u8], Option<&[u8]>)> = diff
            .iter()
            .map(|entry| match entry {
                TreeDiff::Added { key, value } => {
                    (key.as_slice(), Some(value.as_slice()))
                }
                TreeDiff::Removed { key, .. } => (key.as_slice(), None),
                TreeDiff::Modified { key, new_value, .. } => {
                    (key.as_slice(), Some(new_value.as_slice()))
                }
            })
            .collect();
        self.trie.batch_update(&ops)
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
    /// Generates a Merkle proof for exact ordered-key bytes.
    ///
    /// The trie must be synced (call [`merkle_root`](Self::merkle_root)
    /// first) for proof generation to work. The bytes must be exactly
    /// [`KeyEnDeOrdered::to_bytes`] for the logical key; prefer
    /// [`prove_key`](Self::prove_key) when a typed key is available.
    pub fn prove(&self, key: &[u8]) -> Result<SmtProof> {
        self.trie.prove(key)
    }

    /// Generates a Merkle proof for a typed key using the same ordered
    /// encoding committed by the underlying `VerMap`.
    pub fn prove_key(&self, key: &K) -> Result<SmtProof> {
        self.trie.prove(&key.to_bytes())
    }

    /// Verifies a proof against a root hash and exact ordered-key bytes.
    pub fn verify_proof(
        root_hash: &[u8; 32],
        expected_key: &[u8],
        proof: &SmtProof,
    ) -> Result<bool> {
        SmtCalc::verify_proof(root_hash, expected_key, proof)
    }

    /// Verifies a proof against a typed key using its ordered encoding.
    pub fn verify_key_proof(
        root_hash: &[u8; 32],
        expected_key: &K,
        proof: &SmtProof,
    ) -> Result<bool> {
        SmtCalc::verify_proof(root_hash, &expected_key.to_bytes(), proof)
    }
}

// =================================================================
// MPT-specific proof API
// =================================================================

impl<K, V> VerMapWithProof<K, V, MptCalc>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Generates a Merkle proof for the given key.
    ///
    /// The trie must be synced (call [`merkle_root`](Self::merkle_root)
    /// first) for proof generation to work.
    pub fn prove_mpt(&self, key: &K) -> Result<MptProof> {
        self.trie.prove(&key.to_bytes())
    }

    /// Verifies an MPT proof against a root hash for a specific key.
    ///
    /// `expected_key` is the raw-byte key the caller expects this proof
    /// to cover.
    pub fn verify_mpt_proof(
        root_hash: &[u8; 32],
        expected_key: &[u8],
        proof: &MptProof,
    ) -> Result<bool> {
        MptCalc::verify_proof(root_hash, expected_key, proof)
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
// Internal cache identity and loading
// =================================================================

impl<K, V, T: TrieCalc> VerMapWithProof<K, V, T> {
    /// A mutable map reference permits replacing the entire handle. Preserve
    /// incremental synchronization for ordinary edits, but never compare
    /// local branch/commit IDs across different maps.
    fn reset_if_map_replaced(&mut self) {
        let current = self.map.instance_id();
        if current != self.cache_instance {
            self.trie = T::default();
            self.trie_at_head = None;
            self.sync_commit = None;
            self.sync_branch = None;
            self.dirty_applied = false;
            self.cache_instance = current;
        }
    }

    /// Attempts to restore a previously saved trie from disk.
    ///
    /// On success, sets `sync_commit` so that the next `merkle_root`
    /// call can do an incremental diff instead of a full rebuild.
    /// On failure (missing file, corruption, version mismatch), silently
    /// falls back to the default empty trie.
    fn try_load_cache(&mut self) {
        if let Ok((mut trie, sync_tag, saved_hash)) = T::load_cache(
            &self.map.namespace().system_dir(),
            self.cache_instance.map_id,
        ) {
            // Consistency check between two fields of the cache file: the
            // header's saved root hash and the root node's stored hash
            // (root_hash() short-circuits on a committed root, so nothing
            // is recomputed here).  This catches header/payload mix-ups,
            // not tampering — a self-attesting file cannot prove its own
            // integrity; accidental corruption is already rejected by the
            // file checksum inside load_cache().
            let ok = trie.root_hash().map(|h| h == saved_hash).unwrap_or(false);
            if !ok {
                return;
            }
            self.trie = trie;
            self.trie_at_head = None;
            self.sync_commit = Some(CommitId::from_raw(sync_tag));
            self.sync_branch = None;
            self.dirty_applied = false;
        }
    }
}
