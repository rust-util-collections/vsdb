//!
//! [`VerMapWithProof`] — a versioned KV map with Merkle root computation.
//!
//! Combines [`VerMap`] (versioning, branching, merging) with a
//! [`TrieCalc`] back-end (e.g. [`super::MptCalc`]
//! or [`super::SmtCalc`]) to provide cryptographic commitments
//! over versioned state.
//!
//! The trie is treated as a disposable computation layer: it holds an
//! in-memory trie that can be rebuilt from any VerMap snapshot at any
//! time.  A transparent on-disk cache avoids full rebuilds on restart.

use crate::{
    basic::persistent_btree::{RootLease, TreeDiff},
    common::{
        InstanceId, Namespace, PrefixFrame,
        ende::{KeyEnDeOrdered, ValueEnDe},
        ensure_writable,
        error::Result,
    },
    versioned::{BranchId, CommitId, map::VerMap},
};

use super::{Checkpoint, TrieCalc};
use serde::{Deserialize, Serialize};

/// A root and proof computed together for one requested versioned state.
///
/// The result owns both values and remains unchanged by later map operations.
/// For a known checkpoint, compare `root_hash` with that checkpoint's root
/// before verifying the proof. The proof's value uses the map's value codec.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProofWithRoot<P> {
    /// The computed 32-byte root for the requested state.
    pub root_hash: [u8; 32],
    /// Membership or absence proof for the requested key.
    pub proof: P,
}

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
/// [`from_map`](Self::from_map) attempts to load an existing cache for a live
/// commit. A missing, stale, reclaimed-commit, or corrupt cache is rebuilt
/// from the underlying `VerMap` on the next root calculation. Root calculation
/// and `Drop` never write cache files.
///
/// Call [`save_cache`](Self::save_cache) with a committed snapshot at an
/// application-chosen checkpoint. Saving serializes the entire trie, so doing
/// it on every block would turn incremental root calculation into O(total
/// state) work. Cache errors are returned to the caller; the authoritative
/// data remains in `VerMap` and does not depend on a successful cache save.
pub struct VerMapWithProof<K, V, T: TrieCalc> {
    map: VerMap<K, V>,
    committed_trie: T,
    working_trie: T,
    sync_commit: Option<CommitId>,
    working_root: Option<u64>,
    cache_instance: InstanceId,
}

impl<K, V, T: TrieCalc> Serialize for VerMapWithProof<K, V, T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        crate::common::serialize_prefix_handle::<Self, _>(
            &self.map.tree.nodes,
            &[],
            serializer,
        )
    }
}
impl<'de, K: KeyEnDeOrdered, V: ValueEnDe, T: TrieCalc> Deserialize<'de>
    for VerMapWithProof<K, V, T>
{
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        crate::common::deserialize_prefix_handle(deserializer, |frame| {
            Self::open_frame(frame, None)
        })
    }
}

impl<K, V, T> VerMapWithProof<K, V, T>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
    T: TrieCalc,
{
    /// Creates a new `VerMapWithProof` with a fresh VerMap.
    pub fn new() -> Self {
        Self::from_map(VerMap::new())
    }

    pub fn new_in(ns: &Namespace) -> Self {
        Self::from_map(VerMap::new_in(ns))
    }
    pub fn namespace(&self) -> Namespace {
        self.map.namespace()
    }
    pub fn instance_id(&self) -> InstanceId {
        self.map.instance_id()
    }
    pub fn clone_in(&self, ns: &Namespace) -> Result<Self> {
        Ok(Self::from_map(self.map.clone_in(ns)?))
    }
    pub fn save_meta(&self) -> Result<InstanceId> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }
    pub fn from_meta(id: impl Into<InstanceId>) -> Result<Self> {
        let id = id.into();
        let id = InstanceId::new(id.map_id, id.ns.unwrap_or_default());
        let frame: Vec<u8> = crate::common::load_instance_meta(id)?;
        Self::open_frame(
            crate::common::parse_prefix_handle::<Self>(&frame)?,
            Some(id),
        )
    }

    fn open_frame(frame: PrefixFrame<'_>, expected: Option<InstanceId>) -> Result<Self> {
        let PrefixFrame(raw, extra) = frame;
        if !extra.is_empty() || expected.is_some_and(|id| id != raw.instance_id()) {
            return Err(crate::VsdbError::Decode {
                detail: "invalid versioned proof handle".into(),
            });
        }
        Ok(Self::from_map(VerMap::open(raw, false)?))
    }

    /// Restores a checkpoint when its universe, map, commit and tree root
    /// match the supplied map. A disposable-cache miss leaves lazy rebuilding
    /// to the next query.
    pub fn from_map(map: VerMap<K, V>) -> Self {
        let id = map.instance_id();
        let restored = T::load_checkpoint(&map.namespace().system_dir(), id.map_id)
            .ok()
            .and_then(|(trie, checkpoint)| {
                let commit = CommitId::from_raw(checkpoint.commit);
                (checkpoint.map == id
                    && checkpoint.universe == map.namespace().universe_id()
                    && map
                        .get_commit(commit)
                        .is_some_and(|c| c.root == checkpoint.tree_root))
                .then_some((trie, commit))
            });
        let (committed_trie, sync_commit) =
            restored.map(|(t, c)| (t, Some(c))).unwrap_or_default();
        Self {
            map,
            cache_instance: id,
            working_trie: committed_trie.clone(),
            committed_trie,
            sync_commit,
            working_root: None,
        }
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
    pub fn merkle_root(&mut self, branch: BranchId) -> Result<[u8; 32]> {
        self.reset_if_replaced();
        let roots = self.map.branch_roots(branch)?;
        if self.working_root != Some(roots.working.root) {
            if let Some(commit) = roots.head {
                self.sync_committed(commit, &roots.committed)?;
            } else {
                self.committed_trie = T::default();
                self.sync_commit = None;
            }
            let mut candidate = self.committed_trie.clone();
            if roots.working.root != roots.committed.root {
                Self::apply_diff(
                    &mut candidate,
                    &self.map.diff_roots(&roots.committed, &roots.working),
                )?;
            }
            self.working_root = Some(roots.working.root);
            self.working_trie = candidate;
        }
        Ok(self.working_trie.root_hash())
    }
    pub fn merkle_root_at_commit(&mut self, commit: CommitId) -> Result<[u8; 32]> {
        self.reset_if_replaced();
        let root = self.map.commit_root(commit)?;
        self.sync_committed(commit, &root)?;
        self.working_trie = self.committed_trie.clone();
        self.working_root = Some(root.root);
        Ok(self.working_trie.root_hash())
    }
    /// Writes a durable checkpoint of the named committed tree.
    pub fn save_cache(&mut self, commit: CommitId) -> Result<()> {
        ensure_writable(&self.namespace(), "versioned trie cache save")?;
        self.reset_if_replaced();
        let root = self.map.commit_root(commit)?;
        self.sync_committed(commit, &root)?;
        self.working_trie = self.committed_trie.clone();
        self.working_root = Some(root.root);
        let id = Checkpoint {
            universe: self.namespace().universe_id(),
            map: self.instance_id(),
            commit: commit.raw(),
            tree_root: root.root,
        };
        self.committed_trie
            .save_checkpoint(&self.namespace().system_dir(), id)
    }
    fn reset_if_replaced(&mut self) {
        let id = self.instance_id();
        if id != self.cache_instance {
            self.cache_instance = id;
            self.working_root = None;
            self.sync_commit = None;
            self.working_trie = T::default();
            self.committed_trie = T::default();
        }
    }
    fn sync_committed(&mut self, commit: CommitId, root: &RootLease) -> Result<()> {
        if self.sync_commit == Some(commit) {
            return Ok(());
        }
        if let Some(from) = self.sync_commit.and_then(|c| self.map.commit_root(c).ok()) {
            let diff = self.map.diff_roots(&from, root);
            Self::apply_diff(&mut self.committed_trie, &diff)?;
        } else {
            self.committed_trie = T::from_entries(self.map.tree.iter(root.root))?;
        }
        self.sync_commit = Some(commit);
        Ok(())
    }
    /// Apply a diff to the current trie.
    fn apply_diff(trie: &mut T, diff: &[TreeDiff]) -> Result<()> {
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
        trie.batch_update(&ops)
    }
}

// =================================================================
// Proofs over the most recently synchronized view.

impl<K: KeyEnDeOrdered, V: ValueEnDe, T: TrieCalc> VerMapWithProof<K, V, T> {
    pub fn prove_at(
        &mut self,
        branch: BranchId,
        key: &K,
    ) -> Result<ProofWithRoot<T::Proof>> {
        Ok(ProofWithRoot {
            root_hash: self.merkle_root(branch)?,
            proof: self.prove_key(key)?,
        })
    }
    pub fn prove_at_commit(
        &mut self,
        commit: CommitId,
        key: &K,
    ) -> Result<ProofWithRoot<T::Proof>> {
        Ok(ProofWithRoot {
            root_hash: self.merkle_root_at_commit(commit)?,
            proof: self.prove_key(key)?,
        })
    }
    pub fn prove(&self, key: &[u8]) -> Result<T::Proof> {
        self.working_trie.prove(key)
    }
    pub fn prove_key(&self, key: &K) -> Result<T::Proof> {
        key.with_ordered_bytes(|b| self.prove(b))
    }
    pub fn verify_proof(root: &[u8; 32], key: &[u8], proof: &T::Proof) -> Result<bool> {
        T::verify_proof(root, key, proof)
    }
    pub fn verify_key_proof(root: &[u8; 32], key: &K, proof: &T::Proof) -> Result<bool> {
        key.with_ordered_bytes(|bytes| T::verify_proof(root, bytes, proof))
    }
}
impl<K: KeyEnDeOrdered, V: ValueEnDe, T: TrieCalc> Clone for VerMapWithProof<K, V, T> {
    fn clone(&self) -> Self {
        Self::from_map(self.map.clone())
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
