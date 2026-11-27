//! [`VerMapWithProof`] computes Merkle roots and proofs for a [`VerMap`]
//! using [`super::MptCalc`] or [`super::SmtCalc`].
//!
//! The in-memory trie can be rebuilt from a retained map root. An explicit
//! [`VerMapWithProof::save_cache`] saves a checkpoint for a commit. Opening
//! the wrapper can reuse a checkpoint after checking its binding to the map.

use crate::{
    basic::persistent_btree::{EMPTY_ROOT, RootLease, TreeDiff},
    common::{
        InstanceId, Namespace, PrefixFrame,
        ende::{KeyEnDeOrdered, ValueEnDe},
        ensure_writable,
        error::Result,
    },
    versioned::{BranchId, CommitId, map::VerMap, store::load_versioned_meta},
};

use super::{Checkpoint, TrieCalc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const DIFF_STREAM_CHUNK: usize = 4096;

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
/// a 32-byte root for a branch via [`merkle_root`](Self::merkle_root) or a
/// retained commit via [`merkle_root_at_commit`](Self::merkle_root_at_commit).
///
/// # Incremental updates
///
/// The internal trie tracks both the last synchronized commit and, when a
/// branch has uncommitted edits, a cursor lease on its most recently hashed
/// working root. When `merkle_root` is called:
///
/// 1. If the trie is already synced to the target → return cached hash.
/// 2. If the target advances a dirty working root on the same branch or is
///    reachable via diff from the committed sync point → stream the B+ tree
///    diff in bounded chunks (falling back to a streaming rebuild when high
///    churn makes full construction cheaper).
/// 3. Otherwise → full streaming rebuild from the target tree's iterator.
///
/// Call [`flush`](Self::flush) or [`close`](Self::close) on clean shutdown so
/// any pinned working-root lease is released before writing the underlying
/// [`VerMap`]'s `[0x07]` reference checkpoint.
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
    working_lease: Option<Arc<RootLease>>,
    working_branch: Option<BranchId>,
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
        self.map.save_handle_meta(self)
    }
    pub fn from_meta(id: impl Into<InstanceId>) -> Result<Self> {
        let id = id.into();
        let id = InstanceId::new(id.map_id, id.ns.unwrap_or_default());
        let frame = load_versioned_meta::<Self>(id)?;
        Self::open_frame(
            crate::common::parse_prefix_handle_checked::<Self>(&frame, Some(id))?,
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
                .then_some((trie, commit, checkpoint.tree_root))
            });
        let (committed_trie, sync_commit, working_root) = restored
            .map(|(t, c, root)| (t, Some(c), Some(root)))
            .unwrap_or_default();
        Self {
            map,
            cache_instance: id,
            working_trie: committed_trie.clone(),
            committed_trie,
            sync_commit,
            working_root,
            working_lease: None,
            working_branch: None,
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

    /// Releases any pinned working-root lease and durably checkpoints the
    /// underlying [`VerMap`] reference graph (`[0x07]`) for fast reopen.
    pub fn flush(&mut self) -> Result<()> {
        self.working_lease = None;
        self.working_branch = None;
        self.map.flush()
    }

    /// Releases any pinned working-root lease and closes the underlying [`VerMap`].
    pub fn close(mut self) -> Result<()> {
        self.working_lease = None;
        self.working_branch = None;
        self.map.close()
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
        if self.working_root == Some(roots.working.root) {
            if roots.working.root == roots.committed.root {
                self.working_lease = None;
                self.working_branch = None;
                if let Some(commit) = roots.head {
                    self.committed_trie = self.working_trie.clone();
                    self.sync_commit = Some(commit);
                } else {
                    self.committed_trie = T::default();
                    self.sync_commit = None;
                }
            } else if self.working_lease.is_none() {
                self.working_lease = Some(roots.working);
                self.working_branch = Some(branch);
            }
            return Ok(self.working_trie.root_hash());
        }

        let candidate = if roots.working.root == roots.committed.root {
            if let Some(commit) = roots.head {
                self.sync_committed(commit, &roots.committed)?;
            } else {
                self.committed_trie = T::default();
                self.sync_commit = None;
            }
            self.committed_trie.clone()
        } else if self.working_branch == Some(branch)
            && let Some(prev) = self.working_lease.clone()
        {
            self.sync_or_rebuild(&self.working_trie, &prev, &roots.working)?
        } else {
            if let Some(commit) = roots.head {
                self.sync_committed(commit, &roots.committed)?;
            } else {
                self.committed_trie = T::default();
                self.sync_commit = None;
            }
            self.sync_or_rebuild(&self.committed_trie, &roots.committed, &roots.working)?
        };

        self.working_root = Some(roots.working.root);
        self.working_trie = candidate;
        if roots.working.root == roots.committed.root {
            self.working_lease = None;
            self.working_branch = None;
        } else {
            self.working_lease = Some(roots.working);
            self.working_branch = Some(branch);
        }
        Ok(self.working_trie.root_hash())
    }
    pub fn merkle_root_at_commit(&mut self, commit: CommitId) -> Result<[u8; 32]> {
        self.reset_if_replaced();
        let root = self.map.commit_root(commit)?;
        self.sync_committed(commit, &root)?;
        self.working_trie = self.committed_trie.clone();
        self.working_root = Some(root.root);
        self.working_lease = None;
        self.working_branch = None;
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
        self.working_lease = None;
        self.working_branch = None;
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
            self.working_lease = None;
            self.working_branch = None;
            self.sync_commit = None;
            self.working_trie = T::default();
            self.committed_trie = T::default();
        }
    }
    fn sync_committed(&mut self, commit: CommitId, root: &RootLease) -> Result<()> {
        if self.sync_commit == Some(commit) {
            return Ok(());
        }
        if self.working_root == Some(root.root) {
            self.committed_trie = self.working_trie.clone();
            self.sync_commit = Some(commit);
            return Ok(());
        }
        if let Some(from) = self.sync_commit.and_then(|c| self.map.commit_root(c).ok()) {
            self.committed_trie =
                self.sync_or_rebuild(&self.committed_trie, &from, root)?;
        } else {
            self.committed_trie = T::from_entries(self.map.tree.iter(root.root))?;
        }
        self.sync_commit = Some(commit);
        Ok(())
    }
    fn sync_or_rebuild(&self, base: &T, from: &RootLease, to: &RootLease) -> Result<T> {
        if to.root == EMPTY_ROOT {
            #[cfg(test)]
            let _ = self.map.iter_leased_root(to);
            return Ok(T::default());
        }
        if from.root == EMPTY_ROOT {
            return T::from_entries(self.map.iter_leased_root(to));
        }
        let rebuild_threshold = (self.map.tree.estimate_entries(to.root) / 2).max(512);
        let mut iter = self.map.diff_roots_iter(from, to)?;
        let mut candidate = base.clone();
        let mut total_diffs = 0usize;
        let mut chunk = Vec::with_capacity(DIFF_STREAM_CHUNK.min(256));
        loop {
            chunk.clear();
            while chunk.len() < DIFF_STREAM_CHUNK
                && let Some(entry) = iter.try_next()?
            {
                chunk.push(entry);
            }
            if chunk.is_empty() {
                return Ok(candidate);
            }
            total_diffs += chunk.len();
            if total_diffs > rebuild_threshold {
                drop(iter);
                return T::from_entries(self.map.tree.iter(to.root));
            }
            Self::apply_diff(&mut candidate, &chunk)?;
        }
    }
    /// Apply a diff chunk to `trie`.
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
