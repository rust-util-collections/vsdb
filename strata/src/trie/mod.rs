//! Lightweight, stateless Merkle trie implementations.
//!
//! This module provides two in-memory Merkle data structures plus a
//! versioned-store integration layer:
//!
//! - **[`MptCalc`]** — Merkle Patricia Trie (16-ary, nibble-based).
//!   Best for general key-value Merkle commitments.
//! - **[`SmtCalc`]** — Sparse Merkle Tree (binary, 256-bit key-hash
//!   paths). Supports compact membership and non-membership proofs.
//! - **[`VerMapWithProof`]** — Wraps a [`VerMap`](crate::versioned::map::VerMap)
//!   with a [`TrieCalc`] back-end to provide versioned Merkle root computation
//!   with incremental diff-based updates and disposable cache persistence.
//!
//! Both `MptCalc` and `SmtCalc` are designed as **stateless computation
//! layers** on top of a versioned store (e.g. `VerMap`): the trie is
//! ephemeral and all persistence is handled by the underlying store.
//!
//! # Architecture: Trie + VerMap
//!
//! ```text
//!   VerMap<K,V>          MptCalc / SmtCalc
//!   (persistence)        (computation)
//!   +-------------+      +-------------+
//!   | branch/     |      | in-memory   |
//!   | commit/     | diff | trie nodes  |  root_hash()
//!   | merge/      |----->| (ephemeral) |-------------> [u8; 32]
//!   | rollback    |      |             |
//!   +-------------+      +-------------+
//!        |                  |         ^
//!        |           eager save    auto-load
//!        |           on sync      on new/from_map
//!        |                  |         |
//!        |                +--v--------+-+
//!        |                | disk cache  | (disposable)
//!        +----------------+-------------+
//! ```
//!
//! 1. **`VerMap`** handles persistence, branching, commits, merges.
//! 2. **`MptCalc` / `SmtCalc`** mirrors the current state as an
//!    in-memory trie, synchronized via full rebuild or incremental diff.
//! 3. **`root_hash()`** returns the 32-byte Merkle commitment.
//! 4. **Automatic cache** — on construction, the trie is silently
//!    restored from a previous cache file; after each commit sync,
//!    the clean state is eagerly saved.  No manual calls required.
//!
//! # SMT proofs
//!
//! [`SmtCalc`] additionally supports [`prove`](SmtCalc::prove) and
//! [`verify_proof`](SmtCalc::verify_proof) for Merkle inclusion and
//! exclusion proofs.  Proofs are compact (Diem/JMT style): sibling
//! hashes only down to the terminal lone-leaf or empty subtree on the
//! key's path — O(log N) hashes for N keys — and verification folds
//! the root in as many hash operations.

mod cache;
mod codec_util;
mod error;
mod mpt;
mod nibbles;
mod node;
pub mod proof;
mod smt;

#[cfg(test)]
mod test;

pub use error::TrieError;
pub use mpt::{MAX_MPT_KEY_LEN, MptProof};
pub use proof::VerMapWithProof;
pub use smt::SmtProof;

use std::{mem, path::Path};

use crate::common::error::Result;
use mpt::{TrieMut, TrieRo};
use node::NodeHandle;

/// Common interface for stateless, in-memory Merkle trie engines.
///
/// Implemented by [`MptCalc`] and [`SmtCalc`].  Used as the trie
/// back-end in [`VerMapWithProof`].
pub trait TrieCalc: Clone + Default {
    /// Builds a trie from key-value pairs.
    fn from_entries(
        kvs: impl IntoIterator<Item = (impl AsRef<[u8]>, impl AsRef<[u8]>)>,
    ) -> Result<Self>;

    /// Looks up a value by key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Inserts a key-value pair.
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Removes a key.
    fn remove(&mut self, key: &[u8]) -> Result<()>;

    /// Computes the 32-byte Merkle root hash.
    fn root_hash(&mut self) -> Result<Vec<u8>>;

    /// Applies a batch of insert/remove operations.
    fn batch_update(&mut self, ops: &[(&[u8], Option<&[u8]>)]) -> Result<()>;

    /// Saves the trie to a file under `dir` for fast restoration.
    fn save_cache(&mut self, dir: &Path, cache_id: u64, sync_tag: u64) -> Result<()>;

    /// Loads a previously saved trie from a file under `dir`.
    fn load_cache(dir: &Path, cache_id: u64) -> Result<(Self, u64, Vec<u8>)>;
}

/// A stateless, in-memory Merkle Patricia Trie.
///
/// `MptCalc` holds an in-memory trie that can be incrementally updated
/// with [`insert`](Self::insert) / [`remove`](Self::remove) /
/// [`batch_update`](Self::batch_update), queried with [`get`](Self::get),
/// and hashed with [`root_hash`](Self::root_hash).
///
/// Unlike a traditional persistent MPT, `MptCalc` does **not** manage
/// node storage or lifecycle.  All versioning, branching, and persistence
/// should be handled by an external store.
#[derive(Clone)]
pub struct MptCalc {
    root: NodeHandle,
}

impl Default for MptCalc {
    fn default() -> Self {
        Self::new()
    }
}

impl MptCalc {
    /// Creates an empty trie.
    pub fn new() -> Self {
        Self {
            root: NodeHandle::default(),
        }
    }

    /// Builds a trie by inserting all key-value pairs from an iterator.
    pub fn from_entries(
        kvs: impl IntoIterator<Item = (impl AsRef<[u8]>, impl AsRef<[u8]>)>,
    ) -> Result<Self> {
        let mut calc = Self::new();
        for (k, v) in kvs {
            calc.insert(k.as_ref(), v.as_ref())?;
        }
        Ok(calc)
    }

    /// Looks up a value by key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let trie = TrieRo::new(&self.root);
        Ok(trie.get(key)?)
    }

    /// Inserts a key-value pair into the trie.
    ///
    /// Returns an error if `key` is longer than [`MAX_MPT_KEY_LEN`] —
    /// unbounded key lengths would allow adversarial key sets to build
    /// stack-overflowing path depths.  `self`'s root is always restored
    /// before returning, so a rejected insert never loses trie data.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut trie = TrieMut::new(mem::take(&mut self.root));
        let result = trie.insert(key, value);
        self.root = trie.into_root();
        Ok(result?)
    }

    /// Removes a key from the trie.
    ///
    /// `self`'s root is always restored before returning, so a rejected
    /// remove never loses trie data.
    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let mut trie = TrieMut::new(mem::take(&mut self.root));
        let result = trie.remove(key);
        self.root = trie.into_root();
        Ok(result?)
    }

    /// Computes and returns the 32-byte Merkle root hash.
    ///
    /// Internally caches node hashes so that a subsequent call without
    /// intervening mutations is essentially free.  `self`'s root is
    /// always restored before returning, so a rejected commit never
    /// loses trie data.
    pub fn root_hash(&mut self) -> Result<Vec<u8>> {
        let mut trie = TrieMut::new(mem::take(&mut self.root));
        let hash = trie.commit();
        self.root = trie.into_root();
        Ok(hash?)
    }

    /// Applies a batch of insert/remove operations.
    ///
    /// Each entry is `(key, Some(value))` for insert or `(key, None)` for remove.
    /// `self`'s root is always restored before returning — even if an
    /// operation partway through the batch fails — so a rejected batch
    /// never loses trie data (the batch is not atomic: operations before
    /// the failure remain applied).
    pub fn batch_update(&mut self, ops: &[(&[u8], Option<&[u8]>)]) -> Result<()> {
        let mut trie = TrieMut::new(mem::take(&mut self.root));
        let mut result = Ok(());
        for (key, val) in ops {
            result = if let Some(v) = val {
                trie.insert(key, v)
            } else {
                trie.remove(key)
            };
            if result.is_err() {
                break;
            }
        }
        self.root = trie.into_root();
        Ok(result?)
    }

    // =================================================================
    // Proofs
    // =================================================================

    /// Generates a Merkle proof for the given key.
    ///
    /// The tree must be committed (call [`root_hash`](Self::root_hash)
    /// first) for proof generation to work.
    pub fn prove(&self, key: &[u8]) -> Result<MptProof> {
        Ok(mpt::proof::prove(&self.root, key)?)
    }

    /// Verifies an MPT proof against a root hash for a specific key.
    ///
    /// `expected_key` is the key the caller expects this proof to cover.
    pub fn verify_proof(
        root_hash: &[u8; 32],
        expected_key: &[u8],
        proof: &MptProof,
    ) -> Result<bool> {
        Ok(mpt::proof::verify_proof(root_hash, expected_key, proof)?)
    }

    // =================================================================
    // Cache (disposable persistence)
    // =================================================================

    /// Saves the trie to a file for fast restoration on restart.
    ///
    /// `sync_tag` is an opaque identifier (e.g. a `CommitId`) that the
    /// caller can use to determine whether the cache is still current.
    /// Call [`root_hash`](Self::root_hash) before saving to ensure node
    /// hashes are computed.
    ///
    /// The cache is **disposable**: if the file is lost or corrupted,
    /// the trie can always be rebuilt from the authoritative store.
    pub fn save_cache(
        &mut self,
        dir: &Path,
        cache_id: u64,
        sync_tag: u64,
    ) -> Result<()> {
        let hash = self.root_hash()?;
        let path = dir.join(format!("mpt_cache_{}.bin", cache_id));
        Ok(cache::save_to_file(&self.root, sync_tag, &hash, &path)?)
    }

    /// Loads a previously saved trie from a file.
    ///
    /// Returns `(MptCalc, sync_tag, root_hash)`.  The caller should
    /// compare `sync_tag` with the current store head and apply any
    /// diff via [`insert`](Self::insert)/[`remove`](Self::remove).
    pub fn load_cache(dir: &Path, cache_id: u64) -> Result<(Self, u64, Vec<u8>)> {
        let path = dir.join(format!("mpt_cache_{}.bin", cache_id));
        let (root, sync_tag, root_hash) = cache::load_from_file(&path)?;
        Ok((Self { root }, sync_tag, root_hash))
    }
}

impl TrieCalc for MptCalc {
    fn from_entries(
        kvs: impl IntoIterator<Item = (impl AsRef<[u8]>, impl AsRef<[u8]>)>,
    ) -> Result<Self> {
        Self::from_entries(kvs)
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(key)
    }
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.insert(key, value)
    }
    fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.remove(key)
    }
    fn root_hash(&mut self) -> Result<Vec<u8>> {
        self.root_hash()
    }
    fn batch_update(&mut self, ops: &[(&[u8], Option<&[u8]>)]) -> Result<()> {
        self.batch_update(ops)
    }
    fn save_cache(&mut self, dir: &Path, cache_id: u64, sync_tag: u64) -> Result<()> {
        self.save_cache(dir, cache_id, sync_tag)
    }
    fn load_cache(dir: &Path, cache_id: u64) -> Result<(Self, u64, Vec<u8>)> {
        Self::load_cache(dir, cache_id)
    }
}

// =====================================================================
// SmtCalc — Sparse Merkle Tree
// =====================================================================

/// A stateless, in-memory Sparse Merkle Tree.
///
/// Keys are hashed with Keccak256 to produce 256-bit paths.
/// The tree is a binary trie with compressed paths, fixed logical
/// depth of 256, and deterministic hashing.
///
/// API mirrors [`MptCalc`]: insert/remove/batch_update/get/root_hash,
/// plus [`prove`](Self::prove) and [`verify_proof`](Self::verify_proof)
/// for Merkle inclusion/exclusion proofs.
#[derive(Clone)]
pub struct SmtCalc {
    root: smt::SmtHandle,
}

impl Default for SmtCalc {
    fn default() -> Self {
        Self::new()
    }
}

impl SmtCalc {
    /// Creates an empty SMT.
    pub fn new() -> Self {
        Self {
            root: smt::SmtHandle::default(),
        }
    }

    /// Builds an SMT from key-value pairs.
    ///
    /// Keys are hashed internally via Keccak256.
    pub fn from_entries(
        kvs: impl IntoIterator<Item = (impl AsRef<[u8]>, impl AsRef<[u8]>)>,
    ) -> Result<Self> {
        let mut calc = Self::new();
        for (k, v) in kvs {
            calc.insert(k.as_ref(), v.as_ref())?;
        }
        Ok(calc)
    }

    /// Looks up a value by key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key_hash = Self::hash_key(key);
        let ro = smt::query::SmtRo::new(&self.root);
        Ok(ro.get(&key_hash)?)
    }

    /// Inserts a key-value pair.
    ///
    /// `self`'s root is always restored before returning, so a rejected
    /// insert never loses tree data.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let key_hash = Self::hash_key(key);
        let mut m = smt::mutation::SmtMut::new(mem::take(&mut self.root));
        let result = m.insert(&key_hash, value);
        self.root = m.into_root();
        Ok(result?)
    }

    /// Removes a key.
    ///
    /// `self`'s root is always restored before returning, so a rejected
    /// remove never loses tree data.
    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let key_hash = Self::hash_key(key);
        let mut m = smt::mutation::SmtMut::new(mem::take(&mut self.root));
        let result = m.remove(&key_hash);
        self.root = m.into_root();
        Ok(result?)
    }

    /// Computes the 32-byte Merkle root hash.
    ///
    /// Caches node hashes so repeated calls without mutations are free.
    /// `self`'s root is always restored before returning, so a rejected
    /// commit never loses tree data.
    pub fn root_hash(&mut self) -> Result<Vec<u8>> {
        let mut m = smt::mutation::SmtMut::new(mem::take(&mut self.root));
        let hash = m.commit();
        self.root = m.into_root();
        Ok(hash?)
    }

    /// Applies a batch of insert/remove operations.
    ///
    /// `self`'s root is always restored before returning — even if an
    /// operation partway through the batch fails — so a rejected batch
    /// never loses tree data (the batch is not atomic: operations before
    /// the failure remain applied).
    pub fn batch_update(&mut self, ops: &[(&[u8], Option<&[u8]>)]) -> Result<()> {
        let mut m = smt::mutation::SmtMut::new(mem::take(&mut self.root));
        let mut result = Ok(());
        for (key, val) in ops {
            let key_hash = Self::hash_key(key);
            result = if let Some(v) = val {
                m.insert(&key_hash, v)
            } else {
                m.remove(&key_hash)
            };
            if result.is_err() {
                break;
            }
        }
        self.root = m.into_root();
        Ok(result?)
    }

    /// Generates a Merkle proof for the given key.
    ///
    /// The tree must be committed (call [`root_hash`](Self::root_hash)
    /// first) for proof generation to work.
    pub fn prove(&self, key: &[u8]) -> Result<SmtProof> {
        let key_hash = Self::hash_key(key);
        Ok(smt::proof::prove(&self.root, &key_hash)?)
    }

    /// Verifies a proof against a root hash and expected key.
    pub fn verify_proof(
        root_hash: &[u8; 32],
        expected_key: &[u8],
        proof: &SmtProof,
    ) -> Result<bool> {
        let expected_key_hash = Self::hash_key(expected_key);
        Ok(smt::proof::verify_proof(
            root_hash,
            &expected_key_hash,
            proof,
        )?)
    }

    // =================================================================
    // Cache
    // =================================================================

    /// Saves the SMT to a file for fast restoration.
    pub fn save_cache(
        &mut self,
        dir: &Path,
        cache_id: u64,
        sync_tag: u64,
    ) -> Result<()> {
        let hash = self.root_hash()?;
        let path = dir.join(format!("smt_cache_{}.bin", cache_id));
        Ok(smt::cache::save_to_file(
            &self.root, sync_tag, &hash, &path,
        )?)
    }

    /// Loads a previously saved SMT from a file.
    pub fn load_cache(dir: &Path, cache_id: u64) -> Result<(Self, u64, Vec<u8>)> {
        let path = dir.join(format!("smt_cache_{}.bin", cache_id));
        let (root, sync_tag, root_hash) = smt::cache::load_from_file(&path)?;
        Ok((Self { root }, sync_tag, root_hash))
    }

    // =================================================================
    // Internal
    // =================================================================

    fn hash_key(key: &[u8]) -> [u8; 32] {
        use sha3::{Digest, Keccak256};
        Keccak256::digest(key).into()
    }
}

impl TrieCalc for SmtCalc {
    fn from_entries(
        kvs: impl IntoIterator<Item = (impl AsRef<[u8]>, impl AsRef<[u8]>)>,
    ) -> Result<Self> {
        Self::from_entries(kvs)
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(key)
    }
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.insert(key, value)
    }
    fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.remove(key)
    }
    fn root_hash(&mut self) -> Result<Vec<u8>> {
        self.root_hash()
    }
    fn batch_update(&mut self, ops: &[(&[u8], Option<&[u8]>)]) -> Result<()> {
        self.batch_update(ops)
    }
    fn save_cache(&mut self, dir: &Path, cache_id: u64, sync_tag: u64) -> Result<()> {
        self.save_cache(dir, cache_id, sync_tag)
    }
    fn load_cache(dir: &Path, cache_id: u64) -> Result<(Self, u64, Vec<u8>)> {
        Self::load_cache(dir, cache_id)
    }
}
