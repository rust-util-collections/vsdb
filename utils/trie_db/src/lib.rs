//! # vsdb_trie_db
//!
//! A lightweight, stateless Merkle Patricia Trie (MPT) implementation.
//!
//! This crate provides a purely in-memory MPT for computing Merkle root
//! hashes over key-value datasets.  It is designed to be used as a
//! computation layer on top of a versioned store (e.g. `VerMap`), where
//! the trie itself is ephemeral and all persistence is handled by the
//! underlying store.

mod cache;
mod error;
mod nibbles;
mod node;
mod trie;

#[cfg(test)]
mod test;

pub use error::{Result, TrieError};

use node::NodeHandle;
use trie::{TrieMut, TrieRo};

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
        trie.get(key)
    }

    /// Inserts a key-value pair into the trie.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut trie = TrieMut::new(std::mem::take(&mut self.root));
        trie.insert(key, value)?;
        self.root = trie.into_root();
        Ok(())
    }

    /// Removes a key from the trie.
    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let mut trie = TrieMut::new(std::mem::take(&mut self.root));
        trie.remove(key)?;
        self.root = trie.into_root();
        Ok(())
    }

    /// Computes and returns the 32-byte Merkle root hash.
    ///
    /// Internally caches node hashes so that a subsequent call without
    /// intervening mutations is essentially free.
    pub fn root_hash(&mut self) -> Result<Vec<u8>> {
        let trie = TrieMut::new(std::mem::take(&mut self.root));
        let (hash, new_root) = trie.commit()?;
        self.root = new_root;
        Ok(hash)
    }

    /// Applies a batch of insert/remove operations.
    ///
    /// Each entry is `(key, Some(value))` for insert or `(key, None)` for remove.
    pub fn batch_update(&mut self, ops: &[(&[u8], Option<&[u8]>)]) -> Result<()> {
        let mut trie = TrieMut::new(std::mem::take(&mut self.root));
        for (key, val) in ops {
            if let Some(v) = val {
                trie.insert(key, v)?;
            } else {
                trie.remove(key)?;
            }
        }
        self.root = trie.into_root();
        Ok(())
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
        path: &std::path::Path,
        sync_tag: u64,
    ) -> Result<()> {
        let hash = self.root_hash()?;
        cache::save_to_file(&self.root, sync_tag, &hash, path)
    }

    /// Loads a previously saved trie from a file.
    ///
    /// Returns `(MptCalc, sync_tag, root_hash)`.  The caller should
    /// compare `sync_tag` with the current store head and apply any
    /// diff via [`insert`](Self::insert)/[`remove`](Self::remove).
    pub fn load_cache(
        path: &std::path::Path,
    ) -> Result<(Self, u64, Vec<u8>)> {
        let (root, sync_tag, root_hash) = cache::load_from_file(path)?;
        Ok((Self { root }, sync_tag, root_hash))
    }
}
