//! # vsdb_trie_db
//!
//! A lightweight, production-grade Merkle Patricia Trie (MPT) implementation.
//!
//! This crate provides a storage-agnostic MPT implementation, with a default
//! backend using `vsdb`.

mod config;
mod error;
mod nibbles;
mod node;
mod storage;
mod trie;

#[cfg(test)]
mod test;

pub use error::{Result, TrieError};
pub use storage::TrieBackend;
pub use storage::vsdb_impl::VsdbTrieBackend;
use trie::{TrieMut, TrieRo};

/// A handle to the Trie storage.
#[derive(Clone, Default)]
pub struct MptStore {
    backend: VsdbTrieBackend,
}

impl MptStore {
    pub fn new() -> Self {
        Self {
            backend: VsdbTrieBackend::new(),
        }
    }

    /// Initialize a new Trie with an empty root.
    pub fn trie_init(&self) -> MptOnce {
        MptOnce::new(self.backend.clone(), vec![0u8; 32])
    }

    /// Load an existing Trie from a root hash.
    pub fn trie_load(&self, root: &[u8]) -> MptOnce {
        MptOnce::new(self.backend.clone(), root.to_vec())
    }
}

/// An owned MPT instance that can be mutated.
pub struct MptOnce {
    backend: VsdbTrieBackend,
    root: Vec<u8>,
}

impl MptOnce {
    pub fn new(backend: VsdbTrieBackend, root: Vec<u8>) -> Self {
        Self { backend, root }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let trie = TrieRo::new(self.root.clone(), &self.backend);
        trie.get(key)
    }

    /// Insert a key-value pair and immediately commit to storage.
    ///
    /// **Note**: Each call performs a full trie commit (hashing + DB write).
    /// For bulk operations, use [`batch_update`](Self::batch_update) instead.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut trie = TrieMut::new(&self.root, &mut self.backend);
        trie.insert(key, value)?;
        self.root = trie.commit()?;
        Ok(())
    }

    /// Remove a key and immediately commit to storage.
    ///
    /// **Note**: Each call performs a full trie commit (hashing + DB write).
    /// For bulk operations, use [`batch_update`](Self::batch_update) instead.
    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let mut trie = TrieMut::new(&self.root, &mut self.backend);
        trie.remove(key)?;
        self.root = trie.commit()?;
        Ok(())
    }

    pub fn root(&self) -> Vec<u8> {
        self.root.clone()
    }

    pub fn batch_update(&mut self, ops: &[(&[u8], Option<&[u8]>)]) -> Result<()> {
        let mut trie = TrieMut::new(&self.root, &mut self.backend);
        for (key, val) in ops {
            if let Some(v) = val {
                trie.insert(key, v)?;
            } else {
                trie.remove(key)?;
            }
        }
        self.root = trie.commit()?;
        Ok(())
    }
}
