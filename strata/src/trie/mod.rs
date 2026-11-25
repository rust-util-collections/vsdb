//! Lightweight, stateless Merkle trie implementations.
//!
//! This module provides two in-memory Merkle data structures plus a
//! versioned-store integration layer:
//!
//! - **[`MptCalc`]** — Merkle Patricia Trie (16-ary, nibble-based).
//!   Best for general key-value Merkle commitments.
//! - **[`SmtCalc`]** — Sparse Merkle Tree (binary, 256-bit key-hash
//!   paths). Supports compact membership and non-membership proofs.
//! - **[`VerMapWithProof`]** — Wraps a `VerMap`
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
//!        |           checkpoint    auto-load
//!        |           on request    on from_map
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
//! 4. **Disposable cache** — [`VerMapWithProof::from_map`] attempts to restore a
//!    previous live-commit checkpoint. Call `VerMapWithProof::save_cache(commit)`
//!    to save one explicitly; root calculations and handle destruction never
//!    write it.
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
mod mpt;
mod nibbles;
mod node;
pub mod proof;
mod shared;
mod smt;
mod transport;

#[cfg(test)]
mod test;

pub use mpt::{MAX_MPT_KEY_LEN, MptProof};
pub use proof::{ProofWithRoot, VerMapWithProof};
pub use smt::SmtProof;

use crate::{InstanceId, Result};
use cache::CacheNode;
use std::path::Path;

fn invalid_state(detail: String) -> crate::VsdbError {
    crate::VsdbError::Trie { detail }
}

/// Binding of a disposable checkpoint to the exact committed tree it represents.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub universe: u128,
    pub map: InstanceId,
    pub commit: u64,
    pub tree_root: u64,
}
impl Checkpoint {
    fn standalone(cache_id: u64, tag: u64) -> Self {
        Self {
            universe: 0,
            map: cache_id.into(),
            commit: tag,
            tree_root: 0,
        }
    }
}

/// Stateless in-memory commitment and proof engine. Failed updates leave the
/// previous root accessible, including across unwinding.
pub trait TrieCalc: Clone + Default {
    type Proof;
    fn from_entries(
        kvs: impl IntoIterator<Item = (impl AsRef<[u8]>, impl AsRef<[u8]>)>,
    ) -> Result<Self>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn remove(&mut self, key: &[u8]) -> Result<()>;
    fn batch_update(&mut self, ops: &[(&[u8], Option<&[u8]>)]) -> Result<()>;
    fn root_hash(&self) -> [u8; 32];
    fn prove(&self, key: &[u8]) -> Result<Self::Proof>;
    fn verify_proof(root: &[u8; 32], key: &[u8], proof: &Self::Proof) -> Result<bool>;
    fn save_checkpoint(&self, dir: &Path, id: Checkpoint) -> Result<()>;
    fn load_checkpoint(dir: &Path, cache_id: u64) -> Result<(Self, Checkpoint)>;
    fn save_cache(&self, dir: &Path, cache_id: u64, tag: u64) -> Result<()> {
        self.save_checkpoint(dir, Checkpoint::standalone(cache_id, tag))
    }
    fn load_cache(dir: &Path, cache_id: u64) -> Result<(Self, u64, [u8; 32])> {
        let (calc, id) = Self::load_checkpoint(dir, cache_id)?;
        let hash = calc.root_hash();
        Ok((calc, id.commit, hash))
    }
}

/// An in-memory Merkle Patricia Trie with shared immutable subtrees.
#[derive(Clone, Default)]
pub struct MptCalc {
    root: node::NodeHandle,
}
/// An in-memory compressed sparse Merkle tree with shared immutable subtrees.
#[derive(Clone, Default)]
pub struct SmtCalc {
    root: smt::SmtHandle,
}

impl MptCalc {
    fn edit(
        root: &mut node::NodeHandle,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<()> {
        if let Some(v) = value {
            mpt::mutation::insert(root, key, v)?
        } else {
            *root = mpt::mutation::remove(root, key)
        }
        Ok(())
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(mpt::query::lookup(&self.root, key, |_| {}))
    }
    pub fn prove(&self, key: &[u8]) -> Result<MptProof> {
        mpt::proof::prove(&self.root, key)
    }
    pub fn verify_proof(root: &[u8; 32], key: &[u8], proof: &MptProof) -> Result<bool> {
        mpt::proof::verify_proof(root, key, proof)
    }
}
impl SmtCalc {
    fn hash_key(key: &[u8]) -> [u8; 32] {
        use sha3::{Digest, Keccak256};
        Keccak256::digest(key).into()
    }
    fn edit(root: &mut smt::SmtHandle, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let hash = Self::hash_key(key);
        if let Some(v) = value {
            smt::mutation::insert(root, hash, v)
        } else {
            *root = smt::mutation::remove(root, &hash)
        }
        Ok(())
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(smt::query::get(&self.root, &Self::hash_key(key)))
    }
    pub fn prove(&self, key: &[u8]) -> Result<SmtProof> {
        smt::proof::prove(&self.root, &Self::hash_key(key))
    }
    pub fn verify_proof(root: &[u8; 32], key: &[u8], proof: &SmtProof) -> Result<bool> {
        smt::proof::verify_proof(root, &Self::hash_key(key), proof)
    }
}

macro_rules! trie_calc {
    ($calc:ident,$node:ty,$proof:ty) => {
        impl $calc {
            pub fn new() -> Self {
                Self::default()
            }
            pub fn from_entries(
                kvs: impl IntoIterator<Item = (impl AsRef<[u8]>, impl AsRef<[u8]>)>,
            ) -> Result<Self> {
                let mut calc = Self::new();
                for (k, v) in kvs {
                    Self::edit(&mut calc.root, k.as_ref(), Some(v.as_ref()))?
                }
                Ok(calc)
            }
            pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
                self.batch_update(&[(key, Some(value))])
            }
            pub fn remove(&mut self, key: &[u8]) -> Result<()> {
                self.batch_update(&[(key, None)])
            }
            /// Applies all operations to a candidate root before publishing it.
            pub fn batch_update(
                &mut self,
                ops: &[(&[u8], Option<&[u8]>)],
            ) -> Result<()> {
                self.root.try_update(|candidate| {
                    for &(key, value) in ops {
                        Self::edit(candidate, key, value)?
                    }
                    Ok(())
                })
            }
            /// Returns the root commitment, caching each node's hash in place.
            pub fn root_hash(&self) -> [u8; 32] {
                self.root.root_hash()
            }
            pub fn save_cache(&self, dir: &Path, cache_id: u64, tag: u64) -> Result<()> {
                TrieCalc::save_cache(self, dir, cache_id, tag)
            }
            pub fn load_cache(
                dir: &Path,
                cache_id: u64,
            ) -> Result<(Self, u64, [u8; 32])> {
                <Self as TrieCalc>::load_cache(dir, cache_id)
            }
        }
        impl TrieCalc for $calc {
            type Proof = $proof;
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
            fn batch_update(&mut self, ops: &[(&[u8], Option<&[u8]>)]) -> Result<()> {
                self.batch_update(ops)
            }
            fn root_hash(&self) -> [u8; 32] {
                self.root_hash()
            }
            fn prove(&self, key: &[u8]) -> Result<Self::Proof> {
                self.prove(key)
            }
            fn verify_proof(
                root: &[u8; 32],
                key: &[u8],
                proof: &Self::Proof,
            ) -> Result<bool> {
                Self::verify_proof(root, key, proof)
            }
            fn save_checkpoint(&self, dir: &Path, id: Checkpoint) -> Result<()> {
                let path =
                    dir.join(format!("{}_cache_{}.bin", <$node>::NAME, id.map.map_id));
                cache::save(&path, &self.root, id, cache::MAX_FILE)
            }
            fn load_checkpoint(dir: &Path, cache_id: u64) -> Result<(Self, Checkpoint)> {
                let path = dir.join(format!("{}_cache_{}.bin", <$node>::NAME, cache_id));
                let (root, id) = cache::load(&path)?;
                if id.map.map_id != cache_id {
                    return Err(crate::VsdbError::Trie {
                        detail: "wrong cache instance".into(),
                    });
                }
                Ok((Self { root }, id))
            }
        }
    };
}
trie_calc!(MptCalc, node::Node, MptProof);
trie_calc!(SmtCalc, smt::SmtNode, SmtProof);
