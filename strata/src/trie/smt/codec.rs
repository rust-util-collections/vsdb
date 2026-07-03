//!
//! SMT hash functions.
//!
//! The hash domain is the **compressed** binary trie with a
//! depth-independent leaf shortcut (the Diem/JMT construction):
//!
//! - **Empty**:    `EMPTY_HASH = [0u8; 32]`
//! - **Lone leaf**: a subtree containing exactly one leaf commits to
//!   `Keccak256(0x01 || key_hash || value)` — *regardless of the
//!   subtree's depth or the leaf's residual path*.  The full `key_hash`
//!   binds the leaf's position, so depth-independence costs nothing:
//!   relocating the leaf changes the sibling fold and thus the root.
//! - **Internal**: a subtree containing two or more leaves commits to
//!   `Keccak256(0x00 || left_hash || right_hash)`; if both children are
//!   empty → `EMPTY_HASH`.
//!
//! Leaf and internal preimages live in disjoint domains (`0x01` vs
//! `0x00` tag), the standard second-preimage hardening for Merkle
//! trees.
//!
//! The empty-empty special case ensures that `wrap_hash(EMPTY, path)`
//! stays `EMPTY` regardless of path length, which is essential for
//! proof verification (non-membership proofs fold `EMPTY_HASH` upward
//! and must reconstruct empty subtrees correctly).
//!
//! A compressed **internal** node that skips N levels is equivalent to
//! N nested single-child internal nodes (each with an empty sibling);
//! [`wrap_hash`] reproduces that chain.  Leaf paths are *not* wrapped —
//! that is exactly the leaf shortcut, which keeps hashing O(N) total
//! instead of O(N × 256).
//!

use sha3::{Digest, Keccak256};

use super::EMPTY_HASH;
use super::bitpath::BitPath;

/// Hashes a leaf: `Keccak256(0x01 || key_hash || value)`.
pub fn hash_leaf(key_hash: &[u8; 32], value: &[u8]) -> [u8; 32] {
    let mut h = Keccak256::new();
    h.update([0x01]);
    h.update(key_hash);
    h.update(value);
    h.finalize().into()
}

/// Hashes an internal node.
///
/// If both children are empty, returns `EMPTY_HASH` (preserving the
/// empty-subtree invariant).  Otherwise `Keccak256(0x00 || left_hash ||
/// right_hash)` — the `0x00` domain tag keeps internal preimages
/// disjoint from leaf preimages (`0x01`).
pub fn hash_internal(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    if *left == EMPTY_HASH && *right == EMPTY_HASH {
        return EMPTY_HASH;
    }
    let mut h = Keccak256::new();
    h.update([0x00]);
    h.update(left);
    h.update(right);
    h.finalize().into()
}

/// "Lifts" a hash through a compressed **internal** path by nesting it
/// inside single-child internal nodes.
///
/// For each bit in `path` (from the last bit back to the first):
/// - bit = 0 → `H(hash || EMPTY_HASH)` (hash is on the left)
/// - bit = 1 → `H(EMPTY_HASH || hash)` (hash is on the right)
///
/// Only internal-node prefixes are wrapped; lone-leaf subtrees commit
/// depth-independently (see the module docs).
pub fn wrap_hash(mut hash: [u8; 32], path: &BitPath) -> [u8; 32] {
    for i in (0..path.len()).rev() {
        let bit = path.bit_at(i);
        if bit == 0 {
            hash = hash_internal(&hash, &EMPTY_HASH);
        } else {
            hash = hash_internal(&EMPTY_HASH, &hash);
        }
    }
    hash
}
