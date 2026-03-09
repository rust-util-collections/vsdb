//!
//! SMT hash functions.
//!
//! The hash is defined on the *logical* (uncompressed) 256-level binary tree:
//!
//! - **Empty**:    `EMPTY_HASH = [0u8; 32]`
//! - **Leaf**:     `Keccak256(0x01 || key_hash || value)`
//! - **Internal**: if both children are empty → `EMPTY_HASH`,
//!   otherwise `Keccak256(left_hash || right_hash)`
//!
//! The empty-empty special case ensures that `wrap_hash(EMPTY, path)`
//! stays `EMPTY` regardless of path length, which is essential for
//! proof verification (non-membership proofs start from `EMPTY_HASH`
//! at depth 256 and must reconstruct empty subtrees correctly).
//!
//! Compression is transparent to hashing.  A compressed node that skips
//! N levels is equivalent to N nested single-child internal nodes.
//! [`wrap_hash`] reproduces that chain efficiently.
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
/// empty-subtree invariant).  Otherwise `Keccak256(left_hash || right_hash)`.
pub fn hash_internal(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    if *left == EMPTY_HASH && *right == EMPTY_HASH {
        return EMPTY_HASH;
    }
    let mut h = Keccak256::new();
    h.update(left);
    h.update(right);
    h.finalize().into()
}

/// "Lifts" a hash through a compressed path by nesting it inside
/// single-child internal nodes.
///
/// For each bit in `path` (from the last bit back to the first):
/// - bit = 0 → `H(hash || EMPTY_HASH)` (hash is on the left)
/// - bit = 1 → `H(EMPTY_HASH || hash)` (hash is on the right)
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
