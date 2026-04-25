//!
//! SMT proof generation and verification.
//!
//! Each proof records 256 sibling hashes — one per level of the logical
//! (uncompressed) 256-level binary tree.  Compressed path levels have
//! `EMPTY_HASH` siblings since only one child is occupied.
//!
//! Verification recomputes the root hash bottom-up in exactly 256 hash
//! operations, independent of tree shape or compression.
//!

use crate::trie::error::{Result, TrieError};

use super::bitpath::BitPath;
use super::codec::{hash_internal, hash_leaf, wrap_hash};
use super::{EMPTY_HASH, SmtHandle, SmtNode, TREE_DEPTH};

/// A complete SMT proof for a single key.
///
/// For membership proofs, `value` is `Some(v)`.
/// For non-membership proofs, `value` is `None`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SmtProof {
    /// The key hash this proof covers.
    pub key_hash: [u8; 32],
    /// `Some(value)` for membership, `None` for non-membership.
    pub value: Option<Vec<u8>>,
    /// 256 sibling hashes, from depth 0 (root level) to depth 255.
    pub siblings: Vec<[u8; 32]>,
}

// =========================================================================
// Proof generation
// =========================================================================

/// Generates a proof for `key_hash` against a committed tree.
///
/// The tree must have been committed (all nodes hashed) before calling.
pub fn prove(root: &SmtHandle, key_hash: &[u8; 32]) -> Result<SmtProof> {
    let key_path = BitPath::from_hash(key_hash);
    let mut siblings = vec![EMPTY_HASH; TREE_DEPTH];
    let value = prove_walk(root, &key_path, key_hash, 0, &mut siblings)?;
    Ok(SmtProof {
        key_hash: *key_hash,
        value,
        siblings,
    })
}

fn prove_walk(
    handle: &SmtHandle,
    key_path: &BitPath,
    key_hash: &[u8; 32],
    depth: usize,
    siblings: &mut [[u8; 32]],
) -> Result<Option<Vec<u8>>> {
    let node = handle.node();

    match node {
        SmtNode::Empty => {
            // Non-membership: everything below is EMPTY (already initialised).
            Ok(None)
        }

        SmtNode::Leaf {
            key_hash: leaf_kh,
            value,
            path: _,
        } => {
            if leaf_kh == key_hash {
                // Membership proof.
                // All siblings from `depth` to 255 are EMPTY_HASH,
                // which is the correct representation: this leaf is
                // the sole occupant of this subtree.
                Ok(Some(value.clone()))
            } else {
                // Non-membership: a different leaf occupies part of
                // this subtree.  We need to place that leaf's hash
                // as a sibling at the divergence depth.
                let existing_full = BitPath::from_hash(leaf_kh);

                // Find where the two keys diverge (in absolute depth).
                let remaining_key = key_path.slice(depth, TREE_DEPTH);
                let remaining_existing = existing_full.slice(depth, TREE_DEPTH);
                let common = remaining_key.common_prefix(&remaining_existing);

                let diverge_depth = depth + common;
                if diverge_depth >= TREE_DEPTH {
                    return Err(TrieError::InvalidState(
                        "identical key hashes in non-membership proof".into(),
                    ));
                }

                // Compute the existing leaf's subtree hash at one level
                // below the divergence point, then that becomes the
                // sibling at the divergence level.
                let existing_leaf_hash = hash_leaf(leaf_kh, value);
                let below_diverge = existing_full.slice(diverge_depth + 1, TREE_DEPTH);
                siblings[diverge_depth] = wrap_hash(existing_leaf_hash, &below_diverge);

                Ok(None)
            }
        }

        SmtNode::Internal { path, left, right } => {
            let remaining = key_path.slice(depth, TREE_DEPTH);

            if !remaining.starts_with(path) {
                // Path diverges within the compressed prefix.
                // Compute the subtree hash at the divergence point.
                let common = remaining.common_prefix(path);
                let diverge_depth = depth + common;

                if diverge_depth >= TREE_DEPTH {
                    return Err(TrieError::InvalidState(
                        "depth overflow in divergent path".into(),
                    ));
                }

                let left_h = to_hash32(left.expect_hash()?)?;
                let right_h = to_hash32(right.expect_hash()?)?;
                let internal_h = hash_internal(&left_h, &right_h);

                // Wrap from the split level up to diverge_depth + 1.
                let suffix = path.slice(common + 1, path.len());
                siblings[diverge_depth] = wrap_hash(internal_h, &suffix);

                Ok(None)
            } else {
                // Full prefix matches.  Compressed levels already have
                // EMPTY_HASH siblings (correct: single-child chains).
                let split_depth = depth + path.len();
                if split_depth >= TREE_DEPTH {
                    return Err(TrieError::InvalidState(
                        "SMT depth exceeded 256 bits".into(),
                    ));
                }

                let bit = key_path.bit_at(split_depth);
                if bit == 0 {
                    siblings[split_depth] = to_hash32(right.expect_hash()?)?;
                    prove_walk(left, key_path, key_hash, split_depth + 1, siblings)
                } else {
                    siblings[split_depth] = to_hash32(left.expect_hash()?)?;
                    prove_walk(right, key_path, key_hash, split_depth + 1, siblings)
                }
            }
        }
    }
}

// =========================================================================
// Proof verification
// =========================================================================

/// Verifies an SMT proof against a root hash and expected key hash.
///
/// Returns `Ok(true)` if the proof is valid.
pub fn verify_proof(
    root_hash: &[u8; 32],
    expected_key_hash: &[u8; 32],
    proof: &SmtProof,
) -> Result<bool> {
    if &proof.key_hash != expected_key_hash {
        return Ok(false);
    }

    if proof.siblings.len() != TREE_DEPTH {
        return Err(TrieError::InvalidState(format!(
            "proof must have {} siblings, got {}",
            TREE_DEPTH,
            proof.siblings.len()
        )));
    }

    let key_path = BitPath::from_hash(&proof.key_hash);

    // Hash at depth 256 (the leaf level).
    let mut current = match proof.value {
        Some(ref v) => hash_leaf(&proof.key_hash, v),
        None => EMPTY_HASH,
    };

    // Walk from depth 255 up to depth 0.
    for depth in (0..TREE_DEPTH).rev() {
        let bit = key_path.bit_at(depth);
        if bit == 0 {
            current = hash_internal(&current, &proof.siblings[depth]);
        } else {
            current = hash_internal(&proof.siblings[depth], &current);
        }
    }

    Ok(current == *root_hash)
}

fn to_hash32(slice: &[u8]) -> Result<[u8; 32]> {
    slice
        .try_into()
        .map_err(|_| TrieError::InvalidState("expected 32-byte hash".into()))
}
