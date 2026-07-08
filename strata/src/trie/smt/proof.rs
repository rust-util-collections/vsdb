//!
//! SMT proof generation and verification.
//!
//! Proofs follow the Diem/JMT construction for a leaf-shortcut hash
//! domain: `siblings` holds one hash per level from depth 0 (root)
//! down to the top of the **terminal subtree** on the key's path — the
//! point where the walk ends at a lone leaf, an empty slot, or a
//! divergence.  Compressed single-child levels contribute `EMPTY_HASH`
//! siblings; typical proofs are O(log N) hashes instead of 256.
//!
//! The terminal subtree itself is described by `leaf`:
//! - `Some((kh, v))` with `kh == key_hash` → membership of `v`;
//! - `Some((kh, v))` with `kh != key_hash` → non-membership: a
//!   different lone leaf occupies the subtree the key would live in
//!   (the verifier checks both keys share the first `siblings.len()`
//!   path bits);
//! - `None` → non-membership: the key's slot is empty.
//!
//! Verification folds the terminal commitment upward along the key's
//! path bits in `siblings.len()` hash operations.
//!

use crate::trie::error::{Result, TrieError};

use super::{
    EMPTY_HASH, SmtHandle, SmtNode, TREE_DEPTH,
    bitpath::BitPath,
    codec::{hash_internal, hash_leaf, wrap_hash},
};

/// A compact SMT proof for a single key.
///
/// `siblings[d]` is the sibling hash at depth `d`; the terminal
/// subtree (lone leaf or empty slot) sits at depth `siblings.len()`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SmtProof {
    /// The key hash this proof covers.
    pub key_hash: [u8; 32],
    /// The lone leaf occupying the terminal subtree, if any.
    ///
    /// `Some((leaf_key_hash, value))`: membership when
    /// `leaf_key_hash == key_hash`, otherwise a conflicting leaf
    /// proving non-membership.  `None`: the slot is empty.
    pub leaf: Option<([u8; 32], Vec<u8>)>,
    /// Sibling hashes from depth 0 (root level) to the terminal
    /// subtree, at most [`TREE_DEPTH`] entries.
    pub siblings: Vec<[u8; 32]>,
}

impl SmtProof {
    /// Returns the proven value if this is a membership proof.
    pub fn value(&self) -> Option<&[u8]> {
        match &self.leaf {
            Some((kh, v)) if *kh == self.key_hash => Some(v),
            _ => None,
        }
    }
}

// =========================================================================
// Proof generation
// =========================================================================

/// Generates a proof for `key_hash` against a committed tree.
///
/// The tree must have been committed (all nodes hashed) before calling.
pub fn prove(root: &SmtHandle, key_hash: &[u8; 32]) -> Result<SmtProof> {
    let key_path = BitPath::from_hash(key_hash);
    let mut siblings: Vec<[u8; 32]> = Vec::new();
    let mut handle = root;
    let mut depth = 0usize;

    loop {
        match handle.node() {
            SmtNode::Empty => {
                // Only reachable at the root (internal nodes never have
                // empty children): the whole tree is empty.
                return Ok(SmtProof {
                    key_hash: *key_hash,
                    leaf: None,
                    siblings,
                });
            }

            SmtNode::Leaf {
                key_hash: leaf_kh,
                value,
                path: _,
            } => {
                // The terminal lone-leaf subtree starts here.  Whether
                // this is membership (leaf_kh == key_hash) or a
                // conflicting leaf, the proof simply carries it.
                return Ok(SmtProof {
                    key_hash: *key_hash,
                    leaf: Some((*leaf_kh, value.clone())),
                    siblings,
                });
            }

            SmtNode::Internal { path, left, right } => {
                if !key_path.starts_with_from(depth, path) {
                    // The key diverges inside the compressed prefix:
                    // its slot below the divergence bit is empty, and
                    // the sibling there is this internal node's
                    // remaining chain (which holds >= 2 leaves, so it
                    // wraps like any internal path).
                    let common = key_path.common_prefix_from(depth, path);
                    siblings.resize(depth + common, EMPTY_HASH);

                    let left_h = to_hash32(left.expect_hash()?)?;
                    let right_h = to_hash32(right.expect_hash()?)?;
                    let internal_h = hash_internal(&left_h, &right_h);
                    let suffix = path.slice(common + 1, path.len());
                    siblings.push(wrap_hash(internal_h, &suffix));

                    return Ok(SmtProof {
                        key_hash: *key_hash,
                        leaf: None,
                        siblings,
                    });
                }

                // Full prefix match: the compressed levels have empty
                // siblings, then the split contributes the other child.
                let split_depth = depth + path.len();
                if split_depth >= TREE_DEPTH {
                    return Err(TrieError::InvalidState(
                        "SMT depth exceeded 256 bits".into(),
                    ));
                }
                siblings.resize(split_depth, EMPTY_HASH);

                let bit = key_path.bit_at(split_depth);
                let (next, other) = if bit == 0 {
                    (left, right)
                } else {
                    (right, left)
                };
                siblings.push(to_hash32(other.expect_hash()?)?);

                handle = next;
                depth = split_depth + 1;
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

    if proof.siblings.len() > TREE_DEPTH {
        return Err(TrieError::InvalidState(format!(
            "proof must have at most {} siblings, got {}",
            TREE_DEPTH,
            proof.siblings.len()
        )));
    }

    let key_path = BitPath::from_hash(&proof.key_hash);

    // Commitment of the terminal subtree at depth `siblings.len()`.
    let mut current = match &proof.leaf {
        None => EMPTY_HASH,
        Some((leaf_kh, leaf_val)) => {
            if leaf_kh != &proof.key_hash {
                // Conflicting-leaf non-membership: the two keys must
                // share every path bit above the terminal subtree,
                // otherwise the fold below would not walk the leaf's
                // true position.
                let leaf_path = BitPath::from_hash(leaf_kh);
                if key_path.common_prefix(&leaf_path) < proof.siblings.len() {
                    return Ok(false);
                }
            }
            hash_leaf(leaf_kh, leaf_val)
        }
    };

    // Fold upward from the terminal subtree to the root.
    for depth in (0..proof.siblings.len()).rev() {
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
