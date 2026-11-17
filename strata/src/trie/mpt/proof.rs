//! MPT proof generation and verification.
//!
//! Each proof contains the full encoded form of every node on the lookup
//! path from root to the key's position.  Verification is stateless:
//! the verifier hashes each node, checks the hash chain, and follows the
//! key's nibble path to determine membership or non-membership.

use sha3::{Digest, Keccak256};

use crate::trie::{
    codec_util::{checked_end, read_bytes, read_u8, read_varint},
    error::{Result, TrieError},
    mpt::MAX_MPT_KEY_LEN,
    nibbles::Nibbles,
    node::{Node, NodeCodec, NodeHandle},
};

const MAX_TOTAL_NIBBLES: usize = 2 * MAX_MPT_KEY_LEN;

/// A Merkle Patricia Trie proof for a single key.
///
/// Contains an ordered sequence of encoded nodes from the root to the
/// key's position.  For membership proofs `value` is `Some(v)`.
/// For non-membership proofs `value` is `None`.
///
/// Use the accessors to inspect a proof and [`to_bytes`](Self::to_bytes) /
/// [`from_bytes`](Self::from_bytes) to transport it between processes.
/// Serde uses the same versioned representation. Decoding does not verify
/// the proof; verify it against the expected root and key before using it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MptProof {
    /// The original key this proof covers.
    pub(crate) key: Vec<u8>,
    /// `Some(value)` for membership, `None` for non-membership.
    pub(crate) value: Option<Vec<u8>>,
    /// Encoded nodes from root to the key's position in the trie.
    /// Each entry is `NodeCodec::encode(node)` for a node on the path.
    /// The first entry corresponds to the root node.
    pub(crate) nodes: Vec<Vec<u8>>,
}

impl MptProof {
    /// Returns the key this proof covers.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns `Some(value)` for membership proofs, `None` for
    /// non-membership proofs.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Returns the encoded proof nodes from root to the key's position.
    pub fn nodes(&self) -> &[Vec<u8>] {
        &self.nodes
    }
}

// =========================================================================
// Proof generation
// =========================================================================

/// Generates a proof for `key` against a committed trie.
///
/// The trie must have been committed (all nodes hashed via `root_hash()`)
/// before calling.
pub fn prove(root: &NodeHandle, key: &[u8]) -> Result<MptProof> {
    let path = Nibbles::from_raw(key);
    let mut nodes = Vec::new();

    // Special case: empty trie — root is InMemory(Null).
    if let NodeHandle::InMemory(n) = root
        && **n == Node::Null
    {
        nodes.push(NodeCodec::encode(&Node::Null));
        return Ok(MptProof {
            key: key.to_vec(),
            value: None,
            nodes,
        });
    }

    let value = prove_walk(root, path.as_slice(), &mut nodes)?;
    Ok(MptProof {
        key: key.to_vec(),
        value,
        nodes,
    })
}

fn prove_walk(
    mut handle: &NodeHandle,
    mut path: &[u8],
    nodes: &mut Vec<Vec<u8>>,
) -> Result<Option<Vec<u8>>> {
    loop {
        let node = resolve(handle)?;
        nodes.push(NodeCodec::encode(node));
        match node {
            Node::Null => return Ok(None),
            Node::Leaf {
                path: leaf_path,
                value,
            } => {
                return Ok((leaf_path.as_slice() == path).then(|| value.clone()));
            }
            Node::Extension {
                path: ext_path,
                child,
            } => {
                let Some(remaining) = path.strip_prefix(ext_path.as_slice()) else {
                    return Ok(None);
                };
                path = remaining;
                handle = child;
            }
            Node::Branch { children, value } => {
                let Some((&index, remaining)) = path.split_first() else {
                    return Ok(value.clone());
                };
                let Some(child) = &children[index as usize] else {
                    return Ok(None);
                };
                path = remaining;
                handle = child;
            }
        }
    }
}

fn resolve(handle: &NodeHandle) -> Result<&Node> {
    match handle {
        NodeHandle::InMemory(n) => {
            if **n == Node::Null {
                Ok(n)
            } else {
                Err(TrieError::InvalidState(
                    "prove() requires a committed trie — call root_hash() first".into(),
                ))
            }
        }
        NodeHandle::Cached(_, n) => Ok(n),
    }
}

// =========================================================================
// Proof verification
// =========================================================================

/// Verifies an MPT proof against a root hash for a specific key.
///
/// `expected_key` is the key the caller expects this proof to cover.
/// If the proof's key does not match, verification fails immediately.
///
/// Returns `Ok(true)` if the proof is valid.
pub fn verify_proof(
    root_hash: &[u8; 32],
    expected_key: &[u8],
    proof: &MptProof,
) -> Result<bool> {
    if proof.key != expected_key {
        return Ok(false);
    }

    if proof.nodes.is_empty() {
        return Ok(false);
    }

    // Step 1: root node hash must match.
    let first_hash: [u8; 32] = Keccak256::digest(&proof.nodes[0]).into();
    if first_hash != *root_hash {
        // Special case: empty trie has all-zero root hash and the proof
        // node is a single encoded Null (0x00).
        if *root_hash == [0u8; 32]
            && proof.nodes.len() == 1
            && proof.nodes[0] == [0x00]
            && proof.value.is_none()
        {
            return Ok(true);
        }
        return Ok(false);
    }

    // Step 2: walk through proof nodes following the key path.
    let path = Nibbles::from_raw(&proof.key);
    let mut remaining = path.as_slice();
    let mut node_idx: usize = 0;
    let mut consumed: usize = 0;
    let mut expect_branch = false;

    loop {
        if node_idx >= proof.nodes.len() {
            return Ok(false);
        }

        let decoded = decode_node(&proof.nodes[node_idx])?;
        if expect_branch && !matches!(decoded, DecodedNode::Branch { .. }) {
            return Err(TrieError::InvalidState(
                "non-canonical extension child in MPT proof".into(),
            ));
        }

        match decoded {
            DecodedNode::Null => {
                return Err(TrieError::InvalidState(
                    "hashed MPT proof node cannot be Null".into(),
                ));
            }

            DecodedNode::Leaf {
                path: leaf_path,
                value,
            } => {
                check_nibble_budget(consumed, leaf_path.len())?;
                if node_idx + 1 != proof.nodes.len() {
                    return Ok(false);
                }
                if leaf_path.as_slice() == remaining {
                    return Ok(proof.value.as_deref() == Some(value.as_slice()));
                } else {
                    return Ok(proof.value.is_none());
                }
            }

            DecodedNode::Extension {
                path: ext_path,
                child_hash,
            } => {
                check_nibble_budget(consumed, ext_path.len())?;
                let Some(rest) = remaining.strip_prefix(ext_path.as_slice()) else {
                    if node_idx + 1 != proof.nodes.len() {
                        return Ok(false);
                    }
                    return Ok(proof.value.is_none());
                };
                consumed += ext_path.len();
                expect_branch = true;
                remaining = rest;
                node_idx += 1;
                if node_idx >= proof.nodes.len() {
                    return Ok(false);
                }
                let next_hash: [u8; 32] =
                    Keccak256::digest(&proof.nodes[node_idx]).into();
                if next_hash != child_hash {
                    return Ok(false);
                }
                // continue loop
            }

            DecodedNode::Branch { children, value } => {
                check_nibble_budget(consumed, 1)?;
                let Some((&index, rest)) = remaining.split_first() else {
                    if node_idx + 1 != proof.nodes.len() {
                        return Ok(false);
                    }
                    return match (&proof.value, &value) {
                        (Some(pv), Some(bv)) => Ok(pv.as_slice() == bv.as_slice()),
                        (None, None) => Ok(true),
                        _ => Ok(false),
                    };
                };
                remaining = rest;
                match &children[index as usize] {
                    None => {
                        if node_idx + 1 != proof.nodes.len() {
                            return Ok(false);
                        }
                        return Ok(proof.value.is_none());
                    }
                    Some(child_hash) => {
                        consumed += 1;
                        expect_branch = false;
                        node_idx += 1;
                        if node_idx >= proof.nodes.len() {
                            return Ok(false);
                        }
                        let next_hash: [u8; 32] =
                            Keccak256::digest(&proof.nodes[node_idx]).into();
                        if next_hash != *child_hash {
                            return Ok(false);
                        }
                        // continue loop
                    }
                }
            }
        }
    }
}

// =========================================================================
// Node decoder (exact reverse of NodeCodec::encode)
// =========================================================================

#[derive(Debug)]
enum DecodedNode {
    Null,
    Leaf {
        path: Nibbles,
        value: Vec<u8>,
    },
    Extension {
        path: Nibbles,
        child_hash: [u8; 32],
    },
    Branch {
        children: Box<[Option<[u8; 32]>; 16]>,
        value: Option<Vec<u8>>,
    },
}

fn decode_node(data: &[u8]) -> Result<DecodedNode> {
    if data.is_empty() {
        return Err(TrieError::InvalidState("empty node data".into()));
    }

    let mut cursor = 1; // skip tag byte

    match data[0] {
        0x00 => {
            ensure_no_trailing(data, cursor)?;
            Ok(DecodedNode::Null)
        }

        0x01 => {
            // Leaf: tag + path + value
            let path = decode_path(data, &mut cursor)?;
            let value = read_bytes(data, &mut cursor)?;
            ensure_no_trailing(data, cursor)?;
            Ok(DecodedNode::Leaf { path, value })
        }

        0x02 => {
            // Extension: tag + path + 32-byte child_hash
            let path = decode_path(data, &mut cursor)?;
            if path.is_empty() {
                return Err(TrieError::InvalidState("empty extension path".into()));
            }
            let end = checked_end(cursor, 32, data.len(), "extension child hash")?;
            let mut child_hash = [0u8; 32];
            child_hash.copy_from_slice(&data[cursor..end]);
            cursor = end;
            ensure_no_trailing(data, cursor)?;
            Ok(DecodedNode::Extension { path, child_hash })
        }

        0x03 => {
            // Branch: tag + u16_le bitmap + has_value + [value] + [child_hashes]
            let end = checked_end(cursor, 2, data.len(), "branch bitmap")?;
            let bitmap = u16::from_le_bytes([data[cursor], data[cursor + 1]]);
            cursor = end;

            let has_value = match read_u8(data, &mut cursor)? {
                0 => false,
                1 => true,
                flag => {
                    return Err(TrieError::InvalidState(format!(
                        "invalid branch value flag: {flag}"
                    )));
                }
            };

            let value = if has_value {
                Some(read_bytes(data, &mut cursor)?)
            } else {
                None
            };

            let child_count = bitmap.count_ones() as usize;
            if child_count == 0 || (child_count == 1 && value.is_none()) {
                return Err(TrieError::InvalidState("non-canonical branch node".into()));
            }

            let mut children: [Option<[u8; 32]>; 16] = [None; 16];
            for (i, slot) in children.iter_mut().enumerate() {
                if bitmap & (1 << i) != 0 {
                    let end = checked_end(cursor, 32, data.len(), "branch child hash")?;
                    let mut h = [0u8; 32];
                    h.copy_from_slice(&data[cursor..end]);
                    cursor = end;
                    *slot = Some(h);
                }
            }
            ensure_no_trailing(data, cursor)?;

            Ok(DecodedNode::Branch {
                children: Box::new(children),
                value,
            })
        }

        tag => Err(TrieError::InvalidState(format!(
            "unknown node tag: 0x{tag:02x}"
        ))),
    }
}

fn ensure_no_trailing(data: &[u8], cursor: usize) -> Result<()> {
    if cursor != data.len() {
        return Err(TrieError::InvalidState("trailing bytes in MPT node".into()));
    }
    Ok(())
}

fn check_nibble_budget(consumed: usize, additional: usize) -> Result<()> {
    if consumed.saturating_add(additional) > MAX_TOTAL_NIBBLES {
        return Err(TrieError::InvalidState(
            "MPT proof: cumulative path length exceeds MAX_MPT_KEY_LEN".into(),
        ));
    }
    Ok(())
}

fn decode_path(data: &[u8], cursor: &mut usize) -> Result<Nibbles> {
    let nibble_count = read_varint(data, cursor)?;
    if nibble_count > MAX_TOTAL_NIBBLES {
        return Err(TrieError::InvalidState(
            "MPT proof: node path length exceeds MAX_MPT_KEY_LEN".into(),
        ));
    }
    // Nibbles are packed 2 per byte (high nibble first).
    let byte_count = nibble_count.div_ceil(2);
    let end = checked_end(*cursor, byte_count, data.len(), "path data")?;
    if nibble_count % 2 == 1 && (data[end - 1] & 0x0F) != 0 {
        return Err(TrieError::InvalidState(
            "non-zero odd-nibble padding".into(),
        ));
    }
    let mut nibbles = Vec::with_capacity(nibble_count);
    for i in 0..nibble_count {
        let byte_idx = *cursor + i / 2;
        let nibble = if i % 2 == 0 {
            data[byte_idx] >> 4
        } else {
            data[byte_idx] & 0x0F
        };
        nibbles.push(nibble);
    }
    *cursor = end;
    Ok(Nibbles::from_nibbles_unsafe(nibbles))
}
