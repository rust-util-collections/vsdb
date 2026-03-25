//! MPT proof generation and verification.
//!
//! Each proof contains the full encoded form of every node on the lookup
//! path from root to the key's position.  Verification is stateless:
//! the verifier hashes each node, checks the hash chain, and follows the
//! key's nibble path to determine membership or non-membership.

use sha3::{Digest, Keccak256};

use crate::trie::error::{Result, TrieError};
use crate::trie::nibbles::Nibbles;
use crate::trie::node::{Node, NodeCodec, NodeHandle};

/// A Merkle Patricia Trie proof for a single key.
///
/// Contains an ordered sequence of encoded nodes from the root to the
/// key's position.  For membership proofs `value` is `Some(v)`.
/// For non-membership proofs `value` is `None`.
///
/// Fields are crate-private to prevent external mutation; use the
/// accessor methods to read them.
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
    let path = Nibbles::from_raw(key, false);
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

    let value = prove_walk(root, path, &mut nodes)?;
    Ok(MptProof {
        key: key.to_vec(),
        value,
        nodes,
    })
}

fn prove_walk(
    handle: &NodeHandle,
    path: Nibbles,
    nodes: &mut Vec<Vec<u8>>,
) -> Result<Option<Vec<u8>>> {
    let node = resolve(handle)?;

    match &node {
        Node::Null => {
            nodes.push(NodeCodec::encode(&Node::Null));
            Ok(None)
        }

        Node::Leaf {
            path: leaf_path,
            value,
        } => {
            nodes.push(NodeCodec::encode(&node));
            if *leaf_path == path {
                Ok(Some(value.clone()))
            } else {
                Ok(None)
            }
        }

        Node::Extension {
            path: ext_path,
            child,
        } => {
            nodes.push(NodeCodec::encode(&node));
            if path.starts_with(ext_path) {
                let (_, remaining) = path.split_at(ext_path.len());
                prove_walk(child, remaining, nodes)
            } else {
                Ok(None) // non-membership: path diverges in extension
            }
        }

        Node::Branch { children, value } => {
            nodes.push(NodeCodec::encode(&node));
            if path.is_empty() {
                return Ok(value.clone());
            }
            let idx = path.at(0) as usize;
            if let Some(child) = &children[idx] {
                let (_, remaining) = path.split_at(1);
                prove_walk(child, remaining, nodes)
            } else {
                Ok(None) // non-membership: no child at this nibble
            }
        }
    }
}

fn resolve(handle: &NodeHandle) -> Result<Node> {
    match handle {
        NodeHandle::InMemory(n) => {
            if **n == Node::Null {
                Ok(Node::Null)
            } else {
                Err(TrieError::InvalidState(
                    "prove() requires a committed trie — call root_hash() first".into(),
                ))
            }
        }
        NodeHandle::Cached(_, n) => Ok(*n.clone()),
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
        // node is the encoded Null (0x00).
        if *root_hash == [0u8; 32] && proof.nodes[0] == [0x00] && proof.value.is_none() {
            return Ok(true);
        }
        return Ok(false);
    }

    // Step 2: walk through proof nodes following the key path.
    let path = Nibbles::from_raw(&proof.key, false);
    let mut remaining = path;
    let mut node_idx: usize = 0;

    loop {
        if node_idx >= proof.nodes.len() {
            return Ok(false);
        }

        let decoded = decode_node(&proof.nodes[node_idx])?;

        match decoded {
            DecodedNode::Null => {
                return Ok(proof.value.is_none());
            }

            DecodedNode::Leaf {
                path: leaf_path,
                value,
            } => {
                if leaf_path == remaining {
                    return Ok(proof.value.as_deref() == Some(value.as_slice()));
                } else {
                    return Ok(proof.value.is_none());
                }
            }

            DecodedNode::Extension {
                path: ext_path,
                child_hash,
            } => {
                if !remaining.starts_with(&ext_path) {
                    return Ok(proof.value.is_none());
                }
                let (_, rest) = remaining.split_at(ext_path.len());
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
                if remaining.is_empty() {
                    return match (&proof.value, &value) {
                        (Some(pv), Some(bv)) => Ok(pv.as_slice() == bv.as_slice()),
                        (None, None) => Ok(true),
                        _ => Ok(false),
                    };
                }
                let idx = remaining.at(0) as usize;
                let (_, rest) = remaining.split_at(1);
                remaining = rest;
                match &children[idx] {
                    None => {
                        return Ok(proof.value.is_none());
                    }
                    Some(child_hash) => {
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
        0x00 => Ok(DecodedNode::Null),

        0x01 => {
            // Leaf: tag + path + value
            let path = decode_path(data, &mut cursor)?;
            let value = decode_bytes(data, &mut cursor)?;
            Ok(DecodedNode::Leaf { path, value })
        }

        0x02 => {
            // Extension: tag + path + 32-byte child_hash
            let path = decode_path(data, &mut cursor)?;
            if cursor + 32 > data.len() {
                return Err(TrieError::InvalidState(
                    "extension node truncated: missing child hash".into(),
                ));
            }
            let mut child_hash = [0u8; 32];
            child_hash.copy_from_slice(&data[cursor..cursor + 32]);
            Ok(DecodedNode::Extension { path, child_hash })
        }

        0x03 => {
            // Branch: tag + u16_le bitmap + has_value + [value] + [child_hashes]
            if cursor + 2 > data.len() {
                return Err(TrieError::InvalidState(
                    "branch node truncated: missing bitmap".into(),
                ));
            }
            let bitmap = u16::from_le_bytes([data[cursor], data[cursor + 1]]);
            cursor += 2;

            if cursor >= data.len() {
                return Err(TrieError::InvalidState(
                    "branch node truncated: missing value flag".into(),
                ));
            }
            let has_value = data[cursor] == 1;
            cursor += 1;

            let value = if has_value {
                Some(decode_bytes(data, &mut cursor)?)
            } else {
                None
            };

            let mut children: [Option<[u8; 32]>; 16] = [None; 16];
            for (i, slot) in children.iter_mut().enumerate() {
                if bitmap & (1 << i) != 0 {
                    if cursor + 32 > data.len() {
                        return Err(TrieError::InvalidState(format!(
                            "branch node truncated: missing child hash at index {i}"
                        )));
                    }
                    let mut h = [0u8; 32];
                    h.copy_from_slice(&data[cursor..cursor + 32]);
                    cursor += 32;
                    *slot = Some(h);
                }
            }

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

fn decode_varint(data: &[u8], cursor: &mut usize) -> Result<usize> {
    let mut result: usize = 0;
    let mut shift = 0;
    loop {
        if *cursor >= data.len() {
            return Err(TrieError::InvalidState("varint truncated".into()));
        }
        let byte = data[*cursor];
        *cursor += 1;
        result |= ((byte & 0x7F) as usize) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err(TrieError::InvalidState("varint overflow".into()));
        }
    }
}

fn decode_path(data: &[u8], cursor: &mut usize) -> Result<Nibbles> {
    let nibble_count = decode_varint(data, cursor)?;
    // Nibbles are packed 2 per byte (high nibble first).
    let byte_count = nibble_count.div_ceil(2);
    if *cursor + byte_count > data.len() {
        return Err(TrieError::InvalidState("path data truncated".into()));
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
    *cursor += byte_count;
    Ok(Nibbles::from_nibbles_unsafe(nibbles))
}

fn decode_bytes(data: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = decode_varint(data, cursor)?;
    if *cursor + len > data.len() {
        return Err(TrieError::InvalidState("bytes data truncated".into()));
    }
    let result = data[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(result)
}
