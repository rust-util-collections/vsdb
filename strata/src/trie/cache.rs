//!
//! Disposable cache for the in-memory trie.
//!
//! After [`MptCalc::root_hash`] is called, the trie nodes carry their
//! precomputed hashes.  This module can save that state to a file so
//! that a future process can reload and apply only the diff since the
//! last snapshot, instead of rebuilding the entire trie.
//!
//! The cache is **disposable**: if the file is missing, corrupted, or
//! stale, the caller simply rebuilds from the authoritative store.
//!

use crate::trie::error::{Result, TrieError};
use crate::trie::nibbles::Nibbles;
use crate::trie::node::{Node, NodeHandle};
use sha3::{Digest, Keccak256};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

const MAGIC: &[u8; 4] = b"MPTC";
const VERSION: u8 = 1;
const CHECKSUM_LEN: usize = 8;

// =========================================================================
// Public API (called from MptCalc)
// =========================================================================

/// Saves an `MptCalc` trie to a writer.
pub(crate) fn save(
    root: &NodeHandle,
    sync_tag: u64,
    root_hash: &[u8],
    w: &mut impl Write,
) -> Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(MAGIC);
    payload.push(VERSION);
    payload.extend_from_slice(&sync_tag.to_le_bytes());

    let hash_len = root_hash.len() as u32;
    payload.extend_from_slice(&hash_len.to_le_bytes());
    payload.extend_from_slice(root_hash);

    let tree_data = serialize_handle(root);
    payload.extend_from_slice(&tree_data);

    let checksum = compute_checksum(&payload);
    w.write_all(&payload).map_err(io_err)?;
    w.write_all(&checksum).map_err(io_err)?;
    Ok(())
}

/// Loads an `MptCalc` trie from a reader.
///
/// Returns `(root_handle, sync_tag, root_hash)`.
pub(crate) fn load(r: &mut impl Read) -> Result<(NodeHandle, u64, Vec<u8>)> {
    let mut all_data = Vec::new();
    r.read_to_end(&mut all_data).map_err(io_err)?;

    if all_data.len() < CHECKSUM_LEN {
        return Err(TrieError::InvalidState("cache file too short".into()));
    }

    let (payload, stored_checksum) = all_data.split_at(all_data.len() - CHECKSUM_LEN);
    let expected = compute_checksum(payload);
    if stored_checksum != expected {
        return Err(TrieError::InvalidState("cache checksum mismatch".into()));
    }

    if payload.len() < 5 {
        return Err(TrieError::InvalidState("cache header too short".into()));
    }
    if &payload[0..4] != MAGIC {
        return Err(TrieError::InvalidState("invalid cache magic".into()));
    }
    if payload[4] != VERSION {
        return Err(TrieError::InvalidState(format!(
            "unsupported cache version {}",
            payload[4]
        )));
    }

    let mut cursor = 5;

    if cursor + 8 > payload.len() {
        return Err(TrieError::InvalidState(
            "unexpected EOF reading sync_tag".into(),
        ));
    }
    let sync_tag = u64::from_le_bytes(payload[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;

    if cursor + 4 > payload.len() {
        return Err(TrieError::InvalidState(
            "unexpected EOF reading hash_len".into(),
        ));
    }
    let hash_len =
        u32::from_le_bytes(payload[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;

    if cursor + hash_len > payload.len() {
        return Err(TrieError::InvalidState(
            "unexpected EOF reading root_hash".into(),
        ));
    }
    let root_hash = payload[cursor..cursor + hash_len].to_vec();
    cursor += hash_len;

    let root = deserialize_handle(payload, &mut cursor)?;
    Ok((root, sync_tag, root_hash))
}

/// Convenience: save to a file path.
pub(crate) fn save_to_file(
    root: &NodeHandle,
    sync_tag: u64,
    root_hash: &[u8],
    path: &Path,
) -> Result<()> {
    let mut f = File::create(path).map_err(io_err)?;
    save(root, sync_tag, root_hash, &mut f)
}

/// Convenience: load from a file path.
pub(crate) fn load_from_file(path: &Path) -> Result<(NodeHandle, u64, Vec<u8>)> {
    let mut f = File::open(path).map_err(io_err)?;
    load(&mut f)
}

// =========================================================================
// Serialization
// =========================================================================

// NodeHandle tag bytes
const HANDLE_INMEMORY: u8 = 0x00;
const HANDLE_CACHED: u8 = 0x01;

// Node type tag bytes
const NODE_NULL: u8 = 0x00;
const NODE_LEAF: u8 = 0x01;
const NODE_EXT: u8 = 0x02;
const NODE_BRANCH: u8 = 0x03;

fn serialize_handle(handle: &NodeHandle) -> Vec<u8> {
    let mut buf = Vec::new();
    match handle {
        NodeHandle::InMemory(node) => {
            buf.push(HANDLE_INMEMORY);
            serialize_node(&mut buf, node);
        }
        NodeHandle::Cached(hash, node) => {
            buf.push(HANDLE_CACHED);
            write_bytes(&mut buf, hash);
            serialize_node(&mut buf, node);
        }
    }
    buf
}

fn serialize_node(buf: &mut Vec<u8>, node: &Node) {
    match node {
        Node::Null => buf.push(NODE_NULL),
        Node::Leaf { path, value } => {
            buf.push(NODE_LEAF);
            write_nibbles(buf, path);
            write_bytes(buf, value);
        }
        Node::Extension { path, child } => {
            buf.push(NODE_EXT);
            write_nibbles(buf, path);
            let child_data = serialize_handle(child);
            buf.extend_from_slice(&child_data);
        }
        Node::Branch { children, value } => {
            buf.push(NODE_BRANCH);

            let mut bitmap: u16 = 0;
            for (i, c) in children.iter().enumerate() {
                if c.is_some() {
                    bitmap |= 1 << i;
                }
            }
            buf.extend_from_slice(&bitmap.to_le_bytes());

            match value {
                Some(v) => {
                    buf.push(1);
                    write_bytes(buf, v);
                }
                None => buf.push(0),
            }

            for child in children.iter().flatten() {
                let child_data = serialize_handle(child);
                buf.extend_from_slice(&child_data);
            }
        }
    }
}

// =========================================================================
// Deserialization
// =========================================================================

fn deserialize_handle(data: &[u8], cursor: &mut usize) -> Result<NodeHandle> {
    let tag = read_u8(data, cursor)?;
    match tag {
        HANDLE_INMEMORY => {
            let node = deserialize_node(data, cursor)?;
            Ok(NodeHandle::InMemory(Box::new(node)))
        }
        HANDLE_CACHED => {
            let hash = read_bytes(data, cursor)?;
            let node = deserialize_node(data, cursor)?;
            Ok(NodeHandle::Cached(hash, Box::new(node)))
        }
        _ => Err(TrieError::InvalidState(format!(
            "invalid handle tag: {tag}"
        ))),
    }
}

fn deserialize_node(data: &[u8], cursor: &mut usize) -> Result<Node> {
    let tag = read_u8(data, cursor)?;
    match tag {
        NODE_NULL => Ok(Node::Null),
        NODE_LEAF => {
            let path = read_nibbles(data, cursor)?;
            let value = read_bytes(data, cursor)?;
            Ok(Node::Leaf { path, value })
        }
        NODE_EXT => {
            let path = read_nibbles(data, cursor)?;
            let child = deserialize_handle(data, cursor)?;
            Ok(Node::Extension { path, child })
        }
        NODE_BRANCH => {
            if *cursor + 2 > data.len() {
                return Err(TrieError::InvalidState(
                    "unexpected EOF in branch bitmap".into(),
                ));
            }
            let bitmap = u16::from_le_bytes([data[*cursor], data[*cursor + 1]]);
            *cursor += 2;

            let has_value = read_u8(data, cursor)?;
            let value = if has_value == 1 {
                Some(read_bytes(data, cursor)?)
            } else {
                None
            };

            let mut children: Box<[Option<NodeHandle>; 16]> = Box::new([
                None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None,
            ]);
            for i in 0..16 {
                if (bitmap & (1 << i)) != 0 {
                    children[i] = Some(deserialize_handle(data, cursor)?);
                }
            }
            Ok(Node::Branch { children, value })
        }
        _ => Err(TrieError::InvalidState(format!("invalid node tag: {tag}"))),
    }
}

// =========================================================================
// Primitive helpers
// =========================================================================

fn write_varint(buf: &mut Vec<u8>, mut n: usize) {
    while n >= 0x80 {
        buf.push(((n as u8) & 0x7F) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

fn read_varint(data: &[u8], cursor: &mut usize) -> Result<usize> {
    let mut n: usize = 0;
    let mut shift: u32 = 0;
    loop {
        if *cursor >= data.len() {
            return Err(TrieError::InvalidState("varint unexpected EOF".into()));
        }
        let b = data[*cursor];
        *cursor += 1;
        if shift >= usize::BITS {
            return Err(TrieError::InvalidState("varint overflow".into()));
        }
        let val = ((b & 0x7F) as usize)
            .checked_shl(shift)
            .ok_or_else(|| TrieError::InvalidState("varint overflow".into()))?;
        n |= val;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(n)
}

fn write_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    write_varint(buf, bytes.len());
    buf.extend_from_slice(bytes);
}

fn read_bytes(data: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = read_varint(data, cursor)?;
    if *cursor + len > data.len() {
        return Err(TrieError::InvalidState("bytes unexpected EOF".into()));
    }
    let bytes = data[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(bytes)
}

fn write_nibbles(buf: &mut Vec<u8>, nibbles: &Nibbles) {
    let raw = nibbles.as_slice();
    write_varint(buf, raw.len());
    buf.extend_from_slice(raw);
}

fn read_nibbles(data: &[u8], cursor: &mut usize) -> Result<Nibbles> {
    let len = read_varint(data, cursor)?;
    if *cursor + len > data.len() {
        return Err(TrieError::InvalidState("nibbles unexpected EOF".into()));
    }
    let raw = data[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(Nibbles::from_nibbles_unsafe(raw))
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Result<u8> {
    if *cursor >= data.len() {
        return Err(TrieError::InvalidState("unexpected EOF".into()));
    }
    let v = data[*cursor];
    *cursor += 1;
    Ok(v)
}

fn io_err(e: std::io::Error) -> TrieError {
    TrieError::InvalidState(format!("I/O error: {e}"))
}

/// Computes a truncated Keccak256 checksum (first 8 bytes).
fn compute_checksum(data: &[u8]) -> [u8; CHECKSUM_LEN] {
    let hash = Keccak256::digest(data);
    let mut out = [0u8; CHECKSUM_LEN];
    out.copy_from_slice(&hash[..CHECKSUM_LEN]);
    out
}
