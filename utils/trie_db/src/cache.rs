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

use crate::error::{Result, TrieError};
use crate::nibbles::Nibbles;
use crate::node::{Node, NodeHandle};
use std::io::{Read, Write};
use std::path::Path;

const MAGIC: &[u8; 4] = b"MPTC";
const VERSION: u8 = 1;

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
    w.write_all(MAGIC).map_err(io_err)?;
    w.write_all(&[VERSION]).map_err(io_err)?;
    w.write_all(&sync_tag.to_le_bytes()).map_err(io_err)?;

    let hash_len = root_hash.len() as u32;
    w.write_all(&hash_len.to_le_bytes()).map_err(io_err)?;
    w.write_all(root_hash).map_err(io_err)?;

    let tree_data = serialize_handle(root);
    w.write_all(&tree_data).map_err(io_err)?;
    Ok(())
}

/// Loads an `MptCalc` trie from a reader.
///
/// Returns `(root_handle, sync_tag, root_hash)`.
pub(crate) fn load(
    r: &mut impl Read,
) -> Result<(NodeHandle, u64, Vec<u8>)> {
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic).map_err(io_err)?;
    if &magic != MAGIC {
        return Err(TrieError::InvalidState("invalid cache magic".into()));
    }

    let mut ver = [0u8; 1];
    r.read_exact(&mut ver).map_err(io_err)?;
    if ver[0] != VERSION {
        return Err(TrieError::InvalidState(
            format!("unsupported cache version {}", ver[0]),
        ));
    }

    let mut tag_bytes = [0u8; 8];
    r.read_exact(&mut tag_bytes).map_err(io_err)?;
    let sync_tag = u64::from_le_bytes(tag_bytes);

    let mut hash_len_bytes = [0u8; 4];
    r.read_exact(&mut hash_len_bytes).map_err(io_err)?;
    let hash_len = u32::from_le_bytes(hash_len_bytes) as usize;
    let mut root_hash = vec![0u8; hash_len];
    r.read_exact(&mut root_hash).map_err(io_err)?;

    let mut tree_data = Vec::new();
    r.read_to_end(&mut tree_data).map_err(io_err)?;

    let mut cursor = 0;
    let root = deserialize_handle(&tree_data, &mut cursor)?;
    Ok((root, sync_tag, root_hash))
}

/// Convenience: save to a file path.
pub(crate) fn save_to_file(
    root: &NodeHandle,
    sync_tag: u64,
    root_hash: &[u8],
    path: &Path,
) -> Result<()> {
    let mut f = std::fs::File::create(path).map_err(io_err)?;
    save(root, sync_tag, root_hash, &mut f)
}

/// Convenience: load from a file path.
pub(crate) fn load_from_file(
    path: &Path,
) -> Result<(NodeHandle, u64, Vec<u8>)> {
    let mut f = std::fs::File::open(path).map_err(io_err)?;
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
        _ => Err(TrieError::InvalidState(
            format!("invalid handle tag: {tag}"),
        )),
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
                return Err(TrieError::InvalidState("unexpected EOF in branch bitmap".into()));
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
                None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None,
            ]);
            for i in 0..16 {
                if (bitmap & (1 << i)) != 0 {
                    children[i] = Some(deserialize_handle(data, cursor)?);
                }
            }
            Ok(Node::Branch { children, value })
        }
        _ => Err(TrieError::InvalidState(
            format!("invalid node tag: {tag}"),
        )),
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
