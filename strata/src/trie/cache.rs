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

use crate::trie::codec_util::{
    CHECKSUM_LEN, compute_checksum, io_err, read_bytes, read_u8, read_varint,
    write_bytes, write_varint,
};
use crate::trie::error::{Result, TrieError};
use crate::trie::mpt::MAX_MPT_KEY_LEN;
use crate::trie::nibbles::Nibbles;
use crate::trie::node::{Node, NodeHandle};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

const MAGIC: &[u8; 4] = b"MPTC";
const VERSION: u8 = 1;

/// Maximum cumulative nibbles consumed from the root down to any node —
/// the deepest position an organic trie can reach, since insertion
/// rejects keys over [`MAX_MPT_KEY_LEN`] bytes (2 nibbles per byte).
///
/// Together with the empty-extension rejection below (every level
/// consumes at least one nibble), this also bounds the deserializer's
/// recursion — and every later tree walk — to the same depth organic
/// tries are already documented to stay within.
const MAX_TOTAL_NIBBLES: usize = 2 * MAX_MPT_KEY_LEN;

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

    let root = deserialize_handle(payload, &mut cursor, 0)?;
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

/// Deserializes one handle, validating whole-tree structure that
/// per-node checks cannot see.
///
/// `consumed` is the number of nibbles consumed from the root down to
/// this node.  The tree walkers (insert / remove / commit / `Drop`
/// glue) recurse per level and index branch children by nibble value,
/// assuming every loaded trie stays within the bounds organic
/// insertion enforces ([`MAX_MPT_KEY_LEN`], non-empty extensions,
/// nibble values < 16).  A checksum-valid but malformed file violating
/// those bounds could otherwise overflow the stack or panic on child
/// indexing, so it is rejected here, at the trust boundary — the cache
/// is disposable and the caller simply rebuilds from authoritative
/// data.
fn deserialize_handle(
    data: &[u8],
    cursor: &mut usize,
    consumed: usize,
) -> Result<NodeHandle> {
    let tag = read_u8(data, cursor)?;
    match tag {
        HANDLE_INMEMORY => {
            let node = deserialize_node(data, cursor, consumed)?;
            Ok(NodeHandle::InMemory(Box::new(node)))
        }
        HANDLE_CACHED => {
            let hash = read_bytes(data, cursor)?;
            if hash.len() != 32 {
                return Err(TrieError::InvalidState(format!(
                    "MPT cache: cached hash length {} != 32",
                    hash.len()
                )));
            }
            let node = deserialize_node(data, cursor, consumed)?;
            // A `Cached` parent must not hold `InMemory` children:
            // `commit_rec` skips `Cached` subtrees wholesale, so such a
            // child would never be (re-)hashed by `root_hash()`, and
            // `NodeCodec::encode` on the parent later panics on the
            // child's missing hash (e.g. in `prove`).  Organic saves
            // can never produce this shape — `save_cache` commits the
            // whole tree first — so it is rejected here, at the trust
            // boundary.  Checking direct children is sufficient: every
            // deeper `Cached`→`InMemory` edge is caught by the same
            // check when its own parent handle is deserialized.
            let mixed = match &node {
                Node::Extension { child, .. } => {
                    matches!(child, NodeHandle::InMemory(_))
                }
                Node::Branch { children, .. } => children
                    .iter()
                    .flatten()
                    .any(|c| matches!(c, NodeHandle::InMemory(_))),
                Node::Null | Node::Leaf { .. } => false,
            };
            if mixed {
                return Err(TrieError::InvalidState(
                    "MPT cache: unhashed (InMemory) child under a Cached parent".into(),
                ));
            }
            Ok(NodeHandle::Cached(hash, Box::new(node)))
        }
        _ => Err(TrieError::InvalidState(format!(
            "invalid handle tag: {tag}"
        ))),
    }
}

fn deserialize_node(data: &[u8], cursor: &mut usize, consumed: usize) -> Result<Node> {
    let tag = read_u8(data, cursor)?;
    match tag {
        NODE_NULL => Ok(Node::Null),
        NODE_LEAF => {
            let path = read_nibbles(data, cursor)?;
            check_nibble_budget(consumed, path.len())?;
            let value = read_bytes(data, cursor)?;
            Ok(Node::Leaf { path, value })
        }
        NODE_EXT => {
            let path = read_nibbles(data, cursor)?;
            // Organic extensions are never empty (every construction
            // site guards or merges paths).  An empty one makes zero
            // progress, so a crafted chain of them would recurse — in
            // the walkers and in this deserializer — without ever
            // exhausting the nibble budget.
            if path.is_empty() {
                return Err(TrieError::InvalidState(
                    "MPT cache: empty extension path".into(),
                ));
            }
            check_nibble_budget(consumed, path.len())?;
            let child = deserialize_handle(data, cursor, consumed + path.len())?;
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

            // Descending into a child consumes the routing nibble.
            check_nibble_budget(consumed, 1)?;

            let mut children: Box<[Option<NodeHandle>; 16]> = Box::new([
                None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None,
            ]);
            for i in 0..16 {
                if (bitmap & (1 << i)) != 0 {
                    children[i] = Some(deserialize_handle(data, cursor, consumed + 1)?);
                }
            }
            Ok(Node::Branch { children, value })
        }
        _ => Err(TrieError::InvalidState(format!("invalid node tag: {tag}"))),
    }
}

/// Rejects a node whose path would push the cumulative consumed-nibble
/// count past what any organically inserted key can reach.
fn check_nibble_budget(consumed: usize, additional: usize) -> Result<()> {
    if consumed + additional > MAX_TOTAL_NIBBLES {
        return Err(TrieError::InvalidState(
            "MPT cache: cumulative path length exceeds MAX_MPT_KEY_LEN".into(),
        ));
    }
    Ok(())
}

// =========================================================================
// Primitive helpers
// =========================================================================

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
    // Branch children are indexed by nibble value — an out-of-range
    // nibble from a malformed file would panic on `children[idx]`.
    if raw.iter().any(|&n| n > 0x0F) {
        return Err(TrieError::InvalidState(
            "MPT cache: nibble value out of range".into(),
        ));
    }
    Ok(Nibbles::from_nibbles_unsafe(raw))
}
