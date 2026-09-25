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

use crate::trie::{
    codec_util::{
        CHECKSUM_LEN, checked_end, compute_checksum, io_err, read_bytes,
        read_cache_bytes, read_u8, read_varint, validate_cache_file_size, write_bytes,
        write_varint,
    },
    error::{Result, TrieError},
    mpt::MAX_MPT_KEY_LEN,
    nibbles::Nibbles,
    node::{Node, NodeCodec, NodeHandle},
};
use sha3::{Digest, Keccak256};
use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

const MAGIC: &[u8; 4] = b"MPTC";
const VERSION: u8 = 1;

/// Maximum cumulative nibbles consumed from the root down to any node —
/// the deepest position an organic trie can reach, since insertion
/// rejects keys over [`MAX_MPT_KEY_LEN`] bytes (2 nibbles per byte).
///
/// The decoder tracks this budget explicitly while parsing iteratively.
/// Accepted cache paths therefore match the public insertion limit without
/// placing one call frame on the worker stack for every node.
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
    let all_data = read_cache_bytes(r)?;

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

    let end = checked_end(cursor, 8, payload.len(), "sync_tag")?;
    let sync_tag = u64::from_le_bytes(payload[cursor..end].try_into().unwrap());
    cursor = end;

    let end = checked_end(cursor, 4, payload.len(), "hash_len")?;
    let hash_len = u32::from_le_bytes(payload[cursor..end].try_into().unwrap()) as usize;
    cursor = end;
    if hash_len != 32 {
        return Err(TrieError::InvalidState(format!(
            "cache root hash length {hash_len} != 32"
        )));
    }

    let end = checked_end(cursor, hash_len, payload.len(), "root_hash")?;
    let root_hash = payload[cursor..end].to_vec();
    cursor = end;

    let root = deserialize_handle(payload, &mut cursor, 0)?;
    if cursor != payload.len() {
        return Err(TrieError::InvalidState(
            "trailing bytes after MPT cache root".into(),
        ));
    }
    let computed = validate_cached_handle(&root, true)?;
    if root_hash.as_slice() != computed {
        return Err(TrieError::InvalidState(
            "MPT cache root hash does not match the tree".into(),
        ));
    }
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
    validate_cache_file_size(path)?;
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
    let mut pending = vec![handle];
    while let Some(handle) = pending.pop() {
        match handle.hash() {
            Some(hash) => {
                buf.push(HANDLE_CACHED);
                write_bytes(&mut buf, hash);
            }
            None => buf.push(HANDLE_INMEMORY),
        }
        match handle.node() {
            Node::Null => buf.push(NODE_NULL),
            Node::Leaf { path, value } => {
                buf.push(NODE_LEAF);
                write_nibbles(&mut buf, path);
                write_bytes(&mut buf, value);
            }
            Node::Extension { path, child } => {
                buf.push(NODE_EXT);
                write_nibbles(&mut buf, path);
                pending.push(child);
            }
            Node::Branch { children, value } => {
                buf.push(NODE_BRANCH);
                let mut bitmap: u16 = 0;
                for (i, child) in children.iter().enumerate() {
                    if child.is_some() {
                        bitmap |= 1 << i;
                    }
                }
                buf.extend_from_slice(&bitmap.to_le_bytes());
                match value {
                    Some(value) => {
                        buf.push(1);
                        write_bytes(&mut buf, value);
                    }
                    None => buf.push(0),
                }
                pending.extend(children.iter().rev().flatten());
            }
        }
    }
    buf
}

// =========================================================================
// Deserialization
// =========================================================================

/// Parses the preorder wire format into shallow nodes, then attaches each
/// child to its parent in reverse order. No recursive decoder or recursive
/// cleanup is needed, including for incomplete or invalid trees.
fn deserialize_handle(
    data: &[u8],
    cursor: &mut usize,
    consumed: usize,
) -> Result<NodeHandle> {
    struct Decoded {
        hash: Option<Vec<u8>>,
        node: Node,
        parent: Option<(usize, usize)>,
    }
    let mut pending = vec![(None, consumed)];
    let mut decoded: Vec<Decoded> = Vec::new();
    while let Some((parent, consumed)) = pending.pop() {
        let tag = read_u8(data, cursor)?;
        let hash = match tag {
            HANDLE_INMEMORY => None,
            HANDLE_CACHED => {
                let hash = read_bytes(data, cursor)?;
                if hash.len() != 32 {
                    return Err(TrieError::InvalidState(format!(
                        "MPT cache: cached hash length {} != 32",
                        hash.len()
                    )));
                }
                Some(hash)
            }
            _ => {
                return Err(TrieError::InvalidState(format!(
                    "invalid handle tag: {tag}"
                )));
            }
        };
        let index = decoded.len();
        let node = match read_u8(data, cursor)? {
            NODE_NULL => Node::Null,
            NODE_LEAF => {
                let path = read_nibbles(data, cursor)?;
                check_nibble_budget(consumed, path.len())?;
                Node::Leaf {
                    path,
                    value: read_bytes(data, cursor)?,
                }
            }
            NODE_EXT => {
                let path = read_nibbles(data, cursor)?;
                if path.is_empty() {
                    return Err(TrieError::InvalidState(
                        "MPT cache: empty extension path".into(),
                    ));
                }
                check_nibble_budget(consumed, path.len())?;
                pending.push((Some((index, 0)), consumed + path.len()));
                Node::Extension {
                    path,
                    child: NodeHandle::default(),
                }
            }
            NODE_BRANCH => {
                let end = checked_end(*cursor, 2, data.len(), "branch bitmap")?;
                let bitmap = u16::from_le_bytes(data[*cursor..end].try_into().unwrap());
                *cursor = end;
                let value = if read_u8(data, cursor)? == 1 {
                    Some(read_bytes(data, cursor)?)
                } else {
                    None
                };
                check_nibble_budget(consumed, 1)?;
                for i in (0..16).rev() {
                    if bitmap & (1 << i) != 0 {
                        pending.push((Some((index, i)), consumed + 1));
                    }
                }
                Node::Branch {
                    children: Box::new(std::array::from_fn(|_| None)),
                    value,
                }
            }
            tag => {
                return Err(TrieError::InvalidState(format!("invalid node tag: {tag}")));
            }
        };
        decoded.push(Decoded { hash, node, parent });
    }
    while let Some(Decoded { hash, node, parent }) = decoded.pop() {
        // Cached subtrees are skipped by the hasher, so an unhashed child
        // under a cached parent must still be rejected at this boundary.
        let mixed = match &node {
            Node::Extension { child, .. } => child.hash().is_none(),
            Node::Branch { children, .. } => {
                children.iter().flatten().any(|c| c.hash().is_none())
            }
            _ => false,
        };
        let handle = match hash {
            Some(_) if mixed => {
                return Err(TrieError::InvalidState(
                    "MPT cache: unhashed (InMemory) child under a Cached parent".into(),
                ));
            }
            Some(hash) => NodeHandle::Cached(hash, Box::new(node)),
            None => NodeHandle::InMemory(Box::new(node)),
        };
        let Some((index, slot)) = parent else {
            return Ok(handle);
        };
        match &mut decoded[index].node {
            Node::Extension { child, .. } => *child = handle,
            Node::Branch { children, .. } => children[slot] = Some(handle),
            _ => unreachable!("only extensions and branches schedule children"),
        }
    }
    unreachable!("the parser always schedules a root")
}

fn validate_cached_handle(handle: &NodeHandle, is_root: bool) -> Result<[u8; 32]> {
    let mut pending = vec![(handle, is_root)];
    let mut root_hash = [0u8; 32];
    while let Some((handle, is_root)) = pending.pop() {
        let (stored, node) = match handle {
            NodeHandle::InMemory(node) if is_root && **node == Node::Null => continue,
            NodeHandle::InMemory(_) => {
                return Err(TrieError::InvalidState(
                    "MPT cache contains an unhashed node".into(),
                ));
            }
            NodeHandle::Cached(stored, node) => (stored, node),
        };
        let stored: [u8; 32] = stored.as_slice().try_into().map_err(|_| {
            TrieError::InvalidState("MPT cache has a bad hash length".into())
        })?;
        match node.as_ref() {
            Node::Null => {
                return Err(TrieError::InvalidState(if is_root {
                    "MPT cache contains a non-canonical cached empty root".into()
                } else {
                    "MPT cache contains a nested Null node".into()
                }));
            }
            Node::Leaf { .. } => {}
            Node::Extension { child, .. } => {
                if !matches!(child, NodeHandle::Cached(_, child) if matches!(child.as_ref(), Node::Branch { .. }))
                {
                    return Err(TrieError::InvalidState(
                        "MPT cache contains a non-canonical extension child".into(),
                    ));
                }
                pending.push((child, false));
            }
            Node::Branch { children, value } => {
                let count = children.iter().flatten().count();
                if count == 0 || (count == 1 && value.is_none()) {
                    return Err(TrieError::InvalidState(
                        "MPT cache contains a non-canonical branch".into(),
                    ));
                }
                pending.extend(children.iter().flatten().map(|c| (c, false)));
            }
        }
        let computed: [u8; 32] = Keccak256::digest(NodeCodec::encode(node)).into();
        if stored != computed {
            return Err(TrieError::InvalidState(
                "MPT cache contains an incorrect cached hash".into(),
            ));
        }
        if is_root {
            root_hash = computed;
        }
    }
    Ok(root_hash)
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
    let end = checked_end(*cursor, len, data.len(), "nibbles")?;
    let raw = data[*cursor..end].to_vec();
    *cursor = end;
    // Branch children are indexed by nibble value — an out-of-range
    // nibble from a malformed file would panic on `children[idx]`.
    if raw.iter().any(|&n| n > 0x0F) {
        return Err(TrieError::InvalidState(
            "MPT cache: nibble value out of range".into(),
        ));
    }
    Ok(Nibbles::from_nibbles_unsafe(raw))
}
