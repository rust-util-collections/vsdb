//!
//! Disposable cache for the in-memory SMT.
//!
//! Mirrors [`crate::cache`] but serialises [`SmtHandle`] / [`SmtNode`]
//! instead of MPT nodes.
//!

use crate::trie::{
    codec_util::{
        CHECKSUM_LEN, checked_end, compute_checksum, io_err, read_bytes,
        read_cache_bytes, read_u8, read_varint, validate_cache_file_size, write_bytes,
        write_varint,
    },
    error::{Result, TrieError},
};
use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

use super::{
    EMPTY_HASH, SmtHandle, SmtNode,
    bitpath::BitPath,
    codec::{hash_internal, hash_leaf, wrap_hash},
};

const MAGIC: &[u8; 4] = b"SMTC";
// v3: leaf-shortcut hash domain (lone-leaf subtrees commit to the leaf
// hash depth-independently, Diem/JMT style) — v2 caches carry
// incompatible hashes and must be rejected so the trie is rebuilt from
// authoritative data.
// v2: internal-node hashing gained a 0x00 domain byte (leaf/internal
// domain separation).
const VERSION: u8 = 3;

// =========================================================================
// Public API
// =========================================================================

pub(crate) fn save(
    root: &SmtHandle,
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

pub(crate) fn load(r: &mut impl Read) -> Result<(SmtHandle, u64, Vec<u8>)> {
    let all_data = read_cache_bytes(r)?;

    if all_data.len() < CHECKSUM_LEN {
        return Err(TrieError::InvalidState("SMT cache file too short".into()));
    }

    let (payload, stored_checksum) = all_data.split_at(all_data.len() - CHECKSUM_LEN);
    let expected = compute_checksum(payload);
    if stored_checksum != expected {
        return Err(TrieError::InvalidState(
            "SMT cache checksum mismatch".into(),
        ));
    }

    if payload.len() < 5 {
        return Err(TrieError::InvalidState("SMT cache header too short".into()));
    }
    if &payload[0..4] != MAGIC {
        return Err(TrieError::InvalidState("invalid SMT cache magic".into()));
    }
    if payload[4] != VERSION {
        return Err(TrieError::InvalidState(format!(
            "unsupported SMT cache version {}",
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
            "SMT cache root hash length {hash_len} != 32"
        )));
    }

    let end = checked_end(cursor, hash_len, payload.len(), "root_hash")?;
    let root_hash = payload[cursor..end].to_vec();
    cursor = end;

    let root = deserialize_handle(payload, &mut cursor, &BitPath::default())?;
    if cursor != payload.len() {
        return Err(TrieError::InvalidState(
            "trailing bytes after SMT cache root".into(),
        ));
    }
    let computed = validate_cached_handle(&root, true)?;
    if root_hash.as_slice() != computed {
        return Err(TrieError::InvalidState(
            "SMT cache root hash does not match the tree".into(),
        ));
    }
    Ok((root, sync_tag, root_hash))
}

pub(crate) fn save_to_file(
    root: &SmtHandle,
    sync_tag: u64,
    root_hash: &[u8],
    path: &Path,
) -> Result<()> {
    let mut f = File::create(path).map_err(io_err)?;
    save(root, sync_tag, root_hash, &mut f)
}

pub(crate) fn load_from_file(path: &Path) -> Result<(SmtHandle, u64, Vec<u8>)> {
    validate_cache_file_size(path)?;
    let mut f = File::open(path).map_err(io_err)?;
    load(&mut f)
}

// =========================================================================
// Serialization
// =========================================================================

const HANDLE_INMEMORY: u8 = 0x00;
const HANDLE_CACHED: u8 = 0x01;

const NODE_EMPTY: u8 = 0x00;
const NODE_LEAF: u8 = 0x01;
const NODE_INTERNAL: u8 = 0x02;

fn serialize_handle(handle: &SmtHandle) -> Vec<u8> {
    let mut buf = Vec::new();
    match handle {
        SmtHandle::InMemory(node) => {
            buf.push(HANDLE_INMEMORY);
            serialize_node(&mut buf, node);
        }
        SmtHandle::Cached(hash, node) => {
            buf.push(HANDLE_CACHED);
            write_bytes(&mut buf, hash);
            serialize_node(&mut buf, node);
        }
    }
    buf
}

fn serialize_node(buf: &mut Vec<u8>, node: &SmtNode) {
    match node {
        SmtNode::Empty => buf.push(NODE_EMPTY),
        SmtNode::Leaf {
            path,
            key_hash,
            value,
        } => {
            buf.push(NODE_LEAF);
            write_bitpath(buf, path);
            buf.extend_from_slice(key_hash);
            write_bytes(buf, value);
        }
        SmtNode::Internal { path, left, right } => {
            buf.push(NODE_INTERNAL);
            write_bitpath(buf, path);
            let left_data = serialize_handle(left);
            buf.extend_from_slice(&left_data);
            let right_data = serialize_handle(right);
            buf.extend_from_slice(&right_data);
        }
    }
}

// =========================================================================
// Deserialization
// =========================================================================

/// Number of bits in a full SMT key path (Keccak-256 key hash).
const FULL_PATH_BITS: usize = 256;
/// Byte length of every node hash (Keccak-256 output).
const NODE_HASH_LEN: usize = 32;

/// Deserializes one handle, validating the whole-tree structure that
/// per-node checks cannot see.
///
/// `prefix` is the routing path accumulated from the root down to this
/// node (internal-node paths plus one branch bit per level).  The
/// mutation/query walkers assume every loaded tree is *canonically
/// positioned* — leaves sit exactly at their key hash's path,
/// cumulative depth never exceeds 256 bits, and cached hashes are
/// 32 bytes.  A checksum-valid but malformed file violating any of
/// these could otherwise drive the walkers into states organic trees
/// can never reach (an insert-depth rejection that drops the consumed
/// working tree, out-of-range path arithmetic, or a bad hash length
/// surfacing as a `commit` failure), so such files are rejected here,
/// at the trust boundary — the cache is disposable and the caller
/// simply rebuilds from authoritative data.
fn deserialize_handle(
    data: &[u8],
    cursor: &mut usize,
    prefix: &BitPath,
) -> Result<SmtHandle> {
    let tag = read_u8(data, cursor)?;
    match tag {
        HANDLE_INMEMORY => {
            let node = deserialize_node(data, cursor, prefix)?;
            Ok(SmtHandle::InMemory(Box::new(node)))
        }

        HANDLE_CACHED => {
            let hash = read_bytes(data, cursor)?;
            if hash.len() != NODE_HASH_LEN {
                return Err(TrieError::InvalidState(format!(
                    "SMT cache: cached hash length {} != {NODE_HASH_LEN}",
                    hash.len()
                )));
            }
            let node = deserialize_node(data, cursor, prefix)?;
            Ok(SmtHandle::Cached(hash, Box::new(node)))
        }
        _ => Err(TrieError::InvalidState(format!(
            "invalid SMT handle tag: {tag}"
        ))),
    }
}

fn validate_cached_handle(handle: &SmtHandle, is_root: bool) -> Result<[u8; 32]> {
    let SmtHandle::Cached(stored, node) = handle else {
        return Err(TrieError::InvalidState(
            "SMT cache contains an unhashed node".into(),
        ));
    };
    let stored: [u8; 32] = stored.as_slice().try_into().map_err(|_| {
        TrieError::InvalidState("SMT cache has a bad hash length".into())
    })?;

    let computed = match node.as_ref() {
        SmtNode::Empty => {
            if !is_root {
                return Err(TrieError::InvalidState(
                    "SMT cache contains an empty internal child".into(),
                ));
            }
            EMPTY_HASH
        }
        SmtNode::Leaf {
            key_hash, value, ..
        } => hash_leaf(key_hash, value),
        SmtNode::Internal { path, left, right } => {
            if left.is_empty() || right.is_empty() {
                return Err(TrieError::InvalidState(
                    "SMT cache contains a non-canonical single-child internal node"
                        .into(),
                ));
            }
            let left_hash = validate_cached_handle(left, false)?;
            let right_hash = validate_cached_handle(right, false)?;
            wrap_hash(hash_internal(&left_hash, &right_hash), path)
        }
    };
    if stored != computed {
        return Err(TrieError::InvalidState(
            "SMT cache contains an incorrect cached hash".into(),
        ));
    }
    Ok(computed)
}

fn deserialize_node(
    data: &[u8],
    cursor: &mut usize,
    prefix: &BitPath,
) -> Result<SmtNode> {
    let tag = read_u8(data, cursor)?;
    match tag {
        NODE_EMPTY => Ok(SmtNode::Empty),
        NODE_LEAF => {
            let path = read_bitpath(data, cursor)?;
            let end = checked_end(*cursor, 32, data.len(), "key_hash")?;
            let mut key_hash = [0u8; 32];
            key_hash.copy_from_slice(&data[*cursor..end]);
            *cursor = end;
            let value = read_bytes(data, cursor)?;
            // A leaf's routing position plus its residual path must
            // reconstruct its key hash exactly (this is how insert
            // places leaves, and what every walker assumes).
            if prefix.len() + path.len() != FULL_PATH_BITS
                || BitPath::from_hash(&key_hash) != prefix.concat(&path)
            {
                return Err(TrieError::InvalidState(
                    "SMT cache: leaf position incoherent with its key hash".into(),
                ));
            }
            Ok(SmtNode::Leaf {
                path,
                key_hash,
                value,
            })
        }
        NODE_INTERNAL => {
            let path = read_bitpath(data, cursor)?;
            // Children live one branch bit below the compressed path;
            // that bit must still fit inside a 256-bit key path.  This
            // also bounds the deserializer's own recursion (every
            // level consumes at least the branch bit).
            if prefix.len() + path.len() >= FULL_PATH_BITS {
                return Err(TrieError::InvalidState(
                    "SMT cache: cumulative node depth exceeds 256 bits".into(),
                ));
            }
            let child_base = prefix.concat(&path);
            let left = deserialize_handle(
                data,
                cursor,
                &child_base.concat(&BitPath::from_bits(&[0])),
            )?;
            let right = deserialize_handle(
                data,
                cursor,
                &child_base.concat(&BitPath::from_bits(&[1])),
            )?;
            Ok(SmtNode::Internal { path, left, right })
        }
        _ => Err(TrieError::InvalidState(format!(
            "invalid SMT node tag: {tag}"
        ))),
    }
}

// =========================================================================
// Primitive helpers
// =========================================================================

fn write_bitpath(buf: &mut Vec<u8>, path: &BitPath) {
    write_varint(buf, path.len());
    buf.extend_from_slice(path.as_packed());
}

fn read_bitpath(data: &[u8], cursor: &mut usize) -> Result<BitPath> {
    let bit_len = read_varint(data, cursor)?;
    if bit_len > 256 {
        return Err(TrieError::InvalidState(format!(
            "bitpath length {bit_len} exceeds 256 bits"
        )));
    }
    let byte_len = bit_len.div_ceil(8);
    let end = checked_end(*cursor, byte_len, data.len(), "bitpath")?;
    let packed = &data[*cursor..end];
    *cursor = end;
    Ok(BitPath::from_packed(packed, bit_len))
}
