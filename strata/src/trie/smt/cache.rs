//!
//! Disposable cache for the in-memory SMT.
//!
//! Mirrors [`crate::cache`] but serialises [`SmtHandle`] / [`SmtNode`]
//! instead of MPT nodes.
//!

use crate::trie::error::{Result, TrieError};
use sha3::{Digest, Keccak256};
use std::io::{Read, Write};
use std::path::Path;

use super::bitpath::BitPath;
use super::{SmtHandle, SmtNode};

const MAGIC: &[u8; 4] = b"SMTC";
const VERSION: u8 = 1;
const CHECKSUM_LEN: usize = 8;

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
    let mut all_data = Vec::new();
    r.read_to_end(&mut all_data).map_err(io_err)?;

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

pub(crate) fn save_to_file(
    root: &SmtHandle,
    sync_tag: u64,
    root_hash: &[u8],
    path: &Path,
) -> Result<()> {
    let mut f = std::fs::File::create(path).map_err(io_err)?;
    save(root, sync_tag, root_hash, &mut f)
}

pub(crate) fn load_from_file(path: &Path) -> Result<(SmtHandle, u64, Vec<u8>)> {
    let mut f = std::fs::File::open(path).map_err(io_err)?;
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

fn deserialize_handle(data: &[u8], cursor: &mut usize) -> Result<SmtHandle> {
    let tag = read_u8(data, cursor)?;
    match tag {
        HANDLE_INMEMORY => {
            let node = deserialize_node(data, cursor)?;
            Ok(SmtHandle::InMemory(Box::new(node)))
        }
        HANDLE_CACHED => {
            let hash = read_bytes(data, cursor)?;
            let node = deserialize_node(data, cursor)?;
            Ok(SmtHandle::Cached(hash, Box::new(node)))
        }
        _ => Err(TrieError::InvalidState(format!(
            "invalid SMT handle tag: {tag}"
        ))),
    }
}

fn deserialize_node(data: &[u8], cursor: &mut usize) -> Result<SmtNode> {
    let tag = read_u8(data, cursor)?;
    match tag {
        NODE_EMPTY => Ok(SmtNode::Empty),
        NODE_LEAF => {
            let path = read_bitpath(data, cursor)?;
            if *cursor + 32 > data.len() {
                return Err(TrieError::InvalidState(
                    "unexpected EOF reading key_hash".into(),
                ));
            }
            let mut key_hash = [0u8; 32];
            key_hash.copy_from_slice(&data[*cursor..*cursor + 32]);
            *cursor += 32;
            let value = read_bytes(data, cursor)?;
            Ok(SmtNode::Leaf {
                path,
                key_hash,
                value,
            })
        }
        NODE_INTERNAL => {
            let path = read_bitpath(data, cursor)?;
            let left = deserialize_handle(data, cursor)?;
            let right = deserialize_handle(data, cursor)?;
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

fn write_bitpath(buf: &mut Vec<u8>, path: &BitPath) {
    write_varint(buf, path.len());
    buf.extend_from_slice(path.as_packed());
}

fn read_bitpath(data: &[u8], cursor: &mut usize) -> Result<BitPath> {
    let bit_len = read_varint(data, cursor)?;
    let byte_len = bit_len.div_ceil(8);
    if *cursor + byte_len > data.len() {
        return Err(TrieError::InvalidState("bitpath unexpected EOF".into()));
    }
    let packed = data[*cursor..*cursor + byte_len].to_vec();
    *cursor += byte_len;
    Ok(BitPath::from_packed(packed, bit_len))
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

fn compute_checksum(data: &[u8]) -> [u8; CHECKSUM_LEN] {
    let hash = Keccak256::digest(data);
    let mut out = [0u8; CHECKSUM_LEN];
    out.copy_from_slice(&hash[..CHECKSUM_LEN]);
    out
}
